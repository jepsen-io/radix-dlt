(ns jepsen.radix-dlt.checker.util
  "Utilities for analyzing Radix histories."
  (:require [clojure [set :as set]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [util :refer [map-vals pprint-str parse-long]]]
            [knossos.op :as op]))

(def init-balance
  "How much XRD do accounts start with?"
  1000000000000000000000000000000000000000000000)

(defn txn-id
  "Takes a txn from a txn-log operation and returns the ID encoded in its
  message, if one exists."
  [txn]
  (when-let [msg (:message txn)]
    (when-let [[match id] (re-find #"t(\d+)" msg)]
      (parse-long id))))

(defn xrd?
  "Is an RRI XRD?"
  [rri]
  (when rri
    (re-find #"^xrd_" rri)))

(defn mop-accounts
  "Takes a micro-op in a Radix transaction (e.g. [:transfer 1 2 50]) and
  returns the collection of involved accounts (e.g. [1 2])"
  [op]
  (case (first op)
    :transfer (subvec op 1 3)))

(defn txn-op-accounts
  "A sorted set of all accounts involved in a Radix :txn op"
  [op]
  (->> (:value op)
       :ops
       (mapcat mop-accounts)
       (cons (:from (:value op)))
       (into (sorted-set))))

(defn op-accounts
  "A set of all accounts involved in a Radix op."
  [{:keys [f value] :as op}]
  (case f
    :balance #{(:account value)}
    :txn-log #{(:account value)}
    :txn     (txn-op-accounts op)))

(defn all-accounts
  "Takes a Radix history, and returns the sorted set of all accounts in it."
  [history]
  (->> history
       (filter op/invoke?)
       (map op-accounts)
       (reduce set/union (sorted-set))))

(defn longest-txn-logs
  "Takes a Radix history and finds a map of account IDs to the longest possible
  transaction log for that account."
  [history]
  (reduce (fn [longest {:keys [type f value] :as op}]
            (if (and (= :ok type) (= :txn-log f))
              (let [account (:account value)
                    log     (get longest account)
                    log2    value]
                (if (or (nil? log) (< (count (:txns log))
                                      (count (:txns log2))))
                  ; This log is longer
                  (assoc longest account log2)
                  longest))
              ; Not a txn log
              longest))
          {}
          history))

(defn apply-txn
  "Takes an RRI of interest, an account ID, a balance number and applies a
  transaction (i.e. from a txn-log :txns field) to it, returning a new balance
  number."
  [account balance txn]
  ;(info :account account :balance balance :txn txn)
  ; If we're the originator of this transaction, then we paid the fee
  (let [balance (if (-> txn :actions first :from (= account))
                 (- balance (:fee txn))
                 balance)]
    (reduce (fn [balance {:keys [type from to amount rri]}]
              ;(info :balance balance :type type :from from :to to :amount amount :rri rri :xrd? (xrd? rri))
              ; We're only interested in xrd transfers
              (if-not (and (= type :transfer) (xrd? rri))
                balance
                (cond-> balance
                  (= from account) (- amount)     ; Debit
                  (= to account)   (+ amount))))  ; Credit
            balance
            (:actions txn))))

(defn add-txn-ids-to-txn-log
  "Takes a transaction log on an account and rewrites it to add :id fields
  extracted from the transaction messages, where possible."
  [txn-log]
  (let [txns' (map (fn [txn]
                     (if-let [id (txn-id txn)]
                       (assoc txn :id id)
                       txn))
                   (:txns txn-log))]
    (assoc txn-log :txns txns')))

(defn add-id-index-to-txn-log
  "Takes a transaction log with transaction IDs and adds a :by-id field: a map
  of txn ids to the corresponding txn map in the log."
  [txn-log]
  (->> (:txns txn-log)
       (filter :id)
       (map (juxt :id identity))
       (into {})
       (assoc txn-log :by-id)))

(defn add-balances-to-txn-log
  "Takes a transaction log on an account, and rewrites it to add a :balance and
  balance' field to each transaction: the XRD balance before and after that
  transaction was applied."
  [txn-log]
  (let [account (:account txn-log)
        txns' (reduce (fn [journal txn]
                        (let [balance (if-let [prev (peek journal)]
                                        (:balance' prev)
                                        init-balance)
                              balance' (apply-txn account balance txn)
                              txn' (assoc txn
                                          :balance  balance
                                          :balance' balance')]
                          (conj journal txn')))
                      []
                      (:txns txn-log))]
    (assoc txn-log :txns txns')))

(defn txn-logs
  "Takes a Radix history and computes a map of account to the augmented txn
  log for that account."
  [history]
  (->> history
       longest-txn-logs
       (map-vals (fn [txn-log]
                   (-> txn-log
                       add-txn-ids-to-txn-log
                       add-balances-to-txn-log
                       add-id-index-to-txn-log)))))

(defn txn-log->balance->txn-ids
  "Takes a txn-log. Plays forward the transactions, building a map of balances
  to the vector of transaction IDs which produced that balance, or :multiple if
  more than one exists."
  [{:keys [account txns]}]
  ; First, we're going to need a series of vectors of transaction IDs: one for
  ; each set of transactions "read" by each balance.
  ;
  ;   []
  ;   [1]
  ;   [1 3]
  ;   [1 3 8]
  ;   ...
  ; We're going to do this by constructing a single vector of all txn IDs, and
  ; constructing subvecs of it as needed.
  (let [txn-ids (vec (keep txn-id txns))]
    ; Now, step through transactions, updating our account balance and building
    ; a map of balances to subvecs of txn-ids
    (loop [m       {}
           balance init-balance
           ; Index into our transactions list
           txn-i     0
           ; Index into the transaction ids list, since not all txns *have* IDs
           txn-ids-i -1]
      (if (<= (count txns) txn-i)
        ; Done!
        m
        ; OK, let's look at this transaction
        (let [txn        (nth txns txn-i)
              ; If this transaction has an ID, we should step one further into
              ; the txn-ids vector.
              txn-ids-i' (if (txn-id txn)
                           (inc txn-ids-i)
                           txn-ids-i)
              ; What's the resulting balance if we apply it?
              balance'  (apply-txn account balance txn)
              ; Do we already have an entry for this balance?
              extant    (get m balance')
              m'        (assoc m balance'
                               (if extant
                                 ; If we already have a list, convert it to
                                 ; :multiple
                                 :multiple
                                 ; If not, we know this sequence of
                                 ; transactions produced this balance.
                                 (subvec txn-ids 0 (inc txn-ids-i'))))]
          (recur m' balance' (inc txn-i) txn-ids-i'))))))

(defn account->balance->txn-ids
  "Takes a Radix history and computes a map of accounts to maps of balances to
  the sequence of transaction IDs which produced that balance. If we don't know
  how a balance was produced, that value is `nil`; if a balance is not unique
  within an account, that value is `:multiple`."
  [history]
  (->> history
       txn-logs
       (map-vals txn-log->balance->txn-ids)))

(defn balance-balances
  "Takes a Radix history and returns a map of accounts to the sorted set of all
  balances we observed via a :balance operation on that account."
  [history]
  (->> history
       (filter op/ok?)
       (filter (comp #{:balance} :f))
       (map :value)
       (reduce (fn [balances {:keys [account balance]}]
                 (let [bs (-> (get balances account (sorted-set))
                              (conj balance))]
                   (assoc balances account bs)))
               {})))

(defn txn-log-balances
  "Takes a Radix history and returns a map of accounts to the sorted set of all
  balances we think occurred, thanks to the longest :txn-log operation."
  [history]
  (->> (account->balance->txn-ids history)
       (map-vals (fn [balances->txn-ids]
                   (->> balances->txn-ids
                        keys
                        (into (sorted-set)))))))

(defn known-balances
  "Takes a Radix history and returns a map of accounts to the sorted set of all
  balances we think definitely occurred on that account."
  [history]
  (merge-with set/union
              (balance-balances history)
              (txn-log-balances history)))
