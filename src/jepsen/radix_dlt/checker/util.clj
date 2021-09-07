(ns jepsen.radix-dlt.checker.util
  "Utilities for analyzing Radix histories.

  We call the transaction history that Radix returns a 'transaction log', to
  avoid confusion with Jepsen histories.

  We read many transaction logs per account, but we call the longest such log
  'the transaction log' for that account.

  A 'logged' transaction is one which appears in the transaction log.

  An 'unlogged' transaction is one which is not logged, but could have
  happened (i.e. its txn was :ok or :info)

  A 'unique' balance is one which is uniquely resolvable to a specific
  transaction in the transaction log.

  An 'ambiguous' balance is one which is resolvable to more than one
  transaction in the transaction log.

  An 'unresolvable' balance doesn't correspond to any point in the txn log.

  An 'explicable' balance is one which is resolvable to at least one
  transaction in the transaction log, plus zero or more extant unlogged
  transactions.

  An 'inexplicable' balance is any balance which is not explicable."
  (:require [clojure [set :as set]]
            [clojure.core.typed :as t]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :as dt :refer [assert+]]
            [jepsen [util :refer [map-vals pprint-str parse-long]]]
            [jepsen.radix-dlt [util :as u
                               :refer [Account
                                       Balance]]]
            [knossos [history :as history]
                     [op :as op]]
            [slingshot.slingshot :refer [try+ throw+]]))

(t/defalias ActionType
  "These are the types of actions we might see in a transaction log."
  (t/U (t/Value :msg)
       (t/Value :transfer)
       (t/Value :stake)
       (t/Value :unstake)
       (t/Value :unknown)))

(t/defalias TxnLogAction
  "The representation of an action in a transaction log."
  (t/HMap :mandatory {:type       ActionType
                      :from       Account
                      :to         Account
                      :rri        String
                      :validator  (t/Option String)}))

(t/defalias TxnLogTxn
  "The representation of a transaction as read in a transaction log."
  (t/HMap :mandatory {:fee      Balance
                      :message  String
                      :actions  (t/Seq TxnLogAction)}))

(t/defalias TxId Long)

(t/defalias OpAction
  "The representation of a transaction action in a :txn operation."
  (t/HMap :mandatory {:type ActionType
                      :from Account
                      :to Account
                      :amount Balance
                      :rri String}))

(t/defalias OpTxn
  "The representation of a transaction in a :txn operation."
  (t/HMap :mandatory {:id   TxId
                      :from Account
                      :ops  (t/Seq OpAction)}))

(t/defalias GeneralOp (t/HMap :mandatory
                              {:process (t/U (t/Value :nemesis) Long)
                               :time    Long
                               :index   Long
                               :f       t/Any
                               :value   t/Any}))

(t/defalias Invoke    (t/I GeneralOp (t/HMap :mandatory {:type (t/Value :invoke)})))
(t/defalias Ok        (t/I GeneralOp (t/HMap :mandatory {:type (t/Value :ok)})))
(t/defalias Info      (t/I GeneralOp (t/HMap :mandatory {:type (t/Value :info)})))
(t/defalias Fail      (t/I GeneralOp (t/HMap :mandatory {:type (t/Value :fail)})))
(t/defalias Complete  (t/U Ok Info Fail))
(t/defalias Op        (t/U Invoke Ok Info Fail))

(t/defalias TxnOp
  "An arbitrary :txn operation."
  (t/I GeneralOp (t/HMap :mandatory {:f     (t/Value :txn)
                                     :value OpTxn})))

(t/defalias TxnLogInvoke
  "An txn-log invocation."
  (t/I Invoke (t/HMap :mandatory {:f     (t/Value :txn-log)
                                  :value (t/HMap :mandatory {:account Account})})))

(t/defalias TxnLogOk
  "A successful transaction log operation."
  (t/I Ok (t/HMap :mandatory {:f     (t/Value :txn-log)
                              :value (t/HMap :mandatory {:account Account
                                                         :txns    (t/Vec OpTxn)})})))

(t/defalias TxnLogInfo
  "An info transaction log operation."
  (t/I Info (t/HMap :mandatory {:f     (t/Value :txn-log)
                                :value (t/HMap :mandatory {:account Account})})))

(t/defalias TxnLogFail
  "An failed transaction log operation."
  (t/I Fail (t/HMap :mandatory {:f     (t/Value :txn-log)
                                :value (t/HMap :mandatory {:account Account})})))

(t/defalias TxnLogOp
  "All possible txn log ops."
  (t/U TxnLogInvoke TxnLogOk TxnLogInfo TxnLogFail))

(t/defalias BalanceInvoke
  "A balance invocation."
  (t/I Invoke (t/HMap :mandatory {:f      (t/Value :balance)
                                  :value  (t/HMap :mandatory {:account Account})})))

(t/defalias BalanceOk
  "An OK balance op."
  (t/I Ok (t/HMap :mandatory {:f      (t/Value :balance)
                              :value  (t/HMap :mandatory {:account Account
                                                          :balance Balance})})))

(t/defalias BalanceInfo
  "An info balance op."
  (t/I Info (t/HMap :mandatory {:f      (t/Value :balance)
                                :value  (t/HMap :mandatory {:account Account})})))

(t/defalias BalanceFail
  "A failed balance op."
  (t/I Fail (t/HMap :mandatory {:f      (t/Value :balance)
                                :value  (t/HMap :mandatory {:account Account})})))

(t/defalias BalanceOp
  "All kinds of balance ops."
  (t/U BalanceInvoke BalanceOk BalanceInfo BalanceFail))

(t/defalias RadixOp
  "All possible Radix history operations."
  (t/U TxnOp TxnLogOp BalanceOp))

(t/defalias History
  "A Radix history is a vector of RadixOps."
  (t/Vec RadixOp))

(t/defalias PairIndex
  "A mapping of invocations to completions and vice versa."
  (t/Map RadixOp RadixOp))

(t/defalias TxnIndex
  "A mapping of transaction IDs to completion ops."
  (t/Map TxId TxnOp))

(t/ann ^:no-check jepsen.util/parse-long [String -> Long])

(t/ann ^:no-check knossos.history/pair-index+
       (t/All [op] [(t/Seqable op) -> (t/Map op op)]))

(t/ann ^:no-check knossos.op/invoke?
       [Op -> boolean :filters {:then (is Invoke 0) :else (! Invoke 0)}])

(t/ann txn-id [TxnLogTxn -> (t/Option TxId)])
(defn txn-id
  "Takes a txn from a txn-log operation and returns the ID encoded in its
  message, if one exists."
  [txn]
  (when-let [msg (:message txn)]
    (let [[match id] (re-find #"t(\d+)" msg)]
      (when (string? id)
        (parse-long id)))))

(t/ann xrd? [(t/Option String) -> Boolean])
(defn xrd?
  "Is a (potentially nil) RRI XRD?"
  [rri]
  (boolean
    (when rri
      (re-find #"^xrd_" rri))))

(t/ann raw-txn-log? [(RadixOp) -> Boolean])
(defn raw-txn-log?
  "Is this operation a :f :raw-txn-log op?"
  [op]
  (= :raw-txn-log (:f op)))

(t/ann add-pair-index
       [(t/HMap :mandatory {:history History}) ->
        (t/HMap :mandatory {:history    History
                            :pair-index PairIndex})])
(defn add-pair-index
  "Takes an analysis with a :history and adds its :pair-index"
  [analysis]
  (let [pair-index (history/pair-index+ (:history analysis))]
    (t/ann-form pair-index PairIndex)
    (assoc analysis :pair-index pair-index)))

(t/ann ^:no-check add-txn-index
       [(t/HMap :mandatory {:history    History
                            :pair-index PairIndex}) ->
        (t/HMap :mandatory {:history    History
                            :pair-index PairIndex
                            :txn-index  TxnIndex})])
(defn add-txn-index
  "Takes an analysis with a :history and adds its :txn-index"
  [analysis]
  (->> (:history analysis)
       (remove op/invoke?)
       (filter (comp #{:txn} :f))
       (reduce (t/fn [txn-index :- TxnIndex, op :- TxnOp]
                 (assoc txn-index (:id (:value op)) op))
               {})
       (assoc analysis :txn-index)))

(t/ann ^:no-check mop-accounts [OpAction -> (t/Option (t/Seq Account))])
(defn mop-accounts
  "Takes a micro-op in a Radix transaction (e.g. {:type :transfer, :from 1, :to
  2, :amount 50, :rri ...}) and returns the collection of involved accounts
  (e.g. [1 2]) or nil."
  [op]
  (case (:type op)
    :transfer [(:from op) (:to op)]))


(t/ann txn-op-accounts [TxnOp -> (t/Set Account)])
(defn txn-op-accounts
  "A sorted set of all accounts involved in a Radix :txn op"
  [op]
  (->> (:value op)
       :ops
       (mapcat mop-accounts)
       (cons (:from (:value op)))
       (into (sorted-set))))

(t/ann op-accounts [RadixOp -> (t/Set Account)])
(defn op-accounts
  "A (conservative) set of all accounts involved in a Radix op."
  [{:keys [f value] :as op}]
  (case f
    :balance      #{(:account value)}
    :txn-log      #{(:account value)}
    :txn          (txn-op-accounts op)
    ; For raw balances, even though they're technically a read of every account
    ; extant at that time, we consider them just a read of those keys they
    ; returned. We don't try to handle raw hex accounts--just those we could
    ; map back to account IDs.
    :raw-balances (set (filter number? (keys value)))))

(t/ann ^:no-check all-accounts [History -> (t/Set Account)])
(defn all-accounts
  "Takes a Radix history, and returns the sorted set of all accounts in it."
  [history]
  (->> history
       (filter op/invoke?)
       (remove raw-txn-log?)
       (map op-accounts)
       (reduce set/union (sorted-set))))

(t/tc-ignore

(defn init-balance
  "Infers an initial balance for a given account, given a history."
  [account history]
  (if (u/default-account-id? account)
    ; For default account IDs, we should have performed an initial read at the
    ; start of the test.
    (or (->> history
             (take-while (comp #{:balance} :f))
             (keep (fn [{:keys [type value]}]
                     (when (and (= type :ok)
                                (= (:account value) account))
                       (:balance value))))
             first)
        ; Wasn't read?
        (throw+ {:type    :no-initial-balance-read
                 :account account}))

    ; Non-default account IDs: we assume they start at 0
    0N))

(defn txn-op-txn->txn-log-txn
  "Converts a :txn op representation of a transaction to the txn-log
  representation. This is kind of a broken version of this--we're guessing
  about the xrd token name. At some point I'm going to rip out the workload
  code and replace its representation with a map like this."
  [{:keys [id fee ops]}]
  {:id      id
   :fee     fee
   :message (str "t" id)
   :actions ops})

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

(defn txn-account-delta
  "Takes an account ID and a transaction, and returns the net effect of this
  transaction on that account's XRD."
  [account txn]
  (assert (:fee txn) (str "Txn " (pr-str txn) " has no fee!"))
  ; If we're the originator of this transaction, then we paid the fee
  (let [delta (if (-> txn :actions first :from (= account))
                (- (:fee txn))
                0)]
    ; Step through the actions one by one
    (reduce (fn [delta {:keys [type from to amount rri]}]
              ; We're only interested in XRD transfers
              (if-not (and (= type :transfer) (xrd? rri))
                delta
                (cond-> delta
                  (= account from)  (- amount)    ; Debit
                  (= account to)    (+ amount)))) ; Credit
            delta
            (:actions txn))))

(defn apply-txn
  "Takes an account ID, a balance number and applies a
  transaction (i.e. from a txn-log :txns field) to it, returning a new balance
  number."
  [account balance txn]
  (+ balance (txn-account-delta account txn)))

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

(defn add-balances-to-txn-log
  "Takes a transaction log on an account, and an initial balance, and rewrites
  it to add a :balance and balance' field to each transaction: the XRD balance
  before and after that transaction was applied."
  [txn-log init-balance]
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

(defn add-id-index-to-txn-log
  "Takes a transaction log with transaction IDs and adds a :by-id field: a map
  of txn ids to the corresponding txn map in the log."
  [txn-log]
  (->> (:txns txn-log)
       (filter :id)
       (map (juxt :id identity))
       (into {})
       (assoc txn-log :by-id)))

(defn add-balance'-index-to-txn-log
  "Takes a txn log and adds a :by-balance' map to it, which maps balances to
  the vector of transactions which we think could have resulted in that balance
  (or nil if none are known)."
  [txn-log]
  (->> (:txns txn-log)
       (group-by :balance')
       (assoc txn-log :by-balance')))

(defn txn-logs
  "Takes a Radix history and computes a map of account to the augmented txn
  log for that account. An augmented transaction log is of the form:

    {:account   This account ID
     :txns      A vector of transactions
     :by-id     A map of txn ids to corresponding txn maps in the txn log}

  Where each txn is:

    {:id        The txn id (if we have one)
     :actions   The actions that txn performed
     :balance   The balance we think occurred before this txn
     :balance'  The balance we think occurred after this txn}"
  [history]
  (->> history
       longest-txn-logs
       (map-vals (fn [txn-log]
                   (-> txn-log
                       add-txn-ids-to-txn-log
                       (add-balances-to-txn-log
                         (init-balance (:account txn-log) history))
                       add-id-index-to-txn-log
                       add-balance'-index-to-txn-log)))))

(defn raw-txn-logs
  "As an alternative approach to computing transaction logs, we can use the
  *raw* txn logs rather than the archive API. If we do this, it won't let us
  catch as many errors in the archive API. That's why this is broken out into a
  separate code path."
  [{:keys [history txn-index] :as analysis}]
  ; First, find the single longest txn log
  (let [longest (->> history
                     (filter raw-txn-log?)
                     (filter op/ok?)
                     (map :value)
                     (sort-by count)
                     last)
        ; Right. Now this log is *only* a sequence of txn IDs, so we need to
        ; inflate those back into proper txns. We'll do this via the analysis'
        ; transaction index, and build up a map of account IDs to txn logs.
        logs
        (reduce (fn [logs id]
                  (let [op (assert+ (get txn-index id)
                                    {:type :no-txn-with-id
                                     :id   id})
                        accts (txn-op-accounts op)
                        txn   (txn-op-txn->txn-log-txn (:value op))]
                    (reduce (fn [logs acct]
                              (let [log' (-> logs
                                             (get acct {:account acct
                                                        :txns []})
                                             (update :txns conj txn))]
                                (assoc logs acct log')))
                            logs
                            accts)))
                {}
                longest)]
    ; Now that we've built up the txn logs, we augment them with indices and
    ; balances.
    (map-vals (fn [txn-log]
                (-> txn-log
                    (add-balances-to-txn-log
                      (init-balance (:account txn-log) history))
                    add-id-index-to-txn-log
                    add-balance'-index-to-txn-log))
              logs)))

(defn account-history
  "Restricts a history to a single account, and assigns each operation a
  sequential :sub-index."
  [account history]
  (->> history
       (filter (comp integer? :process))
       (filter (fn of-interest? [op]
                 ; The simple variant of this is
                 ; (contains? (op-accounts op) account)
                 ; But this is a significant hotspot, so we do something a
                 ; little faster
                 (let [f     (:f op)
                       value (:value op)]
                   (case f
                     :balance (= account (:account value))
                     :txn-log (= account (:account value))
                     :txn     (or (= account (:from value))
                                  (some #{account} (map :to (:ops value))))
                     :raw-balances true
                     :raw-txn-log true))))
       (map-indexed (fn sub-index [i op] (assoc op :sub-index i)))
       vec))

(def inexplicable-balance-max-unlogged
  "How many unlogged deltas do we allow an inexplicable-balance set to track
  before giving up? Each delta doubles the search space!"
  8)

(defn inexplicable-balances
  "Takes an account analysis, and finds all balance reads in that account's
  history which can't be explained by the transaction log, plus unlogged txns
  (if any)."
  [account-analysis]
  (let [{:keys [account history txn-log pair-index logged-txn-ids init-balance]}
        account-analysis]
    ; (info :checking-account account)
    (loop [history            (->> history
                                   (filter (comp integer? :process))
                                   seq)
           ; A set of balances we think are possible at this point in time
           balances           #{init-balance}
           ; A collection of offsets (from unlogged transactions) that could be
           ; added to the balance.
           unlogged-deltas   #{0}
           ; A vector of inexplicable balance reads.
           inexplicable       []]
      (cond
        ; Done
        (nil? history) inexplicable

        ; Too many unlogged deltas; search space is blowing up!
        (< inexplicable-balance-max-unlogged (count unlogged-deltas))
        (let [h (:history account-analysis) ; Original history
              n (count h)
              i (- n (count history))]
          (warn "Giving up on inexplicable-balance check of account" account
                "at " i "/" n "operations into the history;"
                ;(into (sorted-set) unlogged-deltas)
                "exceeded" inexplicable-balance-max-unlogged
                "unlogged deltas")
          inexplicable)

        ; Right, now consider the next operation
        :else
        (let [{:keys [type f value] :as op} (first history)]
          ; (info :op op)
          (case f
            ; We can ignore txn-log and raw-txn-log ops
            (:txn-log :raw-txn-log)
            (recur (next history) balances unlogged-deltas inexplicable)

            ; For a txn invocation, we need to determine if it's possible this
            ; invocation *will* happen.
            :txn
            (if (or (not= :invoke type)
                    (-> pair-index (get op) op/fail?))
              ; Either this is a completion (in which case we already processed
              ; its impact) or an invocation of a failed op (in which case
              ; nothing changes)
              (recur (next history) balances unlogged-deltas inexplicable)
              ; This invocation might take place. Did we observe it in the log?
              (let [id      (:id value)
                    logged? (contains? logged-txn-ids id)]
                ; (info :id id :logged? logged?)
                (if logged?
                  ; If it's logged, look up what balance this transaction
                  ; yielded.
                  (let [balance' (-> txn-log :by-id (get id) :balance')
                        ; Great, now what are all the possible balances if we
                        ; factor in concurrently executing unlogged txns? Merge
                        ; that into the current set of possible balances.
                        balances' (->> unlogged-deltas
                                       (map (partial + balance'))
                                       (into balances))]
                    (recur (next history) balances' unlogged-deltas
                           inexplicable))
                  ; Not logged. Add this to our unlogged deltas *and* consider
                  ; how it might affect every extant balance.
                  (let [; We need the completed txn with fee in order to figure
                        ; out what its impact might be on the balance
                        value'           (-> pair-index
                                             (get op)
                                             :value
                                             (assert+ "no value!"))
                        txn*             (txn-op-txn->txn-log-txn value')
                        delta            (txn-account-delta account txn*)
                        unlogged-deltas' (->> unlogged-deltas
                                              (map (partial + delta))
                                              (into unlogged-deltas))
                        balances'        (->> balances
                                              (map (partial + delta))
                                              (into balances))]
                    (recur (next history) balances' unlogged-deltas'
                           inexplicable)))))

            ; For a balance, only successful reads matter
            :balance
            (if-not (= :ok type)
              (recur (next history) balances unlogged-deltas inexplicable)

              ; OK, we read something. Was it legal?
              (let [balance (:balance value)]
                ;(info :balance balance :allowed (sort balances))
                (if (or (nil? balance)                ; Account didn't exist
                        (contains? balances balance)) ; Possible
                  (recur (next history) balances unlogged-deltas inexplicable)
                  ; Aha, we caught something!
                  (recur (next history) balances unlogged-deltas
                         (conj inexplicable
                               {:op       op
                                :expected (into (sorted-set) balances)})))))

            ; For a raw balance read, its read of this specific acct has to be
            ; valid.
            :raw-balances
            (if-not (= :ok type)
              (recur (next history) balances unlogged-deltas inexplicable)

              ; OK, we read something. Was the read of this account legal?
              (let [balance (get value account)]
                (if (or (nil? balance)
                        (contains? balances balance))
                  (recur (next history) balances unlogged-deltas inexplicable)
                  ; Aha! Caught one
                  (recur (next history) balances unlogged-deltas
                         (conj inexplicable
                               {:op       op
                                :account  account
                                :actual   balance
                                :expected (into (sorted-set) balances)})))))))))))

(defn balance-balances
  "Takes an account analysis and returns a set of all balances we
  observed via a :balance operation on that account."
  [account-analysis]
  (->> (:history account-analysis)
       (filter op/ok?)
       (filter (comp #{:balance} :f))
       (keep (comp :balance :value))
       set))

(defn txn-log-balances
  "Takes an account analysis and returns a set of all balances we think
  occurred based on the txn log."
  [account-analysis]
  (->> (:txn-log account-analysis)
       :txns
       (map :balance')
       set))

(defn known-balances
  "Takes a partial account analysis and returns a sorted set of all balances we
  think occurred for that account."
  [account-analysis]
  (into (sorted-set)
        (concat [(:init-balance account-analysis)]
                (balance-balances account-analysis)
                (txn-log-balances account-analysis))))

(defn add-accounts
  "Takes an analysis and adds an :accounts map to it."
  [analysis test]
  (let [history   (:history analysis)
        txn-index (:txn-index analysis)
        txn-logs  (if (:raw-txn-logs test)
                    (do (info "Using raw txn logs for inference")
                        (raw-txn-logs analysis))
                    (txn-logs history))]
    ;(info :txn-logs (pprint-str txn-logs))
    (->> (all-accounts history)
         (pmap (fn [account]
                 (let [history      (account-history account history)
                       init-balance (init-balance account history)
                       pair-index   (history/pair-index+ history)
                       txn-log (get txn-logs account)
                       txn-ids (->> history
                                    (filter (comp #{:txn} :f))
                                    (filter (comp #{:ok :info} :type))
                                    (map (comp :id :value))
                                    set)
                       logged-txn-ids   (->> txn-log :by-id keys
                                             (into (sorted-set)))
                       unlogged-txn-ids (set/difference txn-ids logged-txn-ids)
                       ; Build a partial account analysis
                       account-analysis {:account          account
                                         :history          history
                                         :pair-index       pair-index
                                         :init-balance     init-balance
                                         :txn-log          txn-log
                                         :txn-ids          txn-ids
                                         :logged-txn-ids   logged-txn-ids
                                         :unlogged-txn-ids unlogged-txn-ids}
                       ; And go on to compute inexplicable balances
                       account-analysis (-> account-analysis
                                            (assoc :inexplicable-balances
                                                   (inexplicable-balances
                                                     account-analysis))
                                            (assoc :known-balances
                                                   (known-balances
                                                     account-analysis)))]
                   [account account-analysis])))
         (into {})
         (assoc analysis :accounts))))

(defn analysis
  "Computes an *analysis* of a history: a complex map with lots of intermediate
  data structures representing various aspects of each account.

    {:history     The original history
     :pair-index  A map which takes invocations to completions and vice-versa
     :txn-index   A map of transaction IDs to the completion operation for
                  that transaction.
     :accounts    A map of account IDs to account analyses

  An account analysis is:

    {:account     The account ID
     :history     The history restricted to only ops involving this specific
                  account. Includes :sub-index fields on ops: their index
                  within the subhistory.
     :pair-index  A pair index for the account history.
     :init-balance  The balance we think this account started with.
     :txn-log     The longest augmented transaction log for this account.
     :txn-ids     All transactions IDs (except failed ones) which could have
                  possibly interacted with this account.
     :logged-txn-ids    The set of txn ids which appeared in this account's txn
                        log.
     :unlogged-txn-ids  Transaction IDs which might have occurred but do not
                        appear in the transaction log for this particular
                        account.
     :inexplicable-balances A collection of maps which describe balance
                            operations which cannot be mapped to any possible
                            balance for that account.}"
  [history test]
  (-> {:history history}
      add-pair-index
      add-txn-index
      (add-accounts test)))

(defn balance->txn-id-prefix
  "Takes an analysis, an account, and a balance. Returns:

  nil             if that balance was either `nil` or the initial balance
  [t1 ... tn]     if that balance was uniquely resolvable to tn.
  :unresolvable   if that balance is unresolvable
  :ambiguous      if that balance is resolvable to multiple points in the log"
  [analysis account balance]
  (let [txn-log       (get-in analysis [:accounts account :txn-log])
        init-balance  (get-in analysis [:accounts account :init-balance])]
    (cond (nil? balance)            nil
          (= balance init-balance)  nil
          :else (let [txns (get-in txn-log [:by-balance' balance])]
                  (case (count txns)
                    0 :unresolvable
                    1 (let [id (:id (first txns))
                            _ (assert (integer? id))
                            prefix (->> (:txns txn-log)
                                        (keep :id)
                                        (take-while (complement #{id}))
                                        vec)
                            ids (conj prefix id)]
                        (assert (every? integer? ids))
                        ids)
                    :ambiguous)))))


(defn rewrite-info-txns
  "Takes a Radix-DLT history, and rewrites crashed (:info) :txn operations when
  a later :check-txn operation has determined their status. Specifically, the
  :txn :info operation is deleted from the history and is replaced by the
  completion of the txn-check operation, as if the txn-check completion were
  the completion of the original :txn operation. The process of the txn
  operation is replaced by that of the txn-check operation, since we don't know
  if the original node actually confirmed the transaction or not.

  All txn-check operations are removed from the history."
  [history]
  ; First, build an map of txns which we know will later be observed by a
  ; check-txn operation. The keys in this map are txn ids, and the values are
  ; the completion check-txn operation which first observed it. We use this to
  ; look ahead and figure out whether we can skip the :info operation for a
  ; :txn.
  (let [checked (->> history
                     (filter (comp #{:check-txn} :f))
                     (filter op/ok?)
                     ; Specifically, we need to know that the operation was
                     ; confirmed or failed; pending tells us nothing.
                     (reduce (fn [checked {:keys [value] :as op}]
                               (let [id     (:txn-id value)
                                     status (:status value)]
                                 (if (and (not (contains? checked id))
                                          (#{:confirmed :failed} status))
                                   ; This is the first definite read of this tx
                                   (assoc checked id op)
                                   checked)))
                             {}))
        ; We need to look forward to see crashes
        pairs (history/pair-index+ history)]
    ;(info :checked checked)
    (loop [history   (seq history)  ; Remaining ops
           history'  []             ; Resulting history
           txn's     {}]            ; Map of txn-id to :txn completion op
      ;(info :op (first history) :txn's txn's)
      (if-not history
        ; Done
        history'
        (let [{:keys [f type value process] :as op} (first history)]
          (case f
            :txn
            (cond ; We *will* crash, and a later txn-check operation will save
                  ; us. We need to record the check-txn process in our
                  ; checked-by field.
                  (and (= type :invoke)
                       (let [txn-id (:txn-id (:value (get pairs op)))]
                         (contains? checked txn-id)))
                  (let [txn-id (-> pairs (get op) :value :txn-id)
                        p      (-> checked (get txn-id) :process)]
                    ;(info "Invoke" (pr-str op) "will crash and be recovered by"
                    ;      p)
                    (recur (next history)
                           (conj history' (assoc op :checked-by p))
                           txn's))

                  ; We crashed, but a later txn-check operation will save us.
                  ; Save this info op until we get to that point.
                  (and (= type :info)
                       (contains? checked (:txn-id value)))
                  (do ;(info "Txn" (pr-str op) "crashed but will be recovered")
                      (recur (next history)
                             history'
                             (assoc txn's (:txn-id value) op)))

                  ; Something else
                  true
                  (recur (next history)
                         (conj history' op)
                         txn's))

            :check-txn
            (if (and (= type :ok)
                     (#{:confirmed :failed} (:status value)))
              (let [id   (:txn-id value)
                    ; Fetch txn info, if possible
                    txn' (get txn's id)]
                ;(info "Completing" (pr-str txn') "with process" process)
                (if txn'
                  ; Rewrite this check-txn ok to a txn ok/fail
                  (recur (next history)
                         (conj history' (assoc op
                                               :f     :txn
                                               :type  (case (:status value)
                                                        :confirmed  :ok
                                                        :failed     :fail)
                                               :value      (:value txn')
                                               :process    (:process txn')
                                               :checked-by process))
                         (dissoc txn's id))
                  ; Either already resolved, or didn't crash in the first place?
                  (recur (next history) history' txn's)))
              ; Either an invocation, crash, or fail: can't tell us anything
              (recur (next history) history' txn's))

            ; Otherwise, add as normal
            (recur (next history)
                   (conj history' op)
                   txn's)))))))


)
