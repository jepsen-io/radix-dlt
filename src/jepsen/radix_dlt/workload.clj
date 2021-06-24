(ns jepsen.radix-dlt.workload
  "A generator, client, and checker for RadixDLT"
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.radix-dlt [checker :as rchecker]
                              [client :as rc]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def default-account-ids
  "The IDs of the accounts that the default universe ships with."
  (range 1 6))

(defn accounts
  "This structure helps us track the mapping between our logical account IDs
  (small integers), keypairs, and account addresses. It comprises a collection
  of accounts, each a map of {:id, :key-pair, :address}, and indices :by-id,
  :by-address"
  []
  {:accounts       []
   :by-id          {}
   :by-address     {}
   :by-address-str {}})

(defn conj-account
  "Adds an account to an accounts structure."
  [accounts account]
  {:accounts    (conj  (:accounts accounts)   account)
   :by-id       (assoc (:by-id accounts)      (:id account)       account)
   :by-address  (assoc (:by-address accounts) (:address account)  account)
   :by-address-str (assoc (:by-address-str accounts)
                          (str (:address account))
                          account)})

(defn id->address
  "Converts an ID to an address, given an accounts structure."
  [accounts id]
  (-> accounts :by-id (get id) :address
      (assert+ {:type :no-such-id
                :id   id})))

(defn address-str->id
  "Converts an address string to an ID, given an accounts structure."
  [accounts address]
  (-> accounts :by-address-str (get address) :id))

(defn id->key-pair
  "Converts an ID to a key-pair, given an accounts structure."
  [accounts id]
  (-> accounts :by-id (get id) :key-pair
      (assert+ {:type :no-such-id
                :id   id})))

(defn rand-id
  "A random account ID drawn from the accounts map."
  [accounts]
  (-> accounts :accounts rand-nth :id
      (assert+ {:type :no-id?
                :accounts accounts})))

(defn initial-accounts
  "Constructs the initial accounts map, where accounts 1-5 are the hardcoded
  accounts from the universe."
  []
  (reduce (fn [accounts id]
            (let [key-pair (rc/key-pair id)]
              (conj-account
                accounts
                {:id        id
                 :key-pair  key-pair
                 :address   (rc/key-pair->account-address key-pair)})))
          (accounts)
          default-account-ids))

(defmacro with-errors
  "Takes an operation and a body. Evaluates body, converting known exceptions to fail/info operations."
  [op & body]
  `(try+ ~@body
         (catch [:type :timeout] e#
           (assoc ~op :type :info, :error :timeout))
         (catch [:type :radix-dlt/failure, :code 1500] e#
           (assoc ~op :type :info, :error [:substate-not-found
                                           (:message e#)]))))


(defrecord Client [conn node accounts token-rri]
  client/Client
  (open! [this test node]
    (assoc this
           :node node
           :conn (rc/open node)))

  (setup! [this test]
    ; Fetch the native token RRI
    (locking token-rri
      (when-not (realized? token-rri)
        (deliver token-rri (:rri (rc/native-token conn))))))

  (invoke! [this test {:keys [:f :value] :as op}]
    (with-errors op
      (case f
        :txn
        (let [; Map account IDs to addresses and add the token RRI to each op.
              ops (map (fn [op]
                         (case (first op)
                           :transfer (let [[f id1 id2 amount] op]
                                       [f
                                        (id->address @accounts id1)
                                        (id->address @accounts id2)
                                        amount
                                        @token-rri])))
                       (:ops value))
              ;_ (info :ops ops)
              ; Execute transaction
              txn' (rc/txn! conn
                            (id->key-pair @accounts (:from value))
                            (str "t" (:id value))
                            ops)
              ; No matter what, we at least know our transaction ID
              value' (assoc value :tx-id (:id txn'))
              ; Await execution
              status (-> txn' :status deref :status)]
          (assoc op :type (case status
                            :confirmed  :ok
                            :pending    :info
                            :failed     :fail)))

        :txn-log
        (let [; A little helper: we want to translate addresses into numeric IDs
              ; when we know them, but leave them as big hex strings otherwise
              address->id (fn [address]
                            (or (address-str->id @accounts address)
                                address))
              log (->> (rc/txn-history conn (id->address @accounts
                                                         (:account value)))
                       reverse
                       (mapv (fn [{:keys [fee message actions]}]
                               {:fee      fee
                                :message  message
                                ; Rewrite accounts to IDs
                                :actions (mapv (fn [action]
                                                 (-> action
                                                     (update :from address->id)
                                                     (update :to address->id)))
                                               actions)})))
              value'  (assoc value :txns log)]
          (assoc op :type :ok :value value'))

        :balance
        (let [b (->> (rc/token-balances conn (id->address @accounts
                                                          (:account value)))
                     :balances
                     (filter (comp #{@token-rri} :rri))
                     first
                     :amount)
              value' (assoc value :balance b)]
          (assoc op :type :ok, :value value')))))

  (teardown! [this test])

  (close! [this test]))

(defn client
  "Constructs a fresh Jepsen client. Takes an accounts atom."
  [accounts]
  (Client. nil nil accounts (promise)))

;; Generator

(defn rand-op
  "Constructs a randomized transfer operation like [:transfer 1 2 300] from the
  given account. Options:

    :accounts An atom of the accounts map."
  [from {:keys [accounts]}]
  ; We can't generate 0 transfers
  [:transfer from (rand-id @accounts) (inc (rand-int 100))])

(defn rand-ops
  "Constructs a short random series of operations all from the same account.
  Options:

    :accounts An atom of the accounts map."
  [from opts]
  (->> (partial rand-op from opts)
       repeatedly
       ; We can't construct empty transactions
       (take (inc (rand-int 2)))
       vec))

(defn txns
  "Emits random transactions. Our transactions are written:

    {:id    1
     :from  2
     :ops   [[:transfer 1 2 300] ...]}

  This transaction has id 1, which is a globally unique identifier encoded in
  the message of the transaction. It's going to be signed by account 2. It
  begins by transferring 300 units from account 1 to account 2. Options:

    :txn-rate   The approximate rate, in hz, of txns"
  [opts]
  (->> (range)
       (map (fn [id]
              (let [from (rand-id @(:accounts opts))]
                {:f     :txn
                 :value {:id id, :from from, :ops (rand-ops from opts)}})))
       (gen/stagger (/ (:txn-rate opts)))))

(defn txn-logs
  "Generates reads of the transaction log on individual accounts, e.g.:

    {:type :invoke, :f :txn-log, :value {:account 1}}

  Options:

    :txn-log-rate   The approximate rate, in hz, of txn log requests"
  [opts]
  (->> (fn gen []
         {:f     :txn-log
          :value {:account (rand-id @(:accounts opts))}})
       (gen/stagger (/ (:txn-log-rate opts)))))

(defn balances
  "Generates reads of the current balance on individual accounts, e.g.:

    {:type :invoke, :f :balance, {:account 1}}

  Options:

    :balance-rate   The approximate rate, in hz, of balance requests"
  [opts]
  (->> (fn gen []
         {:f      :balance
          :value  {:account (rand-id @(:accounts opts))}})
       (gen/stagger (/ (:balance-rate opts)))))

(defn gen
  "Generates operations given CLI opts"
  [opts]
  (gen/any (txns opts)
           (txn-logs opts)
           (balances opts)))

(defn workload
  "Constructs a package of a client and generator."
  [opts]
  (let [accounts (atom (initial-accounts))]
    {:client          (client accounts)
     :checker         (rchecker/checker)
     :generator       (gen (assoc opts :accounts accounts))
     :final-generator (delay
                        (->> @accounts
                             :accounts
                             (map :id)
                             (map (fn [acct]
                                    {:f :txn-log, :value {:account acct}}))))}))
