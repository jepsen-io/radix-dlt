(ns jepsen.radix-dlt.double-spend
  "This workload attempts to exploit any one of several issues in Radix to
  double-spend a balance.

  For instance, Radix allows transactions to commit in a different order than
  real-time. This means that an attacker Alexis can begin with 10 XRD, pay Bob
  10 XRD, have Bob confirm that the payment has been confirmed, then *override*
  that payment with a later transaction which moves those 10 XRD to a different
  account.

  For each instance of the attack, we use three accounts:

  - payer, where the attacker's money comes from
  - payee, who should be paid by the payer
  - stash, where the attacker tries to squirrel away their funds instead

  We begin with a :f :prep op, which funds the payer account from account 1.
  Then we perform a :f :pay from payer to payee, and quickly follows up with a
  :f :stash op which tries to transfer funds away from the payer to the stash.
  A few seconds later, we perform a :check op which a.) checks the txn log and
  balance of all three accounts.

  Our checker verifies that:

  1. For any attack, at most one of [:pay :stash] commits. If both succeed, we
     obviously have a double-spend.
  2. If pay (or stash) committed, then the balances should reflect that payment
     (or stash).
  3. If pay (or stash) committed, then the txn logs should reflect that payment
     (or stash)."
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ letr]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [independent :as independent]
                    [util :as util :refer [pprint-str map-vals]]]
            [jepsen.radix-dlt [accounts :as a]
                              [client :as rc]
                              [util :as u]]
            [knossos.history :as history]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn attack-accounts
  "Returns a map of :payer, :payee, :stash, each an account id, for a given
  attack key k."
  [k]
  ; We start at 20 to avoid collision with default accounts.
  (let [base (* (+ k 2) 10)]
    {:payer (+ base 0)
     :payee (+ base 1)
     :stash (+ base 2)}))

(def max-expected-fee
  "How big do we expect fees to get for a single txn?"
  200000000000000000N)

(def payment-amount
  "How much does the payer owe the payee?"
  10000000000000000000N)

(defn complete-op
  "Takes an operation and a txn receipt from txn!, and fills in the op's :type
  based on the :status of the txn."
  [op txn]
  (assoc op :type (case (-> txn :status deref :status)
                    :confirmed  :ok
                    :pending    :info
                    :failed     :fail)))

(defrecord Client [conn node accounts token-rri]
  client/Client
  (open! [this test node]
    (assoc this :node node :conn (rc/open node)))

  (setup! [this test]
    ; Fetch the native token RRI
    (locking token-rri
      (when-not (realized? token-rri)
        (deliver token-rri (:rri (rc/native-token conn))))))

  (invoke! [this test {:keys [f value] :as op}]
    (let [[k v]                       value
          {:keys [payer payee stash]} (->> (attack-accounts k)
                                           (map-vals (partial a/id->address
                                                              @accounts)))]
      (rc/with-errors op
        (case f
          ; Fund the payer with enough for the payment and 2 txn fees
          :prep
          (let [funder (rand-nth (vec u/default-account-ids))
                txn' (rc/txn! conn (a/id->key-pair @accounts funder)
                              (str "prep " k)
                              [[:transfer
                                (a/id->address @accounts funder)
                                payer
                                (+ payment-amount (* 2 max-expected-fee))
                                @token-rri]])]
            (complete-op op txn'))

          ; Pay the payee
          :pay
          (let [txn' (rc/txn! conn (a/address->key-pair @accounts payer)
                              (str "pay " k)
                              [[:transfer payer payee payment-amount
                                @token-rri]])]
            (complete-op op txn'))

          ; Stash the money in our stash
          :stash
          (let [txn' (rc/txn! conn (a/address->key-pair @accounts payer)
                              (str "stash " k)
                              [[:transfer payer stash payment-amount
                                @token-rri]])]
            (complete-op op txn'))

          ; Fetch the balances and logs for all 3 accounts
          :check
          (->> [[:payee payee]
                [:payer payer]
                [:stash stash]]
               (map (fn [[k acct]]
                      [k {:balance (->> (rc/token-balances conn acct)
                                        :balances
                                        (filter (comp #{@token-rri} :rri))
                                        first
                                        :amount)
                          :log (->> (rc/txn-history conn acct)
                                    reverse
                                    (mapv (fn [{:keys [message] :as txn}]
                                            (condp re-find message
                                              #"^prep"  :prep
                                              #"^pay"   :pay
                                              #"^stash" :stash
                                              message))))}]))
               (into {})
               (independent/tuple k)
               (assoc op :type :ok, :value))))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defn client
  "Constructs a fresh Client. Takes an accounts atom and an RRI promise."
  [accounts token-rri]
  (map->Client {:accounts accounts
                :token-rri token-rri}))

(defrecord SingleThreaded [gen active-process]
  gen/Generator
  (op [this test ctx]
    (if active-process
      [:pending this]
      (when-let [[op gen'] (gen/op gen test ctx)]
        [op (SingleThreaded. gen' (:process op))])))

  (update [this test ctx event]
    (let [gen' (gen/update gen test ctx event)]
      (if (and active-process
               (= active-process (:process event))
               (#{:ok :info :fail} (:type event)))
        ; Our active process has completed!
        (SingleThreaded. gen' nil)
        (SingleThreaded. gen' active-process)))))

(defn single-threaded
  "Wraps a generator such that only a single worker at a time may execute
  operations on it. Does *not* ensure no operations are logically concurrent:
  if an operation crashes with :info, the generator may go on to invoke
  another."
  [gen]
  (SingleThreaded. gen nil))

(defn gen-attack
  "A function which takes a key and yields a generator for a single attack. As
  a side effect, updates an atom with a set of keys, and the accounts map, with
  that key and account info for that key."
  [ks accounts k]
  ; Record this key
  (swap! ks conj k)
  ; Record account keys/IDs
  (->> (attack-accounts k)
       vals
       (map a/rand-account)
       (mapv (partial swap! accounts a/conj-account)))
  (gen/phases
    ; First, one thread needs to prepare us
    (single-threaded (gen/until-ok (repeat {:f :prep})))
    ; Then we can execute the payment and stash either concurrently or
    ; sequentially
    (rand-nth [
               [{:f :pay} {:f :stash}]
               [{:f :stash} {:f :pay}]
               (single-threaded [{:f :pay} {:f :stash}])
               (single-threaded [{:f :stash} {:f :pay}])
               ])))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (letr [pairs     (history/pair-index+ history)
             completes (->> history
                            (remove (comp #{:invoke} :type))
                            (filter (comp number? :process))
                            (group-by :f)
                            (map-vals last))
             pay       (completes :pay)
             stash     (completes :stash)
             _         (when (or (nil? pay) (nil? stash))
                         ; We didn't manage to actually execute both txns
                         (return {:valid? true}))
             check     (completes :check)
             ; Did the pay and stash operation execute concurrently, or did one
             ; happen first?
             concurrency         (cond (< (:index pay) (:index (pairs stash)))
                                       :pay-first

                                       (< (:index stash) (:index (pairs pay)))
                                       :stash-first

                                       :else :concurrent)
             pay-confirmed?      (= :ok (:type pay))
             stash-confirmed?    (= :ok (:type stash))
             pay-failed?         (= :fail (:type pay))
             stash-failed?       (= :fail (:type stash))
             double-confirmed?   (and pay-confirmed? stash-confirmed?)
             cv                  (:value check)
             {:keys [payer payee stash]} cv

             pos? (fnil pos? 0)

             ; Do balances line up with what we expect?
             balance-consistent?
             (cond ; Nothing we can say; didn't read balances
                   (nil? cv)
                   true

                   pay-confirmed?
                   (and (pos? (:balance payee))
                        (nil? (:balance stash)))

                   stash-confirmed?
                   (and (pos? (:balance stash))
                        (nil? (:balance payee)))

                   ; Nothing should have happened
                   (and pay-failed? stash-failed?)
                   (and (nil? (:balance stash))
                        (nil? (:balance payee)))

                   ; Could be either or none
                   :else
                   (not (and (pos? (:balance stash))
                             (pos? (:balance payee)))))

             ; Do logs line up with what we expect?
             log-consistent?
             (cond ; Didn't read logs; can't say anything
                   (nil? cv)
                   true

                   pay-confirmed?
                   (and (= [:prep :pay] (:log payer))
                        (= [:pay]       (:log payee))
                        (= []           (:log stash)))

                   stash-confirmed?
                   (and (= [:prep :stash] (:log payer))
                        (= [:stash]       (:log stash))
                        (= []             (:log payee)))

                   ; Should be nothing beyond prep
                   (and pay-failed? stash-failed?)
                   (and (or (= [] (:log payer))
                            (= [:prep] (:log payer)))
                        (= [] (:log stash))
                        (= [] (:log payee)))

                   ; Not sure which path we took
                   :else
                   (and ; Definitely prepped
                        (= :prep (first (:log payer)))
                        ; Can't have executed more than 2 txns
                        (<= (count (:log payer)) 2)
                        ; And at least one of stash or payee must be empty
                        (or (= [] (:log payee))
                            (= [] (:log stash)))))
             valid? (and (not double-confirmed?)
                         balance-consistent?
                         log-consistent?)]
        (cond-> {:valid? valid?
                 :concurrency concurrency}
          (not valid?)
          (assoc :pay         (completes :pay)
                 :stash       (completes :stash)
                 :check       cv)

          double-confirmed?
          (assoc :double-confirmed? true)

          (not balance-consistent?)
          (assoc :balance-inconsistent? true)

          (not log-consistent?)
          (assoc :log-inconsistent? true))))))

(defn multi-checker
  "Checks each key independently, then computes aggregate statistics"
  []
  (let [c (independent/checker (checker))]
    (reify checker/Checker
      (check [this test history opts]
        (let [res (checker/check c test history opts)
              n   (count (:results res))
              rs  (vals (:results res))
              double-confirm-count (count (filter :double-confirmed? rs))
              balance-inconsistent-count (count (filter :balance-inconsistent?
                                                        rs))
              log-inconsistent-count (count (filter :log-inconsistent? rs))
              attack-success-count (count (filter (comp :balance :stash :check)
                                                  rs))
              concurrencies (frequencies (map :concurrency rs))
              rate (fn [x] (float (/ x n)))]
          (assoc res
                 :n                         n
                 :double-confirm-rate       (rate double-confirm-count)
                 :balance-inconsistent-rate (rate balance-inconsistent-count)
                 :log-inconsistent-rate     (rate log-inconsistent-count)
                 :attack-success-rate       (rate attack-success-count)
                 :concurrencies             concurrencies))))))

(defn workload
  "Constructs a package of a client, generator, and checker"
  [opts]
  (let [accounts  (atom (a/initial-accounts))
        token-rri (promise)
        ks        (atom (sorted-set))]
    {:client    (client accounts token-rri)
     :checker   (multi-checker)
     :generator (->> (independent/concurrent-generator
                       2 ; Only takes 2 concurrent txns per attack
                       (range)
                       (partial gen-attack ks accounts))
                     (gen/stagger (/ (:rate opts))))
     ; Should do a pure version of this, but stateful seems simpler
     :final-generator (->> (fn [test ctx]
                             (when-let [k (util/rand-nth-empty (vec @ks))]
                               {:f :check, :value (independent/tuple k nil)}))
                           (gen/on-update (fn [gen test ctx event]
                                            (when (and (= :ok (:type event))
                                                       (= :check (:f event)))
                                              ; Mark this as a successful query
                                              (let [[k _] (:value event)]
                                                (swap! ks disj k)))
                                            gen))
                           ; Just to avoid overwhelming Radix with duplicate
                           ; reqs
                           (gen/delay 1/50))}))
