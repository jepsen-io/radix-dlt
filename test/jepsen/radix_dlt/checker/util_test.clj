(ns jepsen.radix-dlt.checker.util-test
  (:require [clojure [test :refer :all]]
            [clojure.core.typed :as t]
            [jepsen.util :refer [map-vals]]
            [jepsen.radix-dlt.checker.util :refer :all]
            [knossos.history :as history]))

;(deftest typecheck
;  (is (t/check-ns 'jepsen.radix-dlt.checker.util)))

(def xrd "xrd_rb1qya85pwq")

(defn strip-sub-indices
  "Removes :sub-index fields from a history."
  [history]
  (mapv (fn [op] (dissoc op :sub-index)) history))

(defn check-sub-indices
  "Takes a history and verifies sub-indices are in ascending order."
  [history]
  (is (= (range 0 (count history))
         (map :sub-index history))))

(defn analysis-
  "A stripped down version of analysis which doesn't require a pair index etc."
  [history]
  (let [history (history/index history)
        a       (analysis history {})]
    (is (map? (:pair-index a)))
    (is (= history (:history a)))
    (-> a
        (dissoc :pair-index :history)
        (update :accounts (partial map-vals
                                   (fn [account-analysis]
                                     (check-sub-indices (:history account-analysis))
                                     (is (map? (:pair-index account-analysis)))
                                     (-> account-analysis
                                         (update :history strip-sub-indices)
                                         (dissoc :pair-index))))))))

(deftest analysis-test
  (testing "empty"
    (let [history []]
          (is (= {:txn-index  {}
                  :accounts   {}}
                 (analysis- history)))))

  (testing "initial nil balance read"
    (let [history
          [{:index 0, :type :invoke, :process 0, :f :balance, :value {:account 11}}
           {:index 1, :type :ok, :process 0, :f :balance, :value {:account 11, :balance nil}}]]
      (is (= {:txn-index {}
              :accounts {11 {:account 11
                             :history history
                             :txn-log nil
                             :txn-ids #{}
                             :logged-txn-ids #{}
                             :unlogged-txn-ids #{}
                             ; Even though we read nil, that's not a "real" balance
                             :known-balances #{0}
                             :inexplicable-balances []}}}
             (analysis- history)))))

  (testing "read of unexpected balance"
    (let [b1  {:index 0, :type :invoke, :process 0, :f :balance, :value {:account 1}}
          b1' {:index 1, :type :ok, :process 0, :f :balance, :value {:account 1, :balance -1}}
          history [b1 b1']]
      (is (= {:txn-index {}
              :accounts {1 {:account 1
                            :history history
                            :txn-log nil
                            :txn-ids #{}
                            :logged-txn-ids #{}
                            :unlogged-txn-ids #{}
                            :known-balances #{1000000000000000000000000000N -1}
                            :inexplicable-balances [{:op       (assoc b1' :sub-index 1)
                                                     :expected #{1000000000000000000000000000N}}]}}}
             (analysis- history)))))

  (testing "unlogged txn"
    (let [t1  {:index 0, :type :invoke, :process 0, :f :txn, :value {:id 0, :from 1, :ops [{:type :transfer :from 1 :to 20 :amount 50 :rri xrd}]}}
          t1' {:index 1, :type :info,   :process 0, :f :txn, :value {:id 0, :from 1, :fee 123, :ops [{:type :transfer :from 1 :to 20 :amount 50 :rri xrd}]}}
          history [t1 t1']]
      (is (= {:txn-index {0 t1'}
              :accounts {1 {:account 1
                            :history history
                            :txn-log nil
                            :txn-ids #{0}
                            :logged-txn-ids #{}
                            :unlogged-txn-ids #{0}
                            :known-balances #{1000000000000000000000000000N}
                            :inexplicable-balances []}
                         20 {:account 20
                            :history history
                            :txn-log nil
                            :txn-ids #{0}
                            :logged-txn-ids #{}
                            :unlogged-txn-ids #{0}
                            :known-balances #{0}
                            :inexplicable-balances []}}}
             (analysis- history)))))

  (testing "info txn unlogged but visible by balance read"
    (let [; Inexplicable read: balance of 10 must be 0 initially
          b20  {:index 0, :type :invoke, :process 1, :f :balance, :value {:account 20}}
          b20' {:index 1, :type :ok,     :process 1, :f :balance, :value {:account 20, :balance 50}}
          ; Transaction adds 50 to 10
          t1  {:index 2, :type :invoke, :process 0, :f :txn, :value {:id 0, :from 1,           :ops [{:type :transfer :from 1 :to 20 :amount 50 :rri xrd}]}}
          t1' {:index 3, :type :info,   :process 0, :f :txn, :value {:id 0, :from 1, :fee 123, :ops [{:type :transfer :from 1 :to 20 :amount 50 :rri xrd}]}}
          ; Now that same read is explicable
          b20-2  {:index 4, :type :invoke, :process 1, :f :balance, :value {:account 20}}
          b20-2' {:index 5, :type :ok,     :process 1, :f :balance, :value {:account 20, :balance 50}}
          history [b20 b20' t1 t1' b20-2 b20-2']]
      (is (= {:txn-index {0 t1'}
              :accounts {1 {:account 1
                            :history [t1 t1']
                            :txn-log nil
                            :txn-ids #{0}
                            :logged-txn-ids #{}
                            :unlogged-txn-ids #{0}
                            :known-balances #{1000000000000000000000000000N}
                            :inexplicable-balances []}
                         20 {:account 20
                            :history history
                            :txn-log nil
                            :txn-ids #{0}
                            :logged-txn-ids #{}
                            :unlogged-txn-ids #{0}
                            :known-balances #{0 50}
                            :inexplicable-balances [{:op (assoc b20' :sub-index 1)
                                                     :expected #{0}}]}}}
             (analysis- history)))))

  (testing "logged txn"
    (let [; Transaction log 1's view of txn 0
          l1-0 {:fee 100000000000000000N
                 :message "t0"
                 :actions [{:type :transfer
                            :rri xrd
                            :from 1
                            :to 11
                            :amount 50}
                           {:type :transfer
                            :rri xrd
                            :from 1
                            :to 12
                            :amount 60}]}
          ; And the augmented representation with inferred id and balances
          l1-0+ (assoc l1-0
                        :id 0
                        :balance 1000000000000000000000000000N
                        ; On account 1, we should subtract both the fee *and* the 50 XRD
                        :balance' 999999999899999999999999890N)
          ; On log 11, we have the same view
          l11-0 l1-0
          ; But different balances
          l11-0+ (assoc l11-0
                        :id 0
                        :balance 0N
                        :balance' 50)
          t1   {:index 0, :type :invoke, :process 0, :f :txn, :value {:id 0, :from 1, :ops [{:type :transfer :from 1 :to 11 :amount 50 :rri xrd}
                                                                                            {:type :transfer :from 1 :to 12 :amount 60 :rri xrd}]}}
          t1'  {:index 1, :type :info,   :process 0, :f :txn, :value {:id 0, :from 1, :fee 100000000000000000N
                                                                      :ops [{:type :transfer :from 1 :to 11 :amount 50 :rri xrd}
                                                                            {:type :transfer :from 1 :to 12 :amount 60 :rri xrd}]}}
          l1   {:index 2, :type :invoke, :process 1, :f :txn-log, :value {:account 1}}
          l1'  {:index 3, :type :ok,     :process 1, :f :txn-log, :value {:account 1, :txns [l1-0]}}
          l11  {:index 4, :type :invoke, :process 2, :f :txn-log, :value {:account 11}}
          l11' {:index 5, :type :ok,     :process 2, :f :txn-log, :value {:account 11, :txns [l11-0]}}
          history [t1 t1' l1 l1' l11 l11']]
      (is (= {:txn-index {0 t1'}
              :accounts {1 {:account 1
                            :history [t1 t1' l1 l1']
                            :txn-log {:account 1
                                      :txns [l1-0+]
                                      :by-id {0 l1-0+}
                                      :by-balance' {999999999899999999999999890N [l1-0+]}}
                            :txn-ids #{0}
                            :logged-txn-ids #{0}
                            :unlogged-txn-ids #{}
                            :known-balances #{1000000000000000000000000000N 999999999899999999999999890N}
                            :inexplicable-balances []}
                         ; Also visible on account 11 as a deposit
                         11 {:account 11
                             :history [t1 t1' l11 l11']
                             :txn-log {:account 11
                                       :txns [l11-0+]
                                       :by-id {0 l11-0+}
                                       :by-balance' {50 [l11-0+]}}
                             :txn-ids #{0}
                             :logged-txn-ids #{0}
                             :unlogged-txn-ids #{}
                             :known-balances #{0 50}
                             :inexplicable-balances []}
                         ; Not logged on acct 12
                         12 {:account 12
                             :history [t1 t1']
                             :txn-log nil
                             ; We know this could have touched 12
                             :txn-ids #{0}
                             ; But it wasn't in the log
                             :logged-txn-ids #{}
                             :unlogged-txn-ids #{0}
                             ; Account 8's balance and txn log were never read,
                             ; so we don't know what balances it went through
                             :known-balances #{0}
                             :inexplicable-balances []}}}
             (analysis- history)))))
)

(deftest rewrite-info-txns-test
  (testing "empty"
    (is (= []
           (rewrite-info-txns []))))

  (testing "basic"
    ; Process is rewritten to the check-txn process
    (is (= [{:index 0, :process 1, :type :invoke, :f :txn, :value {}}
            {:index 3, :process 1, :type :ok, :f :txn, :value {:txn-id :x}}
            ; Crashed before getting a txn-id; passed through
            {:index 4, :process 2, :type :invoke, :f :txn, :value {}}
            {:index 5, :process 2, :type :info, :f :txn, :value {}}
            ; Succeeds
            {:index 6, :process 3, :type :invoke, :f :txn,        :value {}}
            {:index 7, :process 3, :type :ok,     :f :txn,        :value {:txn-id :y}}
           ]
           (rewrite-info-txns
             ; This txn crashes, but is confirmed later
             [{:index 0, :process 0, :type :invoke, :f :txn,        :value {}}
              {:index 1, :process 0, :type :info,   :f :txn,        :value {:txn-id :x}}
              {:index 2, :process 1, :type :invoke, :f :check-txn,  :value {:txn-id :x}}
              {:index 3, :process 1, :type :ok,     :f :check-txn,  :value {:txn-id :x, :status :confirmed}}
              ; This txn crashes before even getting a txn-id
              {:index 4, :process 2, :type :invoke, :f :txn,        :value {}}
              {:index 5, :process 2, :type :info,   :f :txn,        :value {}}
              ; This one succeeds
              {:index 6, :process 3, :type :invoke, :f :txn,        :value {}}
              {:index 7, :process 3, :type :ok,     :f :txn,        :value {:txn-id :y}}
              ])))))
