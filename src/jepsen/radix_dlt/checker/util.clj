(ns jepsen.radix-dlt.checker.util
  "Utilities for analyzing Radix histories."
  (:require [clojure [set :as set]]
            [knossos.op :as op]))

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

(defn known-balances
  "Takes a Radix history and returns a map of accounts to the sorted set of all
  balances we think definitely occurred on that account."
  [history]
  (merge-with set/union
              (balance-balances history)
              ; TODO: Play forward txn logs to get balances too
              ))
