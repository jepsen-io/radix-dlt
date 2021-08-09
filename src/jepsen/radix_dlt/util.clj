(ns jepsen.radix-dlt.util
  "Kitchen sink for Radix stuff"
  (:require [clojure.core.typed :as t]))

(t/defalias Account
  "An Account ID is an integer."
  Long)

(t/defalias Balance
  "Account balances are BigInts."
  clojure.lang.BigInt)

(t/ann ^:no-check default-account-ids (t/ASeq Account))
(def default-account-ids
  "The IDs of the accounts that the default universe ships with."
  (range 1 11))

(t/ann default-account-id? [Account -> Boolean])
(defn default-account-id?
  "Is this a default account ID?"
  [id]
  (< 0 id 11))

(t/ann unused-account-ids (t/Set Account))
(def unused-account-ids
  "These are IDs that our workload doesn't touch, but that still carry a
  balance. We use this to filter out balance reads of these spurious accounts."
  (disj (set default-account-ids) 1))

(def fee-scale
  "Roughly how large are fees?"
  100000000000000000N)

