(ns jepsen.radix-dlt.util
  "Kitchen sink for Radix stuff"
  (:require [clojure.core.typed :as t]))

(t/defalias Account
  "An Account ID is an integer."
  Long)

(t/defalias Balance
  "Account balances are BigInts."
  clojure.lang.BigInt)

(t/ann fee Balance)
(def fee
  "How much are fees?"
  100000000000000000N)

(t/ann ^:no-check default-account-ids (t/ASeq Account))
(def default-account-ids
  "The IDs of the accounts that the default universe ships with."
  (range 1 11))

(t/ann default-account-id? [Account -> Boolean])
(defn default-account-id?
  "Is this a default account ID?"
  [id]
  (< 0 id 11))
