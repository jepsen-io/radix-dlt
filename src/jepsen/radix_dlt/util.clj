(ns jepsen.radix-dlt.util
  "Kitchen sink for Radix stuff")

(def fee
  "How much are fees?"
  100000000000000000N)

(def default-account-ids
  "The IDs of the accounts that the default universe ships with."
  (range 1 6))

(defn default-account-id?
  "Is this a default account ID?"
  [id]
  (< 0 id 6))
