(ns jepsen.radix-dlt.accounts
  "Helper functions for keeping track of accounts and mapping them back and
  forth from addresses to short numeric IDs."
  (:require [clojure.edn :as edn]
            [dom-top.core :refer [assert+]]
            [jepsen.radix-dlt [client :as rc]
                              [util :as u]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn normalize-address-str
  "Takes an address string (a series of hex digits, optionally surrounded by {
  ... }), and strips the braces from it."
  [^String addr]
  (cond (re-find #"^\{[0-9a-f]+\}$" addr)
        (subs addr 1 (dec (.length addr)))

        (re-find #"^[0-9a-f]+$" addr)
        addr

        :else
        (throw+ {:type :malformed-address-str
                 :address addr})))

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
  (if (get-in accounts [:by-id (:id account)])
    ; Already exists
    (throw+ {:type :account-already-exists
             :id   (:id account)})
    {:accounts    (conj  (:accounts accounts)   account)
     :by-id       (assoc (:by-id accounts)      (:id account)       account)
     :by-address  (assoc (:by-address accounts) (:address account)  account)
     :by-address-str (assoc (:by-address-str accounts)
                            (normalize-address-str (str (:address account)))
                            account)}))

(defn stokenet-account
  "Loads an account map from `stokenet.edn` and assigns it the given ID. Used
  to figure out our main funding account for stokenet testing."
  [id]
  (try
    (let [stokenet (edn/read-string (slurp "stokenet.edn"))
          private  (:private-base64 stokenet)
          _ (assert private "No :private-base64 key in stokenet.edn!")
          key-pair (rc/private-key-str->key-pair private)]
      {:id        id
       :key-pair  key-pair
       :address   (rc/->account-address key-pair)})))

(defn small-account
  "I don't know a good word for this. The default accounts are generated by
  calling (rc/key-pair 1), (rc/key-pair 2), etc, up to 5. We're going to
  generate *more* accounts than just 5: 6, 7, 8, etc. We call these \"small\"
  accounts, as opposed to ones with larger key pairs."
  [id]
  (let [key-pair (rc/key-pair id)]
    {:id        id
     :key-pair  key-pair
     :address   (rc/->account-address key-pair)}))

(defn rand-account
  "Generates a random account and assigns it the given ID."
  [id]
  (let [key-pair (rc/new-key-pair)]
    {:id        id
     :key-pair  key-pair
     :address   (rc/->account-address key-pair)}))

(defn id->address
  "Converts an ID to an address, given an accounts structure."
  [accounts id]
  (-> accounts :by-id (get id) :address
      (assert+ {:type :no-such-id
                :id   id})))

(defn address-str->id
  "Converts an address string to an ID, given an accounts structure."
  [accounts address]
  (-> accounts :by-address-str (get (normalize-address-str address)) :id))

(defn address->id
  "Converts an AccountAddress to an ID, given an accounts structure."
  [accounts address]
  (-> accounts :by-address (get address) :id))

(defn id->key-pair
  "Converts an ID to a key-pair, given an accounts structure."
  [accounts id]
  (-> accounts :by-id (get id) :key-pair
      (assert+ {:type :no-such-id
                :id   id})))

(defn address->key-pair
  "Converts an AccountAddress to a key-pair, given an accounts structure."
  [accounts address]
  (->> address
       (address->id accounts)
       (id->key-pair accounts)))

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
                 :address   (rc/->account-address key-pair)})))
          (accounts)
          u/default-account-ids))
