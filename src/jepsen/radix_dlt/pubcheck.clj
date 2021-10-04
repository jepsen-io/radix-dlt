(ns jepsen.radix-dlt.pubcheck
  "This namespace explores a public Radix network, looking for consistency
  violations."
  (:require [clojure [set :as set]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :as dt]
            [fipp.edn :refer [pprint]]
            [jepsen [fs-cache :as cache]
                    [store :as store]
                    [util :as util :refer [pprint-str]]]
            [jepsen.radix-dlt [accounts :as a]
                              [client :as rc]
                              [util :as u]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.io File)))

(defn addresses-in-txn
  "The set of all addresses involved in a txn."
  [txn]
  (->> txn
       :actions
       (mapcat (juxt :from :to))
       (remove nil?)
       set))

(defn addresses-in-log
  "The set of all addresses in a txn log."
  [log]
  (->> log
       (map addresses-in-txn)
       (reduce set/union)))

(defn enqueue-addresses
  "Takes a state and a set of addresses. Enqueues those addresses for checking
  if we haven't checked them already."
  [state addresses]
  (assert (set? addresses))
  (update state :unchecked set/union
          (set/difference addresses (:checked state))))

(defn enqueue-txns
  "Takes a state and a set of txn ids. Enqueues those txns for checking if we
  haven't checked them already."
  [state txn-ids]
  (assert (set? txn-ids))
  (update state :unchecked-txns set/union
          (set/difference txn-ids (:checked-txns state))))

(defn check-log
  "Takes a state, a client, and an address, and checks that address's log
  against the state. Returns a new state."
  [state client address]
  ;(info :checking address)
  (let [log   (->> (rc/txn-history client (rc/->account-address address)) vec)
        ; All txn IDs we touched
        txns  (set (map :id log))
        ;_ (info :txns txns)
        ; What txns did we expect to see?
        expected-txns (get-in state [:addrs->txns address])
        ;_ (info :expected expected-txns)
        _ (info "Expected" (count expected-txns) "/" (count txns) "txns on"
                (-> address
                    rc/->account-address
                    (.toString @rc/current-network-id)))
        ; Are there any txns we *should* have seen that didn't appear?
        missing-txns (set/difference expected-txns txns)
        ; Turn those into errors
        errors (map (fn [txn-id]
                      {:type          :txn-missing-from-log
                       :txn           txn-id
                       :missing-from  address
                       :expected-in   (get-in state [:txns->addrs txn-id])})
                      missing-txns)
        _ (when (seq errors)
            (warn :errors (pprint-str errors)))
        state (update state :errors into errors)
        ; Now run through each txn in the log and link each txn to/from its
        ; addresses.
        state
        (reduce
          (fn [state {:keys [id] :as txn}]
            (reduce
              (fn [state address]
                (-> state
                    (update-in [:addrs->txns address] set/union #{id})
                    (update-in [:txns->addrs id] set/union #{address})))
              state
              (addresses-in-txn txn)))
          state
          log)
        state (-> state
                  ; We've looked at this address now; don't need to again.
                  (update :unchecked disj address)
                  (update :checked conj address)
                  ; Expand our search queue
                  (enqueue-txns txns)
                  (enqueue-addresses (addresses-in-log log)))]
    state))

(defn check-txn-status
  "Takes a state, a client, and a transaction ID, and checks that that txn ID
  is indeed confirmed. If not, adds an error to the state."
  [state client txn-id]
  ; (info "Check txn" txn-id)
  (let [status (:status (rc/txn-status client txn-id))
        state (if (not= status :confirmed)
                (do (warn "Txn" txn-id
                          "visible in log but not confirmed: has status" status)
                    (update state :errors conj
                        {:type    :failed-txn-in-log
                         :txn-id  txn-id
                         :status  status
                         :in-logs (get-in state [:txns->addrs txn-id])}))
                state)
        state (-> state
                  (assoc-in [:txns txn-id :status] status)
                  (update :unchecked-txns disj txn-id)
                  (update :checked-txns conj txn-id))]
    state))

(defn check-step
  "Tries to move the system forward one step by checking either a transaction
  or an address log. Returns nil if nothing more to check."
  [{:keys [unchecked unchecked-txns] :as state} client]
  (cond (seq unchecked-txns)
        (check-txn-status state client (rand-nth (vec unchecked-txns)))

        (seq unchecked)
        (check-log state client (rand-nth (vec unchecked)))

        :else
        nil))

(defmacro with-retry
  "Catches timeouts and retries."
  [& body]
  `(dt/with-retry []
     (try+ ~@body
           (catch [:type :radix-dlt/failure, :code 1004] e#
             (info "Timed out; backing off and retrying.")
             (Thread/sleep (rand-int 10000))
             (~'retry)))))

(defn init-state
  "Constructs a search state based on a starting address."
  [address]
  {; Addresses we know exist but haven't looked at yet.
   :unchecked #{address}
   ; Addresses we've already checked
   :checked   #{}
   ; Transactions we know exist but haven't looked at yet.
   :unchecked-txns #{}
   ; Transactions we've checked.
   :checked-txns #{}
   ; A map of addresses to the txns which *should* have interacted
   ; with it.
   :addrs->txns {}
   ; A map of txn ids to the addresses whose logs contain them.
   :txns->addrs {}
   ; A map of txns to maps like
   ; {:status <status of that txn>}
   :txns {}
   ; Consistency errors
   :errors #{}})

(defn merge-states
  "Merges two states together."
  [a b]
  (let [checked       (set/union (:checked a) (:checked b))
        checked-txns  (set/union (:checked-txns a) (:checked-txns b))]
    {:checked     checked
     :unchecked   (-> (set/union (:unchecked a) (:unchecked b))
                      (set/difference checked))
     :checked-txns   checked-txns
     :unchecked-txns (-> (set/union (:unchecked-txns a) (:unchecked-txns b))
                         (set/difference checked-txns))
     :addrs->txns (merge-with set/union (:addrs->txns a) (:addrs->txns b))
     :txns->addrs (merge-with set/union (:txns->addrs a) (:txns->addrs b))
     :txns        (merge-with merge (:txns a) (:txns b))
     :errors      (set/union (:errors a) (:errors b))}))

(def cache-path
  "Where do we cache our state?"
  [:radixdlt :pubcheck])

(defn load-state
  "Reads state from cache."
  []
  (when-let [f (cache/load-file cache-path)]
    (let [s (store/load-fressian-file f)
          ; Infer checked/unchecked-txns fresh each time
          all-txns     (set/union (set (keys (:txns s)))
                                  (set (keys (:txns->addrs s))))
          checked-txns (->> (:txns s)
                            (keep (fn [[k v]]
                                    (when (:status v)
                                      k)))
                            set)
          unchecked-txns (set/difference all-txns checked-txns)
          s (assoc s
                   :unchecked-txns unchecked-txns
                   :checked-txns   checked-txns)]
      s)))

(defn save-state!
  "Writes state to disk."
  [state]
  (let [t1    (System/nanoTime)
        f     (File/createTempFile "radix-pubcheck-state" ".fressian")
        state (dissoc state :unchecked-txns :checked-txns)
        _     (store/write-fressian-file! state f)
        _     (cache/save-file! f cache-path)
        dt    (long (util/nanos->ms (- (System/nanoTime) t1)))]
    ;(info "Cached state in" dt "ms"))
    ))

(def checkpointing? (atom false))

(defn checkpoint-state!
  "Writes state to disk asynchronously."
  [state]
  (future
    (when (compare-and-set! checkpointing? false true)
      (try
        (save-state! state)
        (finally
          (reset! checkpointing? false))))))

(defn state-summary
  "Takes a state and returns a quick summary."
  [{:keys [unchecked checked unchecked-txns checked-txns txns addrs->txns
           txns->addrs errors]}]
  {:checked-count       (count checked)
   :unchecked-count     (count unchecked)
   :addr-count          (count addrs->txns)
   :checked-txn-count   (count checked-txns)
   :unchecked-txn-count (count unchecked-txns)
   :txn-count           (count txns->addrs)
   :error-count         (count errors)})

(defn uncheck
  "Re-flags all addresses as unchecked in the given state."
  [state]
  (-> state
      (assoc :unchecked (set (keys (:addrs->txns state))))
      (assoc :checked #{})))

(defn uncheck!
  "Re-flags all checked addresses as unchecked in the persistent state."
  []
  (-> (load-state)
      uncheck
      save-state!))

(defn psearch!
  "Parallel search. Keeps track of a single state, and spawns n workers which
  explore the address space; forking off of the main state and merging their
  findings back in. Takes a state, a client, and a number of threads to spawn,
  and returns the resulting state after the search. Also periodically
  checkpoints the state to disk."
  [state client n]
  (let [state     (atom state)
        running?  (atom true)
        saver     (future (while @running?
                            (Thread/sleep 10000)
                            (info (str "\n" (pprint-str (state-summary @state))))
                            (checkpoint-state! @state)))
        search (dt/real-pmap (fn search [i]
                               (when-let [s' (with-retry
                                               (check-step @state client))]
                                 ; Merge our findings back in
                                 (swap! state merge-states s')
                                 ; And go again
                                 (recur i)))
                             (range n))]
    ; Wait for search
    (dorun search)
    ; Wait for saver to complete
    (reset! running? false)
    @saver
    ; Final save
    (checkpoint-state! @state)
    @state))

(defn pubcheck
  "Takes a map of

    :node         The validator node, as an IP or hostname
    :address      The starting address to query
    :concurrency  The number of threads
    :recheck      Reset the checked state of all addresses

  Explores the network reachable from that address, looking for consistency
  violations."
  [{:keys [node address concurrency recheck] :as opts}]
  (info :opts (pprint-str opts))
  ; We're not necessarily stokenet, but as a quick hack, this is how we
  ; tell open! to use https."
  (let [client (rc/open {:stokenet true} node)
        ; Normalize address in case we get an rdx/ddx/etc format; we're gonna
        ; work in {hex} addresses here.
        address (rc/->clj (rc/->account-address address))
        _ (info :node node :address address)
        state (merge (init-state address)
                     (load-state))
        state (if recheck
                (uncheck state)
                state)
        ; Search
        state (psearch! state client concurrency)]
    (info (pprint-str (state-summary state)))
    ))
