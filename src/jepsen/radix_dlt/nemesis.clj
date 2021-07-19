(ns jepsen.radix-dlt.nemesis
  "Fault injection for Radix clusters"
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :as dt :refer [letr]]
            [jepsen [generator :as gen]
                    [nemesis :as n]
                    [util :refer [pprint-str]]]
            [jepsen.nemesis [combined :as nc]
                            [membership :as membership]]
            [jepsen.radix-dlt [client :as rc]
                              [db :as db]]
            [slingshot.slingshot :refer [try+ throw+]]))


(def staker
  "Which account do we use to restake funds?"
  2)

(def supermajority
  "How much stake do you need to dominate consensus?"
  2/3)

(defn total-stake
  "What's the total stake in this view of the cluster?"
  [view]
  (->> view
       (map :total-delegated-stake)
       (reduce + 0)))

(defn supermajority-node
  "Which node currently holds a supermajority of this view? Nil if no node is
  known."
  [view]
  (letr [total-stake (total-stake view)
         _ (when (= 0 total-stake) (return nil))
         threshold (* supermajority total-stake)]
    (->> view
         (filter (comp (partial < threshold) :total-delegated-stake))
         first
         :node)))

(defn stake-op
  "Takes a Membership state and returns an op which restakes a supermajority of
  the stake onto a single node, or nil if no such op is needed."
  [{:keys [view pending] :as membership}]
  ; Look at the distribution of stake
  (letr [_ (when (seq pending)
             ; Don't bother emitting a stake action if one is still
             ; pending.
             (return nil))
         total-stake (->> view
                          (map :total-delegated-stake)
                          (reduce + 0))
         _ (when (= 0 total-stake) (return nil))
         ; Pick a target we want to make a heavyweight
         heavyweight (first (sort-by :address view))
         ; What's their fraction of the total?
         heavyweight-frac (/ (:total-delegated-stake heavyweight)
                             total-stake)
         ; How much do we need to add to tip them into supermajority territory?
         ;             supermaj = (heavy + x) / (total + x)
         ; supermaj (total + x) = heavy + x
         ; supermaj * total + supermaj * x = heavy + x
         ; supermaj * total - heavy = x - (supermaj * x)
         ; supermaj * total - heavy = x * (1 - supermaj)
         ; supermaj * total - heavy / (1 - supermaj) = x
         x (/ (- (* supermajority total-stake)
                 (:total-delegated-stake heavyweight))
              (- 1 supermajority))
         ; Give a liiiittle extra just to make sure--not sure if 2/3 exactly
         ; is enough or not.
         x (inc x)
         ;_ (info :heavyweight heavyweight :frac heavyweight-frac :has (:total-delegated-stake heavyweight) :needs x)
         ]
    {:type :info, :f :stake, :value {:validator (:address heavyweight)
                                     :amount x}}))

(defrecord Membership
  [clients    ; A map of nodes to RadixApi clients.
   node-views ; A map of nodes to that node's view of the cluster
   view       ; Merged view of operations
   pending    ; Pending [op op'] pairs.
   ]

  membership/State
  (setup! [this test]
    (assoc this :clients (->> (:nodes test)
                              (map (juxt identity rc/open))
                              (into {}))))

  (node-view [this test node]
    ;(info :fetching-view-for node)
    ;(info :view node (pprint-str (rc/validators (clients node))))
    (try+
      (let [validators (rc/validators (clients node))]
        ; Add node names to each validator
        (->> validators
             (mapv (fn [validator]
                     (assoc validator :node
                            (db/validator-address->node (:db test)
                                                        (:address validator)))))))
      (catch [:type :radix-dlt/failure, :code 1604] e
        ; Parse error
        nil)))

  (merge-views [this test]
    (->> node-views
         first
         val))

  (fs [this]
    #{:stake :unstake})

  (op [this test]
    (or (stake-op this) :pending))

  (invoke! [this test {:keys [f value] :as op}]
    (case f
      :stake
      (let [key-pair (rc/key-pair staker)
            client   (-> test :nodes rand-nth clients)
            txn'     (-> client
                         (rc/txn! key-pair
                                  "nemesis stake"
                                  [[:stake
                                    (rc/key-pair->account-address key-pair)
                                    (:validator value)
                                    (:amount value)]]))]
        ; Await txn
        (update op :value assoc
                :txn-id (:id txn')
                :status @(:status txn')))))

  (resolve [this test]
    this)

  (resolve-op [this test [op op']]
    nil)

  (teardown! [this test]
    ; No way to actually close clients, I think.
    ))

(defn membership-package
  "A membership nemesis package for Radix-DLT"
  [opts]
  (info :membership opts)
  (membership/package
    (assoc opts
           :membership {:state (map->Membership {})
                        :log-node-views? false
                        :log-view? true})))

(defn package
  "Given CLI options, constructs a package of {:generator, :final-generator,
  :nemesis, ..."
  [opts]
  (-> opts
      nc/nemesis-packages
      (conj (membership-package opts))
      nc/compose-packages))
