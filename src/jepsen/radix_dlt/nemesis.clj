(ns jepsen.radix-dlt.nemesis
  "Fault injection for Radix clusters"
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :as dt :refer [letr]]
            [jepsen [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as n]
                    [util :refer [pprint-str rand-nth-empty]]]
            [jepsen.nemesis [combined :as nc]
                            [membership :as membership]]
            [jepsen.radix-dlt [client :as rc]
                              [db :as rdb]]
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
  (->> (:validators view)
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
         total-stake (total-stake view)
         _ (when (= 0 total-stake) (return nil))
         ; Pick a target we want to make a heavyweight
         heavyweight (last (sort-by (juxt :total-delegated-stake
                                          :address)
                                    (:validators view)))
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

(defn active-nodes
  "Returns all nodes which are currently participating in the cluster."
  [{:keys [nodes free]}]
  (remove free nodes))

(defn remove-node-op
  "Takes a Membership state and returns an op (if possible) for removing a node
  from the cluster."
  [{:keys [view free] :as membership}]
  ; Pick a random node not in the free state
  (when-let [node (rand-nth-empty (active-nodes membership))]
    {:type :info, :f :remove-node, :value node}))

(defn add-node-op
  "Takes a Membership state and returns an op (if possible) for
  adding a node to the cluster."
  [{:keys [view free] :as membership}]
  (when-let [node (-> free vec rand-nth-empty)]
    {:type  :info
     :f     :add-node,
     :value {:add node
             :seed (rand-nth (active-nodes membership))}}))

(defn unregister-node-op
  "Takes a membership state and returns an operation for unregistering a node
  as a validator."
  [{:keys [validators view]}]
  ; TODO
  )

(defrecord Membership
  [clients    ; A map of nodes to RadixApi clients.
   node-views ; A map of nodes to that node's view of the cluster
   view       ; Merged view of cluster state
   pending    ; Pending [op op'] pairs.
   free       ; A set of nodes which are not participating in the cluster.
   validators ; A set of nodes we've instructed to become validators.
   ]

  membership/State
  (setup! [this test]
    (assoc this
           ; We're going to need the node set frequently
           :nodes      (:nodes test)
           ; Initially, every node is a validator.
           :validators (set (:nodes test))
           ; Keep a client for each node.
           :clients (->> (:nodes test)
                         (map (juxt identity (partial rc/open test)))
                         (into {}))))

  (node-view [this test node]
    ;(info :fetching-view-for node)
    ;(info :view node (pprint-str (rc/validators (clients node))))
    (try+
      (let [validators (rc/validators (clients node))]
        ; Add node names to each validator
        {:validators (->> validators
                          (mapv (fn [validator]
                                  (assoc validator :node
                                         (rdb/validator-address->node (:db test)
                                                                     (:address validator))))))
         ;:validation-info (rdb/validation-node-info node)
         })
      (catch [:type :radix-dlt/failure, :code 1604] e
        ; Parse error
        nil)))

  (merge-views [this test]
    ; We take each node's own view of its validation node info, and make a map
    ; of nodes to those structures.
    {:validation-info (reduce (fn [vi [node view]]
                                (assoc vi node (:validation-info view)))
                              {}
                              node-views)
     ; And for the validators, we combine all views and pick any value for each
     ; distinct node. No way to get a causal timestamp here, far as I know:
     ; we're just gonna be wrong sometimes.
     :validators (->> (vals node-views)
                      (mapcat :validators)
                      (group-by :node)
                      vals
                      (mapv first))})

  (fs [this]
    #{:stake
      :unstake
      :add-node
      :remove-node})

  (op [this test]
    (->> [(stake-op this)
          (add-node-op this)
          (remove-node-op this)
          :pending]
         (remove nil?)
         vec
         rand-nth))

  (invoke! [this test {:keys [f value] :as op}]
    (case f
      :stake
      (let [key-pair (rc/key-pair staker)
            client   (-> test :nodes rand-nth clients)
            txn'     (-> client
                         (rc/txn! key-pair
                                  "nemesis stake"
                                  [[:stake
                                    (rc/->account-address key-pair)
                                    (:validator value)
                                    (:amount value)]]))]
        ; Await txn
        (update op :value assoc
                :txn-id (:id txn')
                :status @(:status txn')))

      ; We're doing something simple and maybe unsafe (?): just killing and
      ; wiping the node.
      :remove-node
      (do (c/on-nodes test [value]
                      (fn [test node]
                        (db/kill! (:db test) test node)
                        (rdb/wipe!)))
          (assoc op :value [:removed value]))))

  (resolve [this test]
    this)

  (resolve-op [this test [op op']]
    ;(info :resolve-op :op op :op' op')
    (case (:f op)
      ; We assume removes take place immediately.
      :remove-node
      (if (= :removed (first (:value op')))
        ; Record that this node is now free
        (update this :free conj (:value op))
        (throw+ {:type :unexpected-remove-node-value
                 :op op'}))

      nil))

  (teardown! [this test]
    ; No way to actually close clients, I think.
    ))

(defn membership-package
  "A membership nemesis package for Radix-DLT"
  [opts]
  (info :membership opts)
  (membership/package
    (assoc opts
           :membership {:state (map->Membership
                                 {:free #{}
                                  })
                        :log-node-views? false
                        :log-resolve? true
                        :log-view? true})))

(defn rollback-package
  "Nodes in Radix basically aren't supposed to fail: when they do, a certain
  fraction of transactions will time out until the next leader can take over
  for each round. To that end, they suggest that when you need to perform
  routine maintenance, you have a non-validator standby node running, then copy
  the validator's key file over to that node, stop the old node, and restart
  the new one."
  []
  ; TODO: need capability to change nodes between validators and non-validators
  )

(defn package
  "Given CLI options, constructs a package of {:generator, :final-generator,
  :nemesis, ..."
  [opts]
  (-> opts
      nc/nemesis-packages
      (conj (membership-package opts))
      nc/compose-packages))
