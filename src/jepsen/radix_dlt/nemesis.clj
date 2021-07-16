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
                              [db :as db]]))

(def staker
  "Which account do we use to restake funds?"
  2)

(def supermajority
  "How much stake do you need to dominate consensus?"
  2/3)

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
    (vec (rc/validators (clients node))))

  (merge-views [this test]
    (->> node-views
         first
         val))

  (fs [this]
    #{:stake :unstake})

  (op [this test]
    ; Look at the distribution of stake
    (letr [_ (when (seq pending)
               ; Don't bother emitting a stake action if one is still
               ; pending.
               (return :pending))
           total-stake (->> view
                           (map :total-delegated-stake)
                           (reduce + 0))
           _ (when (= 0 total-stake) (return :pending))
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
