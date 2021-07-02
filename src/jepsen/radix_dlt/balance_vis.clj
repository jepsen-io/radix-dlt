(ns jepsen.radix-dlt.balance-vis
  "This namespace renders HTML diagrams of the balance of a signle account over
  time, like so:

           balance ---->

    time   █
           b
      |    █ █
      |      b
      V      █
               █
           b   █
               █

  Time flows top to bottom, and balances are arranged left to right. Both are
  logically quantized: each distinct balance has an equally-sized range,
  because the values involved here are sparse and distinguished by vastly
  different scales."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [clj-time.coerce :as t-coerce]
            [hiccup.core :as h]
            [knossos.history :as history]
            [jepsen [store :as store]
                    [util :as util :refer [name+ pprint-str]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.radix-dlt.checker.util :refer [known-balances
                                                   txn-log->balance->txn-ids
                                                   txn-id
                                                   txn-logs
                                                   op-accounts]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def stylesheet nil)

(def time-height
  "How high, in em, is a timeslice?"
  1)

(def balance-width
  "How wide, in em, is a balance track?"
  1)

(def balance-stylesheet
  (str ".ops { position: absolute; width: 100%; }\n"
       ".op  { position: absolute; padding: 2px; border-radius: 2px; box-shadow: 0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24); overflow: hidden; z-index: 2 }\n"
       ".op.balance { background: #A5F7B7 }\n"
       ".op.balance.unresolved { background: #F7B7A5; }\n"
       ".op.txn { background: #A5DEF7 }\n"
       ".op.txn.unseen { background: #D0D0D0; z-index: 1 }\n"
       ))

(defn account-history
  "Restricts a history to a single account, and assigns each operation a
  sequential :sub-index."
  [account history]
  (->> history
       (filter (fn of-interest? [op] (contains? (op-accounts op) account)))
       (map-indexed (fn sub-index [i op] (assoc op :sub-index i)))
       vec))

(defn balance-index
  "Takes a single account history, and returns a map of balances to
  column indices"
  [history]
  (let [bs (known-balances history)]
    (condp = (count bs)
      0 {}
      1 (->> bs first val
             ; Balances generally fall thanks to fees, so we reverse these
             reverse
             (map-indexed (fn [i balance] [balance i]))
             (into {}))
      (throw+ {:type :extra-balances!? :balances bs}))))

(defn partial-coords
  "Takes an invocation and a completion, and constructs a map of

      {:top
       :width
       :height}

  The :left coordinate is balance-specific, and is computed elsewhere."
  [invoke complete]
  {:top     (str (* time-height (:sub-index invoke)) "em")
   :width   (str balance-width "em")
   :height  (str (* time-height (- (:sub-index complete)
                                   (:sub-index invoke))) "em")})

(defn render-balances
  "Takes a map with:

    :history          A single-account history
    :txn-log          An augmented transaction log for this account
    :balance-index    A map of balances to column indices

  and returns a sequence of boxes, one for each balance read."
  [{:keys [balance-index txn-log history]}]
  (pprint :txn-log)
  (pprint txn-log)
  (let [balance->txn-ids (txn-log->balance->txn-ids txn-log)]
    (->> history
         (filter (comp #{:balance} :f))
         history/pairs+
         (keep (fn [[invoke complete]]
                 (let [{:keys [account balance]} (:value complete)]
                   (if (= :ok (:type complete))
                     (let [txn-ids (if (nil? balance)
                                     :init
                                     (balance->txn-ids balance))
                           txn-id (if (vector? txn-ids)
                                    (peek txn-ids)
                                    txn-ids)]
                       [:div {:class (str "op balance"
                                          (when (nil? txn-id)
                                            " unresolved"))
                              :style (timeline/style
                                       (assoc (partial-coords invoke complete)
                                              :left (-> balance-index
                                                        (get balance)
                                                        (* balance-width)
                                                        (str "em"))))
                              :title (str "Balance read: " balance
                                          " from txn " (pr-str txn-id)
                                          "\n\n"
                                          (timeline/render-op complete))}
                        "b " balance]))))))))

(defn render-txns
  "Takes a map with:

    :history          A single-account history
    :txn-log          An augmented txn log for this account
    :balance-index    A map of balances to column indices

  And returns a sequence of boxes, one for each txn operation."
  [{:keys [balance-index history txn-log]}]
  (->> history
       (filter (comp #{:txn} :f))
       history/pairs+
       (keep (fn [[invoke complete]]
               (let [id (:id (:value invoke))]
                 ; Do we have an entry in the txn log?
                 (if-let [log-txn (get-in txn-log [:by-id id])]
                   (let [balance' (:balance' log-txn)
                         left (-> balance-index
                                  (get balance')
                                  (* balance-width)
                                  (str "em"))]
                     [:div {:class "op txn"
                            :style (timeline/style
                                     (assoc (partial-coords invoke complete)
                                            :left left))
                            :title (str "Txn: " (:balance log-txn) " -> "
                                        balance' "\n\n"
                                        (timeline/render-op complete))}
                      (str "t" id)])
                   ; We don't have an entry in the txn log for this. It might
                   ; have been serialized after our final read, or it might
                   ; have failed, or it might be ~illegal~; who knows. We'll
                   ; render it as a full-width bar if it's ok/info.
                   (when (not= :fail (:type complete))
                     [:div {:class "op txn unseen"
                            :style (timeline/style
                                     (-> (partial-coords invoke complete)
                                         (assoc :left 0
                                                :width "100%"
                                                :right 0)))
                            :title (str "Unseen txn:\n"
                                        (timeline/render-op complete))}
                      (str "t" id)
                      ])))))))

(defn render-account-balances!
  "Writes out an HTML file for an account's analysis, plotting operations on a
  time x balance space."
  [analysis]
  (let [{:keys [account test]} analysis]
    (->> (h/html [:html
                  [:head
                   [:style balance-stylesheet]]]
                 [:body
                  [:h1 (str "Account " account " balances")]
                  [:div {:class "ops"}
                   (render-balances analysis)
                   (render-txns     analysis)]])
         (spit (store/path! test "accounts" (str account "-balance.html"))))))

(def txn-log-stylesheet
  "CSS styles for the transaction log."
  "")

(defn render-account-txn-log!
  "Writes out an HTML file for an account's longest transaction log."
  [analysis]
  (let [{:keys [test account txn-log]} analysis]
    (->> (h/html [:html
                  [:head
                   [:style txn-log-stylesheet]]
                  [:body
                   [:h1 (str "Account " account " transaction log")]
                   [:div {:class "ops"}
                    [:pre
                     (->> txn-log
                          :txns
                          (map (fn [txn]
                                 (update txn :actions
                                         (partial map (fn [op]
                                                        (dissoc op
                                                                :rri
                                                                :validator))))))
                          util/pprint-str)]]]])
         (spit (store/path! test "accounts" (str account "-txn-log.html"))))))

(def txn-stylesheet
  "CSS for the transaction view."
  "")

(defn render-account-timeline!
  "Writes out an HTML file that represents all ops involving that
  account."
  [{:keys [test account history]}]
  (let [pairs         (history/pairs+ history)
        pair-count    (count pairs)
        process-index (timeline/process-index history)]
  (->> (h/html
         [:html
          [:head
           [:style timeline/stylesheet]]
          [:body
           [:h1 (str "Account " account " transactions")]
           [:div {:class "ops"}
            (map (partial timeline/pair->div history test process-index)
                 pairs)]]])
       (spit (store/path! test "accounts" (str account "-timeline.html"))))))

(defn render-account!
  "Writes out an account's HTML files."
  [test history account]
  (let [history  (account-history account history)
        txn-log  (-> history txn-logs (get account {:account account}))
        analysis {:test          test
                  :account       account
                  :history       history
                  :txn-log       txn-log
                  :balance-index (balance-index history)}]
    (render-account-balances! analysis)
    (render-account-txn-log!  analysis)
    (render-account-timeline! analysis)))
