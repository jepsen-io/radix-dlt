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
                    [util :as util :refer [name+ pprint-str nanos->secs]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.radix-dlt.checker.util :refer [balance->txn-id-prefix
                                                   txn-id
                                                   op-accounts]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def stylesheet nil)

(def time-height
  "How high, in em, is a timeslice?"
  2)

(def balance-width
  "How wide, in em, is a balance track?"
  2)

(def balance-stylesheet
  (str ".ops { position: absolute; width: 100%; }\n"
       ".op  { position: absolute; padding: 2px; border-radius: 2px; box-shadow: 0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24); overflow: hidden; z-index: 2 }\n"
       ".op.balance { background: #A5F7B7 }\n"
       ".op.balance.unresolvable { background: #F7B7A5; }\n"
       ".op.txn { background: #A5DEF7 }\n"
       ".op.txn.unseen { background: #D0D0D0; z-index: 1 }\n"
       ))

(defn balance-index
  "Takes an account analysis and returns a map of balances to column indices"
  [account-analysis]
  (->> (:known-balances account-analysis)
       ; The nil balance always appears in column 0
       (cons nil)
       (map-indexed (fn [i balance] [balance i]))
       (into {})))

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
  [{:keys [account balance-index txn-log history] :as analysis}]
  ;(pprint :txn-log)
  ;(pprint txn-log)
  (->> history
       (filter (comp #{:balance} :f))
       history/pairs+
       (keep (fn [[invoke complete]]
               (let [{:keys [account balance]} (:value complete)]
                 (if (= :ok (:type complete))
                   (let [txn-ids (balance->txn-id-prefix
                                   {:accounts {account analysis}}
                                   account
                                   balance)
                         txn-id (if (vector? txn-ids)
                                  (peek txn-ids)
                                  txn-ids)]
                     [:div {:class (str "op balance"
                                        (when (keyword txn-id)
                                          (str " " (name txn-id))))
                            :style (timeline/style
                                     (assoc (partial-coords invoke complete)
                                            :left (-> balance-index
                                                      (get balance)
                                                      (* balance-width)
                                                      (str "em"))))
                            :title (str "Balance read: " (pr-str balance)
                                        " from txn " (pr-str txn-id)
                                        "\n\n"
                                        (timeline/render-op complete))}
                      "b " balance])))))))

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

(def txn-logs-stylesheet
  "CSS styles for the txn-logs visualization"
  "")

(defn rand-bg-color
  "Hashes anything and assigns a lightish hex color to it--helpful for
  highlighting different values differently."
  [x]
  (let [h (hash x)
        ; Break up hash into 3 8-bit chunks
        r (bit-and 255 h)
        g (-> h (bit-and 65280)    (bit-shift-right 8))
        b (-> h (bit-and 16711680) (bit-shift-right 16))
        ; Squeeze these values into the range 130-250, so they're not too dark
        ; to read, but not pure white (which would disappear).
        ceil     250
        floor    130
        range    (- ceil floor)
        compress (fn compress [x]
                   (-> x (/ 255) (* range) (+ floor) short))
        r (compress r)
        g (compress g)
        b (compress b)]
    (str "#"
         (format "%02x" r)
         (format "%02x" g)
         (format "%02x" b))))

(defn render-account-txn-logs!
  "Writes out an HTML table showing all the different txn-log ops for a given
  account, so we can understand where and how they diverged."
  [{:keys [test account txn-log history]}]
  (let [nodes   (:nodes test)
        ; The txn IDs from the longest txn log:
        log     (->> txn-log :txns (mapv :id))]
    (->> (h/html
           [:html
            [:head
             [:style txn-logs-stylesheet]]
            [:body
             [:h1 (str "Account " account " transaction logs")]
             [:table
              [:thead
               [:tr
                [:th "Time (s)"]
                [:th "Node"]
                [:th {:colspan 32} "Txns"]]]
              [:tbody
               (for [{:keys [time process f type value]} history
                     :when (and (= f :txn-log)
                                (= type :ok)
                                (= account (:account value)))]
                 [:tr
                  (concat [[:td (format "%.2f" (nanos->secs time))]
                           [:td (nth nodes (mod process (count nodes)))]]
                          (->> (:txns value)
                               (map-indexed vector)
                               (keep (fn [[i txn]]
                                       (when-let [id (txn-id txn)]
                                         ; Is this compatible with the longest
                                         ; log?
                                         (let [compat? (= id (nth log i))
                                               style (cond-> {}
                                                       (not compat?)
                                                       (assoc :background
                                                              (rand-bg-color id))
                                                       true
                                                       timeline/style)]
                                           [:td {:style style} id]))))))])]]]])
         (spit (store/path! test "accounts" (str account "-txn-logs.html"))))))

(defn render-account!
  "Writes out an account's HTML files."
  [test analysis account]
  (let [analysis (get-in analysis [:accounts account])
        history  (:history analysis)
        balance-index (balance-index analysis)
        analysis (assoc analysis
                        :test          test
                        :balance-index balance-index)]
    (render-account-balances! analysis)
    (render-account-txn-log!  analysis)
    (render-account-timeline! analysis)
    (render-account-txn-logs! analysis)))
