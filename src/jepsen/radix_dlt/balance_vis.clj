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
  (:require [clojure.string :as str]
            [clj-time.coerce :as t-coerce]
            [hiccup.core :as h]
            [knossos.history :as history]
            [jepsen [store :as store]
                    [util :as util :refer [name+ pprint-str]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.radix-dlt.checker.util :refer [known-balances
                                                   op-accounts]]))

(def stylesheet nil)

(def time-height
  "How high, in em, is a timeslice?"
  2)

(def balance-width
  "How wide, in em, is a balance track?"
  4)

(def stylesheet
  (str ".ops { position: absolute; }\n"
       ".op  { position: absolute; padding: 2px; border-radius: 2px; box-shadow: 0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24); overflow: hidden; }\n"
       ".op.balance { background: #A5F7B7 }"))

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
    (assert (= 1 (count bs)))
    (->> bs first val
         ; Balances generally fall thanks to fees, so we reverse these
         reverse
         (map-indexed (fn [i balance] [balance i]))
         (into {}))))

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
    :balance-index    A map of balances to column indices

  and returns a sequence of boxes, one for each balance read."
  [{:keys [balance-index history]}]
  (->> history
       (filter (comp #{:balance} :f))
       history/pairs+
       (keep (fn [[invoke complete]]
               (let [{:keys [account balance]} (:value complete)]
                 (if (= :ok (:type complete))
                   [:div {:class "op balance"
                          :style (timeline/style
                                   (assoc (partial-coords invoke complete)
                                          :left (-> balance-index
                                                    (get balance)
                                                    (* balance-width)
                                                    (str "em"))))
                          :title (str "Balance read: " balance "\n\n"
                                      (timeline/render-op complete))}
                    "b " balance]))))))

(defn render-account!
  "Writes out an account's HTML file."
  [test history account]
  (let [history  (account-history account history)
        analysis {:history       history
                  :balance-index (balance-index history)}]
    (->> (h/html [:html
                  [:head
                   [:style stylesheet]]]
                 [:body
                  [:h1 (str "Account " account)]
                  [:div {:class "ops"}
                   (render-balances analysis)]])
         (spit (store/path! test "balances" (str account ".html"))))))
