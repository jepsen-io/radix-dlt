(ns jepsen.radix-dlt.checker
  "Analyzes the correctness of Radix-DLT histories."
  (:require [clojure.tools.logging :refer [info warn]]
            [elle [core :as elle]
                  [graph :as g]]
            [jepsen [checker :as checker]
                    [util :as util :refer [map-vals
                                           parse-long
                                           pprint-str]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.radix-dlt [balance-vis :as balance-vis]
                              [util :as u]]
            [jepsen.radix-dlt.checker.util :refer [account->balance->txn-ids
                                                   all-accounts
                                                   longest-txn-logs
                                                   mop-accounts
                                                   op-accounts
                                                   txn-id
                                                   txn-op-accounts]]
            [jepsen.tests.cycle.append :as append]))

(defn unseen-txn-ids
  "Takes a Radix history and computes a map of accounts to collections of
  unseen transaction IDs---those which did not appear in the longest txn
  history."
  [history]
  (let [seen (->> (longest-txn-logs history)
                  (map-vals (comp set :id :txns)))]
    ; Find all the txns that *could* have taken effect
    (->> history
         (filter (comp #{:txn} :f))
         (filter (comp #{:ok :info} :type))
         ; Take these transactions and build a map of accounts to possible txn
         ; ids, only if they're not in the longest sets.
         (reduce (fn [m op]
                   (let [id (:id (:value op))]
                     (reduce (fn [m acct]
                               (let [seen (get seen acct (sorted-set))]
                                 (if (seen id)
                                   m
                                   ; Aha, an unseen txn!
                                   (assoc m acct (conj (get m acct []) id)))))
                             m
                             (txn-op-accounts op))))
                 {}))))

(defn list-append-history
  "Takes a Radix-DLT history and rewrites it to look like an Elle list-append
  history.

  :txn operations are rewritten to a transaction which appends the transaction
  ID to each involved account.

  :txn-log operations are rewritten to a transaction performing a single read
  of that account as if it were a list of txn ids (derived from the transaction
  messages)

  :balance operations are (and this is such a hack) also mapped to reads of
  lists of txn ids: we play forward what we *think* the sequence of txns was,
  and use that to construct a mapping of balances to sequences of txns."
  [history]
  (let [account->balance->txn-ids (account->balance->txn-ids history)]
    ;(info :account->balance->txn-ids
    ;      (pprint-str account->balance->txn-ids))
    (mapv (fn [{:keys [type f value] :as op}]
            (case f
              :txn
              (let [id  (:id value)
                    ; Generate synthetic transaction writing to all involved
                    ; accounts
                    ;_ (when (= id 9)
                    ;    (info "Generating txn for" id ":" op "\n"
                    ;          (->> (txn-op-accounts op)
                    ;               (mapv (fn [acct] [:append acct id])))))
                    txn (->> (txn-op-accounts op)
                             (mapv (fn [acct] [:append acct id])))]
                (assoc op :value txn))

              :txn-log
              (let [k   (:account value)
                    ; Find all txids
                    ids (when (= type :ok)
                          (->> (:txns value)
                               (keep txn-id)
                               vec))]
                (assoc op :value [[:r k ids]]))

              :balance
              (if (= type :ok)
                (let [{:keys [account balance]} value
                      txn-ids (get-in account->balance->txn-ids
                                      [account balance])]
                  (case txn-ids
                    ; We info this read, since we don't know WHERE this came
                    ; from
                    nil
                    (assoc op
                           :type    :info
                           :error   :unknown-balance
                           :balance (:balance value)
                           :value   [[:r account nil]])

                    :multiple
                    ; We info this read, since it can't be interpreted
                    ; distinctly
                    (assoc op
                           :type    :info
                           :error   :ambiguous-balance
                           :balance (:balance value)
                           :value   [[:r account nil]])

                    ; Great, we know which txns must have produced this
                    (assoc op :type :ok, :value [[:r account txn-ids]])))

                ; For invocations, infos, fails, we just turn this into a nil
                ; read.
                (assoc op :value [[:r (:account value) nil]]))))
          history)))

(defn write?
  "Is this a write operation?"
  [op]
  (->> op
       :value
       (map first)
       (every? #{:append})))

(defrecord RealtimeNodeExplainer [realtime-graph node-graph pairs]
  elle/DataExplainer
  (explain-pair-data [_ a' b']
    ; Look up invocation
    (let [b (get pairs b')]
      (cond (when-let [out (g/out node-graph a')]
              (.contains out b'))
            {:type :process
             :a'   a'
             :b    b}

            (when-let [out (g/out realtime-graph a')]
              (.contains out b'))
            {:type :realtime
             :a'   a'
             :b    b}

            :else nil)))

  (render-explanation [_ {:keys [type a' b]} a'-name b'-name]
    (let [a-type (if (write? a') "write" "read")
          b-type (if (write? b)  "write" "read")]
      (str a-type " " a'-name " completed at index " (:index a') ","
           (when (and (:time a') (:time b))
             (let [dt (- (:time b) (:time a'))]
               ; Times are approximate--what matters is the serialization
               ; order. If we observe a zero or negative delta, just fudge it.
               (if (pos? dt)
                 (str (format " %.3f" (util/nanos->secs (- (:time b) (:time a'))))
                      " seconds")
                 ; Times are inaccurate
                 " just")))
           " before the invocation of " b-type " " b'-name
           ", at index " (:index b)
           (when (= :process type)
             ", on the same node")))))

(def elle-realtime-graph
  "We're going to redefine this dynamically, so we squirrel away a copy for
  ourselves."
  elle/realtime-graph)

(defn realtime-graph
  "Radix DLT's realtime properties only hold for writes (Radix transactions),
  not reads (requests for transaction history or balances). To check this, we
  restrict the list-append history to write transactions only, then ask Elle to
  compute a realtime precedence graph over that."
  [history]
  (->> history
       ; Find write-only transactions
       (filter write?)
       ; And compute a precedence graph over those
       elle-realtime-graph))

(defn realtime->process-edges
  "Takes a graph map (with a :graph inside) and rewrites that graph to have
  :process edges, rather than :realtime."
  [graph]
  (update graph :graph (partial g/keep-edge-values (fn [_] #{:process}))))

(defn node-graphs
  "Radix DLT nodes are supposed to behave sequentially. We partition the
  history by node (taking advantage of the fact that `(process % node-count) =
  node-index`, compute realtime graphs over those, then merge them together. We
  *call* this a process order, because that way we can bring our existing
  anomaly detectors to bear, and it's sort of like treating the nodes, rather
  than clients, as if they were processses."
  [test history]
  (let [node-count (count (:nodes test))]
    (->> history
         ; Partition by node
         (group-by (fn group [{:keys [process]}]
                     (mod process node-count)))
         vals
         ; Compute a realtime graph for each node separately
         (map elle-realtime-graph)
         ; And call those edges :process
         (map realtime->process-edges))))

(defn realtime+node-graph
  "Computes a realtime and node (:process) graph for a list append history."
  [test history]
  (let [realtime        (realtime-graph history)
        realtime-graph  (:graph realtime)
        nodes           (node-graphs test history)
        ; Merge all their pair indices together
        pairs           (->> nodes
                             (cons realtime)
                             (map (comp :pairs :explainer))
                             (reduce merge))
        ; Merge the per-node graphs together
        node-graph      (->> nodes
                             (map :graph)
                             (reduce g/digraph-union))]
    ; Construct a unified graph and explainer. Our :graph is the combined graph
    ; of both realtime and node (:process) edges, which Elle will use to find
    ; cycles.
    {:graph     (g/digraph-union realtime-graph node-graph)
     ; And our custom explainer, which knows about both types of edges
     :explainer (RealtimeNodeExplainer. realtime-graph node-graph pairs)}))

(defn sample
  "Like take, but returns n items max, taken from the start *and* end, with
  middle items elided by '..."
  [n coll]
  (if (<= (count coll) n)
    coll
    (concat (take (Math/ceil (/ n 2)) coll)
            ['...]
            (take-last (Math/floor (/ n 2)) coll))))

(defn list-append-checker
  "We'd like to ensure that our transactions are serializable, (and ideally,
  that writes are strict-serializable). Elle has extension points for writing
  arbitrary transaction semantics, but doing the full range of anomalies is
  complex and would be time-consuming. Instead, we're going to cheat: instead
  of teaching Elle about the semantics of transfer transactions, we can
  interpret accounts as objects which are lists of transaction IDs, and recast
  each transaction as a *collection* of writes which append that transaction's
  ID to every involved account.

  This list-append interpretation is something Elle already knows how to check,
  and finds a broad array of anomalies with relatively little effort. For
  example:

  1. If a read observes a transaction which the client believes failed, Elle
     can inform us of an aborted read.
  2. If a transaction appears multiple times, we can detect the duplicate write
  3. If account's orders of transactions are inconsistent with one another, we
     can detect that as well

  The realtime/node semantics are a little trickier to encode. Radix's writes
  should be strict serializable, but Radix has no concept of a realtime-strict
  read: if we used Elle's realtime orders, we'd build improper graphs. What we
  want to do instead is use the *process* order (since processes are restricted
  to single nodes), and in addition, build a *partial* realtime graph
  restricted to only write transactions."
  []
  (let [elle (append/checker {;:max-plot-bytes     1000000
                              :cycle-search-timeout 100000
                              :consistency-models [:strict-serializable]})]
    (reify checker/Checker
      (check [this test history opts]
        (let [history    (subvec history 0 500)
              la-history (list-append-history history)]
          ;(info :history (pprint-str la-history))
          ; Oh this is such a gross hack. There's all this *really* nice,
          ; sophisticated machinery for detecting various anomalies in
          ; elle.txn, but it all assumes the builtin realtime/process graphs.
          ; Instead of redefining all the anomaly types for our own weird
          ; temporal model, we're going to sneak in there and redefine how Elle
          ; computes its realtime graph.
          (let [res (with-redefs [elle.core/realtime-graph
                                  (partial realtime+node-graph test)]
                      (checker/check elle test la-history opts))
                ; As an extra piece of debugging info, we're going to record
                ; how many reads went recognized/unrecognized.
                balances           (filter (comp #{:balance} :f) la-history)
                ok-balances        (filter (comp #{:ok} :type) balances)
                info-balances      (filter (comp #{:info} :type) balances)
                unknown-balances   (filter (comp #{:unknown-balance} :error)
                                           info-balances)
                ambiguous-balances (filter (comp #{:ambiguous-balance} :error)
                                           info-balances)]

            ; Render balance visualizations
            (doseq [account (all-accounts history)]
              (balance-vis/render-account! test history account))

            (assoc res
                   :ok-balance-count        (count ok-balances)
                   :unknown-balance-count   (count unknown-balances)
                   :ambiguous-balance-count (count ambiguous-balances)
                   :unknown-balances        (sample 6 unknown-balances)
                   :ambiguous-balances      (sample 6 ambiguous-balances)
                   :unseen-txn-ids          (unseen-txn-ids history))))))))

(defn error-types
  "A small checker which sums up the different kinds of errors we encounter"
  []
  (reify checker/Checker
    (check [this test history opts]
      (->> history
           (keep :error)
           (group-by (fn [e]
                       ; Some errors are bare keywords, others are [:something,
                       ; details]
                       (if (keyword? e)
                         e
                         (first e))))
           (map-vals count)
           (merge {:valid? true})))))

(defn checker
  "Unified checker."
  []
  (checker/compose
    {:timeline    (timeline/html)
     :errors      (error-types)
     :list-append (list-append-checker)}))
