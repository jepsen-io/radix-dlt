(ns jepsen.radix-dlt.checker
  "Analyzes the correctness of Radix-DLT histories."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [elle [core :as elle]
                  [graph :as g]
                  [list-append :as list-append]]
            [jepsen [checker :as checker]
                    [util :as util :refer [map-vals
                                           parse-long
                                           pprint-str]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.radix-dlt [balance-vis :as balance-vis]
                              [util :as u]]
            [jepsen.radix-dlt.checker.util :refer [analysis
                                                   balance->txn-id-prefix
                                                   rewrite-info-txns
                                                   txn-id
                                                   txn-op-accounts]]
            [jepsen.tests.cycle.append :as append]
            [knossos [op :as op]]))

(def elle-lock
  "We're going to pull some thread-unsafe with-redefs weirdness when using
  Elle--we need a lock to protect it."
  (Object.))

(defn list-append-history
  "Takes an analysis and rewrites its history to look like an Elle list-append
  history.

  :txn operations are rewritten to a transaction which appends the transaction
  ID to each involved account.

  :txn-log operations are rewritten to a transaction performing a single read
  of that account as if it were a list of txn ids (derived from the transaction
  messages)

  :balance operations are (and this is such a hack) also mapped to reads of
  lists of txn ids: we play forward what we *think* the sequence of txns was,
  and use that to construct a mapping of balances to sequences of txns."
  [{:keys [accounts history pair-index] :as analysis}]
  ;(info :account->balance->txn-ids
  ;      (pprint-str account->balance->txn-ids))
  (mapv (fn xform-op [{:keys [type f value] :as op}]
          (case f
            :txn
            (let [id  (:id value)
                  ; Generate synthetic transaction writing to all involved
                  ; accounts
                  txn (->> (txn-op-accounts op)
                           (mapv (fn [acct] [:append acct id])))]
              (assoc op :value txn))

            ; For raw txn logs, we rewrite them to a single transaction reading
            ; every extant key, by projecting the log to just those txns which
            ; involved that key. This isn't really a *true* read of keys, but
            ; it still gives us txn ordering information, and that's critical
            ; to the rest of our balance inference process.
            :raw-txn-log
            (->> (keys accounts)
                 (map (fn raw-txn-log->txn [acct]
                        (let [txn-ids (-> accounts (get acct) :txn-ids)
                              ; Filter the log to only involved txn IDs
                              sublog  (->> value
                                           (filter txn-ids)
                                           vec)]
                          [:r acct sublog])))
                 (assoc op :value))

            :txn-log
            (let [k   (:account value)
                  ; Find all txids
                  ids (when (= type :ok)
                        (->> (:txns value)
                             (keep txn-id)
                             vec))]
              (assoc op :value [[:r k ids]]))

            :balance
            (if (not= type :ok)
              ; For invocations, infos, fails, we just turn this into a nil
              ; read.
              (assoc op :value [[:r (:account value) nil]])

              ; For ok balances, try to resolve what string of txns they saw
              (let [{:keys [account balance]} value
                    ids (balance->txn-id-prefix analysis account balance)]
                (case ids
                  :unresolvable
                  (assoc op
                         :type    :info
                         :error   :unresolvable-balance
                         :balance (:balance value)
                         :value   [[:r account nil]])

                  :ambiguous
                  (assoc op
                         :type    :info
                         :error   :ambiguous-balance
                         :balance (:balance value)
                         :value   [[:r account nil]])

                  (assoc op :type :ok, :value [[:r account ids]]))))

            ; Raw balance reads are translated to a read of every account
            ; *ever*.
            :raw-balances
            (let [; We need to look forward to figure out which accounts we can
                  ; even appear to *try* to read.
                  complete (if (= :invoke type)
                             (get pair-index op)
                             op)
                  ; What *will* we read later?
                  value'   (:value complete)
                  ; In general, Radix is so flaky about losing transactions
                  ; from the log that pretty much every balances read is going
                  ; to have some balances we can't explain. To allow the
                  ; checker to tell us at least *some* things, we project
                  ; a raw-balances read to just those accounts which were
                  ; uniquely resolvable, and construct a transaction which
                  ; reads those. Unresolvable and ambiguous balances we just
                  ; skip over.
                  txn (->> (keys accounts)
                           (keep (fn raw-balances->txn [acct]
                                   (let [balance (get value' acct)
                                         ids     (balance->txn-id-prefix
                                                   analysis acct balance)]
                                     (case ids
                                       :unresolvable nil
                                       :ambiguous    nil
                                       [:r acct (if (= :invoke type)
                                                  nil
                                                  ids)]))))
                           vec)]
              (assoc op :value txn))
            ))
        (filter (comp integer? :process) history)))

(defn write?
  "Is this a write operation?"
  [op]
  (->> op
       :value
       (map first)
       (some #{:append})))

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
  than clients, as if they were processses.

  Note that when transactions are submitted (via :f :txn) to one node, and
  confirmed (via :f :check-txn) on another, we use the check-txn process
  (recorded in the :txn op's :checked-by field) to determine the node."
  [test history]
  (let [node-count (count (:nodes test))]
    (->> history
         ; Partition by node
         (group-by (fn group [{:keys [process checked-by] :as op}]
                     ;(when checked-by
                       ;(info "using checked-by " checked-by " instead of " process " for txn " (pr-str op)))
                     (mod (or checked-by process) node-count)))
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

(defn realtime+node-check
  "Runs an Elle checker, but with the realtime graph redefined to only hold
  between operations on single nodes, rather than across all operations."
  [elle-checker test history opts]
  (locking elle-lock
    (with-redefs [elle.core/realtime-graph
                  (partial realtime+node-graph test)]
      (checker/check elle-checker test history opts))))

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
      (check [this test history {:keys [analysis] :as opts}]
        (let [la-history (list-append-history analysis)
              ;_          (pprint la-history)
              res        (realtime+node-check elle test la-history opts)
              ; As an extra piece of debugging info, we're going to record
              ; how many reads went recognized/unrecognized.
              balances           (filter (comp #{:balance} :f) la-history)
              ok-balances        (filter (comp #{:ok} :type) balances)
              info-balances      (filter (comp #{:info} :type) balances)
              unresolvable-balances (filter (comp #{:unresolvable-balance} :error)
                                            info-balances)
              ambiguous-balances (filter (comp #{:ambiguous-balance} :error)
                                         info-balances)]

          (assoc res
                 :ok-balance-count           (count ok-balances)
                 :unresolvable-balance-count (count unresolvable-balances)
                 :ambiguous-balance-count    (count ambiguous-balances)
                 :unresolvable-balances      (sample 6 unresolvable-balances)
                 :ambiguous-balances         (sample 6 ambiguous-balances)))))))

(defn raw-txn-history
  "Takes an analysis and rewrites its :txn and :raw-txn-log operations to look
  like transactions operating on a single global list."
  [{:keys [history] :as analysis}]
  (->> history
       (filter (comp #{:txn :raw-txn-log} :f))
       (mapv (fn [{:keys [type f value] :as op}]
               (case f
                 ; For transactions, we rewrite them as a single append of the
                 ; txn ID to a global key :log.
                 :txn (if-let [id (:id value)]
                        (assoc op :value [[:append :log (:id value)]])
                        (assoc op :value []))

                 ; And for raw-txn-log ops, we interpret them as reads.
                 :raw-txn-log
                 (assoc op :value [[:r :log (when value (vec value))]]))))))

(defn raw-txn-checker
  "The raw txn checker treats the raw txn log as a single strict-serializable
  list, and every txn as an append to it."
  []
  (reify checker/Checker
    (check [this test history {:keys [analysis] :as opts}]
      (let [history (raw-txn-history analysis)
            ;_       (pprint history)
            elle    (append/checker
                      {:consistency-models [:strict-serializable]})
            opts    (assoc opts :subdirectory "raw-txn")
            res     (realtime+node-check elle test history opts)]
        res))))

(defn inexplicable-balance-checker
  "Looks for cases where we can't explain how some balance was ever read."
  []
  (reify checker/Checker
    (check [this test history {:keys [analysis] :as opts}]
      ;(info :accounts
      ;      (pprint-str (:accounts analysis)))
      (let [inexplicable-balances (->> (:accounts analysis)
                                       vals
                                       (mapcat :inexplicable-balances)
                                       (sort-by :index))]
        (if (seq inexplicable-balances)
          {:valid? false
           :inexplicable-balances inexplicable-balances}
          {:valid? true})))))

(defn negative-checker
  "Looks for cases where the balance of an account appears to have gone
  negative."
  []
  (reify checker/Checker
    (check [this test history {:keys [analysis] :as opts}]
      (let [; Negative, but considers nil OK
            neg?     (fnil neg? 0)
            ; Scan balances for negatives--this is VERY bad!
            balances (->> history
                          (filter op/ok?)
                          (filter (comp #{:balance} :f))
                          (filter (comp neg? :balance :value)))
            ; Scan txn logs for negative inferred balances--less bad, knowing
            ; that txn logs appear to basically be swiss cheese, but still, we
            ; shouldn't do this
            txns (->> (:accounts analysis)
                      vals
                      (mapcat (fn [{:keys [account txn-log]}]
                                (->> (:txns txn-log)
                                     (filter (comp neg? :balance'))
                                     (map (fn [txn]
                                            {:account account
                                             :txn txn}))))))]
        {:valid? (and (not (seq balances))
                      (not (seq txns)))
         :txns      txns
         :balances  balances}))))

(defn txn-diff
  "Takes a txn1 (from a :txn op invocation) and a txn2 (from a txn log) and
  returns a map describing their difference, if one exists."
  [test txn1 txn2]
  ; Radix is going to silently drop self-transfers from
  ; the transactions. Not sure whether this is bad or
  ; not, but it feels... minor? We'll let it pass by
  ; filtering them out of our own representation here.
  (let [txn1 (if (:raw-txn-logs test)
               txn1
               (->> (:ops txn1)
                    (remove (fn [op]
                              (= (:from op) (:to op))))
                    (assoc txn1 :ops)))]
    (cond (nil? txn1)
          {:type   :out-of-nowhere
           :actual txn2}

          (not= (count (:ops txn1)) (count (:actions txn2)))
          {:type :diff-action-count
           :expected txn1
           :actual   txn2}

          :else
          (let [action-errs
                (->> (map (fn [a1 a2]
                            (when (or (not= (:type a1)   (:type a2))
                                      (not= (:from a1)   (:from a2))
                                      (not= (:to a1)     (:to a2))
                                      (not= (:amount a1) (:amount a2))
                                      (not= (:rri a1)    (:rri a2)))
                              [a1 a2]))
                          (:ops txn1)
                          (:actions txn2))
                     (remove nil?))]
            (when (seq action-errs)
              {:type     :diff-actions
               :expected txn1
               :actual   txn2
               :errors   (action-errs)})))))

(defn faithful-checker
  "Verifies that the representations of txns in the txn log align with the txns
  we actually submitted."
  []
  (reify checker/Checker
    (check [this test history {:keys [analysis] :as opts}]
      (let [; Compute a map of IDs to txns we submitted
            txn-txns (->> history
                          (filter op/invoke?)
                          (filter (comp #{:txn} :f))
                          (map :value)
                          (group-by :id)
                          (map-vals first))
            ; Now go through the txn logs for each account, making sure txns
            ; are faithfully represented.
            errs (->> (for [[account account-analysis] (:accounts analysis)
                            txn (:txns (:txn-log account-analysis))]
                        (when-let [id (:id txn)]
                          (txn-diff test (get txn-txns id) txn)))
                      (remove nil?))]
        {:valid? (empty? errs)
         :errors errs}))))

(defn balance-vis-checker
  "Renders balances, transaction logs, etc for each account"
  []
  (reify checker/Checker
    (check [this test history {:keys [analysis] :as opts}]
      (->> (:accounts analysis)
           keys
           (pmap (fn [account]
                   (balance-vis/render-account! test analysis account)))
           dorun)
      {:valid? true})))

(defn stats
  "Basic statistics"
  []
  (reify checker/Checker
    (check [this test history {:keys [analysis] :as opts}]
      (let [errs (->> history
                      (keep :error)
                      (map (fn [e] (if (keyword? e) e (first e))))
                      frequencies)
            accounts (->> analysis :accounts vals)
            possible-txn-count (->> accounts (mapcat :txn-ids) set count)
            logged-txn-count   (->> accounts (mapcat :logged-txn-ids) set count)
            unlogged-txn-count (->> accounts (mapcat :unlogged-txn-ids) set
                                    count)
            balance-count (->> history
                               (filter (comp #{:balance} :f))
                               (filter op/ok?)
                               count)
            inexplicable-balance-count (->> accounts
                                            (map (comp count
                                                       :inexplicable-balances))
                                            (reduce + 0))
            info-txn-count (->> history
                                (filter op/info?)
                                (filter (comp #{:txn} :f))
                                count)]
           {:valid?                     true
            :errors                     errs
            :info-txn-count             info-txn-count
            :possible-txn-count         possible-txn-count
            :logged-txn-count           logged-txn-count
            :unlogged-txn-count         unlogged-txn-count
            :balance-count              balance-count
            :inexplicable-balance-count inexplicable-balance-count}))))

(defn txn-perf
  "Perf graphs just for transactions--sometimes the plots can get a little
  noisy."
  []
  (reify checker/Checker
    (check [this test history opts]
      (checker/check (checker/perf (:perf-opts test))
                     test
                     (->> history
                          (filter (fn [op]
                                    (or (= :txn (:f op))
                                        (= :nemesis (:process op)))))
                          vec)
                     (assoc opts :subdirectory "txn-perf")))))

(defn raw-txn-log-compatible-orders
  "A checker which looks only at :raw-txn-log operations and verifies they all
  have compatible orders."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [logs (->> history
                      (filter (comp #{:raw-txn-log} :f))
                      (filter op/ok?)
                      (map :value)
                      (sort-by count)
                      distinct)
            incompatible (list-append/incompatible-orders {:log logs})]
        (if incompatible
          {:valid? false
           :incompatible incompatible}
          {:valid? true})))))

(defn checker
  "Unified checker."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [; Checkers we always run
            base-checkers {:stats     (stats)
                           :txn-perf  (txn-perf)}
            ; Our particular checker
            checker (if ; We're JUST doing compatible orders--the full
                      ; analysis is super expensive over raw txn logs, and
                      ; we're looking for rare anomalies so we need to be
                      ; able to test longer histories here.
                      (:check-only-raw-txn-log-compatible-orders test)
                      (assoc base-checkers
                             :raw-txn-log-compatible-orders
                             (raw-txn-log-compatible-orders))

                      ; The full suite
                      (assoc base-checkers
                             :faithful     (faithful-checker)
                             :negative     (negative-checker)
                             :inexplicable (inexplicable-balance-checker)
                             :list-append  (list-append-checker)
                             :raw-txn      (raw-txn-checker)
                             :balance-vis  (balance-vis-checker)
                             :timeline     (timeline/html)))
            checker (checker/compose checker)

            ; We start by rewriting info txns using check-txn operations--we're
            ; going to hit a *ton* of timeouts, and basically everything
            ; benefits from reducing those.
            history  (rewrite-info-txns history)]
        (if (:check-only-raw-txn-log-compatible-orders test)
          ; We're trying to do this as quick as possible; don't even both with
          ; analysis phase.
          (checker/check checker test history opts)

          ; When we're doing the full analysis, we want to compute the
          ; analysis just once, and let every checker use it independently.
          (checker/check checker test history
                         (assoc opts :analysis (analysis history test))))))))
