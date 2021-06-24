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
            [jepsen.tests.cycle.append :as append]))

(defn mop-accounts
  "Takes a micro-op in a Radix transaction (e.g. [:transfer 1 2 50]) and
  returns the collection of involved accounts (e.g. [1 2])"
  [op]
  (case (first op)
    :transfer (subvec op 1 3)))

(defn txn-id
  "Takes a txn from a txn-log operation and returns the ID encoded in its
  message, if one exists."
  [txn]
  (when-let [msg (:message txn)]
    (when-let [[match id] (re-find #"t(\d+)" msg)]
      (parse-long id))))

(defn xrd?
  "Is an RRI XRD?"
  [rri]
  (when rri
    (re-find #"^xrd_" rri)))

(defn longest-txn-logs
  "Takes a Radix history and finds a map of account IDs to the longest possible
  transaction log for that account."
  [history]
  (reduce (fn [longest {:keys [type f value] :as op}]
            (if (and (= :ok type) (= :txn-log f))
              (let [account (:account value)
                    log     (get longest account)
                    log2    (:txns value)]
                (if (or (nil? log) (< (count log) (count log2)))
                  ; This log is longer
                  (assoc longest account log2)
                  longest))
              ; Not a txn log
              longest))
          {}
          history))

(defn apply-txn
  "Takes an RRI of interest, an account ID, a balance number and applies a
  transaction (i.e. from a txn-log :txns field) to it, returning a new balance
  number."
  [account balance txn]
  (info nil)
  ;(info :account account :balance balance :txn txn)
  ; If we're the originator of this transaction, then we paid the fee
  (let [balance (if (-> txn :actions first :from (= account))
                 (- balance (:fee txn))
                 balance)]
    (reduce (fn [balance {:keys [type from to amount rri]}]
              ;(info :balance balance :type type :from from :to to :amount amount :rri rri :xrd? (xrd? rri))
              ; We're only interested in xrd transfers
              (if-not (and (= type :transfer) (xrd? rri))
                balance
                (cond-> balance
                  (= from account) (- amount)     ; Debit
                  (= to account)   (+ amount))))  ; Credit
            balance
            (:actions txn))))

(def init-balance
  "How much XRD do accounts start with?"
  ;999999999999999999999999999499999999999999995
  1000000000000000000000000000000000000000000000)

(defn account-balance->txn-ids
  "Takes an account and a longest-txn-logs series of transactions on that
  account. Plays forward the transactions, building a map of balances to the
  vector of transaction IDs which produced that balance, or :multiple if more
  than one exists."
  [account txns]
  ; First, we're going to need a series of vectors of transaction IDs: one for
  ; each set of transactions "read" by each balance.
  ;
  ;   []
  ;   [1]
  ;   [1 3]
  ;   [1 3 8]
  ;   ...
  ; We're going to do this by constructing a single vector of all txn IDs, and
  ; constructing subvecs of it as needed.
  (let [txn-ids (vec (keep txn-id txns))]
    ; Now, step through transactions, updating our account balance and building
    ; a map of balances to subvecs of txn-ids
    (loop [m       {}
           balance init-balance
           ; Index into our transactions list
           txn-i     0
           ; Index into the transaction ids list, since not all txns *have* IDs
           txn-ids-i -1]
      (if (<= (count txns) txn-i)
        ; Done!
        m
        ; OK, let's look at this transaction
        (let [txn        (nth txns txn-i)
              ; If this transaction has an ID, we should step one further into
              ; the txn-ids vector.
              txn-ids-i' (if (txn-id txn)
                           (inc txn-ids-i)
                           txn-ids-i)
              ; What's the resulting balance if we apply it?
              balance'  (apply-txn account balance txn)
              ; Do we already have an entry for this balance?
              extant    (get m balance')
              m'        (assoc m balance'
                               (if extant
                                 ; If we already have a list, convert it to
                                 ; :multiple
                                 :multiple
                                 ; If not, we know this sequence of
                                 ; transactions produced this balance.
                                 (subvec txn-ids 0 (inc txn-ids-i'))))]
          (recur m' balance' (inc txn-i) txn-ids-i'))))))

(defn account->balance->txn-ids
  "Takes a Radix history and computes a map of accounts to maps of balances to
  the sequence of transaction IDs which produced that balance. If we don't know
  how a balance was produced, that value is `nil`; if a balance is not unique
  within an account, that value is `:multiple`."
  [history]
  (reduce (fn [m [account txns]]
            (assoc m account (account-balance->txn-ids account txns)))
          {}
          (longest-txn-logs history)))

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
                    ; Compute a set of involved accounts
                    txn (->> (:ops value)
                             (mapcat mop-accounts)
                             (cons (:from value))
                             distinct
                             ; Generate synthetic transaction
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
                           :type :info
                           :error :unknown-balance
                           :value [[:r account nil]])

                    :multiple
                    ; We info this read, since it can't be interpreted
                    ; distinctly
                    (assoc op :type :info
                           :error :ambiguous-balance
                           :value [[:r account nil]])

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
  (let [elle (append/checker {:consistency-models [:strict-serializable]})]
    (reify checker/Checker
      (check [this test history opts]
        (let [history (list-append-history history)]
          (info :history (pprint-str history))
          ; Oh this is such a gross hack. There's all this *really* nice,
          ; sophisticated machinery for detecting various anomalies in
          ; elle.txn, but it all assumes the builtin realtime/process graphs.
          ; Instead of redefining all the anomaly types for our own weird
          ; temporal model, we're going to sneak in there and redefine how Elle
          ; computes its realtime graph.
          (let [res (with-redefs [elle.core/realtime-graph
                                  (partial realtime+node-graph test)]
                      (checker/check elle test history opts))
                ; As an extra piece of debugging info, we're going to record
                ; how many reads went recognized/unrecognized.
                balances           (filter (comp #{:balance} :f) history)
                ok-balances        (filter (comp #{:ok} :type) balances)
                info-balances      (filter (comp #{:info} :type) balances)
                unknown-balances   (filter (comp #{:unknown-balance} :error)
                                           info-balances)
                ambiguous-balances (filter (comp #{:ambiguous-balance} :error)
                                           info-balances)]
            (assoc res
                   :ok-balance-count        (count ok-balances)
                   :unknown-balance-count   (count unknown-balances)
                   :ambiguous-balance-count (count ambiguous-balances)
                   :unknown-balances        (take 5 unknown-balances)
                   :ambiguous-balances      (take 5 ambiguous-balances))))))))

(defn checker
  "Unified checker."
  []
  (checker/compose
    {:timeline    (timeline/html)
     :list-append (list-append-checker)}))
