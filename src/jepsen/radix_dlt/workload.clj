(ns jepsen.radix-dlt.workload
  "A generator, client, and checker for RadixDLT"
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ letr]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [util :as util :refer [pprint-str]]]
            [jepsen.radix-dlt [accounts :as a]
                              [checker :as rchecker]
                              [client :as rc]
                              [util :as u]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn txn!
  "Takes a client, an accounts map atom, an atom mapping radix txn ids to
  Jepsen txn ids, and a :txn op. Executes the txn op and constructs a resulting
  op. As a side effect, updates the radix->jepsen txn id map so that we can
  interpret this txn ID when we read it in the future."
  [client accounts radix-txn-id->txn-id op]
  (letr [value (:value op)
         ; Map account IDs to addresses and add the token RRI to each op.
         ops (map (fn [{:keys [type] :as op}]
                    (case type
                      :transfer (let [{:keys [from to amount rri]} op]
                                  [type
                                   (a/id->address @accounts from)
                                   (a/id->address @accounts to)
                                   amount
                                   rri])))
                  (:ops value))
         ; Prepare txn. This gives us the txid and fee, which we're going
         ; to need if we crash.
         txn (try+ (rc/prep-txn client (a/id->key-pair @accounts (:from value))
                                (str "t" (:id value))
                                ops)
                   (catch [:type :timeout] e
                     (throw+ {:type :txn-prep-failed}))
                   (catch [:type :radix-dlt/failure] e
                     (throw+ {:type :txn-prep-failed})))
         ; Save the txn ID *before* we actually create the txn--ensuring we can
         ; interpret it if it comes up later.
         _   (swap! radix-txn-id->txn-id assoc (:id txn) (:id value))
         op' (update op :value
                     assoc
                     :txn-id (:id txn)
                     :fee    (:fee txn))]
    ; Submit txn. We wrap this with another with-errors, so that we can
    ; return a info/fail op with a fee and txn-id.
    (rc/with-errors op'
      (let [txn'   (rc/submit-txn! client (:finalized txn))
            status (-> txn' :status deref :status)]
        (assoc op' :type (case status
                           :confirmed  :ok
                           :pending    :info
                           :failed     :fail))))))

(defrecord Client [conn node token-rri radix-txn-id->txn-id]
  client/Client
  (open! [this test node]
    (assoc this
           :node node
           :conn (rc/open node)))

  (setup! [this test]
    ; Fetch the native token RRI
    (locking token-rri
      (when-not (realized? token-rri)
        (deliver token-rri (:rri (rc/native-token conn))))))

  (invoke! [this test {:keys [f value] :as op}]
    (rc/with-errors op
      (case f
        :txn (txn! conn (:accounts test) radix-txn-id->txn-id op)

        :check-txn
        (try+ (let [status (:status (rc/txn-status conn (:txn-id value)))]
                (assoc op
                       :type :ok
                       :value (assoc value :status status)))
          (catch [:type :radix-dlt/failure
                  :code 1604] e#
            (if (re-find #"missing required creator property 'status'"
                         (:message e#))
              ; After a few hours, beta.35.1 returns JSON objects the client
              ; doesn't know how to deserialize. We treat these as a :info op,
              ; because sometimes they actually DO go on to be resolved.
              (assoc op :type :info, :error :missing-status-field)
              (throw+ e#))))

        :txn-log
        (let [accounts (:accounts test)
              ; A little helper: we want to translate addresses into numeric IDs
              ; when we know them, but leave them as big hex strings otherwise
              address->id (fn [address]
                            (when address
                              (or (a/address-str->id @accounts address)
                                  address)))
              log (->> (rc/txn-history conn (a/id->address @accounts
                                                         (:account value)))
                       reverse
                       (mapv (fn [{:keys [fee message actions]}]
                               {:fee      fee
                                :message  message
                                ; Rewrite accounts to IDs
                                :actions (mapv (fn [action]
                                                 (-> action
                                                     (update :from address->id)
                                                     (update :to address->id)))
                                               actions)})))
              value'  (assoc value :txns log)]
          (assoc op :type :ok :value value'))

        :raw-balances
        (let [accounts (:accounts test)
              res (rc/raw-balances node)
              ; Map those balances back to a map of account IDs to balances.
              balances (->> (:entries res)
                            ; Rewrite known accounts to IDs
                            (keep (fn [{:keys [amount key] :as m}]
                                    (let [id (or (->> key
                                                      rc/->account-address
                                                      (a/address->id @accounts))
                                                 key)]
                                      ; Don't bother recording default accounts
                                      ; that we don't touch
                                      (when-not (u/unused-account-ids id)
                                        [id (bigint amount)]))))
                            (into {}))]
          (assoc op :type :ok, :value balances))

        :raw-txn-log
        (let [res (rc/raw-transactions node)
              txn-ids (->> (:transactions res)
                           (map :transaction_identifier)
                           ; Translate Radix txn ids back to Jepsen's smaller
                           ; txn IDs.
                           (keep @radix-txn-id->txn-id)
                           )]
          (assoc op :type :ok, :value txn-ids))

        :balance
        (let [b (->> (rc/token-balances conn (a/id->address @(:accounts test)
                                                            (:account value)))
                     :balances
                     (filter (comp #{@token-rri} :rri))
                     first
                     :amount)
              value' (assoc value :balance b)]
          (assoc op :type :ok, :value value')))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defn client
  "Constructs a fresh Jepsen client. Takes an rri promise for the default
  token."
  [token-rri]
  (Client. nil nil token-rri (atom {})))

;; Generator

(defn gen-key
  "Turns a key index into a key on a generator."
  [gen ki]
  (-> gen :pool (nth ki)))

(defn gen-rand-key
  "Selects a random key from a Generator's pool."
  [gen]
  ; Choosing our random numbers from this range converts them to an
  ; index in the range [0, key-count]
  (case (:key-dist gen)
    :uniform (rand-int (:key-count gen))

    :exponential
    (let [key-dist-base (:key-dist-base gen)
          key-count     (:key-count gen)
          ; Choosing our random numbers from this range converts them to an
          ; index in the range [0, key-count).
          key-dist-scale (-> (Math/pow key-dist-base key-count)
                             (- 1)
                             (* key-dist-base)
                             (/ (- key-dist-base 1)))
          ki (-> (rand key-dist-scale)
                 (+ key-dist-base)
                 Math/log
                 (/ (Math/log key-dist-base))
                 (- 1)
                 Math/floor
                 long)]
      (-> gen :key-pool (nth ki)))))

(defn gen-rand-read-key
  "Selects a random key from a Generator for a read. We sometimes emit reads of
  the default account 1, in addition to those in the pool."
  [gen]
  (if (< (rand) (/ (inc (:key-count gen))))
    1
    (gen-rand-key gen)))

(defn gen-record-write!
  "Takes a generator and a key index, and records that key as having been
  written. If the key is written more than max-writes-per-key, replaces the key
  in the pool. As a side effect, adds new keys to the accounts index."
  [gen k]
  (let [writes (-> gen :write-counts (get k 0) inc)]
    ; If we've written it too much, replace it with a new key.
    (if (<= (:max-writes-per-key gen) writes)
      (let [k' (:next-key gen)
            ki (.indexOf ^java.util.List (:key-pool gen) k)
            acct (a/rand-account k')]
        ; Record this new account
        (swap! (:accounts gen) a/conj-account acct)

        (assoc gen
               :next-key     (inc k')
               :key-pool     (assoc (:key-pool gen) ki k')
               :write-counts (-> (:write-counts gen)
                                 (dissoc k)
                                 (assoc k' 0))))
      ; Otherwise, just record the write
      (assoc-in gen [:write-counts k] writes))))

(defn gen-first-write-of?
  "Takes a generator and a key, and returns true if this is the first
  write of that key index."
  [gen k]
  (-> gen :write-counts (get k 0) zero?))

(defn gen-transfer!
  "Takes a generator. Generates a series of transfer actions, and returns
  [transfer gen'], where gen' records the writes of keys involved in the
  transfer. As a side effect, updates the account structure with any new keys."
  [gen]
  (let [; What account is performing this transaction?
        from  (gen-rand-key gen)
        ; Is this account funded? If not, let's replace it with the default
        ; address probabilistically; no sense in pulling money out of thin air.
        from  (if (and (< (rand) 1.0)
                       (not (get (:funded? gen) from)))
                1
                from)
        ; How many actions should we generate?
        n     (inc (rand-int (:max-txn-size gen)))
        ; Construct ops
        ops (->> (range n)
                 (mapv (fn [_]
                         {:type   :transfer
                          :from   from
                          :to     (gen-rand-key gen)
                          :amount (if (u/default-account-id? from)
                                    ; If this is the first write, give them a
                                    ; bunch to play with; otherwise almost
                                    ; every transfer will fail.
                                    (-> 100 rand-int inc (* u/fee-scale 10))

                                    ; For transfers between normal accounts,
                                    ; pick 1-100x fee
                                    (-> 100 rand-int inc (* u/fee-scale)))
                          :rri    @(:token-rri gen)})))
        ; What accounts are we touching?
        tos   (set (map :to ops))
        ; Don't bother with default accounts; they're excluded from write
        ; counting
        accts (->> (conj tos from)
                   (remove u/default-account-id?))
        ; Record those as being written
        gen' (reduce gen-record-write! gen accts)
        ; And bump the txn id
        gen' (update gen' :next-txn-id inc)]

    [{:id   (:next-txn-id gen)
      :from from
      :ops  ops} gen']))

; This stores the state we need to select keys, and generates all three types
; of operations: transactions, balance reads, and txn-log reads.
(defrecord Generator
  [key-count
   key-dist
   key-dist-base
   max-writes-per-key
   max-txn-size
   fs           ; Set of :fs we generate
   accounts     ; An atom to an accounts structure
   token-rri    ; A promise of an RRI for the token we'll transfer
   key-pool     ; A vector of active keys; always of size key-count
   write-counts ; A map of keys to the number of times they've been written.
   funded?      ; A map of keys to whether we think they currently contain XRD.
   next-key     ; What's the next key we'll allocate?
   next-txn-id  ; What's our next transaction ID?
   ]
  gen/Generator
  (update [this test context event]
    (let [{:keys [type f value]} event]
      ; For transactions...
      (case f
        ; When we see a txn go through...
        :txn
        (case (:type event)
          ; Record receiving accounts as being funded
          :ok (let [funded?' (reduce (fn [funded? {:keys [to]}]
                                       (assoc funded? to true))
                                     funded?
                                     (:ops value))]
                (assoc this :funded? funded?'))

          ; When a transaction fails, decrement its write count; we can give it
          ; another shot.
          :fail (let [wc' (->> (:ops value)
                               (map :to)
                               (cons (:from value))
                               set
                               (reduce (fn [wc k]
                                         (if (contains? wc k)
                                           (update wc k dec)
                                           wc))
                                       write-counts))]
                  (assoc this :write-counts wc'))

          this)

        ; Not a txn
        this)))

  (op [this test context]
    ; Generate a generic invocation
    (let [op (gen/fill-in-op {} context)]
      (if (= :pending op)
        ; Every process is busy right now
        [:pending this]

        ; Right, what thread is this?
        (let [t (->> op :process (gen/process->thread context))
              ; How far into the concurrency of the test is that?
              zone (/ t (count (:workers context)))
              ; Pick a f based on the zone: first quarter do txns, others are
              ; reads. We do this to avoid blocking readers on txns when txns
              ; are super slow.
              f (if (< zone 1/4)
                  :txn
                  (->> [:balance :txn-log :raw-balances :raw-txn-log]
                       (filter fs)
                       vec
                       rand-nth))
              op (assoc op :f f)]
          (case f
            :txn (let [[transfer gen'] (gen-transfer! this)]
                   [(assoc op :value transfer)
                    gen'])

            ; For transaction logs and balances, we either select a key from the
            ; pool, or our default account 1.
            :txn-log [(assoc op :value {:account (gen-rand-read-key this)})
                      this]

            ; For raw balances, no value needed
            :raw-balances [(assoc op :value nil) this]

            ; For the raw txn log, there's no value required--we're reading
            ; everything.
            :raw-txn-log [(assoc op :value nil) this]

            :balance [(assoc op :value {:account (gen-rand-read-key this)})
                      this]))))))

(defn generator!
  "Constructs a Generator out of an options map. Options are:

    :accounts             An atom to an accounts structure

    :fs                   The :f functions of operations we generate

    :key-dist             Controls probability distribution for keys being
                          selected for a given operation. Choosing :uniform
                          means every key has an equal probability of appearing.
                          :exponential means that key i in the current key pool
                          is k^i times more likely than the first key to be
                          chosen. Defaults to :exponential.

    :key-dist-base        The base for an exponential distribution. Defaults
                          to 2, so the first key is twice as likely as the
                          second, which is twice as likely as the third, etc.

    :key-count            Number of distinct keys at any point. Defaults to
                          10 for exponential, 3 for uniform.

    :max-writes-per-key   Maximum number of operations per key. Defaults to 32.

  The selection of account IDs is a little tricky.

  We want some accounts to be frequently accessed in order to create contention
  and find race conditions. We also want some accounts to be infrequently
  accessed to expose behaviors over longer time horizons without growing to
  enormous size. This implies we should choose some sort of biased
  distribution: we use an exponential distribution.

  Some accounts (1-5) start with a good chunk of XRD; the others have none at
  all initially. This suggests that we need to involve XRD-bearing accounts
  throughout the lifespan of the test; otherwise we'll spread a fixed pool of
  XRD over a larger and larger group of accounts and hit more and more
  insufficient-balance errors. We want *some* insufficient balances (since this
  is an important invariant to test!) but not so many that transfers never
  happen.

  Moreover, fees continually drain money out of the system, which implies we
  should keep involving the default accounts in transfers throughout the
  lifespan of the test.

  We *also* want to limit the number of transfers per account, so that
  per-account histories are easier to read and don't take too long to query.
  That implies we need a constantly-rotating pool of keys, from which we take
  an exponential distribution:

  █
  █     [we gradually rotate higher keys]
  █
  █     █▅▂▂▁█▅█▅▂█▅▂▂▁
  12345 6789...

  But this doesn't help us bound histories: default accounts will accrue huge
  numbers of transactions over the course of the history. So instead, we treat
  these accounts specially: when we *would* have transferred money from a fresh
  account, we emit a special transaction which fills up that account with funds
  from a default account. From that point on, we can derive our account IDs
  from an exponentially-distributed rotating pool of non-default accounts.

  As a side-effect, adds initial accounts to the accounts atom."
  [opts]
  (let [key-dist  (:key-dist opts :exponential)
        key-count (:key-count opts (case key-dist
                                     :exponential 10
                                     :uniform     3))
        key-pool (->> (iterate inc 1)
                      (remove u/default-account-id?)
                      (take key-count)
                      vec)]
    ; Record initial keys as accounts
    (->> key-pool
         (map a/rand-account)
         (mapv (partial swap! (:accounts opts) a/conj-account)))
    ; And build generator
    (map->Generator
      {:accounts           (:accounts opts)
       :token-rri          (:token-rri opts)
       :fs                 (:fs opts)
       :key-dist           key-dist
       :key-dist-base      (:key-dist-base opts 2)
       :key-count          key-count
       :max-writes-per-key (:max-writes-per-key opts 32)
       :max-txn-size       (:max-txn-size opts 4)
       :key-pool           key-pool
       :write-counts       {}
       :funded?            {}
       :next-key           (inc (peek key-pool))
       :next-txn-id        0})))

(defrecord CheckTxnGenerator [gen txns]
  gen/Generator
  (update [this test context {:keys [f type value] :as event}]
    (when (< (rand) 1/10)
      (info :check-txn-queue (count txns)))
    (if (or (and (= :txn f)
                 (= :info type)
                 (:txn-id value)) ; We can't always get a txn-id here
            (and (= :check-txn f)
                 (or (= :fail type)
                     (= :info type)
                     (= :pending (:status value)))))
      (do ; Transaction crashed, or our attempt to check it failed, or it's
          ; still pending. Add it to our queue.
          ;(info :adding :txn-id (:txn-id value) :value value)
          (CheckTxnGenerator. (gen/update gen test context event)
                              (conj txns (:txn-id value))))

      ; Pass through
      (CheckTxnGenerator. (gen/update gen test context event) txns)))

  (op [this test context]
    ; Get an op from the underlying generator
    (let [[op gen'] (gen/op gen test context)
          ; And a txn-id we might want to check on
          txn-id    (peek txns)]
      ;(info :txn-id txn-id :f (:f op))
      (if (and txn-id
               (< (rand) 1/6) ; Only rewrite some requests.
               (#{:balance :txn-log :raw-balances :raw-txn-log} (:f op)))
        ; We've got a txn-id we haven't checked on, and this would have been a
        ; read. Rewrite it to a txn-check op.
        [(assoc op :f :check-txn, :value {:txn-id txn-id})
         (CheckTxnGenerator. gen' (pop txns))]

        ; Just pass through
        [op (CheckTxnGenerator. gen' txns)]))))

(defn check-txn-generator
  "Wraps a Generator in one which detects crashed :txn operations and, every so
  often, issues a :check-txn request which attempts to determine whether the
  txn actually happened or not."
  [gen]
  (CheckTxnGenerator. gen clojure.lang.PersistentQueue/EMPTY))

(defn workload
  "Constructs a package of a client and generator."
  [opts]
  (let [token-rri (promise)
        accounts  (:accounts opts)]
    {:client          (client token-rri)
     :checker         (rchecker/checker)
     :generator       (->> (assoc opts
                                  :accounts  accounts
                                  :token-rri token-rri)
                           generator!
                           check-txn-generator)
     :final-generator [(when (get (:fs opts) :raw-txn-log) {:f :raw-txn-log})
                       (when (get (:fs opts) :txn-log)
                         (delay
                           (->> @accounts
                                :accounts
                                (map :id)
                                (map (fn [acct]
                                       {:f :txn-log, :value {:account acct}})))))]}))
