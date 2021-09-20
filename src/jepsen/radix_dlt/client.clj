(ns jepsen.radix-dlt.client
  "Wrapper around the Java client library.

  See https://github.com/radixdlt/radixdlt/blob/1.0-beta.35.1/radixdlt-java/radixdlt-java/src/test/java/com/radixdlt/client/lib/impl/SynchronousRadixApiClientTest.java for some test example code."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.core.typed :as t]
            [clj-http.client :as http]
            [dom-top.core :as dt :refer [assert+]]
            [jepsen [util :as util :refer [pprint-str]]]
            [jepsen.radix-dlt [util :as u]]
            [potemkin :refer [def-derived-map]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import ;(com.radixdlt.api.data.action UpdateValidatorMetadataAction)
           (com.radixdlt.client.lib.api AccountAddress
                                        ActionType
                                        NavigationCursor
                                        TransactionRequest
                                        TransactionRequest$TransactionRequestBuilder
                                        TxTimestamp
                                        ValidatorAddress)
           (com.radixdlt.client.lib.api.sync RadixApi)
           (com.radixdlt.client.lib.dto Action
                                        Balance
                                        BuiltTransaction
                                        FinalizedTransaction
                                        NetworkId
                                        TokenBalances
                                        TokenInfo
                                        TransactionDTO
                                        TransactionHistory
                                        TransactionStatus
                                        TransactionStatusDTO
                                        TxBlobDTO
                                        TxDTO
                                        ValidatorsResponse
                                        ValidatorDTO)
           (com.radixdlt.client.lib.network HttpClients)
           (com.radixdlt.crypto ECKeyPair
                                ECPublicKey)
           (com.radixdlt.identifiers AID
                                     AccountAddressing)
           (com.radixdlt.utils Ints
                               UInt256)
           (com.radixdlt.utils.functional Failure
                                          Result)
           (java.util Base64
                      Optional
                      OptionalLong)
           (java.util.function Function)
           (org.bouncycastle.util.encoders Hex)))

;(def stokenet-network-id
;  2)

;(def local-network-id
;  99)

(def current-network-id
  "We need this network ID to deserialize responses. We fill it in upon first
  connection to any network--this prevents us from testing multiple networks in
  the same JVM, but... ah well. It's a hack."
  (promise))

(defprotocol ToClj
  "Transforms RadixDLT client types into Clojure structures."
  (->clj [x]))

(def-derived-map ActionMap [^Action a]
  :type       (->clj (.getType a))
  :from       (->clj (.getFrom a))
  :to         (->clj (.getTo a))
  :validator  (->clj (.getValidator a))
  :amount     (->clj (.getAmount a))
  :rri        (->clj (.getRri a)))

(def-derived-map TokenInfoMap [^TokenInfo t]
  :name            (.getName t)
  :rri             (.getRri t)
  :symbol          (.getSymbol t)
  :description     (.getDescription t)
  :granularity     (->clj (.getGranularity t))
  :supply-mutable? (.isSupplyMutable t)
  :current-supply  (->clj (.getCurrentSupply t))
  :token-info-url  (.getTokenInfoURL t)
  :icon-url        (.getIconURL t))

(def-derived-map TransactionMap [^TransactionDTO t]
  :id       (->clj (.getTxID t))
  :sent-at  (->clj (.getSentAt t))
  :fee      (->clj (.getFee t))
  :message  (->clj (.getMessage t))
  :actions  (map ->clj (.getActions t)))

(def-derived-map ValidatorMap [^ValidatorDTO v]
  :address        (->clj (.getAddress v))
  :owner-address  (->clj (.getOwnerAddress v))
  :name           (.getName v)
  :info-url       (.getInfoURL v)
  :total-delegated-stake  (->clj (.getTotalDelegatedStake v))
  :owner-delegation (->clj (.getOwnerDelegation v))
  :external-stake-accepted? (.isExternalStakeAccepted v))

(defn unres
  "Radix's client API wraps absolutely everything in Result types to avoid
  throwing exceptions. unres unwraps those boxes, and throws exceptions if
  necessary."
  [^Result r]
  ; We return the value of the Result directly, and throw a Slingshot stone
  ; for errors.
  (.fold r
         (reify Function (apply [_ error] (throw+ (->clj error))))
         (reify Function (apply [_ value] value))))

(extend-protocol ToClj
  AccountAddress
  (->clj [x]
    (.toString x))

  Action
  (->clj [x] (->ActionMap x))

  ActionType
  (->clj [x]
    (condp = x
      ActionType/MSG      :msg
      ActionType/TRANSFER :transfer
      ActionType/STAKE    :stake
      ActionType/UNSTAKE  :unstake
      ActionType/UNKNOWN  :unknown
                          x))

  AID
  (->clj [x]
    (.toString x))

  Balance
  (->clj [x]
    {:rri     (.getRri x)
     :amount  (->clj (.getAmount x))})

  ; Radix doesn't do exceptions; they have a non-throwable Failure type. We
  ; turn this into a throwable Slingshot stone.
  Failure
  (->clj [x]
    {:type    :radix-dlt/failure
     :message (.message x)
     :code    (.code x)})

  NetworkId
  (->clj [x]
    (.getNetworkId x))

  Optional
  (->clj [x] (->clj (.orElse x nil)))

  OptionalLong
  (->clj [x]
    (when (.isPresent x)
      (.getAsLong x)))

  Result
  (->clj [x] (->clj (unres x)))

  TokenBalances
  (->clj [x]
    {:owner     (->clj (.getOwner x))
     :balances  (map ->clj (.getTokenBalances x))})

  TokenInfo
  (->clj [x] (->TokenInfoMap x))

  TransactionHistory
  (->clj [x]
    ; They renamed this from getCursor to getNextOffset, which is sort of
    ; annoying because some other APIs still use `cursor`. We rename this to
    ; `cursor` here so we can use the same logic for paginating through both
    ; types of collections without having to parameterize the cursor field
    ; name.
    {:cursor (->clj (.getNextOffset x))
     :txns   (map ->clj (.getTransactions x))})

  TransactionDTO
  (->clj [x] (->TransactionMap x))

  TransactionStatus
  (->clj [x]
    (condp = x
      TransactionStatus/PENDING               :pending
      TransactionStatus/CONFIRMED             :confirmed
      TransactionStatus/FAILED                :failed
                                              x))

  TransactionStatusDTO
  (->clj [x]
    {:id     (->clj (.getTxId x))
     :status (->clj (.getStatus x))})

  TxDTO
  (->clj [x] {:id (.getTxId x)})

  TxTimestamp
  (->clj [x] (.getInstant x))

  UInt256
  (->clj [x]
    ; I'm *pretty* sure the byte array representation here is the same as
    ; BigInteger.
    ; https://github.com/radixdlt/radixdlt/blob/353e5e0cde7c0f109da581a894e778dfa04a2203/radixdlt-java-common/src/main/java/com/radixdlt/utils/UInt256.java#L511
    ; seems to do this.
    (BigInteger. 1 (.toByteArray x)))

  ValidatorAddress
  (->clj [x] (.toString x))

  ValidatorDTO
  (->clj [x] (->ValidatorMap x))

  ValidatorsResponse
  (->clj [x]
    {:cursor (.orElse (.getCursor x) nil)
     :validators (map ->clj (.getValidators x))})

  Object
  (->clj [x] x)

  nil
  (->clj [x] nil))

(defprotocol ToAccountAddress
  "Converts strings and AccountAddresses back into AccountAddresses."
  (->account-address [x]))

;; Keypair management

(defn ^ECKeyPair key-pair
  "Takes an integer id and returns a keypair. These correspond to hardcoded
  keypair IDs in the dev universe, which run from 1-10.

  Adapted from
  https://github.com/radixdlt/radixdlt/blob/1.0-beta.35.1/radixdlt-java/radixdlt-java/src/test/java/com/radixdlt/client/lib/impl/SynchronousRadixApiClientTest.java."
  [id]
  (let [private-key (byte-array ECKeyPair/BYTES)]
    (Ints/copyTo id private-key (- ECKeyPair/BYTES Integer/BYTES))
    (ECKeyPair/fromPrivateKey private-key)))

(defn ^ECKeyPair new-key-pair
  "Constructs a new ECKeyPair randomly."
  []
  (ECKeyPair/generateNew))

(defn private-key
  "Returns the private key (a byte array) of an ECKeyPair."
  [^ECKeyPair key-pair]
  (.getPrivateKey key-pair))

(defn ^ECPublicKey public-key
  "Returns the public key (ECPublicKey) of an ECKeyPair."
  [^ECKeyPair key-pair]
  (.getPublicKey key-pair))

(defn private-key-str->key-pair
  "Converts a private key string (e.g.
  ZCpcCF2EhnPBt774Y6UTZ5qSaIiPhfkBmr1zw9ZtklA=; a base64-encoded string from
  the generated universe) to an ECKeyPair."
  [^String base64-str]
  (-> base64-str u/base64->bytes ECKeyPair/fromPrivateKey))

;; Coercions for account & validator addresses

(extend-protocol ToAccountAddress
  AccountAddress
  (->account-address [x] x)

  ECKeyPair
  (->account-address [x]
    (AccountAddress/create (public-key x)))

  String
  (->account-address [x]
    (let [readdr (-> (com.radixdlt.networks.Addressing/ofNetworkId @current-network-id)
                     .forAccounts
                     (.parse x))]
      (AccountAddress/create readdr))))

(defprotocol ToValidatorAddress
  (->validator-address [x] "Converts something to a ValidatorAddress"))

(extend-protocol ToValidatorAddress
  ValidatorAddress
  (->validator-address [x] x)

  String
  (->validator-address [s]
    (-> (com.radixdlt.networks.Addressing/ofNetworkId @current-network-id)
        .forValidators
        (.parse s)
        ->validator-address))

  ECPublicKey
  (->validator-address [k] (ValidatorAddress/of k))

  ECKeyPair
  (->validator-address [kp]
    (->validator-address (public-key kp))))

(defprotocol ToAID
  "Converts strings and AIDs back into AIDs. We do this because it's nice, for
  our purposes, to have cleanly serializable atom IDs that we can write to logs
  and include in operations, but when we invoke methods, they're going to
  expect AID types again."
  (->aid [x]))

(extend-protocol ToAID
  String
  (->aid [s] (AID/from s))

  AID
  (->aid [a] a))


;; Basic type coercions

(defn ^UInt256 uint256
  "Turns a number into a UInt256"
  [n]
  (condp instance? n
    clojure.lang.BigInt (UInt256/from (str n))
    Long                (UInt256/from ^long n)))

(declare network-id)

(defn ^RadixApi open
  "Opens a connection to a node. As a side effect, delivers the current network
  ID."
  [test node]
  (let [client (->clj
                 (if (:stokenet test)
                   (RadixApi/connect (str "https://" node) 443 443)
                   (RadixApi/connect (str "http://" node))))]
    (when-not (realized? current-network-id)
      (deliver current-network-id (network-id client)))
    client))

;; API operations

(defn native-token
  "Gets the native token of this client."
  [^RadixApi client]
  (-> client .token .describeNative ->clj))

(defn network-id
  "Returns the Network ID of this client."
  [^RadixApi client]
  (-> client .network .id ->clj))

(defn token-balances
  "Looks up the token balances of a given address."
  [^RadixApi client address]
  (-> client .account (.balances (->account-address address)) ->clj))

(defn ^TransactionRequest txn-request
  "Builds a new transaction request. Takes a from account address, a message
  string and a seq of actions of the form...

    [[:transfer from to amount rri]
     [:stake from validator amount]
     [:unstake from validator amount]]"
  [from message actions]
  (.build ^TransactionRequest$TransactionRequestBuilder
    (reduce (fn [^TransactionRequest$TransactionRequestBuilder builder action]
              (case (first action)
                :transfer (let [[_ from to amount rri] action]
                            (.transfer builder
                                       (->account-address from)
                                       (->account-address to)
                                       (uint256 amount)
                                       rri))

                :stake    (let [[_ from validator amount] action]
                            (.stake builder
                                    (->account-address from)
                                    (->validator-address validator)
                                    (uint256 amount)))

                :unstake  (let [[_ from validator amount] action]
                            (.unstake builder
                                      (->account-address from)
                                      (->validator-address validator)
                                      (uint256 amount)))))
            (.. (TransactionRequest/createBuilder (->account-address from))
                (message (str message)))
            actions)))

(defn ^BuiltTransaction build-txn
  "Constructs a transaction from a from account, message, and actions. See
  `transaction-request`."
  [^RadixApi client from message actions]
  (-> client
      .transaction
      (.build (txn-request from message actions))
      unres))

(defn ^FinalizedTransaction local-finalize-txn
  "Takes a BuiltTransaction and finalizes it with a given key pair."
  [^BuiltTransaction txn ^ECKeyPair key-pair]
  (.toFinalized txn key-pair))

(defn ^TxBlobDTO remote-finalize-txn
  "Takes a 'finalized' transaction and sends it to the server for... even more
  finalizing? Returns a map."
  [^RadixApi client ^FinalizedTransaction txn]
  (-> client
      .transaction
      (.finalize txn false)
      unres))

(defn ^TxBlobDTO finalize-txn
  "Takes a built txn and finalizes it using the given keypair. Obtains a txid
  from the server, and returns the FinalizedTransaction, ID and all."
  [^RadixApi client ^BuiltTransaction built-txn key-pair]
  (let [local  (local-finalize-txn built-txn key-pair)
        remote (remote-finalize-txn client local)]
;        txid      (.getTxId (remote-finalize-txn client finalized))]
;    (.withTxId finalized txid)))
    remote))

(defn txn-status
  "Checks the status of a transaction. Transaction IDs can be given either as
  an AID or a String. Returns a map of:

    {:id     tx-id
     :status :confirmed, :pending, or :failed}"
  [^RadixApi client txid]
  (-> client
      .transaction
      (.status (->aid txid))
      ->clj))

(defn await-txn
  "Waits up to <timeout> millis, checking the status of the given txn until
  it's known to be complete. Returns the transaction status as a keyword, e.g.
  :failed, :confirmed. txid can be given either as an AID or a String."
  [^RadixApi client txid timeout]
  (util/await-fn (fn check-status []
                   (let [status (txn-status client txid)]
                     (when (= :pending (:status status))
                       (throw+ {:type ::txn-pending
                                :txid txid}))
                     status))
                 {:retry-interval 10
                  :log-interval   60000
                  :log-message    (str "Waiting for transaction " txid)
                  :timeout        timeout}))

; A little deref-able which resolves to a transaction's status.
(deftype TxnStatus [client id]
  clojure.lang.IDeref
  (deref [_]
    (await-txn client id 10000))

  clojure.lang.IBlockingDeref
  (deref [_ ms timeout-value]
    (try+ (await-txn client id ms)
          (catch [:type :timeout]
            timeout-value)))

  Object
  (toString [_]
    (str "#TxnStatus{:id " (str id) "}")))

; Clojure's going to want to be nice and deref this when printing, which is a
; tad inconvenient.
(defmethod print-method TxnStatus [ts ^java.io.Writer writer]
  (.write writer (str ts)))

(defn submit-txn!
  "Submits a finalized transaction with an ID for processing. Returns a map:

    {:id      tx-id, as a string
     :status  A deref-able of the eventual status of the transaction.}"
  [^RadixApi client ^TxBlobDTO txn]
  (let [^TxDTO tx (-> client .transaction (.submit txn) unres)
        id (.getTxId tx)]
    {:id     (->clj id)
     :status (TxnStatus. client id)}))

(defn prep-txn
  "Prepares, but does not submit, a transaction. Phase 1, 2, 3, 4, and 5 of
  txn! Returns a map of

    {:id        The ID of this txn
     :fee       The fee for this txn
     :finalized TxBlobDTO, which can be submitted via submit-txn!}"
  [^RadixApi client key-pair message actions]
  (let [built     (build-txn client key-pair message actions)
        finalized (finalize-txn client built key-pair)]
    {:id          (->clj (.getTxId finalized))
     :fee         (->clj (.getFee built))
     :finalized   finalized}))

(defn txn!
  "So the transaction flow here is:

  1. Construct a TransactionRequest, which specifies the high-level things we'd
     like to do.
  2. Ask the server to turn that into a concrete transaction which involves
     specific coins.
  3. Sign that transaction (I think) using a keypair. The API calls this a
     'finalized' transaction, but it's not actually final, because we then have
     to:
  4. Submit that 'finalized' transaction to the server to be finalized... more?
  5. That returns a transaction ID, which we then have to stamp back onto our
     local finalized transaction
  6. And with that finalized + ID-stamped transaction, we can submit it

  This is just for the request; making sure the transaction went through is a
  separate problem.

  This function does all of these steps in sequence. It takes a client, an
  ECKeyPair, a string-able message, and a series of actions (as vectors; see
  build-txn). It constructs, finalizes, and submits a transaction, returning a
  map of:

    {:id      tx-id
     :status  A deref-able which checks the eventual status of the transaction}"
  [^RadixApi client key-pair message actions]
  (let [prepped (prep-txn client key-pair message actions)]
    (submit-txn! client (:finalized prepped))))

(defn drain!
  "Tries to empty (or close to it) one account into another. Helpful for
  cleaning up temporary accounts at the end of a test on Stokenet. to-address
  can be anything convertible to an address. Synchronous."
  [client test from-key-pair to-address]
  (let [ops (->> (token-balances client from-key-pair)
                 :balances
                 (keep (fn [{:keys [rri amount]}]
                         ;(info :rri rri :amount amount)
                         (let [transfer-amount (- amount
                                                  (* 2 (u/fee-scale test)))]
                           ;(info :transfer-amount transfer-amount)
                           (when (pos? transfer-amount)
                             [:transfer from-key-pair to-address
                              transfer-amount rri])))))]
    (when (seq ops)
      ; (info :actions ops)
      (let [txn (txn! client from-key-pair "drain" ops)]
        (assert+ (= :confirmed (:status @(:status txn)))
                 (assoc @(:status txn)
                        :type :txn-unconfirmed))))))

(defn paginated
  "Takes a function (chunk cursor), where cursor is an Optional<Cursor>, where
  `chunk` returns something which can be coerced via ->clj to a map of {:cursor
  Cursor, ...}, representing a page of results. Also takes a function of each
  chunk which extracts results from it. Yields a lazy sequence of results."
  ([chunk results]
   ; A bit weird: here the absence of a cursor means we're *starting* the
   ; sequence; later the absence of a cursor means the sequence is *ending*.
   (lazy-seq
     ; Fetch initial chunk
     (let [c (->clj (chunk (OptionalLong/empty)))]
       (concat (results c)
               (paginated chunk results (:cursor c))))))
  ([chunk results cursor]
   ; Without a cursor, we're at the end of the sequence. Right? Right? Argh why
   ; are there two different ways of paginating :-O
   (when cursor
     (lazy-seq
       (let [c (-> cursor OptionalLong/of chunk ->clj)]
         (concat (results c)
                 (paginated chunk results (:cursor c))))))))

(def history-chunk-size
  "What's the `size` parameter we pass to each transaction history request?"
  32)

(defn txn-history
  "Takes an account address, a size (perhaps a number of transactions?).
  Returns a lazy seq of transactions from that account's history, in reverse
  chronological order."
  ([^RadixApi client address]
   (txn-history client address history-chunk-size))
  ([^RadixApi client address size]
   (paginated (fn chunk [cursor]
                (-> client .account (.history address size cursor)))
              :txns)))

(def validator-chunk-size
  "How many validators do we fetch at a time, by default?"
  128)

(defn validators
  "Takes a Radix Client and returns a lazy seq of validator states."
  [^RadixApi client]
  (paginated (fn chunk [cursor]
               (-> client .validator (.list validator-chunk-size cursor)))
             :validators))

(defn await-initial-convergence
  "When Radix first starts up, there's a period of ~80 seconds where it's not
  really ready to process transactions yet, but it *is* responding to API
  requests. We use this function to perform an initial no-op transaction as a
  part of DB setup, and ensure that the DB's all ready to go."
  [test node]
  (info "Waiting for initial convergence...")
  (let [client (open test node)]
    ; We choose key 5 here so as not to interfere with key 1, which the
    ; workload uses extensively. This simplifies our checker: key 1 won't have
    ; any initial balance-changing transactions to factor into account.
    (let [k1 (key-pair 5)
          k2 (key-pair 5)
          addr1 (->account-address k1)
          addr2 (->account-address k2)
          ; Before the API is ready, we'll get HTML error pages from nginx
          ; instead of the JSON the client expects.
          rri   (util/await-fn
                  (fn get-rri [] (:rri (native-token client)))
                  {:retry-interval 1000
                   :log-interval 10000
                   :log-message "Waiting for API to become available."})]
      ; So what I've been seeing in tests is that the first node is able to
      ; complete this process fine in ~80 seconds, but *other* nodes (e.g. n2,
      ; n3, ...) get stuck indefinitely, even when the cluster appears to be
      ; healthy. On a hunch, I'm going to try giving up on transactions and
      ; submitting fresh ones--maybe nodes, I don't know, accept transactions
      ; and then reset their mempool during the cluster join process?
      (util/await-fn
        (fn try-a-txn []
          (-> client
              (txn! k1 "initial await" [[:transfer addr1 addr2 1 rri]])
              :status
              (deref 20000 nil)
              (assert+
                {:type :txn-timeout})))
        {:timeout        300000
         :retry-interval 0
         :log-interval   20000
         :log-message    "Waiting for transactions to start working"})))
  (info "Consensus ready!"))

;; JSONRPC methods not supported by the normal client

(defn dev-rpc!
  "Makes a call to the /developer JSONRPC endpoint. Takes a node, a method name
  (e.g. :index.get_transactions) and a parameter map."
  [node method params]
  (let [res (http/post (str "https://" node "/developer")
                       {:form-params {:jsonrpc "2.0"
                                      :id      1
                                      :method (name method)
                                      :params params}
                        ; TODO: move this somewhere shared with jepsen.db
                        :basic-auth   ["admin" "jepsenpw"]
                        :insecure?    true
                        :content-type :json
                        :as           :json})]
    (when-let [e (:error (:body res))]
      (throw+ (assoc e :type :radix-dlt/error)))
    (:result (:body res))))

(defn raw-transactions
  "Lists the set of raw transactions directly from the index."
  [node]
  (dev-rpc! node :index.get_transactions {:offset 1 :limit Integer/MAX_VALUE}))

(defn raw-balances
  "Lists the balances of every account."
  [node]
  (dev-rpc! node :developer.query_resource_state
            {:prefix "06" ; TOKENS_IN_ACCOUNT
             :groupBy "owner"
             :query {:type "resource"
                     :value "01" ; XRD
                     }}))

(defmacro with-errors
  "Takes an operation and a body. Evaluates body, converting known exceptions to fail/info operations."
  [op & body]
  `(try+ ~@body
         (catch [:type :timeout] e#
           (assoc ~op :type :info, :error :timeout))
         (catch [:type :txn-prep-failed] e#
           (assoc ~op :type :fail
                  :error [:txn-prep-failed (.getMessage (:cause ~'&throw-context))]))
         (catch [:type :radix-dlt/failure, :code 1004] e#
           (condp re-find (:message e#)
             #"header parser received no bytes"
             (assoc ~op :type :info, :error [:header-empty (:message e#)])

             #"Connection refused"
             ; Slow these down juust a tad so we can focus on live nodes.
             (do (Thread/sleep 1000)
                 (assoc ~op :type :fail, :error [:conn-refused (:message e#)]))

             #"request timed out"
             (assoc ~op :type :info, :error [:request-timed-out (:message e#)])

             (throw+ e#)))
         ; Not sure why there are two codes for substate not found...
         (catch [:type :radix-dlt/failure, :code -2014] e#
           (assoc ~op :type :fail, :error [:substate-also-not-found
                                           (:message e#)]))
         (catch [:type :radix-dlt/failure, :code 1500] e#
           (assoc ~op :type :fail, :error [:substate-not-found
                                           (:message e#)]))
         (catch [:type :radix-dlt/failure, :code 1604] e#
           (assoc ~op :type :fail, :error [:parse-error (:message e#)]))
         (catch [:type :radix-dlt/failure, :code 2515] e#
           (assoc ~op :type :fail, :error :insufficient-balance))

         ; clj-http errors when we call things directly via jsonrpc
         (catch [:status 404] e#
           (assoc ~op :type :fail, :error :not-found))
         (catch [:status 500] e#
           (assoc ~op :type :info, :error [:http-500 (:body e#)]))))
