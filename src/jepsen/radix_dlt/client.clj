(ns jepsen.radix-dlt.client
  "Wrapper around the Java client library.

  See https://github.com/radixdlt/radixdlt/blob/1.0-beta.35.1/radixdlt-java/radixdlt-java/src/test/java/com/radixdlt/client/lib/impl/SynchronousRadixApiClientTest.java for some test example code."
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :as dt :refer [assert+]]
            [jepsen [util :as util :refer [pprint-str]]]
            [potemkin :refer [def-derived-map]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (com.radixdlt.client.lib.api AccountAddress
                                        ActionType
                                        NavigationCursor
                                        RadixApi
                                        TransactionRequest
                                        TransactionRequest$TransactionRequestBuilder
                                        TxTimestamp
                                        ValidatorAddress)
           (com.radixdlt.client.lib.dto ActionDTO
                                        BalanceDTO
                                        BuiltTransactionDTO
                                        FinalizedTransaction
                                        NetworkIdDTO
                                        TokenBalancesDTO
                                        TokenInfoDTO
                                        TransactionDTO
                                        TransactionHistoryDTO
                                        TransactionStatus
                                        TransactionStatusDTO
                                        TxDTO)
           (com.radixdlt.client.lib.impl SynchronousRadixApiClient)
           (com.radixdlt.client.lib.network HttpClients)
           (com.radixdlt.crypto ECKeyPair
                                ECPublicKey)
           (com.radixdlt.identifiers AID)
           (com.radixdlt.utils Ints
                               UInt256)
           (com.radixdlt.utils.functional Failure
                                          Result)
           (java.util Optional)
           (java.util.function Function)))

(defprotocol ToClj
  "Transforms RadixDLT client types into Clojure structures."
  (->clj [x]))

(def-derived-map ActionMap [^ActionDTO a]
  :type       (->clj (.getType a))
  :from       (->clj (.getFrom a))
  :to         (->clj (.getTo a))
  :validator  (->clj (.getValidator a))
  :amount     (->clj (.getAmount a))
  :rri        (->clj (.getRri a)))

(def-derived-map TokenInfoMap [^TokenInfoDTO t]
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

  ActionDTO
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

  BalanceDTO
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

  NetworkIdDTO
  (->clj [x]
    (.getNetworkId x))

  Optional
  (->clj [x] (->clj (.orElse x nil)))

  Result
  (->clj [x] (->clj (unres x)))

  TokenBalancesDTO
  (->clj [x]
    {:owner     (->clj (.getOwner x))
     :balances  (map ->clj (.getTokenBalances x))})

  TokenInfoDTO
  (->clj [x] (->TokenInfoMap x))

  TransactionHistoryDTO
  (->clj [x]
    {:cursor (.orElse (.getCursor x) nil)
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
  (->clj [x]
    (.toString x))

  Object
  (->clj [x] x)

  nil
  (->clj [x] nil))

(defprotocol ToAccountAddress
  "Converts strings and AccountAddresses back into AccountAddresses."
  (->account-address [x]))

(extend-protocol ToAccountAddress
  AccountAddress
  (->account-address [x] x))

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

;; Keypair management

(defn ^ECKeyPair key-pair
  "Takes an integer id and returns a keypair. These correspond to hardcoded
  keypair IDs in the dev universe: valid inputs are 1-5.

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

(defn ^AccountAddress key-pair->account-address
  "Turns an ECKeyPair into an AccountAddress."
  [^ECKeyPair key-pair]
  (AccountAddress/create (public-key key-pair)))

(defn ^SynchronousRadixApiClient open
  "Opens a connection to a node."
  [node]
  (->clj (SynchronousRadixApiClient/connect
           (str "https://" node)
           (HttpClients/getSslAllTrustingClient))))

(defn address
  "Constructs a RadixAddress."
  [^RadixApi client]
  (unres (.getAddress client)))

;; API operations

(defn native-token
  "Gets the native token of this client."
  [^RadixApi client]
  (->clj (.nativeToken client)))

(defn network-id
  "Returns the Network ID of this client."
  [^RadixApi client]
  [client]
  (->clj (.networkId client)))

(defn token-balances
  "Looks up the token balances of a given address."
  [^RadixApi client ^AccountAddress address]
  (->clj (.tokenBalances client address)))

(defn txn-request
  "Builds a new transaction request. Takes a message string and a seq of
  actions of the form...

    [[:transfer from to amount rri]
     [:stake from validator amount]
     [:unstake from validator amount]]"
  [message actions]
  (.build ^TransactionRequest$TransactionRequestBuilder
    (reduce (fn [^TransactionRequest$TransactionRequestBuilder builder action]
              (case (first action)
                :transfer (let [[_ from to amount rri] action]
                            (.transfer builder from to (uint256 amount) rri))

                :stake    (let [[_ from validator amount] action]
                            (.stake builder from validator (uint256 amount)))

                :unstake  (let [[_ from validator amount] action]
                            (.unstake builder from validator
                                      (uint256 amount)))))
            (.. (TransactionRequest/createBuilder)
                (message (str message)))
            actions)))

(defn build-txn
  "Constructs a transaction from a message and actions. See
  `transaction-request`."
  [^RadixApi client message actions]
  (unres (.buildTransaction client (txn-request message actions))))

(defn ^FinalizedTransaction local-finalize-txn
  "Takes a BuiltTransactionDTO and finalizes it with a given key pair."
  [^BuiltTransactionDTO txn ^ECKeyPair key-pair]
  (.toFinalized txn key-pair))

(defn ^TxDTO remote-finalize-txn
  "Takes a 'finalized' transaction and sends it to the server for... even more
  finalizing? Returns a map."
  [^RadixApi client ^FinalizedTransaction txn]
  (unres (.finalizeTransaction client txn)))

(defn ^FinalizedTransaction finalize-txn
  "Takes a built txn and finalizes it using the given keypair. Obtains a txid
  from the server, and returns the FinalizedTransaction, ID and all."
  [^RadixApi client built-txn key-pair]
  (let [finalized (local-finalize-txn built-txn key-pair)
        txid      (.getTxId (remote-finalize-txn client finalized))]
    (.withTxId finalized txid)))

(defn txn-status
  "Checks the status of a transaction. Transaction IDs can be given either as
  an AID or a String. Returns a map of:

    {:id     tx-id
     :status :confirmed, :pending, or :failed}"
  [^RadixApi client txid]
  (-> client
      (.statusOfTransaction (->aid txid))
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
(defmethod print-method TxnStatus [ts writer]
  (.write writer (str ts)))

(defn submit-txn!
  "Submits a finalized transaction with an ID for processing. Returns a map:

    {:id      tx-id, as a string
     :status  A deref-able of the eventual status of the transaction.}"
  [^RadixApi client ^FinalizedTransaction txn]
  (let [tx (unres (.submitTransaction client txn))
        id (.getTxId tx)]
    {:id     (->clj id)
     :status (TxnStatus. client id)}))

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
  (as-> (build-txn client message actions) txn
        (finalize-txn client txn key-pair)
        (submit-txn! client txn)))

(def history-chunk-size
  "What's the `size` parameter we pass to each transaction history request?"
  128)

(defn txn-history
  "Takes an account address, a size (perhaps a number of transactions?).
  Returns a lazy seq of transactions from that account's history, in reverse
  chronological order."
  ([^RadixApi client address]
   (txn-history client address history-chunk-size))
  ([^RadixApi client address size]
   ; A bit weird: here the absence of a cursor means we're *starting* the
   ; sequence; later the absence of a cursor means the sequence is *ending*.
   (lazy-seq
     ; Fetch initial chunk
     (let [chunk (-> client
                     (.transactionHistory address size (Optional/empty))
                     ->clj)]
       (info :initial-chunk (->> chunk :txns count)
             :ids      (->> chunk :txns (map :id))
             :messages (->> chunk :txns (map :message)))
       (concat (:txns chunk)
               (txn-history client address size (:cursor chunk))))))
  ([^RadixApi client ^AccountAddress address size cursor]
   ; Without a cursor, we're at the end of the sequence
   (when (and cursor
              ; It looks like they signal the end with an empty string value?
              (not= "" (.value cursor)))
     (lazy-seq
       (let [chunk (-> client
                       (.transactionHistory
                         address size (Optional/of cursor))
                       ->clj)]
         (info :later-chunk (->> chunk :txns count)
               :cursor (.value cursor)
               :ids      (->> chunk :txns (map :id))
               :messages (->> chunk :txns (map :message)))
         (concat (:txns chunk)
                 (txn-history client address size (:cursor chunk))))))))

(defn await-initial-convergence
  "When Radix first starts up, there's a period of ~80 seconds where it's not
  really ready to process transactions yet, but it *is* responding to API
  requests. We use this function to perform an initial no-op transaction as a
  part of DB setup, and ensure that the DB's all ready to go."
  [node]
  (info "Waiting for initial convergence...")
  (let [client (open node)]
    (let [k1 (key-pair 1)
          k2 (key-pair 2)
          addr1 (key-pair->account-address k1)
          addr2 (key-pair->account-address k2)
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
