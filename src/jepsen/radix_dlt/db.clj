(ns jepsen.radix-dlt.db
  "Sets up Radix DLT clusters."
  (:require [byte-streams :as bs]
            [cheshire.core :as json]
            [clojure [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [clj-cbor.core :as cbor]
            [clj-http.client :as http]
            [dom-top.core :as dt :refer [assert+]]
            [jepsen [control :as c]
                    [core :as jepsen :refer [primary]]
                    [db :as db]
                    [fs-cache :as cache]
                    [util :as util :refer [parse-long pprint-str]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.radix-dlt.client :as rc]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util Base64)))

(def broadcast-port 30000)
(def listen-port 30001)
(def node-api-port 3334)
(def client-api-port 8081)

(def network-id
  "This is what radixdlt-core/docker/Dockerfile.core uses"
  99)

(def dir
  "Where do we install Radix stuff?"
  "/opt/radix")

(def log-file
  "Where do we put console logs?"
  (str dir "/radix.log"))

(def pid-file
  "Where do we put the daemon's pidfile?"
  (str dir "/radix.pid"))

(def dist-dir
  "Where do we extract Radix's package to?"
  dir)

(def data-dir
  "Where does Radix data live?"
  (str dir "/data"))

(def universe-file
  "Where do we store universe.json?"
  (str dir "/universe.json"))

(def secret-dir
  "Where do we store secret keys?"
  (str dir "/secret"))

(def keystore
  "Where do we store the validator.ks keystore?"
  (str secret-dir "/validator.ks"))

(def bin-dir
  "Where do binaries live?"
  (str dir "/bin"))

(def password
  "The password we use for our secrets"
  "jepsenpw")

(def radixnode
  "The full path to the radixnode binary"
  (str dir "/radixnode"))

(def radix-git-dir
  "Where do we clone the radixdlt git repo?"
  (str dir "/radixdlt"))

(defn node-index
  "Takes a node in a test and returns its 0-indexed place in the nodes list. We
  use this to extract the appropriate private key for each validator."
  [test node]
  (let [i (.indexOf (:nodes test) node)]
    (assert (not (neg? i)))
    i))

(defn install-radix!
  "Installs Radix directly."
  [test]
  (c/su (debian/install [:rng-tools :openjdk-11-jdk :unzip])
        (try+ (c/exec :rngd :-r "/dev/random")
              (catch [:exit 10] _
                ; Already running
                ))
        (let [url (if-let [zip (:zip test)]
                    (let [remote (str "/tmp/jepsen/radix.zip")]
                      (c/exec :mkdir :-p "/tmp/jepsen")
                      (c/upload zip remote)
                      (str "file://" remote))
                    (str "https://github.com/radixdlt/radixdlt/releases/download/"
                       (:version test) "/radixdlt-dist-"
                       (:version test) ".zip"))]
          (cu/install-archive! url dist-dir))

        ; Create data and secret dirs
        (c/exec :mkdir :-p data-dir)
        (c/exec :mkdir :-p secret-dir)))

(defn install-nginx!
  "Installs nginx and deletes the default sites."
  [test]
  (c/su
    (debian/install [:nginx :apache2-utils])
    (c/exec :rm :-rf (c/lit "/etc/nginx/sites-*"))))

(defn install!
  "Installs a full Radix node."
  [test]
  (install-radix! test)
  (install-nginx! test))

(defn parse-universe-exports
  "Takes a big string like

export RADIXDLT_VALIDATOR_0_PRIVKEY=kMbmxZri+FCC0ktmWbwJ/zJVFPXjRgaRHmDBmE3L+b0=
export RADIXDLT_VALIDATOR_1_PRIVKEY=p7vk1dMv5A0agIbcgB6TWdhKnyunAJTFW9bK6ZiSCHg=
export RADIXDLT_VALIDATOR_2_PRIVKEY=UCZRvnk5Jm9hEbpiingYsx7tbjf3ASNLHDf3BLmFaps=

  and returns a parsed Clojure structure--see gen-universe for details."
  [exports]
  (->> (str/split exports #"\n")
       ; A line here is like "RADIXDLT_UNIVERSE_TYPE=DEVELOPMENT". We build up
       ; a universe map line by line.
       (reduce (fn [u line]
                 (let [[match k v] (re-find #"export RADIXDLT_(\w+)=(.*)"
                                            line)]
                   ;(info :k k :v v)
                   (cond (= k "GENESIS_TXN")
                         (assoc u :genesis-txn v)

                         :else
                         ; something like RADIXDLT_VALIDATOR_0_PRIVKEY
                         (if-let [[m k index subkey]
                                  (re-find #"(\w+?)_(\d+?)_(\w+)" k)]
                           (assoc-in u
                                     [(keyword (str (.toLowerCase k) "s"))
                                      (parse-long index)
                                      (keyword (.toLowerCase subkey))]
                                     v)
                           ; Dunno!
                           (assoc u (keyword k) v)))))
               {:stakers    []
                :validators []})))

(defn validate-universe-exports
  "Ensures that a parsed universe exports map has the right structure"
  [exports]
  (info :exports (pprint-str exports))
  (assert (map? exports))
  (let [{:keys [validators
                genesis-txn]} exports]
    (assert (string? genesis-txn))
    (doseq [v validators]
      (assert (string? (:privkey v)))
      (assert (string? (:pubkey v)))))
  exports)

(defn gen-universe
  "Generates an initial universe. Returns a map of:

    {:validators      [{:privkey \"...\"} ...]
     :genesis-txn     \"hex string\"}"
  [test]
  (info "Generating universe")
  (c/su
    ; Clone repo
    (when-not (cu/exists? radix-git-dir)
      (debian/install [:git])
      (c/exec :git :clone "https://github.com/radixdlt/radixdlt.git"
              radix-git-dir))
    (c/cd radix-git-dir
          ; Check out our specific version
          (c/exec :git :fetch)
          (c/exec :git :reset :--hard (:version test))
          (c/exec :git :clean :-fdx)

          ; Use Gradle to build a dev universe
          (let [out (c/exec "./gradlew"
                            :-q
                            :-P (str "validators=" (:validators test))
                            ":radixdlt:clean"
                            ":radixdlt:generateDevUniverse")]
            ;(info "dev universe:\n" out)
            (-> out
                parse-universe-exports
                validate-universe-exports)))))

(defn get-universe
  "Returns a cached universe, or generates a new one."
  [test]
  (let [path [:radixdlt (:version test) :universe]]
    (cache/locking path
      (or (cache/load-edn path)
          (cache/save-edn! (gen-universe test) path)))))

(defn gen-keys!
  "Generates keys for the node, writing them to secret-dir"
  []
  (c/exec (str bin-dir "/keygen")
          :--keystore keystore
          :--password password))

(defn env
  "The environment map for a node."
  [test node universe]
  ;(info :universe (pprint-str universe))
  (assert (= (count (:nodes test)) (:validators test))
          "We don't know how to start a cluster where some nodes aren't validators yet.")
  {:JAVA_OPTS "-server -Xms3g -Xmx3g -XX:+HeapDumpOnOutOfMemoryError -Djavax.net.ssl.trustStore=/etc/ssl/certs/java/cacerts -Djavax.net.ssl.trustStoreType=jks -Djava.security.egd=file:/dev/urandom -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
   :RADIX_NODE_KEYSTORE_PASSWORD password
   :RADIXDLT_UNIVERSE_ENABLE true
   :RADIXDLT_NODE_KEY (dt/assert+
                        (get-in universe
                                [:validators
                                 (node-index test node)
                                 :privkey])
                        {:type       :no-node-key
                         :node       node
                         :index      (node-index test node)
                         :validators (:validators universe)})})

(defn config
  "The configuration map for a node."
  [test node universe]
  (cond-> {:ntp                        false
           :network.id                 network-id
           :network.genesis_txn        (:genesis-txn universe)
           :network.p2p.default_port   listen-port
           :network.p2p.listen_port    listen-port
           :network.p2p.broadcast_port broadcast-port
           :cp.port                    client-api-port
           :api.node.port              node-api-port
           ; The docs (https://docs.radixdlt.com/main/radix-nodes/running-a-full-node-standalone.html#_configuration) say to use host.ip, but the example config file (https://github.com/radixdlt/radixdlt/blob/ba7a42999d82a71696209e275c53cb99c6916114/radixdlt-core/radixdlt/src/main/resources/default.config#L95) says network.host_ip; maybe we need to change this?
           :network.host_ip            (cn/local-ip)
           :db.location                data-dir
           :api.account.enable         true
           :api.archive.enable         true
           :api.developer.enable       true
           :api.developer.transaction_index.enable true
           :api.health.enable          true
           :api.system.enable          true
           :api.construction.enable    true
           :log.level                  "debug"

           ; Timeout tuning. Our goal is to reduce the time it takes for
           ; initial cluster convergence, and to speed recovery from faults.
           ; See radixdlt-core/radixdlt/src/main/resources/default.config for
           ; documentation. I'm using grep -r RuntimeProperties radixdlt-core/
           ; to find property names and defaults.

           ; Specifies how often the discovery round is triggered (in
           ; milliseconds). Default: 120000
           :network.p2p.discovery_interval 10000

           ; Specifies how often a ping message is sent. Default: 10000
           :network.p2p.peer_liveness_check_interval 1000

           ; A timeout for receiving a pong message. Default: 5000
           :network.p2p.ping_timeout 500


           ; Old parameters? They changed everything in 1.0-beta.39

           ; Time to wait for a connection to a discovery oracle to complete
           ; before abandoning the attempt, in milliseconds
           :network.discovery.connection.timeout 5000 ; default 60k
           ; Time to wait for data to be returned from a discovery oracle
           ; before abandoning the connection, in milliseconds.
           :network.discovery.read.timeout 5000 ; default 60k

           ; Time between querying a random known host for its peer list, in
           ; milliseconds. Every specified time period, a random peer is
           ; queried for the peers that it knows of in order to keep the list
           ; of peers synchronised between nodes.
           :network.peers.broadcast.interval 3000 ; default 30k
           ; Time to wait on system start before attempting to query for peers
           ; lists from known hosts, in milliseconds.
           :network.peers.broadcast.delay 6000 ; default 60k

           ; Time between selecting a number of random peers and attempting to
           ; ping/pong them for liveness, in milliseconds.
           :network.peers.probe.interval 1000 ; default 1000
           ; Time to wait on system start before attempting to ping/pong known
           ; peers to check liveness, in milliseconds.
           :network.peers.probe.delay 0 ; default 0
           ; Time to consider a peer unresponsive after an unacknowledged ping,
           ; in milliseconds.
           :network.peers.probe.timeout 2000 ; default 20k
           ; Individual nodes will not be ping/ponged more frequently than this
           ; duration, in milliseconds
           :network.peers.probe.frequency 3000 ; default 30k

           ; Time to wait on between (sic) sending heartbeat messages to all
           ; known peers, in milliseconds.
           :network.peers.heartbeat.interval 1000 ; default 10k
           ; Time to wait on system start before attempting to heartbeat known
           ; peers to check liveness, in milliseconds.
           :network.peers.heartbeat.delay 1000 ; default 10k

           ; Time to wait on between querying discovery nodes, in milliseconds.
           :network.peers.discover.interval 1000 ; default 10k
           ; Time to wait on system start before attempting to query discovery
           ; nodes, in milliseconds.
           :network.peers.discover.delay 1000 ; default 1k
           }
    ; All non-primary nodes get the primary as their seed
    (not= node (primary test))
    (assoc :network.p2p.seed_nodes
           (let [n (primary test)]
             (str "radix://"
                  (assert+
                    (get-in universe
                            [:validators
                             (node-index test n)
                             :pubkey])
                    {:type :no-node-in-universe-validators
                     :node node
                     :validators (:validators universe)})
                  "@"
                  (cn/ip n)
                  ":" broadcast-port
                  )))))

(defn config-str
  "The configuration file string for a node."
  [test node universe]
  (->> (config test node universe)
       (map (fn [[k v]] (str (name k) "=" v)))
       (str/join "\n")))

(defn write-config!
  "Writes out the config file for a node."
  [test node universe]
  (cu/write-file! (config-str test node universe)
                  (str dir "/default.config")))

(defn configure-nginx!
  "Sets up the nginx config files for Radix, generates TLS certs, etc."
  [test node]
  (c/su
    ; Install config files
    (let [url (str "https://github.com/radixdlt/radixdlt-nginx/releases/download/"
                   (:version test) "/radixdlt-nginx-archive-conf.zip")
          ; We have to rename files specially here, so we can't just extract
          ; over /etc/nginx directly.
          tmp "/tmp/radix/nginx"]
      (cu/install-archive! url tmp)
    (c/exec :cp :-r (str tmp "/conf.d") "/etc/nginx/")
    (c/exec :mv (str tmp "/nginx-archive.conf") "/etc/nginx/nginx.conf"))

    ; Create cache dir
    (c/exec :mkdir :-p "/var/cache/nginx/radixdlt-hot")

    ; Create TLS certs
    (c/exec :rm :-rf "/etc/nginx/secrets")
    (c/exec :mkdir "/etc/nginx/secrets")
    (c/exec :openssl :req :-nodes :-new :-x509 :-nodes :-subj (str "/CN=" node)
            :-keyout "/etc/nginx/secrets/server.key"
            :-out    "/etc/nginx/secrets/server.pem")
    ; They suggest 4096 but we wanna go fast, and security doesn't matter here
    (c/exec :openssl :dhparam :-out "/etc/nginx/secrets/dhparam.pem" 1024)
    ; securrrrre
    (c/exec :htpasswd :-bc "/etc/nginx/secrets/htpasswd.admin"
            "admin" password)))

(defn configure!
  "Configures Radix and nginx."
  [test node universe]
  ; We don't generate a keystore; all our keys are generated as a part of
  ; the initial universe, and passed in via env vars.
  ; (gen-keys!)
  (write-config! test node universe)
  (configure-nginx! test node))

(defn restart-nginx!
  "(re)starts nginx"
  []
  (c/su (c/exec :systemctl :restart :nginx)))

(defn admin-http-opts
  "Returns common HTTP options for making calls to the admin HTTP interface."
  []
  {:basic-auth ["admin" password]
   :as         :json
   :insecure?  true})

(defn rpc!
  "Makes a JSONRPC call to the given path on the given node."
  [node path rpc-req]
  (info "Requesting" (str "https://" node path))
  (let [res (http/post (str "https://" node path)
                       (assoc (admin-http-opts)
                              :content-type :json
                              :form-params (assoc rpc-req
                                                  :jsonrpc "2.0"
                                                  :id 1)))]
    (:body res)))

(defn validation-node-info
  "Returns the node's current validator information."
  [node]
  (rpc! node "/validation" {:method :validation.get_node_info
                            :params []}))

(defn await-health
  "Blocks until we can fetch the /health JSON data"
  [node]
  (let [res (util/await-fn
              (fn []
                (let [res (:body (http/get (str "https://" node "/health")
                                           (admin-http-opts)))]
                  (when (= "BOOTING" (:status res))
                    (throw+ {:type ::still-booting}))
                  res))
              {:log-interval 30000
               :log-message (str "Waiting for https://" node "/health")})]
    (dt/assert+ (= "UP" (:status res))
                {:type ::malformed-node-data?
                 :res  res
                 :node node})))

(defn wipe!
  "Deletes all data & config files (but not logs)"
  []
  (c/su
    (c/exec :rm :-rf (str dir "/data") (str dir "/default.config"))))

(defn validator-address->node
  "Takes a DB and string representation of a validator address (e.g.
  vb1qwyxnktxunxsvav0ac769m52tagzwy66kckzu8eftl0mew4pnpfj7zzrdty) and converts
  it to a node name like \"n1\"."
  [db validator-address]
  (assert+ (->> db :validators deref vals
                (filter (comp #{validator-address} :address))
                first
                :node)
           {:type       :no-such-validator
            :address    validator-address
            :validators @(:validators db)}))

(defrecord DB [; A promise of a Universe structure
               universe
               ; An atom containing a map of node names to maps like
               ; {:node     "n1"
               ;  :key-pair ECKeyPair
               ;  :validator-address "vb..."}
               validators]
  db/DB
  (setup! [this test node]
    (when (= node (primary test))
      (deliver universe (get-universe test))
      ;(info :universe (pprint-str @uni))
      )

    (install! test)
    (configure! test node @universe)
    (restart-nginx!)
    (db/start! this test node)
    (await-health node)
    ; Hack: even though the /node is ready, the client api might take
    ; longer, and we'll get weird parse errors when HTML shows up instead
    ; of JSON
    (rc/await-initial-convergence test node))

  (teardown! [this test node]
    (db/kill! this test node)
    (c/su (c/exec :rm :-rf dir)))

  db/LogFiles
  (log-files [this test node]
    [log-file
     (str dir "/logs/radixdlt-core.log")
     (str dir "/default.config")
     ])

  db/Process
  (start! [this test node]
    (c/su
      ; Squirrel away this node's validator information
      (let [universe @universe
            key-pair (-> universe
                         (get-in [:validators (node-index test node) :privkey])
                         rc/private-key-str->key-pair)]
        (swap! validators assoc node
               {:node     node
                :key-pair key-pair
                :address  (str (rc/->validator-address key-pair))})
        ;(info :validators @validators)
        (cu/start-daemon!
          {:chdir   dir
           :logfile log-file
           :pidfile pid-file
           :env     (env test node universe)}
          (str bin-dir "/radixdlt")
          ; No args?
          ))))

  (kill! [this test node]
    (c/su
      ; The script immediately execs, so pidfile is actually useless here
      (cu/stop-daemon! "java" pid-file)))

  db/Pause
  (pause! [this test node]
    (cu/grepkill! :STOP "java"))

  (resume! [this test node]
    (cu/grepkill! :CONT "java"))

  db/Primary
  (setup-primary! [db test node])
  (primaries [db test]
    ; Our notion of a primary node is one with a supermajority of the total
    ; stake.
    (try+
      (let [client      (rc/open test (rand-nth (:nodes test)))
            validators  (rc/validators client)
            total-stake (->> validators
                             (map :total-delegated-stake)
                             (reduce + 0))]
        (info :total-stake total-stake)
        ; If there's no stake, we can't do anything here.
        (when-not (zero? total-stake)
          (let [threshold (* 2/3 total-stake)
                supermaj (->> validators
                              (filter (comp (partial < threshold)
                                            :total-delegated-stake))
                              first)]
            (info :supermaj supermaj)
            (when supermaj
              [(validator-address->node db (:address supermaj))])))))))

(defn db
  "The Radix DLT database."
  []
  (DB. (promise) (atom {})))
