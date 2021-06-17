(ns jepsen.radix-dlt.db
  "Sets up Radix DLT clusters."
  (:require [byte-streams :as bs]
            [cheshire.core :as json]
            [clojure [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [clj-cbor.core :as cbor]
            [clj-http.client :as http]
            [dom-top.core :as dt]
            [jepsen [control :as c]
                    [core :as jepsen :refer [primary]]
                    [db :as db]
                    [util :as util :refer [parse-long pprint-str]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util Base64)))

(def broadcast-port 30000)
(def listen-port 30001)
(def node-api-port 3334)
(def client-api-port 8081)

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

(defn upgrade-to-bullseye!
  "Upgrades Debian Bustebr to Bullseye.

  Radixnode is built against glibc 2.29, and Buster uses 2.28. Pyinstaller's
  binaries don't package glibc
  (https://pyinstaller.readthedocs.io/en/stable/usage.html?highlight=glibc#making-gnu-linux-apps-forward-compatible),
  so we need to bump nodes to Bullseye, which has glibc 2.31. We're scripting
  this so that Radix (and third parties) can run it on existing Buster
  clusters, which is what's packaged on AWS."
  []
  (when (re-find #"Debian GNU/Linux 10" (c/exec :cat "/etc/issue"))
    (info "Upgrading Debian Buster to Bullseye")
    (c/su
      (c/exec :sed :-i "s/buster/bullseye/g" "/etc/apt/sources.list")
      ; Comment out security sources, since they're not available for testing
      (c/exec :sed :-i :-e "/security/ s/^#*/#/" "/etc/apt/sources.list")
      (debian/update!)
      (c/exec :env "DEBIAN_FRONTEND=noninteractive" :apt :upgrade :-y)
      (c/exec :env "DEBIAN_FRONTEND=noninteractive" :apt :full-upgrade :-y))))

(defn install-radixnode!
  "Downloads and installs the radixnode script"
  [test]
  (info "Installing Radix")
  (c/su
    (c/exec :mkdir :-p dir)
    (c/cd dir
          (let [script   "radixnode-ubuntu-20.04"
                url (str "https://github.com/radixdlt/node-runner/releases/download/"
                         (:node-runner-version test) "/" script)]
            (cu/wget! url)
            (c/exec :mv script radixnode)
            (c/exec :chmod :+x radixnode)))))

(defn install-radix!
  "Installs Radix directly."
  [test]
  (c/su (debian/install [:rng-tools :openjdk-11-jdk :unzip])
        (try+ (c/exec :rngd :-r "/dev/random")
              (catch [:exit 10] _
                ; Already running
                ))
        (let [url (str "https://github.com/radixdlt/radixdlt/releases/download/"
                       (:version test) "/radixdlt-dist-"
                       (:version test) ".zip")]
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
export RADIXDLT_STAKER_0_PRIVKEY=kMbmxZri+FCC0ktmWbwJ/zJVFPXjRgaRHmDBmE3L+b0=
export RADIXDLT_STAKER_1_PRIVKEY=p7vk1dMv5A0agIbcgB6TWdhKnyunAJTFW9bK6ZiSCHg=

  and returns a parsed Clojure structure--see gen-universe for details."
  [exports]
  (->> (str/split exports #"\n")
       ; A line here is like "RADIXDLT_UNIVERSE_TYPE=DEVELOPMENT". We build up
       ; a universe map line by line.
       (reduce (fn [u line]
                 (let [[match k v] (re-find #"export RADIXDLT_(\w+)=(.*)"
                                            line)]
                   ;(info :k k :v v)
                   (cond (= k "UNIVERSE")
                         ; This is stored as base64-encoded CBOR
                         (assoc u
                                :universe-str v
                                :universe     (-> (Base64/getDecoder)
                                                  (.decode v)
                                                  cbor/decode))

                         (= k "UNIVERSE_TYPE")
                         (assoc u :universe-type (keyword (.toLowerCase v)))

                         (= k "UNIVERSE_TOKEN")
                         (assoc u :universe-token v)

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
                           [k v]))))
               {:env-str    exports
                :stakers    []
                :validators []})))

(defn gen-universe
  "Generates an initial universe. Returns a map of:

    {:env-str         A big ol' blob of env vars as a string
     :validators      [{:privkey \"...\"} ...]
     :stakers         [{:privkey \"...\"} ...]
     :universe        {...}
     :universe-str    \"base64 encoded cbor string\"
     :universe-type   :DEVELOPMENT
     :universe-token  \"xrd_rb...\"}"
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
          (c/exec :git :reset :--hard (:radix-git-version test))
          (c/exec :git :clean :-fdx)

          ; Use Gradle to build a dev universe
          (let [out (c/exec "./gradlew"
                            :-q
                            :-P (str "validators=" (:validators test))
                            ":radixdlt:clean"
                            ":radixdlt:generateDevUniverse")]
            (info "dev universe:\n"
                  out)
            (parse-universe-exports out)))))

(defn gen-keys!
  "Generates keys for the node, writing them to secret-dir"
  []
  (c/exec (str bin-dir "/keygen")
          :--keystore keystore
          :--password password))

(defn env
  "The environment map for a node."
  [test node universe]
  (assert (= (count (:nodes test)) (:validators test))
          "We don't know how to start a cluster where some nodes aren't validators yet.")
  {:JAVA_OPTS "-server -Xms3g -Xmx3g -XX:+HeapDumpOnOutOfMemoryError -Djavax.net.ssl.trustStore=/etc/ssl/certs/java/cacerts -Djavax.net.ssl.trustStoreType=jks -Djava.security.egd=file:/dev/urandom -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
   :RADIX_NODE_KEYSTORE_PASSWORD password
   :RADIXDLT_UNIVERSE_ENABLE true
   :RADIXDLT_UNIVERSE (:universe-str universe)
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
           :ntp.pool                   "pool.ntp.org"
           ; We pass in the universe via env vars
           ; :universe.location          universe-file
           ; Likewise, we pass this in via env
           ; :node.key.path              keystore
           :network.tcp.listen_port    listen-port
           :network.tcp.broadcast_port broadcast-port
           ; The docs (https://docs.radixdlt.com/main/radix-nodes/running-a-full-node-standalone.html#_configuration) say to use host.ip, but the example config file (https://github.com/radixdlt/radixdlt/blob/ba7a42999d82a71696209e275c53cb99c6916114/radixdlt-core/radixdlt/src/main/resources/default.config#L95) says network.host_ip; maybe we need to change this?
           :host.ip                    (cn/local-ip)
           :network.host_ip            (cn/local-ip)
           :db.location                data-dir
           :node_api.port              node-api-port
           :client_api.enable          false
           :client_api.port            client-api-port
           :log.level                  "debug"
           :universe                   (:universe-str universe)}
    ; All non-primary nodes get the primary as their seed
    (not= node (primary test))
    (assoc :network.seeds (str (cn/ip (first (:nodes test)))
                               ":" broadcast-port))))

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

(defn await-node
  "Blocks until we can fetch the /node JSON data"
  [node]
  (info :node (util/await-fn
                (fn []
                  (:body (http/get (str "https://" node "/node")
                                   {:basic-auth  ["admin" password]
                                    :as          :json
                                    :insecure?   true})))
                {:log-message (str "Waiting for https://" node "/node")})))

(defn db
  "The Radix DLT database."
  []
  (let [universe (promise)]
    (reify db/DB
      (setup! [this test node]
        (when (= node (primary test))
          (deliver universe (gen-universe test))
          (info :universe (pprint-str @universe)))

        (install! test)
        (configure! test node @universe)
        (restart-nginx!)
        (db/start! this test node)
        (await-node node)
        (Thread/sleep 20000))

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
          (cu/start-daemon!
            {:chdir   dir
             :logfile log-file
             :pidfile pid-file
             :env     (env test node @universe)}
            (str bin-dir "/radixdlt")
            ; No args?
            )))

      (kill! [this test node]
        (c/su
          ; The script immediately execs, so pidfile is actually useless here
          (cu/stop-daemon! "java" pid-file))))))
