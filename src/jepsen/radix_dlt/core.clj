(ns jepsen.radix-dlt.core
  "Top-level namespace: constructs tests and runs them at the CLI."
  (:require [clojure [string :as str]
                     [pprint :refer [pprint]]]
            [clojure.java [io :as io]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [db]
                    [generator :as gen]
                    [os]
                    [tests :as tests]
                    [util :as util :refer [parse-long]]]
            [jepsen.nemesis.combined]
            [jepsen.os.debian :as debian]
            [jepsen.radix-dlt [accounts :as a]
                              [client :as rc]
                              [db :as db]
                              [nemesis :as nemesis]
                              [pubcheck :as pubcheck]
                              [workload :as workload]
                              [double-spend :as double-spend]
                              [util :as u]])
  (:import (com.radixdlt.identifiers AccountAddressing)))

(def workloads
  "A map of workload names to workload constructor functions."
  {:double-spend double-spend/workload
   :list-append  workload/workload})

(def fs
  "A set of all (logical) functions we use to interact with Radix"
  #{:balance :raw-balances :raw-txn-log :txn-log})

(def nemeses
  "The types of faults our nemesis can produce"
  #{:pause :kill :partition :clock :membership
    :repeated-start})

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none      []
   :standard  [:pause :kill :partition :clock]
   :all       [:pause :kill :partition :clock]})

(def db-targets
  "Valid targets for DB nemesis operations."
  #{:one :primaries :minority-third :majority :all})

(def partition-targets
  "Valid targets for partition nemesis operations."
  #{:one :primaries :minority-third :majority-ring})

(def standard-nemeses
  "A collection of partial options maps for various nemeses we want to run as a
  part of test-all."
  [{:nemesis nil}
   {:nemesis #{:kill}}
   {:nemesis #{:partition}}
   {:nemesis #{:clock}}
   {:nemesis #{:kill :partition :clock}}
   {:nemesis #{:membership}}
   {:nemesis #{:membership :kill}}
   {:nemesis #{:membership :partition}}
   {:nemesis #{:membership :kill :partition :clock}}])

(defn parse-comma-kws
  "Takes a comma-separated string and returns a collection of keywords."
  [spec]
  (->> (str/split spec #",")
       (remove #{""})
       (map keyword)))

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (parse-comma-kws spec)
       (mapcat #(get special-nemeses % [%]))
       set))

(defn radix-test
  "Constructs a Radix-DLT test from parsed CLI options."
  [opts]
  (let [; Construct an initial account map, where account 1 is the main funding
        ; account.
        accounts (-> (a/accounts)
                     (a/conj-account (if (:stokenet opts)
                                       (a/stokenet-account 1)
                                       (a/small-account 1)))
                     atom)
        workload-name (:workload opts :list-append)
        workload      ((workloads workload-name)
                       (assoc opts :accounts accounts))
        os            (if (:stokenet opts)
                        jepsen.os/noop
                        debian/os)
        db            (if (:stokenet opts)
                        jepsen.db/noop
                        (db/db))
        nemesis   (if (:stokenet opts)
                    jepsen.nemesis.combined/noop
                    (nemesis/package
                      {:db db
                       :nodes     (:nodes opts)
                       :faults    (:nemesis opts)
                       :partition {:targets (:partition-targets opts)}
                       :clock     {:targets (:db-targets opts)}
                       :pause     {:targets (:db-targets opts)}
                       :kill      {:targets (:db-targets opts)}
                       :interval  (:nemesis-interval opts)}))]
    (cond->
      (merge tests/noop-test
             opts
             {:accounts         accounts
              :os               os
              :db               db
              :name             (str (name workload-name) " "
                                     (or (when (:stokenet opts) "stokenet")
                                         (.getName (io/file (:zip opts)))
                                         (:version opts)) " "
                                     (pr-str (:nemesis opts)))
              :pure-generators  true
              :client           (:client workload)
              :nemesis          (:nemesis nemesis)
              :nonserializable-keys [:accounts]
              :checker          (checker/compose
                                  {:stats    (checker/stats)
                                   :workload (:checker workload)
                                   :perf     (checker/perf
                                               {:nemeses (:perf nemesis)})
                                   :clock    (checker/clock-plot)
                                   :ex       (checker/unhandled-exceptions)})
              :perf-opts        {:nemeses (:perf nemesis)}
              :generator        (gen/phases
                                  (->> (:generator workload)
                                       (gen/stagger (/ (:rate opts)))
                                       (gen/nemesis
                                         (gen/phases (gen/sleep 10)
                                                     (:generator nemesis)))
                                       (gen/time-limit (:time-limit opts)))
                                  (gen/nemesis (:final-generator nemesis))
                                  (gen/log (str "Waiting "
                                                (:recovery-time opts)
                                                " seconds for recovery..."))
                                  (gen/sleep (:recovery-time opts))
                                  (gen/clients (:final-generator workload)))})

      ; For stokenet, we're not going to be able to log in to any nodes.
      (:stokenet opts) (assoc-in [:ssh :dummy?] true))))

(def validate-non-neg
  [#(and (number? %) (not (neg? %))) "Must be non-negative"])

(def cli-opts
  "Command line option specifications."
  [[nil "--check-only-raw-txn-log" "If set, skips the expensive cycle detection inference and per-key projections of raw-txn-log operations, and simply checks the raw txn log as if it were a single key."]

   [nil "--db-targets TARGETS" "A comma-separated list of nodes to pause/kill/etc; e.g. one,all"
    :default [:minority-third :majority :all]
    :parse-fn parse-comma-kws
    :validate [(partial every? db-targets) (cli/one-of db-targets)]]

   [nil "--fs FUNCTION_TYPES" "A comma-separated list of functions to use when interacting with Radix. This allows you to e.g. only perform raw balance reads instead of archive balance reads. `txn-log` is strongly recommended; balance reads are essentially useless without them. Pass an empty string to disable all reads. Add raw-txn-log and raw-balances to query the raw ledger state--this will only work in certain development builds."
    :default #{:balance :txn-log}
    :parse-fn (comp set parse-comma-kws)
    :validate [(partial every? fs) (cli/one-of fs)]]

   [nil "--partition-targets TARGETS" "A comma-separated list of nodes to target for network partitions; e.g. one,all"
    :default [:minority-third :majorities-ring]
    :parse-fn parse-comma-kws
    :validate [(partial every? partition-targets) (cli/one-of partition-targets)]]

   [nil "--raw-txn-logs" "If set, use raw-txn-log operations, rather than txn-log ops, to infer transaction orders and balances during checking."
    :default false]

   [nil "--recovery-time SECONDS" "How long should we wait for cluster recovery before final reads?"
    :default 10]

   [nil "--rate HZ" "Target number of ops/sec"
    :default  40
    :parse-fn read-string
    :validate validate-non-neg]

   [nil "--[no-]faithful" "Enables checking whether transactions in the log are faithful representations of their original actions."
    :default true]

   [nil "--[no-]self-transfers" "Enables transfers from an account to itself."
    :default true]

   [nil "--stokenet" "If enabled, tests against the public Stokenet rather than a local installation."]

   [nil "--validators COUNT" "Number of validators."
    :default  5
    :parse-fn parse-long]

   [nil "--version VERSION" "RadixDLT version (from https://github.com/radixdlt/radixdlt/releases); should also be a git tag"
    :default "1.0.0"]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? (into nemeses (keys special-nemeses)))
               (str "Faults must be one of " nemeses " or "
                    (cli/one-of special-nemeses))]]

   [nil "--nemesis-interval SECONDS" "How long to wait between nemesis faults."
    :default  30
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--read-concurrency NUMBER" "How many threads should perform reads? 2n means 'twice the number of nodes'"
    :default  "2n"
    :validate [(partial re-find #"^\d+n?$")
               "Must be an integer, optionally followed by n."]]

   ["-w" "--workload NAME" "Which workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]

   [nil "--write-concurrency NUMBER" "How many threads should perform writes? 2n means 'twice the number of nodes'"
    :default  "2n"
    :validate [(partial re-find #"^\d+n?$")
               "Must be an integer, optionally followed by n."]]

   [nil "--zip ZIPFILE" "Path to a local radixdlt-dist-<whatever>.zip file to install, rather than using --version. Helpful if you want to test a feature branch or one-off build."]
   ])

(defn all-tests
  "Takes parsed CLI options and constructs a sequence of tests from them."
  [opts]
  (let [workloads (:workload opts (keys workloads))
        nemeses (if (nil? (:nemesis opts))
                  standard-nemeses
                  [#{}])
        tests (->> (for [i (range (:test-count opts)), nemesis nemeses]
                     (merge opts nemesis))
                   (map radix-test))]
    tests))

(defn keygen-cmd
  "Command to generate a keypair."
  []
  {"keygen"
   {:usage "Generates a fresh keypair and prints it as an EDN map."
    :run (fn [{:keys [options]}]
           (let [key-pair (rc/new-key-pair)]
             (-> {:private-base64 (-> key-pair rc/private-key u/bytes->base64)
                  :public-hex     (-> key-pair rc/public-key .toHex)
                  :address-hex    (-> key-pair rc/->account-address .getAddress str)
                  :address-bech32 (->> key-pair
                                       rc/->account-address
                                       .getAddress
                                       (.of (AccountAddressing/bech32 "tdx")))}
                 pprint)))}})

(defn pubcheck-cmd
  "Command which explores a public radix archive node, looking for traces of
  consistency anomalies."
  []
  {"pubcheck"
   {:usage "Explores a public Radix archive node, starting with the given address and exploring from there, looking for traces of consistency violations."
    :opt-spec
    [["-c" "--concurrency THREADS" "How many threads should we use for the search?"
      :default 10
      :parse-fn parse-long
      :validate [pos? "Must be positive"]]

     ["-n" "--node NODE" "What node should we connect to? See https://docs.radixdlt.com/main/node/cli-install-node-docker.html."
      :default "mainnet.radixdlt.com"]

     ["-a" "--address ADDR" "What address should we start our search from?"
      :default "rdx1qspldshtx0s2l2rcnaqtqpqz8vwps2y6d9se0wq25xrg92l66cmp6mcnc6pyu"]

     ["-r" "--recheck" "If set, resets the checked state of every address to unchecked."
      :default false]]
    :run (comp pubcheck/pubcheck :options)}})

(defn test-opt-fn
  "Pre-processes options before passing them to the test runner."
  [parsed]
  (let [; Start by parsing the write/read concurrencies
        parsed (-> parsed
                   (cli/parse-concurrency :read-concurrency)
                   (cli/parse-concurrency :write-concurrency))
        ; Use that to compute our actual :concurrency
        {:keys [read-concurrency write-concurrency]} (:options parsed)
        parsed (update parsed :options
                       assoc :concurrency (+ read-concurrency write-concurrency))]
    parsed))

(defn -main
  "CLI entry point."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   radix-test
                                         :opt-spec  cli-opts
                                         :opt-fn    test-opt-fn})
                   (cli/test-all-cmd {:tests-fn     all-tests
                                      :opt-spec     cli-opts
                                      :opt-fn       test-opt-fn})
                   (cli/serve-cmd)
                   (pubcheck-cmd)
                   (keygen-cmd))
            args))
