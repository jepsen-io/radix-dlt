(ns jepsen.radix-dlt.core
  "Top-level namespace: constructs tests and runs them at the CLI."
  (:require [clojure [string :as str]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [tests :as tests]
                    [util :as util :refer [parse-long]]]
            [jepsen.os.debian :as debian]
            [jepsen.radix-dlt [db :as db]
                              [nemesis :as nemesis]
                              [workload :as workload]
                              [double-spend :as double-spend]]))

(def workloads
  "A map of workload names to workload constructor functions."
  {:double-spend double-spend/workload
   :list-append  workload/workload})

(def fs
  "A set of all (logical) functions we use to interact with Radix"
  #{:balance :raw-balances :raw-txn-log :txn-log})

(def nemeses
  "The types of faults our nemesis can produce"
  #{:pause :kill :partition :clock :membership})

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
   {:nemesis #{:kill :partition}}])

(defn parse-comma-kws
  "Takes a comma-separated string and returns a collection of keywords."
  [spec]
  (->> (str/split spec #",")
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
  (let [workload-name (:workload opts :list-append)
        workload  ((workloads workload-name) opts)
        db        (db/db)
        nemesis   (nemesis/package
                    {:db db
                     :nodes     (:nodes opts)
                     :faults    (:nemesis opts)
                     :partition {:targets (:partition-targets opts)}
                     :clock     {:targets (:db-targets opts)}
                     :pause     {:targets (:db-targets opts)}
                     :kill      {:targets (:db-targets opts)}
                     :interval  (:nemesis-interval opts)})]
    (merge tests/noop-test
           opts
           {:os               debian/os
            :db               db
            :name             (str (name workload-name) " "
                                   (or (:zip opts) (:version opts)) " "
                                   (pr-str (:nemesis opts)))
            :pure-generators  true
            :client           (:client workload)
            :nemesis          (:nemesis nemesis)
            :checker          (checker/compose
                                {:stats    (checker/stats)
                                 :workload (:checker workload)
                                 :perf     (checker/perf
                                             {:nemeses (:perf nemesis)})
                                 :ex       (checker/unhandled-exceptions)})
            :perf-opts        {:nemeses (:perf nemesis)}
            :generator        (gen/phases
                                (->> (:generator workload)
                                     (gen/stagger (/ (:rate opts)))
                                     (gen/nemesis (:generator nemesis))
                                     (gen/time-limit (:time-limit opts)))
                                (gen/nemesis (:final-generator nemesis))
                                (gen/log (str "Waiting "
                                              (:recovery-time opts)
                                              " seconds for recovery..."))
                                (gen/sleep (:recovery-time opts))
                                (gen/clients (:final-generator workload)))})))

(def validate-non-neg
  [#(and (number? %) (not (neg? %))) "Must be non-negative"])

(def cli-opts
  "Command line option specifications."
  [[nil "--db-targets TARGETS" "A comma-separated list of nodes to pause/kill/etc; e.g. one,all"
    :default [:minority-third :majority :all]
    :parse-fn parse-comma-kws
    :validate [(partial every? db-targets) (cli/one-of db-targets)]]

   [nil "--fs FUNCTION_TYPES" "A comma-separated list of functions to use when interacting with Radix. This allows you to e.g. only perform raw balance reads instead of archive balance reads. `txn-log` is strongly recommended; balance reads are essentially useless without them."
    :default #{:balance :raw-balances :raw-txn-log :txn-log}
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

   ["-w" "--workload NAME" "Which workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]

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

(defn -main
  "CLI entry point."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   radix-test
                                         :opt-spec  cli-opts})
                   (cli/test-all-cmd {:tests-fn     all-tests
                                      :opt-spec     cli-opts})
                   (cli/serve-cmd))
            args))
