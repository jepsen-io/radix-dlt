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
                              [workload :as workload]]))

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
  (let [workload  (workload/workload opts)
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
            :name             (str "radix " (pr-str (:nemesis opts)))
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

   [nil "--node-runner-version VERSION-STRING" "Which version of https://github.com/radixdlt/node-runner/releases should we use to install Radix?"
    :default "1.0-beta.35.1"]

   [nil "--partition-targets TARGETS" "A comma-separated list of nodes to target for network partitions; e.g. one,all"
    :default [:minority-third :majorities-ring]
    :parse-fn parse-comma-kws
    :validate [(partial every? partition-targets) (cli/one-of partition-targets)]]

   [nil "--radix-git-version COMMIT" "What commit from radix-dlt should we check out?"
    :default "1.0-beta.40"]

   [nil "--recovery-time SECONDS" "How long should we wait for cluster recovery before final reads?"
    :default 10]

   [nil "--rate HZ" "Target number of ops/sec"
    :default  100
    :parse-fn read-string
    :validate validate-non-neg]

   [nil "--validators COUNT" "Number of validators."
    :default  5
    :parse-fn parse-long]

   [nil "--version VERSION" "RadixDLT version (from https://github.com/radixdlt/radixdlt/releases)"
    :default "1.0-beta.40"]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? (into nemeses (keys special-nemeses)))
               (str "Faults must be one of " nemeses " or "
                    (cli/one-of special-nemeses))]]

   [nil "--nemesis-interval SECONDS" "How long to wait between nemesis faults."
    :default  30
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]
   ])

(defn -main
  "CLI entry point."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   radix-test
                                         :opt-spec  cli-opts})
                   (cli/serve-cmd))
            args))
