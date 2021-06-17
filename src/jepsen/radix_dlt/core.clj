(ns jepsen.radix-dlt.core
  "Top-level namespace: constructs tests and runs them at the CLI."
  (:require [jepsen [cli :as cli]
                    [tests :as tests]
                    [util :as util :refer [parse-long]]]
            [jepsen.os.debian :as debian]
            [jepsen.radix-dlt.db :as db]))

(defn radix-test
  "Constructs a Radix-DLT test from parsed CLI options."
  [opts]
  (merge tests/noop-test
         opts
         {:os               debian/os
          :db               (db/db)
          :name             "radix"
          :pure-generators  true}))

(def cli-opts
  "Command line option specifications."
  [[nil "--node-runner-version VERSION-STRING" "Which version of https://github.com/radixdlt/node-runner/releases should we use to install Radix?"
    :default "1.0-beta.35.1"]

   [nil "--radix-git-version COMMIT" "What commit from radix-dlt should we check out?"
    :default "09e06232ac26e51faf567bc0af7324341508ddfc"]

   [nil "--validators COUNT" "Number of validators."
    :default  5
    :parse-fn parse-long]

   [nil "--version VERSION" "RadixDLT version (from https://github.com/radixdlt/radixdlt/releases)"
    :default "1.0-beta.35.1"]
   ])

(defn -main
  "CLI entry point."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   radix-test
                                         :opt-spec  cli-opts})
                   (cli/serve-cmd))
            args))
