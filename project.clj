(defproject jepsen.radix-dlt "0.1.0-SNAPSHOT"
  :description "Tests for the Radix DLT proof-of-stake currency"
  :url "https://github.com/jepsen-io/radix-dlt"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :repositories [["jitpack" "https://jitpack.io"]]
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [cheshire "5.10.0"
                  :exclusions [com.fasterxml.jackson.core/jackson-core
                               com.fasterxml.jackson.dataformat/jackson-dataformat-cbor]]
                 [clj-http "3.12.0"
                  :exclusions [commons-codec]]
                 [com.radixdlt/radixdlt-java "1.0-beta.35.1-release~1.0-beta.35-SNAPSHOT"]
                 [jepsen/jepsen "0.2.5-SNAPSHOT"
                  :exclusions [com.fasterxml.jackson.core/jackson-core]]
                 [mvxcvi/clj-cbor "1.1.0"]
                 ; Types
                 [org.clojure.typed/runtime.jvm "1.0.1"]]
  :profiles {:dev {:dependencies [[org.clojure.typed/checker.jvm "1.0.1"]]}}
  :repl-options {:init-ns jepsen.radix-dlt.core}
  :main jepsen.radix-dlt.core)
