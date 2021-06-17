(defproject jepsen.radix-dlt "0.1.0-SNAPSHOT"
  :description "Tests for the Radix DLT proof-of-stake currency"
  :url "https://github.com/jepsen-io/radix-dlt"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [cheshire "5.10.0"]
                 [clj-http "3.12.0"]
                 [jepsen/jepsen "0.2.5-SNAPSHOT"]
                 [mvxcvi/clj-cbor "1.1.0"]]
  :repl-options {:init-ns jepsen.radix-dlt.core}
  :main jepsen.radix-dlt.core)
