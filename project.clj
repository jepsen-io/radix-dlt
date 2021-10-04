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

                 [com.radixdlt/radixdlt-java
                  ; We moved to using the accounting-entries structure rather
                  ; than the txn actions, which were frequently lying to us.
                  "1.0.0-feature~client-support-for-txn-accounting-SNAPSHOT"

                  ; For testing rc-1.0.2
                  ;"1.0.0-rc~1.0.2-SNAPSHOT"

                  ; For testing the fix-user-token-balance branch circa August
                  ; 25, 2021
                  ;"1.0.0-feature~fix-user-token-balance-SNAPSHOT"

                  ; For testing Stokenet circa Sept 2, 2021 (which can't talk
                  ; to new clients because the definition of tokeninfo JSON
                  ; responses changed and the client isn't backwards
                  ; compatible, except that the actual client for Stokenet's
                  ; version ALSO can't talk to Stokenet, because it doesn't
                  ; handle network IDs correctly when deserializing addresses
                  ; for e.g. balance requests. This is fix-user-token-balance,
                  ; which includes the fixes for that issue, but with the 1.0.1
                  ; definition of TokenInfo.java, so we can deserialize the
                  ; older JSON requests.
                  ;"1.0.0-feature~fix-user-token-balance-1.0.1-compat-SNAPSHOT"

                  ; For builds prior to ~Aug 25, 2021
                  ;"1.0-beta.40-release~1.0-beta.40-SNAPSHOT"

                  ; For stokenet???
                  ; For 1.0.0
                  ;"1.0.0-release~1.0.0-SNAPSHOT"

                  ; This breaks clj-http's insecure? option by registering
                  ; something in the guts of Java's SSL context machinery, so
                  ; we leave it out
                  :exclusions [org.bouncycastle/bcprov-jdk15to18]]

                 [jepsen/jepsen "0.2.5-SNAPSHOT"
                  :exclusions [com.fasterxml.jackson.core/jackson-core]]

                 [mvxcvi/clj-cbor "1.1.0"]

                 ; Types
                 [org.clojure.typed/runtime.jvm "1.0.1"]]
  :jvm-opts ["-Djava.awt.headless=true"]
  :profiles {:dev {:dependencies [[org.clojure.typed/checker.jvm "1.0.1"]]}}
  :repl-options {:init-ns jepsen.radix-dlt.core}
  :main jepsen.radix-dlt.core)
