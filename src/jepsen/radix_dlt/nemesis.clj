(ns jepsen.radix-dlt.nemesis
  "Fault injection for Radix clusters"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [generator :as gen]
                    [nemesis :as n]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.radix-dlt.db :as db]))

(defn package
  "Given CLI options, constructs a package of {:generator, :final-generator,
  :nemesis, ..."
  [opts]
  (-> opts
      nc/nemesis-packages
      nc/compose-packages))
