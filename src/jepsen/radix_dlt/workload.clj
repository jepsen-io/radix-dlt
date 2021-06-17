(ns jepsen.radix-dlt.workload
  "A generator, client, and checker for RadixDLT"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.radix-dlt.client :as rc]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (rc/open node)))

  (setup! [this test]
    (info :network-id (rc/network-id conn)))

  (invoke! [this test op]
    (assoc op :type :ok))

  (teardown! [this test])

  (close! [this test]))

(defn client
  "Constructs a fresh Jepsen client."
  []
  (Client. nil))

(defn workload
  "Constructs a package of a client and generator."
  [opts]
  {:client    (client)
   :generator nil})
