(ns jepsen.radix-dlt.client
  "Wrapper around the Java client library."
  (:require [clojure.tools.logging :refer [info warn]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (com.radixdlt.client.lib.api RadixApi)
           (com.radixdlt.client.lib.dto TransactionDTO)
           (com.radixdlt.client.lib.impl SynchronousRadixApiClient)))

(defn ^SynchronousRadixApiClient open
  "Opens a connection to a node."
  [node]
  (RadixApi/connect (str "https://" node)))

(defn network-id
  "Returns the Network ID of this client."
  [^RadixApi client]
  [client]
  (.networkId client))
