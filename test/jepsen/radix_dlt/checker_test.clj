(ns jepsen.radix-dlt.checker-test
  (:refer-clojure :exclude [test])
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.radix-dlt.checker :refer :all]
            [elle [graph :as g]
                  [core :as elle]]))

(def test
  "A mock test"
  {:nodes ["n1" "n2" "n3"]})

(defn explain
  "Shortcut for rendering a text explanation from a graph map"
  [graph op1 op2]
  (let [e (:explainer graph)]
    (when-let [ex (elle/explain-pair-data e op1 op2)]
      (elle/render-explanation e ex "t1" "t2"))))

(deftest realtime+node-graph-test
  (testing "empty"
    (let [g (realtime+node-graph test [])]
      (is (= (g/digraph)
             (:graph g)))))

  (testing "single write"
    (let [g (realtime+node-graph
              test
              [{:index 0, :process 0, :type :invoke, :f :txn, :value [[:append 0 1]]}
               {:index 1, :process 0, :type :ok, :f :txn, :value [[:append 0 1]]}])]
      (is (= (g/digraph)
             (:graph g)))))

  (testing "a successive read and a write on the same node"
    (let [r  {:index 0, :time 0, :process 0, :type :invoke, :f :txn, :value [[:r 0 nil]]}
          r' {:index 1, :time 1, :process 0, :type :ok,     :f :txn, :value [[:r 0 []]]}
          w  {:index 2, :time 2, :process 3, :type :invoke, :f :txn, :value [[:append 0 1]]}
          w' {:index 3, :time 3, :process 3, :type :info,   :f :txn, :value [[:append 0 1]]}
          g  (realtime+node-graph test [r r' w w'])]
      ; Since these happened on the same node, they should produce a :process edge.
      (is (= (-> (g/digraph)
                 (g/link r' w' :process))
             (:graph g)))
      (is (= "read t1 completed at index 1, 0.000 seconds before the invocation of write t2, at index 2, on the same node"
             (explain g r' w')))))

  (testing "a successive read and a write on different nodes"
    (let [r  {:index 0, :time 0, :process 0, :type :invoke, :f :txn, :value [[:r 0 nil]]}
          r' {:index 1, :time 1, :process 0, :type :ok,     :f :txn, :value [[:r 0 []]]}
          w  {:index 2, :time 2, :process 1, :type :invoke, :f :txn, :value [[:append 0 1]]}
          w' {:index 3, :time 3, :process 1, :type :info,   :f :txn, :value [[:append 0 1]]}
          g  (realtime+node-graph test [r r' w w'])]
      ; Since these happened on the different nodes, and one was a read, it shouldn't generate a realtime or process edge.
      (is (= (g/digraph) (:graph g)))
      (is (= nil (explain g r' w')))))

  (testing "successive writes on the same node"
    (let [w1  {:index 0, :time 0, :process 0, :type :invoke, :f :txn, :value [[:append 0 1]]}
          w1' {:index 1, :time 1, :process 0, :type :ok,     :f :txn, :value [[:append 0 1]]}
          w2  {:index 2, :time 2, :process 3, :type :invoke, :f :txn, :value [[:append 2 3]]}
          w2' {:index 3, :time 3, :process 3, :type :info,   :f :txn, :value [[:append 2 3]]}
          g  (realtime+node-graph test [w1 w1' w2 w2'])]
      (is (= (-> (g/digraph)
                 ; Since these happened in strict realtime order, there should be a :realtime edge here
                 (g/link w1' w2' :realtime)
                 ; And also a :process edge, since they happened on the same node in order
                 (g/link w1' w2' :process))
             (:graph g)))
      (is (= "write t1 completed at index 1, 0.000 seconds before the invocation of write t2, at index 2, on the same node"
             (explain g w1' w2')))))

  (testing "successive writes on different nodes"
    (let [w1  {:index 0, :time 0, :process 0, :type :invoke, :f :txn, :value [[:append 0 1]]}
          w1' {:index 1, :time 1, :process 0, :type :ok,     :f :txn, :value [[:append 0 1]]}
          w2  {:index 2, :time 2, :process 1, :type :invoke, :f :txn, :value [[:append 2 3]]}
          w2' {:index 3, :time 3, :process 1, :type :info,   :f :txn, :value [[:append 2 3]]}
          g  (realtime+node-graph test [w1 w1' w2 w2'])]
      ; Since these happened in strict realtime order, there should be a :realtime edge here
      (is (= (-> (g/digraph)
                 (g/link w1' w2' :realtime))
             (:graph g)))
      (is (= "write t1 completed at index 1, 0.000 seconds before the invocation of write t2, at index 2"
             (explain g w1' w2'))))))
