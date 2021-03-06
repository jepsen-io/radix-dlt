(ns jepsen.radix-dlt.workload-test
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]
                     [set :as set]
                     [test :refer :all]]
            [jepsen [generator :as gen]
                    [util :as util]]
            [jepsen.generator.test :as gt]
            [jepsen.radix-dlt [accounts :as as]
                              [util :as u]
                              [workload :as w]]))

(defn keys-in
  "Returns all keys in an op."
  [{:keys [f value]}]
  (case f
    :txn     (->> (:ops value)
                  (mapcat (juxt :from :to)))
    :txn-log [(:account value)]
    :balance [(:account value)]))

(defn approx=
  "Approximate numeric equality: y should fall within +/- (* tolerance x)"
  [tolerance x y]
  (let [margin (* tolerance x)]
    (<= (- x margin) y (+ x margin))))

(deftest gen-test
  (let [n                  10000
        max-writes-per-key 10
        accounts (atom (as/initial-accounts))
        rri      (doto (promise) (deliver "xrd_jepsen"))
        gen      (->> (w/generator! {:fs                 #{:balance :txn-log}
                                     :accounts           accounts
                                     :token-rri          rri
                                     :max-writes-per-key max-writes-per-key
                                     :key-count          10
                                     :key-dist-base      2})
                      (gen/clients)
                      (gen/delay 1)
                      (gen/limit n))
        ; We're slicing the txn/txn-log/balance space into roughly thirds, so
        ; we want 3 workers
        ctx      (gt/n+nemesis-context 3)
        ops      (gt/quick ctx gen)
        ops-by-f (group-by :f ops)]

    (testing "process dispersion"
      (let [freqs (->> ops
                       (map :process)
                       frequencies
                       vals)
            mean (/ (reduce + 0 freqs) (count freqs))]
        (doseq [f freqs]
          (is (approx= 1/10 mean f)))))

    (testing "fs"
      (is (= #{:txn :balance :txn-log} (set (keys ops-by-f)))))

    (testing "txids"
      (is (->> (:txn ops-by-f) (every? (comp integer? :id :value))))
      (is (->> (:txn ops-by-f) (map (comp :id :value)) distinct?)))

    (testing "adds keys to accounts"
      (let [ks (->> ops (mapcat keys-in) (into (sorted-set)))
            as (->> @accounts :by-id keys (into (sorted-set)))]
        (is (set/subset? ks as))
        ;(pprint (set/difference ks as))
        ))

    (testing "txn from"
      (is (->> (:txn ops-by-f) (every? (comp integer? :from :value)))))

    (testing "key distribution"
      (testing "valid IDs"
        (let [ks (mapcat keys-in ops)]
          (is (every? (partial <= 1) ks))))

      (testing "max writes per key"
        (let [ks (->> (:txn ops-by-f)
                      (mapcat (comp distinct keys-in))
                      frequencies
                      (remove (comp u/default-account-id? key))
                      (map val))]
          (is (every? #(<= % max-writes-per-key) ks))))

      (testing "temporal frequency"
        ; We construct a map of keys to [initial-index last-index] appearances,
        ; and use that as a proxy for update frequency.
        (let [[n windows]
              (reduce
                (fn [[i windows] op]
                  [(inc i)
                   (reduce
                     (fn [windows k]
                       (let [window (get windows k [i nil])]
                         ; Record final position
                         (assoc windows k (assoc window 1 i))))
                     windows
                     (keys-in op))])
                [0 (sorted-map)]
                ops)
              ; Transform those windows into fractions of the history length
              ; that key was active.
              windows (util/map-vals (fn [[lower upper]]
                                       (double (/ (- upper lower) n)))
                                     windows)]
          ; Account 1, our funding account, should be active throughout
          (is (< 0.95 (windows 1)))
          ; Remaining accounts should be exponentially distributed: lots of
          ; short-lived accounts, relatively few long-lived ones. Compute
          ; quantiles:
          (let [dist (->> (dissoc windows 1)
                          vals
                          sort
                          vec)
                quantiles (->> (range 0 1 1/10)
                               (map (comp int (partial * (count dist))))
                               (mapv dist))
                ; Also get the maximum
                quantiles (conj quantiles (peek dist))]
            ;(pprint quantiles)
            (mapv (fn [[lower upper] actual]
                    (is (<= lower actual upper)))
                  (partition 2
                             [0.0    0.002
                              0.002  0.003
                              0.002  0.004
                              0.0025 0.04
                              0.003  0.005
                              0.004  0.006
                              0.004  0.006
                              0.005  0.008
                              0.008  0.015
                              0.015  0.03
                              0.5    1.0])
                  quantiles)))))))
