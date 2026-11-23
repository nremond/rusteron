(ns aeron.jepsen.core
  "Jepsen tests of Aeron Cluster: Elle's list-append workload on a cluster of
  Java (`--impl java`) or Rust (`--impl rust`) members, under random faults
  or a staged scenario. See ../README.md."
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [net :as net]
                    [os :as os]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as append]
            [aeron.jepsen [checker :as aeron-checker]
                          [client :as client]
                          [db :as db]
                          [docker :as docker]
                          [nemesis :as nemesis]]))

(def cli-opts
  [[nil "--impl IMPL" "Whose members: java or rust."
    :default "java"
    :validate [#{"java" "rust"} "java or rust"]]
   [nil "--scenario NAME" "random, or a staged scenario: stale-leader (seed 111), stale-election (seed 641)."
    :default "random"
    :validate [#{"random" "stale-leader" "stale-election"} "random, stale-leader or stale-election"]]
   [nil "--rate HZ" "Transactions a second, all clients together."
    :default 20
    :parse-fn parse-double]
   [nil "--faults FAULTS" "Random faults, comma-separated: partition, kill, pause."
    :default #{:partition :kill :pause}
    :parse-fn (fn [s] (set (map keyword (remove str/blank? (str/split s #",")))))]
   [nil "--nemesis-interval SECONDS" "Seconds between random faults."
    :default 10
    :parse-fn parse-long]
   [nil "--heartbeat-timeout-ms MS" "The members' leader heartbeat timeout."
    :default 2000
    :parse-fn parse-long]
   [nil "--election-timeout-ms MS" "The members' election timeout."
    :default 1000
    :parse-fn parse-long]])

(defn aeron-test
  [opts]
  (let [db       (db/db opts)
        workload (append/test {:key-count          6
                               :min-txn-length     1
                               :max-txn-length     4
                               :max-writes-per-key 64
                               :consistency-models [:strict-serializable]})
        nem      (nemesis/package opts db)
        clients  (fn [g] (gen/stagger (/ 1 (:rate opts)) g))]
    (merge tests/noop-test
           opts
           {:name      (str "aeron-cluster-" (:impl opts) "-" (:scenario opts))
            :os        os/noop
            :db        db
            :remote    docker/remote
            :net       net/iptables
            :client    (client/client)
            :nemesis   (:nemesis nem)
            :checker   (checker/compose
                        {:append     (:checker workload)
                         :truncation (aeron-checker/truncation)
                         :liveness   (aeron-checker/liveness)
                         :rejoined   (aeron-checker/rejoined)
                         :stats      (checker/stats)
                         :exceptions (checker/unhandled-exceptions)
                         :timeline   (timeline/html)})
            ;; One client stream for the whole run, so Elle's values stay
            ;; unique; the faults, then their final heal, on the nemesis'
            ;; track; then traffic for 40 s more, which the cluster must serve.
            ;; 20 s after the heal, the members are probed: all must be back.
            :generator (->> (:generator workload)
                            clients
                            (gen/nemesis (gen/phases
                                          (gen/time-limit (:time-limit opts) (:generator nem))
                                          (gen/sleep 1)
                                          (:final-generator nem)
                                          (gen/sleep 20)
                                          {:type :info, :f :states}))
                            (gen/time-limit (+ (:time-limit opts) 40)))})))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  aeron-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
