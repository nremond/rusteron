(ns aeron.jepsen.nemesis
  "Faults: Jepsen's at random (partitions, kills, pauses), or a scenario
  staged step by step from what the members' counters show."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as n]]
            [jepsen.nemesis.combined :as nc]
            [aeron.jepsen.db :as db]))

(defn- cut!
  "Drops at `to` what `from` sends it."
  [test from to]
  (c/on-nodes test [to]
              (fn [_ _] (c/exec* (str "iptables -A INPUT -s " (db/ip from) " -j DROP -w")))))

(defn- mend!
  "Undoes `cut!`."
  [test from to]
  (c/on-nodes test [to]
              (fn [_ _] (c/exec* (str "iptables -D INPUT -s " (db/ip from) " -j DROP -w || true")))))

(defn- sever!
  "Cuts both ways."
  [test a b]
  (cut! test a b)
  (cut! test b a))

(defn- heal!
  [test]
  (c/on-nodes test (fn [_ _] (c/exec* "iptables -F -w"))))

(defn- probe
  "ClusterProbe's output on `node`."
  [test node & args]
  (get (c/on-nodes test [node] (fn [_ _] (apply db/probe! args))) node))

(defn- state
  [test node]
  (db/parse-state (probe test node "state")))

(defn- states
  "Each member's counters, nil for one that does not answer."
  [test]
  (into {} (map (fn [node] [node (state test node)]) (:nodes test))))

(defn- probe-nemesis
  "Answers `:states` with each member's counters, for the rejoin check."
  []
  (reify
    n/Nemesis
    (setup! [this _test] this)
    (invoke! [_ test op] (assoc op :value (states test)))
    (teardown! [_ _test])

    n/Reflection
    (fs [_] #{:states})))

(defn- first-candidate
  "The first of `nodes` to stand as candidate within `timeout-ms`."
  [test nodes timeout-ms]
  (let [p (promise)]
    (doseq [node nodes]
      (future
        (when (probe test node "await" "election" 3 timeout-ms)
          (deliver p node))))
    (deref p (+ timeout-ms 5000) nil)))

(defn stale-leader!
  "Seed 111's sequence (rusteron-cluster-sim), on the real cluster:

  1. The leader is cut off, then asked for a snapshot. It enters SNAPSHOT,
     which it cannot leave without a quorum: its inactive-quorum check runs
     only in ACTIVE, so it stays leader of its term.
  2. The other two hold an election. Once one stands as candidate (the other
     votes at once), the two are cut apart before the candidate, winning by
     quorum after the election timeout, publishes its new term.
  3. The voter, its ballot timed out, canvasses, and is reconnected to the
     stale leader. If it follows the stale leader's older term, clients'
     transactions commit with the two of them.
  4. After a while everything heals: the newer term's leader then meets
     logs longer than its own.

  Returns what each step observed."
  [test]
  (let [nodes  (:nodes test)
        states (into {} (map (fn [node] [node (state test node)]) nodes))
        leader (first (filter #(= 2 (:role (states %))) nodes))]
    (if-not leader
      {:outcome :no-leader, :states states}
      (let [others (vec (remove #{leader} nodes))]
        (info "stale-leader: cutting off leader" leader)
        (doseq [o others] (sever! test leader o))
        (let [toggled   (probe test leader "toggle" "snapshot")
              stuck     (probe test leader "await" "module" 3 3000)
              candidate (first-candidate test others 20000)]
          (if-not candidate
            {:outcome :no-candidate, :leader leader, :toggled toggled, :stuck stuck}
            (let [voter (first (remove #{candidate} others))]
              (info "stale-leader: cutting candidate" candidate "from voter" voter)
              (sever! test candidate voter)
              (info "stale-leader: reconnecting voter" voter "to stale leader" leader)
              (mend! test leader voter)
              (mend! test voter leader)
              (let [joined (probe test voter "await" "election" 17 15000)
                    seen   {:leader    (state test leader)
                            :voter     (state test voter)
                            :candidate (state test candidate)}]
                (info "stale-leader: voter joined:" joined "states:" seen)
                (Thread/sleep 10000)
                (let [before (into {} (map (fn [node] [node (state test node)]) nodes))]
                  (info "stale-leader: healing")
                  (heal! test)
                  (Thread/sleep 5000)
                  {:outcome   :staged
                   :leader    leader
                   :candidate candidate
                   :voter     voter
                   :toggled   toggled
                   :stuck     stuck
                   :joined    joined
                   :after-join seen
                   :before-heal before
                   :after-heal (into {} (map (fn [node] [node (state test node)]) nodes))})))))))))

(defn stale-election!
  "Seed 641's sequence (rusteron-cluster-sim), on the real cluster: the
  stale leader is one whose election never completed.

  1. The leader X is cut off. Once Y stands as candidate (Z votes at once),
     Y and Z are cut apart: Y wins term T1 by quorum and leads it alone.
  2. X and Z are reconnected. X stands for T1 first, which Z already voted
     in, and loses; once Z votes for X's T2, they are cut apart too: X wins
     T2 by quorum and leads it alone.
  3. Z, its ballot timed out, canvasses, and is reconnected to Y. If Z
     follows Y's older term, Y completes T1 with Z and clients' transactions
     commit with the two of them.
  4. After a while everything heals: X's T2 meets logs longer than its own.

  Returns what each step observed."
  [test]
  (let [nodes  (:nodes test)
        states (into {} (map (fn [node] [node (state test node)]) nodes))
        x      (first (filter #(= 2 (:role (states %))) nodes))]
    (if-not x
      {:outcome :no-leader, :states states}
      (let [others (vec (remove #{x} nodes))]
        (info "stale-election: cutting off leader" x)
        (doseq [o others] (sever! test x o))
        (let [y (first-candidate test others 20000)]
          (if-not y
            {:outcome :no-first-candidate, :x x}
            (let [z (first (remove #{y} others))]
              (info "stale-election: cutting first candidate" y "from voter" z)
              (sever! test y z)
              (info "stale-election: reconnecting" x "and" z)
              (mend! test x z)
              (mend! test z x)
              ;; Z's ballot for Y's T1 runs out first (back to CANVASS, 1). X
              ;; stands for T1 too, which Z already voted in, and loses; Z's
              ;; next FOLLOWER_BALLOT (4) is its vote for X's T2.
              (let [voted (and (probe test z "await" "election" 1 10000)
                               (probe test z "await" "election" 4 30000))]
                (if-not voted
                  {:outcome :z-did-not-vote, :x x, :y y, :z z, :z-state (state test z)}
                  (do
                    (info "stale-election: cutting second candidate" x "from voter" z)
                    (sever! test x z)
                    (info "stale-election: reconnecting voter" z "to first candidate" y)
                    (mend! test y z)
                    (mend! test z y)
                    (let [joined (probe test z "await" "election" 17 15000)
                          seen   {:x (state test x), :y (state test y), :z (state test z)}]
                      (info "stale-election: voter joined:" joined "states:" seen)
                      (Thread/sleep 10000)
                      (let [before (into {} (map (fn [node] [node (state test node)]) nodes))]
                        (info "stale-election: healing")
                        (heal! test)
                        (Thread/sleep 5000)
                        {:outcome     :staged
                         :x           x
                         :y           y
                         :z           z
                         :joined      joined
                         :after-join  seen
                         :before-heal before
                         :after-heal  (into {} (map (fn [node] [node (state test node)]) nodes))}))))))))))))

(defn- stale-leader-nemesis
  []
  (reify
    n/Nemesis
    (setup! [this test] (heal! test) this)
    (invoke! [_ test op]
      (case (:f op)
        :stale-leader (assoc op :value (stale-leader! test))
        :stale-election (assoc op :value (stale-election! test))
        :heal (do (heal! test) (assoc op :value :healed))))
    (teardown! [_ test] (heal! test))

    n/Reflection
    (fs [_] #{:stale-leader :stale-election :heal})))

(defn package
  "The nemesis, its generator and its final generator, for `--scenario`."
  [opts db]
  (case (:scenario opts)
    "random"
    ;; Partitions, kills and pauses only: the combined package's file
    ;; corruption and clock faults install tools on the nodes even when off.
    (let [opts {:db        db
                :faults    (set (:faults opts))
                :interval  (:nemesis-interval opts)
                :partition {:targets [:one :majority :majorities-ring :primaries]}
                :kill      {:targets [:one :primaries :minority]}
                :pause     {:targets [:one :primaries :minority]}}]
      (-> (nc/compose-packages [(nc/partition-package opts) (nc/db-package opts)])
          (update :nemesis #(n/compose [% (probe-nemesis)]))))

    ("stale-leader" "stale-election")
    {:nemesis         (n/compose [(stale-leader-nemesis) (probe-nemesis)])
     :generator       (gen/phases (gen/sleep 15)
                                  {:type :info, :f (keyword (:scenario opts))})
     :final-generator {:type :info, :f :heal}}))
