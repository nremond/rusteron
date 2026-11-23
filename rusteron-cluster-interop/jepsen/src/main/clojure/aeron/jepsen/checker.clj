(ns aeron.jepsen.checker
  "Checks beside Elle's: a member truncating its log below its commit
  position, the cluster serving again once the faults end, and every member
  back in it. Elle sees a lost write only when a later read misses it; a
  cluster that loses committed entries and then stalls serves no such read."
  (:require [clojure.java.io :as io]
            [jepsen [checker :as checker]
                    [store :as store]]))

(def ^:private truncation-line
  ;; Java's ClusterEvent, and the Rust module's error, alike.
  #"Truncating Cluster Log - memberId=(\d+).*this\.commitPosition=(\d+).*newPosition=(\d+)")

(defn truncation
  "Invalid when a member's log was cut below its commit position: entries
  a quorum had committed are gone from it."
  []
  (reify checker/Checker
    (check [_ test _history _opts]
      (let [cuts (for [node   (:nodes test)
                       file   ["node.log" "cm.log"]
                       :let   [f (store/path test node file)]
                       :when  (.exists ^java.io.File f)
                       line   (line-seq (io/reader f))
                       :let   [[_ member commit position] (re-find truncation-line line)]
                       :when  member
                       :let   [commit (parse-long commit), position (parse-long position)]]
                   {:node node, :member (parse-long member), :commit-position commit, :new-position position,
                    :below-commit? (< position commit)})
            below (filter :below-commit? cuts)]
        {:valid?               (empty? below)
         :truncations          (count cuts)
         :below-commit-position (vec below)}))))

(defn- probe?
  [op]
  (= :states (:f op)))

(defn liveness
  "Invalid unless a transaction succeeded after the faults' last operation
  (the final heal)."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [last-fault (->> history (filter #(and (= :nemesis (:process %)) (not (probe? %)))) last :time)
            served     (->> history
                            (filter #(and (= :ok (:type %)) (number? (:process %))))
                            (filter #(or (nil? last-fault) (< last-fault (:time %)))))]
        {:valid?            (boolean (seq served))
         :ok-after-faults   (count served)
         :last-fault-time-s (some-> last-fault (/ 1e9))}))))

(defn rejoined
  "Invalid unless, when probed after the final heal, every member was past
  its election, in one term, with one leader. A cluster serving on a
  majority passes the liveness check with a member left behind, such as a
  leader stuck in an older term's election."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [probe (->> history
                       (filter #(and (= :nemesis (:process %)) (probe? %) (= :info (:type %)) (map? (:value %))))
                       last)]
        (if-not probe
          {:valid? :unknown, :error "the members were not probed after the final heal"}
          (let [states   (:value probe)
                answered (into {} (filter val states))
                leaders  (sort (keys (filter (fn [[_ s]] (= 2 (:role s))) answered)))
                behind   (sort (keys (remove (fn [[_ s]] (= 17 (:election s))) answered)))
                terms    (set (map :term (vals answered)))]
            {:valid?      (and (= (count (:nodes test)) (count answered))
                               (= 1 (count leaders))
                               (empty? behind)
                               (= 1 (count terms)))
             :leaders     (vec leaders)
             :in-election (vec behind)
             :terms       terms
             :states      states}))))))
