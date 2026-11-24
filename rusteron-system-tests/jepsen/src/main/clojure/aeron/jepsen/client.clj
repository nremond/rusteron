(ns aeron.jepsen.client
  "Elle's list-append transactions through an Aeron Cluster session: answered
  (:ok), not sent (:fail), or sent and unanswered (:info)."
  (:require [clojure.string :as str]
            [jepsen.client :as client]
            [aeron.jepsen.db :as db])
  (:import (aeron.jepsen AppendClient AppendClient$Indeterminate AppendClient$NotApplied)))

(def timeout-ms
  "How long a transaction waits for its answer."
  5000)

(defn ingress-endpoints
  "The members' ingress: 0=ip:9002,1=ip:9002,..."
  [test]
  (->> (:nodes test)
       (map-indexed (fn [i node] (str i "=" (db/ip node) ":9002")))
       (str/join ",")))

(defn- ops
  "A transaction's micro-ops as [type key value] longs."
  [txn]
  (into-array (for [[f k v] txn]
                (long-array [(if (= f :append) 0 1) k (or v 0)]))))

(defrecord Client [^AppendClient conn]
  client/Client
  (open! [this test _node]
    (assoc this :conn (AppendClient. (ingress-endpoints test) db/host-ip)))
  (setup! [_ _test])
  (invoke! [_ _test op]
    (try
      (let [reads (.invoke conn (ops (:value op)) timeout-ms)]
        (assoc op
               :type :ok
               :value (mapv (fn [[f k v] r] (if (= f :r) [f k (vec r)] [f k v]))
                            (:value op)
                            reads)))
      (catch AppendClient$NotApplied e
        (assoc op :type :fail :error [:not-applied (.getMessage e)]))
      (catch AppendClient$Indeterminate e
        (assoc op :type :info :error [:indeterminate (.getMessage e)]))))
  (teardown! [_ _test])
  (close! [_ _test]
    (when conn (.close conn))))

(defn client
  []
  (->Client nil))
