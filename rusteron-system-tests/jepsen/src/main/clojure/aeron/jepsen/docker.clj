(ns aeron.jepsen.docker
  "A remote that runs Jepsen's commands in a node's container, found by name
  (the node's name is its container's), with `docker exec` and `docker cp`."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [jepsen.control.core :as core]))

(defn- checked
  "Throws when a docker command failed."
  [what {:keys [exit err] :as result}]
  (when-not (zero? exit)
    (throw (ex-info (str what " failed: " err) result)))
  result)

(defrecord ContainerRemote [container]
  core/Remote
  (connect [this conn-spec]
    (assoc this :container (:host conn-spec)))
  (disconnect! [this]
    (assoc this :container nil))
  (execute! [_ _ctx action]
    (apply sh "docker" "exec" "-i" container "sh" "-c" (:cmd action)
           (when-let [in (:in action)] [:in in])))
  (upload! [_ _ctx local-paths remote-path _opts]
    (doseq [p (flatten [local-paths])]
      (checked "docker cp" (sh "docker" "cp" (str p) (str container ":" remote-path)))))
  (download! [_ _ctx remote-paths local-path _opts]
    ;; `docker cp` cannot read a tmpfs, where the nodes keep their data.
    (doseq [p (flatten [remote-paths])]
      (let [local  (io/file (str local-path))
            target (if (.isDirectory local) (io/file local (.getName (io/file (str p)))) local)
            {:keys [out]} (checked "docker exec cat"
                                   (sh "docker" "exec" container "cat" (str p) :out-enc :bytes))]
        (io/make-parents target)
        (with-open [o (io/output-stream target)]
          (.write o ^bytes out))))))

(def remote
  "Commands in the node's container."
  (->ContainerRemote nil))
