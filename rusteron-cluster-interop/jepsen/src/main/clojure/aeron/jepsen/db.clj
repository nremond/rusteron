(ns aeron.jepsen.db
  "A member of the cluster in its container: started, killed, paused and
  wiped. `--impl java` runs Java's media driver, archive and consensus
  module in the node's JVM; `--impl rust` runs rusteron's C media driver,
  Rust archive and Rust consensus module beside it, with the same settings.
  The service is Java's container either way."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util]]))

(def aeron-dir "/dev/shm/aeron")

(def classpath
  "/opt/aeron-jepsen/target/classes:/opt/m2/io/aeron/aeron-all/1.53.3/aeron-all-1.53.3.jar")

(def jvm-flags
  ["--add-opens" "java.base/jdk.internal.misc=ALL-UNNAMED"
   "--add-opens" "java.base/java.util.zip=ALL-UNNAMED"
   "--sun-misc-unsafe-memory-access=allow"
   "-Xmx512m"])

(defn index
  "A node's member id: its place in the test's nodes."
  [test node]
  (.indexOf ^java.util.List (:nodes test) node))

(defn ip
  "n<k> is at 10.77.0.<10+k>: see scripts/cluster.sh."
  [node]
  (str "10.77.0." (+ 10 (Long/parseLong (subs node 1)))))

(def host-ip
  "The host, where Jepsen's clients are, on the nodes' network."
  "10.77.0.1")

(defn java
  "A Java command line on a node."
  [main & args]
  (str/join " " (concat ["java"] jvm-flags ["-cp" classpath main] (map str args))))

(defn probe!
  "Runs ClusterProbe on the current node: its output, or nil when it exits
  non-zero (an await timing out)."
  [& args]
  (try (c/exec* (java "aeron.jepsen.ClusterProbe" aeron-dir (str/join " " args)))
       (catch Exception _ nil)))

(defn parse-state
  "role=2 election=17 ... as a map of longs."
  [line]
  (when line
    (->> (re-seq #"(\w+)=(-?\d+)" line)
         (map (fn [[_ k v]] [(keyword k) (parse-long v)]))
         (into {}))))

(defn- timeouts
  [opts]
  ["--heartbeat-timeout-ms" (:heartbeat-timeout-ms opts)
   "--heartbeat-interval-ms" 100
   "--election-timeout-ms" (:election-timeout-ms opts)
   "--startup-canvass-ms" 5000])

(defn- count-lines
  "How many lines of `file` match `pattern`."
  [file pattern]
  (or (parse-long (str/trim (c/exec* (str "grep -c '" pattern "' " file " 2>/dev/null || true")))) 0))

(defn- running?
  "Whether a process matching `pattern` runs."
  [pattern]
  (not (str/blank? (c/exec* (str "pgrep -f '" pattern "' || true")))))

(defn- launch!
  "Starts `cmd` in the background, its output appended to `file`."
  [file cmd]
  (c/exec* (str "echo START " (System/currentTimeMillis) " >> " file "; "
                "setsid " cmd " >> " file " 2>&1 < /dev/null &")))

(defn- await-ready
  "Waits for one more READY in `file`: :started, :exited (no process
  matching `pattern` left), or :timeout."
  [file before pattern]
  (loop [tries 150]
    (cond (< before (count-lines file "READY")) :started
          (not (running? pattern)) :exited
          (zero? tries) :timeout
          :else (do (Thread/sleep 100) (recur (dec tries))))))

(defn members
  "Java's members string, as JepsenNode builds it."
  [test]
  (->> (:nodes test)
       (map-indexed (fn [i node]
                      (let [h (ip node)]
                        (str i "," h ":9002," h ":9003," h ":9004," h ":9005," h ":8010|"))))
       (apply str)))

(def rust-bin "/opt/rusteron/debug")

(defn- start-rust!
  "rusteron's C media driver, Rust archive and Rust consensus module, with
  JepsenNode's settings."
  [opts test node]
  (let [h (ip node)
        n (index test node)]
    (c/exec* (str "rm -rf " aeron-dir))
    (launch! "/data/driver.log"
             (str "env AERON_DIR=" aeron-dir " AERON_DIR_DELETE_ON_START=true AERON_THREADING_MODE=SHARED"
                  " AERON_TERM_BUFFER_SPARSE_FILE=true " rust-bin "/media_driver"))
    (loop [tries 100]
      (when-not (= "yes" (str/trim (c/exec* (str "test -f " aeron-dir "/cnc.dat && echo yes || true"))))
        (when (zero? tries) (throw (ex-info "media driver did not start" {:node node})))
        (Thread/sleep 100)
        (recur (dec tries))))
    (let [before (count-lines "/data/archive.log" "READY")]
      (launch! "/data/archive.log"
               (str rust-bin "/aeron_archive --aeron-dir " aeron-dir " --archive-dir /data/archive --archive-id " n
                    " --control-channel 'aeron:udp?endpoint=" h ":8010|term-length=64k'"
                    " --local-control-channel 'aeron:ipc?term-length=64k'"
                    " --replication-channel 'aeron:udp?endpoint=" h ":0'"
                    " --archive-client-response-channel 'aeron:udp?endpoint=" h ":0'"))
      (when-not (= :started (await-ready "/data/archive.log" before "[a]eron_archive"))
        (throw (ex-info "rust archive did not start" {:node node}))))
    (let [before (count-lines "/data/cm.log" "READY")]
      (launch! "/data/cm.log"
               (str rust-bin "/consensus_module --aeron-dir " aeron-dir " --cluster-dir /data/cluster"
                    " --cluster-member-id " n " --cluster-members '" (members test) "'"
                    " --service-count 1 --ingress-channel 'aeron:udp?term-length=64k'"
                    " --log-channel 'aeron:udp?term-length=1m'"
                    " --replication-channel 'aeron:udp?endpoint=" h ":0'"
                    " --archive-control-channel 'aeron:ipc?term-length=64k'"
                    " --startup-canvass-timeout 5000ms"
                    " --leader-heartbeat-timeout " (:heartbeat-timeout-ms opts) "ms"
                    " --leader-heartbeat-interval 100ms"
                    " --election-timeout " (:election-timeout-ms opts) "ms"))
      (when-not (= :started (await-ready "/data/cm.log" before "[c]onsensus_module"))
        (throw (ex-info "rust consensus module did not start" {:node node}))))))

(declare kill-node! start-member!)

(defn- member-processes
  "What runs for a member: rusteron's driver, archive and consensus module
  beside the JVM, or the JVM alone."
  [opts]
  (cond-> ["[a]eron.jepsen.JepsenNode"]
    (= "rust" (:impl opts)) (into ["[m]edia_driver" "[a]eron_archive" "[c]onsensus_module"])))

(defn start-node!
  "Starts the member, unless all its processes run; a member partly up is
  killed and started afresh. Waits for its READY."
  [opts test node]
  (if (every? running? (member-processes opts))
    :already-running
    (do (kill-node!)
        (start-member! opts test node))))

(defn- start-member!
  "Starts the member's processes. A member killed moments ago leaves mark
  files that look active for a few seconds, and Java's archive, consensus
  module and service container refuse to start on them: the JVM is started
  again until they have gone stale, as an operator restarting after a crash
  would."
  [opts test node]
  (when (= "rust" (:impl opts))
    (start-rust! opts test node))
  (let [hosts (str/join "," (map ip (:nodes test)))
        args  (concat ["--id" (index test node) "--hosts" hosts] (timeouts opts)
                      (when (= "rust" (:impl opts)) ["--external" "driver,archive,consensus"]))]
    (loop [attempts 30]
      (let [before (count-lines "/data/node.log" "READY")]
        (launch! "/data/node.log" (apply java "aeron.jepsen.JepsenNode" args))
        (let [outcome (await-ready "/data/node.log" before "[a]eron.jepsen.JepsenNode")]
          (cond (= :started outcome) :started

                (and (= :exited outcome)
                     (pos? attempts)
                     (re-find #"active mark file detected"
                              (c/exec* "tail -n 20 /data/node.log")))
                (do (Thread/sleep 1000) (recur (dec attempts)))

                :else (throw (ex-info "node did not start" {:node node :outcome outcome}))))))))

(def processes
  "What a member's processes match: each name bracketed, so the shell
  running pkill does not match itself."
  "[a]eron.jepsen.JepsenNode|[m]edia_driver|[a]eron_archive|[c]onsensus_module")

(defn kill-node!
  []
  (c/exec* (str "pkill -9 -f '" processes "' || true"))
  (loop [tries 50]
    (when (and (pos? tries)
               (not (str/blank? (c/exec* (str "pgrep -f '" processes "' || true")))))
      (Thread/sleep 100)
      (recur (dec tries)))))

(defn db
  [opts]
  (reify
    db/DB
    (setup! [_ test node]
      (info "starting member" (index test node))
      (start-node! opts test node))
    (teardown! [_ _test _node]
      (kill-node!)
      (c/exec* "rm -rf /data/* /dev/shm/aeron*"))

    db/Kill
    (kill! [_ _test _node] (kill-node!))
    (start! [_ test node] (start-node! opts test node))

    db/Pause
    (pause! [_ _test _node] (c/exec* (str "pkill -STOP -f '" processes "' || true")))
    (resume! [_ _test _node] (c/exec* (str "pkill -CONT -f '" processes "' || true")))

    db/Primary
    (setup-primary! [_ test _node]
      ;; The cluster is up once one member leads and every member is past its election.
      (loop [tries 300]
        (let [states (vals (c/on-nodes test (fn [_ _] (parse-state (probe! "state")))))]
          (cond (and (= 1 (count (filter #(= 2 (:role %)) states)))
                     (every? #(= 17 (:election %)) states))
                (info "cluster elected:" states)

                (zero? tries) (throw (ex-info "no leader elected" {:states states}))

                :else (do (Thread/sleep 200) (recur (dec tries)))))))
    (primaries [_ test]
      (->> (c/on-nodes test (fn [_ _] (parse-state (probe! "state"))))
           (filter (fn [[_ state]] (= 2 (:role state))))
           (map key)))

    db/LogFiles
    (log-files [_ _test _node]
      ["/data/node.log" "/data/driver.log" "/data/archive.log" "/data/cm.log"])))
