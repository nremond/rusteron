#!/usr/bin/env bash
# The Rust service container beside Java's on one log: a Java Aeron Cluster
# 1.53.3 consensus module and archive, the Rust test service as service 0,
# Java's twin of it as service 1 (answering no client), each node on its own
# C media driver. A client sends messages, ClusterTool takes a snapshot, the
# client sends more, ClusterTool shuts the cluster down. Passes when every
# echo came back, both services exited cleanly, and their traces match line
# for line.
#
#   scripts/differential.sh [NODES] [COUNT]
#
# SCENARIO picks what happens between the snapshot and the shutdown:
#   basic         nothing more;
#   restart       every process is killed (-9), and restarted from disk;
#   timers        the client asks each service for 10,000 timers at once;
#   failover      (3 nodes) the leader's processes are killed, the client
#                 sends through the new leader, the node is restarted;
#   uncommitted   (3 nodes) the leader is killed while a client sends, so it
#                 dies with entries not yet committed; then as failover;
#   empty-follower (3 nodes) a follower is killed and its disk wiped: it
#                 starts again from an empty log and catches up;
#   quorum-loss   (3 nodes) the leader and a follower are killed, the third
#                 cannot elect, then both come back;
#   snapshot-load the snapshot is taken while a client sends;
#   marks         Java checks the Rust mark file: readable, never torn, and
#                 refused to a Java container of the same service;
#   dead-service  the Rust service is killed: the consensus module must see
#                 its Aeron client go, and the run ends there.
# ARCHIVE=rust runs the Rust archive beside each node instead of Java's (killed and restarted with it).
# CM=rust runs the Rust consensus module beside each node instead of Java's, with JavaNode's settings;
# CM_NODES="0 2" beside those nodes only, Java's on the others.
# CLIENT=rust runs the Rust client (rusteron-cluster's AeronCluster, the echo_client binary) instead of Java's.
# KEEP=1 keeps the work directory.
set -euo pipefail

NODES=${1:-1}
COUNT=${2:-1000}
HERE=$(cd "$(dirname "$0")/.." && pwd)
REPO=$(cd "$HERE/.." && pwd)
JAR=${AERON_ALL_JAR:-$HOME/.m2/repository/io/aeron/aeron-all/1.53.3/aeron-all-1.53.3.jar}
JAVA=${JAVA:-java}
PORT_BASE=${PORT_BASE:-22000}
JVM_FLAGS=(--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED
  --sun-misc-unsafe-memory-access=allow)
DRIVER=$REPO/target/debug/media_driver
RUST_NODE=$REPO/target/debug/rust_service_node
ARCHIVE=${ARCHIVE:-java}
ARCHIVE_BIN=${ARCHIVE_BIN:-$REPO/target/debug/aeron_archive}
[[ $ARCHIVE == rust ]] && JVM_FLAGS+=(-Dcluster.archive.external=true)
CM=${CM:-java}
CM_BIN=${CM_BIN:-$REPO/target/debug/consensus_module}
if [[ -z "${CM_NODES:-}" && $CM == rust ]]; then CM_NODES=$(seq -s ' ' 0 $((NODES - 1))); fi
CM_NODES=${CM_NODES:-}
rust_cm() { [[ " $CM_NODES " == *" $1 "* ]]; }
CLIENT=${CLIENT:-java}
ECHO_CLIENT=${ECHO_CLIENT:-$REPO/target/debug/echo_client}
echo_client() { # AERON_DIR PORT_BASE NODES COUNT [FIRST]: Java's EchoClient, or the Rust one
  if [[ $CLIENT == rust ]]; then
    "$ECHO_CLIENT" "$@"
  else
    "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" "$HERE/java/EchoClient.java" "$@"
  fi
}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/rusteron-cluster.XXXXXX")

SCENARIO=${SCENARIO:-basic}
pids=()
node_log_lines=()
java_pid=()
rust_pid=()
archive_pid=()
cm_pid=()
all_pids() { echo "${java_pid[@]:-}" "${rust_pid[@]:-}" "${archive_pid[@]:-}" "${cm_pid[@]:-}" "${pids[@]:-}"; }
cleanup() {
  for pid in $(all_pids); do kill "$pid" 2>/dev/null || true; done
  for _ in $(seq 50); do
    alive=0
    for pid in $(all_pids); do kill -0 "$pid" 2>/dev/null && alive=1; done
    [[ $alive == 0 ]] && break
    sleep 0.1
  done
  for pid in $(all_pids); do kill -9 "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [[ "${KEEP:-0}" == 1 ]]; then echo "work directory: $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

fail() { echo "FAILED: $*" >&2; exit 1; }

start_driver() { # aeron_dir log
  AERON_DIR=$1 AERON_DIR_DELETE_ON_START=true AERON_DIR_DELETE_ON_SHUTDOWN=true AERON_THREADING_MODE=SHARED \
    AERON_TERM_BUFFER_SPARSE_FILE=true "$DRIVER" >>"$2" 2>&1 &
  pids+=($!)
  for _ in $(seq 100); do [[ -f "$1/cnc.dat" ]] && return 0; sleep 0.1; done
  fail "driver at $1 did not start; see $2"
}

await_line() { # file pattern [tenths]
  for _ in $(seq "${3:-300}"); do grep -q "$2" "$1" 2>/dev/null && return 0; sleep 0.1; done
  echo "no '$2' in $1:" >&2
  tail -30 "$1" >&2
  return 1
}

start_node() { # n
  local n=$1
  if [[ $ARCHIVE == rust ]]; then
    # JavaNode's archive, in Rust: its control channel, its local one, its replication channel.
    "$ARCHIVE_BIN" --aeron-dir "$WORK/aeron-$n" --archive-dir "$WORK/node$n/archive" --archive-id "$n" \
      --control-channel "aeron:udp?endpoint=localhost:$((PORT_BASE + n * 100 + 1))|term-length=64k" \
      --local-control-channel "aeron:ipc?term-length=64k" --replication-channel "aeron:udp?endpoint=localhost:0" \
      --archive-client-response-channel "aeron:udp?endpoint=localhost:0" >>"$WORK/archive-$n.log" 2>&1 &
    archive_pid[n]=$!
    await_line "$WORK/archive-$n.log" "READY" || fail "rust archive $n"
  fi
  local flags=("${JVM_FLAGS[@]}")
  rust_cm "$n" && flags+=(-Dcluster.consensus.external=true)
  touch "$WORK/node-$n.log"
  node_log_lines[n]=$(wc -l <"$WORK/node-$n.log")
  "$JAVA" -cp "$JAR" "${flags[@]}" "$HERE/java/JavaNode.java" \
    "$WORK" "$n" "$NODES" "$WORK/aeron-$n" "$PORT_BASE" 2 "1=oracle-silent" >>"$WORK/node-$n.log" 2>&1 &
  java_pid[n]=$!
  if rust_cm "$n"; then
    # JavaNode's consensus module, in Rust: its members, ingress, replication, archive and timeouts.
    "$CM_BIN" --aeron-dir "$WORK/aeron-$n" --cluster-dir "$WORK/node$n/cluster" --cluster-member-id "$n" \
      --cluster-members "$(members)" --service-count 2 --ingress-channel "aeron:udp?term-length=64k" \
      --replication-channel "aeron:udp?endpoint=localhost:0" --archive-control-channel "aeron:ipc?term-length=64k" \
      --startup-canvass-timeout 5s --leader-heartbeat-timeout 1000ms --leader-heartbeat-interval 100ms \
      --election-timeout 500ms >>"$WORK/cm-$n.log" 2>&1 &
    cm_pid[n]=$!
    # JavaNode keeps the archive up until this module has ended.
    mkdir -p "$WORK/node$n" && echo "${cm_pid[n]}" >"$WORK/node$n/consensus-module.pid"
  fi
  "$RUST_NODE" "$WORK/aeron-$n" "$WORK/node$n/cluster" 0 "$WORK/node$n-service0.trace" >>"$WORK/rust-$n.log" 2>&1 &
  rust_pid[n]=$!
}

# JavaNode's members string: id, then ingress, consensus, log, catchup and archive endpoints.
members() {
  local m n
  for ((n = 0; n < NODES; n++)); do
    m+="$n"
    for offset in 2 3 4 5 1; do m+=",localhost:$((PORT_BASE + n * 100 + offset))"; done
    m+="|"
  done
  echo "$m"
}

await_node() { # n
  await_line "$WORK/node-$1.log" "READY" || fail "java node $1"
  if rust_cm "$1"; then await_line "$WORK/cm-$1.log" "READY" || fail "rust consensus module $1"; fi
  await_line "$WORK/rust-$1.log" "READY" || fail "rust service $1"
}

# READY comes before a node's first election ends: a node restarted catches up in it. A node has joined once
# JavaNode prints ELECTION CLOSED since its last start.
await_joined() { # n
  for _ in $(seq 600); do
    [[ $(tail -n +$((node_log_lines[$1] + 1)) "$WORK/node-$1.log" | grep '^ELECTION ' | tail -1) == "ELECTION CLOSED "* ]] &&
      return 0
    sleep 0.1
  done
  fail "node $1 did not join within 60 s (logs in $WORK)"
}

# A process's exit status once it ends; still running after 60 s is a failure.
await_exit() { # pid name
  for _ in $(seq 600); do kill -0 "$1" 2>/dev/null || break; sleep 0.1; done
  kill -0 "$1" 2>/dev/null && fail "$2 still running 60 s after the shutdown (logs in $WORK)"
  wait "$1"
}

start_nodes() {
  for ((n = 0; n < NODES; n++)); do start_node "$n"; done
  for ((n = 0; n < NODES; n++)); do await_node "$n"; done
}

# A node killed is its machine gone: its media driver with it, or the driver
# keeps the dead processes' endpoints alive for its client liveness timeout.
restart_driver() { # n
  start_driver "$WORK/aeron-$1" "$WORK/driver-$1.log"
  driver_pid[$1]=${pids[${#pids[@]} - 1]}
}

kill_node() { # n
  kill -9 "${java_pid[$1]}" "${rust_pid[$1]}" "${archive_pid[$1]:-}" "${cm_pid[$1]:-}" "${driver_pid[$1]}" 2>/dev/null || true
  wait "${java_pid[$1]}" "${rust_pid[$1]}" "${archive_pid[$1]:-}" "${cm_pid[$1]:-}" "${driver_pid[$1]}" 2>/dev/null || true
}

leader=0
client() { # count [first]
  local out attempt
  for attempt in 1 2 3; do
    # A connect that times out (no leader yet, after a failover) is tried again; anything else fails.
    if out=$(echo_client "$WORK/aeron-client" "$PORT_BASE" "$NODES" "$@" 2>&1); then
      break
    fi
    grep -q "cluster connect timeout" <<<"$out" && [[ $attempt -lt 3 ]] || fail "client: $out"
  done
  echo "$out"
  leader=$(sed -n 's/.*leader=\([0-9]*\).*/\1/p' <<<"$out")
}

cluster_tool() { # node command
  "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" io.aeron.cluster.ClusterTool "$WORK/node$1/cluster" "$2"
}

[[ $ARCHIVE != rust || -x "$ARCHIVE_BIN" ]] || fail "build the archive: cargo build -p rusteron-archive-server --features static"
[[ -z "$CM_NODES" || -x "$CM_BIN" ]] || fail "build the consensus module: cargo build -p rusteron-cluster --features static --bin consensus_module"
[[ $CLIENT != rust || -x "$ECHO_CLIENT" ]] || fail "build the client: cargo build -p rusteron-cluster-interop --features static"
[[ -x "$DRIVER" && -x "$RUST_NODE" ]] || fail "build first: cargo build -p rusteron-media-driver --bin media_driver --features static && cargo build -p rusteron-cluster-interop --features static"

driver_pid=()
for ((n = 0; n < NODES; n++)); do
  start_driver "$WORK/aeron-$n" "$WORK/driver-$n.log"
  driver_pid[n]=${pids[${#pids[@]} - 1]}
done
start_driver "$WORK/aeron-client" "$WORK/driver-client.log"

start_nodes
client "$COUNT"

if [[ "$SCENARIO" == dead-service ]]; then
  kill -9 "${rust_pid[0]}"
  for _ in $(seq 300); do
    if cluster_tool 0 errors 2>&1 | grep -q "Aeron client in service closed unexpectedly: serviceId=0"; then
      echo "the consensus module saw the Rust service's client go"
      echo "OK"
      exit 0
    fi
    sleep 0.1
  done
  fail "the consensus module did not see the Rust service's client go"
fi

cluster_tool "$leader" snapshot || fail "snapshot"
client "$COUNT"

case "$SCENARIO" in
  basic) ;;
  restart)
    sleep 1 # every service applies what was committed before the kill
    for ((n = 0; n < NODES; n++)); do kill_node "$n"; done
    # Aeron refuses to start over a mark file active in the last 10 s, the Rust container as Java's archive does.
    sleep 11
    for ((n = 0; n < NODES; n++)); do restart_driver "$n"; done
    start_nodes
    client "$COUNT"
    ;;
  timers)
    client "$COUNT" timers
    for _ in $(seq 300); do
      [[ $(grep -c "^timer" "$WORK/node0-service1.trace") -ge 20000 ]] && break
      sleep 0.1
    done
    ;;
  failover | uncommitted)
    [[ $NODES == 3 ]] || fail "$SCENARIO needs 3 nodes"
    killed=$leader
    if [[ "$SCENARIO" == uncommitted ]]; then
      # The leader dies mid-stream: that client loses its echoes, the next sends through the new leader. The Rust
      # client is up in milliseconds, where Java's waits for its JVM: it is given ten times as many to send.
      cut_count=$((COUNT * 20))
      if [[ $CLIENT == rust ]]; then cut_count=$((COUNT * 200)); fi
      echo_client "$WORK/aeron-client" "$PORT_BASE" "$NODES" "$cut_count" >"$WORK/client-cut.log" 2>&1 &
      cut_client=$!
      sleep 0.5
    else
      sleep 1 # every service applies what was committed before the kill
    fi
    kill_node "$killed"
    killed_at=$SECONDS
    if [[ "$SCENARIO" == uncommitted ]]; then
      wait "$cut_client" || true
      if grep -q "^OK received=$cut_count " "$WORK/client-cut.log"; then
        fail "the client had all its echoes before the leader was killed: nothing was left uncommitted"
      fi
      # Killed mid-stream, its two services stopped at different entries: one trace a prefix of the other. They
      # are set aside; from the restart on, the two must agree line for line again.
      t0=$WORK/node$killed-service0.trace
      t1=$WORK/node$killed-service1.trace
      shorter=$(($(wc -l <"$t0") < $(wc -l <"$t1") ? $(wc -l <"$t0") : $(wc -l <"$t1")))
      cmp -s <(head -n "$shorter" "$t0") <(head -n "$shorter" "$t1") ||
        fail "node $killed: its services' traces up to the kill are not one a prefix of the other"
      mv "$t0" "$t0.before-kill"
      mv "$t1" "$t1.before-kill"
    fi
    client "$COUNT"
    [[ $leader != "$killed" ]] || fail "node $killed still leads"
    sleep $((killed_at + 11 - SECONDS > 0 ? killed_at + 11 - SECONDS : 0))
    restart_driver "$killed"
    start_node "$killed"
    await_node "$killed"
    client "$COUNT"
    ;;
  empty-follower)
    [[ $NODES == 3 ]] || fail "empty-follower needs 3 nodes"
    sleep 1 # every service applies what was committed before the kill
    wiped=$(((leader + 1) % NODES))
    kill_node "$wiped"
    rm -rf "$WORK/node$wiped" "$WORK/node$wiped-service0.trace" "$WORK/node$wiped-service1.trace"
    mkdir -p "$WORK/node$wiped"
    client "$COUNT"
    restart_driver "$wiped"
    start_node "$wiped"
    await_node "$wiped"
    client "$COUNT"
    ;;
  quorum-loss)
    [[ $NODES == 3 ]] || fail "quorum-loss needs 3 nodes"
    sleep 1 # every service applies what was committed before the kill
    first=$leader
    second=$(((leader + 1) % NODES))
    kill_node "$first"
    kill_node "$second"
    sleep 11 # the third alone cannot elect; the mark files go stale meanwhile
    for n in "$first" "$second"; do restart_driver "$n"; done
    for n in "$first" "$second"; do start_node "$n"; done
    for n in "$first" "$second"; do await_node "$n"; done
    client "$COUNT"
    ;;
  snapshot-load)
    echo_client "$WORK/aeron-client" "$PORT_BASE" "$NODES" "$((COUNT * 20))" >"$WORK/client-load.log" 2>&1 &
    load_client=$!
    sleep 0.5
    cluster_tool "$leader" snapshot || fail "snapshot under load"
    wait "$load_client" || fail "client under load: $(tail -3 "$WORK/client-load.log")"
    ;;
  marks)
    "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" "$HERE/java/MarkFileChecks.java" "$WORK/node0/cluster" "$WORK/aeron-0" 0 0 5000 \
      || fail "mark file checks"
    ;;
  *) fail "unknown scenario $SCENARIO" ;;
esac

# The leader sends the termination position once, and a member takes it only in the term it is in (Java's
# ConsensusModuleAgent as Rust's): one still in its election would miss it and wait for good.
for ((n = 0; n < NODES; n++)); do await_joined "$n"; done
sleep 1 # every service applies what was committed before the shutdown
cluster_tool "$leader" shutdown || fail "shutdown"
for ((n = 0; n < NODES; n++)); do
  await_exit "${java_pid[$n]}" "java node $n" || fail "java node $n exited with $? (logs in $WORK)"
  await_exit "${rust_pid[$n]}" "rust service $n" || fail "rust service $n exited with $? (logs in $WORK)"
  if rust_cm "$n"; then
    await_exit "${cm_pid[$n]}" "rust consensus module $n" || fail "rust consensus module $n exited with $? (logs in $WORK)"
    grep -q "^EXIT Terminated" "$WORK/cm-$n.log" || fail "rust consensus module $n did not terminate cleanly"
  fi
done
if [[ $ARCHIVE == rust ]]; then
  for ((n = 0; n < NODES; n++)); do
    kill -TERM "${archive_pid[$n]}" 2>/dev/null || true
    wait "${archive_pid[$n]}" 2>/dev/null || true
    grep -q "^EXIT" "$WORK/archive-$n.log" || fail "rust archive $n did not stop cleanly"
    if grep -E "^(WARN|ERROR)" "$WORK/archive-$n.log" | grep -v "WARN - control request publication image unavailable" >&2; then
      echo "rust archive $n logged the above" >&2
    fi
    # Java's ArchiveTool on the catalog and every segment file the Rust archive wrote.
    "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" io.aeron.archive.ArchiveTool "$WORK/node$n/archive" verify -a \
      >"$WORK/archive-$n-verify.txt" 2>&1 || fail "node $n: ArchiveTool verify failed (logs in $WORK)"
    echo "node $n: ArchiveTool verify passed on the Rust archive"
  done
fi

for ((n = 0; n < NODES; n++)); do
  grep -q "EXIT Terminated" "$WORK/rust-$n.log" || fail "rust service $n did not terminate cleanly"
  if ! diff -u "$WORK/node$n-service0.trace" "$WORK/node$n-service1.trace" >"$WORK/trace-$n.diff"; then
    head -20 "$WORK/trace-$n.diff" >&2
    fail "node $n: the Rust and Java services' traces differ"
  fi
  echo "node $n: $(wc -l <"$WORK/node$n-service0.trace") trace lines alike, $(grep -c '^timer' "$WORK/node$n-service0.trace") timers"
  cluster_tool "$n" recording-log >"$WORK/recording-log-$n.txt" 2>&1
  python3 -I "$HERE/scripts/compare_snapshots.py" "$WORK/recording-log-$n.txt" "$WORK/node$n/archive" \
    || fail "node $n: the snapshots differ"
  cluster_tool "$n" errors >"$WORK/errors-$n.txt" 2>&1
  # No error from either container, ever; from the consensus module, only what the scenario's fault causes, and
  # one warning at its close: it sends its services a termination position once more (ConsensusModuleAgent
  # .onClose), and when both have acked theirs and gone, as their exits above show, that offer finds no one.
  allowed='failed to send service termination position: result=NOT_CONNECTED'
  # A fault's elections: any of the consensus module's warnings about them, but never a truncation refused or
  # acks out of sequence.
  case "$SCENARIO" in failover | uncommitted | empty-follower | quorum-loss) allowed='.' ;; esac
  forbidden='refusing to truncate|ack out of sequence'
  errors=$(awk -v allowed="$allowed" -v forbidden="$forbidden" '
    /^Type: / { component = $2 }
    /observations from/ {
      getline text
      if (component != "CONSENSUS_MODULE" || text !~ allowed || text ~ forbidden) print component ": " text
    }
  ' "$WORK/errors-$n.txt")
  [[ -z "$errors" ]] || { echo "$errors" >&2; fail "node $n: errors logged"; }
done
echo "OK"
