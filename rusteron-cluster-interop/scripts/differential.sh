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
#   marks         Java checks the Rust mark file: readable, never torn, and
#                 refused to a Java container of the same service;
#   dead-service  the Rust service is killed: the consensus module must see
#                 its Aeron client go, and the run ends there.
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
WORK=$(mktemp -d "${TMPDIR:-/tmp}/rusteron-cluster.XXXXXX")

SCENARIO=${SCENARIO:-basic}
pids=()
java_pid=()
rust_pid=()
all_pids() { echo "${java_pid[@]:-}" "${rust_pid[@]:-}" "${pids[@]:-}"; }
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
  "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" "$HERE/java/JavaNode.java" \
    "$WORK" "$n" "$NODES" "$WORK/aeron-$n" "$PORT_BASE" 2 "1=oracle-silent" >>"$WORK/node-$n.log" 2>&1 &
  java_pid[n]=$!
  "$RUST_NODE" "$WORK/aeron-$n" "$WORK/node$n/cluster" 0 "$WORK/node$n-service0.trace" >>"$WORK/rust-$n.log" 2>&1 &
  rust_pid[n]=$!
}

await_node() { # n
  await_line "$WORK/node-$1.log" "READY" || fail "java node $1"
  await_line "$WORK/rust-$1.log" "READY" || fail "rust service $1"
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
  kill -9 "${java_pid[$1]}" "${rust_pid[$1]}" "${driver_pid[$1]}" 2>/dev/null || true
  wait "${java_pid[$1]}" "${rust_pid[$1]}" "${driver_pid[$1]}" 2>/dev/null || true
}

leader=0
client() { # count [first]
  local out attempt
  for attempt in 1 2 3; do
    # A connect that times out (no leader yet, after a failover) is tried again; anything else fails.
    if out=$("$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" "$HERE/java/EchoClient.java" "$WORK/aeron-client" "$PORT_BASE" \
      "$NODES" "$@" 2>&1); then
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
  failover)
    [[ $NODES == 3 ]] || fail "failover needs 3 nodes"
    sleep 1 # every service applies what was committed before the kill
    killed=$leader
    kill_node "$killed"
    killed_at=$SECONDS
    client "$COUNT"
    [[ $leader != "$killed" ]] || fail "node $killed still leads"
    sleep $((killed_at + 11 - SECONDS > 0 ? killed_at + 11 - SECONDS : 0))
    restart_driver "$killed"
    start_node "$killed"
    await_node "$killed"
    client "$COUNT"
    ;;
  marks)
    "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" "$HERE/java/MarkFileChecks.java" "$WORK/node0/cluster" "$WORK/aeron-0" 0 0 5000 \
      || fail "mark file checks"
    ;;
  *) fail "unknown scenario $SCENARIO" ;;
esac

sleep 1 # every service applies what was committed before the shutdown
cluster_tool "$leader" shutdown || fail "shutdown"
for ((n = 0; n < NODES; n++)); do
  wait "${java_pid[$n]}" || fail "java node $n exited with $? (logs in $WORK)"
  wait "${rust_pid[$n]}" || fail "rust service $n exited with $? (logs in $WORK)"
done

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
  # No error from either container, ever; from the consensus module, only what the scenario's fault causes.
  allowed='^$'
  [[ "$SCENARIO" == failover ]] && allowed='leader heartbeat timeout'
  errors=$(awk -v allowed="$allowed" '
    /^Type: / { component = $2 }
    /observations from/ { getline text; if (component != "CONSENSUS_MODULE" || text !~ allowed) print component ": " text }
  ' "$WORK/errors-$n.txt")
  [[ -z "$errors" ]] || { echo "$errors" >&2; fail "node $n: errors logged"; }
done
echo "OK"
