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
# KEEP=1 keeps the work directory; RESTART=1 also kills every process after
# the snapshot and more traffic, restarts them from what is on disk, and
# sends more before the shutdown.
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

pids=()
node_pids=()
cleanup() {
  for pid in "${node_pids[@]}" "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  for _ in $(seq 50); do
    alive=0
    for pid in "${node_pids[@]}" "${pids[@]}"; do kill -0 "$pid" 2>/dev/null && alive=1; done
    [[ $alive == 0 ]] && break
    sleep 0.1
  done
  for pid in "${node_pids[@]}" "${pids[@]}"; do kill -9 "$pid" 2>/dev/null || true; done
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

start_nodes() { # run label
  node_pids=()
  for ((n = 0; n < NODES; n++)); do
    "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" "$HERE/java/JavaNode.java" \
      "$WORK" "$n" "$NODES" "$WORK/aeron-$n" "$PORT_BASE" 2 "1=oracle-silent" >>"$WORK/node-$n.log" 2>&1 &
    node_pids+=($!)
    "$RUST_NODE" "$WORK/aeron-$n" "$WORK/node$n/cluster" 0 "$WORK/node$n-service0.trace" >>"$WORK/rust-$n.log" 2>&1 &
    node_pids+=($!)
  done
  for ((n = 0; n < NODES; n++)); do
    await_line "$WORK/node-$n.log" "READY" || fail "java node $n"
    await_line "$WORK/rust-$n.log" "READY" || fail "rust service $n"
  done
}

client() { # count
  "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" "$HERE/java/EchoClient.java" "$WORK/aeron-client" "$PORT_BASE" "$NODES" "$1" \
    || fail "client"
}

cluster_tool() { # node command
  "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" io.aeron.cluster.ClusterTool "$WORK/node$1/cluster" "$2"
}

[[ -x "$DRIVER" && -x "$RUST_NODE" ]] || fail "build first: cargo build -p rusteron-media-driver --bin media_driver --features static && cargo build -p rusteron-cluster-interop --features static"

for ((n = 0; n < NODES; n++)); do start_driver "$WORK/aeron-$n" "$WORK/driver-$n.log"; done
start_driver "$WORK/aeron-client" "$WORK/driver-client.log"

start_nodes
client "$COUNT"
leader=0
for ((n = 0; n < NODES; n++)); do cluster_tool "$n" describe 2>/dev/null | grep -q "role=LEADER" && leader=$n; done
cluster_tool "$leader" snapshot || fail "snapshot"
client "$COUNT"

if [[ "${RESTART:-0}" == 1 ]]; then
  sleep 1 # every service applies what was committed before the kill
  for pid in "${node_pids[@]}"; do kill -9 "$pid" 2>/dev/null || true; done
  wait "${node_pids[@]}" 2>/dev/null || true
  # Aeron refuses to start over a mark file active in the last 10 s, the Rust container as Java's archive does.
  sleep 11
  start_nodes
  client "$COUNT"
fi

cluster_tool "$leader" shutdown || fail "shutdown"
for pid in "${node_pids[@]}"; do wait "$pid" || fail "a node exited with $? (logs in $WORK)"; done

for ((n = 0; n < NODES; n++)); do
  grep -q "EXIT Terminated" "$WORK/rust-$n.log" || fail "rust service $n did not terminate cleanly"
  if ! diff -u "$WORK/node$n-service0.trace" "$WORK/node$n-service1.trace" >"$WORK/trace-$n.diff"; then
    head -20 "$WORK/trace-$n.diff" >&2
    fail "node $n: the Rust and Java services' traces differ"
  fi
  echo "node $n: $(wc -l <"$WORK/node$n-service0.trace") trace lines alike"
  cluster_tool "$n" recording-log >"$WORK/recording-log-$n.txt" 2>&1
  python3 -I "$HERE/scripts/compare_snapshots.py" "$WORK/recording-log-$n.txt" "$WORK/node$n/archive" \
    || fail "node $n: the snapshots differ"
  cluster_tool "$n" errors >"$WORK/errors-$n.txt" 2>&1
  errors=$(grep -c "observations from" "$WORK/errors-$n.txt" || true)
  [[ "$errors" == 0 ]] || { grep -A2 "observations from" "$WORK/errors-$n.txt" >&2; fail "node $n: errors logged"; }
done
echo "OK"
