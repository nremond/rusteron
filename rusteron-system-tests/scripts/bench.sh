#!/usr/bin/env bash
# Measures the Rust service container against Java's, on one log: one node of
# Java Aeron Cluster 1.54.2 (consensus module and archive, its cluster clock in
# nanoseconds), the Rust bench service as service 0 and Java's as service 1,
# each on the same C media driver, and an open-loop client on a driver of its
# own. Everything busy-spins, each on cores of its own. Release builds only.
#
# For each responder (the service that answers the client, the other silent)
# and each rate, the cluster is started afresh, the client sends at the rate
# for WARMUP s then MEASURE s, and three histograms of the measured messages
# are kept, in microseconds:
#   rust-apply, java-apply  from the log entry's timestamp (stamped by the
#                           consensus module as it appended it) to the
#                           service's apply: the same entries, side by side;
#   client-rtt              from when the client was due to send to the echo
#                           it got back from the responder.
#
#   scripts/bench.sh [RATES]          e.g. "1000 10000 100000"
#
# ROUNDS (3: every configuration that many times, interleaved, so drift on
# the machine shows as a range), RESPONDERS ("rust java"), WARMUP (15),
# MEASURE (20), PORT_BASE (23000) and OUT (a new directory under ~) override
# the defaults. Linux: needs taskset.
set -euo pipefail

RATES=${1:-"1000 10000 100000"}
ROUNDS=${ROUNDS:-3}
RESPONDERS=${RESPONDERS:-"rust java"}
WARMUP=${WARMUP:-15}
MEASURE=${MEASURE:-20}
PORT_BASE=${PORT_BASE:-23000}
HERE=$(cd "$(dirname "$0")/.." && pwd)
REPO=$(cd "$HERE/.." && pwd)
JAR=${AERON_ALL_JAR:-$HOME/.m2/repository/io/aeron/aeron-all/1.54.2/aeron-all-1.54.2.jar}
JAVA=${JAVA:-java}
JVM_FLAGS=(--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED
  --sun-misc-unsafe-memory-access=allow -XX:+UseZGC -Xms2g -Xmx2g -XX:+AlwaysPreTouch
  -Daeron.pre.touch.mapped.memory=true)
DRIVER=$REPO/target/release/media_driver
RUST_NODE=$REPO/target/release/rust_bench_node
OUT=${OUT:-$HOME/rusteron-bench-$(date +%Y%m%d-%H%M%S)}

# Cores, chosen for a Ryzen 9 7950X: two core complexes, each with its own L3 cache, CPUs 0-7 and
# 16-23 one, 8-15 and 24-31 the other. The whole node shares one complex, as a node does on one: its driver
# (conductor, sender, receiver), the Rust service, the Java node (consensus module, archive, Java service, and
# on the SMT siblings the JVM's own threads); CPU 0 is left to the system. The client and its driver take the
# other complex. A service on the other complex from its driver and consensus module pays each cache line of
# every message across the two: the first measurements put the Rust service there, and it showed.
NODE_DRIVER=(${NODE_DRIVER:-1 2 3})
RUST_CPUS=${RUST_CPUS:-4}
JAVA_NODE_CPUS=${JAVA_NODE_CPUS:-5-7,21-23}
CLIENT_DRIVER=(${CLIENT_DRIVER:-9 10 11})
CLIENT_CPUS=${CLIENT_CPUS:-12-15}

mkdir -p "$OUT"
pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  sleep 1
  for pid in "${pids[@]:-}"; do kill -9 "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  pids=()
}
trap cleanup EXIT

fail() { echo "FAILED: $*" >&2; exit 1; }

start_driver() { # aeron_dir log conductor sender receiver
  AERON_DIR=$1 AERON_DIR_DELETE_ON_START=true AERON_DIR_DELETE_ON_SHUTDOWN=true AERON_THREADING_MODE=DEDICATED \
    AERON_CONDUCTOR_IDLE_STRATEGY=spin AERON_SENDER_IDLE_STRATEGY=noop AERON_RECEIVER_IDLE_STRATEGY=noop \
    AERON_CONDUCTOR_CPU_AFFINITY=$3 AERON_SENDER_CPU_AFFINITY=$4 AERON_RECEIVER_CPU_AFFINITY=$5 \
    AERON_TERM_BUFFER_SPARSE_FILE=false AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY=true \
    taskset -c "$3,$4,$5" "$DRIVER" >>"$2" 2>&1 &
  pids+=($!)
  for _ in $(seq 100); do [[ -f "$1/cnc.dat" ]] && return 0; sleep 0.1; done
  fail "driver at $1 did not start; see $2"
}

await_line() { # file pattern
  for _ in $(seq 300); do grep -q "$2" "$1" 2>/dev/null && return 0; sleep 0.1; done
  tail -30 "$1" >&2
  fail "no '$2' in $1"
}

run() { # round responder rate
  local round=$1 responder=$2 rate=$3 work java_services rust_mode
  work=$(mktemp -d /dev/shm/rusteron-bench.XXXXXX)
  if [[ $responder == rust ]]; then java_services="1=bench-silent" rust_mode=""; else java_services="1=bench" rust_mode=silent; fi

  start_driver "$work/aeron-0" "$work/driver-0.log" "${NODE_DRIVER[@]}"
  start_driver "$work/aeron-client" "$work/driver-client.log" "${CLIENT_DRIVER[@]}"
  taskset -c $JAVA_NODE_CPUS "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" -Dcluster.bench=true "$HERE/java/JavaNode.java" \
    "$work" 0 1 "$work/aeron-0" "$PORT_BASE" 2 "$java_services" >>"$work/node-0.log" 2>&1 &
  local java_pid=$!
  pids+=($java_pid)
  taskset -c $RUST_CPUS "$RUST_NODE" "$work/aeron-0" "$work/node0/cluster" 0 "$work/node0-service0.bench" $rust_mode \
    >>"$work/rust-0.log" 2>&1 &
  local rust_pid=$!
  pids+=($rust_pid)
  await_line "$work/node-0.log" READY
  await_line "$work/rust-0.log" READY

  taskset -c $CLIENT_CPUS "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" "$HERE/java/BenchClient.java" \
    "$work/aeron-client" "$PORT_BASE" 1 "$rate" "$WARMUP" "$MEASURE" "$work/client.bench" client-rtt \
    >>"$work/client.log" 2>&1 || { tail -5 "$work/client.log" >&2; fail "client ($responder, $rate/s)"; }

  "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" io.aeron.cluster.ClusterTool "$work/node0/cluster" shutdown >/dev/null 2>&1 \
    || fail "shutdown"
  wait "$java_pid" || fail "java node exited with $?"
  wait "$rust_pid" || fail "rust service exited with $?"
  for report in client.bench node0-service0.bench node0-service1.bench; do
    [[ -s "$work/$report" ]] || fail "no $report"
    echo "$round $responder $rate $(cat "$work/$report")" >>"$OUT/results.txt"
  done
  cleanup
  cp "$work"/*.log "$OUT/" 2>/dev/null || true
  rm -rf "$work"
}

[[ -x "$DRIVER" && -x "$RUST_NODE" ]] || fail "build first: cargo build --release -p rusteron-media-driver --bin media_driver --features static && cargo build --release -p rusteron-system-tests --features static"
command -v taskset >/dev/null || fail "taskset not found"

{
  echo "# $(date -Is) $(uname -srm), $(nproc) CPUs, governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)"
  echo "# cores: node driver ${NODE_DRIVER[*]}, rust service $RUST_CPUS, java node $JAVA_NODE_CPUS, client driver ${CLIENT_DRIVER[*]}, client $CLIENT_CPUS"
  echo "# warm-up ${WARMUP}s, measured ${MEASURE}s; columns: round responder rate name count p50 p90 p99 p99.9 p99.99 max (us)"
} >"$OUT/results.txt"
for ((round = 1; round <= ROUNDS; round++)); do
  for rate in $RATES; do
    for responder in $RESPONDERS; do
      echo "== round $round, $responder answering, $rate/s"
      run "$round" "$responder" "$rate"
      tail -3 "$OUT/results.txt"
    done
  done
done
echo "results: $OUT/results.txt"
