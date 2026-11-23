#!/usr/bin/env bash
# Runs NODES Java Aeron Cluster 1.53.3 nodes, each on its own C media driver
# from rusteron-media-driver, with the Java echo service, then an echo client
# on a C driver of its own. Exits 0 when every echo came back in order.
#
#   scripts/skeleton.sh [NODES] [COUNT]
#
# AERON_ALL_JAR overrides the jar, JAVA the java binary, PORT_BASE the ports
# (node N uses PORT_BASE + 100 N + 1..5), KEEP=1 keeps the work directory.
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
WORK=$(mktemp -d "${TMPDIR:-/tmp}/rusteron-cluster.XXXXXX")

pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [[ "${KEEP:-0}" == 1 ]]; then echo "work directory: $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

start_driver() { # aeron_dir log
  AERON_DIR=$1 AERON_DIR_DELETE_ON_START=true AERON_DIR_DELETE_ON_SHUTDOWN=true AERON_THREADING_MODE=SHARED \
    AERON_TERM_BUFFER_SPARSE_FILE=true "$DRIVER" >"$2" 2>&1 &
  pids+=($!)
  for _ in $(seq 100); do [[ -f "$1/cnc.dat" ]] && return 0; sleep 0.1; done
  echo "driver at $1 did not start; see $2" >&2
  return 1
}

await_line() { # file pattern
  for _ in $(seq 300); do grep -q "$2" "$1" 2>/dev/null && return 0; sleep 0.1; done
  echo "no '$2' in $1:" >&2
  cat "$1" >&2
  return 1
}

[[ -x "$DRIVER" ]] || cargo build -q -p rusteron-media-driver --bin media_driver --manifest-path "$REPO/Cargo.toml"

for ((n = 0; n < NODES; n++)); do
  start_driver "$WORK/aeron-$n" "$WORK/driver-$n.log"
  "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" "$HERE/java/JavaNode.java" \
    "$WORK" "$n" "$NODES" "$WORK/aeron-$n" "$PORT_BASE" 1 "0=echo" >"$WORK/node-$n.log" 2>&1 &
  pids+=($!)
done
for ((n = 0; n < NODES; n++)); do await_line "$WORK/node-$n.log" READY; done

start_driver "$WORK/aeron-client" "$WORK/driver-client.log"
"$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" "$HERE/java/EchoClient.java" "$WORK/aeron-client" "$PORT_BASE" "$NODES" "$COUNT"
