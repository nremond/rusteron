#!/usr/bin/env bash
# Rust's cluster backup beside Java's ClusterBackup 1.53.3 on one cluster: three JavaNodes, each on its own C
# media driver with Java's echo service, and Java's consensus modules or Rust's. Java's backup (java/JavaBackup.java)
# runs on a Java archive of its own, Rust's (cluster_backup) on the Rust archive, each on its own C driver, both
# copying the log from the members' archives (SOURCE, default LEADER).
#
# A client sends messages, ClusterTool takes a snapshot, the client sends more; then both backups start, and the
# client sends more. Passes when both backups hold the log to the leader's commit position, the leader's log and
# both backups' give the same CRC-32C replayed to it (java/BackupCheck.java), and both backups' recording logs list
# the same entries.
#
#   scripts/backup.sh [COUNT]
#
# SCENARIO=failover then kills the leader's processes: both backups must follow the new leader, and agree with it
# again. CM=rust runs the Rust consensus module beside each node in place of Java's. Binaries are the release builds
# (BIN, default target/release): media_driver, aeron_archive, consensus_module, cluster_backup. KEEP=1 keeps the
# work directory.
set -euo pipefail

COUNT=${1:-1000}
NODES=3
HERE=$(cd "$(dirname "$0")/.." && pwd)
REPO=$(cd "$HERE/.." && pwd)
JAR=${AERON_ALL_JAR:-$HOME/.m2/repository/io/aeron/aeron-all/1.53.3/aeron-all-1.53.3.jar}
JAVA=${JAVA:-java}
PORT_BASE=${PORT_BASE:-23000}
JVM_FLAGS=(--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED
  --sun-misc-unsafe-memory-access=allow)
BIN=${BIN:-$REPO/target/release}
DRIVER=$BIN/media_driver
ARCHIVE_BIN=$BIN/aeron_archive
CM_BIN=$BIN/consensus_module
BACKUP_BIN=$BIN/cluster_backup
CM=${CM:-java}
SOURCE=${SOURCE:-LEADER}
SCENARIO=${SCENARIO:-basic}
JAVA_SLOT=$NODES
RUST_SLOT=$((NODES + 1))
WORK=$(mktemp -d "${TMPDIR:-/tmp}/rusteron-backup.XXXXXX")

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true; done
  for _ in $(seq 50); do
    alive=0
    for pid in "${pids[@]:-}"; do [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && alive=1; done
    [[ $alive == 0 ]] && break
    sleep 0.1
  done
  for pid in "${pids[@]:-}"; do [[ -n "$pid" ]] && kill -9 "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [[ "${KEEP:-0}" == 1 ]]; then echo "work directory: $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

fail() { echo "FAILED: $*" >&2; exit 1; }

port() { echo $((PORT_BASE + $1 * 100 + $2)); } # slot offset: 1 archive, 2 ingress, 3 consensus, 4 log, 5 catch-up

await_line() { # file pattern [tenths]
  for _ in $(seq "${3:-300}"); do grep -q "$2" "$1" 2>/dev/null && return 0; sleep 0.1; done
  echo "no '$2' in $1:" >&2
  tail -30 "$1" >&2
  return 1
}

start_driver() { # aeron_dir log
  AERON_DIR=$1 AERON_DIR_DELETE_ON_START=true AERON_DIR_DELETE_ON_SHUTDOWN=true AERON_THREADING_MODE=SHARED \
    AERON_TERM_BUFFER_SPARSE_FILE=true "$DRIVER" >>"$2" 2>&1 &
  pids+=($!)
  for _ in $(seq 100); do [[ -f "$1/cnc.dat" ]] && return 0; sleep 0.1; done
  fail "driver at $1 did not start; see $2"
}

members() {
  local m n
  for ((n = 0; n < NODES; n++)); do
    m+="$n"
    for offset in 2 3 4 5 1; do m+=",localhost:$(port "$n" "$offset")"; done
    m+="|"
  done
  echo "$m"
}

node_pids=()
start_node() { # n
  local n=$1 flags=("${JVM_FLAGS[@]}")
  start_driver "$WORK/aeron-$n" "$WORK/driver-$n.log"
  node_pids[n]="${pids[${#pids[@]} - 1]}"
  [[ $CM == rust ]] && flags+=(-Dcluster.consensus.external=true)
  "$JAVA" -cp "$JAR" "${flags[@]}" "$HERE/java/JavaNode.java" \
    "$WORK" "$n" "$NODES" "$WORK/aeron-$n" "$PORT_BASE" 1 "0=echo" >>"$WORK/node-$n.log" 2>&1 &
  pids+=($!)
  node_pids[n]+=" $!"
  if [[ $CM == rust ]]; then
    "$CM_BIN" --aeron-dir "$WORK/aeron-$n" --cluster-dir "$WORK/node$n/cluster" --cluster-member-id "$n" \
      --cluster-members "$(members)" --service-count 1 --ingress-channel "aeron:udp?term-length=64k" \
      --replication-channel "aeron:udp?endpoint=localhost:0" --archive-control-channel "aeron:ipc?term-length=64k" \
      --startup-canvass-timeout 5s --leader-heartbeat-timeout 1000ms --leader-heartbeat-interval 100ms \
      --election-timeout 500ms >>"$WORK/cm-$n.log" 2>&1 &
    pids+=($!)
    node_pids[n]+=" $!"
  fi
}

leader=0
client() { # count
  local out attempt
  for attempt in 1 2 3; do
    if out=$("$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" "$HERE/java/EchoClient.java" "$WORK/aeron-client" "$PORT_BASE" \
      "$NODES" "$1" 2>&1); then
      break
    fi
    grep -q "cluster connect timeout" <<<"$out" && [[ $attempt -lt 3 ]] || fail "client: $out"
  done
  echo "$out"
  leader=$(sed -n 's/.*leader=\([0-9]*\).*/\1/p' <<<"$out")
}

cluster_tool() { # dir command
  "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" io.aeron.cluster.ClusterTool "$1" "$2"
}

java_backup_pid=
rust_backup_pid=
start_backups() {
  start_driver "$WORK/aeron-backup-java" "$WORK/driver-backup-java.log"
  "$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" "$HERE/java/JavaBackup.java" "$WORK/backup-java" "$WORK/aeron-backup-java" \
    "$PORT_BASE" "$NODES" "$JAVA_SLOT" "$SOURCE" >>"$WORK/backup-java.log" 2>&1 &
  java_backup_pid=$!
  pids+=($!)

  start_driver "$WORK/aeron-backup-rust" "$WORK/driver-backup-rust.log"
  "$ARCHIVE_BIN" --aeron-dir "$WORK/aeron-backup-rust" --archive-dir "$WORK/backup-rust/archive" \
    --archive-id "$RUST_SLOT" --control-channel "aeron:udp?endpoint=localhost:$(port "$RUST_SLOT" 1)|term-length=64k" \
    --local-control-channel "aeron:ipc?term-length=64k" --replication-channel "aeron:udp?endpoint=localhost:0" \
    --archive-client-response-channel "aeron:udp?endpoint=localhost:0" >>"$WORK/archive-backup-rust.log" 2>&1 &
  pids+=($!)
  await_line "$WORK/archive-backup-rust.log" "READY" || fail "the Rust backup's archive"
  local endpoints n
  for ((n = 0; n < NODES; n++)); do endpoints+="${endpoints:+,}localhost:$(port "$n" 3)"; done
  "$BACKUP_BIN" --aeron-dir "$WORK/aeron-backup-rust" --cluster-dir "$WORK/backup-rust/cluster" --delete-dir-on-start \
    --cluster-consensus-endpoints "$endpoints" \
    --consensus-channel "aeron:udp?term-length=64k|endpoint=localhost:$(port "$RUST_SLOT" 3)" \
    --catchup-endpoint "localhost:$(port "$RUST_SLOT" 5)" --source-type "$SOURCE" \
    --cluster-backup-cool-down-interval 1s --cluster-archive-control-request-channel "aeron:udp?term-length=64k" \
    --cluster-archive-control-response-channel "aeron:udp?endpoint=localhost:0" --progress \
    >>"$WORK/backup-rust.log" 2>&1 &
  rust_backup_pid=$!
  pids+=($!)
  await_line "$WORK/backup-java.log" "READY" || fail "Java's backup"
  await_line "$WORK/backup-rust.log" "READY" || fail "Rust's backup"
}

progress() { # log: the backup's live log position, last printed
  { grep -o "^PROGRESS .*position=[0-9]*" "$1" || true; } | tail -1 | sed 's/.*position=//'
}

# Both backups' live log positions equal, past MIN, and unchanged for 2 s: the position. A backup prints its
# position only while backing up, not while it queries again (after a failover, its recording goes on meanwhile).
await_backups() { # [min]
  local min=${1:-1} java rust last= same=0
  for _ in $(seq 600); do
    java=$(progress "$WORK/backup-java.log")
    rust=$(progress "$WORK/backup-rust.log")
    if [[ -n "$java" && "$java" == "$rust" && "$java" == "$last" && $java -ge $min ]]; then
      same=$((same + 1))
      [[ $same -ge 20 ]] && { echo "$java"; return 0; }
    else
      same=0
    fi
    last=$java
    sleep 0.1
  done
  fail "the backups did not settle at one position: Java's at ${java:-none}, Rust's at ${rust:-none} (logs in $WORK)"
}

# The leader's log and both backups' replayed to the backups' position: one CRC-32C, and the leader recorded no
# further.
check_logs() {
  local position=$1 out
  out=$("$JAVA" -cp "$JAR" "${JVM_FLAGS[@]}" "$HERE/java/BackupCheck.java" "$WORK/aeron-client" "$PORT_BASE" \
    "$position" "localhost:$(port "$leader" 1)" "localhost:$(port "$JAVA_SLOT" 1)" "localhost:$(port "$RUST_SLOT" 1)") ||
    fail "replays: $out"
  echo "$out"
  [[ $(wc -l <<<"$out") == 3 ]] || fail "three replays expected"
  [[ $(awk '{print $NF}' <<<"$out" | sort -u | wc -l) == 1 ]] || fail "the logs differ"
  grep -q "^localhost:$(port "$leader" 1) .* recorded=$position " <<<"$out" ||
    fail "the leader recorded beyond the backups' position $position"
}

for d in "$DRIVER" "$ARCHIVE_BIN" "$BACKUP_BIN"; do [[ -x "$d" ]] || fail "build $d first (release, --features static)"; done
[[ $CM != rust || -x "$CM_BIN" ]] || fail "build $CM_BIN first"

start_driver "$WORK/aeron-client" "$WORK/driver-client.log"
for ((n = 0; n < NODES; n++)); do start_node "$n"; done
for ((n = 0; n < NODES; n++)); do
  await_line "$WORK/node-$n.log" "READY" || fail "java node $n"
  if [[ $CM == rust ]]; then await_line "$WORK/cm-$n.log" "READY" || fail "rust consensus module $n"; fi
done

client "$COUNT"
cluster_tool "$WORK/node$leader/cluster" snapshot || fail "snapshot"
client "$COUNT"
start_backups
client "$COUNT"
position=$(await_backups)
echo "both backups at $position"
check_logs "$position"

if [[ $SCENARIO == failover ]]; then
  killed=$leader
  # shellcheck disable=SC2086
  kill -9 ${node_pids[$killed]} 2>/dev/null || true
  echo "killed node $killed's processes"
  client "$COUNT"
  [[ $leader != "$killed" ]] || fail "node $killed still leads"
  client "$COUNT"
  position=$(await_backups $((position + 1)))
  echo "both backups at $position after the failover"
  check_logs "$position"
fi

kill -TERM "$rust_backup_pid" "$java_backup_pid"
for pid in "$rust_backup_pid" "$java_backup_pid"; do
  for _ in $(seq 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
done
grep -q "^EXIT Stopped" "$WORK/backup-rust.log" || fail "Rust's backup did not stop cleanly: $(tail -3 "$WORK/backup-rust.log")"
grep -q "^STOPPED" "$WORK/backup-java.log" || fail "Java's backup did not stop cleanly"
echo "responses: Java's $(grep -c '^RESPONSE' "$WORK/backup-java.log"), Rust's $(grep -c '^RESPONSE' "$WORK/backup-rust.log")"

cluster_tool "$WORK/backup-java/cluster" recording-log >"$WORK/recording-log-java.txt" 2>&1
cluster_tool "$WORK/backup-rust/cluster" recording-log >"$WORK/recording-log-rust.txt" 2>&1
cat "$WORK/recording-log-rust.txt"
diff -u "$WORK/recording-log-java.txt" "$WORK/recording-log-rust.txt" || fail "the recording logs differ"
cluster_tool "$WORK/backup-java/cluster" errors >"$WORK/errors-java.txt" 2>&1 || true
cluster_tool "$WORK/backup-rust/cluster" errors >"$WORK/errors-rust.txt" 2>&1 || true
for side in java rust; do
  if grep -q "observations from" "$WORK/errors-$side.txt"; then
    echo "$side backup's errors:"
    grep -A1 "observations from" "$WORK/errors-$side.txt" | grep -v "observations from\|^--"
  fi
done
if grep -A1 "observations from" "$WORK/errors-rust.txt" | grep -q "ERROR"; then
  fail "Rust's backup logged errors"
fi
echo "OK"
