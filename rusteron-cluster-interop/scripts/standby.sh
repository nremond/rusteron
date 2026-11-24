#!/usr/bin/env bash
# The Rust standby (rusteron-cluster-standby) on a cluster with no JVM, started by
# rusteron-cluster-tools/scripts/cluster.sh: three members, each Aeron's C driver (node_driver), the Rust archive,
# the Rust consensus module and the test service (rust_service_node, with no service messages), and a standby,
# cluster_standby with its own test service. Checks the standby against the members, a member restarted from the
# standby's snapshot, then the disaster-recovery runbook:
#
#   scripts/standby.sh [COUNT]
#
# 1. COUNT echoes (echo_client), the members' snapshot (cluster_tool snapshot), COUNT more, a standby snapshot
#    (cluster_tool standby-snapshot, done once the leader hears of it), COUNT more.
# 2. The standby caught up, it must agree: its module state at the members' snapshot is theirs (standby_check
#    states); its service's trace is the leader's, line for line; every log is the same (log_check, the members'
#    and the standby's).
# 3. A follower restarted (cluster.sh restart): its recording log lists the standby's set, and its service starts
#    from it (its trace's last start line at the set's position); COUNT more echoes, and every log the same.
# 4. The disaster: every member killed, their files moved aside, the standby left running; cluster.sh dr-seed
#    gives new members copies of the standby's log and set, and they start. They elect a leader, the services
#    start from the set, COUNT more echoes go through, and every log, the standby's too, is the same from the
#    start.
#
# PLANTED=1 seeds with standby_seed --stop-at-set, the log copied only to the set: the members then lose what the
# standby held after it, and the last check must fail (the script passes only if it does). Binaries are the
# release builds (BIN, default target/release): node_driver, aeron_archive, consensus_module, rust_service_node,
# echo_client, cluster_standby, standby_seed, standby_check, cluster_tool, log_check. PORT_BASE (21000) moves the
# cluster's ports, AERON_ROOT (/dev/shm or TMPDIR) its drivers' directories. KEEP=1 keeps the work directory.
set -euo pipefail

COUNT=${1:-1000}
HERE=$(cd "$(dirname "$0")/.." && pwd)
REPO=$(cd "$HERE/.." && pwd)
export BIN=${BIN:-$REPO/target/release}
export PORT_BASE=${PORT_BASE:-21000}
CLUSTER=$REPO/rusteron-cluster-tools/scripts/cluster.sh
WORK=$(mktemp -d "${TMPDIR:-/tmp}/rusteron-standby.XXXXXX")
if [ -d /dev/shm ]; then AERON_ROOT=${AERON_ROOT:-/dev/shm}; else AERON_ROOT=${AERON_ROOT:-$WORK}; fi
export DATA=$WORK/nodes
export AERON_DIR=$AERON_ROOT/rusteron-standby-$$
export STANDBY=1
export STANDBY_OPTS="--record-module-states"
CLIENT_DIR=$WORK/client
LAYOUT=(--hosts localhost,localhost,localhost --port-base "$PORT_BASE" --dir "$DATA" --aeron-dir "$AERON_DIR")
CLIENT_PID=""

cleanup() {
  "$CLUSTER" stop > /dev/null 2>&1 || true
  if [ -n "$CLIENT_PID" ]; then kill "$CLIENT_PID" 2> /dev/null || true; wait "$CLIENT_PID" 2> /dev/null || true; fi
  rm -rf "$AERON_DIR"
  if [ "${KEEP:-}" = 1 ]; then echo "kept $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

fail() {
  echo "FAILED: $*"
  exit 1
}

step() { echo "== $* ($(date +%T))"; }

for bin in node_driver aeron_archive consensus_module rust_service_node echo_client cluster_standby standby_seed \
  standby_check cluster_tool log_check; do
  [ -x "$BIN/$bin" ] || fail "no $BIN/$bin: build it first (see $CLUSTER's header)"
done

# The clients' own driver.
mkdir -p "$CLIENT_DIR"
"$BIN/node_driver" --node 0 --members 1 --dir "$CLIENT_DIR" > "$WORK/client-driver.log" 2>&1 &
CLIENT_PID=$!
CLIENT_AERON=$CLIENT_DIR/node0/aeron
for _ in $(seq 1 100); do grep -q " up on " "$WORK/client-driver.log" && break; sleep 0.1; done
grep -q " up on " "$WORK/client-driver.log" || fail "the clients' driver did not start: $(cat "$WORK/client-driver.log")"

echoes() {
  "$BIN/echo_client" "$CLIENT_AERON" "$PORT_BASE" 3 "$COUNT" | tee -a "$WORK/echoes.log" | grep -q '^OK' ||
    fail "echoes: $(tail -1 "$WORK/echoes.log")"
}

leader() {
  "$BIN/cluster_tool" status "${LAYOUT[@]}" | sed -n 's/^node \([0-9]*\) role=LEADER .*/\1/p' | head -1
}

commit_of() { # a status line's prefix: "node N" or "standby 0"
  "$BIN/cluster_tool" status "${LAYOUT[@]}" --standbys 1 | sed -n "s/^$1 .*commit=\([0-9]*\).*/\1/p"
}

# Waits until the standby's commit position is the leader's, 60 s at most.
await_standby() {
  local l s
  for _ in $(seq 1 600); do
    l=$(commit_of "node $(leader)")
    s=$(commit_of "standby 0")
    [ -n "$l" ] && [ "$l" = "$s" ] && return 0
    sleep 0.1
  done
  fail "the standby stayed behind: at ${s:-?}, the leader at ${l:-?}"
}

# The standby's service trace against the leader's: the standby's whole, the leader's start.
traces_agree() {
  local standby=$DATA/standby0/service.trace leader_trace size
  leader_trace=$DATA/node$(leader)/service.trace
  size=$(stat -c %s "$standby" 2> /dev/null || stat -f %z "$standby")
  [ "$size" -gt 0 ] || fail "the standby's service traced nothing"
  cmp -n "$size" "$standby" "$leader_trace" > /dev/null ||
    fail "the standby's service trace differs from the leader's: $(cmp -n "$size" "$standby" "$leader_trace" || true)"
  echo "traces     the standby's service trace is the leader's, $(wc -l < "$standby") lines"
}

logs_agree() {
  "$BIN/log_check" --aeron-dir "$CLIENT_AERON" --hosts localhost,localhost,localhost,localhost \
    --port-base "$PORT_BASE"
}

# The log position where node N's service last started, once it has started more than BEFORE times (its trace is
# appended to across starts), 60 s at most.
started_at() { # node, starts before
  local trace=$DATA/node$1/service.trace
  for _ in $(seq 1 600); do
    [ "$(grep -c '^start ' "$trace" 2> /dev/null || echo 0)" -gt "$2" ] && break
    sleep 0.1
  done
  grep '^start ' "$trace" 2> /dev/null | tail -1 | cut -d' ' -f2
}

# The log position of the standby's newest set.
newest_set() {
  tail -1 "$DATA/standby0/standby-snapshots" | cut -d' ' -f1
}

step "1. start, echoes, the members' snapshot, a standby snapshot"
"$CLUSTER" start > "$WORK/start.log" 2>&1 || fail "start: $(tail -5 "$WORK/start.log")"
cat "$WORK/start.log"
echoes
"$BIN/cluster_tool" snapshot "${LAYOUT[@]}" || fail "the members' snapshot"
echoes
"$BIN/cluster_tool" standby-snapshot "${LAYOUT[@]}" || fail "the standby snapshot"
echoes
set_position=$(newest_set)
[ -n "$set_position" ] || fail "the standby lists no set"
echo "set        the standby's newest set at $set_position"

step "2. the standby against the members"
await_standby
"$BIN/standby_check" states --aeron-dir "$CLIENT_AERON" "${LAYOUT[@]}" || fail "the module states"
traces_agree
logs_agree || fail "the logs"

step "3. a follower restarted from the standby's set"
leader_id=$(leader)
follower=$(( (leader_id + 1) % 3 ))
starts=$(grep -c '^start ' "$DATA/node$follower/service.trace")
"$CLUSTER" restart "$follower" || fail "restart $follower"
"$BIN/standby_check" recording-log "$DATA/node$follower/cluster" | grep "type=STANDBY_SNAPSHOT" |
  grep -q "logPosition=$set_position " || fail "node $follower's recording log lists no standby set at $set_position"
started=$(started_at "$follower" "$starts")
[ "$started" = "$set_position" ] || fail "node $follower's service started at ${started:-nothing}, not at the set's $set_position"
echo "restart    node $follower's service started from the standby's set at $set_position"
echoes
await_standby
logs_agree || fail "the logs after the restart"

step "4. the disaster: every member lost, a new cluster from the standby"
set_position=$(newest_set)
seed_options=()
[ "${PLANTED:-}" = 1 ] && seed_options=(--stop-at-set)
"$CLUSTER" dr-seed ${seed_options[@]+"${seed_options[@]}"} || fail "dr-seed"
for i in 0 1 2; do "$CLUSTER" restart "$i" > /dev/null || fail "start of new member $i"; done
"$BIN/cluster_tool" status "${LAYOUT[@]}" --standbys 1 --await-leader 60s || fail "no leader in the new cluster"
for i in 0 1 2; do
  started=$(started_at "$i" 0)
  [ "$started" = "$set_position" ] || fail "new member $i's service started at ${started:-nothing}, not at $set_position"
done
echo "seeded     every new member's service started from the standby's set at $set_position"
echoes
if [ "${PLANTED:-}" = 1 ]; then
  # The standby holds log the new cluster never had: it cannot catch up with it, so it is not waited for.
  sleep 2
  if logs_agree; then fail "the planted bug went unseen: the logs agree"; fi
  echo "OK: the planted bug (the log copied only to the set) was caught"
  exit 0
fi
await_standby
logs_agree || fail "the logs after the disaster"
echo "OK"
