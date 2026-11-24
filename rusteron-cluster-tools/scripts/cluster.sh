#!/bin/bash
# An Aeron Cluster on this machine, a process per part of a node: Aeron's C media driver (node_driver), the Rust
# archive (aeron_archive), the Rust consensus module (consensus_module) and the service. With STANDBY=1, a standby
# too, in the slot after the members': its driver, its archive, the standby (cluster_standby) in a consensus
# module's place, and its own service.
#
#   scripts/cluster.sh start [--sync 0|1] [--spin] [--threading SHARED|SHARED_NETWORK|DEDICATED]
#                            [--verify-archives] [options for the service and the extension]
#   scripts/cluster.sh status
#   scripts/cluster.sh restart N      node N stops (SIGTERM) and starts again on its own data
#   scripts/cluster.sh start-process N node|archive|service|cm|UNIT
#                                     one process of node N starts again, the rest left as they are, as a
#                                     supervisor of one unit per process would (supervise.sh)
#   scripts/cluster.sh dr-seed [standby_seed's options, e.g. --no-snapshot]
#                                     the members lost, new ones in their slots seeded from the standby; then
#                                     restart 0 to MEMBERS-1 starts them
#   scripts/cluster.sh stop
#
# Node N is slot N of the layout the tools share (rusteron_cluster_tools::layout): its ports from PORT_BASE
# (21000) + PORT_STRIDE (100) times N, archive control + 1, client-facing + 2, member-facing + 3, log + 4,
# catch-up + 5; its files in DATA/nodeN, the standby's in DATA/standby0. MEMBERS (3) members, the standby node
# MEMBERS to this script (its pid and log files are DATA/nodeMEMBERS*). DATA (default data/nodes, from the current
# directory) holds the archives and logs; AERON_DIR the drivers' directories (default /dev/shm/rusteron-cluster
# where /dev/shm exists).
#
# Build first, in this repository: cargo build --release --features static -p rusteron-cluster-tools
# -p rusteron-cluster-standby -p rusteron-archive-server -p rusteron-cluster -p rusteron-cluster-interop.
# BIN (default this repository's target/release) holds them; DRIVER_BIN, ARCHIVE_BIN, CM_BIN, SERVICE_BIN,
# STANDBY_BIN, SEED_BIN and TOOL_BIN name each instead.
#
# The service is the interop crate's test service by default (rust_service_node AERON_DIR CLUSTER_DIR 0 TRACE
# no-service-messages), its trace in the node's directory. CLUSTER_EXT names a bash file this script sources, for a
# cluster of another service; it may define:
#
#   ext_service_command N HOST AERON_DIR CLUSTER_DIR [start options]   set SERVICE_CMD, the service's command line
#   ext_units                                                         print the node's other units, e.g. "events"
#   ext_start_unit UNIT N [start options]                             start one, with run_unit N UNIT COMMAND...
#   ext_ready                                                         wait for the cluster to serve, 0 once it does
#   ext_status N                                                      print node N's status, nothing until it serves
#   ext_seed_options N                                                print standby_seed's extra options, one a line
#   ext_clean                                                         what start deletes besides DATA and AERON_DIR
#   ext_lose_node N LOST                                              dr-seed: node N's directory went to LOST
#   EXT_SETTINGS=(NAME ...)                                           its own settings, kept as this script's
#
# It may use these: port N OFFSET, host_of N, dir_of N, aeron_of N, is_standby N, run_unit, up_count N,
# ready_count N UNIT (the READY lines in its log), SB (the standby's node), the *_BIN variables and LAYOUT. An
# example: rusteron-cluster-interop/scripts/events-ext.sh, a second archive on every node.
#
# STANDBY=1 at start adds the standby. It idles whatever the members do: a SHARED driver, nothing busy-spinning,
# its archive unsynced. STANDBY_OPTS adds to cluster_standby's options (--keep-snapshots K), CM_OPTS to every
# consensus module's. NODE_CPUS ("0-3 4-7 8-11") pins node 0, 1, ... and STANDBY_CPUS the standby, where taskset
# exists. ZONES=1 at start puts node N in zone N of zones.sh, at ZONE_NET.N+1 (ZONE_NET 10.77.0); the standby's
# zone is MEMBERS (sudo env ZONE_COUNT=4 scripts/zones.sh up for three members and a standby), and the cluster's
# clients run in a zone (scripts/zones.sh exec 0 ...). AERON_* environment variables reach every process:
# Aeron's own settings.
#
# Start empties DATA and AERON_DIR. Restart and the rest keep to what start ran with: these variables (in
# DATA/env), its options (DATA/options), the zones and the standby, unless their caller sets one.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
DATA=${DATA:-data/nodes}
SETTINGS=(MEMBERS PORT_BASE PORT_STRIDE NODE_CPUS STANDBY_CPUS AERON_DIR CM_OPTS STANDBY_OPTS BIN DRIVER_BIN
  ARCHIVE_BIN CM_BIN SERVICE_BIN STANDBY_BIN SEED_BIN TOOL_BIN CLUSTER_EXT ZONE_NET ZONE_PREFIX)
EXT_SETTINGS=()
if [ "${1:-}" != start ] && [ -f "$DATA/env" ]; then
  while IFS= read -r setting; do
    name=${setting%%=*}
    [ -n "${!name+x}" ] || eval "export $setting"
  done < "$DATA/env"
fi
[ "${1:-}" != start ] && [ -f "$DATA/zones" ] && ZONES=1
if [ "${1:-}" != start ] && [ -f "$DATA/standby" ]; then STANDBY=$(cat "$DATA/standby"); STANDBY=${STANDBY:-1}; fi
if [ -n "${CLUSTER_EXT:-}" ]; then
  CLUSTER_EXT=$(cd "$(dirname "$CLUSTER_EXT")" && pwd)/$(basename "$CLUSTER_EXT")
  # shellcheck source=/dev/null
  . "$CLUSTER_EXT"
fi
MEMBERS=${MEMBERS:-3}
PORT_BASE=${PORT_BASE:-21000}
PORT_STRIDE=${PORT_STRIDE:-100}
ZONE_NET=${ZONE_NET:-10.77.0}
if [ -z "${AERON_DIR:-}" ]; then
  if [ -d /dev/shm ]; then AERON_DIR=/dev/shm/rusteron-cluster; else AERON_DIR=$DATA/aeron; fi
fi
read -r -a CPUS <<< "${NODE_CPUS:-}"
BIN=${BIN:-$HERE/../../target/release}
DRIVER_BIN=${DRIVER_BIN:-$BIN/node_driver}
ARCHIVE_BIN=${ARCHIVE_BIN:-$BIN/aeron_archive}
CM_BIN=${CM_BIN:-$BIN/consensus_module}
SERVICE_BIN=${SERVICE_BIN:-$BIN/rust_service_node}
STANDBY_BIN=${STANDBY_BIN:-$BIN/cluster_standby}
SEED_BIN=${SEED_BIN:-$BIN/standby_seed}
TOOL_BIN=${TOOL_BIN:-$BIN/cluster_tool}
# The standby's node: the slot after the members'.
SB=$MEMBERS

has() { declare -F "$1" > /dev/null; }
port() { echo $((PORT_BASE + PORT_STRIDE * $1 + $2)); }
host_of() { if [ -n "${ZONES:-}" ]; then echo "$ZONE_NET.$(($1 + 1))"; else echo localhost; fi; }
members() { seq 0 $((MEMBERS - 1)); }
all() { members; if [ -n "${STANDBY:-}" ]; then echo "$SB"; fi; }
is_standby() { [ "$1" -ge "$MEMBERS" ]; }
dir_of() { if is_standby "$1"; then echo "$DATA/standby0"; else echo "$DATA/node$1"; fi; }
aeron_of() { if is_standby "$1"; then echo "$AERON_DIR/standby0"; else echo "$AERON_DIR/node$1"; fi; }
cpus_of() { if is_standby "$1"; then echo "${STANDBY_CPUS:-}"; else echo "${CPUS[$1]:-}"; fi; }
HOSTS=$(for i in $(members); do host_of "$i"; done | paste -sd, -)
# The layout as the tools take it.
LAYOUT=(--hosts "$HOSTS" --port-base "$PORT_BASE" --port-stride "$PORT_STRIDE" --dir "$DATA" --aeron-dir "$AERON_DIR")
# The command that runs a process in node N's zone, if any: a word a line, for mapfile. The process is forked
# away from zones.sh's sudos (setsid -f), which exit at once: sudo stops itself when its command is stopped and
# stays stopped when a test continues the command alone, and a command killed under it stays a zombie, which
# kill -0 finds alive (stop_pid then waits out its 10 s).
zone_of() { if [ -n "${ZONES:-}" ]; then printf '%s\n' "$HERE/zones.sh" exec "$1" setsid -f; fi; }

# Starts a process of node N: UNIT is "" for the node's own process (its driver), else its name. It is pinned to
# the node's cores, in its zone, detached; it writes its own pid to DATA/nodeN[-UNIT].pid (in a zone $! would be
# sudo's, which passes on no SIGKILL or SIGSTOP) and appends to DATA/nodeN[-UNIT].log.
run_unit() { # node, unit, then the command
  local i=$1 unit=$2 pin="" detach="" suffix="" cpus zone=()
  shift 2
  [ -n "$unit" ] && suffix="-$unit"
  cpus=$(cpus_of "$i")
  command -v taskset > /dev/null && [ -n "$cpus" ] && pin="taskset -c $cpus"
  command -v setsid > /dev/null && detach=setsid
  [ -n "${ZONES:-}" ] && mapfile -t zone < <(zone_of "$i")
  rm -f "$DATA/node$i$suffix.pid"
  # shellcheck disable=SC2086,SC2016
  $detach nohup ${zone[@]+"${zone[@]}"} sh -c 'echo $$ > "$0"; exec "$@"' "$DATA/node$i$suffix.pid" \
    $pin "$@" >> "$DATA/node$i$suffix.log" 2>&1 < /dev/null &
  for _ in $(seq 1 100); do [ -s "$DATA/node$i$suffix.pid" ] && break; sleep 0.05; done
}

# How many times node N's driver has said it is up.
up_count() {
  grep -c " up on " "$DATA/node$1.log" 2> /dev/null || true
}

# Node N's own process, its C driver: a member's, or the standby's. It takes the start's whole command line and
# uses what it needs; the standby's is SHARED and idles.
launch() { # node, then the start's options
  local i=$1 options=()
  shift
  if is_standby "$i"; then
    while [ $# -gt 0 ]; do
      case $1 in
        --spin) ;;
        --threading) shift ;;
        *) options+=("$1") ;;
      esac
      shift
    done
    run_unit "$i" "" "$DRIVER_BIN" --standby 0 --host "$(host_of "$i")" "${LAYOUT[@]}" ${options[@]+"${options[@]}"}
  else
    run_unit "$i" "" "$DRIVER_BIN" --node "$i" "${LAYOUT[@]}" "$@"
  fi
}

# Waits until node N's driver is up past its count of up lines before: then it is the new one, never the files
# of one killed moments ago, which a process would take for a live driver and stop on once it found it dead.
await_driver() { # node, up lines before
  for _ in $(seq 1 600); do [ "$(up_count "$1")" -gt "$2" ] && break; sleep 0.1; done
}

# How many times node N's archive (or another unit's process) has said it serves.
ready_count() {
  if [ -f "$DATA/node$1-$2.log" ]; then grep -c "^READY" "$DATA/node$1-$2.log" || true; else echo 0; fi
}

# The node's cluster archive, a process beside the node's, started once its driver's files are there: its
# control channel and stream, its local one, replication, sync level, writes of 8 MiB. The standby's never spins
# nor syncs. Serving before the node's service starts, or 60 s, or its process gone.
launch_archive() { # node, then the start's options
  local i=$1 sync=0 spin="" verify="" host before
  shift
  while [ $# -gt 0 ]; do
    case $1 in
      --sync) is_standby "$i" || sync=$2; shift ;;
      --spin) is_standby "$i" || spin=--spin ;;
      --verify-archives) verify="--truncate-torn-fragments --verify" ;;
    esac
    shift
  done
  host=$(host_of "$i")
  for _ in $(seq 1 600); do [ -f "$(aeron_of "$i")/cnc.dat" ] && break; sleep 0.05; done
  before=$(ready_count "$i" archive)
  # shellcheck disable=SC2086
  run_unit "$i" archive "$ARCHIVE_BIN" --aeron-dir "$(aeron_of "$i")" --archive-dir "$(dir_of "$i")/archive" \
    --control-channel "aeron:udp?endpoint=$host:$(port "$i" 1)|term-length=64k" \
    --local-control-channel "aeron:ipc?term-length=64k" --replication-channel "aeron:udp?endpoint=$host:0" \
    --archive-client-response-channel "aeron:udp?endpoint=$host:0" --file-sync-level "$sync" \
    --catalog-file-sync-level "$sync" --file-io-max-length 8m $spin $verify
  for _ in $(seq 1 600); do
    [ "$(ready_count "$i" archive)" -gt "$before" ] && break
    kill -0 "$(cat "$DATA/node$i-archive.pid" 2> /dev/null)" 2> /dev/null || break
    sleep 0.1
  done
}

# The extension's units of node N, started after its archive.
launch_units() { # node, then the start's options
  local i=$1 unit
  shift
  has ext_units || return 0
  for unit in $(ext_units); do ext_start_unit "$unit" "$i" "$@"; done
}

# Node N's service: on its driver, its cores, its cluster directory (the standby's in standby0/service), once its
# driver is up past its count of up lines before.
launch_service() { # node, up lines before its launch, then the start's options
  local i=$1 before=$2 cluster_dir
  shift 2
  if is_standby "$i"; then cluster_dir="$(dir_of "$i")/service"; else cluster_dir="$(dir_of "$i")/cluster"; fi
  await_driver "$i" "$before"
  SERVICE_CMD=("$SERVICE_BIN" "$(aeron_of "$i")" "$cluster_dir" 0 "$(dir_of "$i")/service.trace" no-service-messages)
  if has ext_service_command; then ext_service_command "$i" "$(host_of "$i")" "$(aeron_of "$i")" "$cluster_dir" "$@"; fi
  run_unit "$i" service "${SERVICE_CMD[@]}"
}

# The cluster's members as Aeron names them: id,client-facing,member-facing,log,catch-up,archive|...
cluster_members() {
  local i host out=""
  for i in $(members); do
    host=$(host_of "$i")
    out+="$i,$host:$(port "$i" 2),$host:$(port "$i" 3),$host:$(port "$i" 4),$host:$(port "$i" 5),$host:$(port "$i" 1)|"
  done
  echo "$out"
}

# The node's consensus module, once its driver is up past its count of up lines before: nanosecond time, a
# startup canvass of 5 s, heartbeats every 100 ms timing out after 1 s, elections of 500 ms, 64 sessions, ingress
# terms of 16 MiB, Aeron's default file sync level, standbys' snapshots accepted. The standby's is
# cluster_standby, in its place.
launch_consensus() { # node, up lines before its launch, then the start's options
  local i=$1 before=$2 spin="" host
  shift 2
  await_driver "$i" "$before"
  host=$(host_of "$i")
  if is_standby "$i"; then
    # shellcheck disable=SC2086
    run_unit "$i" cm "$STANDBY_BIN" --standby 0 --host "$host" "${LAYOUT[@]}" ${STANDBY_OPTS:-}
    return 0
  fi
  while [ $# -gt 0 ]; do
    case $1 in
      --spin) spin=--spin ;;
    esac
    shift
  done
  # shellcheck disable=SC2086
  run_unit "$i" cm "$CM_BIN" --aeron-dir "$(aeron_of "$i")" --cluster-dir "$(dir_of "$i")/cluster" \
    --cluster-member-id "$i" --cluster-members "$(cluster_members)" --service-count 1 \
    --ingress-channel "aeron:udp?term-length=16m" --replication-channel "aeron:udp?endpoint=$host:0" \
    --archive-control-channel "aeron:ipc?term-length=64k" --time-unit ns --startup-canvass-timeout 5s \
    --leader-heartbeat-timeout 1000ms --leader-heartbeat-interval 100ms --election-timeout 500ms \
    --max-concurrent-sessions 64 --accept-standby-snapshots $spin ${CM_OPTS:-}
}

# Every process of node N, in order: driver, archive, the extension's units, service, consensus module.
launch_node() { # node, up lines before, then the start's options
  local i=$1 before=$2
  shift 2
  launch "$i" "$@"
  launch_archive "$i" "$@"
  launch_units "$i" "$@"
  launch_service "$i" "$before" "$@"
  launch_consensus "$i" "$before" "$@"
}

status() {
  local i standbys=0
  if has ext_status; then
    for i in $(all); do ext_status "$i"; done
    return 0
  fi
  [ -n "${STANDBY:-}" ] && standbys=1
  "$TOOL_BIN" status "${LAYOUT[@]}" --standbys "$standbys"
}

# Node N's status line once it serves: nothing until then.
node_status() {
  local line
  if has ext_status; then ext_status "$1"; return 0; fi
  if is_standby "$1"; then
    line=$("$TOOL_BIN" status "${LAYOUT[@]}" --standbys 1 | grep "^standby 0 " || true)
  else
    line=$("$TOOL_BIN" status "${LAYOUT[@]}" | grep "^node $1 " || true)
  fi
  case $line in *" down" | "") ;; *) echo "$line" ;; esac
}

stop_pid() { # pid file
  local pid
  pid=$(cat "$1" 2> /dev/null || echo "")
  [ -z "$pid" ] && return 0
  kill "$pid" 2> /dev/null || true
  for _ in $(seq 1 100); do kill -0 "$pid" 2> /dev/null || break; sleep 0.1; done
  kill -9 "$pid" 2> /dev/null || true
  rm -f "$1"
}

# The consensus module first: on its driver gone it would record an error. Then the service, the archive and the
# extension's units. The node's own process last: its driver deletes its files, which the others still use until
# they stop.
stop_node() {
  local pid
  stop_pid "$DATA/node$1-cm.pid"
  stop_pid "$DATA/node$1-service.pid"
  for pid in "$DATA/node$1"-*.pid; do [ -e "$pid" ] && stop_pid "$pid"; done
  stop_pid "$DATA/node$1.pid"
}

# A new member in node N's slot, seeded from the standby (dr-seed): its driver, archive and the extension's
# units started on its empty directory for the copy, stopped after it.
seed_member() { # node, then standby_seed's own options
  local i=$1 status=0 extra=()
  shift
  rm -f "$(aeron_of "$i")/cnc.dat"
  launch "$i"
  launch_archive "$i"
  launch_units "$i"
  if has ext_seed_options; then mapfile -t extra < <(ext_seed_options "$i"); fi
  "$SEED_BIN" --node "$i" "${LAYOUT[@]}" --standby-host "$(host_of "$SB")" ${extra[@]+"${extra[@]}"} "$@" \
    >> "$DATA/node$i-seed.log" 2>&1 || status=$?
  stop_node "$i"
  rm -f "$(aeron_of "$i")/cnc.dat"
  return "$status"
}

usage="usage: scripts/cluster.sh start [--sync 0|1] [--spin] [--threading SHARED|SHARED_NETWORK|DEDICATED] \
[--verify-archives] [...] | status | restart N | start-process N node|archive|service|cm|UNIT | dr-seed [...] | stop"

case "${1:-}" in
  start)
    shift
    "$0" stop > /dev/null 2>&1 || true
    if has ext_clean; then ext_clean; fi
    rm -rf "$DATA" "$AERON_DIR"
    # Before the drivers start at once: Aeron's C driver creates a missing parent and fails when another driver
    # created it first ("Failed to mkdir aeron directory"), and the cluster runs a member short.
    mkdir -p "$DATA" "$AERON_DIR"
    [ -n "${ZONES:-}" ] && touch "$DATA/zones"
    [ -n "${STANDBY:-}" ] && echo "$STANDBY" > "$DATA/standby"
    : > "$DATA/options"
    [ $# -gt 0 ] && printf '%s\n' "$@" > "$DATA/options"
    for name in "${SETTINGS[@]}" ${EXT_SETTINGS[@]+"${EXT_SETTINGS[@]}"} $(compgen -e | grep '^AERON_' || true); do
      if [ -n "${!name+x}" ]; then printf '%s=%q\n' "$name" "${!name}"; fi
    done > "$DATA/env"
    bins=("$DRIVER_BIN" "$ARCHIVE_BIN" "$CM_BIN" "$TOOL_BIN")
    has ext_service_command || bins+=("$SERVICE_BIN")
    [ -n "${STANDBY:-}" ] && bins+=("$STANDBY_BIN")
    for bin in "${bins[@]}"; do
      if [ ! -x "$bin" ]; then
        echo "cluster.sh: no $bin: see this script's header for how to build it" >&2
        exit 2
      fi
    done
    for i in $(all); do launch "$i" "$@"; done
    for i in $(all); do launch_archive "$i" "$@"; done
    for i in $(all); do launch_units "$i" "$@"; done
    for i in $(all); do launch_service "$i" 0 "$@"; done
    for i in $(all); do launch_consensus "$i" 0 "$@"; done
    if has ext_ready; then
      ext_ready || { echo "cluster.sh: the cluster is not serving; see $DATA/node*.log" >&2; exit 1; }
    elif ! "$TOOL_BIN" status "${LAYOUT[@]}" --await-leader 60s > /dev/null; then
      echo "cluster.sh: no leader after 60 s; see $DATA/node*.log" >&2
      exit 1
    fi
    status
    ;;
  status)
    status
    ;;
  restart)
    node=${2:?usage: scripts/cluster.sh restart N}
    options=()
    while IFS= read -r option; do options+=("$option"); done < "$DATA/options"
    before=$(up_count "$node")
    stop_node "$node"
    # A driver killed leaves its files: the archive waits for the new driver's.
    rm -f "$(aeron_of "$node")/cnc.dat"
    launch_node "$node" "$before" ${options[@]+"${options[@]}"}
    for _ in $(seq 1 600); do
      line=$(node_status "$node")
      [ -n "$line" ] && { echo "$line"; exit 0; }
      if ! kill -0 "$(cat "$DATA/node$node.pid")" 2> /dev/null; then
        echo "cluster.sh: node $node exited; see $DATA/node$node.log" >&2
        exit 1
      fi
      sleep 0.1
    done
    echo "cluster.sh: node $node did not come back in 60 s; see $DATA/node$node*.log" >&2
    exit 1
    ;;
  start-process)
    # One process of the node, with no wait for the others: a supervisor of one unit per process starts each
    # on its own, whatever state its node's other processes are in. Those that need the driver find it or
    # exit, to be started again in turn.
    node=${2:?usage: scripts/cluster.sh start-process N node|archive|service|cm|UNIT}
    name=${3:?usage: scripts/cluster.sh start-process N node|archive|service|cm|UNIT}
    options=()
    while IFS= read -r option; do options+=("$option"); done < "$DATA/options"
    # The node's own process is up already, or comes up on its own: the service and module need no new up line.
    before=$(($(up_count "$node") - 1))
    case $name in
      node)
        # A driver killed leaves its files: its node's other processes then wait for the new driver's.
        rm -f "$(aeron_of "$node")/cnc.dat"
        launch "$node" ${options[@]+"${options[@]}"}
        ;;
      archive) launch_archive "$node" ${options[@]+"${options[@]}"} ;;
      service) launch_service "$node" "$before" ${options[@]+"${options[@]}"} ;;
      cm) launch_consensus "$node" "$before" ${options[@]+"${options[@]}"} ;;
      *)
        if has ext_units && [[ " $(ext_units | tr '\n' ' ') " == *" $name "* ]]; then
          ext_start_unit "$name" "$node" ${options[@]+"${options[@]}"}
        else
          echo "cluster.sh: start-process N node|archive|service|cm or a unit of the extension, not $name" >&2
          exit 2
        fi
        ;;
    esac
    ;;
  dr-seed)
    # The disaster-recovery runbook: every member lost, new ones in their slots from what the standby holds, the
    # standby left running. Each member's processes are killed first, the fence: an old member alive beside the
    # new cluster would diverge from it. Its directory goes to DATA/lost, and the new one gets copies of the
    # standby's log and newest set (standby_seed, every member at once). Then restart 0 to MEMBERS-1 start the
    # new cluster, which elects a leader from the copies.
    shift
    if [ -z "${STANDBY:-}" ]; then
      echo "cluster.sh dr-seed: no standby to seed from" >&2
      exit 2
    fi
    for bin in "$SEED_BIN" "$DRIVER_BIN" "$ARCHIVE_BIN"; do
      if [ ! -x "$bin" ]; then
        echo "cluster.sh dr-seed: no $bin" >&2
        exit 2
      fi
    done
    for i in $(members); do
      for pid in "$DATA/node$i".pid "$DATA/node$i"-*.pid; do
        [ -f "$pid" ] && kill -9 "$(cat "$pid")" 2> /dev/null || true
        rm -f "$pid"
      done
    done
    mkdir -p "$DATA/lost"
    for i in $(members); do
      lost=$DATA/lost/node$i.$(date +%s%N)
      [ -d "$DATA/node$i" ] && mv "$DATA/node$i" "$lost"
      if has ext_lose_node; then ext_lose_node "$i" "$lost"; fi
      mkdir -p "$DATA/node$i"
    done
    seeding=()
    for i in $(members); do
      seed_member "$i" "$@" &
      seeding+=($!)
    done
    failed=""
    for i in $(members); do
      wait "${seeding[$i]}" || failed+=" $i"
    done
    for i in $(members); do grep -h "^node $i: seeded\|^standby_seed" "$DATA/node$i-seed.log" | tail -1; done
    if [ -n "$failed" ]; then
      echo "cluster.sh dr-seed: node(s)$failed not seeded; see $DATA/node*-seed.log" >&2
      exit 1
    fi
    ;;
  stop)
    # Every node's consensus module at once, and its service, then each node's other processes.
    for i in $(all); do
      for pid in "$DATA/node$i-cm.pid" "$DATA/node$i-service.pid"; do
        [ -f "$pid" ] && kill "$(cat "$pid")" 2> /dev/null || true
      done
    done
    for i in $(all); do stop_node "$i"; done
    ;;
  *)
    echo "$usage" >&2
    exit 2
    ;;
esac
