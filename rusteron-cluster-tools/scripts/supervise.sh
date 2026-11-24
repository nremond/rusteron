#!/bin/bash
# A supervisor for a cluster.sh cluster, as production would run one: it watches every process cluster.sh
# started (node N's own, its C driver, and its -archive, -service, -cm and the extension's units, by their pid
# files) and starts again whatever has exited, DELAY_MS (default 1000) after it noticed:
#
#   scripts/supervise.sh process [DELAY_MS]  that process alone, its node's others left as they are: one unit or
#                                            container per process, as systemd restarts a unit and Kubernetes a
#                                            pod's container (cluster.sh start-process)
#   scripts/supervise.sh node [DELAY_MS]     the whole node: one unit per node, or a pod that fails whole
#                                            (cluster.sh restart)
#
# Start it once the cluster is up, with the cluster's DATA: it supervises the processes it then finds. Stop it
# (SIGTERM) before cluster.sh stop; it waits for the starts under way. Each restart is a line in
# $DATA/supervise.log, what the starts printed in $DATA/supervise-starts.log.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
MODE=${1:-}
if [ "$MODE" != process ] && [ "$MODE" != node ]; then
  echo "usage: scripts/supervise.sh process|node [DELAY_MS]" >&2
  exit 2
fi
DELAY_MS=${2:-1000}
DATA=${DATA:-data/nodes}
LOG=$DATA/supervise.log
# Every pid file cluster.sh wrote: nodeN.pid is node N's own process, nodeN-UNIT.pid its UNIT.
units=()
declare -A pid_of=()
for pid in "$DATA"/node*.pid; do
  [ -s "$pid" ] || continue
  base=${pid##*/node}
  base=${base%.pid}
  n=${base%%-*}
  name=node
  [ "$base" != "$n" ] && name=${base#*-}
  units+=("$n:$name")
  pid_of[$n:$name]=$pid
done
# When each unit was first seen gone, the start under way for each unit (process) or node (node), restarts so far.
declare -A gone_since=() starting=() restarts=()

running() {
  local pid
  pid=$(cat "$1" 2> /dev/null) && [ -n "$pid" ] && kill -0 "$pid" 2> /dev/null
}

log() { echo "$(date +%T.%3N) $*" >> "$LOG"; }

stopping=""
trap 'stopping=1' TERM INT
log "supervisor: $MODE, ${DELAY_MS} ms, ${units[*]}"
while [ -z "$stopping" ]; do
  for unit in "${units[@]}"; do
    n=${unit%%:*}
    name=${unit#*:}
    key=$unit
    [ "$MODE" = node ] && key=$n
    if [ -n "${starting[$key]:-}" ]; then
      kill -0 "${starting[$key]}" 2> /dev/null && continue
      unset "starting[$key]"
    fi
    if running "${pid_of[$unit]}"; then
      unset "gone_since[$unit]"
      continue
    fi
    t=$(date +%s%3N)
    if [ -z "${gone_since[$unit]:-}" ]; then
      gone_since[$unit]=$t
      continue
    fi
    [ $((t - gone_since[$unit])) -ge "$DELAY_MS" ] || continue
    unset "gone_since[$unit]"
    restarts[$key]=$((${restarts[$key]:-0} + 1))
    if [ "$MODE" = node ]; then
      log "node $n: its $name gone, the node started again (restart ${restarts[$key]} of the node)"
      "$HERE/cluster.sh" restart "$n" >> "$DATA/supervise-starts.log" 2>&1 &
    else
      log "node $n: its $name gone, started again (restart ${restarts[$key]} of it)"
      "$HERE/cluster.sh" start-process "$n" "$name" >> "$DATA/supervise-starts.log" 2>&1 &
    fi
    starting[$key]=$!
  done
  sleep 0.2
done
log "supervisor stops, waiting for the starts under way"
wait
log "supervisor stopped"
