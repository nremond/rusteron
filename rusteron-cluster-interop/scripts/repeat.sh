#!/usr/bin/env bash
# Runs every scenario of differential.sh N times in a row, and stops at the
# first failure, keeping its work directory.
#
#   scripts/repeat.sh [N]
set -uo pipefail

N=${1:-20}
HERE=$(cd "$(dirname "$0")" && pwd)
SCENARIOS=("basic 1" "restart 1" "timers 1" "marks 1" "dead-service 1" "failover 3")

start=$SECONDS
for ((i = 1; i <= N; i++)); do
  for entry in "${SCENARIOS[@]}"; do
    read -r scenario nodes <<<"$entry"
    if ! out=$(KEEP=1 SCENARIO=$scenario "$HERE/differential.sh" "$nodes" 1000 2>&1); then
      echo "run $i, $scenario: FAILED"
      echo "$out" | tail -20
      exit 1
    fi
    work=$(sed -n 's/^work directory: //p' <<<"$out")
    [[ -n "$work" ]] && rm -rf "$work"
    echo "run $i, $scenario: $(grep -c "lines alike" <<<"$out") nodes alike, $(grep -c "frames alike" <<<"$out") snapshots alike"
  done
done
echo "all ${#SCENARIOS[@]} scenarios passed $N times in a row ($((SECONDS - start)) s)"
