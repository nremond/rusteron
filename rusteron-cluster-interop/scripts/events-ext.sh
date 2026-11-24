# shellcheck shell=bash
# A cluster.sh extension (CLUSTER_EXT) for scripts/standby.sh, and an example of one: a second archive on every
# node, the "events" unit, as an application keeps for its own recordings beside the cluster's (an event log,
# say), and the standby's copied into each new member at dr-seed.
#
# Node N's events archive runs on the node's driver, its files in DATA/nodeN/events (the standby's in
# DATA/standby0/events), with archive id 1000 + N. It answers on control stream 110, locally on
# aeron:ipc?term-length=64k and remotely at the slot's port + 6. cluster.sh starts it after the node's archive and
# stops it with the node's other processes; supervise.sh restarts it, and start-process N events starts it alone.
# At dr-seed, standby_seed copies the newest recording of each of its streams from the standby's into the new
# member's (--extra-archive).

EVENTS_PORT_OFFSET=6
EVENTS_CONTROL_STREAM=110

ext_units() { echo events; }

# The events archive of node N, serving once it says READY, or 60 s, or its process gone.
ext_start_unit() { # unit, node, then the start's options
  local i=$2 host before
  host=$(host_of "$i")
  for _ in $(seq 1 600); do [ -f "$(aeron_of "$i")/cnc.dat" ] && break; sleep 0.05; done
  before=$(ready_count "$i" events)
  run_unit "$i" events "$ARCHIVE_BIN" --aeron-dir "$(aeron_of "$i")" --archive-dir "$(dir_of "$i")/events" \
    --archive-id $((1000 + i)) \
    --control-channel "aeron:udp?endpoint=$host:$(port "$i" "$EVENTS_PORT_OFFSET")|term-length=64k" \
    --control-stream-id "$EVENTS_CONTROL_STREAM" --local-control-channel "aeron:ipc?term-length=64k" \
    --local-control-stream-id "$EVENTS_CONTROL_STREAM" --replication-channel "aeron:udp?endpoint=$host:0" \
    --archive-client-response-channel "aeron:udp?endpoint=$host:0"
  for _ in $(seq 1 600); do
    [ "$(ready_count "$i" events)" -gt "$before" ] && break
    kill -0 "$(cat "$DATA/node$i-events.pid" 2> /dev/null)" 2> /dev/null || break
    sleep 0.1
  done
}

# standby_seed's options for new member N: the standby's events archive copied into the member's.
ext_seed_options() { # node
  echo --extra-archive
  echo "endpoint=$(host_of "$SB"):$(port "$SB" "$EVENTS_PORT_OFFSET"),control-stream=$EVENTS_CONTROL_STREAM"
}
