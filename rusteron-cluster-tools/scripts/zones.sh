#!/bin/bash
# Availability zones on one machine, to measure a cluster as if its nodes
# were in several: a network namespace a zone (ZONE_PREFIX0, ZONE_PREFIX1,
# ...; ZONE_PREFIX rz by default), node N at ZONE_NET.N+1 in zone N
# (ZONE_NET 10.77.0 by default), each pair of zones joined by a link of its
# own, and a round trip of RTT_MS between any two zones (tc netem, half each
# way). No bridge, so no firewall rule sees the traffic. A client in zone 0
# shares node 0's namespace and address. ZONE_COUNT (3 by default) at `up`
# sets how many: 4 for three members and a standby, as cluster.sh places
# it (`sudo env ZONE_COUNT=4 scripts/zones.sh up`: sudo drops a variable put
# before it). The other commands count the zones that are there.
#
#   sudo scripts/zones.sh up [RTT_MS] [JITTER_MS] [LOSS_PCT]   create them; RTT_MS 1 by default
#   sudo scripts/zones.sh rtt RTT_MS [JITTER_MS] [LOSS_PCT]    every link's round trip, and loss
#   sudo scripts/zones.sh link A B RTT_MS [LOSS_PCT]           the link between zones A and B, both ways
#   sudo scripts/zones.sh mtu BYTES               every link's MTU (1500 by default; 9000 for jumbo frames)
#   sudo scripts/zones.sh tcp tuned|default       TCP's retransmit timeout floor and delayed acks, per route
#   scripts/zones.sh status                       each link's delay, loss and MTU, and a ping between each pair
#   scripts/zones.sh counts                       each link's netem: packets sent, packets dropped, bytes sent
#   sudo scripts/zones.sh down                    remove them
#   scripts/zones.sh exec N COMMAND...            COMMAND in zone N, as this user, with this environment
#
# Loss drops each packet on its own, as on a wire: the links send no
# segmentation offloads (TSO, GSO off, gso_max_segs 1), or netem would drop
# TCP a GSO packet of several segments at a time, and UDP a datagram, so
# the two would not lose alike. Jitter delays
# each packet on its own, so it reorders them: leave it at 0 unless
# reordering is what is measured. LOSS_PCT drops that share of the packets
# on each link, each way, at random (0 by default). `tcp tuned` sets
# rto_min 5ms and quickack on every route between zones: TCP's 200 ms
# floor and its delayed acks are for the internet, not a 1 ms round trip.
set -euo pipefail
ZONE_PREFIX=${ZONE_PREFIX:-rz}
ZONE_NET=${ZONE_NET:-10.77.0}
if [ "${1:-}" = up ]; then
  ZONE_COUNT=${ZONE_COUNT:-3}
else
  ZONE_COUNT=${ZONE_COUNT:-$(ip netns list | grep -cE "^$ZONE_PREFIX[0-9]+( |\$)" || true)}
fi

ns() { echo "$ZONE_PREFIX$1"; }
ip_of() { echo "$ZONE_NET.$(($1 + 1))"; }
# In zone $1, the link to zone $2.
link() { echo "zl$1to$2"; }

others() { # the zones other than $1
  local z
  for z in $(seq 0 $((ZONE_COUNT - 1))); do [ "$z" != "$1" ] && echo "$z"; done
  return 0
}

netem() { # A B RTT_MS JITTER_MS LOSS_PCT: the link from zone A to zone B
  local half jitter
  half=$(awk -v r="$3" 'BEGIN { printf "%.3fms", r / 2 }')
  jitter=$(awk -v j="${4:-0}" 'BEGIN { printf "%.3fms", j / 2 }')
  tc -n "$(ns "$1")" qdisc replace dev "$(link "$1" "$2")" root netem delay "$half" "$jitter" \
    loss random "${5:-0}%" limit 100000
}

set_delay() { # RTT_MS JITTER_MS LOSS_PCT
  local a b
  for a in $(seq 0 $((ZONE_COUNT - 1))); do
    for b in $(others "$a"); do
      netem "$a" "$b" "$1" "${2:-0}" "${3:-0}"
    done
  done
}

route() { # A B [ATTRIBUTES...]: zone A's route to zone B's node
  local a=$1 b=$2
  shift 2
  ip -n "$(ns "$a")" route replace "$(ip_of "$b")/32" dev "$(link "$a" "$b")" src "$(ip_of "$a")" "$@"
}

need_root() {
  [ "$(id -u)" = 0 ] || { echo "zones.sh $1: run it with sudo" >&2; exit 1; }
}

case "${1:-}" in
  up)
    need_root up
    for z in $(seq 0 $((ZONE_COUNT - 1))); do
      ip netns add "$(ns "$z")"
      ip -n "$(ns "$z")" link set lo up
      ip -n "$(ns "$z")" addr add "$(ip_of "$z")/32" dev lo
    done
    for a in $(seq 0 $((ZONE_COUNT - 1))); do
      for b in $(others "$a"); do
        [ "$a" -lt "$b" ] || continue
        ip link add "$(link "$a" "$b")" netns "$(ns "$a")" type veth peer name "$(link "$b" "$a")" netns "$(ns "$b")"
        for end in "$a $b" "$b $a"; do
          read -r x y <<< "$end"
          ip netns exec "$(ns "$x")" ethtool -K "$(link "$x" "$y")" tso off gso off > /dev/null
          ip -n "$(ns "$x")" link set dev "$(link "$x" "$y")" gso_max_segs 1 up
        done
        # Each node's address is on its loopback; a route a peer, over the link to it.
        route "$a" "$b"
        route "$b" "$a"
      done
    done
    set_delay "${2:-1}" "${3:-0}" "${4:-0}"
    ;;
  rtt)
    need_root rtt
    set_delay "${2:?usage: sudo scripts/zones.sh rtt RTT_MS [JITTER_MS] [LOSS_PCT]}" "${3:-0}" "${4:-0}"
    ;;
  link)
    need_root link
    usage="usage: sudo scripts/zones.sh link A B RTT_MS [LOSS_PCT]"
    netem "${2:?$usage}" "${3:?$usage}" "${4:?$usage}" 0 "${5:-0}"
    netem "$3" "$2" "$4" 0 "${5:-0}"
    ;;
  mtu)
    need_root mtu
    for a in $(seq 0 $((ZONE_COUNT - 1))); do
      for b in $(others "$a"); do
        ip -n "$(ns "$a")" link set dev "$(link "$a" "$b")" mtu "${2:?usage: sudo scripts/zones.sh mtu BYTES}"
      done
    done
    ;;
  tcp)
    need_root tcp
    case "${2:-}" in
      tuned) attributes=(rto_min 5ms quickack 1) ;;
      default) attributes=() ;;
      *) echo "usage: sudo scripts/zones.sh tcp tuned|default" >&2; exit 2 ;;
    esac
    for a in $(seq 0 $((ZONE_COUNT - 1))); do
      for b in $(others "$a"); do
        route "$a" "$b" "${attributes[@]}"
      done
    done
    ;;
  status)
    for a in $(seq 0 $((ZONE_COUNT - 1))); do
      for b in $(others "$a"); do
        printf 'zone %s to %s: %s, mtu %s, gso_max_segs %s, %s; ' "$a" "$b" \
          "$(sudo -n tc -n "$(ns "$a")" qdisc show dev "$(link "$a" "$b")" | grep -oE 'delay [0-9.]+[mu]?s( +[0-9.]+[mu]?s)?|loss [0-9.]+%' | tr '\n' ' ' || echo none)" \
          "$(sudo -n ip -n "$(ns "$a")" -o link show dev "$(link "$a" "$b")" | grep -oE 'mtu [0-9]+' | cut -d' ' -f2)" \
          "$(sudo -n ip -n "$(ns "$a")" -d -o link show dev "$(link "$a" "$b")" | grep -oE 'gso_max_segs [0-9]+' | cut -d' ' -f2)" \
          "$(sudo -n ip -n "$(ns "$a")" route show "$(ip_of "$b")/32" | grep -oE 'rto_min [0-9a-z]+|quickack [0-9]' | tr '\n' ' ' || true)tcp"
        sudo -n ip netns exec "$(ns "$a")" ping -c 5 -i 0.2 -q "$(ip_of "$b")" | grep -o 'rtt.*' || echo "no ping"
      done
    done
    ;;
  counts)
    for a in $(seq 0 $((ZONE_COUNT - 1))); do
      for b in $(others "$a"); do
        sudo -n tc -n "$(ns "$a")" -s qdisc show dev "$(link "$a" "$b")" |
          awk -v l="$a>$b" '/Sent/ { gsub(/,/, ""); print l, $4, $7, $2 }'
      done
    done
    ;;
  down)
    need_root down
    for z in $(seq 0 $((ZONE_COUNT - 1))); do
      ip netns del "$(ns "$z")" 2> /dev/null || true
    done
    ;;
  exec)
    zone=${2:?usage: scripts/zones.sh exec N COMMAND...}
    shift 2
    # The environment passed on as arguments: sudo resets it, and sudo-rs has no -E.
    mapfile -d '' vars < <(env -0)
    exec sudo -n ip netns exec "$(ns "$zone")" sudo -n -u "$(id -un)" -- env "${vars[@]}" "$@"
    ;;
  *)
    sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//' >&2
    exit 2
    ;;
esac
