#!/usr/bin/env bash
# The Jepsen nodes: containers n1..nN on the bridge network aeron-jepsen,
# n<k> at 10.77.0.<10+k>, the host (Jepsen and its clients) at 10.77.0.1.
#
# Their image is empty: the host's /usr and /etc are mounted read-only, so a
# node runs the host's JDK and tools (iptables, pkill), with nothing pulled
# or installed. The suite, the Maven repository and rusteron's binaries are
# mounted read-only too; each node's data lives in a tmpfs at /data.
#
#   scripts/cluster.sh up [N]      scripts/cluster.sh down [N]
set -euo pipefail

HERE=$(cd "$(dirname "$0")/.." && pwd)
REPO=$(cd "$HERE/../.." && pwd)
N=${2:-3}
NET=aeron-jepsen
IMAGE=aeron-jepsen-node

case "${1:-}" in
  up)
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
      ctx=$(mktemp -d)
      echo "aeron jepsen node" >"$ctx/marker"
      printf 'FROM scratch\nCOPY marker /.jepsen-node\n' >"$ctx/Dockerfile"
      docker build -q -t "$IMAGE" "$ctx" >/dev/null
      rm -rf "$ctx"
    fi
    docker network inspect "$NET" >/dev/null 2>&1 ||
      docker network create --subnet 10.77.0.0/24 --gateway 10.77.0.1 "$NET" >/dev/null
    for ((k = 1; k <= N; k++)); do
      docker rm -f "n$k" >/dev/null 2>&1 || true
      docker run -d --name "n$k" --hostname "n$k" --network "$NET" --ip "10.77.0.$((10 + k))" \
        --cap-add NET_ADMIN --shm-size 2g \
        -v /usr:/usr:ro -v /etc:/etc:ro -v /usr/lib:/lib:ro -v /usr/lib64:/lib64:ro \
        -v /usr/bin:/bin:ro -v /usr/sbin:/sbin:ro \
        --tmpfs /tmp --tmpfs /run --tmpfs /var --tmpfs /data:size=4g \
        -v "$HERE:/opt/aeron-jepsen:ro" -v "$HOME/.m2/repository:/opt/m2:ro" -v "$REPO/target:/opt/rusteron:ro" \
        "$IMAGE" /usr/bin/sleep infinity >/dev/null
    done
    echo "nodes n1..n$N up"
    ;;
  down)
    for ((k = 1; k <= N; k++)); do docker rm -f "n$k" >/dev/null 2>&1 || true; done
    ;;
  *)
    echo "usage: $0 up|down [N]" >&2
    exit 2
    ;;
esac
