#!/usr/bin/env bash
# Runs a Jepsen test: the Java side built, the classpath resolved, the nodes
# up (scripts/cluster.sh), then Jepsen with the options given, e.g.
#
#   ./run.sh --scenario stale-leader --election-timeout-ms 3000 --time-limit 90
#   ./run.sh --scenario random --time-limit 300
#
# Results go to store/ (./run.sh serve shows them on port 8080).
set -euo pipefail
cd "$(dirname "$0")"
mvn -q -B compile dependency:build-classpath -Dmdep.outputFile=classpath.txt 2>&1 | grep -v '^WARNING' || true
NODES=${NODES:-3}
cmd=${1:-test}
if [[ $cmd == serve ]]; then
  exec java -cp "target/classes:src/main/clojure:$(cat classpath.txt)" clojure.main -m aeron.jepsen.core serve
fi
scripts/cluster.sh up "$NODES"
nodes=()
for ((k = 1; k <= NODES; k++)); do nodes+=(--node "n$k"); done
exec java -Xmx4g -Djava.awt.headless=true --sun-misc-unsafe-memory-access=allow \
  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED \
  -cp "target/classes:src/main/clojure:$(cat classpath.txt)" \
  clojure.main -m aeron.jepsen.core test "${nodes[@]}" --concurrency 5 "$@"
