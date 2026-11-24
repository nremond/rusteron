#!/usr/bin/env bash
# Rewrites tests/fixtures with Aeron 1.54.2's own code: java/io/aeron/cluster/ConsensusFixtures.java writes a
# recording log and the operations that made it, recovery plans of real recordings on an embedded archive,
# node state files, and the counters a consensus module allocates.
#
#   ./fixtures.sh
#
# AERON_ALL_JAR overrides the jar.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
AERON_ALL_JAR=${AERON_ALL_JAR:-$HOME/.m2/repository/io/aeron/aeron-all/1.54.2/aeron-all-1.54.2.jar}
OUT=$(mktemp -d "${TMPDIR:-/tmp}/rusteron-cluster-fixtures.XXXXXX")
trap 'rm -rf "$OUT"' EXIT

javac -cp "$AERON_ALL_JAR" -d "$OUT" "$HERE"/java/io/aeron/cluster/*.java
java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  --sun-misc-unsafe-memory-access=allow -cp "$OUT:$AERON_ALL_JAR" io.aeron.cluster.ConsensusFixtures \
  "$HERE/tests/fixtures"
