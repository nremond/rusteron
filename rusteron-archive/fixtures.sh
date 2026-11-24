#!/usr/bin/env bash
# Rewrites tests/fixtures with Aeron 1.54.2's own code:
# - java/io/aeron/archive/CatalogFixtures.java: a catalog and the operations that made it, segment
#   files with the stop position Java recovers, and an archive before and after ArchiveTool verify;
# - java/io/aeron/archive/ConductorFixtures.java: what the archive makes of channels, and the counters
#   it allocates, on an embedded driver.
#
#   ./fixtures.sh
#
# AERON_ALL_JAR overrides the jar.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
AERON_ALL_JAR=${AERON_ALL_JAR:-$HOME/.m2/repository/io/aeron/aeron-all/1.54.2/aeron-all-1.54.2.jar}
OUT=$(mktemp -d "${TMPDIR:-/tmp}/rusteron-archive.XXXXXX")
trap 'rm -rf "$OUT"' EXIT

javac -cp "$AERON_ALL_JAR" -d "$OUT" "$HERE"/java/io/aeron/archive/*.java
for fixtures in CatalogFixtures ConductorFixtures; do
  java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    --sun-misc-unsafe-memory-access=allow -cp "$OUT:$AERON_ALL_JAR" "io.aeron.archive.$fixtures" "$HERE/tests/fixtures"
done
