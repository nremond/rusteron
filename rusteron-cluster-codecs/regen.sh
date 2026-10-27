#!/usr/bin/env bash
# Regenerates src/cluster and src/mark from schemas/ with sbe-tool 1.40.2, the
# SBE version Aeron 1.53.3 generates its own codecs with. The output is
# checked in, so building the crate needs no Java.
#
#   ./regen.sh
#
# SBE_TOOL_JAR and AERON_ALL_JAR (which supplies Agrona) override the jars.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
SBE_TOOL_JAR=${SBE_TOOL_JAR:-$HOME/.m2/repository/uk/co/real-logic/sbe-tool/1.40.2/sbe-tool-1.40.2.jar}
AERON_ALL_JAR=${AERON_ALL_JAR:-$HOME/.m2/repository/io/aeron/aeron-all/1.53.3/aeron-all-1.53.3.jar}
OUT=$(mktemp -d "${TMPDIR:-/tmp}/rusteron-cluster-codecs.XXXXXX")
trap 'rm -rf "$OUT"' EXIT

java --sun-misc-unsafe-memory-access=allow \
  -Dsbe.output.dir="$OUT" \
  -Dsbe.target.language=Rust \
  -Dsbe.validation.stop.on.error=true \
  -cp "$SBE_TOOL_JAR:$AERON_ALL_JAR" \
  uk.co.real_logic.sbe.SbeTool \
  "$HERE/schemas/aeron-cluster-codecs.xml" \
  "$HERE/schemas/aeron-cluster-mark-codecs.xml"

# sbe-tool writes one crate per schema; each becomes a module of this crate,
# its lib.rs the module's mod.rs, and its paths to the crate root paths to the
# module.
module() { # generated-crate module
  rm -rf "$HERE/src/$2"
  mkdir -p "$HERE/src/$2"
  cp "$OUT/$1/src/"*.rs "$HERE/src/$2/"
  mv "$HERE/src/$2/lib.rs" "$HERE/src/$2/mod.rs"
  perl -pi -e 's/\bcrate::/super::/g' "$HERE/src/$2/"*.rs
}
module io_aeron_cluster_codecs cluster
module io_aeron_cluster_codecs_mark mark
