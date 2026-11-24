#!/usr/bin/env bash
# Regenerates src/cluster, src/mark and src/node from schemas/ with sbe-tool 1.42.0, the
# SBE version Aeron 1.54.2 generates its own codecs with. The output is
# checked in, so building the crate needs no Java.
#
#   ./regen.sh
#
# SBE_TOOL_JAR and AERON_ALL_JAR (which supplies Agrona) override the jars.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
SBE_TOOL_JAR=${SBE_TOOL_JAR:-$HOME/.m2/repository/uk/co/real-logic/sbe-tool/1.42.0/sbe-tool-1.42.0.jar}
AERON_ALL_JAR=${AERON_ALL_JAR:-$HOME/.m2/repository/io/aeron/aeron-all/1.54.2/aeron-all-1.54.2.jar}
OUT=$(mktemp -d "${TMPDIR:-/tmp}/rusteron-cluster-codecs.XXXXXX")
trap 'rm -rf "$OUT"' EXIT

java --sun-misc-unsafe-memory-access=allow \
  -Dsbe.output.dir="$OUT" \
  -Dsbe.target.language=Rust \
  -Dsbe.validation.stop.on.error=true \
  -cp "$SBE_TOOL_JAR:$AERON_ALL_JAR" \
  uk.co.real_logic.sbe.SbeTool \
  "$HERE/schemas/aeron-cluster-codecs.xml" \
  "$HERE/schemas/aeron-cluster-mark-codecs.xml" \
  "$HERE/schemas/aeron-cluster-node-state-codecs.xml"

# sbe-tool writes one crate per schema; each becomes a module of this crate,
# its lib.rs the module's mod.rs, and its paths to the crate root paths to the
# module.
module() { # generated-crate module
  rm -rf "$HERE/src/codecs/$2"
  mkdir -p "$HERE/src/codecs/$2"
  cp "$OUT/$1/src/"*.rs "$HERE/src/codecs/$2/"
  mv "$HERE/src/codecs/$2/lib.rs" "$HERE/src/codecs/$2/mod.rs"
  perl -pi -e 's/\bcrate::/super::/g' "$HERE/src/codecs/$2/"*.rs
}
module io_aeron_cluster_codecs cluster
module io_aeron_cluster_codecs_mark mark
module io_aeron_cluster_codecs_node node
