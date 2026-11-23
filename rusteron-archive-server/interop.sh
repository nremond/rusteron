#!/usr/bin/env bash
# Java's archive client against the Rust archive (java/io/aeron/archive/ArchiveInterop.java), on a Java
# media driver; then Java's ArchiveTool verifies the catalog and segment files the Rust archive wrote. Last,
# the archive replicates from a Java archive and from a Rust one. `./interop.sh java` runs the same against
# Java's own archive: the checks are Java's behaviour, and Java's replication from the Rust archive.
#
#   cargo build --release -p rusteron-archive-server --features static && ./interop.sh
#
# AERON_ALL_JAR overrides the jar, ARCHIVE_BIN the archive's binary.
set -euo pipefail

WHICH=${1:-rust}
HERE=$(cd "$(dirname "$0")" && pwd)
JAR=${AERON_ALL_JAR:-$HOME/.m2/repository/io/aeron/aeron-all/1.54.2/aeron-all-1.54.2.jar}
ARCHIVE_BIN=${ARCHIVE_BIN:-$HERE/../target/release/aeron_archive}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/rusteron-archive-interop.XXXXXX")
AERON_DIR=$WORK/aeron
JAVA=(java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED
  --sun-misc-unsafe-memory-access=allow)
DRIVER=""
ARCHIVE=""
SOURCE=""
cleanup() {
  [ -n "$ARCHIVE" ] && kill "$ARCHIVE" 2>/dev/null || true
  [ -n "$SOURCE" ] && kill "$SOURCE" 2>/dev/null || true
  [ -n "$DRIVER" ] && kill "$DRIVER" 2>/dev/null || true
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

await() { # FILE PATTERN
  for _ in $(seq 1 200); do grep -q "$2" "$1" 2>/dev/null && return 0; sleep 0.05; done
  echo "timed out waiting for '$2' in $1" >&2; cat "$1" >&2; return 1
}

javac -cp "$JAR" -d "$WORK/classes" "$HERE/java/io/aeron/archive/ArchiveInterop.java"
"${JAVA[@]}" -cp "$JAR" -Daeron.dir="$AERON_DIR" -Daeron.threading.mode=SHARED -Daeron.dir.delete.on.start=true \
  io.aeron.driver.MediaDriver > "$WORK/driver.log" 2>&1 &
DRIVER=$!
for _ in $(seq 1 200); do [ -f "$AERON_DIR/cnc.dat" ] && break; sleep 0.05; done

# Starts the archive on $WORK/archive with id $2, logging to $WORK/archive-$1.log. Each start takes an id
# of its own: a killed archive's counters stay with the driver until it times the client out.
start_archive() {
  LOG=$WORK/archive-$1.log
  ID=$2
  if [ "$WHICH" = java ]; then
    "${JAVA[@]}" -cp "$JAR" -Daeron.dir="$AERON_DIR" -Daeron.archive.dir="$WORK/archive" -Daeron.archive.id="$ID" \
      -Daeron.archive.control.channel.enabled=false \
      -Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0 \
      -Daeron.archive.client.control.response.channel=aeron:udp?endpoint=localhost:0 \
      -Daeron.archive.control.response.channel=aeron:udp?endpoint=localhost:0 \
      -Daeron.archive.threading.mode=SHARED -Daeron.archive.mark.file.dir="$WORK/archive" \
      io.aeron.archive.Archive > "$LOG" 2>&1 &
    ARCHIVE=$!
    for _ in $(seq 1 200); do [ -f "$WORK/archive/archive-mark.dat" ] && break; sleep 0.05; done
    sleep 1
    echo READY >> "$LOG"
  else
    "$ARCHIVE_BIN" --aeron-dir "$AERON_DIR" --archive-dir "$WORK/archive" --archive-id "$ID" > "$LOG" 2>&1 &
    ARCHIVE=$!
  fi
  await "$LOG" READY
}

# Stops the archive cleanly, then Java's ArchiveTool checks its files.
stop_archive() {
  kill -TERM "$ARCHIVE"
  wait "$ARCHIVE" || true
  ARCHIVE=""
  [ "$WHICH" = java ] || grep -q EXIT "$LOG"
  "${JAVA[@]}" -cp "$JAR" io.aeron.archive.ArchiveTool "$WORK/archive" verify
}

interop() {
  "${JAVA[@]}" -cp "$WORK/classes:$JAR" io.aeron.archive.ArchiveInterop "$AERON_DIR" "$ID" "$@" || {
    cat "$LOG"
    exit 1
  }
}

start_archive first 4242
interop run
stop_archive
"${JAVA[@]}" -cp "$JAR" io.aeron.archive.ArchiveTool "$WORK/archive" describe

# Killed while it records: started again on its files, it recovers where the recording got to.
start_archive second 4243
interop record > "$WORK/record.log" 2>&1 &
RECORDER=$!
await "$WORK/record.log" RECORDED
read -r _ RECORDING POSITION MESSAGES < <(grep RECORDED "$WORK/record.log")
kill -KILL "$ARCHIVE"
wait "$ARCHIVE" 2>/dev/null || true
kill "$RECORDER" 2>/dev/null || true
wait "$RECORDER" 2>/dev/null || true
echo "killed the archive at recording $RECORDING position $POSITION ($MESSAGES messages)"
# Its mark file goes stale.
sleep 11
start_archive third 4244
interop recovered "$RECORDING" "$POSITION"
stop_archive
# Replication: this archive copies from a source archive, Java's then Rust's, on the same driver: the source's
# requests come over UDP, its local control stream another than this one's.
SOURCE_CONTROL=aeron:udp?endpoint=localhost:8010
start_source() { # KIND ID
  SOURCE_LOG=$WORK/source-$1.log
  if [ "$1" = java ]; then
    "${JAVA[@]}" -cp "$JAR" -Daeron.dir="$AERON_DIR" -Daeron.archive.dir="$WORK/source" -Daeron.archive.id="$2" \
      -Daeron.archive.control.channel="$SOURCE_CONTROL" -Daeron.archive.local.control.stream.id=11 \
      -Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0 \
      -Daeron.archive.threading.mode=SHARED -Daeron.archive.mark.file.dir="$WORK/source" \
      io.aeron.archive.Archive > "$SOURCE_LOG" 2>&1 &
    SOURCE=$!
    for _ in $(seq 1 200); do [ -f "$WORK/source/archive-mark.dat" ] && break; sleep 0.05; done
    sleep 1
  else
    "$ARCHIVE_BIN" --aeron-dir "$AERON_DIR" --archive-dir "$WORK/source" --archive-id "$2" \
      --control-channel "$SOURCE_CONTROL" --local-control-stream-id 11 > "$SOURCE_LOG" 2>&1 &
    SOURCE=$!
    await "$SOURCE_LOG" READY
  fi
}
REPLICATION_ID=4245
SOURCE_ID=5000
for SOURCE_KIND in java rust; do
  start_source "$SOURCE_KIND" "$SOURCE_ID"
  start_archive "replica-of-$SOURCE_KIND" "$REPLICATION_ID"
  interop replicate "$SOURCE_CONTROL" || { cat "$SOURCE_LOG"; exit 1; }
  stop_archive
  kill -TERM "$SOURCE"
  wait "$SOURCE" || true
  rm -rf "$WORK/source" "$WORK/archive"
  echo "replicated from a $SOURCE_KIND archive"
  REPLICATION_ID=$((REPLICATION_ID + 1))
  SOURCE_ID=$((SOURCE_ID + 1))
done
echo "INTEROP OK ($WHICH)"
