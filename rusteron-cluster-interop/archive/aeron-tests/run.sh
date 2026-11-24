#!/usr/bin/env bash
# Runs Aeron 1.54.2's own C archive client tests (aeron-archive/src/test/c/client) against the Rust archive:
# builds them from rusteron's vendored Aeron with `java` pointed at java.in (Aeron's C media driver and
# aeron_archive in place of Java's ArchivingMediaDriver), then runs them and lists what failed.
#
#   cargo build --release -p rusteron-archive --features static,server && aeron-tests/run.sh [GTEST_FILTER]
#
# BUILD_DIR (default ~/aeron-ctest) keeps the C build between runs.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
AERON=$ROOT/rusteron-archive/aeron
BUILD_DIR=${BUILD_DIR:-$HOME/aeron-ctest}
JAR=${AERON_ALL_JAR:-$HOME/.m2/repository/io/aeron/aeron-all/1.54.2/aeron-all-1.54.2.jar}
ARCHIVE_BIN=${ARCHIVE_BIN:-$ROOT/target/release/aeron_archive}
mkdir -p "$BUILD_DIR"

# The tests' build wants the jar Java's archive runs from; the stand-in does not use it.
mkdir -p "$AERON/aeron-all/build/libs"
[ -f "$AERON/aeron-all/build/libs/aeron-all-1.54.2.jar" ] || cp "$JAR" "$AERON/aeron-all/build/libs/"

REAL_JAVA=${JAVA_HOME:+$JAVA_HOME/bin/}java
REAL_JAVA=$(command -v "$REAL_JAVA")
sed -e "s|@AERONMD@|$BUILD_DIR/binaries/aeronmd|" -e "s|@ARCHIVE_BIN@|$ARCHIVE_BIN|" -e "s|@REAL_JAVA@|$REAL_JAVA|" \
  "$HERE/java.in" > "$BUILD_DIR/java"
chmod +x "$BUILD_DIR/java"

rm -f "$BUILD_DIR/CMakeCache.txt"
cmake -S "$AERON" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release -DAERON_TESTS=ON -DAERON_UNIT_TESTS=OFF \
  -DAERON_SYSTEM_TESTS=ON -DAERON_BUILD_SAMPLES=OFF -DAERON_BUILD_DOCUMENTATION=OFF -DBUILD_AERON_ARCHIVE_API=ON \
  -DJava_JAVA_EXECUTABLE="$BUILD_DIR/java" > "$BUILD_DIR/cmake.log"
cmake --build "$BUILD_DIR" --parallel 16 --target aeronmd aeron_archive_test > "$BUILD_DIR/build.log"

cd "$BUILD_DIR"
TEST=$(find "$BUILD_DIR" -name aeron_archive_test -type f -perm -u+x | head -1)
# Each test on its own, so one that hangs costs its timeout, not the run.
"$TEST" --gtest_list_tests --gtest_filter="${1:-*}" | awk '/^[^ ]/ { suite = $1 } /^  / { print suite $1 }' > tests.txt
mkdir -p logs
passed=0
failed=()
while read -r test; do
  log="logs/${test//\//_}.log"
  if timeout --kill-after=10 180 "$TEST" --gtest_filter="$test" > "$log" 2>&1; then
    passed=$((passed + 1))
  else
    failed+=("$test")
  fi
done < tests.txt
echo "passed $passed of $(wc -l < tests.txt)"
for test in "${failed[@]}"; do
  echo "FAILED $test: $(grep -m1 -E "Failure|error|Expected|ERROR" "logs/${test//\//_}.log" | cut -c1-160)"
done
