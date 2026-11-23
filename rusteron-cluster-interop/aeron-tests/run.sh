#!/usr/bin/env bash
# Runs Aeron 1.54.2's own cluster system tests with rusteron's consensus module in place of Java's on every
# node: a copy of rusteron's vendored Aeron with testnode.patch applied (TestNode starts the consensus_module
# binary that AERON_TEST_RUST_CM names, and reads its counters from the node's media driver), run through
# Gradle. Lists what passed and what failed, with the failure's first line.
#
#   cargo build --release -p rusteron-cluster --features static --bin consensus_module && aeron-tests/run.sh [TESTS...]
#
# TESTS are Gradle's --tests filters (default io.aeron.cluster.ClusterTest). BUILD_DIR (default
# ~/aeron-cluster-tests) keeps the Gradle build between runs, and each Rust module's output in rust-cm-logs.
# JAVA_CM=1 runs the same tests on Java's consensus modules, the oracle. RustClientTest drives rusteron's cluster
# client (rusteron-cluster-interop's client_lines, CLIENT_BIN) in place of Java's AeronCluster. RUST_BACKUP=1 runs
# rusteron's cluster backup (cluster_backup, BACKUP_BIN) in place of Java's ClusterBackup on every backup node: for
# ClusterBackupTest, say.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
AERON=$ROOT/rusteron-archive/aeron
BUILD_DIR=${BUILD_DIR:-$HOME/aeron-cluster-tests}
# Release: a debug build's blocking adds sleep 10 ms a poll, and a new leader's term then reaches its voters after
# their ballots time out, 1 ms after the candidate's.
CM_BIN=${CM_BIN:-$ROOT/target/release/consensus_module}
CLIENT_BIN=${CLIENT_BIN:-$ROOT/target/release/client_lines}
BACKUP_BIN=${BACKUP_BIN:-$ROOT/target/release/cluster_backup}
FILTERS=()
for t in "${@:-io.aeron.cluster.ClusterTest}"; do FILTERS+=(--tests "$t"); done
[[ -n "${JAVA_CM:-}" || -x "$CM_BIN" ]] || { echo "build the consensus module first" >&2; exit 1; }
RUST_CM=$CM_BIN
[[ -n "${JAVA_CM:-}" ]] && RUST_CM=
RUST_BACKUP_BIN=
if [[ -n "${RUST_BACKUP:-}" ]]; then
  [[ -x "$BACKUP_BIN" ]] || { echo "build the cluster backup first" >&2; exit 1; }
  RUST_BACKUP_BIN=$BACKUP_BIN
fi

# Pristine sources each run, the build kept.
rsync -a --delete --exclude '/build/' --exclude '/*/build/' --exclude '/.gradle/' --exclude '/.git' "$AERON/" "$BUILD_DIR/"
(cd "$BUILD_DIR" && patch -s -p1 < "$HERE/testnode.patch")

cd "$BUILD_DIR"
rm -rf aeron-system-tests/build/test-results rust-cm-logs
# ClusterTest is a @SlowTest: the slowTest task, not test, runs it.
AERON_TEST_RUST_CLIENT=$CLIENT_BIN AERON_TEST_RUST_CM=$RUST_CM AERON_TEST_RUST_BACKUP=$RUST_BACKUP_BIN \
  AERON_TEST_RUST_CM_LOG_DIR=$BUILD_DIR/rust-cm-logs ./gradlew --no-daemon -q :aeron-system-tests:slowTest "${FILTERS[@]}" \
  -x checkstyleMain -x checkstyleTest -x javadoc > "$BUILD_DIR/gradle-test.log" 2>&1 || true

python3 -I - "$BUILD_DIR/aeron-system-tests/build/test-results/slowTest" <<'PY'
import glob, sys, xml.etree.ElementTree as ET
passed, failed, skipped = [], [], []
for f in sorted(glob.glob(sys.argv[1] + "/*.xml")):
    for case in ET.parse(f).getroot().iter("testcase"):
        name = case.get("classname").split(".")[-1] + "." + case.get("name")
        failure = case.find("failure") if case.find("failure") is not None else case.find("error")
        if case.find("skipped") is not None:
            skipped.append(name)
        elif failure is not None:
            failed.append((name, (failure.get("message") or failure.text or "").strip().splitlines()[:1]))
        else:
            passed.append(name)
print(f"passed {len(passed)}, failed {len(failed)}, skipped {len(skipped)}")
for name, message in failed:
    print(f"FAILED {name}: {(message[0] if message else '')[:160]}")
PY
