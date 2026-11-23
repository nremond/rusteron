# rusteron-cluster-interop

Runs `rusteron-cluster` against Java Aeron Cluster 1.54.2: a Java consensus module and archive per node, the Rust test service (`src/lib.rs`) as service 0 in `rust_service_node`, its Java twin (`java/OracleService.java`, the same logic) as service 1 of the same log, each node on its own C media driver from `rusteron-media-driver`, and a Java client. Not published.

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-23.jdk/Contents/Home BUILD_JAVA_HOME=$JAVA_HOME
cargo build -p rusteron-media-driver --bin media_driver --features static
cargo build -p rusteron-cluster-interop --features static
SCENARIO=basic scripts/differential.sh 1 1000
SCENARIO=failover scripts/differential.sh 3 1000
scripts/repeat.sh 20
```

Scenarios: `basic`, `restart`, `timers`, `failover` (3 nodes), `marks`, `dead-service`; what each does and what passing means is in `scripts/differential.sh` and `../rusteron-cluster/docs/design.md` §5. `scripts/skeleton.sh` runs a Java cluster alone on the C driver.

`ARCHIVE=rust` puts the Rust archive (`aeron_archive`, from `rusteron-archive-server`) in place of Java's, and `CM=rust` the Rust consensus module (`consensus_module`, from `rusteron-cluster`) in place of Java's, on every node.

`scripts/backup.sh` runs the Rust cluster backup (`cluster_backup`) beside Java's ClusterBackup (`java/JavaBackup.java`) on one cluster, Java's modules or Rust's (`CM=rust`), and checks that both hold the leader's log byte for byte (`java/BackupCheck.java`, CRC-32C of replays) with the same recording-log entries; `SCENARIO=failover` kills the leader on the way. It runs on the release builds.

The Java programs run as single-file sources on Java 22 or later, with `aeron-all-1.54.2.jar` (`AERON_ALL_JAR`, default the local Maven repository).

`jepsen/` holds Jepsen tests of three-member clusters, Java or Rust, under random faults and staged scenarios: see `jepsen/README.md`.
