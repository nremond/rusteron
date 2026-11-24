# rusteron-system-tests

The tests that run rusteron's Aeron Archive and Aeron Cluster as a whole, as Aeron's `aeron-system-tests` does for Aeron: against Java Aeron 1.54.2 on the same log, under Jepsen, and in a deterministic simulator. Not published.

## Beside Java's nodes

`scripts/differential.sh` runs a cluster of Java consensus modules and archives, one per node, the Rust test service (`src/lib.rs`) as service 0 in `rust_service_node`, its Java twin (`java/OracleService.java`, the same logic) as service 1 of the same log, each node on its own C media driver from `rusteron-driver`, and a Java client.

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-23.jdk/Contents/Home BUILD_JAVA_HOME=$JAVA_HOME
cargo build -p rusteron-driver --bin media_driver --features static
cargo build -p rusteron-system-tests --features static
SCENARIO=basic scripts/differential.sh 1 1000
SCENARIO=failover scripts/differential.sh 3 1000
scripts/repeat.sh 20
```

Scenarios: `basic`, `restart`, `timers`, `failover` (3 nodes), `marks`, `dead-service`, and `tool`, which runs Java's ClusterTool and rusteron's on every node and requires the same output and exit status from both; what each does and what passing means is in `scripts/differential.sh` and `../rusteron-cluster/docs/design.md` §5. `scripts/skeleton.sh` runs a Java cluster alone on the C driver.

Each Java piece can be swapped for rusteron's: `TOOL=rust` runs rusteron's ClusterTool (`cluster_tool`) wherever a scenario runs Java's, `ARCHIVE=rust` puts the Rust archive (`aeron_archive`, from `rusteron-archive`) under every node, `CM=rust` the Rust consensus module (`consensus_module`, from `rusteron-cluster`) on every node and `CM_NODES="0 2"` on some, and `CLIENT=rust` the Rust client.

`scripts/backup.sh` runs the Rust cluster backup (`cluster_backup`) beside Java's ClusterBackup (`java/JavaBackup.java`) on one cluster, Java's modules or Rust's (`CM=rust`), and checks that both hold the leader's log byte for byte (`java/BackupCheck.java`, CRC-32C of replays) with the same recording-log entries; `SCENARIO=failover` kills the leader on the way. `scripts/bench.sh` measures the Rust container against Java's.

`archive/interop.sh` runs Java's archive client against the Rust archive (`archive/java`), kills it mid-recording and restarts it, has it replicate from a Java and a Rust archive, and runs Java's `ArchiveTool verify` on what it wrote. `archive/aeron-tests/run.sh` runs Aeron 1.54.2's own C archive client tests against it.

`aeron-tests/run.sh` runs Aeron 1.54.2's own Java cluster tests (`ClusterTest`, `ClusterBackupTest`, `RustClientTest` and others) with `TestNode` patched to start rusteron's consensus module, backup or client.

The Java programs run as single-file sources on Java 22 or later, with `aeron-all-1.54.2.jar` (`AERON_ALL_JAR`, default the local Maven repository).

## Jepsen

`jepsen/` holds Jepsen tests of three-member clusters, Java or Rust, under random faults and staged scenarios: see `jepsen/README.md`.

## The simulator

`sim` is a deterministic simulator of a cluster whose every member runs rusteron's consensus module: members, archives, services and a client on one thread in virtual time, under seeded crashes, partitions, clock jumps and snapshots, with invariants checked at every step (`docs/sim.md`).

```bash
cargo run -p rusteron-system-tests --release --bin cluster_sim -- --seeds 0..1000
cargo test -p rusteron-system-tests --test sim_seeds
```

## Licence

Apache-2.0, as Aeron: see `NOTICE`.
