# rusteron-cluster-interop

Runs `rusteron-cluster` against Java Aeron Cluster 1.53.3: a Java consensus module and archive per node, the Rust test service (`src/lib.rs`) as service 0 in `rust_service_node`, its Java twin (`java/OracleService.java`, the same logic) as service 1 of the same log, each node on its own C media driver from `rusteron-media-driver`, and a Java client. Not published.

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-23.jdk/Contents/Home BUILD_JAVA_HOME=$JAVA_HOME
cargo build -p rusteron-media-driver --bin media_driver --features static
cargo build -p rusteron-cluster-interop --features static
SCENARIO=basic scripts/differential.sh 1 1000
SCENARIO=failover scripts/differential.sh 3 1000
scripts/repeat.sh 20
```

Scenarios: `basic`, `restart`, `timers`, `failover` (3 nodes), `marks`, `dead-service`; what each does and what passing means is in `scripts/differential.sh` and `../rusteron-cluster/docs/design.md` §5. `scripts/skeleton.sh` runs a Java cluster alone on the C driver.

The Java programs run as single-file sources on Java 22 or later, with `aeron-all-1.53.3.jar` (`AERON_ALL_JAR`, default the local Maven repository).
