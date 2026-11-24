# rusteron-cluster

Aeron Cluster's clustered service container on Aeron's C client: runs a Rust service against Aeron Cluster's consensus module, wire- and on-disk-compatible with Aeron 1.54.2. The archive can be Rust's (`rusteron-archive`). The consensus module is being rebuilt in Rust, with Java as the oracle: its core is `rusteron-consensus-core`, and its shell the `consensus_module` binary here, which runs a node of a cluster of any size, beside Java's nodes or Rust's.

The container's logic is `rusteron-cluster-core`'s, a sans-IO state machine; this crate is its shell: one thread running the duty cycle and the Aeron client's conductor, the log up to the commit position, snapshots through the local archive, the mark file.

```rust,no_run
use rusteron_cluster::*;

fn run<S: ClusteredService>(service: S) -> Result<Exit, ContainerError> {
    let mut ctx = ContainerContext::new("/dev/shm/aeron-node0", "/var/lib/cluster/node0");
    ctx.config.service_id = 0;
    Ok(ServiceContainer::launch(ctx, service)?.run())
}
```

A service implements `ClusteredService`: it is handed the log's messages and timers, answers clients and schedules timers through `Cx`, and gives its snapshot as a `SnapshotSource` the container pulls without blocking.

What it guarantees, the tests behind it, and what is next: [docs/design.md](docs/design.md).

## Build

Aeron C is built from source, which needs CMake and, for its Gradle, a JDK older than 27 (`JAVA_HOME` and `BUILD_JAVA_HOME`). The `static` feature links it statically.

## Licence

Apache-2.0: see `NOTICE`.
