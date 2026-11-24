# Rusteron

[![Crates.io](https://img.shields.io/crates/v/rusteron-archive)](https://crates.io/crates/rusteron-archive)
[![CI](https://github.com/gsrxyz/rusteron/actions/workflows/ci.yml/badge.svg)](https://github.com/gsrxyz/rusteron/actions/workflows/ci.yml)
[![API Docs](https://docs.rs/rusteron-archive/badge.svg)](https://docs.rs/rusteron-archive/)
[![GitHub API Docs](https://custom-icon-badges.demolab.com/badge/githubdocs-blue.svg?logo=log&logoSource=feather)](https://gsrxyz.github.io/rusteron)

Rusteron provides fast, idiomatic Rust wrappers over the [Aeron](https://github.com/real-logic/aeron) C API
for IPC and UDP messaging, recording, replay and replication. It exposes the C API with
minimal abstraction, optimised for production use in latency-sensitive environments,
and manages resource ownership and callback lifetimes.

> **This fork's `cluster` branch** adds Aeron Cluster and the Aeron Archive server in Rust,
> wire- and on-disk-compatible with Aeron 1.54.2. They are not part of upstream rusteron: see
> [Aeron Cluster in Rust](#aeron-cluster-in-rust).
> [![CI (cluster)](https://github.com/nremond/rusteron/actions/workflows/ci.yml/badge.svg?branch=cluster)](https://github.com/nremond/rusteron/actions/workflows/ci.yml?query=branch%3Acluster)

## Design goals

- Match the safety and cost of the Aeron Java and C++ clients. Prefer compile-time
  checks such as lifetimes and borrow rules. Avoid runtime validation, locks and
  copies that those clients do not have.
- Pass calls through to Aeron C. If Aeron C accepts an invalid argument or state,
  report it to [Aeron](https://github.com/real-logic/aeron/issues) so every client
  gets the fix, rather than adding a workaround here.
- Document the rules Aeron leaves to the caller, such as not polling an image from
  inside a poll on the same image.
- Keep Aeron's names, so Aeron's own documentation applies. Applications still
  follow Aeron's ownership, thread-safety and polling rules. To check whether an
  issue is in a wrapper, reproduce it against the C API.

## Sponsored by GSR

Rusteron is sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.
Rusteron has been running in production in GSR's low-latency trading system since the beginning of 2025. It is mature and battle tested.
It is developed under the GSR GitHub organisation. We welcome contributions, feedback and discussion. To integrate or contribute, open an issue or reach out directly.

[Getting started](#getting-started) · [Examples](#examples) ·
[Migration to 0.3](docs/migrating_0.2_to_0.3.md) · [Guides](#documentation-and-guides)

## Choose a crate

| Crate | Use it for |
| --- | --- |
| [rusteron-client](rusteron-client/README.md) | Connect, publish and subscribe over IPC or UDP |
| [rusteron-archive](rusteron-archive/README.md) | The transport client plus recording, replay, replication and persistent subscriptions |
| [rusteron-media-driver](rusteron-media-driver/README.md) | Run an embedded or standalone C media driver, and extend it in Rust |
| [rusteron-code-gen](rusteron-code-gen/README.md) | Generate bindings and wrappers; an internal build dependency |

`rusteron-archive` includes the transport APIs, so applications using it do not
need a separate `rusteron-client` dependency.

## Aeron Cluster in Rust

This fork re-implements Aeron Cluster in Rust on Aeron's C client, with Java Aeron 1.54.2 as the
oracle: the clustered service container, the consensus module, the cluster client and backup, and
the Aeron Archive server a cluster records to. Unlike the wrappers above, these crates rewrite
Aeron's logic rather than bind to it. Rust and Java nodes can run in one cluster, on the same log,
and their files on disk follow Java's formats.

Each component is a sans-IO core (no clock, no randomness, no I/O) driven by a thin shell on
Aeron's C client. A deterministic simulator drives the consensus module's core under seeded faults.

| Crate | What it is |
| --- | --- |
| [rusteron-cluster](rusteron-cluster/README.md) | The shells: the service container, the cluster client, the consensus module (`consensus_module`) and the cluster backup (`cluster_backup`) |
| [rusteron-cluster-core](rusteron-cluster-core/README.md) | The service container's sans-IO core |
| [rusteron-consensus-core](rusteron-consensus-core/README.md) | The consensus module's sans-IO core |
| [rusteron-cluster-codecs](rusteron-cluster-codecs/README.md) | Aeron Cluster 1.54.2's SBE codecs |
| [rusteron-archive-server](rusteron-archive-server/README.md) | Aeron Archive's server (`aeron_archive`): what a cluster asks of its archive |
| [rusteron-archive-core](rusteron-archive-core/README.md) | The archive server's sans-IO core: the catalog, the segment files, the control protocol |
| [rusteron-archive-codecs](rusteron-archive-codecs/README.md) | Aeron Archive 1.54.2's SBE codecs |
| [rusteron-cluster-standby](rusteron-cluster-standby/README.md) | A warm standby: a cluster backup that also runs the service, takes snapshots the members load, and seeds a new cluster after a disaster |
| [rusteron-cluster-tools](rusteron-cluster-tools/README.md) | Run and check a cluster of separate processes: a node's media driver, a log comparison, status and snapshots, and the scripts |
| [rusteron-cluster-sim](rusteron-cluster-sim/README.md) | A deterministic simulator of a cluster of Rust consensus modules (not published) |
| [rusteron-cluster-interop](rusteron-cluster-interop/README.md) | Tests beside Java Aeron Cluster 1.54.2, and the scenario scripts (not published) |

**Tested against Java 1.54.2:**

- Aeron's own tests, with Rust consensus modules on every node: `ClusterTest` passes 75 of its 94
  cases, `ClusterBackupTest` 23 of 25 on the Rust backup, and `RustClientTest` and
  `StandbySnapshotNoticeTest` every case but five. Each case left needs something out of scope,
  below.
- The C archive client's tests: 61 of 80 pass on the Rust archive. The others make requests a
  cluster never makes.
- Ten scenarios, each on six setups mixing Java's and Rust's consensus modules, archives and
  clients.
- The simulator: 20,000 seeds with faults and 5,000 with held links.
- Jepsen with Elle, on three members: Rust members are valid under random partitions, kills and
  pauses, and in two staged election scenarios where Java 1.54.2 members cut committed entries or
  stop serving ([rusteron-cluster-interop/jepsen](rusteron-cluster-interop/jepsen/README.md)).
- The standby, with no JVM: its state and log against the members', a member restarted from its
  snapshot, and a new cluster seeded from it.

**Out of scope:** dynamic membership, consensus module extensions, authenticators other than the
default, IPC ingress, recording-log validation, and the archive requests a cluster does not make.
Two behaviours differ from Java 1.54.2 on purpose, each reported upstream: a stopped catch-up
replay is closed rather than revoked ([aeron-io/aeron#2259](https://github.com/aeron-io/aeron/issues/2259)),
and the archive starts over a recording with no frame after a page-aligned start
([aeron-io/aeron#2261](https://github.com/aeron-io/aeron/issues/2261)).

**Design notes:** the [service container](rusteron-cluster/docs/design.md), the
[consensus module](rusteron-cluster/docs/consensus.md), the [client](rusteron-cluster/docs/client.md),
the [backup](rusteron-cluster/docs/backup.md), the [archive server](rusteron-archive-server/docs/design.md)
and the [standby](rusteron-cluster-standby/docs/design.md). To run a cluster of separate processes,
see [rusteron-cluster-tools](rusteron-cluster-tools/README.md); the scenarios are in
`rusteron-cluster-interop/scripts`.

## Getting started

The examples use the 0.3 API. Existing 0.2 applications should
follow the [migration guide](docs/migrating_0.2_to_0.3.md).

For a static build:

```toml
[dependencies]
rusteron-client = { version = "0.3", features = ["static"] }
```

| Build | Features | Requirements |
| --- | --- | --- |
| Dynamic, from source | Default | C build tools and Java; shared libraries available at runtime |
| Static, from source | `static` | C build tools and Java |
| Precompiled static | `static`, `precompile` | Available for macOS and Linux; no CMake or Java |
| Precompiled static, rustls download | `static`, `precompile-rustls` | Same bundles, with a rustls downloader |

Linux also needs the native link dependencies listed in [BUILD.md](BUILD.md).
Replace `rusteron-client` with another crate as needed.

This example launches an embedded Media Driver, then publishes and receives one
message. The embedded driver comes from `rusteron-media-driver`. In production, run
the driver as its own process and connect with `Aeron::connect_dir(dir)`.

```rust
use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::time::Duration;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let driver = EmbeddedDriver::launch()?;
let context = AeronContext::new()?;
context.set_dir(&driver.dir().into_c_string())?;
context.set_error_handler(Some(AeronErrorHandlerLogger))?;
let client = Aeron::new(&context)?;
client.start()?;

let timeout = Duration::from_secs(5);
let subscription = client
    .async_add_subscription(c"aeron:ipc", 123, Handlers::NONE, Handlers::NONE)?
    .poll_blocking(timeout)?;
let publication = client
    .async_add_publication(c"aeron:ipc", 123)?
    .poll_blocking(timeout)?;

let mut idle = BackoffIdleStrategy::new();
loop {
    match publication.offer(b"hello") {
        Ok(position) => break println!("published at {position}"),
        Err(error) if error.is_retryable() => idle.idle(0), // not connected yet or back pressure
        Err(error) => return Err(error.into()),
    }
}

let mut received = None;
while received.is_none() {
    let fragments = subscription.poll_fn(|message, _header| received = Some(message.to_vec()), 10)?;
    idle.idle(fragments);
}
assert_eq!(received.as_deref(), Some(&b"hello"[..]));
# Ok(()) }
```

Without `set_error_handler`, Aeron C's default handler prints an asynchronous
client error to stderr and exits the process.

`poll_blocking` busy-polls the calling thread until the registration completes.
It keeps setup code and examples short. In an event loop, call the async
registration's `poll()` yourself. See the
[non-blocking publisher](rusteron-client/examples/non_blocking_publisher.rs).

## Ownership and threading

| Topic | Rule |
| --- | --- |
| Owning handles | Clones share native ownership where `Clone` is supported; final cleanup waits for the last owner |
| Borrowed getters | `Ref<'_, T>` borrows its parent and cannot be cloned or outlive that parent |
| Buffer claims | `try_claim_owned()` returns a lifetime-bound guard; commit it or let drop abort it |
| Shared handles | `Aeron`, regular publications, counters and counter readers are `Send + Sync`; share one through `Arc` or a clone ([example](docs/client-guide.md#sharing-across-threads)) |
| Local handles | Exclusive publications, images, contexts, async pollers, assemblers, Archive and driver handles stay local; create them on the thread that uses them |
| Subscriptions | `Send` but not `Sync` and not `Clone`; move one to the thread that polls it |
| Configuration | Configure contexts before creating their clients or starting their drivers |

There is no `multi-threaded` feature. Storage and thread traits are selected at
compile time. Publishing and raw polling do not clone an `Arc` or dispatch through
a Rust trait object.

See [owning and borrowed handles](docs/migrating_0.2_to_0.3.md#owning-and-borrowed-handles) and
[thread safety](docs/thread-safety.md) for the full rules.

## Callbacks

| Need | API |
| --- | --- |
| Poll fragments with local state | `subscription.poll_fn(closure, limit)` |
| Receive complete messages | `FragmentAssembler::new(callback)` for a fixed callback, or `AeronFragmentClosureAssembler::new()` for a closure per poll, then `assembler.poll(...)` |
| Share callback state outside the poll loop | `Handler::new(callback)`. Not used by polls |
| Register a callback that may run on the conductor | Pass an owned callback to its setter, or use `SharedHandler::new_shared(callback)` |

**The poll path uses no `Rc`, `Arc`, `RefCell` or `Mutex`.** `poll(&mut callback, limit)`,
`poll_fn` and the owned assemblers pass Aeron C a plain `&mut` to your callback as
`clientd`. For each fragment, the trampoline casts `clientd` back to `&mut F` and calls
it. Rusteron adds no lock, borrow check, reference count or allocation per fragment or
per poll. `Handler` (`Rc<RefCell<T>>`) and `SharedHandler` (`Arc<Mutex<T>>`) exist only for
callbacks that C keeps after the call returns, such as error, available image,
unavailable image and close handlers. The conductor calls those on events, not per
message.

Registered callbacks remain alive while C can call them. Dropping your handler
does not unregister it. See the [client callback guide](docs/client-guide.md#handlers-and-callbacks).

### Panics in callbacks

Synchronous polls return panics to the polling caller. Retained callbacks log
panics and continue. Message redelivery depends on the callback type. See
[callback panics](docs/callback-panics.md) before relying on recovery behaviour.

## Examples

| Example | Shows |
| --- | --- |
| [Basic pub/sub](rusteron-client/examples/basic_pub_sub.rs) | Connect, publish and assemble messages |
| [Non-blocking publisher](rusteron-client/examples/non_blocking_publisher.rs) | Drive setup from an event loop |
| [Zero-copy claim](rusteron-client/examples/zero_copy_claim.rs) | Claim, write and commit a message |
| [Record and replay](rusteron-archive/examples/record_and_replay.rs) | Record a stream and replay its history |
| [Persistent subscription](rusteron-archive/examples/persistent_subscription.rs) | Replay history and join live traffic |
| [Docker samples](rusteron-docker-samples/README.md) | Run a driver, publisher and subscriber |

## Documentation and guides

- [Migrate from 0.2 to 0.3](docs/migrating_0.2_to_0.3.md)
- [Client guide](docs/client-guide.md) and [Archive guide](docs/archive-guide.md)
- [Build requirements](BUILD.md) and [CPU targets and cross-language LTO](docs/cross-language-lto.md)
- [Media Driver configuration and back pressure](docs/media_driver_configuration_and_backpressure_guide.md)
- [Media driver extensions in Rust](docs/driver-extensions.md)
- [Multi-destination subscriptions](docs/mdc_mds_guide.md)
- [Tuning on x86-64 Linux](docs/tuning_x86_64_linux.md)
- [How to benchmark and measured results](BENCHMARKS.md)
- Earlier releases: [What's new in 0.2](docs/whats_new_0.2.md), [0.1-to-0.2 migration](docs/migrating_0.1.168_to_0.2.md)

## Development

Run `just` to list build and test tasks. See [CONTRIBUTING.md](CONTRIBUTING.md)
for the contribution workflow and required checks.

## License

Dual-licensed under MIT or Apache-2.0.

## Acknowledgements

- [@mimran1980](https://github.com/mimran1980), Rusteron's original creator at GSR.
- [@bspeice](https://github.com/bspeice), for the original [libaeron-sys](https://github.com/bspeice/libaeron-sys).
- The [Aeron](https://github.com/real-logic/aeron) community.
