# Aeron Cluster in Rust: the cluster client

The fourth milestone of Aeron Cluster in Rust (see `design.md` for the service container and `consensus.md` for the consensus module): the client a gateway connects with, wire-compatible with Java Aeron 1.54.2's `AeronCluster` (1.53.3's until 2026-11-23), as a sans-IO state machine driven by a shell on Aeron's C client.

| | |
|---|---|
| **Status** | M4, 2026-10-31. A Rust `AeronCluster`: the connect, redirects, challenges, offers, keepalives, admin requests, the egress and its session events, a new leader followed, the session closed. Every interop scenario passes with it on Java's modules, Rust's and mixes; Aeron's own client cases pass on both module kinds, the authentication ones on Java's only (§5). The simulator's client is the real one, and 22,000 seeds of faults pass. An application's bench on it measures at parity with Java's client (§6). Its tests found two faults in the Rust consensus module, both fixed. |
| **Code** | `rusteron-cluster::client` (`async_connect`, `aeron_cluster`, `codec`, the `ClientPorts` trait), and on the C client `client::io` (`AeronCluster`, `AsyncConnect`); `rusteron-system-tests` (the simulator's client; `echo_client`, `bench_client`, `client_lines`; `RustClientTest` in `aeron-tests`). |
| **Licence** | Apache-2.0 only, as Aeron: derived from its schemas and behaviour. |

---

## 1. Why

With the container, the archive and the consensus module in Rust, a node can run with no JVM, but every client still needed one: an application's gateways and benches, and this fork's own interop clients. Aeron 1.53.3 has no cluster client in C or C++.

## 2. The contract

Java 1.53.3's `AeronCluster`, read from the source, kept as Java does it:

- **The connect** (`AsyncConnect`): one deadline for the whole connect, `messageTimeoutNs` (5 s). The egress subscription first, then its port resolved for the response channel, with `rejoin=false` added to a UDP egress. With ingress endpoints, a publication to every member, all added before any is used, and the connect request sent through the first that connects; without them, one publication on the ingress channel as given, multicast or IPC. A follower's REDIRECT re-parses the members, the leader's publication kept if its endpoint is unchanged. A challenge is answered through a `CredentialsSupplier`. ERROR and AUTHENTICATION_REJECTED fail the connect, CLOSED is ignored, messages of other schemas skipped. The new leader timeout is twice the leader heartbeat timeout unless given.
- **The connected client**: offers with the 32-byte session header pre-encoded, as one message or in parts; keepalives and the snapshot admin request, tried three times; the egress polled ten messages at most, each for this session only, through `EgressListener` or `ControlledEgressListener` (`on_message`, `on_session_event`, `on_new_leader`, `on_admin_response`). A message of another schema is an error, as Java's `EgressAdapter` throws without an extension.
- **The states**: CONNECTED, AWAIT_NEW_LEADER, AWAIT_NEW_LEADER_CONNECTION, PENDING_CLOSE, CLOSED, with Java's transitions. An offer refused as NOT_CONNECTED or CLOSED, or the egress image going, awaits a new leader; a NewLeaderEvent switches the ingress to it, and only an offer that goes through counts it connected. MAX_POSITION_EXCEEDED, the cluster's CLOSED event, or no new leader within the timeout close the session.
- **The context**: Java's settings and defaults (ingress and egress channels, streams 101 and 102, endpoints, message timeout, new leader timeout, client name, exclusive ingress), and `conclude()`'s checks. The client says it is `version=1.54.2 commit=rusteron-cluster-<version>`.

Out of scope: `tryClaim` (offers copy), the extension listeners for other schemas, Java's agent-invoker settings (the shell has its own, §3), `isDirectAssemblers`, a cluster backup's client side (Java's `ClusterBackup` covers it, `consensus.md` §5).

## 3. The design

**The core** (`rusteron-cluster::client`, `forbid(unsafe_code)`): `Connect` is `AsyncConnect` step by step, `poll` returning the connected `Client` once done; `Client` is the connected `AeronCluster`. Both reach Aeron through the `ClientPorts` trait: asynchronous adds and removes, offers of parts, a controlled poll that hands over whole messages with their image, whether an image went, the clock, correlation ids. The codecs are written by hand and check every length, against Java's bytes (Challenge's fixture added to the golden ones).

**The shell** (`rusteron-cluster::client::io`): `AeronCluster::connect` blocks on the core with Java's backoff; `async_connect` returns an `AsyncConnect` the caller polls. It reassembles egress fragments per image and learns of lost images from the C client's callbacks. Its Aeron client is the caller's or its own, run by a conductor thread or, with `use_conductor_agent_invoker`, from its polls; `ClientIdle` picks spinning or Java's backoff.

**The simulator** (`rusteron-system-tests`) implements `ClientPorts` on its message-level network: a publication ready a step after it is added, an image per member incarnation. Its application sends tagged commands, keeps an idle session alive, and connects again when the session closes or its messages go unanswered for 3 s, so every seed runs the real client through crashes, partitions and elections.

## 4. Choices, and why

- **A new leader's publication is awaited across polls.** Java blocks in `pollEgress` until the new leader's publication is added, up to the message timeout. The core tracks it as pending, and offers meanwhile return NOT_CONNECTED, as Java's would after its wait. The states and timeouts a caller sees are Java's.
- **Members in id order.** Java iterates a hash map, the client a sorted map: which member gets the request first is not part of the contract, and tests check outcomes, not which member was asked.
- **An offer refused for the message itself** (too long, too many parts) returns `PUBLICATION_ERROR` (-6, Aeron C's code), the session untouched. Java's offer throws. The shell first mapped every other error to CLOSED, which dropped a connected session to await a new leader.
- **A refused add is not removed.** Before cancelling a pending add, the shell polls it once. Java's client asks the driver to remove only an add it has not heard fail; C's cancel asks regardless, and the driver logs an unknown publication. Aeron's redirect test with an invalid endpoint caught it.

## 5. Tests

- **Unit tests** (`rusteron-cluster`): 26 scenarios on fake ports with a virtual clock, from Java's `AeronClusterAsyncConnectTest`, `AeronClusterTest`, `EgressPollerTest`, `EgressAdapterTest` and `AeronClusterContextTest`, and one per rule they leave out: the challenge, the leader's publication kept on a redirect, the connect's single deadline. Five codec tests check the client's messages against Java's bytes.
- **The simulator**: 20,000 seeds with faults and 2,000 with 60 s of faults pass with the real client, as do the regression seeds.
- **Interop** (`differential.sh` with `CLIENT=rust`): `echo_client` and `bench_client` are `EchoClient.java`'s and `BenchClient.java`'s twins. `repeat.sh` passes every scenario 3 times on Java's modules, 13 on Rust's, twice with a Rust module on node 0 and on nodes 1 and 2, and twice on Rust's modules and archives. Two of the harness's assumptions broke on a client that starts in milliseconds:
  - In `uncommitted`, the client had every echo back before the leader was killed. It now sends ten times as many, and the scenario fails if nothing was left uncommitted.
  - The shutdown came while a restarted node was still in its election, which a node's READY does not wait for. The leader sends the termination position once, and a member takes it only in its current term, in Java's module as in Rust's, so the node waited for good. JavaNode now prints its election state counter as it changes, and the script waits for every node's ELECTION CLOSED.
- **Aeron's own tests** (`aeron-tests`): `RustClient`, in the test harness, drives `client_lines` as a test drives `AeronCluster`. `RustClientTest` has 23 cases: from `ClusterTest`, connect, redirect, the leader's publication reused (3), a new leader, session timeout, keepalives, the cluster gone, admin requests, fragmented messages and two sessions; `ClusterSessionReliabilityTest`'s ingress image lost (unicast or multicast, with or without a failover) and egress not rejoined; `AuthenticationTest`'s five; and a message too long. All 23 pass on Java's modules; on rusteron's, the 18 others pass, the authentication cases needing an authenticator the module leaves out. `ClusterTest` still passes its 74 cases on the fixed module, the same 19 out of scope as before.
- **What they found in the consensus module** (`consensus.md` §5):
  - A restarted one-node leader logged "quorum position went backwards" in 8 runs of 20 on Rust archives. It had taken its recording counter before the archive stored the recording's start position, a window Java's archive closes within a millisecond and rusteron's debug builds left open 10 ms. The module now waits until the counter reads that position: none in 20 since.
  - Followers subscribed to a multicast ingress, which Java's `connectIngress` skips, and redirected clients to the leader's unicast endpoint, where nothing listens: 5 multicast connects of 6 timed out. The leader alone takes it now.

## 6. Measurements

Measured on Java 1.53.3 and not rerun on 1.54.2: the application's benchmark they went through no longer has the Java parts compared (`design.md` §10).

An application's commits on a 16-core Linux workstation: three members with its Rust engine, archives and module on Aeron's C driver, no JVM, busy-spinning with a thread for each driver agent. Its Java bench client against its Rust one on this client, each with a driver of its own on the client's cores, three rounds interleaved; commands a second and median latency, offer to answer:

| In flight | Log | Java's client | Rust client |
|---|---|---|---|
| 1 | memory | 36,100–36,500 · 25.6–27.0 µs | 36,100–38,600 · 25.8–26.7 µs |
| 1 | synced | 1,540–1,730 · 517.8–519.9 µs | 1,610–1,730 · 516.2–518.4 µs |
| 64 | memory | 455,000–571,000 · 111–141 µs | 441,000–561,000 · 114–145 µs |
| 64 | synced | 22,900–35,800 · 1.59–2.80 ms | 33,000–36,100 · 1.58–1.63 ms |
| 1,024 | memory | 562,000–572,000 · 1.78–1.81 ms | 449,000–566,000 · 1.81–2.28 ms |
| 1,024 | synced | 297,000–321,000 · 3.15–3.28 ms | 312,000–316,000 · 3.20–3.22 ms |

The clients are at parity. At 64 and 1,024 in flight in memory, runs land near 440,000 or near 565,000 commands a second with either client, as the members on the C driver did before. At one in flight, changing the whole client side, library and driver, moves the median by less than the rounds differ, so the client is not where those 26 µs go. Every run's replicas agreed.

## 7. Run it

```bash
cargo test -p rusteron-cluster --test client --test client_codec
cargo build -p rusteron-driver --bin media_driver --features static
cargo build -p rusteron-system-tests --features static
CLIENT=rust SCENARIO=failover rusteron-system-tests/scripts/differential.sh 3
cargo build --release -p rusteron-cluster --features static --bin consensus_module
cargo build --release -p rusteron-system-tests --features static --bin client_lines
JAVA_CM=1 rusteron-system-tests/aeron-tests/run.sh io.aeron.cluster.RustClientTest
rusteron-system-tests/aeron-tests/run.sh io.aeron.cluster.RustClientTest
```
