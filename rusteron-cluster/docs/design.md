# Aeron Cluster in Rust: the service container

Aeron Cluster re-implemented in Rust, in this fork of rusteron, wire- and on-disk-compatible with Java Aeron 1.53.3, as sans-IO state machines driven by a shell on Aeron's C client. This document covers its first piece, the clustered service container (M1), and what it rests on (M0).

| | |
|---|---|
| **Status** | M1 done, 2026-10-27. A Rust service runs in a Rust container beside a Java service on a Java 1.53.3 consensus module and archive, over Aeron's C media driver: started, joined, snapshotted, restarted from its snapshot, through a leader's failover, terminated. Every scenario's traces match the Java twin's line for line, and the container's part of every snapshot is Java's bytes; the six scenarios passed 20 times in a row, 120 runs, none failed (§5). Not measured yet: latency, throughput (§7). |
| **Code** | `rusteron-cluster-codecs` (Aeron's SBE codecs, generated, checked against Java's bytes), `rusteron-cluster-core` (the container as a sans-IO state machine: `service`, plus `log`, `position`, `counters`, `markfile`, and a `testkit`), `rusteron-cluster` (the shell: `container.rs`, `markfile.rs`), `rusteron-cluster-interop` (the Rust test service, `rust_service_node`, the Java side, `scripts/`) |
| **Plan** | Re-implement Aeron Cluster one piece at a time, each replacing its Java twin inside a working Java cluster, Java the oracle at every step: M0 foundations, M1 service container (this), M2 archive server, M3 consensus module, M4 cluster client, M5 cexy on it, M6 operations. |
| **Licence** | The four crates are Apache-2.0 only, as Aeron is: they are derived from its schemas and behaviour (`NOTICE` in each). rusteron's own crates stay MIT OR Apache-2.0. |

---

## 1. Why

cexy, SwissBorg's central limit order book, replicates its log with openraft. Its owner decided that Aeron is the right transport for a venue whose first criterion is latency control, and that Aeron Cluster's design is the consensus to run on it. Aeron Cluster is Java only: its consensus module, service container and archive server have no C implementation, and rusteron wraps only Aeron's C client, archive client and media driver. So the cluster is rebuilt here in Rust.

Three decisions shape it:

- **Sans-IO.** Each piece is a state machine that takes events and time and returns actions: no clock, no randomness, no I/O. A shell drives it on Aeron's C client in production; a simulator can drive it in virtual time, as cexy's simulator drives openraft. Upstream fixes are re-derived from Java, not ported line by line.
- **Wire- and on-disk-compatible with Java 1.53.3**, the version rusteron's Aeron submodule pins. Rust and Java pieces interoperate, and Java's `ClusterTool` and `ArchiveTool` read Rust nodes' files.
- **One piece at a time, Java as the oracle.** The container first, against Java's consensus module; then the archive under Java's cluster; then the consensus module among Java nodes.

## 2. The contract

The container honours Java 1.53.3's `ClusteredServiceAgent`, read from the source:

| Part | What |
|---|---|
| Streams | To the consensus module on stream 105, from it on stream 104, both `aeron:ipc?term-length=128k`. Out: `ScheduleTimer`, `CancelTimer`, `ServiceAck`, `CloseSession`, service messages (a `SessionMessageHeader` whose session id is the service id). In: `JoinLog`, `ServiceTerminationPosition` (with v17's `ackRequired`), `RequestServiceAck`. |
| Acks | Numbered 0, 1, 2…: at start, with the Aeron client's id as `relevantId` (the consensus module watches that client's heartbeat to know the service alive); each log joined; each snapshot, with its recording; each requested ack; termination (at most 5 offers). The consensus module stops on an ack out of sequence. |
| The log | Joined with `alias=log-sc-<id>`, its image required to join at the service's position; polled up to the commit-position counter (type 203), 50 fragments a poll, reassembled as `BoundedLogAdapter` does, each message at the position after its last frame; ended at the next join's position, or end of stream. Cluster actions and new terms use the position in their message, as Java does. |
| Recovery | The recovery-state counter (type 204) names the snapshot each service starts from; it is replayed from position 0 on IPC stream 103. |
| Snapshots | Taken at exactly the request's position, the log waiting: an exclusive publication (`aeron:ipc?alias=snapshot`, stream 106) recorded by the local archive, `SnapshotMarker` BEGIN (type id 2), the client sessions in id order, END, then the service's own bytes; acked once the recording reaches the publication's position. |
| Mark file | `cluster-mark-service-<id>.dat` in the cluster's directory: an 8 KiB header (SBE header, `MarkFileHeader`), then Agrona's distinct error log; the activity timestamp refreshed every second, the version written last, a null activity at stop. A live one (activity under 10 s old) is not started over. |

## 3. The design

```text
                 rusteron-cluster (shell, one thread)                rusteron-cluster-core (sans-IO)
 C media driver ◄─ Aeron C client (conductor invoked on this thread)
   stream 104 ────► poll ─────────────────────────────► on_cm_message, after_cm_poll
   log image  ────► bounded_controlled_poll ──────────► on_log_fragment ─► ClusteredService
   replay     ────► controlled_poll ──────────────────► on_snapshot_fragment
   counters   ────► commit position, recording position ► log_poll_limit, on_io(SnapshotRecorded)
   archive    ◄──── start_replay, start_recording ◄──── poll_action (JoinLog, RecordSnapshot, …)
   stream 105 ◄──── offer ◄─────────────────────────── drain_cm (in order, never dropped)
   egress     ◄──── offer ◄─────────────────────────── drain_egress (a queue per session)
   mark file  ◄──── pwrite ◄────────────────────────── MemberId, Error, Stop
```

**The core** (`rusteron-cluster-core::service::Container`) owns every position: the log's, the max log position, the ack id, termination and requested-ack positions, the snapshot's, the reassembly buffer. The shell reports only what it observes: the commit counter's value, an image's status, a recording's position, the outcome of an offer. Time enters through `on_tick` alone; the service's own clock is the log's timestamp (`Cx::time`).

**The service** implements `ClusteredService`. Callbacks that may not act on the cluster get a `ReadCx` (start, role change, snapshot, termination, background work); the others a `Cx`, through which they answer clients, send service messages, schedule and cancel timers, close sessions. What a callback sends is staged, and kept only once it returns `Apply::Done`: on `Apply::Retry` the entry is delivered again later and what was staged is discarded, so a service can push onto a full ring without blocking.

**Snapshots** are pulled, never pushed: `on_take_snapshot` returns a `SnapshotSource` the core pulls a publication's payload at a time without blocking, which another thread may fill (`channel_source`). Loading is the mirror: a `SnapshotSink` whose `finish` may stay pending while a restore runs elsewhere.

**The shell** (`rusteron-cluster::ServiceContainer`) runs one thread: the Aeron client uses Aeron's conductor-invoker mode, so its conductor runs on the container's thread, every millisecond. Each duty cycle reads the clocks once, then: every millisecond, the conductor, the commit counter's liveness and the mark file and the consensus module's stream; the actions the core asked for; the log; a snapshot's replay or recording; the queues. Calls into the service from inside a C callback run under `catch_unwind`: a panic there must not unwind through C, and becomes a fatal error.

## 4. Choices, and why

| Choice | Why |
|---|---|
| Sans-IO, re-derived from Java | Simulation in virtual time, as cexy's simulator found openraft's defects; at the cost of porting each upstream fix by hand. |
| Wire and on-disk compatibility with 1.53.3 | Each Rust piece is tested inside a Java cluster, and Java's tools work on Rust nodes. 1.53.3 is what rusteron's Aeron C is. |
| Codecs from sbe-tool 1.40.2, checked in | The SBE version Aeron 1.53.3 generates its own with; checked in so building needs no Java. Its readers check only the buffer's length, so the core checks lengths and variable-length fields before decoding. |
| Nothing for the consensus module dropped | Java's `scheduleTimer` returns false under back-pressure and leaves the retry to the service; cexy's Java branch dropped such a timer and stopped its cluster's clock. Here the queue is retried in order, and a full queue (1,024 messages) holds the log instead, ending the poll at the message that filled it. |
| `Apply::Retry` | A service fed by a bounded ring (cexy's engine) can refuse an entry it cannot take yet, without blocking the container's thread. |
| One queue per client, 4,096 frames in all, 5 s to be taken | A slow client holds the log rather than lose answers, but one that never takes them is dropped and reported, not waited on forever. |
| The mark file written with `pwrite` | Java reads it through a mapping of the same page cache; positional writes need no `unsafe` mapping. Header first, activity timestamp, version last, as Java signals ready. Java's volatile reads of the timestamp never saw it torn (§5). |
| The recording counter checked by registration id | Java checks the recording id in the counter's key; rusteron's `get_counter_key` reads a key up to its first zero byte, which cuts a binary key short (§6). A registration id names one allocation as surely. |
| App version 0.0.1 by default | Java's default, written in every snapshot marker: a Rust and a Java service's snapshots are then the same bytes. |
| Synchronous archive connect, replay and record | Java's container does the same; the core models them as request and completion, so moving to the archive's async proxy later changes only the shell. |

## 5. Tests

**Codecs** (`rusteron-cluster-codecs/tests/golden.rs`, 18 tests): every message the container reads or writes, and the mark file's header, written by Aeron 1.53.3's own Java codecs (`java/GoldenCodecs.java`), decodes in Rust to the values Java set and encodes back to the same bytes. A field new in version 17 reads as unset from an older message.

**The core** (`rusteron-cluster-core`, 22 unit tests, 9 container tests) against the testkit: a log writer that frames messages as a publication does, the consensus module's messages, a fake consensus module holding acks to Java's rule. A whole life through a snapshot and termination; a restart from that snapshot; a snapshot cut short; a refused join tried again; 1,000 timers under refusals arriving in order; a full queue holding the log; a follower's egress mocked; the log ended at the next join. A property test (200 cases): logs of random messages up to 6,000 bytes, framed at MTUs of 1,408 to 8,192, committed a few at a time, polled a few fragments at a time, with random retries: each message applied once, in order, at the position after its last frame, and what a refused delivery sent discarded.

**Against Java** (`rusteron-cluster-interop`, `scripts/differential.sh`): a Java 1.53.3 consensus module and archive per node, the Rust test service as service 0 and its Java twin (`OracleService.java`, the same logic) as service 1 of the same log, each node on its own C media driver, a Java client. Every scenario sends 1,000 messages, snapshots through `ClusterTool`, sends 1,000 more, then:

| Scenario | What happens | One run on this Mac |
|---|---|---|
| basic | shutdown through `ClusterTool` | 2,566 trace lines alike, 400 timers |
| restart | every process killed (-9) and restarted from disk, then 1,000 more | both start again from their snapshots at 116,928; 5,132 lines alike |
| timers | each service schedules 10,000 timers from one callback | 20,600 timers alike in 23,849 lines |
| failover | 3 nodes: the leader's machine killed, 1,000 through the new leader, the node restarted, 1,000 more | 6,414 and 5,131 lines alike on the three nodes |
| marks | Java's `ClusterMarkFile` reads the Rust mark file; its activity watched for 5 s; a Java container for service 0 started over it | 389M volatile reads, 5 changes, never torn; the Java container refuses |
| dead-service | the Rust service killed | the consensus module logs "Aeron client in service closed unexpectedly: serviceId=0" |

Each passes when every echo came back in order, the Rust container exited as terminated, the two traces match line for line, the container's part of each pair of snapshots (BEGIN to END) is the same bytes, and `ClusterTool errors` shows no error from either container (in failover, the consensus module's leader heartbeat timeout only). `scripts/repeat.sh N` runs them all N times in a row: on this Mac (M-series, macOS, Java 27 for the nodes), `repeat.sh 20` passed all six 20 times in a row, 120 runs in 21 minutes, on 2026-10-27.

## 6. What the first runs found

- **rusteron's `get_counter_key` stops at the key's first zero byte** (`aeron_custom.rs`), so binary keys come back short. The snapshot's recording counter looked gone, the container acked the snapshot without a recording, and the consensus module, logging "service=0 failed to take snapshot", dropped the snapshot from its recording log. The container checks registration ids now; the fix belongs in rusteron (return the whole key, `AERON_COUNTER_MAX_KEY_LENGTH` bytes), to be proposed upstream.
- **Java's default app version is 0.0.1**, not 0: the markers differed by that field until the defaults agreed.
- **A container restarted within 10 s of a kill refuses its own live mark file**, as Java's archive refuses its own: the restart waits out the liveness timeout, Aeron's own rule.
- In the harness: a node killed without its media driver keeps answering for its endpoints until the driver's client liveness timeout, so a client connecting just after a failover waited on the dead leader; a Java node whose launch failed could not exit, its shutdown barrier waiting to be closed.

## 7. Next steps

1. **Measure.** Nothing here is measured yet for latency or throughput: commit to apply, apply to egress, against the Java container on the same log, busy-spinning, on Linux.
2. **cexy's engine as a service** (on a cexy branch): its `Machine` behind `ClusteredService`, its snapshotter behind `channel_source`, `Apply::Retry` onto its ring. That is Option B of cexy's design without the FFI: the Java consensus module, the Rust engine.
3. **The archive's async proxy** in the shell, so a snapshot's connect, replay and record never hold the duty cycle.
4. **M2, the archive server**, the cluster's subset: 14 of the archive's 41 control requests, record, replay and replicate, on-disk compatible; tested by Aeron's own C archive-client tests (they launch the archive as a process), by `ArchiveTool verify` on Rust-written archives, and by Java clusters on it.
5. **M3, the consensus module**, among Java nodes, then alone; and the simulator: the cores in virtual time, Aeron's streams modelled at the positions they show.
6. **Upstream to rusteron**: the counter-key fix; whether gsrxyz takes Apache-2.0-only crates.

## 8. Run it

Building Aeron C needs a JDK older than 27 for its Gradle; the Java nodes run on 27.

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-23.jdk/Contents/Home BUILD_JAVA_HOME=$JAVA_HOME
cargo test -p rusteron-cluster-codecs -p rusteron-cluster-core -p rusteron-cluster
cargo build -p rusteron-media-driver --bin media_driver --features static
cargo build -p rusteron-cluster-interop --features static
SCENARIO=failover rusteron-cluster-interop/scripts/differential.sh 3 1000
rusteron-cluster-interop/scripts/repeat.sh 20
```

`AERON_ALL_JAR` names the Java 1.53.3 jar (default: the local Maven repository); `KEEP=1` keeps a run's work directory, logs, traces and archives.
