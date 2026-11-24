# Aeron Cluster in Rust: the service container

Aeron Cluster re-implemented in Rust, in this fork of rusteron, wire- and on-disk-compatible with Java Aeron 1.54.2 (1.53.3 until 2026-11-23, which the dated results below were measured on unless they say otherwise; §10 has the move), as sans-IO state machines driven by a shell on Aeron's C client. This document covers its first piece, the clustered service container (M1), and what it rests on (M0).

| | |
|---|---|
| **Status** | M1 done, 2026-10-27. A Rust service runs in a Rust container beside a Java service on a Java 1.53.3 consensus module and archive, over Aeron's C media driver: started, joined, snapshotted, restarted from its snapshot, through a leader's failover, terminated. Every scenario's traces match the Java twin's line for line, and the container's part of every snapshot is Java's bytes; the six scenarios passed 20 times in a row, 120 runs, none failed, on macOS (arm64) and on Linux (x86_64) (§5). Measured on 2026-10-28, one node: the Rust container is at parity with Java's, applying the log 1.0–1.4 µs after append at p50 and answering a client in 9.2–9.9 µs round trip at p50, with p99.99 and maxima mostly shorter (§6). |
| **Code** | `rusteron-cluster`: `service` (the container as a sans-IO state machine, with `clustered_service`, `consensus_module_proxy`, `service_snapshot_taker`), `cluster_counters`, `service::cluster_mark_file` and a `testkit`; the shell `service::clustered_service_container` and `service::cluster_mark_file::io`; the SBE codecs in `codecs`, generated, checked against Java's bytes. `rusteron-system-tests` (the Rust test service, `rust_service_node`, the Java side, `scripts/`). |
| **Plan** | Re-implement Aeron Cluster one piece at a time, each replacing its Java twin inside a working Java cluster, Java the oracle at every step: M0 foundations, M1 service container (this), M2 archive server, M3 consensus module, M4 cluster client, M5 the cluster backup and a node with no JVM, M6 operations. |
| **Licence** | The four crates are Apache-2.0 only, as Aeron is: they are derived from its schemas and behaviour (`NOTICE` in each). rusteron's own crates stay MIT OR Apache-2.0. |

---

## 1. Why

Aeron is a transport built for latency control, and Aeron Cluster runs Raft-style consensus on it. Aeron Cluster is Java only: its consensus module, service container and archive server have no C implementation, and rusteron wraps only Aeron's C client, archive client and media driver. So the cluster is rebuilt here in Rust.

Three decisions shape it:

- **Sans-IO.** Each piece is a state machine that takes events and time and returns actions: no clock, no randomness, no I/O. A shell drives it on Aeron's C client in production; a simulator can drive it in virtual time. Upstream fixes are re-derived from Java, not ported line by line.
- **Wire- and on-disk-compatible with Java 1.54.2**, the version rusteron's Aeron submodule pins. Rust and Java pieces interoperate, and Java's `ClusterTool` and `ArchiveTool` read Rust nodes' files.
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
| Mark file | `cluster-mark-service-<id>.dat` in the cluster's directory: an 8 KiB header (SBE header, `MarkFileHeader`), then Agrona's distinct error log; the activity timestamp refreshed every second, the version written last, a null activity at stop. A live one is not started over: one whose writer holds the lock file beside it (`cluster-mark-service-<id>.lck`, with the writer's pid and start time), or, with no lock naming its writer, one active within 10 s (`rusteron-cluster::markfile::owner`). A writer killed is proven gone by its released lock, and is replaced at once. |

## 3. The design

```text
                 clustered_service_container (shell, one thread)     service (sans-IO)
 C media driver ◄─ Aeron C client (conductor invoked on this thread)
   stream 104 ────► poll ─────────────────────────────► on_cm_message, after_cm_poll
   log image  ────► bounded_controlled_poll ──────────► on_log_fragment ─► ClusteredService
   egress     ◄──── offer_parts ◄───────────────────── its sink, as each callback returns
   replay     ────► controlled_poll ──────────────────► on_snapshot_fragment
   counters   ────► commit position, recording position ► log_poll_limit, on_io(SnapshotRecorded)
   archive    ◄──── start_replay, start_recording ◄──── poll_action (JoinLog, RecordSnapshot, …)
   stream 105 ◄──── offer ◄─────────────────────────── drain_cm (in order, never dropped)
   egress     ◄──── offer_parts ◄───────────────────── drain_egress (what was refused)
   mark file  ◄──── pwrite ◄────────────────────────── MemberId, Error, Stop
```

**The core** (`rusteron-cluster::service::Container`) owns every position: the log's, the max log position, the ack id, termination and requested-ack positions, the snapshot's, the reassembly buffer. The shell reports only what it observes: the commit counter's value, an image's status, a recording's position, the outcome of an offer. Time enters through `on_tick` alone; the service's own clock is the log's timestamp (`Cx::time`).

**The service** implements `ClusteredService`. Callbacks that may not act on the cluster get a `ReadCx` (start, role change, snapshot, termination, background work); the others a `Cx`, through which they answer clients, send service messages, schedule and cancel timers, close sessions. What a callback sends is staged, and kept only once it returns `Apply::Done`: on `Apply::Retry` the entry is delivered again later and what was staged is discarded, so a service can push onto a full ring without blocking. Answers are staged in one reused buffer, and on `Done` go straight into the session's publication, its header and payload as Aeron's two-part offer takes them, through a sink the shell hands `on_log_fragment`: no allocation, no wait for the end of the duty cycle. Only an answer the publication refuses is copied into its session's queue, and the session's next answers queue behind it.

**Snapshots** are pulled, never pushed: `on_take_snapshot` returns a `SnapshotSource` the core pulls a publication's payload at a time without blocking, which another thread may fill (`channel_source`). Loading is the mirror: a `SnapshotSink` whose `finish` may stay pending while a restore runs elsewhere.

**The shell** (`rusteron-cluster::ServiceContainer`) runs one thread: the Aeron client uses Aeron's conductor-invoker mode, so its conductor runs on the container's thread, every millisecond. Each duty cycle reads the clocks once, then: every millisecond, the conductor, the commit counter's liveness and the mark file and the consensus module's stream; the actions the core asked for; the log; a snapshot's replay or recording; the queues. Calls into the service from inside a C callback run under `catch_unwind`: a panic there must not unwind through C, and becomes a fatal error.

## 4. Choices, and why

| Choice | Why |
|---|---|
| Sans-IO, re-derived from Java | Simulation in virtual time, where consensus defects show that real clusters rarely reach; at the cost of porting each upstream fix by hand. |
| Wire and on-disk compatibility with 1.54.2 | Each Rust piece is tested inside a Java cluster, and Java's tools work on Rust nodes. 1.54.2 is what rusteron's Aeron C is. |
| Codecs from sbe-tool 1.42.0, checked in | The SBE version Aeron 1.54.2 generates its own with; checked in so building needs no Java. Its readers check only the buffer's length, so the core checks lengths and variable-length fields before decoding. |
| Nothing for the consensus module dropped | Java's `scheduleTimer` returns false under back-pressure and leaves the retry to the service; a service that drops such a timer stops its cluster's clock. Here the queue is retried in order, and a full queue (1,024 messages) holds the log instead, ending the poll at the message that filled it. |
| `Apply::Retry` | A service fed by a bounded ring (an engine on its own thread) can refuse an entry it cannot take yet, without blocking the container's thread. |
| Answers offered as their callback returns; a queue per client only for refusals, 4,096 frames in all, 5 s to be taken | Java's service offers from inside its callback; this offers right after, so a refused answer can still be discarded with its entry. A slow client holds the log rather than lose answers, but one that never takes them is dropped and reported, not waited on forever. |
| The mark file written with `pwrite` | Java reads it through a mapping of the same page cache; positional writes need no `unsafe` mapping. Header first, activity timestamp, version last, as Java signals ready. Java's volatile reads of the timestamp never saw it torn (§5). |
| The recording counter checked by registration id | Java checks the recording id in the counter's key; rusteron's `get_counter_key` reads a key up to its first zero byte, which cuts a binary key short (§7). A registration id names one allocation as surely. |
| App version 0.0.1 by default | Java's default, written in every snapshot marker: a Rust and a Java service's snapshots are then the same bytes. |
| Synchronous archive connect, replay and record | Java's container does the same; the core models them as request and completion, so moving to the archive's async proxy later changes only the shell. |

## 5. Tests

**Codecs** (`rusteron-cluster/tests/codecs_golden.rs`, 18 tests): every message the container reads or writes, and the mark file's header, written by Aeron 1.54.2's own Java codecs (`java/GoldenCodecs.java`), decodes in Rust to the values Java set and encodes back to the same bytes. A field new in version 17 reads as unset from an older message.

**The core** (`rusteron-cluster`, 22 unit tests, 11 container tests) against the testkit: a log writer that frames messages as a publication does, the consensus module's messages, a fake consensus module holding acks to Java's rule. A whole life through a snapshot and termination; a restart from that snapshot; a snapshot cut short; a refused join tried again; 1,000 timers under refusals arriving in order; a full queue holding the log; an answer offered in the poll that applied its message; a refused answer offered again, in its place, with the session's next answers behind it; a follower's egress mocked; the log ended at the next join. A property test (200 cases): logs of random messages up to 6,000 bytes, framed at MTUs of 1,408 to 8,192, committed a few at a time, polled a few fragments at a time, with random retries: each message applied once, in order, at the position after its last frame, and what a refused delivery sent discarded.

**Against Java** (`rusteron-system-tests`, `scripts/differential.sh`): a Java 1.53.3 consensus module and archive per node, the Rust test service as service 0 and its Java twin (`OracleService.java`, the same logic) as service 1 of the same log, each node on its own C media driver, a Java client. Every scenario sends 1,000 messages, snapshots through `ClusterTool`, sends 1,000 more, then:

| Scenario | What happens | One run on this Mac |
|---|---|---|
| basic | shutdown through `ClusterTool` | 2,566 trace lines alike, 400 timers |
| restart | every process killed (-9) and restarted from disk, then 1,000 more | both start again from their snapshots at 116,928; 5,132 lines alike |
| timers | each service schedules 10,000 timers from one callback | 20,600 timers alike in 23,849 lines |
| failover | 3 nodes: the leader's machine killed, 1,000 through the new leader, the node restarted, 1,000 more | 6,414 and 5,131 lines alike on the three nodes |
| marks | Java's `ClusterMarkFile` reads the Rust mark file; its activity watched for 5 s; a Java container for service 0 started over it | 389M volatile reads, 5 changes, never torn; the Java container refuses |
| dead-service | the Rust service killed | the consensus module logs "Aeron client in service closed unexpectedly: serviceId=0" |

Each passes when every echo came back in order, the Rust container exited as terminated, the two traces match line for line, the container's part of each pair of snapshots (BEGIN to END) is the same bytes, and `ClusterTool errors` shows no error from either container (in failover, the consensus module's leader heartbeat timeout only). `scripts/repeat.sh N` runs them all N times in a row. `repeat.sh 20` passed all six 20 times in a row, 120 runs, on 2026-10-27:

| Machine | Build | Nodes | 120 runs |
|---|---|---|---|
| Mac, arm64, macOS | Apple clang, JDK 23 for Aeron's Gradle | Java 27 | 21 min |
| Linux workstation, x86_64, 32 cores, Ubuntu 26.04 | gcc 15, JDK 25 | Java 25 | 20 min |

The first loop on Linux stopped at its 42nd run, on a warning the consensus module logs as it closes (§7); the check now allows it, and the loop then passed whole.

## 6. Measurements

`scripts/bench.sh` on a Linux workstation, 2026-10-28: AMD Ryzen 9 7950X (16 cores in two complexes of 8, each with its own L3 cache; SMT on), Ubuntu 26.04, `powersave` governor, no isolated cores; release builds (fat LTO, Aeron C static); one node of Java 1.53.3 with its cluster clock in nanoseconds; every agent busy-spinning (the drivers' conductor, sender and receiver, the archive, the consensus module, both services, the client), each process on CPUs of its own: the node (its media driver, the Rust service, the Java node) on one core complex, the client and its driver on the other; ZGC for the JVMs. The Rust bench service is service 0 and the Java one service 1 of the same log; each run starts afresh, warms up 15 s, measures 20 s; three rounds, interleaved, each figure below a range over them.

- **Apply**, from the consensus module stamping an entry as it appends it to the service applying it, for both services on the same entries at the same time. It includes the archive recording the entry and the commit position moving.
- **Round trip**, from when an open-loop client was due to send to the answer it got back, through both media drivers, the consensus module's ingress and the answering service's egress.

| Rate | Measure | p50 | p99 | p99.9 | p99.99 | max |
|---|---|---|---|---|---|---|
| 1k/s | apply, Rust | 1.2–1.4 | 2.4–2.6 | 4.2–5.0 | 5.1–7.2 | 5.5–15.2 |
| | apply, Java | 1.2–1.4 | 2.4–2.6 | 3.7–5.2 | 5.3–20.3 | 7.6–26.5 |
| | round trip, Rust answering | 9.7–9.9 | 11.7–11.9 | 14.1–14.4 | 16.7–22.1 | 21.8–23.3 |
| | round trip, Java answering | 9.7–10.0 | 11.6–11.7 | 14.4–15.3 | 23.6–24.8 | 75.2–83.6 |
| 10k/s | apply, Rust | 1.1–1.3 | 2.1–2.4 | 3.9–4.2 | 4.6–6.1 | 45.8–119 |
| | apply, Java | 1.1–1.2 | 2.1–2.3 | 2.6–3.6 | 4.7–6.8 | 45.8–117 |
| | round trip, Rust answering | 9.4–9.5 | 11.7–13.2 | 13.8–15.1 | 17.5–18.5 | 69.3–127 |
| | round trip, Java answering | 9.3 | 11.2–11.4 | 13.5–14.0 | 18.3–22.5 | 122–151 |
| 100k/s | apply, Rust | 1.0–1.2 | 2.0–2.3 | 3.7–4.0 | 4.7–5.8 | 118–180 |
| | apply, Java | 1.0–1.1 | 2.0–2.3 | 2.5–4.0 | 5.0–6.5 | 147–236 |
| | round trip, Rust answering | 9.2–9.3 | 11.3–11.7 | 13.0–13.6 | 16.5–17.8 | 165–234 |
| | round trip, Java answering | 9.2–9.3 | 11.5 | 13.3–13.7 | 17.1–18.7 | 274–312 |
| 200k/s | apply, Rust | 1.0–1.2 | 2.0–2.2 | 2.8–3.9 | 9.6–12.5 | 114–119 |
| | apply, Java | 1.0–1.2 | 2.0–2.2 | 3.0–4.1 | 11.6–16.2 | 150–211 |
| | round trip, Rust answering | 9.3–9.4 | 12.0–12.5 | 13.5–14.6 | 25.5–28.9 | 182–203 |
| | round trip, Java answering | 9.3–9.4 | 12.0–12.4 | 13.4–14.7 | 24.8–38.6 | 310–354 |

µs; the apply rows pool the runs with either service answering, the same entries for both.

What it says:
- **The Rust container is at parity with Java's.** It applies the log within 0.1 µs of Java at p50 and p99 at every rate, and answers within 0.1 µs at the median from 1k to 200k/s (9.2–10.0 µs either way). p99.9 is the same, except at 10k/s, where Rust's round-trip p99 and p99.9 came out up to 1.8 and 1.1 µs higher in one round of three. Both follow the log at 200k/s without falling behind.
- **Its worst cases are mostly shorter.** With Rust answering, the round trip's maximum is 22–23 µs against 75–84 at 1k/s, 165–234 against 274–312 at 100k/s, 182–203 against 310–354 at 200k/s; at 10k/s the two overlap.

How it got there, three runs of the same bench the same morning:

| Round trip p50 (µs) | 10k/s | 100k/s | 200k/s |
|---|---|---|---|
| Rust answering, first layout, answers queued | 9.8–10.2 | 9.8 | 9.8–9.9 |
| Rust answering, same layout, answers offered straight (§3) | 9.8–10.0 | 9.7–9.9 | 9.8–9.9 |
| Rust answering, the node on one core complex | 9.4–9.5 | 9.2–9.3 | 9.3–9.4 |
| Java answering, the three runs | 9.3–9.5 | 9.2–9.4 | 9.3–9.6 |

- **The first layout put the Rust service on the other core complex** from its media driver and consensus module, which the Java service shared: every message crossed between the two L3 caches on its way in and its answer on its way out. That was nearly all of the 0.4–0.5 µs Rust gave away at the median then.
- **Offering answers straight into the publication** rather than queueing them for the end of the duty cycle moved the median by at most 0.2 µs. That run also had stalls of 1.9 to 4.0 ms at 100k and 200k/s, mostly in both services' apply at once, so upstream of either; they did not come back in the next run.

**On Aeron 1.54.2** (2026-11-24): the same bench and layout on the same machine, Java 1.54.2's node, rusteron at the port to 0.3 (`bc5024f`), three rounds:

| Rate | Measure | p50 | p99 | p99.9 | p99.99 | max |
|---|---|---|---|---|---|---|
| 1k/s | apply, Rust | 1.2–1.3 | 2.5–2.7 | 5.0–5.9 | 5.9–18.3 | 8.7–50.9 |
| | apply, Java | 1.2–1.3 | 2.5–2.7 | 4.8–8.7 | 9.6–31.6 | 15.0–50.9 |
| | round trip, Rust answering | 9.7–9.8 | 11.9–12.0 | 14.7–15.8 | 22.5–28.5 | 72.9–133 |
| | round trip, Java answering | 9.8–10.0 | 11.7–14.5 | 16.3–17.0 | 44.8–51.9 | 80.0–124 |
| 10k/s | apply, Rust | 1.0–1.1 | 2.1–2.2 | 4.3–4.4 | 5.3–5.7 | 49.2–88.8 |
| | apply, Java | 1.0–1.1 | 2.1–2.2 | 2.7–3.8 | 4.9–6.5 | 66.4–88.8 |
| | round trip, Rust answering | 9.2–9.4 | 11.0–12.1 | 12.9–14.5 | 15.8–26.9 | 96.4–118 |
| | round trip, Java answering | 9.2–9.3 | 11.2–12.8 | 13.6–14.7 | 23.8–52.5 | 142–195 |
| 100k/s | apply, Rust | 1.0–1.2 | 2.0–2.2 | 4.1–4.3 | 7.6–14.4 | 140–240 |
| | apply, Java | 1.0–1.2 | 2.0–2.2 | 2.7–3.7 | 10.3–29.9 | 187–303 |
| | round trip, Rust answering | 9.1–9.3 | 11.5–11.7 | 13.7–14.0 | 33.8–53.0 | 285–293 |
| | round trip, Java answering | 9.1–9.2 | 11.3–11.8 | 13.4–14.3 | 34.1–54.5 | 271–289 |
| 200k/s | apply, Rust | 1.0–1.1 | 2.0–2.1 | 3.0–4.0 | 17.6–26.9 | 150–158 |
| | apply, Java | 1.0 | 2.0 | 3.1–4.1 | 24.8–35.6 | 155–217 |
| | round trip, Rust answering | 9.2 | 11.7–12.4 | 14.0–14.8 | 40.4–59.3 | 196–242 |
| | round trip, Java answering | 9.1–9.2 | 11.7–12.4 | 14.1–14.6 | 48.9–64.8 | 326–347 |

- **Still at parity.** The medians and p99 are 1.53.3's, and Rust's are within 0.1–0.2 µs of Java's at every rate.
- **The far tails moved for both.** At 100k and 200k/s, p99.99 came out at 34–65 µs, Rust's and Java's alike, where 1.53.3's runs had 17–39. Both services moved together, so the container is not the cause.
- **Rust's worst cases are shorter at 10k and 200k/s** (96–118 against 142–195 µs, 196–242 against 326–347). At 1k and 100k/s they now overlap Java's, where 1.53.3's runs had them shorter.
- **Rust's apply p99.9 is higher than Java's at 10k and 100k/s** (4.3–4.4 against 2.7–3.8 µs, 4.1–4.3 against 2.7–3.7), as it was at 10k/s on 1.53.3. At 1k and 200k/s the two overlap.

Not measured here: replication. On one node the commit is the local archive recording the entry; across machines and zones the network dominates, which is the consensus module's and the transport's to answer (M3), not the container's. Nor is the cost of isolating cores, setting the governor to `performance` or turning SMT off, which would lower every figure alike.

## 7. What the first runs found

- **rusteron's `get_counter_key` stops at the key's first zero byte** (`aeron_custom.rs`), so binary keys come back short. The snapshot's recording counter looked gone, the container acked the snapshot without a recording, and the consensus module, logging "service=0 failed to take snapshot", dropped the snapshot from its recording log. The container checks registration ids now; the fix belongs in rusteron (return the whole key, `AERON_COUNTER_MAX_KEY_LENGTH` bytes), to be proposed upstream.
- **Java's default app version is 0.0.1**, not 0: the markers differed by that field until the defaults agreed.
- **A container restarted within 10 s of a kill refuses its own live mark file**, as Java's archive refuses its own: the restart waits out the liveness timeout, Aeron's own rule. Since 2026-11-24, a lock beside each Rust mark file proves a killed writer gone, and nothing is waited for (`rusteron-cluster::markfile::owner`).
- **The consensus module's last termination notice can find no one.** `ConsensusModuleAgent.onClose` sends its services a termination position once more as it closes; when both have already acked theirs and exited, the offer fails with `NOT_CONNECTED` and it logs a warning. Timing, and the same with Java's own services; seen once in 162 runs, on Linux.
- In the harness: a node killed without its media driver keeps answering for its endpoints until the driver's client liveness timeout, so a client connecting just after a failover waited on the dead leader; a Java node whose launch failed could not exit, its shutdown barrier waiting to be closed.

## 8. Next steps

1. **An application's engine as a service**: its state machine behind `ClusteredService`, its snapshotter behind `channel_source`, `Apply::Retry` onto its ring, on the Java consensus module first: done, 2026-10-28.
2. **The archive's async proxy** in the shell, so a snapshot's connect, replay and record never hold the duty cycle.
3. **Three nodes across machines**, once the engine runs: the container's figures here are a node's, and replication is where a commit's time goes.
4. **M2, the archive server**, the cluster's subset: done, 2026-10-29; `rusteron-archive/docs/server.md`.
5. **M3, the consensus module**, among Java nodes, then alone; and the simulator: the cores in virtual time, Aeron's streams modelled at the positions they show: done, 2026-10-30; `consensus.md`.
6. **M4, the cluster client**, Java's `AeronCluster` in Rust, measured against Java's: done, 2026-10-31; `client.md`.
7. **M5, no JVM anywhere**: Java's `ClusterBackup` in Rust (`backup.md`), so that a cluster, its clients and a standby run on the fork's pieces alone, Java's tools the oracle: done, 2026-11-02.
8. **Upstream to rusteron**: the counter-key fix (gsrxyz/rusteron#67); whether gsrxyz takes Apache-2.0-only crates.

## 9. Run it

Building Aeron C needs CMake, and a JDK older than 27 for Aeron's Gradle (25 works; the Java nodes run on 22 or later). On Ubuntu, `apt install cmake libbsd-dev uuid-dev` besides a C compiler and libclang.

```bash
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 BUILD_JAVA_HOME=$JAVA_HOME
cargo test -p rusteron-cluster
cargo build -p rusteron-driver --bin media_driver --features static
cargo build -p rusteron-system-tests --features static
SCENARIO=failover rusteron-system-tests/scripts/differential.sh 3 1000
rusteron-system-tests/scripts/repeat.sh 20
cargo build --release -p rusteron-driver --bin media_driver --features static
cargo build --release -p rusteron-system-tests --features static
rusteron-system-tests/scripts/bench.sh "1000 10000 100000 200000"   # on an otherwise idle machine
```

`AERON_ALL_JAR` names the Java 1.54.2 jar (default: the local Maven repository); `KEEP=1` keeps a run's work directory, logs, traces and archives.

## 10. rusteron 0.3 and Aeron 1.54.2

On 2026-11-23 the cluster's crates moved onto upstream rusteron 0.3.2, its Aeron C at 1.54.2, and Java 1.54.2 became the oracle. The results in the sections above were measured on 1.53.3 unless they say otherwise.

**rusteron 0.3's API.** The archive server and the cluster shells follow 0.3's ownership and thread-safety redesign:

- Callbacks Aeron keeps (images available and gone, counters gone) take a `SharedHandler`, their state behind a mutex, and receive borrowed views.
- The counters reader is borrowed from the client where it is needed.
- A subscription a poll hands over is shared through an `Rc`, as the 0.2 handle was, and only its last owner closes it.
- A destination change's command holds its publication or subscription, so the shell drops it once its poll has finished. Forgetting it, as 0.2 needed, left the resource open after its close: a leader that stepped down kept publishing its log, so its service never learned it was a follower. Aeron's election tests found it.
- A counter's value is a `Result`.
- The cores, being sans-IO, did not change.

Aeron C 1.54's fail-fast client (every call refused once a fatal error has closed the client) needed nothing: each shell already ends once `aeron.is_closed()`, as Java's agents do.

**What Java 1.54 changed, followed.**

- The cluster's codecs are at schema 18, regenerated with sbe-tool 1.42.0. A cluster action and a snapshot marker carry the term's base log position, from the recording log or else the recovery plan, as Java's `termBaseLogPosition()` has it. The container copies the action's into its markers.
- A cluster action's log position allows for a term's padding: one that does not fit the rest of its term lands at the next term's start, as Java takes the position from its claim.
- The archive counts its recording limit from its subscriptions and recordings, and checks again as an image arrives.
- A service message or ack naming an unknown service is reported and dropped.
- A leader with no service snapshots as soon as the snapshot's action is committed.
- Every Java-written fixture was rewritten with Java 1.54.2.

**What it changed, not followed.**

- A stopped catch-up replay is closed, not revoked (aeron-io/aeron#2259; proposed for Java in #2260).
- Java 1.54's archive takes an empty segment read from a page-aligned offset for a torn fragment, and refuses to start over it (aeron-io/aeron#2261). This archive keeps finding no straddle, and its tests state the divergence.

**Checked on the move.**

- **Aeron 1.54.2's own tests**, with Rust modules on every node:
  - `ClusterTest` passes 75 of its 94 cases. The 19 others are the ones out of scope on 1.53.3 (`consensus.md` §5). The 75 include 1.54's `shouldCaptureTermBaseLogPositionInSnapshotMarker`, the harness reading the module's recording log from its file.
  - `RustClientTest` and `StandbySnapshotNoticeTest` pass every case but the five authentication ones, left out as before (`client.md` §5).
  - `ClusterBackupTest` on Rust's backup: 23 of 25, the two needing an authenticator (`backup.md` §5).
  - The C archive client's tests: 61 of 80 on the Rust archive, the other 19 using what it does not serve (the archive's design, §5).
- **Interop beside Java 1.54.2**: every scenario, twice in a row, on each of the six setups (Java's module and archive, the Rust archive, the Rust module, both, the Rust client, Rust modules on two nodes of three).
- **The fork's CI steps**: fmt, clippy with warnings denied, the doc tests, the release build and the workspace's 1,562 tests pass. The simulator runs 20,000 seeds with faults and 5,000 with held links, none failing.
- **Found on the way**, besides the destination command above: a harness race. JavaNode closed its archive while the Rust module beside it could still be terminating, and that module then ended as failed, its archive gone. It failed 1 run in 10 of the mixed quorum-loss scenario, on 1.53.3 as on 1.54.2. JavaNode now waits for the module first, as a Java node closes its module before its archive: 20 runs out of 20 since.

**Jepsen on 1.54.2** (2026-11-24, `rusteron-system-tests/jepsen`, its Aeron now set by `AERON_VERSION`):
- Rust members are valid in all six runs: random faults and the two staged election scenarios, twice each.
- Java 1.54.2 members are valid under random faults, and fail both staged scenarios as 1.53.3 did. In one, two members cut their logs below their commit position; in the other, no transaction succeeds after the heal.
- aeron-io/aeron#2212 fixes these in Java, but is in no release yet.

The fork's GitHub workflow (Valgrind, the sanitizers, macOS) passes on the port.

§6's measurements were rerun on 1.54.2 (2026-11-24): still at parity. Not rerun: those of `consensus.md`, `client.md` and the archive's design. They went through an application's benchmark, each comparing one of its Java parts with the Rust one, and that application no longer has the Java parts.
