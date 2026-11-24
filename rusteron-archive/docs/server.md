# Aeron Archive in Rust: the server, the cluster's part

Aeron Archive's server re-implemented in Rust, wire- and on-disk-compatible with Java Aeron 1.54.2 (1.53.3 until 2026-11-23, which the dated results below were measured on), as a sans-IO state machine driven by a shell on Aeron's C client: M2 of the plan in `rusteron-cluster/docs/design.md`, which this document follows.

| | |
|---|---|
| **Status** | M2 done, 2026-10-29. Java 1.53.3 clusters run on it: rusteron's six interop scenarios with the Rust archive under every node (started, snapshotted, restarted from disk, three nodes through a failover), and an application's parity check (a Rust and a Java engine on one log, event logs, snapshots, a leader killed and back). Aeron's own C archive-client tests pass 58 of 74 against it, the other 16 using what it does not serve. Java's `ArchiveTool verify` passes on what it writes; it reads Java's catalogs and Java reads its. Measured under an application's three-node cluster on 2026-10-29: at parity with Java's archive in every configuration, 24 µs at the median at one command in flight, the same with either (§6). |
| **Code** | `rusteron-archive`, its modules named after Aeron's classes: sans-IO, `archive_conductor`, `catalog`, `segments`, `replay_session`, `channel_uri`, `archive_counters`, `control_session_adapter`, `archive_tool`, `client` and a `testkit`; with the `server` feature, `archive`, `recording_writer`, `replay_session::io`, `archive_mark_file`, `archive_counters::io` and the `aeron_archive` binary; the SBE codecs in `codecs`, generated, checked against Java's bytes. The scenarios: `rusteron-system-tests/archive` (`interop.sh`, `aeron-tests/`). |
| **Licence** | The server's files are Apache-2.0 only, as Aeron is (`NOTICE`). |

---

## 1. Why

Aeron Cluster needs an archive beside each node: the consensus module records the log into it, replays it on recovery, takes and loads snapshots through it, and catches up a lagging follower by replicating the leader's recording into it. Aeron C has the archive's client only; its server, about 20,000 lines, is Java's. With M1's service container in Rust, the archive and the consensus module were the two Java pieces left in a node. The archive comes first because a Java consensus module can test it: it is a separate process speaking a documented protocol, and Java's archive is there to compare with at every step.

It keeps the plan's three decisions: sans-IO (the conductor takes events and time and returns actions), wire and on-disk compatibility with 1.54.2 (a Rust archive replaces Java's under a Java consensus module, Java's tools read its files, either side replicates from the other), and Java as the oracle.

## 2. The contract

What a cluster asks of its archive, read from the 1.53.3 sources, and what this one serves of it:

| Part | What |
|---|---|
| Control | The control protocol (schema 101, version 14) on a UDP control channel and a local IPC one: connect (with a response channel and stream; credentials accepted and not checked, as Java's default authenticator does), archive id, keep-alive, close; start recording (v1, v2), extend (v1, v2), stop by channel, by subscription id, by recording id; list one recording, by URI fragment and stream, all of them; the start, stop and recorded positions and the max recorded position; find the last matching recording; replay, bounded replay, stop one, stop all; truncate, purge; replicate (v1, v2), stop replication. Answered as Java answers: the connect resent until the session's first request, pings, the connect and response timeouts, Java's error codes and texts. The 10 other requests (auth challenge, replay tokens, segment detach/attach/delete/migrate/purge, channel update, tagged replicate, list recording subscriptions) are answered with an error. |
| Signals | START, EXTEND, STOP, DELETE, REPLICATE, SYNC and REPLICATE_END, to the session that asked, in Java's order: a replication's REPLICATE_END before its recording's STOP, as the consensus module waits for both. |
| Catalog | `archive.catalog` as Java writes it: a 32-byte header (version 3.1.0, entries aligned to 64), each entry a 32-byte header and the `RecordingDescriptor` with its three strings, padded; grown by half again when full; a deleted entry kept, out of the index; written back range by range, synced as `catalogFileSyncLevel` says. |
| Segments | `<recording>-<base>.rec`, bases counted from the start position's term base; a padding frame written as its header alone, the rest a hole; a full segment ends at its length and the next file is created; synced after each write as `fileSyncLevel` says (1: data, 2: data and metadata). After a crash, a recording left without a stop position gets it back from its last segment, as Java's `Catalog` does it, a last frame straddling a half-written page refused. With `--truncate-torn-fragments`, such a frame is cut off instead, as Java's `ArchiveTool verify` cuts it when told to: the file keeps its length, the frame and what follows zeros, the recording stopped before it. A node with no JVM has no `ArchiveTool` to run before its archive starts. With `--verify`, every recording is checked first as the tool checks it without options, as a Java node has it done before its archive starts: its positions against the catalog's invariants and its last segment file, and that file's frame headers from where the recording starts. A damaged recording is marked invalid, and the archive lists it no more; a whole one's stop position is set where its frames end; an invalid one found whole is valid again. Two departures, where Java's archive itself reads the file otherwise: a last segment file left unsized by a crash (empty) holds frames to its end, where the tool fails to read a header in it and marks the whole recording invalid; and a frame of any length is read, where the tool throws on one longer than its 1 MiB buffer and verifies nothing more. The recovery runs on a thread of its own while the archive's Aeron client is kept alive: reading and syncing a large segment on a busy disk can outlast the client's 10 s, which then closes, as it did once in fault-injection runs. |
| Counters | The recording position (type 100) with Java's key and label (`rec-pos: <recording> <session> <stream> <channel>`), as the consensus module finds it; it moves only after the write and its sync. The archive's own (types 101–113): errors, control sessions, cycle time, recorder and replayer bytes and times, session counts. |
| Mark file | `archive-mark.dat` (schema 100, version 2): the header in the first 8 KiB, Agrona's distinct error log after it, the activity timestamp every second, the version written last. A live one is not started over: one whose writer holds `archive-mark.lck` beside it, or, with no lock naming its writer (Java's archive takes none), one active within 10 s (`rusteron_archive::mark_file::owner`). An archive killed is proven gone by its released lock, and is replaced at once. |
| Replication | As a client of the source archive: connect, archive id, the descriptor, a new copy (or an empty one replaced, or an existing one extended), the source asked to replay on the replication channel with the copy's session id, recorded until the source's stop (SYNC) or the position asked, then REPLICATE_END and STOP. Credentials passed on. |

Out: an authenticator and its challenge, response-channel control mode, checksums, recording events (templates 101–103, off everywhere), DEDICATED threading, catalog migration from older versions, live merge.

## 3. The design

```text
                 archive (shell, one thread)                 archive_conductor (sans-IO)
 C media driver ◄─ Aeron C client (conductor invoked on this thread)
   control   ─────► poll, reassembled ─────────────────────► on_control_message ─► control sessions
   responses ◄───── offer ◄─────────────────────────────── do_work(ResponseChannels), queued under back pressure
   images    ─────► Recorder: block_poll ► pwrite ► fsync ─► on_recorded (counter moved after the write)
   replays   ◄───── Replayer: pread ► offer_block, padding ◄ ReplaySession (where to read, what to offer)
   source    ◄────► SourceLink: a client of another archive ► Replication (connect, descriptor, replay, end)
   catalog   ◄───── pwrite of the dirty ranges ◄─────────── Catalog (an image of the file)
   files     ◄───── list, cut, delete, one a cycle ◄─────── deletes (truncate, purge)
   actions   ◄───── execute_actions ◄─────────────────────── poll_action (add publication, start recording, …)
```

**The core** (`archive_conductor::Conductor`) is Java's `ArchiveConductor` in SHARED mode: the control sessions and their queues, every request's checks in Java's order, the catalog, the recordings' lifecycle (the duplicate key, auto-stop, the extension checked once its image is acknowledged), the replays' admission and ids (`replay id << 32 | session id`), deletes deferred until a truncated recording's replays stop, and each replication's state machine (link, connect, descriptor, extend or create, replay, await the image, record, done). It reads no clock: time enters through `on_tick`. It does no I/O: it asks for it through `Action`s (add a publication or a subscription, start a recording on an image, start a replay, list a recording's files, cut a segment, delete a file, connect to a source archive) and learns the outcome through `on_io`. The catalog is an image of the file in memory; the core records which ranges a change dirtied, and the shell writes them back.

**The replay** (`replay_session::ReplaySession`) is Java's `ReplaySession` without its I/O: the segment file and offset to read next, the frame at the start position checked, reads that never cross a term, a read's data frames stamped with the replay's session and stream and offered as one block, padding appended after them, a limit counter followed while the recording grows. The shell's `Replayer` does the reads (`pread`) and the offers (`offer_block`, `append_padding`).

**The shell** (`archive::Archive`) runs one thread, the Aeron client's conductor invoked on it every millisecond as M1's container does. Each duty cycle: the clock read once; every millisecond the client's conductor, the mark file and the gauges; image events; the control subscriptions; the source archives' answers; the core's sessions; its actions; the replayers; the recorders; the catalog's dirty ranges. The recorder takes one block an image offers (up to `fileIoMaxLength`), writes it where the segment arithmetic places it, syncs it when asked, and only then moves the position counter: the durability boundary a synced cluster's commit rests on.

## 4. Choices, and why

| Choice | Why |
|---|---|
| SHARED mode only | What Aeron Cluster nodes run (`ArchiveThreadingMode.SHARED`); one thread, the C client's conductor on it, no locks. DEDICATED's recorder and replayer threads are a later step if measurements ask. |
| The catalog as an in-memory image, written back by range | Java maps the file; positional writes of the dirty ranges need no `unsafe` mapping, and the core stays free of I/O. The bytes are Java's: checked against Java's own `Catalog` byte for byte. |
| Positional reads and writes, no mapping of segments | Java's recorder and replayer use `FileChannel` positional I/O too; the same system calls, the same page cache. |
| Responses queued, as many sent a cycle as the publication takes | Java sends one queued response a cycle; draining is faster under back pressure and changes no order. |
| A listing waits for what is queued before it | Java's descriptors can overtake a queued response; here a session's answers keep their order. |
| A frame of an unknown type, or a block the publication refuses, ends the replay with an error | Java loops on them. |
| A replay-all-and-stop of an empty recording is refused | Java answers with an error and starts it anyway. |
| A listing of a negative count ends at once | Java's never ends. |
| Requests out of scope answered with an error | Not dropped: a client learns at once, rather than waiting on its response timeout. |
| The replication's source treated as connected once its publication is | Waiting for the response subscription too deadlocked: the source answers on it only once it has a request. |
| A replay its client stops on a channel with `eos=false` (a cluster's catch-up, a replay merge's, a replication's) is closed, not revoked; other replays a client stops, and those whose recording goes (truncation, purge), still are | Java's 1.54 archive revokes them all, and a revoked image is dropped at once wherever its readers are, the end of stream signalled whatever `eos` says. A cluster leader whose busy archive stops a follower's catch-up replay late, the follower's next catch-up reading on from it, has the follower lose what its service had not read: the service refuses the next log for good, every process running (seen on Java's members under fault injection; reproduced in Aeron's own tests and in `rusteron-system-tests`'s `stale_catchup_replay`; aeron-io/aeron#2259). Closed, the image is left to its readers, as `eos=false` means. Other stopped replays are revoked so that a client needn't read the rest, as Java's archive means them to be (`BasicArchiveTest`'s stop-replay tests). The same rule is proposed for Java's archive in aeron-io/aeron#2260. |

## 5. Tests

**Codecs** (`rusteron-archive/tests/codecs_golden.rs`, 11 tests): every request the archive reads, written by Java's own codecs, decodes to Java's values; every message it writes (control response, recording signal, ping, recording descriptor, the catalog's header and entry, the mark file's header) encodes to Java's bytes.

**The core** (`rusteron-archive`, 64 tests). Against Java's own code, run by `fixtures.sh`: the catalog's operations replayed write Java's file byte for byte, grown alike; `replaceRecording` for a longer and a shorter entry; the stop positions recovered from eight sets of segment files, page straddles included, and the files and stop positions Java leaves when told to truncate them; `ArchiveTool verify`'s verdicts on 18 recordings (whole, active, stopped short or long, damaged in each of the eight ways the tool reports, invalid, deleted, torn, with an empty last segment): the same damage in the tool's words, the same stop positions, and an archive started with `--verify` leaving the catalog as the tool leaves it, the torn segment truncated alike, but for the empty segment, valid where the tool marks it invalid; Java's channel handling for 33 channels (the stripped channel, the duplicate key, the response channel, the replay channel) and its counters' keys and labels. On the testkit (a fake Aeron: control images, publications as buffers, a fake source archive answering from its own catalog): 30 conductor tests following Java's behaviour step by step (connect and timeouts, every request's checks and errors, signals and their order, auto-stop, extension checks, truncation and purge with deferred deletes, four replication paths), 6 replay-session tests.

**Against Java's client** (`interop.sh`): Java's `AeronArchive` drives the Rust archive: start, list, a duplicate refused, stop, extend over 16 terms, stop by identity, an unknown recording; replays whole, from a position, bounded by a counter, and following a live recording, the messages and their checksums checked; truncate and purge; the archive killed (-9) mid-recording and restarted, the stop position recovered, the recording replayed whole; replication of a stopped recording and of one still recording, from a Java and from a Rust source, the copies replayed and compared; Java's `ArchiveTool describe` and `verify` after each stop. `interop.sh java` runs the same against Java's archive, which then replicates from a Rust one.

**Aeron's own tests** (`aeron-tests/run.sh`): Aeron 1.54.2's C archive-client tests (`aeron_archive_test`) launch Java's `ArchivingMediaDriver` through `java`; a stand-in (`java.in`) starts Aeron's C media driver and `aeron_archive` instead, the `-D` properties mapped. 61 of the 80 cases pass (58 of 74 on 1.53.3); the 19 others use an authenticator rejecting null credentials (2, whose test binary crashes when the connect succeeds), response channels and replay tokens (12), segment detach, attach and purge (3), channel update (1), listing recording subscriptions (1).

**Clusters on it.** rusteron's interop scenarios (`rusteron-system-tests/scripts/differential.sh`, `ARCHIVE=rust`): Java 1.53.3 consensus modules started without their archive, `aeron_archive` beside each node on its driver; the Rust and the Java service on one log, traces and snapshots compared as in M1. Each run then stops the archives with SIGTERM, checks they logged no warning, and runs Java's `ArchiveTool verify -a` on every node's archive (every segment file). `ARCHIVE=rust repeat.sh 10` passed all six scenarios ten times in a row, 60 runs in 11 minutes, on Linux on 2026-10-29, and once more with the verification added. An application's parity check: its Rust and Java engines on one log, each node's two archives in Rust (the cluster's, and the event log's), the workload, snapshots compared, the leader's node killed (-9) with its archives, a new leader, the node restarted and caught up, compared again.

## 6. Measurements

On a Linux workstation, 2026-10-29, through an application's benchmark: a three-node Java 1.53.3 cluster on one machine (AMD Ryzen 9 7950X, one drive), the application's Rust engine as each node's service, every agent busy-spinning, the drivers' agents on threads of their own (DEDICATED), each node on four cores and their SMT siblings, the client on four more. Each node's archive is Java's, a SHARED-mode thread in the node's JVM, or the Rust one, a process of its own on the same cores. The same workload throughout; the two archives interleaved over three rounds, each figure a range over them. Commands a second and median latency, from the client's offer to its answer:

| In flight | Log | Java's archive | Rust archive |
|---|---|---|---|
| 1 | memory | 38,400–40,400 · 23.8–24.4 µs | 37,000–39,900 · 23.4–24.7 µs |
| 1 | synced | 1,670–1,710 · 517–519 µs | 1,700–1,750 · 518 µs |
| 64 | memory | 309,000–391,000 · 161–205 µs | 313,000–387,000 · 163–203 µs |
| 64 | synced | 32,000–32,600 · 1.76–1.91 ms | 31,100–33,100 · 1.69–1.93 ms |
| 1,024 | memory | 311,000–394,000 · 2.6–3.3 ms | 314,000–388,000 · 2.6–3.3 ms |
| 1,024 | synced | 311,000–315,000 · 3.2 ms | 301,000–315,000 · 3.2 ms |

At one in flight in memory, ten rounds more (`CONFIGS="1 no 20000 20000"`), the median of the rounds and their range:

| | Java's archive | Rust archive |
|---|---|---|
| commands a second | 39,600 (36,400–40,900) | 38,900 (37,700–41,300) |
| p50 | 23.7 (23.1–26.9) | 24.1 (23.0–24.9) |
| p90 | 31.9 (27.9–33.4) | 32.1 (27.9–33.2) |
| p99 | 40.0 (37.2–41.8) | 39.7 (37.2–41.6) |
| p99.9 | 60.1 (57.1–67.8) | 61.0 (58.0–62.9) |
| max | 94 (75–123) | 104 (77–125) |

µs. What it says:
- **The Rust archive is at parity with Java's** in every configuration: the same medians, the same two levels from run to run under load in memory (about 310,000 and 390,000 commands a second, a limit set by the leader's sender), the same synced figures, where the drive's sync sets the pace. The figures are in line with those measured on Java's archive the day before.
- **Three rounds were not enough at one in flight**: the Rust archive's p90 came out 1–3 µs higher in two of them. Ten rounds more put the two on top of each other, each p90 moving between about 28 and 32 µs from one run to the next.
- **Its worst cases are not shorter**, as the container's were. Single rounds stall on either side: in memory, once the Rust archive's maximum at 64 in flight (1.9 ms) and once its p99 at 1,024 (5.8 ms); synced, both archives' p99.9 range from 4 to 11 ms between rounds.

Not measured: replay and replication speed (a follower catching up, a snapshot loaded), which the runs above barely use; the archive across machines, where the network sits between the leader and the followers' archives; the Rust archive's own cost per write, against Java's, outside a cluster.

## 7. What the runs found

- **A replay that started and finished in one duty cycle never told its client OK** (a short recording, its subscriber already there): Java tells it as the replay starts. Aeron's C tests found it; the replayer now reports the start with the end.
- **Java's client sends `extendRecording` as version 1** (template 11), not v2: both are served.
- **The C tests use three requests the plan had left out**: listing every recording, the max recorded position, and finding the last matching recording. Served as Java serves them.
- **A replication waiting for its source's response subscription deadlocked**: the source answers only once it has a request. The link is ready once its publication is connected.
- **SIGTERM must stop the archive cleanly**: harnesses stop nodes with it; the binary stops its recordings where they are, writes the catalog and leaves the mark file.
- In the harnesses: a driver killed leaves its `cnc.dat`, so a node's restarted archive must wait for the new driver's, not attach to the dead one's; Aeron's C tests launch the archive through `java` with properties the C media driver rejects (Java's termination validator class), which the stand-in maps.

## 8. Next steps

1. **M3, the consensus module**, among Java nodes, then alone, on this archive: a node with no Java at all.
2. **The out-of-scope requests**, if a user needs them: response-channel control mode and replay tokens are the largest (9 of the 16 C tests).
3. **DEDICATED threading**, if the recorder's writes ever hold the control path: the measurements say whether they do.
4. **Upstream to rusteron**, with the cluster's crates.

## 9. Run it

On Linux with Aeron C's build dependencies (the container's §9):

```bash
cargo test -p rusteron-archive --features server
```

```bash
cargo build --release -p rusteron-archive --features static,server
```

```bash
rusteron-system-tests/archive/interop.sh
```

```bash
rusteron-system-tests/archive/aeron-tests/run.sh
```

```bash
ARCHIVE=rust PORT_BASE=23000 rusteron-system-tests/scripts/repeat.sh 10
```

`aeron_archive`'s flags are Java's `Archive.Context` options by name (`--archive-dir`, `--archive-id`, `--control-channel`, `--file-sync-level`, `--segment-file-length`, `--spin`, …; `src/bin/aeron_archive.rs`); it prints `READY` once serving and stops on SIGTERM or SIGINT. `ARCHIVE_DEBUG=1` logs the conductor's actions.
