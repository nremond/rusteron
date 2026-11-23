# Aeron Cluster in Rust: the cluster backup

Part of the fifth milestone of Aeron Cluster in Rust (see `design.md` for the service container, `consensus.md` for the consensus module and `client.md` for the client): Java Aeron 1.53.3's `ClusterBackup`, which copies a cluster's snapshots and live log into an archive of its own, as a sans-IO state machine driven by a shell on Aeron's C client.

| | |
|---|---|
| **Status** | M5, 2026-11-01. A Rust `ClusterBackup`: the query, the snapshots copied, the live log recorded from a member's bounded replay, the recording log kept, a stall or an error reset, a restart extending what it recorded. Beside Java's backup on one cluster, Java's modules or Rust's, it holds the leader's log byte for byte with the same recording log, through a failover too; Aeron's own `ClusterBackupTest` passes as Java's backup does (§5). The simulator runs it among its faults over 10,500 seeds. An application's Rust standby embeds it. |
| **Code** | `rusteron-consensus-core::backup` (the backup: `ClusterBackupAgent`, its `Ports`, `Action`s and `Event`s); `rusteron-cluster::backup` (the shell: `ClusterBackupNode`, `spawn`, `BackupListener`; binary `cluster_backup`); `rusteron-cluster-core` (its counters and mark file header); `rusteron-cluster-sim` (`--backup`); `rusteron-cluster-interop` (`scripts/backup.sh`, `java/JavaBackup.java`, `java/BackupCheck.java`; `RustBackup` in `aeron-tests`). |
| **Licence** | Apache-2.0 only, as Aeron: derived from its schemas and behaviour. |

---

## 1. Why

A standby can be the last JVM in a cluster: one process holding Java's driver and archives, Aeron's `ClusterBackup` to copy the log, a module feeding the log to a service, and the application's engine. Every other piece of it had a Rust twin by M4; `ClusterBackup` has no C or C++ version in Aeron 1.53.3.

## 2. The contract

Java 1.53.3's `ClusterBackupAgent` with its `SnapshotReplication`, `MultipleRecordingReplication`, `RecordingReplication`, `PublicationGroup` and `LogSourceValidator`, read from the source, kept as Java does it:

- **The states**: BACKUP_QUERY, SNAPSHOT_RETRIEVE, LIVE_LOG_RECORD, LIVE_LOG_REPLAY, UPDATE_RECORDING_LOG, BACKING_UP, RESET_BACKUP and CLOSED, Java's transitions between them, the state counter set on each.
- **The query**: the members' consensus endpoints shuffled once and tried in turn, the next one when a query goes unanswered for the response timeout (5 s). A response from an unacceptable source (`ANY`, `LEADER`, `FOLLOWER`) or a redirect excludes that member; a member with no id, or not the member the log already comes from, resets the backup. A challenge is answered through a `CredentialsSupplier`; ERROR and AUTHENTICATION_REJECTED are errors.
- **The snapshots** the response names and the local recording log does not hold, copied one at a time from the member's archive into the local one, each again until its copy ends synced, a copy without progress for the replication timeout an error.
- **The live log**: recorded, or the last term's recording extended, from the catch-up endpoint (a wildcard port bound first by a tagged subscription), as the member's archive replays it bounded by the member's commit position counter: from where the local recording stopped, or for a first copy from the log's start or the latest consensus module snapshot (`ReplayStart`).
- **The recording log** as Java files it: the log's term before the snapshots when snapshots were copied, the snapshots, then the member's last term if unknown, with its local recording and a NULL timestamp, synced.
- **Backing up** until the next query (the interval, 1 h), the live log position counter following the recording's; a stall without a live log counter (the progress timeout, 10 s) or an error resets the backup, which cools down (30 s) and queries again.
- **The files and counters**: `recording.log`, a BACKUP mark file with its link when its directory is not the cluster's, the counters 208–211 and 240 as Java allocates them. ClusterTool reads the mark file and sets the next query's deadline, which the backup reads back each cycle.
- **The errors**: in the mark file's error log and the errors counter, WARN for what Java records as a `ClusterEvent`. A terminating error (the local archive gone or full, the recording log unwritable, the Aeron client closed) ends the backup, as Java's `AgentTerminationException`.

Java 1.53.3's open-source backup sends members no heartbeat request: its liveness is its mark file's activity, refreshed every second, and so is this one's.

Out of scope: Java's agent runner and invoker settings and thread factory (the shell has its own, §3), `ClusterLog`'s event logging (the shell logs state changes), the extended termination hook's exception object (`on_terminated` takes the reason).

## 3. The design

**The core** (`rusteron-consensus-core::backup`, `forbid(unsafe_code)`): `ClusterBackupAgent` is Java's agent as one state machine, epoch milliseconds entering once per duty cycle and a seeded generator for the shuffle and the recording's session id. Its two archives are `rusteron-archive-core::session::ClientSession`s, the consensus module's archive client. Events go in (`on_consensus_message`, `on_archive_message`, `on_io`); offers, counters and the counter lookups Java does at once go through `Ports`; publications, subscriptions, archive links and recording-log writes are `Action`s; the listener's calls are `Event`s.

**The shell** (`rusteron-cluster::backup`, binary `cluster_backup`) runs it on one thread with the C client's conductor invoked there every millisecond, as the consensus module's shell does, with Java's `ClusterBackup.Context` options. A member's archive with a response endpoint is reached on a response channel: the subscription first, the publication then naming it, as Java's `AsyncConnect` does. To embed it, `spawn(ctx, listener)` runs a backup on a thread of its own with an Aeron client of its own, and returns once it has started; `ClusterBackupNode::launch` and `run` run one on the caller's thread. `BackupListener` is Java's `ClusterBackupEventsListener`, called on the backup's thread, plus `on_terminated`.

**The simulator** (`rusteron-cluster-sim`, `--backup`) runs a backup beside the members on an archive of its own: it queries them on the consensus stream, its cluster archive session is a control session of the member's archive, and the member's replays reach its catch-up endpoint at the network's level, as the log reaches followers. Its source type comes from the seed; its process crashes and restarts on its files, and its links to members are cut, among the members' faults.

## 4. Choices, and why

- **Java's blocking calls are waits.** Java blocks on its local archive for a recording started or extended, for the local recording's stop position, and for a replication accepted. Each is a request here, its state waiting for the answer. The stop position is read after the extension is asked for and before the replay is: an archive keeps a recording's stop position until the extension's image joins, so the replay starts where the recording stopped, as Java's does.
- **Requests made in a duty cycle are sent at its end**, as Java's calls send theirs, rather than at the next cycle's start.
- **Java's quirks are kept.** The publication group excludes one member at a time, until it excludes another: with source `LEADER`, a leader excluded while it was a follower is asked again only once a follower answers. A closed publication stays the current one until a query times out. The live log position is not updated while the backup queries again, the recording going on meanwhile. A stall counts only before the live log's counter is found. With a single endpoint, its exclusion is ignored, where Java's group would spin.
- **The member archive's errors are taken as they come.** Java polls for them in its slow tick when no request of its own is awaited, and for the replay's answer in LIVE_LOG_REPLAY; the backup routes each response as it arrives by the same rules. Java's archive client also reports "not connected" when its subscription loses the member; rusteron's session does not, so through a failover Java's backup logs two warnings this one does not (§5).
- **A copy's counter is looked up by its recording** when the archive signalled EXTEND before the driver registered the counter, as the consensus module's standby copies do. Java looks only on EXTEND, and would see no progress until the copy ends.
- **Two options that are not Java's**, both off by default, for a standby that must keep up. Under faults at 64 commands in flight, Java's backup and this one fell 14–28 s behind the members each time their source was killed, and Java's once stood 137 s with no error:
  - `live_log_progress_timeout_ns`: the live log standing still that long while backing up resets the backup. Java's resets when its live log recording ends, never when one left open stops growing, and waits through a dead source's replay until the driver times its image out (10 s). Only for a cluster whose log moves on its own, by timers: an idle one would reset each time.
  - `retrieve_snapshots`: false copies no member snapshot. Java's copies the newest after each reset, before it follows the live log again; a standby that takes its own never loads them. With `ReplayStart::LatestSnapshot` and no term filed, the log is then replayed from its start.
- **The binary's credentials are fixed**: the credentials and an answer to any challenge. Aeron's test suppliers answer the same whatever the challenge; the core takes any `CredentialsSupplier`.

## 5. Tests

- **Unit tests** (`rusteron-consensus-core/tests/backup.rs`): 41 scenarios against fake archives and a fake member, with a virtual clock. Java's `ClusterBackupAgentTest` ported (the replay's start three ways, the termination on a closed client), then each transition and each reset: a query unanswered, a source excluded, a redirect, an authentication rejected, a challenge answered, a copy without progress or not synced, a wildcard catch-up endpoint, the replay refused, the live log's counter gone, a recording refused or the archive full, the local archive lost, a restart extending the recording from its stop position, a query again with the recording kept, close. Then the two options: a still live log resetting the backup, one moving on not, a query leaving its clock running, and a response's snapshots not copied. `consensus::backup_query` now carries credentials, checked against Java's bytes.
- **The simulator**: 10,000 seeds of 20 s of faults and 500 of 60 s pass with the backup. Checked: its live log position never goes back, and at the end it holds the log to the commit position, each entry the one members' services applied there. Members answer a backup only outside elections, and the exclusion above can leave the leader out: two seeds (2698, 3951) ended with the backup still querying followers that had been in elections until the last seconds, the leader excluded. The backup now has up to 20 s more once a run is over and the cluster quiet; it needed 84 ms at most. The archive model gained what a backup asks of it: bounded replays, Java's replay session ids (the replay's number, then its publication's session), a replay from NULL_POSITION, an extension keeping its stop position until it joins, SYNC at the end of a copy without a stop position. Runs without the backup are unchanged: 2,000 seeds give the same elections, crashes and partitions.
- **Interop** (`scripts/backup.sh`): three JavaNodes with Java's modules or Rust's (`CM=rust`); Java's `ClusterBackup` on a Java archive (`JavaBackup.java`) and `cluster_backup` on the Rust archive, both from the leader, started after a snapshot, between two batches of messages. `BackupCheck.java` replays the leader's log and both backups' to the backups' common position: one CRC-32C. The two recording logs must match entry for entry. `SCENARIO=failover` kills the leader's processes in between. Three rounds of both scenarios on both module kinds, 12 runs, all passed, the new term filed alike after each failover. Through a failover Java's backup logs two warnings (its cluster archive client not connected, its replay not stopped on a closed client), Rust's none.
- **Aeron's own tests** (`aeron-tests`): `RustBackup`, in the test harness, runs `cluster_backup` in `TestBackupNode` in place of Java's `ClusterBackup` (`run.sh RUST_BACKUP=1`), on the backup node's driver and Java archive, its counters and error log read as Java's. `ClusterBackupTest`, 25 cases:

  | | Java's modules | Rust's modules |
  |---|---|---|
  | Rust's backup | 25 | 23 |
  | Java's backup | 25 | 23 |

  The two failing on Rust's modules need a challenge-response authenticator, which the Rust module does not have (`consensus.md` §2).
- **ClusterTool** reads a Rust backup's mark file as a BACKUP component's, its activity fresh while it runs and cleared at its stop.
- **An application's standby** embeds it through `spawn` and `on_live_log_progress`, as the application's Java standby embeds Java's. Its twins of the Java standby tests pass on Java's members and on members with no JVM, with Java tools as the oracle: the log byte for byte, the state hashes, its module state against members' snapshots, a member restoring from its set. They found one fault, in the consensus module's snapshot loader, not the backup: a module snapshot's tracker session ids were read with SBE's null as 0, where Java reads `Long.MIN_VALUE` raw, so a standby restarted from its own snapshot wrote 0 where members write `Long.MIN_VALUE` (fixed).

## 6. Measurements

None of the backup's own: it records the log at the pace of the member archive's replay, bounded by the commit position. The standby's figures with it, and the fault-injection runs with no JVM, are the application's.

## 7. Run it

```bash
cargo test -p rusteron-consensus-core --test backup
cargo run -p rusteron-cluster-sim --release --bin cluster_sim -- --seeds 0..1000 --backup
cargo build --release -p rusteron-media-driver --bin media_driver --features static
cargo build --release -p rusteron-archive-server --features static
cargo build --release -p rusteron-cluster --features static --bin cluster_backup --bin consensus_module
SCENARIO=failover rusteron-cluster-interop/scripts/backup.sh
CM=rust SCENARIO=failover rusteron-cluster-interop/scripts/backup.sh
JAVA_CM=1 RUST_BACKUP=1 rusteron-cluster-interop/aeron-tests/run.sh io.aeron.cluster.ClusterBackupTest
RUST_BACKUP=1 rusteron-cluster-interop/aeron-tests/run.sh io.aeron.cluster.ClusterBackupTest
```
