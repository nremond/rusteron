# Aeron Cluster in Rust: the consensus module

The third milestone of Aeron Cluster in Rust (see `design.md` for the service container and `rusteron-archive-server/docs/design.md` for the archive): the consensus module, wire- and on-disk-compatible with Java Aeron 1.54.2 (1.53.3 until 2026-11-23), as a sans-IO state machine driven by a shell on Aeron's C client. Where Java's election can lose committed entries, the module follows Raft's rules instead.

| | |
|---|---|
| **Status** | M3, 2026-10-30. A node's consensus module in Rust, alone or among Java's: elections, replication, catch-up, sessions, timers, snapshots and termination. Every interop scenario passes on every mix of Rust and Java modules; Aeron's own `ClusterTest` passes every case in scope; the Jepsen suite's scenarios pass on Rust members; the deterministic simulator runs 480,000 seeds of faults without a failure, 140,000 of them with partitions that hold what they cut off; an application's engine commits on Rust modules at parity with Java's. Backups are answered and standbys' snapshots taken in and restarted from, as Java's open-source module does (§4): an application's standby runs beside Rust members through fault-injection runs, and spares them the snapshot pause as it does Java's (§6). |
| **Code** | `rusteron-consensus-core` (the module: `module`, `module/election.rs`, `module/peers.rs`, `module/standby.rs`, sessions, timers, pending service messages, the module's snapshot, a testkit); `rusteron-cluster-core` (recording log, node state, mark file, counters); `rusteron-cluster` (the shell, binary `consensus_module`); `rusteron-cluster-sim` (the simulator); `rusteron-cluster-interop` (differential scenarios, the Jepsen suite, Aeron's own tests). |
| **Licence** | Apache-2.0 only, as Aeron: derived from its schemas and behaviour. |

---

## 1. Why

The consensus module is the last Java piece of a node: with it in Rust, a node can run with no JVM. It is also where Aeron Cluster's guarantees live, and where a deterministic simulator of the Rust module could test them over many more fault schedules than real clusters can run.

## 2. The contract

- **The wire.** Every consensus, ingress, egress, service and archive message as Java 1.54.2 encodes it (cluster schema 18); a Rust module joins a cluster of Java modules, as leader or follower, and Java clients and services see no difference.
- **The disk.** `recording.log`, `node-state.dat`, `cluster-mark.dat` and the counters 200–241 as Java writes them: ClusterTool reads a Rust node's files. The mark file's error log gives each entry Java's category, WARN for what Java records as a `ClusterEvent`, since Aeron's tests and tools filter on it.
- **The behaviour.** Java's, step for step (the election's 18 states, recovery, catch-up, the commit rule, sessions, timers, snapshots, termination), except where it loses committed entries or leaves a member out for good (§4).

Out of scope: taking and sending standby snapshots (a standby's work: Aeron Premium's, or an application's own), recording-log validation (the node toggle's `VALIDATE_RECORDING_LOG`), consensus module extensions, authenticators other than the default (so no challenge), IPC ingress, the priority-heap timer service, dynamic membership. A multicast ingress is taken by the leader alone, as Java takes it.

## 3. The design

**The core** (`rusteron-consensus-core`, `forbid(unsafe_code)`) is Java's `ConsensusModuleAgent` and `Election` as one state machine. Time enters once per duty cycle; randomness comes from a seeded generator. Events go in (`on_consensus_fragment`, `on_ingress_fragment`, `on_log_fragment`, service and archive messages, `on_io` completions); the offers Java branches on at once go through a `Ports` trait; slower work leaves as `Action`s. Java's blocking waits are explicit states; its exceptions that restart an election are `Fault::Error`, or `Fault::Warning` where Java gives them the WARN category, its fatal ones `Fault::Terminate`.

**The shell** (`rusteron-cluster::consensus`, binary `consensus_module`) runs it on one thread with the C client's conductor invoked there, as the service container's shell does, with Java's `ConsensusModule.Context` flags.

**The archive client session** (`rusteron-archive-core::session`) is the module's link to its archive, in the shell, the testkit and the simulator alike.

## 4. Choices, and why

### Raft's rules where Java's election is laxer

The simulator, then Jepsen on real Java 1.53.3 and 1.54.0 members, found Java's election losing committed entries after partitions (aeron-io/aeron#2212 proposes a fix for its core). The module follows Raft's rules instead, with nothing changed on the wire:

- **Votes are kept.** A member that voted in a term follows no leader of an older one, and canvasses with the term it voted in.
- **A stale leader steps down.** A member leading a term steps down when it hears of a newer one, in its election or after, whatever its module's state. Java leaves a leader in LEADER_LOG_REPLICATION or LEADER_READY for good; #2212 too.
- **A refused leader is not a leader.** Its messages do not keep a member from standing.
- **Commit counts what is known.** A leader counts a member's position towards its quorum only as far as that member's log is known to be its own: what it reported in the leader's term, or of a term both hold entries of. Java counts a canvassing member's position whatever its log.
- **Log matching.** A leader names the latest term both logs hold entries of; a member cuts back the terms the leader never had, empty ones included. Java names the member's own term whatever its log holds.
- **Every entry filed under its term.** A member files a term before the term's entries can reach its log. Java files it once joined or once replicated, and an election starting over in between leaves entries no term holds.
- **Logs compared by their last entry.** A member canvasses and votes with the term of its log's last entry. Java's election takes the term of the last term event it replayed.
- **Committed entries stay.** No member truncates below its commit position.

### Where Java can leave a member out for good

The simulator also found members that never rejoin. Two of the faults Java has too, and its rarer elections seldom meet: the module reads its log with the term the log holds where it starts (Java reads it with one it stood for in an election that failed, skipping cluster actions its services apply), and starts over from follower replay without word from its leader (Java waits for good). A third is the module's own: its waits being states, an election can give up on a services' join that Java's blocking wait sees through, and the module drops that join's acks.

### Where Java keeps the others waiting

A member in follower replay applies its own log to its services before following its leader's, and Java's election lets it neither canvass nor vote until the replay ends, however long, whatever its leader does. If the leader is lost meanwhile, the others may have no majority without it: in an application's fault-injection runs a member restarted from a snapshot 2.5 to 2.9 million commands back, the leader then paused, and no leader was elected for 9 to more than 15 s, long enough for Aeron's client timeouts to close the paused member's module once continued. The module gives the replay up once its leader, the one whose term it replays for, has said nothing for the heartbeat timeout: no new term, no commit position. The member then canvasses and votes like any other, and what it replayed stays replayed.

That last part needed one more thing, which an election starting over mid-replay needed all along. The services read a replay after the module, up to the commit position it sets, and the replay's image goes when the replay is stopped: given up while they lag, it left them short of where the next replay begins, they refused to join that one ("Cluster log must be contiguous"), and the election waited on their ack for good (in one fault-injection run, a member 29.6 MB behind its module stayed in `FOLLOWER_REPLAY`). An election starting over now holds a replay the module was reading and asks the services to ack its position (Java's RequestServiceAck); the replay stops once they have. The module reads no further than the end of a message: the services' log position, Java's container's as Rust's, follows the messages they read, so one just past a term's padding frame (where a poll that only steps over the padding leaves the module) they never report reaching mid-log. A snapshot under way, whose acks are at that position too, completes first. In the simulator's staging (`rusteron-cluster-sim --leader-lost-during-replay`, its README) the leader is replaced after 2.1 to 2.3 s instead of 4.1 to 4.3 s.

### Where Java elects twice

A leader cut off from the others for a few seconds brings a second election once its links are back: in fault-injection runs across three zones, 20 of 23 partitions of the leader on this module, and Java's nodes do the same. Aeron's streams hold what is sent through a cut shorter than their timeouts, about 5 s, and resend it once the links are back. So the member cut off hears the others' finished election late: their canvass positions, and the vote request of the term they elected a leader in.
- Its log is ahead, with entries it appended but never committed, so it refuses the vote, and takes the term, as Java does.
- The stale positions make it a quorum candidate, and it stands for the term after.
- Its vote request sends the leader and its follower into an election ("unexpected vote request"). Aeron has no pre-vote.

Two rules stop it, with nothing changed on the wire:
- **A term taken by a refusal waits a canvass round.** A member in Canvass or Nominate that takes a newer term from a vote it refuses sends its canvass position at once, and stands no sooner than one status interval later (100 ms). A leader of that term, if there is one, answers a canvass with its new term within a round trip.
- **A member about to stand follows a leader of its term.** In Nominate, as in Canvass, a member follows a leader of the term it would stand for, or of a newer one. Java follows one only in Canvass.

Refusing without taking the term would not do. In an election with no leader, the member with the longest log refuses every other candidate, and one refusal sinks a ballot, so it must take their terms to get ahead of them. In the simulator, a held cut of 1.5 s then left the cluster without a leader for more than 10 s.

The simulator now holds what a cut carries, as Aeron's streams do (`--held`, its README). Staged, the leader cut off for 2.0, 3.6 or 4.9 s on 100 seeds each, with held and with lost links: the others elected another leader 595 times in 600, and the new leader kept its term after the heal every time, where it lost it 250 times before. A cut too short to replace the leader elects about as fast as before: a median of 198 ms after the heal, against 165 ms. Across zones, the same nine fault-injection runs brought no second election in 22 partitions of the leader, and the members logged no unexpected vote request (41 before). New leaders came 1.5 to 1.9 s after each fault on the leader, as before. With held partitions among the random faults, seeds 0..100,000, and 40,000 with faults for 60 s, keep every invariant, once a member drops an empty term a leader never had (seed 72608, the log matching rule above).

Reviewing the rules found a fault of the module's own: a vote went out before the term it was cast in reached the node state on disk, so a crash in between could let a member vote twice in one term. The node state's write is now a `Ports` call, made before the vote goes out, as Java writes first.

### Backups and standbys

A member answers what Java's `ClusterBackup` and a standby send it on the consensus stream as Java's open-source module does:

- **Backup sessions.** Each request is a session of id -1 back to its response channel, taken only outside an election in ACTIVE, SUSPENDED or SNAPSHOT, and answered once that channel connects, by a leader or a follower in ACTIVE.
  - A backup's query is told the log's recording and last term, the commit position's counter id (a backup bounds its replay of the member's archive by it), the leader, the cluster, and the local snapshots at or before the position it names.
  - A heartbeat gets its response.
  - A request of another protocol is rejected and the rejection recorded as Java records it.
- **Standby snapshots.** With `--accept-standby-snapshots`, a standby's notice is filed in the recording log once the commit position passes it (counter 232). The STANDBY_SNAPSHOT toggle has the leader append a flagged SNAPSHOT action, which members and their services skip.
- **Restarting from them.** At every start, before the recovery plan, a member copies the newest complete standby set newer than its own snapshots from the archive the set names, one recording at a time, and files the copies as its own; the node control toggle asks for the same at runtime.
  - An archive that fails gives way to the next; with none left, the member starts on its own snapshots.
  - That includes a set the standby no longer has, which a standby deletes as it keeps its newest only. The local archive takes the copy, then answers the source's refusal ("unknown src recording id") under the replication's id, as Java's `ReplicationSession` does. Java's replicator catches every error on an archive client of its own. The module shares one client with everything else, so it takes an error under the copy's replication id, no request pending under that id, as the copy's. Until 2026-11-06 such an error ended the member at every start: in fault-injection runs, a member down long enough for its set to go never came back.

Where Java's code would loop or throw, the module does neither:

- A notice the recording log refuses is dropped with its error; Java retries it every tick, appending its entries again.
- An invalidated standby entry restored keeps its endpoint; Java's restore builds it without one, and throws.
- A request that leaves out its closing credentials reads as having none, as Java reads the zeros past it: Aeron's own backup query runner sends one so.

## 5. Tests

- **Unit tests** (`rusteron-consensus-core`): the module on the testkit's fake ports, archive, services and members; Java's `ElectionTest` and `ConsensusModuleAgentTest` scenarios ported, and one or two tests per rule of §4.
- **The simulator** (`rusteron-cluster-sim`, its README): three members with their archives, services and a client in virtual time, under crashes, partitions, clock jumps and snapshots; invariants on every step. Java's rules fail 8 seeds in 1,000; the module's, none of 340,000 (40,000 of them with faults for 60 s), nor of 140,000 more whose partitions hold what they cut off (`--held`). Every seed that failed on the way is a regression test.
- **Interop** (`rusteron-cluster-interop/scripts/differential.sh`): Java nodes with Rust and Java services on one log, the consensus module Java's or Rust's per node (`CM_NODES`): basic, restart, timers, marks, dead service, failover, the leader killed with entries in flight, a follower started again from an empty log, quorum lost and regained, a snapshot under load. Every scenario passes on every mix: `repeat.sh` ran the ten in a row 10 times on Rust modules alone, 5 times with Rust's on node 0 only and on nodes 1 and 2, and 3 times on Java's alone.
- **Jepsen** (`rusteron-cluster-interop/jepsen`): Elle's list-append on three members under random faults and two staged scenarios that lose entries on Java; checks for truncation below the commit position, liveness after the heal, and every member rejoining. All pass on Rust members.
- **Aeron's own tests** (`rusteron-cluster-interop/aeron-tests`): Aeron 1.53.3's `ClusterTest` with `TestNode` starting the Rust module, release build, on every node. 74 of its 93 cases pass, every one the module and the harness cover. Of the other 19:
  - 13 need what the module leaves out: extensions (6), admin requests through an authoriser (3), an authenticator, recording-log validation, unresolvable names (2).
  - 6 reach into Java's module itself: its agent runner, Aeron client, error log object or snapshot duration tracker.

  Two findings came from it: the mark file's error log needs Java's categories, which the tests filter on, and a debug build's blocking adds are too slow for a five-member election. A new leader's term must reach its voters within about 1 ms of its win, before their ballots time out, as with Java.

  `ClusterBackupTest`, Java's `ClusterBackup` against Rust modules, passes 23 of its 25 cases; the other two need a challenge-response authenticator. `StandbySnapshotNoticeTest`, added to the patched tests, has a follower that missed the cluster's snapshot told of the leader's as a standby tells members of its own; restarted, it copies the set from the leader's archive, loads it and agrees with the others, on Java's modules (`JAVA_CM=1`) and on rusteron's alike.
- **A supervisor's probes**: one process of a member killed, its supervisor starting that process alone. A member whose archive was killed kept its module running without it, and never caught up. Java's module ends once its local archive's responses stop ("local archive is not connected", from its slow tick, its idle and an election's log replication). Since 2026-11-08 this module does too, checked on every duty cycle. The probes then passed on a follower and on the leader: the module ended with its service, and the supervisor started both again with the archive.
- **The client's tests** (`client.md` §5) run every scenario and Aeron's client cases on this module with the Rust client, and found two faults, both fixed. A restarted one-node leader took its recording counter before the archive had stored the recording's start position, and logged "quorum position went backwards"; the module now waits until the counter reads that position. Followers subscribed to a multicast ingress and redirected clients to the leader's unicast endpoint; the leader alone takes it now.
- **Backups and standbys, unit tests** (`tests/standby.rs`, `tests/golden.rs`): the six messages against Java's bytes, a backup answered by the leader and by a follower, refusals, notices filed once committed and after their delay, a set copied recording by recording and filed, a copy without its sync made again, a start going on when the standby's archive does not deliver, or no longer has the set; Java's `RecordingLogTest` standby cases ported.
- **An application's standby**: three members on Rust modules, with Java's archives and services, beside the application's standby, under its fault-injection runs' standby scenario. Each run lasts 300 s with a fault every 6 s on average: the standby is killed, or killed mid-snapshot; a follower is killed (half the time the standby's source); or the leader is killed, paused, or killed during a standby snapshot.
  - Nine runs of ten passed: 13.6 to 17.7 million commands each, every one answered and applied once, and all four nodes agreeing.
  - Members restarted from a snapshot 73 times; 60 of those were from a standby's set the module had copied from the standby's archive.
  - The tenth run had a member whose service failed to load its snapshot three times. The module stopped each time, as Java's does. Its logs were lost to the next run, and the same seed passed when run again. It is not explained.
  - On members with no JVM (the Rust service and archives on Aeron's C driver), thirteen of fourteen runs passed. The failure was the same seed, 35: a member restarted from its own snapshot stayed in `FOLLOWER_REPLAY` for good, its service short of the log's end, with no error logged anywhere. The seed's five later runs passed. Not explained.

## 6. Measurements

An application's commits on a 16-core Linux workstation: three nodes on one machine, each with the application's Rust engine as its service, rusteron's archives and a Java media driver, busy-spinning. Java's consensus module and the Rust one, three rounds interleaved; commands a second and median latency, offer to answer:

| In flight | Log | Java's module | Rust module |
|---|---|---|---|
| 1 | memory | 35,200–38,000 · 24.3–25.9 µs | 35,600–36,700 · 25.4–26.3 µs |
| 1 | synced | 1,690–1,710 · 517–518 µs | 1,720–1,730 · 517–518 µs |
| 64 | memory | 294,000–379,000 · 164–215 µs | 290,000–298,000 · 213–218 µs |
| 64 | synced | 31,600–32,200 · 1.76–1.82 ms | 32,200–34,100 · 1.61–1.92 ms |
| 1,024 | memory | 298,000–306,000 · 3.3–3.4 ms | 293,000–383,000 · 2.7–3.5 ms |
| 1,024 | synced | 288,000–315,000 · 3.2–3.3 ms | 270,000–314,000 · 3.2–3.6 ms |

The module is at parity. At 64 and 1,024 in flight with the log in memory, a run lands near 300,000 or near 378,000 commands a second, whichever the module, as with Java's archive and rusteron's. Ten rounds more at 64 put Java's module near 378,000 4 times and the Rust one 6 times: 292,000–382,000 and 293,000–381,000 commands a second, medians 164–217 and 165–216 µs. At one in flight, a first set of three rounds had the Rust module at 23.4–24.8 µs and Java's at 24.4–25.6. The one gap is the slowest synced runs at 1,024: two of the Rust module's six at about 270,000, Java's slowest at 288,000.

That first set also found a fault: about one cluster start in 25 lost a node, its service having taken the module's commit counter for closed. A container captured the counter's registration id before the driver had set it, a race Java's container does not have. The container now waits for the id. Every run since has had its replicas agree.

The standby's point is the members' snapshot pause, measured on the same machine. The setup:

- three members and the standby, each in processes of its own, with Java's archives and services and the log in memory;
- a 500,000-account ledger (73 MB snapshots) and 1,024 commands in flight;
- two snapshots asked for under load, the members' own or the standby's;
- Java's consensus module and the Rust one, two rounds interleaved.

The figure is the longest wait between two answers after each snapshot was asked for:

| Snapshot | Java's module | Rust module |
|---|---|---|
| the members' own | 123 and 117 ms; 95 and 118 ms | 103 and 106 ms; 123 and 100 ms |
| the standby's | 4.4 and 13.4 ms; 4.8 and 18.1 ms | 5.3 and 12.4 ms; 5.9 and 2.6 ms |

- **A standby's snapshot stops no member** on either module. Members' own snapshots stop the answers for 95–123 ms; a standby's leaves the longest gap at 2.6–18.1 ms. The runs did 417,000–434,000 commands a second with the members' snapshots and 441,000–454,000 with the standby's.
- **The 12–18 ms are not looked into.** An earlier run measured 3.4–4.9 ms for the same second snapshot on both modules. That run's first snapshot was spoilt: the bench asked for it over HTTP on the thread that reads the answers.

## 7. Run it

```bash
cargo build -p rusteron-media-driver --bin media_driver --features static
cargo build -p rusteron-cluster-interop --features static
cargo build -p rusteron-cluster --features static --bin consensus_module
SCENARIO=failover CM_NODES="0 1 2" rusteron-cluster-interop/scripts/differential.sh 3
cargo run -p rusteron-cluster-sim --release --bin cluster_sim -- --seeds 0..10000
cargo build --release -p rusteron-cluster --features static --bin consensus_module
rusteron-cluster-interop/aeron-tests/run.sh
```
