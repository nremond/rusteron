# The cluster simulator

A deterministic simulator of an Aeron Cluster whose every member runs the Rust consensus module (`rusteron-cluster`). The members, their archives, their services and a client run on one thread in virtual time; a seed sets everything, and the same seed replays the same run.

```bash
cargo run -p rusteron-system-tests --release --bin cluster_sim -- --seeds 0..1000
cargo run -p rusteron-system-tests --release --bin cluster_sim -- --seed 641      # one seed, with its trace
cargo run -p rusteron-system-tests --release --bin cluster_sim -- --leader-lost-during-replay --seeds 0..1000
```

## What it models

- **The network**, at the level of the log's and the consensus stream's messages: each link up or down. Up, it delivers in order after a latency of 1 to 3 ms. Down, what was on it is lost and the images it carried go, as Aeron's do once their connection times out. With `--held` (`SimConfig::held_partitions`), a partition holds what members send each other instead, and sends it all, in order, once the links are back, as Aeron's streams resend what a cut shorter than their timeouts held back.
- **The leader's log**: a multi-destination publication. A follower's image joins at the position of the first frame it gets; catch-up replays join at their start. Loss within two terms of the publication is repaired, as Aeron's NAKs do; further back, the image times out.
- **Archives**: recordings of the leader's publication and of a follower's image, replays to the module or to a follower's catch-up endpoint, replication between members' archives, with the control protocol's answers and signals.
- **Services** apply the log up to the commit position, take a snapshot at each snapshot action, and ack as Java's container does.
- **A client** connects through any member, follows redirects and new leaders, and sends messages tagged with an id.

The archive and services are models of what the module asks of them, not the archive server and service container: those have their own interop tests.

## Faults and checks

For the fault phase (20 s by default):
- members crash and restart on what they wrote;
- members are cut off from the others, both ways or one way between two;
- clocks jump ahead;
- snapshots are asked for at random.

Then the network heals, every member restarts, and the cluster has 15 s to come back.

Checked all along:
- a member's commit position never goes back;
- members' services never apply different entries at a position, nor overlapping ones;
- a client message is applied at one position only.

Checked at the end:
- every member is up, with one leader in one term;
- a client message sent after the faults was committed;
- every member's service applied every client message any applied.

## Findings

With the module doing as Java's 1.53.3 does, about 8 seeds in 1,000 fail (`--seeds 0..1000`: 111, 116, 178, 186, 380, 621, 641, 945). Six lose committed entries, and two never settle after their logs were cut below their commit positions. All follow from three of Java's election rules, which the module ports as they are:

- A member canvassing follows a leader of any term, even one older than the term it voted in (`Election.onNewLeadershipTerm` accepts any `leadershipTermId` in `CANVASS`).
- A member truncates its log to where the new term begins without checking its commit position.
- A leader takes a canvassing member's position, from that member's own log, as an append position towards its quorum (`ConsensusModuleAgent.onCanvassPosition`). It counts it even when that log has gone another way.

Seed 641 is the shortest. Member 1 wins term 0, but is cut off before it can publish it. Member 0 then wins term 1. Member 2 voted in term 1, then canvasses, and still follows member 1's term 0, which commits. Later member 0's term 1 truncates them both below their commit positions.

The Jepsen suite (`rusteron-system-tests/jepsen`) stages seeds 111 and 641 on real nodes. Java 1.53.3 members lose committed entries and stop serving, just as Rust members do.

### Raft's rules

The module now applies Raft's rules where Java's election is laxer, with nothing changed on the wire (`module/election.rs`):

- **Votes are kept.** A member that voted in a term follows no leader of an older one, and canvasses with the term it voted in.
- **A stale leader steps down.** A member leading a term steps down when it hears of a newer one, from a canvass, a vote request, a new term or a commit position, in its election or after. Java leaves a leader in LEADER_LOG_REPLICATION or LEADER_READY for good.
- **A refused leader is not a leader.** Its messages do not keep a member from standing.
- **Commit counts what is known.** A leader counts a member's position towards its quorum only as far as that member's log is known to be its own: what it reported in the leader's term, or of a term both hold.
- **Log matching.** A leader names the latest term both logs hold entries of; a member cuts back the terms the leader never had. A recording log fills in the terms its member skipped as empty ones, and Java takes those for the member's.
- **Every entry filed under its term.** A member files a term as soon as its log is about to receive that term's entries: the leader's term before the leader's log can reach its recording, a replicated term before its replication. Java files them once joined or once replicated, so an election starting over in between leaves entries no term holds, and matching by the log's last term then goes wrong.
- **Logs compared by their last entry.** A member canvasses and votes with the term of its log's last entry, as its recording log files it. Java's election takes the term of the last term event it replayed, which a replay stopping short of the log's end leaves older, and a member then votes for a log shorter in its own last term.
- **Committed entries stay.** No member truncates below its commit position: a member refuses a leader that asks it to, and records it.

Making them hold took more fixes elsewhere, each found here, some of them for faults Java has too, which its rarer elections seldom meet:
- a follower catching up from the leader's replay reports its old term until it reads the new one's first event: its position counts as of the leader's term (seed 972);
- an election restarting mid catch-up or mid replay keeps what its services already applied (999, 860);
- the module reads its log with the term the log holds where it starts, not one it stood for in an election that failed: otherwise it skips cluster actions its services apply, and their acks go out of sequence (13933);
- the acks of a services' join given up on are dropped before the next is awaited (8876, 98212);
- a member in follower replay without word from its leader starts over, where Java waits for good (298580);
- a member whose log holds nothing after the leader's last term takes that term as its own (391);
- a recording log whose terms are all dropped takes the log's next recording;
- a member drops a term it filed but never received entries of, once a leader that never had it names an older one: otherwise the leader's entries it replicates are filed under that term, and a later leader holding its recording log sends members back below their commit position (72608, with held partitions).

| Rules | Seeds 0..1,000 | 0..100,000 | 100,000..300,000 | Faults for 60 s, 40,000 seeds |
|---|---|---|---|---|
| Java 1.53.3's | 8 fail | not run | not run | not run |
| Raft's, terms filed once joined (c84620f) | 0 | 42 fail | not run | 4 fail of 10,000 |
| Raft's, terms filed as recorded (c9cec64) | 0 | 30 fail, all stalls | not run | 2 fail of 10,000 |
| Raft's, with the fixes above | 0 | 0 | 0 | 0 |

`seeds.rs` keeps the seeds above as regressions.

### A leader lost during a follower's replay

A fault injector pausing a leader (`kill -STOP` on its processes) until another leads, 15 s at most; past Aeron's 10 s client timeouts, the paused member's module and service close when it is continued. With no JVM, four runs lost members so, each election taking 9 to more than 15 s, each time the leader paused 3.5 to 4.7 s after another member restarted. That member was replaying 2.5 to 2.9 million commands from its snapshot in follower replay, which has no timeout and where a member neither canvasses nor votes: the other could not win a majority until the replay ended and a second more passed failing to reach the paused leader ("failed to send catchup position", or "failed to join catchup log"). Java's `Election` is the same there.

`--leader-lost-during-replay` stages it: no random faults; a follower crashes after `--fault-ms` (20 s here), restarts 6 s later and replays its log, 3 messages a millisecond to its module (`--replay-rate`, `SimConfig::local_replay_rate`) and 2 to its service (`--service-rate`), which lags as on real members; once it replays, the leader is cut off until another member leads a newer term in LEADER_READY or past it. Over seeds 0..1,000:

| The member in follower replay | Leader replaced after | Invariants |
|---|---|---|
| replays to the end (Java's way) | 4.05 to 4.31 s; 9.0 to 9.3 s with services as fast as the module, one message a millisecond | kept |
| gives the replay up once its leader is silent for the heartbeat timeout (1 s) | 1.57 to 1.83 s | broken in every seed: the service, its replay stopped while it lagged, is told to join the next one past where it is |
| gives it up, and holds it until the service acks the module's position | 2.07 to 2.33 s | kept |

The module does the last. With it, seeds 0..200,000, 20,000 with faults for 60 s and 5,000 with a backup keep every invariant.

### A leader's partition healed

A fault injector across zones cut the leader off for 2 to 5 s. In 20 of 23 such partitions on Rust members, a second election followed 0.7 to 2.2 s after the links came back. Aeron's streams held what the others sent during the cut, their finished election among it, and the member cut off got it late. Its log ahead, it refused their candidate's vote but took the term; their stale canvass positions then made it a quorum candidate, and its vote request for the term after sent the leader and its follower into an election. The simulator lost what a cut carried, so it never showed what late messages do after a heal.

`leader_partitioned` stages it (its tests in `tests/sim_seeds.rs`): no random faults; the leader of a steady cluster is cut off from both others for a given time, then the links heal and the cluster settles. The module now holds a member back a canvass round after it takes a term by refusing a vote, and has a member in Nominate follow a leader of its term as one in Canvass does (`rusteron-cluster/docs/consensus.md` §4). Over seeds 0..100, cuts of 2.0, 3.6 and 4.9 s, held and lost links:

- the others elected another leader in 595 of 600 runs, and it kept its term after the heal in every one, where 250 lost it before;
- a held cut of 1.5 s, too short to replace the leader, elects within 5 s of the heal: a median of 198 ms, against 165 ms before.

With held partitions as random faults, seeds 0..100,000, and 40,000 with faults for 60 s, keep every invariant once 72608's empty term is dropped (above).

## Licence

Apache-2.0, as Aeron: see `NOTICE`.
