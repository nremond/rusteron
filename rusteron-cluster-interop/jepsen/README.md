# Jepsen tests of Aeron Cluster

Jepsen tests of three-member clusters running Elle's list-append workload, under random faults or staged scenarios. A member is either Java Aeron 1.54.2 or rusteron's Rust consensus module.
- **Java** (`--impl java`): media driver, archive and consensus module in the member's JVM.
- **Rust** (`--impl rust`): rusteron's C media driver, Rust archive and Rust consensus module, beside the JVM.

Either way, the service is Java's container, running `ListAppendService`. Clients are Aeron Cluster sessions from the host.

```bash
./run.sh --scenario random --time-limit 120                                   # partitions, kills, pauses
./run.sh --impl rust --scenario random --time-limit 120
./run.sh --scenario stale-leader --election-timeout-ms 3000 --time-limit 90     # seed 111's sequence
./run.sh --scenario stale-election --election-timeout-ms 3000 --time-limit 100  # seed 641's
./run.sh serve                                                                  # results in store/, on port 8080
```

## How it runs

- **Nodes** are Docker containers n1..n3 (`scripts/cluster.sh`), at 10.77.0.11 onwards, on a bridge network whose host side, 10.77.0.1, is where Jepsen and its clients run.
- **Their image is empty.** The host's `/usr` and `/etc` are mounted read-only, so a node runs the host's JDK and tools (`iptables`, `pkill`) and nothing is pulled or installed. The suite, the Maven repository and rusteron's `target/` are mounted read-only too. Each node's data is a tmpfs at `/data`.
- **Commands** reach the nodes through `docker exec` (`docker.clj`), not SSH.
- **Builds.** The Java side and the classpath come from Maven (`pom.xml`): Jepsen 0.3.14, and the Aeron that `AERON_VERSION` names, 1.54.2 by default. Java members run any version Maven has. Rust members need 1.54.2, the version rusteron's crates follow, and rusteron's debug `media_driver`, `aeron_archive` and `consensus_module`, built with `--features static` (`cargo build --features static -p rusteron-media-driver -p rusteron-archive -p rusteron-cluster --bins`).
- **Requirements:** Docker, Maven and a JDK of 21 or later on the host.

## What it checks

- **`:append`:** Elle's checker for strict serializability. Every transaction, reads too, goes through the log, and the leader's service answers it.
- **`:truncation`:** fails when a member cut its log below its commit position (`Truncating Cluster Log`, Java's or the Rust module's).
- **`:liveness`:** fails when no transaction succeeded after the final heal. A cluster that loses committed entries and then stalls serves no read for Elle to catch the loss with.
- **`:rejoined`:** fails unless, probed 20 s after the final heal, every member is past its election, in one term, with one leader. A cluster serving on a majority passes `:liveness` with a member left behind, such as a leader stuck in an older term's election.

A transaction ends one of three ways:
- **`:ok`:** answered.
- **`:fail`:** never sent.
- **`:info`:** sent, with no answer within 5 s.

## Scenarios

- **`random`:** Jepsen's partitions (one member, a majority, a majorities ring, the leader), kills and pauses, every `--nemesis-interval` seconds.
- **`stale-leader`** stages `rusteron-cluster-sim`'s seed 111 from the members' counters (`ClusterProbe`):
  1. Cut off the leader, then ask it for a snapshot. It stays leader in SNAPSHOT, because its inactive-quorum check runs only in ACTIVE.
  2. When one of the others stands as candidate, cut it from the voter before it publishes its new term.
  3. Reconnect the voter, now canvassing, to the stale leader.
  4. Heal everything.
- **`stale-election`** stages seed 641:
  1. Cut off the leader X. The first candidate, Y, wins T1 with Z's vote, then is cut off.
  2. Reconnect X and Z. Z votes for X's T2, then X is cut off.
  3. Reconnect Z to Y, which is still waiting to complete its T1.
  4. Heal everything.

## Findings on Aeron 1.54.2 (2026-11-24)

The same runs on 1.54.2: Java members once per scenario, Rust members twice. The Rust consensus module is the one with Raft's rules (below).

| Scenario | Java 1.54.2 | Rust consensus module |
|---|---|---|
| `random`, 120 s | Valid | Valid, 2 runs |
| `stale-leader` | Two members cut their logs from 70528 to 39232, below their commit position. No transaction succeeds after the heal, and every member is still in an election 20 s after it | Valid, 2 runs |
| `stale-election` | No member cuts its log, but no transaction succeeds after the heal. n1 is leader of term 2 at commit 38048, while the other two, still in term 1 at 66976, are in an election | Valid, 2 runs |

Elle's `:append` check passes in every run. Where Java fails, nothing succeeds after the heal, so no read can show the loss; `:truncation`, `:liveness` and `:rejoined` are the checks that catch it.

aeron-io/aeron#2212, Java's fix for these losses, was merged on 2026-11-11, after 1.54.2: no release has it yet.

## Findings on Aeron 1.53.3 (2026-10-29)

| Scenario | Java 1.53.3 | Rust consensus module, as Java | Rust, Raft's rules (2026-10-30) |
|---|---|---|---|
| `random`, 120 s | Valid | Valid, after a fix (below) | Valid, 2 runs |
| `stale-leader` | Two members cut their logs from 72544 to 40896, below their commit position; no transaction succeeds after the heal | The same, 68000 to 40352 | Valid, 2 runs: the stale leader steps down on the voter's canvass, and the stale candidate on the new leader's commit position |
| `stale-election` | Z follows T1 after voting in T2. After the heal, the T1 members can't truncate to X's T2 base (39584 is inside one of their frames), and loop on "joinPosition less than logPosition"; no transaction succeeds | The same, the truncation going through, 71328 to 42496 | Valid, 2 runs: Z refuses T1, Y steps down on Z's canvass, X on the new leader's commit position |

The staged scenarios reproduce, on real nodes, what the simulator found in Java's election rules:
- a member that voted in a newer term follows an older one while canvassing;
- truncation ignores the commit position;
- a leader outside ACTIVE never steps down.

Committed entries leave every log, and the cluster stops serving.

With `:rejoined`, Java 1.53.3 also fails it in both staged scenarios: every member is still in an election at the end.

The Rust consensus module now applies Raft's rules where Java's election is laxer (see `rusteron-consensus-core/src/module/election.rs`): the losses go, and every member comes back. aeron-io/aeron#2212 fixes the losses in Java; built and run here, it leaves the leader of the abandoned term in LEADER_LOG_REPLICATION for good.

**The Rust fix.** The first random run on Rust nodes found a bug of the Rust shell. After a pause longer than its service interval, the consensus module's Aeron client closed itself, and the module kept running with it, where Java's agent terminates ("unexpected Aeron close"). Its member never came back.

## Licence

Apache-2.0, as Aeron.
