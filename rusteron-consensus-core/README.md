# rusteron-consensus-core

Aeron Cluster 1.53.3's consensus module as a sans-IO state machine, wire- and on-disk-compatible with Java's: no clock, no randomness, no I/O. A shell (`rusteron-cluster`, on Aeron's C client) drives it in production; a simulator can drive it in virtual time.

`module` is the consensus module itself, Java's `ConsensusModuleAgent` and `Election`: inputs as events and time, outputs through `Ports` and `Action`s, and each of Java's waits in place (for the archive, a recording's counter, the services' acks, a snapshot recorded) a state the duty cycle checks. It runs a member of a cluster of any size: recovery from its snapshot and log; the election (`module/election.rs`: canvass, nomination, ballots, the leader's new term and replay, a follower's replication from the leader's archive, replay, catch-up on the leader's log and join of its live log, truncation of a log ahead of the new leader); the leader's duty cycle (clients, timers, services' messages, the quorum's commit position, catch-up replays for followers); a follower's (the leader's log up to the commit position it is told, its append position reported, the leader's heartbeat watched) in `module/peers.rs`; the control toggle (suspend, resume, snapshot, shutdown, abort); its own snapshot; termination across members.

Its building blocks, each Java's behaviour:

- `members`: the cluster's members, parsed from `aeron.cluster.members`, and the quorum arithmetic over them.
- `log`: the log's messages, as the leader appends them and members read them back.
- `sessions`: clients' sessions with the default authenticator, and their egress.
- `timers`: the services' timers, fired by the leader in deadline order.
- `pending`: the services' messages to the cluster, until the log commits them.
- `service`: the services' protocol and their acks.
- `snapshot`: the module's own snapshot.
- `io`: what the module asks of its shell (`Ports`, `Action`) and hears back (`Io`).
- `consensus`: what members send each other on the consensus stream.
- `ingress`, `config`: what clients send, and how the module runs.

## Tests

`tests/node.rs` runs a node in memory on the testkit (feature `testkit`): fake ports, a fake archive that records the log and snapshots and replays them, fake services that ack as Java's container does. A node starts on an empty log and leads term 0; a client's session is opened in the log and its messages committed; a snapshot is taken on the toggle and entered in the recording log; the node restarts from it, replays the log after it and leads term 1; a shutdown snapshots then terminates; a service gone ends the node.

`tests/election.rs` runs one member of three, the test playing the other two on the consensus stream and the leader's log: the scenarios of Java's `ElectionTest` and `ConsensusModuleAgentTest` a plain cluster meets. A member canvasses; a candidate with every vote leads and publishes its term, one without a majority canvasses again, only an appointed leader stands; votes go to a log no shorter; a leader commits what a quorum appended, steps down without an active quorum, replays its log to a follower catching up and terminates once every follower acked; a follower joins the live log, follows it up to the commit position it is told, starts an election without word from the leader, stops where the leader terminates, catches up on the leader's log, replicates a term it lacks from the leader's archive, and truncates a log ahead of the new leader.

The module's files (recording log, node state, mark file) and counters are `rusteron-cluster-core`'s; its archive client is `rusteron-archive-core`'s.

## Licence

Apache-2.0, as Aeron: see `NOTICE`.
