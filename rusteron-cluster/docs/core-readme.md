# rusteron-cluster

Aeron Cluster's pieces as sans-IO state machines, wire- and on-disk-compatible with Aeron 1.54.2. Nothing in this crate reads a clock, draws a random number or does I/O: a shell (`rusteron-cluster`, on Aeron's C client) drives it in production, and a simulator can drive it in virtual time.

Today it holds the clustered service container (`service`), which runs a Rust service against Aeron Cluster's consensus module, Java's or, later, a Rust one. Its contract is Java's `ClusteredServiceAgent`'s:

- acknowledgements numbered from 0: at start (with the Aeron client's id), for each log joined, each snapshot (with its recording), each requested ack, and at termination;
- the log applied never past the commit position, each message once, in order, a snapshot taken at exactly its request's position;
- the same bytes on the wire and in snapshots as Java writes.

It also holds a consensus module's files, as Java writes them: its recording log of terms and snapshots, and the recovery plan built from it (`recording_log`), its node state file (`node_state`), its mark file's header (`markfile`) and its counters' keys and labels (`counters`).

What the container adds: nothing for the consensus module is dropped under back-pressure (a full queue holds the log instead), and a service may ask for a message again (`Apply::Retry`), which discards what it sent while handling it.

## Tests

`tests/container.rs` runs the container against the testkit (feature `testkit`): a log writer that frames messages as Aeron's term appender does, the consensus module's messages, and a fake consensus module holding acks to Java's rules. A property test applies logs of random messages, framed at random MTUs, committed a few at a time, with random retries: every message is applied once, in order, at its position.

`tests/consensus_files.rs` checks the consensus module's files against Aeron 1.54.2's own code: `fixtures.sh` runs `java/io/aeron/cluster/ConsensusFixtures.java`, which writes a recording log through Java's `RecordingLog` (the operations listed, for the Rust log to replay to the same bytes and entries), recovery plans of real recordings on an embedded Java archive, node state files, and the counters a consensus module allocates.

## Licence

Apache-2.0: see `NOTICE`.
