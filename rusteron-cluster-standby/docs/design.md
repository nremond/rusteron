# A warm standby for Aeron Cluster

## 1. What it is

A standby follows a cluster from outside its membership: it never votes, appends or answers clients. It holds:

- the cluster's committed log, which a cluster backup records from the members' archives (`rusteron_cluster::backup`);
- the cluster's state, kept by a service container of its own on that log, as a follower's service keeps it;
- its own snapshots of that state, which the members can load when they restart.

It serves two purposes:

1. **The members restart faster.** A member's snapshots cost it a pause, and the older its newest one, the more log it replays at a restart. The standby takes snapshots on its own time and hands them to the members (§3).
2. **A new cluster can start after a disaster.** If every member is lost, new members start from copies of the standby's log and newest set (§5).

Everything it speaks is Aeron 1.54.2's: the backup's protocol with the members, the service container's protocol with its consensus module, the module snapshot's format, and the `StandbySnapshot` message the members take.

## 2. How it works

The standby is four processes on one media driver: the driver, its archive, the service container, and `cluster_standby`. `cluster_standby` runs two parts:

- **The backup** (`rusteron_cluster::backup`). It asks the members for the whole log from its start, then records the live log into the standby's archive as the members' archives replay it. A source's archive replays only up to its commit position, so the standby records nothing the cluster could still drop.
- **The module** (`module::StandbyModule`). It plays the consensus module's part for the container:
  - It allocates the two counters a container waits for at its start: the commit position, and a recovery state naming no term (the service starts empty at position 0) or the standby's newest snapshot.
  - Once the service acks its start, the module replays the local log recording from the service's position, following the recording as the backup extends it. It has the container join the replay as a follower would (`JoinLog`).
    - The replay is bounded by a counter of the module's own, the recording's position. That counter waits through the backup's changes of source instead of ending the replay.
    - The module reads the replay only once the service has acked joining it. On IPC a late subscriber joins where the slowest reader is, not where it asked, so reading earlier would have the service join past its position.
  - It reads the replay itself, ahead of the service. It keeps the module's state from what it reads (`state::ModuleView`): sessions, timers, terms, the service's message ids. It moves the commit position up to what it has read.

**Snapshot actions.**

- *The members' snapshot.* The module holds the log at the action until the service acks there. Its state as of the ack is what the members' modules write at that position: `standby_check states` compares the two. Aeron's container snapshots at the members' snapshot too, a standby's container included, and acks with that recording. Nothing lists it, so the module deletes it.
- *A standby snapshot* (the action's standby flag, which members skip). The service snapshots into the standby's archive and acks with its recording. The module writes its own state beside it as a module snapshot and lists the set in `standby-snapshots`, synced. Then it offers every member the notice, for 10 s (§3).

**What it keeps.**

- It keeps its newest sets (`--keep-snapshots`, 3 by default). An older set is first listed no more, then its recordings are deleted. The same goes for the members' sets the backup copied, all but the newest.
- What it deletes must be a snapshot recording that is no longer recorded.
- At its start, before the backup runs, it deletes what its archive lists nowhere. That is what a kill during a snapshot left, or a copy the backup gave up.

**The watch on the live log** (`watch`).

- A backup resets once its live log recording ends. A dead source's replay keeps that recording open until the driver times its image out, so the module stops a recording that has stood still for `--live-log-timeout` (5 s). The backup then resets and asks a member again.
- A cluster taking a snapshot stands still too, its log ending with the members' snapshot action, and a reset then only costs time. So while the log ends with that action the watch holds off, for `--snapshot-hold` (30 s) at most.
- What the log ends with is read on a replay of its own, ahead of the service. A standby running behind would otherwise learn of the snapshot too late.
- The timeout must be longer than the cluster's log ever stands still while its source is alive. A cluster with timers firing every 50 ms moves its log at least that often. An idle cluster with no timer does not move at all: its standby resets once a timeout, to no harm but noise.

**The standby's driver.** Started by `node_driver --standby`, it times an image out after 3 s rather than Aeron's 10 s. A source that dies is let go of sooner, while a live source keeps sending heartbeats.

## 3. The exchange with the members

A member started with standby snapshots accepted (`--accept-standby-snapshots`) takes the standby's `StandbySnapshot` message on its consensus stream. The message carries the set's entries, the service's then the module's, with the standby archive's control endpoint. The member:

1. lists those entries as `STANDBY_SNAPSHOT` entries in its recording log;
2. counts the notice on its standby snapshot counter, which `cluster_tool standby-snapshot` waits for on the leader;
3. at its next start, if a standby set is its newest snapshot, copies the set's recordings from the standby's archive into its own and loads it, as it would its own.

A member down long enough that the set is gone from the standby counts a warning and loads its own snapshot instead.

## 4. Limits

- **One service, id 0.**
- **A service that sends no messages to the cluster.** A module snapshot holds a follower's pending service messages, which a standby cannot know. The module fails at once on a second service's ack or on a service message, rather than write a snapshot that differs from the members'. The interop test service runs this way with `no-service-messages`.
- **A log that moves.** See the watch above.
- **Aeron 1.54.2's formats.** Another version's module snapshot or protocol may differ.

## 5. After a disaster: a new cluster from the standby

When every member is lost, the cluster restarts from what the standby holds. It holds every committed entry it recorded, which can be fewer than the cluster committed: what the cluster committed after the standby's last recorded position is lost.

The runbook (`scripts/cluster.sh dr-seed` runs it on one host):

1. **Fence the old cluster.** Make sure none of its members' processes is alive. An old member alive beside the new cluster would diverge from it.
2. **Seed each new member** while its driver and an empty archive run:
   ```text
   standby_seed --node N --hosts H0,H1,H2 --standby-host S [--extra-archive ...]
   ```
   - The member's archive copies, with the archives' own replication, the standby's log recording whole and its newest set.
   - The tool then writes the member's recording log: the standby's terms, naming the log's copy, followed by the set, naming its copies.
   - `--extra-archive` copies an application's other recordings too, such as an event log on another archive.
   - `--no-snapshot` copies no set, and the members replay the whole log.
3. **Start the members.** Each loads the set, replays the log after it to the copy's end, then they elect. The next term extends the copied log.
4. **Point the clients at the new cluster**, and the standby too if its endpoints changed. With the same endpoints, the standby sees a full restart and follows the new cluster.

The copy keeps the standby's log byte for byte, even when it ends inside a fragmented message. That message reached no service, and the standby's log goes on from that end once the new cluster runs.

`standby_seed --stop-at-set` is a planted bug for tests. It copies the log only up to the set, losing the committed entries the standby held after it. A check after a disaster must catch it: `log_check` then finds the standby's log unlike the new members'.

## 6. Tests

**Unit tests**, with no driver:
- the module state on crafted logs: sessions, timers (one fired before its request), terms, the service's message ids, snapshot actions, and the snapshot written and read back;
- the watch's verdicts, with default and other timeouts;
- what the standby deletes;
- a new member's recording log, checked through rusteron's own recovery plan: the set loaded, then the copied log replayed to its end;
- each binary's options.

**`rusteron-cluster-interop/scripts/standby.sh`** runs, with no JVM, three members of separate processes and the standby with its own test service:

1. Load (echoes), the members' snapshot, more load, a standby snapshot, more load.
2. Checks:
   - the standby's module state at the members' snapshot is theirs, field for field (`standby_check states`);
   - its service's trace is the leader's, line for line;
   - every log is the same (`log_check`).
3. A follower restarted: it lists the standby's set and its service starts from it. More load, and every log is the same.
4. The disaster: every member killed and reseeded from the standby (`dr-seed`). The new cluster elects, every service starts from the set, the load goes through, and every log, the standby's too, is the same from the start.

Every node also runs an events archive: a second archive on its driver, as an application keeps for an event log. `scripts/events-ext.sh`, the `cluster.sh` extension the script uses, starts it. It checks `--extra-archive`:

- Before the disaster, the standby's events archive gets three recordings (`archive_streams record`): stream 200 twice, of different messages, then stream 201.
- `dr-seed` copies them into each new member's events archive.
- Each new member's must then hold exactly the standby's newest recording of each stream, with the same start, stop and messages at the same positions (`archive_streams list`, a CRC-32C of a replay).
- The follower restarted in step 3 must have its events archive back too.

With `PLANTED=1` the seed uses `--stop-at-set`, and the script passes only if the last check fails.

Results on Linux x86-64, 1,000 echoes per load step, the whole run in 16 s:

- **Step 2.** The standby's module state at the members' snapshot (at 102,848) was theirs. Its service trace was the leader's, all 3,308 lines. All four logs were the same to 308,256.
- **Step 3.** The restarted follower listed the standby's set (at 205,600) and its service started there. The logs were the same to 410,912.
- **Step 4.** The three members were seeded at once, the log copied to its end at 410,912, and the events archives' recordings with it. One seed took 2.3 s and two 0.3 s. The new cluster elected a leader in term 1, and each new service started from the set. All four logs were the same from 0 to 513,664, the standby's following the new cluster.
- **Events archives.** Each new member's held the standby's newest recording of stream 200 (to 634,112, not the older one of other messages) and its recording of stream 201 (to 313,728), with the standby's checksums. A run with the copy narrowed to stream 201 (`streams=201`) failed on the members' missing stream 200, as it must.
- **Planted bug.** With `PLANTED=1`, `log_check` found the standby's log unlike the new members' and failed, as it must.
