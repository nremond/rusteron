# rusteron-cluster-standby

A warm standby for Aeron Cluster on Aeron's C client, wire- and on-disk-compatible with Aeron 1.54.2.

A cluster backup copies a cluster's committed log from the members' archives. A standby does more: a service of its own runs on that log, so it holds the cluster's state as a follower does, and it takes snapshots without disturbing the members. The members load those snapshots when they restart, instead of replaying the log from their own older snapshot. If the whole cluster is lost, a new one starts from what the standby holds.

| Binary | What it does |
|---|---|
| `cluster_standby` | The standby's process: rusteron's cluster backup, plus a stand-in consensus module that feeds the standby's service container the recorded log. At each standby snapshot action it writes the module state beside the service's snapshot, then offers the members the set. |
| `standby_seed` | Seeds a new member from the standby after a disaster: copies of the standby's log and newest set into the member's archive, and the recording log a member starts from. |
| `standby_check` | `states`: the standby's module state at a members' snapshot against each member's own. `recording-log`: a recording log, an entry a line. |
| `standby_lag` | How long after the leader commits a position the standby has it, from the drivers' counters. |

The standby runs on its own media driver, next to its archive and its service's container, like a member's processes. `rusteron-cluster-tools`' `scripts/cluster.sh` starts one with `STANDBY=1`, in the slot after the members'. The members must accept standby snapshots (Aeron's `acceptStandbySnapshots`, `consensus_module --accept-standby-snapshots`).

Limits: one service, id 0, which sends no messages to the cluster. The standby's module stops at once on a second service or a service message, rather than write a snapshot the members' would not match. The watch on the backup's live log assumes the cluster's log moves more often than `--live-log-timeout` (5 s by default) while its source is alive; a cluster with no timers can raise it.

How it works, the disaster-recovery runbook, and what was tested: [docs/design.md](docs/design.md).

## Build

Aeron C is built from source, which needs CMake and, for its Gradle, a JDK older than 27. The `static` feature links it statically.

## Licence

Apache-2.0: see `NOTICE`.
