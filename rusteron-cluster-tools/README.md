# rusteron-cluster-tools

Tools to run and check an Aeron Cluster whose nodes are processes of their own, each on its own media driver: Aeron's C driver, the Rust archive (`rusteron-archive-server`), the Rust consensus module (`rusteron-cluster`) and a service. Wire-compatible with Aeron 1.54.2.

| Binary | What it does |
|---|---|
| `node_driver` | A node's own process: Aeron's C media driver. It first waits out the mark files a node killed moments ago left, unless their locks prove their writers gone, as they do for rusteron's archive, consensus module, backup and container. It prints an up line scripts wait for and prints the driver's error log as errors come. On SIGTERM it closes in order and deletes its directory. It also serves as a client's driver. |
| `cluster_tool` | `status`: each node's role, module state, election state, term and commit position, read from its driver's counters with no client of any driver; `--await-leader` waits for a leader. `snapshot` and `standby-snapshot` set the leader's control toggle, as Aeron's `ClusterTool` does. |
| `log_check` | Every node's cluster log compared byte for byte as its archive replays it, members' and standbys' alike: a check to end a test run with. |

The tools share one layout (`layout`): member `N` in slot `N`, a standby in the slot after the members', each slot's ports from `21000 + 100 × slot` (archive control + 1, client-facing + 2, member-facing + 3, log + 4, catch-up + 5), and the files under `data/nodes/node<N>` and `standby<N>`. Every default can be given instead: `--hosts`, `--members`, `--port-base`, `--port-stride`, `--dir`, `--aeron-dir`.

## Scripts

- `scripts/cluster.sh` runs a cluster on one host, a process per part of a node, optionally with a standby (`STANDBY=1`, `rusteron-cluster-standby`). Its commands are `start`, `status`, `restart N`, `start-process N UNIT` and `stop`, plus `dr-seed`, which starts new members from the standby after a disaster. The service is the interop crate's test service by default. `CLUSTER_EXT` names a bash file with hooks for another service: its command line, units of its own beside the archive, its readiness and status, and options for the seed. The script's header lists them.
- `scripts/supervise.sh` restarts what exits, a process or a whole node at a time, as systemd or Kubernetes would.
- `scripts/zones.sh` makes availability zones on one machine: a network namespace each, with `tc netem` delay and loss between them. It needs root.

The scripts are Linux's: bash 4, and `taskset`, `setsid` and `ip netns` where they are used.

## Build

Aeron C is built from source, which needs CMake and, for its Gradle, a JDK older than 27. The `static` feature links it statically.

## Licence

Apache-2.0: see `NOTICE`.
