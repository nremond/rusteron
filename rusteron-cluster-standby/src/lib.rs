//! A warm standby for Aeron Cluster, on Aeron's C client.
//!
//! A cluster backup ([`rusteron_cluster::backup`]) records the cluster's
//! committed log from the members' archives. The standby adds a stand-in
//! consensus module ([`module`]) that feeds that log to a service container
//! of its own, so the standby's service holds the cluster's state as a
//! follower's does. At a standby snapshot action in the log, the service
//! snapshots and the module writes the module state beside it; the module
//! then offers the set to the members, which can load it at their next
//! start instead of their own (Aeron 1.54.2's standby snapshot notice). A
//! standby is also where a new cluster starts from after a disaster
//! ([`seed`], `standby_seed`).
//!
//! - [`state`]: the consensus module's state as a follower's module keeps
//!   it, rebuilt from the log and from the service's timer requests, and
//!   written as Aeron's module snapshot, so members load the standby's sets
//!   as their own.
//! - [`snapshots`]: the standby's own list of its snapshots, which it
//!   starts from again after a restart, and what it deletes.
//! - [`module`]: the module's I/O: the service fed the log its backup
//!   records, standby snapshots taken and announced.
//! - [`seed`]: a new member's recording log from the standby's.
//! - [`watch`]: the watch on the backup's live log, which resets the backup
//!   on a still log unless the members are taking a snapshot.
//!
//! Limits: one service (id 0), and a service that sends no messages to the
//! cluster. The module refuses either rather than diverge (`docs/design.md`).

pub mod module;
pub mod seed;
pub mod snapshots;
pub mod state;
pub mod watch;
