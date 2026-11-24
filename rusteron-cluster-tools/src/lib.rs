//! Tools to run and check an Aeron Cluster whose nodes are processes of
//! their own on a driver each: a media driver process, an archive, a
//! consensus module and a service per node, as `scripts/cluster.sh` starts
//! them on one host.
//!
//! - [`layout`]: where nodes are when they follow one plan, the slot, port
//!   and directory layout the tools and scripts share.
//! - [`driver`]: a node's media driver process (`node_driver`).
//! - [`logs`]: every node's cluster log compared byte for byte as its
//!   archive replays it (`log_check`).
//! - [`args`]: the command line as the tools read it.
//!
//! `cluster_tool` prints each node's role, module state, election state,
//! term and commit position from its driver's counters, waits for a leader,
//! and sets the leader's control toggle for a snapshot, as Aeron's `ClusterTool`.

pub mod args;
pub mod driver;
pub mod layout;
pub mod logs;
