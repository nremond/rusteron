//! Aeron Cluster's pieces as sans-IO state machines, wire- and
//! on-disk-compatible with Aeron 1.54.2: no clock, no randomness, no I/O.
//! A shell (`rusteron-cluster`) drives them on Aeron's C client; a simulator
//! can drive them in virtual time.
//!
//! - [`service`]: the clustered service container.
//! - [`client`]: a client's session with the cluster (`AeronCluster`).
//! - [`log`]: frames reassembled into messages.
//! - [`position`]: Aeron's frame and position arithmetic.
//! - [`counters`]: the counters of a cluster, their keys and labels.
//! - [`markfile`]: the mark file a component keeps.
//! - [`recording_log`]: a consensus module's terms and snapshots.
//! - [`node_state`]: a consensus module's candidate term.

#![forbid(unsafe_code)]

pub mod client;
pub mod counters;
pub mod log;
pub mod markfile;
pub mod node_state;
pub mod position;
pub mod recording_log;
pub mod service;
#[cfg(feature = "testkit")]
pub mod testkit;
