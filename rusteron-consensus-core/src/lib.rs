//! Aeron Cluster's consensus module as a sans-IO state machine, wire- and
//! on-disk-compatible with Aeron 1.53.3: no clock, no randomness, no I/O.
//! A shell (`rusteron-cluster`) drives it on Aeron's C client; a simulator
//! can drive it in virtual time.
//!
//! - [`members`]: the cluster's members and the quorum arithmetic.
//! - [`log`]: the log's messages, as the leader appends and members read them.
//! - [`codec`]: encoding, and the checks before decoding.
//! - [`timers`]: the services' timers.
//! - [`sessions`]: clients' sessions, and what the module sends them.
//! - [`service`]: the services' protocol, and their acks.
//! - [`snapshot`]: the module's own snapshot, taken and loaded.
//! - [`io`]: what the module asks of its shell, and hears back.
//! - [`module`]: the consensus module itself.
//! - [`config`]: how it runs.
//! - [`ingress`]: what clients send it.
//! - [`pending`]: the services' messages to the cluster, until committed.
//! - [`consensus`]: what members send each other.
//! - [`backup`]: a cluster's backup, Java's `ClusterBackupAgent`.

#![forbid(unsafe_code)]

pub mod backup;
pub mod codec;
pub mod config;
pub mod consensus;
pub mod ingress;
pub mod io;
pub mod log;
pub mod members;
pub mod module;
pub mod pending;
pub mod service;
pub mod sessions;
pub mod snapshot;
#[cfg(any(test, feature = "testkit"))]
pub mod testkit;
pub mod timers;
