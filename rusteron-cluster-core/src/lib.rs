//! Aeron Cluster's pieces as sans-IO state machines, wire- and
//! on-disk-compatible with Aeron 1.53.3: no clock, no randomness, no I/O.
//! A shell (`rusteron-cluster`) drives them on Aeron's C client; a simulator
//! can drive them in virtual time.
//!
//! - [`service`]: the clustered service container.
//! - [`log`]: frames reassembled into messages.
//! - [`position`]: Aeron's frame and position arithmetic.
//! - [`counters`]: the counters a service reads, and their keys.
//! - [`markfile`]: the mark file a component keeps.

#![forbid(unsafe_code)]

pub mod counters;
pub mod log;
pub mod markfile;
pub mod position;
pub mod service;
#[cfg(feature = "testkit")]
pub mod testkit;
