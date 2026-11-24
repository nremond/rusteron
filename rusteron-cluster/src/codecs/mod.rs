//! Aeron Cluster 1.54.2's SBE codecs, generated for Rust by sbe-tool 1.42.0
//! from the schemas in `schemas/` (see `regen.sh`).
//!
//! - [`cluster`]: the cluster protocol, schema 111 version 17: client
//!   sessions, the log, the consensus module and its services, snapshots.
//! - [`mark`]: the cluster mark file, schema 110 version 2.
//! - [`node`]: a consensus module's node state file, schema 112 version 10.
//!
//! The generated readers check nothing but the buffer's length, and panic
//! when it is too short. Code that decodes bytes it did not write checks the
//! lengths first.

#![forbid(unsafe_code)]

#[rustfmt::skip]
#[allow(unused_imports, dead_code, unused_mut, clippy::all)]
pub mod cluster;
pub mod message;
#[rustfmt::skip]
#[allow(unused_imports, dead_code, unused_mut, clippy::all)]
pub mod mark;
#[rustfmt::skip]
#[allow(unused_imports, dead_code, unused_mut, clippy::all)]
pub mod node;
