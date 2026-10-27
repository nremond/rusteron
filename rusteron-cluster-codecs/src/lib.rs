//! Aeron Cluster 1.53.3's SBE codecs, generated for Rust by sbe-tool 1.40.2
//! from the schemas in `schemas/` (see `regen.sh`).
//!
//! - [`cluster`]: the cluster protocol, schema 111 version 17: client
//!   sessions, the log, the consensus module and its services, snapshots.
//! - [`mark`]: the cluster mark file, schema 110 version 2.
//!
//! The generated readers check nothing but the buffer's length, and panic
//! when it is too short. Code that decodes bytes it did not write checks the
//! lengths first.

#![forbid(unsafe_code)]

#[rustfmt::skip]
#[allow(unused_imports, dead_code, unused_mut, clippy::all)]
pub mod cluster;
#[rustfmt::skip]
#[allow(unused_imports, dead_code, unused_mut, clippy::all)]
pub mod mark;
