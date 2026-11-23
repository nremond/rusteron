//! Aeron Archive 1.54.2's SBE codecs, generated for Rust by sbe-tool 1.42.0
//! from the schemas in `schemas/` (see `regen.sh`).
//!
//! - [`archive`]: the archive protocol, schema 101 version 14: control
//!   requests and responses, recording signals, and the catalog's own
//!   structures (its header, a descriptor's header and the descriptor).
//! - [`mark`]: the archive's mark file, schema 100 version 2.
//!
//! The generated readers check nothing but the buffer's length, and panic
//! when it is too short. Code that decodes bytes it did not write checks the
//! lengths first.

#![forbid(unsafe_code)]

#[rustfmt::skip]
#[allow(unused_imports, dead_code, unused_mut, clippy::all)]
pub mod archive;
#[rustfmt::skip]
#[allow(unused_imports, dead_code, unused_mut, clippy::all)]
pub mod mark;
