//! Aeron Archive's server as sans-IO state machines, wire- and
//! on-disk-compatible with Aeron 1.53.3: no clock, no randomness, no I/O. A
//! shell (`rusteron-archive-server`) drives them on Aeron's C client and the
//! file system.
//!
//! - [`catalog`]: the catalog of recordings, `archive.catalog`, as an image
//!   of the file and the ranges to write back.
//! - [`segments`]: the segment files a recording is written to, and the stop
//!   position recovered from them after a crash.
//! - [`conductor`]: the control sessions, the recordings they start and
//!   stop, the listings, replays, truncations and purges.
//! - [`replay`]: a replay session's reads and offers, without their I/O.
//! - [`protocol`]: the control protocol's requests and responses, as the
//!   archive reads and writes them; [`client`], as a client does.
//! - [`session`]: a client session with an archive, as a consensus module
//!   keeps one: connected, its requests answered, its signals routed.
//! - [`channel`]: channel URIs as the archive strips and keys them.
//! - [`counters`]: the counters the archive allocates.
//! - [`verify`]: what `ArchiveTool verify` checks of a recording, for an
//!   archive with no JVM beside it to run the tool.

#![forbid(unsafe_code)]

pub mod catalog;
pub mod channel;
pub mod client;
pub mod conductor;
pub mod counters;
pub mod protocol;
pub mod replay;
pub mod segments;
pub mod session;
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod verify;
