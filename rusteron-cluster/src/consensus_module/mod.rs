//! The consensus module's shell: drives a
//! [`ConsensusModule`](crate::consensus_module_agent::ConsensusModule) on Aeron's C
//! client, one thread, the client's conductor run on it, as Java's
//! `ConsensusModule` in its default mode.
//!
//! Each duty cycle reads the cluster clock once, then: every millisecond,
//! the client's conductor, images and counters come and gone, the mark file;
//! the services' and the archive's messages; the module's work; what it
//! asked for; the images it reads (the ingress, replays).

pub mod context;

#[cfg(feature = "c-client")]
mod io;
#[cfg(feature = "c-client")]
pub use io::*;
