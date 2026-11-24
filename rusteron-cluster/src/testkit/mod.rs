//! For tests: [`service`], a fake consensus module and a log writer for a
//! service on the container; [`consensus`], a consensus module's surroundings
//! (archive, services, clients, members) in memory.

pub mod consensus;
#[cfg(feature = "testkit")]
pub mod service;
