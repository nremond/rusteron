//! A client of an archive without I/O, as Aeron's `io.aeron.archive.client` has it: the
//! session a consensus module keeps with its archive ([`aeron_archive`]), and the requests
//! it sends and responses it reads ([`archive_proxy`]). The archive is such a client too when
//! it replicates. (The C client's `AeronArchive`, with I/O, is at the crate root.)

pub mod aeron_archive;
pub mod archive_proxy;
