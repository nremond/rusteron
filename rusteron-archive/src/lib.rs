/**/
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::all)]
#![allow(unused_unsafe)]
#![allow(unused_variables)]
#![allow(unsafe_op_in_unsafe_fn)]
#![cfg_attr(feature = "c-client", doc = include_str!("../README.md"))]
//! # Features
//!
//! - **`static`**: When enabled, this feature statically links the Aeron C code.
//!   By default, the library uses dynamic linking to the Aeron C libraries.
//! - **`backtrace`**: When enabled will log a backtrace for each AeronCError
//! - **`extra-logging`**: When enabled will log when resource is created and destroyed. Useful if you're seeing a segfault due to a resource being closed
//! - **`log-c-bindings`**: When enabled will log every C binding call with arguments and return values. Useful for debugging FFI interactions
//! - **`c-client`** (default, with `build-from-source` or `precompile`): Aeron's C client, archive
//!   client and the bindings, and everything built on them. Without it, only the pure-Rust modules
//!   below build, and no C is compiled.
//! - **`server`**: Aeron Archive's server in Rust and the `aeron_archive` binary (implies `c-client`).
//! - **`testkit`**: the archive server's test kit.
//! - **`precompile`**: When enabled will use precompiled C code instead of requiring cmake and java to be installed

// Pure-Rust parts of Aeron's client and Agrona that the archive server and the cluster share.
pub mod distinct_error_log;
pub mod fragment_assembler;
pub mod logbuffer;
pub mod mark_file;

// Aeron Archive's server in Rust, as Aeron's `io.aeron.archive` package lays it out, wire- and
// on-disk-compatible with Aeron 1.54.2. Each class is a sans-IO state machine (no clock, no randomness,
// no I/O); with the `server` feature, its I/O on Aeron's C client is beside it, and `archive` runs them.
#[cfg(feature = "server")]
pub mod archive;
pub mod archive_conductor;
pub mod archive_counters;
#[cfg(feature = "server")]
pub mod archive_mark_file;
pub mod archive_tool;
pub mod catalog;
pub mod channel_uri;
pub mod client;
pub mod codecs;
pub mod control_session_adapter;
#[cfg(feature = "server")]
pub mod recording_writer;
pub mod replay_session;
pub mod segments;
#[cfg(feature = "testkit")]
pub mod testkit;

// Aeron's C client and archive client, their bindings, and rusteron's API over them.
#[cfg(feature = "c-client")]
include!("c_client.rs");
// Declared here, not in c_client.rs: the macros it exports must not come from a macro's expansion.
#[cfg(feature = "c-client")]
pub mod testing;
