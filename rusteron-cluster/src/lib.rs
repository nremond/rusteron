//! Aeron Cluster in Rust, as Aeron's `io.aeron.cluster` package lays it out,
//! wire- and on-disk-compatible with Aeron 1.54.2, on Aeron's C client.
//!
//! Each component is a sans-IO state machine (no clock, no randomness, no
//! I/O), named after the Java class it follows, and a shell that drives it on
//! the C client:
//!
//! - the consensus module: [`consensus_module`] (the shell and its context),
//!   [`consensus_module_agent`] (with Election), [`session_manager`],
//!   [`pending_service_message_tracker`], [`timer_service`],
//!   [`cluster_member`], [`recording_log`], [`node_state_file`],
//!   [`log_publisher`], [`consensus_adapter`], [`ingress_adapter`],
//!   [`service_proxy`], [`consensus_module_snapshot_taker`] and
//!   [`cluster_control`];
//! - the cluster backup: [`cluster_backup`] and [`cluster_backup_agent`];
//! - Aeron's `ClusterTool`: [`cluster_tool`] and [`cluster_tool_operator`];
//! - the clustered service container: [`service`] (`io.aeron.cluster.service`);
//! - the cluster client: [`client`] (`io.aeron.cluster.client`);
//! - the counters ([`cluster_counters`]) and the SBE codecs ([`codecs`]).
//!
//! A service on the container:
//!
//! ```no_run
//! # use rusteron_cluster::*;
//! # fn run<S: ClusteredService>(service: S) -> Result<(), ContainerError> {
//! let ctx = ContainerContext::new("/dev/shm/aeron-node0", "/var/lib/cluster/node0");
//! let exit = ServiceContainer::launch(ctx, service)?.run();
//! # Ok(()) }
//! ```

#![deny(unsafe_code)]

pub mod client;
pub mod cluster_backup;
pub mod cluster_backup_agent;
pub mod cluster_control;
pub mod cluster_counters;
pub mod cluster_member;
pub mod cluster_tool;
pub mod cluster_tool_operator;
pub mod codecs;
pub mod consensus_adapter;
pub mod consensus_module;
pub mod consensus_module_agent;
pub mod consensus_module_snapshot_taker;
pub mod ingress_adapter;
pub mod log_publisher;
pub mod node_state_file;
pub mod pending_service_message_tracker;
pub mod recording_log;
pub mod service;
pub mod service_proxy;
pub mod session_manager;
#[cfg(any(test, feature = "testkit"))]
pub mod testkit;
pub mod timer_service;

pub use service::clustered_service_container::{ContainerContext, ContainerError, Exit, Idle, ServiceContainer};
pub use service::*;
