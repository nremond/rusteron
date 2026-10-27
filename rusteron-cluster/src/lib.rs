//! Aeron Cluster's clustered service container on Aeron's C client, through
//! rusteron: runs a Rust [`ClusteredService`] against Aeron Cluster's
//! consensus module (Java's, 1.53.3), wire- and on-disk-compatible.
//!
//! The container's logic is `rusteron-cluster-core`'s, a sans-IO state
//! machine; this crate is its shell: the duty cycle, Aeron's publications,
//! subscriptions and counters, the archive for snapshots, and the mark file.
//!
//! ```no_run
//! # use rusteron_cluster::*;
//! # fn run<S: ClusteredService>(service: S) -> Result<(), ContainerError> {
//! let ctx = ContainerContext::new("/dev/shm/aeron-node0", "/var/lib/cluster/node0");
//! let exit = ServiceContainer::launch(ctx, service)?.run();
//! # Ok(()) }
//! ```

pub mod container;
pub mod markfile;

pub use container::{ContainerContext, ContainerError, Exit, Idle, ServiceContainer};
pub use rusteron_cluster_core::service::*;
