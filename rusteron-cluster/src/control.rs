//! Aeron Cluster's control toggle: the counter in the driver's files through
//! which an operator, or a node's own code, asks the consensus module for an
//! action, a snapshot among them (Java's `ClusterControl`, `ClusterTool
//! snapshot`). The consensus module takes the action and puts the toggle
//! back to neutral.

use std::sync::atomic::{AtomicI64, Ordering};

use rusteron_archive::AeronCountersReader;

use crate::container::find_counter;

/// The control toggle's counter type (`AeronCounters.CLUSTER_CONTROL_TOGGLE_TYPE_ID`).
pub const CONTROL_TOGGLE_TYPE_ID: i32 = 202;
/// The toggle's states (`ClusterControl.ToggleState`) this crate sets.
pub const NEUTRAL: i64 = 1;
pub const SNAPSHOT: i64 = 4;
pub const STANDBY_SNAPSHOT: i64 = 7;

/// The control toggle of the cluster `cluster_id`: its key starts with the cluster's id.
pub fn find_control_toggle(counters: &AeronCountersReader, cluster_id: i32) -> Option<i32> {
    find_counter(counters, |type_id, key| {
        type_id == CONTROL_TOGGLE_TYPE_ID && key.get(..4) == Some(&cluster_id.to_le_bytes()[..])
    })
    .map(|(id, _)| id)
}

/// Asks the cluster's consensus module for a snapshot, as
/// `ClusterControl.ToggleState.SNAPSHOT.toggle` does: taken only from the
/// neutral state, so refused while another action is under way, or with no
/// toggle found.
pub fn request_snapshot(counters: &AeronCountersReader, cluster_id: i32) -> bool {
    request(counters, cluster_id, SNAPSHOT)
}

/// Asks the cluster's leader for a standby's snapshot, as
/// `ClusterControl.ToggleState.STANDBY_SNAPSHOT.toggle` does: the members
/// skip the snapshot action it appends, and a standby takes it. Taken only
/// from the neutral state, as a snapshot is.
pub fn request_standby_snapshot(counters: &AeronCountersReader, cluster_id: i32) -> bool {
    request(counters, cluster_id, STANDBY_SNAPSHOT)
}

fn request(counters: &AeronCountersReader, cluster_id: i32, state: i64) -> bool {
    let Some(id) = find_control_toggle(counters, cluster_id) else {
        return false;
    };
    let value: *mut i64 = counters.addr(id);
    // SAFETY: the value is the counter's 8 bytes in the driver's counters
    // file, which the client maps for as long as `counters` is open, aligned
    // as Aeron aligns every counter's value (to a cache line), and changed
    // by other processes only with atomic instructions, as Java's
    // `AtomicCounter` and this function change it.
    let toggle = unsafe { AtomicI64::from_ptr(value) };
    toggle
        .compare_exchange(NEUTRAL, state, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
}
