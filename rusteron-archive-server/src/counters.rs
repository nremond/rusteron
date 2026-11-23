//! The archive's counters on the driver: allocated with Java's keys and
//! labels, their values set as Java's `setRelease`.

use std::sync::atomic::{AtomicI64, Ordering};

use rusteron_archive::{AeronCounter, AeronCountersReader};

/// Sets a counter's value, release-ordered, as Java's `setRelease`.
pub fn store(counter: &AeronCounter, value: i64) {
    let addr: *mut i64 = counter.addr();
    // SAFETY: the counter's value is an aligned i64 in the driver's counters
    // file, mapped for as long as the counter is open; others read it atomically.
    unsafe { AtomicI64::from_ptr(addr) }.store(value, Ordering::Release);
}

/// Adds to a counter's value.
pub fn add(counter: &AeronCounter, delta: i64) {
    let addr: *mut i64 = counter.addr();
    // SAFETY: as for `store`; this thread is the counter's only writer.
    unsafe { AtomicI64::from_ptr(addr) }.fetch_add(delta, Ordering::Release);
}

/// Raises a counter to `value` if below it.
pub fn propose_max(counter: &AeronCounter, value: i64) {
    let addr: *mut i64 = counter.addr();
    // SAFETY: as for `store`.
    unsafe { AtomicI64::from_ptr(addr) }.fetch_max(value, Ordering::Release);
}

/// A counter's value, by id, acquire-ordered as Java's `getCounterValue`.
pub fn load(counters: &AeronCountersReader, counter_id: i32) -> i64 {
    let addr: *mut i64 = counters.addr(counter_id);
    // SAFETY: the reader's values buffer holds every counter id it knows,
    // aligned, mapped for as long as the client is open.
    unsafe { AtomicI64::from_ptr(addr) }.load(Ordering::Acquire)
}
