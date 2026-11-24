//! Counters on the driver, written as Java's `AtomicCounter` writes them:
//! values stored release-ordered, compared-and-set, read acquire-ordered.

#![allow(unsafe_code)]

use std::sync::atomic::{AtomicI64, Ordering};

use rusteron_archive::{AeronCounter, AeronCountersReaderRef};

fn value(addr: *mut i64) -> &'static AtomicI64 {
    // SAFETY: a counter's value is an aligned i64 in the driver's counters
    // file, mapped for as long as the client that gave the address is open;
    // other processes change it only with atomic instructions.
    unsafe { AtomicI64::from_ptr(addr) }
}

/// Sets a counter's value, as Java's `setRelease`.
pub fn store(counter: &AeronCounter, v: i64) {
    value(counter.addr()).store(v, Ordering::Release);
}

/// Adds to a counter's value.
pub fn add(counter: &AeronCounter, delta: i64) {
    value(counter.addr()).fetch_add(delta, Ordering::Release);
}

/// Sets a counter's value if it is `expected`: whether it was.
pub fn compare_and_set(counter: &AeronCounter, expected: i64, new: i64) -> bool {
    value(counter.addr())
        .compare_exchange(expected, new, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
}

/// A counter's value, as Java's `getCounterValue`.
pub fn load(counters: &AeronCountersReaderRef, counter_id: i32) -> i64 {
    value(counters.addr(counter_id)).load(Ordering::Acquire)
}

/// A counter's own value.
pub fn get(counter: &AeronCounter) -> i64 {
    value(counter.addr()).load(Ordering::Acquire)
}
