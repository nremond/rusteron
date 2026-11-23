//! The services' timers, as the consensus module keeps them (Java's
//! `TimerService`): scheduled by correlation id, a new deadline replacing
//! the old; fired by the leader into the log once due, at most
//! [`POLL_LIMIT`] a poll, a timer the log refuses kept for the next.
//!
//! Java's default is Agrona's timer wheel, which fires the timers of one
//! tick (8 ms) in the order of their slots; these fire in deadline order,
//! then scheduling order. Only the leader fires timers, and the log is the
//! record of when and in which order: what must agree across members is
//! which timers are set at a snapshot, not the order a wheel would have.

use std::collections::{BTreeSet, HashMap};

/// At most this many timers fire a poll: Java's `TimerService.POLL_LIMIT`.
pub const POLL_LIMIT: usize = 20;

#[derive(Clone, Debug, Default)]
pub struct Timers {
    /// A timer's deadline and scheduling sequence, by correlation id.
    by_correlation_id: HashMap<i64, (i64, u64)>,
    /// (deadline, sequence, correlation id), in firing order.
    by_deadline: BTreeSet<(i64, u64, i64)>,
    next_sequence: u64,
}

impl Timers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.by_correlation_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_correlation_id.is_empty()
    }

    /// Sets the timer `correlation_id` for `deadline`, replacing any.
    pub fn schedule(&mut self, correlation_id: i64, deadline: i64) {
        self.cancel(correlation_id);
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.by_correlation_id.insert(correlation_id, (deadline, sequence));
        self.by_deadline.insert((deadline, sequence, correlation_id));
    }

    /// Whether there was such a timer.
    pub fn cancel(&mut self, correlation_id: i64) -> bool {
        match self.by_correlation_id.remove(&correlation_id) {
            Some((deadline, sequence)) => {
                self.by_deadline.remove(&(deadline, sequence, correlation_id));
                true
            }
            None => false,
        }
    }

    pub fn contains(&self, correlation_id: i64) -> bool {
        self.by_correlation_id.contains_key(&correlation_id)
    }

    /// Fires the timers due at `now`, up to [`POLL_LIMIT`]: each goes once
    /// `on_expiry` takes it (true), and the poll stops at one it refuses,
    /// which stays set. How many fired.
    pub fn poll(&mut self, now: i64, mut on_expiry: impl FnMut(i64) -> bool) -> usize {
        let mut expired = 0;
        while expired < POLL_LIMIT {
            let Some(&(deadline, sequence, correlation_id)) = self.by_deadline.first() else {
                break;
            };
            if deadline > now {
                break;
            }
            if !on_expiry(correlation_id) {
                break;
            }
            self.by_deadline.remove(&(deadline, sequence, correlation_id));
            self.by_correlation_id.remove(&correlation_id);
            expired += 1;
        }
        expired
    }

    /// The timers set, by correlation id: what a snapshot keeps.
    pub fn snapshot(&self) -> Vec<(i64, i64)> {
        let mut timers: Vec<(i64, i64)> = self
            .by_correlation_id
            .iter()
            .map(|(&correlation_id, &(deadline, _))| (correlation_id, deadline))
            .collect();
        timers.sort_unstable();
        timers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timers_fire_once_due_in_deadline_order() {
        let mut timers = Timers::new();
        timers.schedule(1, 30);
        timers.schedule(2, 10);
        timers.schedule(3, 20);
        timers.schedule(3, 40);
        let mut fired = Vec::new();
        assert_eq!(
            timers.poll(25, |c| {
                fired.push(c);
                true
            }),
            1
        );
        assert_eq!(
            timers.poll(40, |c| {
                fired.push(c);
                true
            }),
            2
        );
        assert_eq!(fired, [2, 1, 3]);
        assert!(timers.is_empty());
    }

    #[test]
    fn a_refused_timer_stays_set_and_the_poll_is_limited() {
        let mut timers = Timers::new();
        for c in 0..30 {
            timers.schedule(c, 5);
        }
        assert_eq!(timers.poll(5, |c| c != 3), 3);
        assert!(timers.contains(3));
        assert_eq!(timers.poll(5, |_| true), POLL_LIMIT);
        assert_eq!(timers.len(), 7);
        assert!(timers.cancel(29));
        assert!(!timers.cancel(29));
        assert_eq!(timers.snapshot().first(), Some(&(23, 5)));
    }
}
