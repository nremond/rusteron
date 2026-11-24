//! A service's messages to the cluster, as Java's
//! `PendingServiceMessageTracker` tracks them: every member's services send
//! them, each numbered from the service's own range of session ids, and the
//! leader appends them to the log; once the log has a message committed,
//! every member drops it. A follower that becomes leader appends what it
//! still holds.
//!
//! A message is kept as its service sent it, from its SBE header: the
//! `SessionMessageHeader` (its session id and timestamp rewritten), then
//! the payload. The timestamp field holds where the leader appended it,
//! `i64::MAX` before.

use std::collections::VecDeque;

/// At most this many messages are appended a poll.
const SERVICE_MESSAGE_LIMIT: usize = 20;
/// The SBE header and the `SessionMessageHeader` block before a payload.
pub const SESSION_HEADER_LENGTH: usize = 32;
const CLUSTER_SESSION_ID_OFFSET: usize = 8 + 8;
const TIMESTAMP_OFFSET: usize = 8 + 16;
/// What Agrona's `ExpandableRingBuffer` takes for a record's header, and aligns records to.
const RING_HEADER_LENGTH: usize = 8;

/// A tracker whose snapshot does not add up, with Java's message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

/// The session id `session_id` takes in service `service_id`'s range.
pub fn service_session_id(service_id: i32, session_id: i64) -> i64 {
    (i64::from(service_id) << 56) | session_id
}

/// Which service a message in the log came from.
pub fn service_id_from_log_message(cluster_session_id: i64) -> i32 {
    ((cluster_session_id as u64 >> 56) & 0x7F) as i32
}

fn i64_at(bytes: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("8 bytes"))
}

fn put_i64(bytes: &mut [u8], offset: usize, value: i64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Pending {
    message: Vec<u8>,
}

impl Pending {
    fn cluster_session_id(&self) -> i64 {
        i64_at(&self.message, CLUSTER_SESSION_ID_OFFSET)
    }

    fn append_position(&self) -> i64 {
        i64_at(&self.message, TIMESTAMP_OFFSET)
    }

    fn set_append_position(&mut self, position: i64) {
        put_i64(&mut self.message, TIMESTAMP_OFFSET, position);
    }
}

#[derive(Clone, Debug)]
pub struct PendingServiceMessages {
    pub service_id: i32,
    /// The id the next message from the service takes.
    pub next_service_session_id: i64,
    /// The id of the last message the log committed.
    pub log_service_session_id: i64,
    pending: VecDeque<Pending>,
    /// How many of `pending`, from the front, the leader appended.
    appended: usize,
    uncommitted: usize,
}

impl PendingServiceMessages {
    pub fn new(service_id: i32) -> Self {
        let log_service_session_id = service_session_id(service_id, i64::MIN);
        Self {
            service_id,
            next_service_session_id: log_service_session_id + 1,
            log_service_session_id,
            pending: VecDeque::new(),
            appended: 0,
            uncommitted: 0,
        }
    }

    /// A message from the service (its SBE header first): numbered, and
    /// kept unless the log already has it, as on a follower whose services
    /// lag the log.
    pub fn enqueue(&mut self, message: &[u8]) {
        let cluster_session_id = self.next_service_session_id;
        self.next_service_session_id += 1;
        if cluster_session_id > self.log_service_session_id && message.len() >= SESSION_HEADER_LENGTH {
            let mut message = message.to_vec();
            put_i64(&mut message, CLUSTER_SESSION_ID_OFFSET, cluster_session_id);
            put_i64(&mut message, TIMESTAMP_OFFSET, i64::MAX);
            self.pending.push_back(Pending { message });
        }
    }

    /// The leader appends what it has not yet, at most 20: `append` takes a
    /// message's session id and payload and gives the position after it, or
    /// a negative result when the log refused it, which ends the poll. How
    /// many were appended.
    pub fn poll(&mut self, mut append: impl FnMut(i64, &[u8]) -> i64) -> usize {
        let mut count = 0;
        while count < SERVICE_MESSAGE_LIMIT {
            let Some(p) = self.pending.get_mut(self.appended) else {
                break;
            };
            let position = append(p.cluster_session_id(), &p.message[SESSION_HEADER_LENGTH..]);
            if position <= 0 {
                break;
            }
            p.set_append_position(position);
            self.appended += 1;
            self.uncommitted += 1;
            count += 1;
        }
        count
    }

    /// The leader drops what the log committed.
    pub fn sweep_leader_messages(&mut self, commit_position: i64) {
        if self.uncommitted > 0 {
            let swept = self.sweep_committed(commit_position);
            self.appended = self.appended.saturating_sub(swept);
        }
    }

    fn sweep_committed(&mut self, commit_position: i64) -> usize {
        let mut swept = 0;
        while let Some(p) = self.pending.front() {
            if commit_position < p.append_position() {
                break;
            }
            self.log_service_session_id = p.cluster_session_id();
            self.uncommitted = self.uncommitted.saturating_sub(1);
            self.pending.pop_front();
            swept += 1;
        }
        swept
    }

    /// A follower drops what the log has, up to `cluster_session_id`.
    pub fn sweep_follower_messages(&mut self, cluster_session_id: i64) {
        self.log_service_session_id = cluster_session_id;
        while self
            .pending
            .front()
            .is_some_and(|p| p.cluster_session_id() <= cluster_session_id)
        {
            self.pending.pop_front();
        }
    }

    /// An election undid the leader's uncommitted appends: they go again.
    pub fn restore_uncommitted_messages(&mut self, commit_position: i64) {
        if self.uncommitted > 0 {
            self.sweep_committed(commit_position);
            self.reset();
            self.uncommitted = 0;
            self.appended = 0;
        }
    }

    /// Marks every message not appended: after a snapshot loads.
    pub fn reset(&mut self) {
        for p in &mut self.pending {
            p.set_append_position(i64::MAX);
        }
    }

    /// A message from a snapshot.
    pub fn append_message(&mut self, message: &[u8]) {
        self.pending.push_back(Pending {
            message: message.to_vec(),
        });
    }

    /// The tracker's state from a snapshot.
    pub fn load_state(&mut self, next_service_session_id: i64, log_service_session_id: i64) {
        self.next_service_session_id = next_service_session_id;
        self.log_service_session_id = log_service_session_id;
        self.pending.clear();
        self.appended = 0;
        self.uncommitted = 0;
    }

    /// What a snapshot loaded must be: the messages numbered on from the
    /// log's, and the next id after them.
    pub fn verify(&self) -> Result<(), Error> {
        for (i, p) in self.pending.iter().enumerate() {
            let index = i as i64 + 1;
            if p.cluster_session_id() != self.log_service_session_id + index {
                return Err(Error(format!(
                    "snapshot has incorrect pending message: serviceId={} nextServiceSessionId={} \
                     logServiceSessionId={} clusterSessionId={} pendingMessageIndex={index}",
                    self.service_id,
                    self.next_service_session_id,
                    self.log_service_session_id,
                    p.cluster_session_id()
                )));
            }
        }
        let count = self.pending.len() as i64;
        if self.next_service_session_id != self.log_service_session_id + count + 1 {
            return Err(Error(format!(
                "snapshot has incorrect pending message state: serviceId={} nextServiceSessionId={} \
                 logServiceSessionId={} pendingMessageCount={count}",
                self.service_id, self.next_service_session_id, self.log_service_session_id
            )));
        }
        Ok(())
    }

    /// The bytes Java's ring buffer would hold for these messages, without
    /// the padding a wrap adds: what a snapshot records as its capacity.
    pub fn size(&self) -> usize {
        self.pending
            .iter()
            .map(|p| (RING_HEADER_LENGTH + p.message.len()).next_multiple_of(RING_HEADER_LENGTH))
            .sum()
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// The messages, as a snapshot keeps them.
    pub fn messages(&self) -> impl Iterator<Item = &[u8]> {
        self.pending.iter().map(|p| p.message.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A service message as a service sends it: header, block, payload.
    fn message(payload: &[u8]) -> Vec<u8> {
        let mut m = vec![0; SESSION_HEADER_LENGTH];
        m[0..2].copy_from_slice(&24u16.to_le_bytes());
        m[2..4].copy_from_slice(&1u16.to_le_bytes());
        m.extend_from_slice(payload);
        m
    }

    #[test]
    fn the_leader_appends_then_drops_what_was_committed() {
        let mut t = PendingServiceMessages::new(1);
        let base = service_session_id(1, i64::MIN);
        assert_eq!(service_id_from_log_message(base + 1), 1);
        t.enqueue(&message(b"a"));
        t.enqueue(&message(b"b"));
        let mut log = Vec::new();
        assert_eq!(
            t.poll(|id, payload| {
                log.push((id, payload.to_vec()));
                100 * log.len() as i64
            }),
            2
        );
        assert_eq!(log, [(base + 1, b"a".to_vec()), (base + 2, b"b".to_vec())]);
        assert_eq!(t.poll(|_, _| panic!("nothing left to append")), 0);
        t.sweep_leader_messages(150);
        assert_eq!((t.len(), t.log_service_session_id), (1, base + 1));
        t.restore_uncommitted_messages(150);
        let mut again = Vec::new();
        t.poll(|id, _| {
            again.push(id);
            300
        });
        assert_eq!(again, [base + 2], "an uncommitted append goes again");
    }

    #[test]
    fn a_follower_drops_what_the_log_has_and_skips_what_it_had() {
        let mut t = PendingServiceMessages::new(0);
        let base = service_session_id(0, i64::MIN);
        t.enqueue(&message(b"a"));
        t.enqueue(&message(b"b"));
        t.sweep_follower_messages(base + 1);
        assert_eq!(t.len(), 1);
        t.verify().unwrap();
        let mut late = PendingServiceMessages::new(0);
        late.sweep_follower_messages(base + 2);
        late.enqueue(&message(b"a"));
        late.enqueue(&message(b"b"));
        assert!(late.is_empty(), "a service behind the log sends what the log has");
        assert_eq!(late.next_service_session_id, base + 3);
    }

    #[test]
    fn a_snapshot_with_a_gap_is_refused() {
        let mut t = PendingServiceMessages::new(0);
        t.load_state(10, 7);
        let mut m = message(b"x");
        put_i64(&mut m, CLUSTER_SESSION_ID_OFFSET, 9);
        t.append_message(&m);
        assert!(t.verify().is_err());
        assert_eq!(t.size(), 48);
    }
}
