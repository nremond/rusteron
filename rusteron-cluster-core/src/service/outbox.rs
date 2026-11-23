//! What the container sends: to the consensus module, in order and never
//! dropped; to clients, straight into their publications as soon as the
//! callback that answered returns, and through a queue per session only for
//! what a publication refused.

use std::collections::{BTreeMap, VecDeque};

use rusteron_cluster_codecs::cluster::{
    WriteBuf, cancel_timer_codec::CancelTimerEncoder, close_session_codec::CloseSessionEncoder, message_header_codec,
    schedule_timer_codec::ScheduleTimerEncoder, service_ack_codec::ServiceAckEncoder,
    session_message_header_codec::SessionMessageHeaderEncoder,
};

use super::api::CxError;

const HEADER: usize = message_header_codec::ENCODED_LENGTH;
/// A session message's header before its payload: SBE's 8 bytes and a block of 24.
pub const SESSION_HEADER_LENGTH: usize = HEADER + 24;

/// What became of an offer to a publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Offer {
    Sent,
    /// Back-pressured, or the driver was busy: offer it again.
    Retry,
    /// No subscriber yet, or none any more.
    NotConnected,
    /// The publication is closed, or at its maximum position: it takes nothing more.
    Closed,
}

/// Where clients' answers go: a session's publication, offered a message as
/// its header and its payload, as Aeron's two-part offer takes them.
pub trait EgressSink {
    fn offer(&mut self, session_id: i64, header: &[u8], payload: &[u8]) -> Offer;
}

impl<F: FnMut(i64, &[u8], &[u8]) -> Offer> EgressSink for F {
    fn offer(&mut self, session_id: i64, header: &[u8], payload: &[u8]) -> Offer {
        self(session_id, header, payload)
    }
}

#[derive(Debug)]
struct CmMessage {
    bytes: Vec<u8>,
    /// Offers left before it is given up on; `None` for never.
    attempts: Option<u32>,
}

#[derive(Debug)]
struct EgressFrame {
    bytes: Vec<u8>,
    queued_ns: i64,
}

/// An answer a callback gave: its payload is in the outbox's arena.
#[derive(Debug)]
struct StagedEgress {
    session_id: i64,
    leadership_term_id: i64,
    timestamp: i64,
    start: usize,
    end: usize,
}

/// The messages a callback produced are staged until it returns: kept if it
/// applied its entry, discarded if it asked for the entry again.
#[derive(Debug)]
pub(crate) struct Outbox {
    cm: VecDeque<CmMessage>,
    staged_cm: Vec<CmMessage>,
    egress: BTreeMap<i64, VecDeque<EgressFrame>>,
    staged_egress: Vec<StagedEgress>,
    /// The payloads of the staged answers, end to end: kept, and reused, so an
    /// answer costs no allocation.
    staged_bytes: Vec<u8>,
    egress_frames: usize,
    cm_max_message_length: usize,
    pub(crate) egress_dropped: u64,
}

fn encode(length: usize, f: impl FnOnce(&mut [u8])) -> Vec<u8> {
    let mut bytes = vec![0; length];
    f(&mut bytes);
    bytes
}

pub(crate) fn ack(log_position: i64, timestamp: i64, ack_id: i64, relevant_id: i64, service_id: i32) -> Vec<u8> {
    encode(HEADER + 36, |b| {
        let mut e = ServiceAckEncoder::default()
            .wrap(WriteBuf::new(b), HEADER)
            .header(0)
            .parent()
            .expect("parent");
        e.log_position(log_position)
            .timestamp(timestamp)
            .ack_id(ack_id)
            .relevant_id(relevant_id)
            .service_id(service_id);
    })
}

/// A session message's header, on the stack.
pub(crate) fn session_header(
    leadership_term_id: i64,
    cluster_session_id: i64,
    timestamp: i64,
) -> [u8; SESSION_HEADER_LENGTH] {
    let mut header = [0; SESSION_HEADER_LENGTH];
    let mut e = SessionMessageHeaderEncoder::default()
        .wrap(WriteBuf::new(&mut header), HEADER)
        .header(0)
        .parent()
        .expect("parent");
    e.leadership_term_id(leadership_term_id)
        .cluster_session_id(cluster_session_id)
        .timestamp(timestamp);
    header
}

pub(crate) fn session_message(
    leadership_term_id: i64,
    cluster_session_id: i64,
    timestamp: i64,
    payload: &[u8],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(SESSION_HEADER_LENGTH + payload.len());
    bytes.extend_from_slice(&session_header(leadership_term_id, cluster_session_id, timestamp));
    bytes.extend_from_slice(payload);
    bytes
}

impl Outbox {
    pub(crate) fn new(cm_max_message_length: usize) -> Self {
        Self {
            cm: VecDeque::new(),
            staged_cm: Vec::new(),
            egress: BTreeMap::new(),
            staged_egress: Vec::new(),
            staged_bytes: Vec::new(),
            egress_frames: 0,
            cm_max_message_length,
            egress_dropped: 0,
        }
    }

    /// An acknowledgement, after everything sent before it.
    pub(crate) fn push_ack(&mut self, bytes: Vec<u8>, attempts: Option<u32>) {
        self.cm.push_back(CmMessage { bytes, attempts });
    }

    pub(crate) fn stage_schedule_timer(&mut self, correlation_id: i64, deadline: i64) {
        let bytes = encode(HEADER + 16, |b| {
            let mut e = ScheduleTimerEncoder::default()
                .wrap(WriteBuf::new(b), HEADER)
                .header(0)
                .parent()
                .expect("parent");
            e.correlation_id(correlation_id).deadline(deadline);
        });
        self.staged_cm.push(CmMessage { bytes, attempts: None });
    }

    pub(crate) fn stage_cancel_timer(&mut self, correlation_id: i64) {
        let bytes = encode(HEADER + 8, |b| {
            let mut e = CancelTimerEncoder::default()
                .wrap(WriteBuf::new(b), HEADER)
                .header(0)
                .parent()
                .expect("parent");
            e.correlation_id(correlation_id);
        });
        self.staged_cm.push(CmMessage { bytes, attempts: None });
    }

    pub(crate) fn stage_close_session(&mut self, cluster_session_id: i64) {
        let bytes = encode(HEADER + 8, |b| {
            let mut e = CloseSessionEncoder::default()
                .wrap(WriteBuf::new(b), HEADER)
                .header(0)
                .parent()
                .expect("parent");
            e.cluster_session_id(cluster_session_id);
        });
        self.staged_cm.push(CmMessage { bytes, attempts: None });
    }

    pub(crate) fn stage_service_message(
        &mut self,
        service_id: i32,
        leadership_term_id: i64,
        timestamp: i64,
        payload: &[u8],
    ) -> Result<(), CxError> {
        let length = SESSION_HEADER_LENGTH + payload.len();
        if length > self.cm_max_message_length {
            return Err(CxError::MessageTooLong {
                length,
                max: self.cm_max_message_length,
            });
        }
        let bytes = session_message(leadership_term_id, service_id as i64, timestamp, payload);
        self.staged_cm.push(CmMessage { bytes, attempts: None });
        Ok(())
    }

    pub(crate) fn stage_egress(&mut self, session_id: i64, leadership_term_id: i64, timestamp: i64, payload: &[u8]) {
        let start = self.staged_bytes.len();
        self.staged_bytes.extend_from_slice(payload);
        self.staged_egress.push(StagedEgress {
            session_id,
            leadership_term_id,
            timestamp,
            start,
            end: self.staged_bytes.len(),
        });
    }

    /// Keeps what the last callback staged: its messages for the consensus
    /// module queued, its answers offered at once, each after whatever its
    /// session already has queued. An answer refused is queued, a copy of it;
    /// one for a closed publication is dropped, and reported.
    pub(crate) fn commit(&mut self, egress: &mut dyn EgressSink, now_ns: i64, mut report: impl FnMut(String)) {
        self.cm.extend(self.staged_cm.drain(..));
        for staged in self.staged_egress.drain(..) {
            let header = session_header(staged.leadership_term_id, staged.session_id, staged.timestamp);
            let payload = &self.staged_bytes[staged.start..staged.end];
            let queued = self.egress.get(&staged.session_id).is_some_and(|q| !q.is_empty());
            let offer = if queued {
                Offer::Retry
            } else {
                egress.offer(staged.session_id, &header, payload)
            };
            match offer {
                Offer::Sent => {}
                Offer::Retry | Offer::NotConnected => {
                    let mut bytes = Vec::with_capacity(SESSION_HEADER_LENGTH + payload.len());
                    bytes.extend_from_slice(&header);
                    bytes.extend_from_slice(payload);
                    let frame = EgressFrame {
                        bytes,
                        queued_ns: now_ns,
                    };
                    self.egress.entry(staged.session_id).or_default().push_back(frame);
                    self.egress_frames += 1;
                }
                Offer::Closed => {
                    self.egress_dropped += 1;
                    report(format!(
                        "egress to session {} closed: an answer dropped",
                        staged.session_id
                    ));
                }
            }
        }
        self.staged_bytes.clear();
    }

    /// Forgets what the last callback staged.
    pub(crate) fn discard(&mut self) {
        self.staged_cm.clear();
        self.staged_egress.clear();
        self.staged_bytes.clear();
    }

    pub(crate) fn cm_len(&self) -> usize {
        self.cm.len()
    }

    pub(crate) fn egress_len(&self) -> usize {
        self.egress_frames
    }

    /// Drops what is queued for a session: its publication is going.
    pub(crate) fn clear_egress(&mut self, session_id: i64) {
        if let Some(queue) = self.egress.remove(&session_id) {
            self.egress_frames -= queue.len();
        }
    }

    /// Offers the consensus module's messages in order until one is refused.
    /// A closed publication is an error: nothing more can reach it.
    pub(crate) fn drain_cm(&mut self, mut offer: impl FnMut(&[u8]) -> Offer) -> Result<usize, String> {
        let mut sent = 0;
        while let Some(message) = self.cm.front_mut() {
            match offer(&message.bytes) {
                Offer::Sent => {
                    self.cm.pop_front();
                    sent += 1;
                }
                Offer::Retry | Offer::NotConnected => {
                    if let Some(attempts) = message.attempts.as_mut() {
                        *attempts = attempts.saturating_sub(1);
                        if *attempts == 0 {
                            self.cm.pop_front();
                            continue;
                        }
                    }
                    break;
                }
                Offer::Closed => return Err("the consensus module's publication is closed".into()),
            }
        }
        Ok(sent)
    }

    /// Offers each session's frames in order until one is refused. A frame
    /// refused for longer than `timeout_ns` is dropped, and reported.
    pub(crate) fn drain_egress(
        &mut self,
        now_ns: i64,
        timeout_ns: i64,
        egress: &mut dyn EgressSink,
        mut report: impl FnMut(String),
    ) -> usize {
        let mut sent = 0;
        let mut closed = Vec::new();
        for (&session_id, queue) in &mut self.egress {
            while let Some(frame) = queue.front() {
                let (header, payload) = frame.bytes.split_at(SESSION_HEADER_LENGTH);
                match egress.offer(session_id, header, payload) {
                    Offer::Sent => {
                        queue.pop_front();
                        self.egress_frames -= 1;
                        sent += 1;
                    }
                    Offer::Retry | Offer::NotConnected => {
                        if now_ns.saturating_sub(frame.queued_ns) > timeout_ns {
                            queue.pop_front();
                            self.egress_frames -= 1;
                            self.egress_dropped += 1;
                            report(format!("egress to session {session_id} not taken in time: dropped"));
                            continue;
                        }
                        break;
                    }
                    Offer::Closed => {
                        closed.push(session_id);
                        break;
                    }
                }
            }
        }
        for session_id in closed {
            if let Some(queue) = self.egress.remove(&session_id) {
                self.egress_frames -= queue.len();
                self.egress_dropped += queue.len() as u64;
                report(format!(
                    "egress to session {session_id} closed: {} frames dropped",
                    queue.len()
                ));
            }
        }
        self.egress.retain(|_, queue| !queue.is_empty());
        sent
    }
}
