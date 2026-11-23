//! What tests of a service on the container need in place of a cluster: a
//! log writer that frames messages as Aeron's term appender does, the
//! consensus module's messages, and a fake consensus module that takes what
//! the container sends and holds it to Java's rules for acks.

use rusteron_cluster_codecs::cluster::{
    ReadBuf, WriteBuf, boolean_type::BooleanType, cancel_timer_codec::CancelTimerDecoder,
    close_reason::CloseReason as WireCloseReason, close_session_codec::CloseSessionDecoder,
    cluster_action::ClusterAction, cluster_action_request_codec::ClusterActionRequestEncoder,
    cluster_time_unit::ClusterTimeUnit, join_log_codec::JoinLogEncoder, message_header_codec,
    new_leadership_term_event_codec::NewLeadershipTermEventEncoder,
    request_service_ack_codec::RequestServiceAckEncoder, schedule_timer_codec::ScheduleTimerDecoder,
    service_ack_codec::ServiceAckDecoder, service_termination_position_codec::ServiceTerminationPositionEncoder,
    session_close_event_codec::SessionCloseEventEncoder, session_message_header_codec::SessionMessageHeaderDecoder,
    session_open_event_codec::SessionOpenEventEncoder, timer_event_codec::TimerEventEncoder,
};

use crate::log::Fragment;
use crate::position::{
    BEGIN_FRAGMENT, END_FRAGMENT, UNFRAGMENTED, aligned_frame_length, compute_position, fragment_lengths,
    max_payload_length, message_term_length, position_bits_to_shift,
};
use crate::service::{CloseReason, Role, SESSION_HEADER_LENGTH, outbox};

const HEADER: usize = message_header_codec::ENCODED_LENGTH;

/// A frame of a log, owned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub payload: Vec<u8>,
    pub flags: u8,
    pub term_offset: i32,
    pub position: i64,
}

impl Frame {
    pub fn fragment(&self) -> Fragment<'_> {
        Fragment {
            payload: &self.payload,
            flags: self.flags,
            term_offset: self.term_offset,
            position: self.position,
        }
    }
}

/// Appends messages to a log as frames, the way a publication does: split at
/// the MTU's payload, a message that does not fit what is left of a term
/// moving to the next one (behind a padding frame no subscriber sees).
#[derive(Clone, Debug)]
pub struct LogWriter {
    term_length: i32,
    max_payload: usize,
    initial_term_id: i32,
    term_id: i32,
    term_offset: i32,
    bits: u32,
}

impl LogWriter {
    pub fn new(term_length: i32, mtu: i32) -> Self {
        Self::at(term_length, mtu, 0)
    }

    /// A log that starts at `position`, a multiple of 32.
    pub fn at(term_length: i32, mtu: i32, position: i64) -> Self {
        let bits = position_bits_to_shift(term_length);
        Self {
            term_length,
            max_payload: max_payload_length(mtu) as usize,
            initial_term_id: 0,
            term_id: (position >> bits) as i32,
            term_offset: (position & (term_length as i64 - 1)) as i32,
            bits,
        }
    }

    pub fn position(&self) -> i64 {
        compute_position(self.term_id, self.term_offset, self.bits, self.initial_term_id)
    }

    /// The position just after a message of `length`, were it appended now.
    pub fn position_after(&self, length: usize) -> i64 {
        let required = message_term_length(length, self.max_payload);
        if self.term_offset + required > self.term_length {
            compute_position(self.term_id + 1, required, self.bits, self.initial_term_id)
        } else {
            self.position() + required as i64
        }
    }

    pub fn append(&mut self, message: &[u8]) -> Vec<Frame> {
        let required = message_term_length(message.len(), self.max_payload);
        if self.term_offset + required > self.term_length {
            self.term_id += 1;
            self.term_offset = 0;
        }
        let lengths = fragment_lengths(message.len(), self.max_payload);
        let last = lengths.len() - 1;
        let mut frames = Vec::with_capacity(lengths.len());
        let mut at = 0;
        for (i, length) in lengths.into_iter().enumerate() {
            let flags = match (i == 0, i == last) {
                (true, true) => UNFRAGMENTED,
                (true, false) => BEGIN_FRAGMENT,
                (false, true) => END_FRAGMENT,
                (false, false) => 0,
            };
            let term_offset = self.term_offset;
            self.term_offset += aligned_frame_length(length as i32);
            frames.push(Frame {
                payload: message[at..at + length].to_vec(),
                flags,
                term_offset,
                position: self.position(),
            });
            at += length;
        }
        frames
    }
}

fn encode(length: usize, f: impl FnOnce(&mut [u8])) -> Vec<u8> {
    let mut bytes = vec![0; length];
    f(&mut bytes);
    bytes
}

fn wire_close_reason(reason: CloseReason) -> WireCloseReason {
    match reason {
        CloseReason::ClientAction => WireCloseReason::CLIENT_ACTION,
        CloseReason::ServiceAction => WireCloseReason::SERVICE_ACTION,
        CloseReason::Timeout => WireCloseReason::TIMEOUT,
        CloseReason::Unknown => WireCloseReason::NullVal,
    }
}

/// The log's messages, as Java's consensus module appends them.
pub mod log {
    use super::*;

    pub fn session_message(
        leadership_term_id: i64,
        cluster_session_id: i64,
        timestamp: i64,
        payload: &[u8],
    ) -> Vec<u8> {
        outbox::session_message(leadership_term_id, cluster_session_id, timestamp, payload)
    }

    pub fn timer_event(leadership_term_id: i64, correlation_id: i64, timestamp: i64) -> Vec<u8> {
        encode(HEADER + 24, |b| {
            let mut e = TimerEventEncoder::default()
                .wrap(WriteBuf::new(b), HEADER)
                .header(0)
                .parent()
                .expect("parent");
            e.leadership_term_id(leadership_term_id)
                .correlation_id(correlation_id)
                .timestamp(timestamp);
        })
    }

    pub fn session_open(
        leadership_term_id: i64,
        cluster_session_id: i64,
        timestamp: i64,
        response_stream_id: i32,
        response_channel: &str,
    ) -> Vec<u8> {
        encode(HEADER + 36 + 4 + response_channel.len() + 4, |b| {
            let mut e = SessionOpenEventEncoder::default()
                .wrap(WriteBuf::new(b), HEADER)
                .header(0)
                .parent()
                .expect("parent");
            e.leadership_term_id(leadership_term_id)
                .correlation_id(cluster_session_id)
                .cluster_session_id(cluster_session_id)
                .timestamp(timestamp)
                .response_stream_id(response_stream_id)
                .response_channel(response_channel.as_bytes())
                .encoded_principal(&[]);
        })
    }

    pub fn session_close(
        leadership_term_id: i64,
        cluster_session_id: i64,
        timestamp: i64,
        reason: CloseReason,
    ) -> Vec<u8> {
        encode(HEADER + 28, |b| {
            let mut e = SessionCloseEventEncoder::default()
                .wrap(WriteBuf::new(b), HEADER)
                .header(0)
                .parent()
                .expect("parent");
            e.leadership_term_id(leadership_term_id)
                .cluster_session_id(cluster_session_id)
                .timestamp(timestamp)
                .close_reason(wire_close_reason(reason));
        })
    }

    /// A snapshot request at `log_position`: the position just after it.
    pub fn snapshot_request(leadership_term_id: i64, log_position: i64, timestamp: i64) -> Vec<u8> {
        encode(HEADER + 32, |b| {
            let mut e = ClusterActionRequestEncoder::default()
                .wrap(WriteBuf::new(b), HEADER)
                .header(0)
                .parent()
                .expect("parent");
            e.leadership_term_id(leadership_term_id)
                .log_position(log_position)
                .timestamp(timestamp)
                .action(ClusterAction::SNAPSHOT)
                .flags(0);
        })
    }

    /// The length of a snapshot request, to find its position before encoding it.
    pub const SNAPSHOT_REQUEST_LENGTH: usize = HEADER + 32;

    /// A new term at `log_position`: the position just after it.
    pub fn new_leadership_term(
        leadership_term_id: i64,
        log_position: i64,
        timestamp: i64,
        leader_member_id: i32,
        log_session_id: i32,
    ) -> Vec<u8> {
        encode(HEADER + 48, |b| {
            let mut e = NewLeadershipTermEventEncoder::default()
                .wrap(WriteBuf::new(b), HEADER)
                .header(0)
                .parent()
                .expect("parent");
            e.leadership_term_id(leadership_term_id)
                .log_position(log_position)
                .timestamp(timestamp)
                .term_base_log_position(0)
                .leader_member_id(leader_member_id)
                .log_session_id(log_session_id)
                .time_unit(ClusterTimeUnit::NANOS)
                .app_version(0);
        })
    }

    /// The length of a new-term event, to find its position before encoding it.
    pub const NEW_LEADERSHIP_TERM_LENGTH: usize = HEADER + 48;
}

/// The consensus module's messages to its services.
pub mod cm {
    use super::*;

    #[allow(clippy::too_many_arguments)]
    pub fn join_log(
        log_position: i64,
        max_log_position: i64,
        member_id: i32,
        log_session_id: i32,
        log_stream_id: i32,
        is_startup: bool,
        role: Role,
        log_channel: &str,
    ) -> Vec<u8> {
        let role = match role {
            Role::Follower => 0,
            Role::Candidate => 1,
            Role::Leader => 2,
        };
        let flag = |b: bool| if b { BooleanType::TRUE } else { BooleanType::FALSE };
        encode(HEADER + 40 + 4 + log_channel.len(), |b| {
            let mut e = JoinLogEncoder::default()
                .wrap(WriteBuf::new(b), HEADER)
                .header(0)
                .parent()
                .expect("parent");
            e.log_position(log_position)
                .max_log_position(max_log_position)
                .member_id(member_id)
                .log_session_id(log_session_id)
                .log_stream_id(log_stream_id)
                .is_startup(flag(is_startup))
                .role(role)
                .is_standby(BooleanType::FALSE)
                .log_channel(log_channel.as_bytes());
        })
    }

    pub fn service_termination_position(log_position: i64, ack_required: bool) -> Vec<u8> {
        encode(HEADER + 12, |b| {
            let mut e = ServiceTerminationPositionEncoder::default()
                .wrap(WriteBuf::new(b), HEADER)
                .header(0)
                .parent()
                .expect("parent");
            e.log_position(log_position).ack_required(if ack_required {
                BooleanType::TRUE
            } else {
                BooleanType::FALSE
            });
        })
    }

    pub fn request_service_ack(log_position: i64) -> Vec<u8> {
        encode(HEADER + 8, |b| {
            let mut e = RequestServiceAckEncoder::default()
                .wrap(WriteBuf::new(b), HEADER)
                .header(0)
                .parent()
                .expect("parent");
            e.log_position(log_position);
        })
    }
}

/// What a service sent its consensus module.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToCm {
    Ack {
        log_position: i64,
        timestamp: i64,
        ack_id: i64,
        relevant_id: i64,
        service_id: i32,
    },
    ScheduleTimer {
        correlation_id: i64,
        deadline: i64,
    },
    CancelTimer {
        correlation_id: i64,
    },
    CloseSession {
        cluster_session_id: i64,
    },
    ServiceMessage {
        cluster_session_id: i64,
        payload: Vec<u8>,
    },
}

/// Takes what a container sends, decodes it, and holds acks to the rule
/// Java's consensus module stops on: ack ids from 0, one after the other.
#[derive(Debug, Default)]
pub struct FakeCm {
    pub received: Vec<ToCm>,
    next_ack_id: i64,
}

impl FakeCm {
    pub fn receive(&mut self, bytes: &[u8]) -> Result<(), String> {
        let header = message_header_codec::MessageHeaderDecoder::default().wrap(ReadBuf::new(bytes), 0);
        let message = match header.template_id() {
            33 => {
                let d = ServiceAckDecoder::default().header(header, 0);
                if d.ack_id() != self.next_ack_id {
                    return Err(format!(
                        "ack out of sequence: expected ackId={} received {}",
                        self.next_ack_id,
                        d.ack_id()
                    ));
                }
                self.next_ack_id += 1;
                ToCm::Ack {
                    log_position: d.log_position(),
                    timestamp: d.timestamp(),
                    ack_id: d.ack_id(),
                    relevant_id: d.relevant_id(),
                    service_id: d.service_id(),
                }
            }
            31 => {
                let d = ScheduleTimerDecoder::default().header(header, 0);
                ToCm::ScheduleTimer {
                    correlation_id: d.correlation_id(),
                    deadline: d.deadline(),
                }
            }
            32 => ToCm::CancelTimer {
                correlation_id: CancelTimerDecoder::default().header(header, 0).correlation_id(),
            },
            30 => ToCm::CloseSession {
                cluster_session_id: CloseSessionDecoder::default().header(header, 0).cluster_session_id(),
            },
            1 => ToCm::ServiceMessage {
                cluster_session_id: SessionMessageHeaderDecoder::default()
                    .header(header, 0)
                    .cluster_session_id(),
                payload: bytes[SESSION_HEADER_LENGTH..].to_vec(),
            },
            other => return Err(format!("unexpected template {other} to the consensus module")),
        };
        self.received.push(message);
        Ok(())
    }

    /// The acks received, in order.
    pub fn acks(&self) -> Vec<&ToCm> {
        self.received.iter().filter(|m| matches!(m, ToCm::Ack { .. })).collect()
    }
}
