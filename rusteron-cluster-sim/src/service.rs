//! A member's clustered service, as the consensus module sees it: it starts
//! from its snapshot, joins the logs it is told to, applies the log up to the
//! commit position, takes a snapshot at each snapshot action, and acks each
//! as Java's container does. What it applies is kept, by position, so the
//! world can check members agree.

use std::collections::BTreeMap;

use rusteron_cluster_codecs::cluster::{
    WriteBuf, boolean_type::BooleanType, cluster_action::ClusterAction, join_log_codec, request_service_ack_codec,
    service_ack_codec, service_termination_position_codec,
};
use rusteron_cluster_core::counters::RecoveryState;
use rusteron_consensus_core::codec;
use rusteron_consensus_core::log::{self, LogMessage};

use crate::archive::SimArchive;

/// The log's stream.
pub const LOG_STREAM_ID: i32 = 100;
/// The stream a service's snapshot recordings stand on.
const SERVICE_SNAPSHOT_STREAM_ID: i32 = 106;

/// An entry the service applied: where it starts, and its bytes' hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Applied {
    pub start: i64,
    pub hash: u64,
}

/// A service's state at a snapshot: what it had applied.
#[derive(Clone, Debug, Default)]
pub struct ServiceSnapshot {
    pub applied: BTreeMap<i64, Applied>,
}

/// FNV-1a, 64 bits.
pub fn hash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

pub struct SimService {
    pub service_id: i32,
    /// Its Aeron client's id, as its first ack names it.
    pub client_id: i64,
    next_ack_id: i64,
    started: bool,
    /// Entries applied, by end position.
    pub applied: BTreeMap<i64, Applied>,
    /// Where it applies next.
    pub position: i64,
    /// The log joined, read up to this position at most.
    joined: Option<i64>,
    /// The joined log's session.
    joined_session: Option<i32>,
    /// A join told of, taken once the log before is read to where it
    /// begins, as the container's is: (position, max position, session).
    pending_join: Option<(i64, i64, i32)>,
    /// An ack the module asked for, once the log reaches this position.
    requested_ack: Option<i64>,
    termination: Option<(i64, bool)>,
    pub terminated: bool,
    /// The client messages applied since last asked: (message id, end position).
    pub client_messages: Vec<(u64, i64)>,
}

fn ack(service_id: i32, ack_id: i64, log_position: i64, relevant_id: i64) -> Vec<u8> {
    let mut out = vec![0u8; codec::HEADER_LENGTH + service_ack_codec::SBE_BLOCK_LENGTH as usize];
    service_ack_codec::ServiceAckEncoder::default()
        .wrap(WriteBuf::new(&mut out), codec::HEADER_LENGTH)
        .header(0)
        .parent()
        .expect("an encoder")
        .log_position(log_position)
        .timestamp(0)
        .ack_id(ack_id)
        .relevant_id(relevant_id)
        .service_id(service_id);
    out
}

/// A client message's payload in the simulation: a tag and its id.
pub const CLIENT_MESSAGE_TAG: u64 = 0x5349_4d2d_4d53_4721;

impl SimService {
    pub fn new(service_id: i32, client_id: i64) -> Self {
        Self {
            service_id,
            client_id,
            next_ack_id: 0,
            started: false,
            applied: BTreeMap::new(),
            position: 0,
            joined: None,
            joined_session: None,
            pending_join: None,
            requested_ack: None,
            termination: None,
            terminated: false,
            client_messages: Vec::new(),
        }
    }

    /// The log's image gone, as a replay stopped before its end takes it:
    /// read no further.
    pub fn cut_log(&mut self, session_id: i32) {
        if self.joined_session == Some(session_id) {
            self.joined = None;
            self.joined_session = None;
        }
    }

    fn ack(&mut self, log_position: i64, relevant_id: i64) -> Vec<u8> {
        let ack_id = self.next_ack_id;
        self.next_ack_id += 1;
        ack(self.service_id, ack_id, log_position, relevant_id)
    }

    /// One step: what the module sent handled, the log applied up to
    /// `commit_position`, `rate` entries at most; the service's messages to
    /// the module returned.
    #[allow(clippy::too_many_arguments)]
    pub fn step(
        &mut self,
        rate: usize,
        to_services: &[Vec<u8>],
        recovery_key: Option<&[u8]>,
        archive: &mut SimArchive,
        snapshots: &mut BTreeMap<i64, ServiceSnapshot>,
        commit_position: i64,
        member_id: i32,
        violations: &mut Vec<String>,
    ) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        if self.terminated {
            return out;
        }
        if !self.started {
            // A container starts once the recovery state counter is up.
            let Some(state) = recovery_key.and_then(|key| RecoveryState::parse(key).ok()) else {
                return out;
            };
            self.started = true;
            if let Some(&recording_id) = state.snapshot_recording_ids.get(self.service_id as usize) {
                if let Some(snapshot) = snapshots.get(&recording_id) {
                    self.applied = snapshot.applied.clone();
                } else {
                    violations.push(format!(
                        "member {member_id} service {}: snapshot recording {recording_id} unknown",
                        self.service_id
                    ));
                }
            }
            self.position = state.log_position;
            let client_id = self.client_id;
            out.push(self.ack(state.log_position, client_id));
        }
        for message in to_services {
            let Ok(h) = codec::header(message) else {
                continue;
            };
            let header = || codec::header_decoder(message);
            match h.template_id {
                join_log_codec::SBE_TEMPLATE_ID => {
                    let d = join_log_codec::JoinLogDecoder::default().header(header(), 0);
                    self.pending_join = Some((d.log_position(), d.max_log_position(), d.log_session_id()));
                }
                request_service_ack_codec::SBE_TEMPLATE_ID => {
                    let d = request_service_ack_codec::RequestServiceAckDecoder::default().header(header(), 0);
                    self.requested_ack = Some(d.log_position());
                }
                service_termination_position_codec::SBE_TEMPLATE_ID => {
                    let d = service_termination_position_codec::ServiceTerminationPositionDecoder::default()
                        .header(header(), 0);
                    self.termination = Some((d.log_position(), d.ack_required() == BooleanType::TRUE));
                }
                _ => {}
            }
        }
        if let Some((position, max, session)) = self.pending_join {
            if self.joined.is_none() || self.position >= position {
                self.pending_join = None;
                if position != self.position {
                    // The container's "Cluster log must be contiguous".
                    violations.push(format!(
                        "member {member_id} service {}: told to join the log at {position}, at {}",
                        self.service_id, self.position
                    ));
                    self.position = position;
                }
                self.joined = Some(max);
                self.joined_session = Some(session);
                let client_id = self.client_id;
                out.push(self.ack(position, client_id));
            } else if let Some(joined) = self.joined.as_mut() {
                *joined = (*joined).min(position);
            }
        }
        if let Some(max) = self.joined {
            let limit = commit_position.min(max);
            for _ in 0..rate {
                if self.position >= limit {
                    break;
                }
                let Some(m) = log_message_at(archive, self.position) else {
                    break;
                };
                if m.end > limit {
                    break;
                }
                let (start, end) = (m.start, m.end);
                self.applied.insert(
                    end,
                    Applied {
                        start,
                        hash: hash(&m.bytes),
                    },
                );
                self.position = end;
                match log::decode(&m.bytes) {
                    Ok(LogMessage::SessionMessage { .. }) => {
                        let payload = &m.bytes[log::SESSION_HEADER_LENGTH.min(m.bytes.len())..];
                        if payload.len() >= 16
                            && u64::from_le_bytes(payload[..8].try_into().expect("8 bytes")) == CLIENT_MESSAGE_TAG
                        {
                            let id = u64::from_le_bytes(payload[8..16].try_into().expect("8 bytes"));
                            self.client_messages.push((id, end));
                        }
                    }
                    Ok(LogMessage::ClusterAction {
                        action: ClusterAction::SNAPSHOT,
                        log_position,
                        flags,
                        ..
                    }) if flags == log::CLUSTER_ACTION_FLAGS_DEFAULT => {
                        let recording_id = archive.new_stopped_recording(SERVICE_SNAPSHOT_STREAM_ID);
                        snapshots.insert(
                            recording_id,
                            ServiceSnapshot {
                                applied: self.applied.clone(),
                            },
                        );
                        out.push(self.ack(log_position, recording_id));
                    }
                    _ => {}
                }
            }
            if self.position >= max {
                self.joined = None;
                self.joined_session = None;
            }
        }
        if let Some(position) = self.requested_ack
            && self.position >= position
        {
            self.requested_ack = None;
            let at = self.position;
            out.push(self.ack(at, -1));
        }
        if let Some((position, ack_required)) = self.termination
            && self.position >= position
        {
            self.termination = None;
            self.terminated = true;
            if ack_required {
                out.push(self.ack(position, -1));
            }
        }
        out
    }
}

/// The log's message at `position` in the member's archive: the latest log
/// recording that holds it.
fn log_message_at(archive: &SimArchive, position: i64) -> Option<crate::archive::Message> {
    archive
        .recordings
        .values()
        .rev()
        .filter(|r| r.stream_id == LOG_STREAM_ID)
        .find_map(|r| r.message_at(position).cloned())
}
