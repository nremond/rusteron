//! What members send each other on the consensus stream (Java's
//! `ConsensusAdapter` and `ConsensusPublisher`): positions canvassed, votes
//! asked and placed, new terms, append and commit positions, catch-up and
//! termination; and what backups and standbys send a member there, with the
//! member's answers.

use rusteron_cluster_codecs::cluster::{
    Decoder, append_position_codec, backup_query_codec, backup_response_codec, boolean_type::BooleanType,
    canvass_position_codec, catchup_position_codec, challenge_response_codec, commit_position_codec,
    heartbeat_request_codec, heartbeat_response_codec, new_leadership_term_codec, request_vote_codec,
    standby_snapshot_codec, stop_catchup_codec, termination_ack_codec, termination_position_codec, vote_codec,
};

use crate::codec::{self, SCHEMA_ID, encode};
use crate::io::{Offer, Ports};
use crate::members::NULL_VALUE;

/// `ConsensusModule.Configuration.PROTOCOL_SEMANTIC_VERSION`, 1.0.0.
pub const PROTOCOL_SEMANTIC_VERSION: i32 = 1 << 16;
/// An append position sent as the log is followed.
pub const APPEND_POSITION_FLAG_NONE: u8 = 0;
/// An append position sent as a follower catches up.
pub const APPEND_POSITION_FLAG_CATCHUP: u8 = 1;

/// How many times a member's publication is offered a message: Java's.
const SEND_ATTEMPTS: usize = 3;
/// A snapshot's block in a backup response or a standby's notice: recording,
/// term, term base, position, timestamp, service.
const SNAPSHOT_ENTRY_LENGTH: u16 = 44;

/// A snapshot a standby took, as its notice names it (Java's
/// `StandbySnapshotEntry`): the module's has service id -1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandbySnapshotEntry {
    pub recording_id: i64,
    pub leadership_term_id: i64,
    pub term_base_log_position: i64,
    pub log_position: i64,
    pub timestamp: i64,
    pub service_id: i32,
    /// The standby's archive, where the recording is.
    pub archive_endpoint: String,
}

/// A snapshot's recording, as a backup response lists it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackupSnapshot {
    pub recording_id: i64,
    pub leadership_term_id: i64,
    pub term_base_log_position: i64,
    pub log_position: i64,
    pub timestamp: i64,
    pub service_id: i32,
}

/// What a member tells a backup (Java's `ConsensusPublisher.backupResponse`):
/// its log's recording and last term, its commit position's counter, the
/// cluster, and the snapshots to start from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupResponse {
    pub correlation_id: i64,
    pub log_recording_id: i64,
    pub log_leadership_term_id: i64,
    pub log_term_base_log_position: i64,
    pub last_leadership_term_id: i64,
    pub last_term_base_log_position: i64,
    pub commit_position_counter_id: i32,
    pub leader_member_id: i32,
    pub member_id: i32,
    pub snapshots: Vec<BackupSnapshot>,
    pub cluster_members: String,
}

/// A leader's new term, as it tells a member (Java's `NewLeadershipTerm`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewLeadershipTerm {
    pub log_leadership_term_id: i64,
    pub next_leadership_term_id: i64,
    pub next_term_base_log_position: i64,
    pub next_log_position: i64,
    pub leadership_term_id: i64,
    pub term_base_log_position: i64,
    pub log_position: i64,
    pub commit_position: i64,
    pub leader_recording_id: i64,
    pub timestamp: i64,
    pub leader_member_id: i32,
    pub log_session_id: i32,
    pub app_version: i32,
    pub is_startup: bool,
}

/// A member's message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsensusMessage {
    CanvassPosition {
        log_leadership_term_id: i64,
        log_position: i64,
        leadership_term_id: i64,
        follower_member_id: i32,
        protocol_version: i32,
    },
    RequestVote {
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_term_id: i64,
        candidate_member_id: i32,
        protocol_version: i32,
    },
    Vote {
        candidate_term_id: i64,
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_member_id: i32,
        follower_member_id: i32,
        vote: bool,
    },
    NewLeadershipTerm(NewLeadershipTerm),
    AppendPosition {
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        flags: u8,
    },
    CommitPosition {
        leadership_term_id: i64,
        log_position: i64,
        leader_member_id: i32,
    },
    CatchupPosition {
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        catchup_endpoint: String,
    },
    StopCatchup {
        leadership_term_id: i64,
        follower_member_id: i32,
    },
    TerminationPosition {
        leadership_term_id: i64,
        log_position: i64,
    },
    TerminationAck {
        leadership_term_id: i64,
        log_position: i64,
        member_id: i32,
    },
    /// A backup's query for where to copy the cluster from (`log_position`
    /// [`NULL_VALUE`] when it names none).
    BackupQuery {
        correlation_id: i64,
        response_stream_id: i32,
        version: i32,
        log_position: i64,
        response_channel: String,
        encoded_credentials: Vec<u8>,
    },
    HeartbeatRequest {
        correlation_id: i64,
        response_stream_id: i32,
        response_channel: String,
        encoded_credentials: Vec<u8>,
    },
    /// A standby's notice of the snapshots it took at a position.
    StandbySnapshot {
        correlation_id: i64,
        version: i32,
        response_stream_id: i32,
        snapshots: Vec<StandbySnapshotEntry>,
        response_channel: String,
        encoded_credentials: Vec<u8>,
    },
    /// A backup's answer to a challenge, which the default authenticator
    /// never sends.
    ChallengeResponse {
        correlation_id: i64,
        cluster_session_id: i64,
    },
    /// Another schema's.
    Ignored,
}

/// A variable-length field's bytes, from its coordinates in a message whose
/// length was checked.
fn slice(message: &[u8], (offset, length): (usize, usize)) -> &[u8] {
    &message[offset..offset + length]
}

/// The credentials closing a backup's or standby's request, which a sender
/// may leave out: none then, as Java reads the zeros past the message.
fn credentials(message: &[u8], limit: usize) -> Result<Vec<u8>, String> {
    if limit >= message.len() {
        return Ok(Vec::new());
    }
    codec::check_var(message, limit)?;
    let length = u32::from_le_bytes(message[limit..limit + 4].try_into().expect("4 bytes")) as usize;
    Ok(message[limit + 4..limit + 4 + length].to_vec())
}

pub fn decode(message: &[u8]) -> Result<ConsensusMessage, String> {
    let h = codec::header(message)?;
    if h.schema_id != SCHEMA_ID {
        return Ok(ConsensusMessage::Ignored);
    }
    let header = || codec::header_decoder(message);
    Ok(match h.template_id {
        canvass_position_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 28)?;
            let d = canvass_position_codec::CanvassPositionDecoder::default().header(header(), 0);
            ConsensusMessage::CanvassPosition {
                log_leadership_term_id: d.log_leadership_term_id(),
                log_position: d.log_position(),
                leadership_term_id: d.leadership_term_id(),
                follower_member_id: d.follower_member_id(),
                protocol_version: if h.block_length >= 32 {
                    d.protocol_version().unwrap_or(0)
                } else {
                    0
                },
            }
        }
        request_vote_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 28)?;
            let d = request_vote_codec::RequestVoteDecoder::default().header(header(), 0);
            ConsensusMessage::RequestVote {
                log_leadership_term_id: d.log_leadership_term_id(),
                log_position: d.log_position(),
                candidate_term_id: d.candidate_term_id(),
                candidate_member_id: d.candidate_member_id(),
                protocol_version: if h.block_length >= 32 {
                    d.protocol_version().unwrap_or(0)
                } else {
                    0
                },
            }
        }
        vote_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 36)?;
            let d = vote_codec::VoteDecoder::default().header(header(), 0);
            ConsensusMessage::Vote {
                candidate_term_id: d.candidate_term_id(),
                log_leadership_term_id: d.log_leadership_term_id(),
                log_position: d.log_position(),
                candidate_member_id: d.candidate_member_id(),
                follower_member_id: d.follower_member_id(),
                vote: d.vote() == BooleanType::TRUE,
            }
        }
        new_leadership_term_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 96)?;
            let d = new_leadership_term_codec::NewLeadershipTermDecoder::default().header(header(), 0);
            ConsensusMessage::NewLeadershipTerm(NewLeadershipTerm {
                log_leadership_term_id: d.log_leadership_term_id(),
                next_leadership_term_id: d.next_leadership_term_id(),
                next_term_base_log_position: d.next_term_base_log_position(),
                next_log_position: d.next_log_position(),
                leadership_term_id: d.leadership_term_id(),
                term_base_log_position: d.term_base_log_position(),
                log_position: d.log_position(),
                commit_position: d.commit_position(),
                leader_recording_id: d.leader_recording_id(),
                timestamp: d.timestamp(),
                leader_member_id: d.leader_member_id(),
                log_session_id: d.log_session_id(),
                app_version: d.app_version().unwrap_or(0),
                is_startup: d.is_startup() == BooleanType::TRUE,
            })
        }
        append_position_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 20)?;
            let d = append_position_codec::AppendPositionDecoder::default().header(header(), 0);
            ConsensusMessage::AppendPosition {
                leadership_term_id: d.leadership_term_id(),
                log_position: d.log_position(),
                follower_member_id: d.follower_member_id(),
                flags: if h.block_length >= 21 { d.flags() } else { 0 },
            }
        }
        commit_position_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 20)?;
            let d = commit_position_codec::CommitPositionDecoder::default().header(header(), 0);
            ConsensusMessage::CommitPosition {
                leadership_term_id: d.leadership_term_id(),
                log_position: d.log_position(),
                leader_member_id: d.leader_member_id(),
            }
        }
        catchup_position_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 20)?;
            let mut d = catchup_position_codec::CatchupPositionDecoder::default().header(header(), 0);
            let (leadership_term_id, log_position, follower_member_id) =
                (d.leadership_term_id(), d.log_position(), d.follower_member_id());
            codec::check_var(message, d.get_limit())?;
            let e = d.catchup_endpoint_decoder();
            ConsensusMessage::CatchupPosition {
                leadership_term_id,
                log_position,
                follower_member_id,
                catchup_endpoint: codec::ascii(d.catchup_endpoint_slice(e)),
            }
        }
        stop_catchup_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 12)?;
            let d = stop_catchup_codec::StopCatchupDecoder::default().header(header(), 0);
            ConsensusMessage::StopCatchup {
                leadership_term_id: d.leadership_term_id(),
                follower_member_id: d.follower_member_id(),
            }
        }
        termination_position_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 16)?;
            let d = termination_position_codec::TerminationPositionDecoder::default().header(header(), 0);
            ConsensusMessage::TerminationPosition {
                leadership_term_id: d.leadership_term_id(),
                log_position: d.log_position(),
            }
        }
        termination_ack_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 20)?;
            let d = termination_ack_codec::TerminationAckDecoder::default().header(header(), 0);
            ConsensusMessage::TerminationAck {
                leadership_term_id: d.leadership_term_id(),
                log_position: d.log_position(),
                member_id: d.member_id(),
            }
        }
        backup_query_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 12)?;
            let mut d = backup_query_codec::BackupQueryDecoder::default().header(header(), 0);
            // `version` since schema 2 and `logPosition` since 16, absent from an older block.
            let version = if h.block_length >= 16 {
                d.version().unwrap_or(0)
            } else {
                0
            };
            let log_position = if h.block_length >= 24 {
                d.log_position().unwrap_or(NULL_VALUE)
            } else {
                NULL_VALUE
            };
            let (correlation_id, response_stream_id) = (d.correlation_id(), d.response_stream_id());
            codec::check_var(message, d.get_limit())?;
            let channel = d.response_channel_decoder();
            let credentials = credentials(message, d.get_limit())?;
            ConsensusMessage::BackupQuery {
                correlation_id,
                response_stream_id,
                version,
                log_position,
                response_channel: codec::ascii(slice(message, channel)),
                encoded_credentials: credentials,
            }
        }
        heartbeat_request_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 12)?;
            let mut d = heartbeat_request_codec::HeartbeatRequestDecoder::default().header(header(), 0);
            let (correlation_id, response_stream_id) = (d.correlation_id(), d.response_stream_id());
            codec::check_var(message, d.get_limit())?;
            let channel = d.response_channel_decoder();
            let credentials = credentials(message, d.get_limit())?;
            ConsensusMessage::HeartbeatRequest {
                correlation_id,
                response_stream_id,
                response_channel: codec::ascii(slice(message, channel)),
                encoded_credentials: credentials,
            }
        }
        standby_snapshot_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 16)?;
            let d = standby_snapshot_codec::StandbySnapshotDecoder::default().header(header(), 0);
            let (correlation_id, version, response_stream_id) =
                (d.correlation_id(), d.version(), d.response_stream_id());
            codec::check_group(message, d.get_limit(), SNAPSHOT_ENTRY_LENGTH)?;
            let mut g = d.snapshots_decoder();
            let mut snapshots = Vec::with_capacity(g.count() as usize);
            while g.advance().map_err(|e| format!("standby snapshot: {e:?}"))?.is_some() {
                if g.get_limit() > message.len() {
                    return Err("standby snapshot: an entry past the message's end".into());
                }
                let (recording_id, leadership_term_id, term_base_log_position, log_position, timestamp, service_id) = (
                    g.recording_id(),
                    g.leadership_term_id(),
                    g.term_base_log_position(),
                    g.log_position(),
                    g.timestamp(),
                    g.service_id(),
                );
                codec::check_var(message, g.get_limit())?;
                let endpoint = g.archive_endpoint_decoder();
                snapshots.push(StandbySnapshotEntry {
                    recording_id,
                    leadership_term_id,
                    term_base_log_position,
                    log_position,
                    timestamp,
                    service_id,
                    archive_endpoint: codec::ascii(slice(message, endpoint)),
                });
            }
            let mut d = g.parent().map_err(|e| format!("standby snapshot: {e:?}"))?;
            codec::check_var(message, d.get_limit())?;
            let channel = d.response_channel_decoder();
            let credentials = credentials(message, d.get_limit())?;
            ConsensusMessage::StandbySnapshot {
                correlation_id,
                version,
                response_stream_id,
                snapshots,
                response_channel: codec::ascii(slice(message, channel)),
                encoded_credentials: credentials,
            }
        }
        challenge_response_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 16)?;
            let d = challenge_response_codec::ChallengeResponseDecoder::default().header(header(), 0);
            ConsensusMessage::ChallengeResponse {
                correlation_id: d.correlation_id(),
                cluster_session_id: d.cluster_session_id(),
            }
        }
        _ => ConsensusMessage::Ignored,
    })
}

/// A backup response, as a backup reads it.
pub fn decode_backup_response(message: &[u8]) -> Result<BackupResponse, String> {
    let h = codec::header(message)?;
    if h.schema_id != SCHEMA_ID || h.template_id != backup_response_codec::SBE_TEMPLATE_ID {
        return Err(format!("not a backup response: template {}", h.template_id));
    }
    codec::needs(&h, 56)?;
    let d = backup_response_codec::BackupResponseDecoder::default().header(codec::header_decoder(message), 0);
    // `memberId` since schema 10, absent from an older block.
    let member_id = if h.block_length >= 60 {
        d.member_id()
    } else {
        NULL_VALUE as i32
    };
    let mut response = BackupResponse {
        correlation_id: d.correlation_id(),
        log_recording_id: d.log_recording_id(),
        log_leadership_term_id: d.log_leadership_term_id(),
        log_term_base_log_position: d.log_term_base_log_position(),
        last_leadership_term_id: d.last_leadership_term_id(),
        last_term_base_log_position: d.last_term_base_log_position(),
        commit_position_counter_id: d.commit_position_counter_id(),
        leader_member_id: d.leader_member_id(),
        member_id,
        snapshots: Vec::new(),
        cluster_members: String::new(),
    };
    codec::check_group(message, d.get_limit(), SNAPSHOT_ENTRY_LENGTH)?;
    let mut g = d.snapshots_decoder();
    while g.advance().map_err(|e| format!("backup response: {e:?}"))?.is_some() {
        if g.get_limit() > message.len() {
            return Err("backup response: an entry past the message's end".into());
        }
        response.snapshots.push(BackupSnapshot {
            recording_id: g.recording_id(),
            leadership_term_id: g.leadership_term_id(),
            term_base_log_position: g.term_base_log_position(),
            log_position: g.log_position(),
            timestamp: g.timestamp(),
            service_id: g.service_id(),
        });
    }
    let mut d = g.parent().map_err(|e| format!("backup response: {e:?}"))?;
    codec::check_var(message, d.get_limit())?;
    let members = d.cluster_members_decoder();
    response.cluster_members = codec::ascii(slice(message, members));
    Ok(response)
}

/// Offers a message to a member up to three times: whether it went; an
/// error when the publication is closed or full, as Java's `checkResult`
/// throws.
pub fn send(ports: &mut impl Ports, member_id: i32, message: &[u8]) -> Result<bool, String> {
    for _ in 0..SEND_ATTEMPTS {
        match ports.offer_consensus(member_id, message) {
            Offer::Sent => return Ok(true),
            Offer::Closed => return Err("publication is closed".into()),
            Offer::MaxPositionExceeded => return Err("publication at max position".into()),
            Offer::Retry | Offer::NotConnected => {}
        }
    }
    Ok(false)
}

pub fn canvass_position(
    log_leadership_term_id: i64,
    log_position: i64,
    leadership_term_id: i64,
    follower_member_id: i32,
) -> Vec<u8> {
    use canvass_position_codec as c;
    encode!(c::CanvassPositionEncoder, 0, |e| {
        e.log_leadership_term_id(log_leadership_term_id)
            .log_position(log_position)
            .leadership_term_id(leadership_term_id)
            .follower_member_id(follower_member_id)
            .protocol_version(PROTOCOL_SEMANTIC_VERSION);
    })
}

pub fn request_vote(
    log_leadership_term_id: i64,
    log_position: i64,
    candidate_term_id: i64,
    candidate_member_id: i32,
) -> Vec<u8> {
    use request_vote_codec as c;
    encode!(c::RequestVoteEncoder, 0, |e| {
        e.log_leadership_term_id(log_leadership_term_id)
            .log_position(log_position)
            .candidate_term_id(candidate_term_id)
            .candidate_member_id(candidate_member_id)
            .protocol_version(PROTOCOL_SEMANTIC_VERSION);
    })
}

pub fn vote(
    candidate_term_id: i64,
    log_leadership_term_id: i64,
    log_position: i64,
    candidate_member_id: i32,
    follower_member_id: i32,
    vote: bool,
) -> Vec<u8> {
    use vote_codec as c;
    encode!(c::VoteEncoder, 0, |e| {
        e.candidate_term_id(candidate_term_id)
            .log_leadership_term_id(log_leadership_term_id)
            .log_position(log_position)
            .candidate_member_id(candidate_member_id)
            .follower_member_id(follower_member_id)
            .vote(if vote { BooleanType::TRUE } else { BooleanType::FALSE });
    })
}

pub fn new_leadership_term(t: &NewLeadershipTerm) -> Vec<u8> {
    use new_leadership_term_codec as c;
    encode!(c::NewLeadershipTermEncoder, 0, |e| {
        e.log_leadership_term_id(t.log_leadership_term_id)
            .next_leadership_term_id(t.next_leadership_term_id)
            .next_term_base_log_position(t.next_term_base_log_position)
            .next_log_position(t.next_log_position)
            .leadership_term_id(t.leadership_term_id)
            .term_base_log_position(t.term_base_log_position)
            .log_position(t.log_position)
            .leader_recording_id(t.leader_recording_id)
            .timestamp(t.timestamp)
            .leader_member_id(t.leader_member_id)
            .log_session_id(t.log_session_id)
            .app_version(t.app_version)
            .is_startup(if t.is_startup {
                BooleanType::TRUE
            } else {
                BooleanType::FALSE
            })
            .commit_position(t.commit_position);
    })
}

pub fn append_position(leadership_term_id: i64, log_position: i64, follower_member_id: i32, flags: u8) -> Vec<u8> {
    use append_position_codec as c;
    encode!(c::AppendPositionEncoder, 0, |e| {
        e.leadership_term_id(leadership_term_id)
            .log_position(log_position)
            .follower_member_id(follower_member_id)
            .flags(flags);
    })
}

pub fn commit_position(leadership_term_id: i64, log_position: i64, leader_member_id: i32) -> Vec<u8> {
    use commit_position_codec as c;
    encode!(c::CommitPositionEncoder, 0, |e| {
        e.leadership_term_id(leadership_term_id)
            .log_position(log_position)
            .leader_member_id(leader_member_id);
    })
}

pub fn catchup_position(
    leadership_term_id: i64,
    log_position: i64,
    follower_member_id: i32,
    catchup_endpoint: &str,
) -> Vec<u8> {
    use catchup_position_codec as c;
    encode!(c::CatchupPositionEncoder, 4 + catchup_endpoint.len(), |e| {
        e.leadership_term_id(leadership_term_id)
            .log_position(log_position)
            .follower_member_id(follower_member_id)
            .catchup_endpoint(catchup_endpoint.as_bytes());
    })
}

pub fn stop_catchup(leadership_term_id: i64, follower_member_id: i32) -> Vec<u8> {
    use stop_catchup_codec as c;
    encode!(c::StopCatchupEncoder, 0, |e| {
        e.leadership_term_id(leadership_term_id)
            .follower_member_id(follower_member_id);
    })
}

pub fn termination_position(leadership_term_id: i64, log_position: i64) -> Vec<u8> {
    use termination_position_codec as c;
    encode!(c::TerminationPositionEncoder, 0, |e| {
        e.leadership_term_id(leadership_term_id).log_position(log_position);
    })
}

pub fn termination_ack(leadership_term_id: i64, log_position: i64, member_id: i32) -> Vec<u8> {
    use termination_ack_codec as c;
    encode!(c::TerminationAckEncoder, 0, |e| {
        e.leadership_term_id(leadership_term_id)
            .log_position(log_position)
            .member_id(member_id);
    })
}

pub fn backup_response(r: &BackupResponse) -> Vec<u8> {
    use backup_response_codec as c;
    let var = 4 + r.snapshots.len() * SNAPSHOT_ENTRY_LENGTH as usize + 4 + r.cluster_members.len();
    encode!(c::BackupResponseEncoder, var, |e| {
        e.correlation_id(r.correlation_id)
            .log_recording_id(r.log_recording_id)
            .log_leadership_term_id(r.log_leadership_term_id)
            .log_term_base_log_position(r.log_term_base_log_position)
            .last_leadership_term_id(r.last_leadership_term_id)
            .last_term_base_log_position(r.last_term_base_log_position)
            .commit_position_counter_id(r.commit_position_counter_id)
            .leader_member_id(r.leader_member_id)
            .member_id(r.member_id);
        let mut g = e.snapshots_encoder(r.snapshots.len() as u16, c::encoder::SnapshotsEncoder::default());
        for s in &r.snapshots {
            g.advance().expect("a snapshot");
            g.recording_id(s.recording_id)
                .leadership_term_id(s.leadership_term_id)
                .term_base_log_position(s.term_base_log_position)
                .log_position(s.log_position)
                .timestamp(s.timestamp)
                .service_id(s.service_id);
        }
        let mut e = g.parent().expect("the response");
        e.cluster_members(r.cluster_members.as_bytes());
    })
}

pub fn heartbeat_response(correlation_id: i64) -> Vec<u8> {
    use heartbeat_response_codec as c;
    encode!(c::HeartbeatResponseEncoder, 0, |e| {
        e.correlation_id(correlation_id);
    })
}

/// A backup's query, as Java's `ConsensusPublisher.backupQuery` sends it.
pub fn backup_query(
    correlation_id: i64,
    response_stream_id: i32,
    version: i32,
    log_position: i64,
    response_channel: &str,
    encoded_credentials: &[u8],
) -> Vec<u8> {
    use backup_query_codec as c;
    let var = 4 + response_channel.len() + 4 + encoded_credentials.len();
    encode!(c::BackupQueryEncoder, var, |e| {
        e.correlation_id(correlation_id)
            .response_stream_id(response_stream_id)
            .version(version)
            .log_position(log_position)
            .response_channel(response_channel.as_bytes())
            .encoded_credentials(encoded_credentials);
    })
}

pub fn heartbeat_request(correlation_id: i64, response_stream_id: i32, response_channel: &str) -> Vec<u8> {
    use heartbeat_request_codec as c;
    encode!(c::HeartbeatRequestEncoder, 4 + response_channel.len() + 4, |e| {
        e.correlation_id(correlation_id)
            .response_stream_id(response_stream_id)
            .response_channel(response_channel.as_bytes())
            .encoded_credentials(b"");
    })
}

/// A standby's notice, as Java's `ConsensusPublisher.standbySnapshotTaken`
/// sends it.
pub fn standby_snapshot(
    correlation_id: i64,
    version: i32,
    response_stream_id: i32,
    snapshots: &[StandbySnapshotEntry],
    response_channel: &str,
) -> Vec<u8> {
    use standby_snapshot_codec as c;
    let entries: usize = snapshots
        .iter()
        .map(|s| SNAPSHOT_ENTRY_LENGTH as usize + 4 + s.archive_endpoint.len())
        .sum();
    encode!(
        c::StandbySnapshotEncoder,
        4 + entries + 4 + response_channel.len() + 4,
        |e| {
            e.correlation_id(correlation_id)
                .version(version)
                .response_stream_id(response_stream_id);
            let mut g = e.snapshots_encoder(snapshots.len() as u16, c::encoder::SnapshotsEncoder::default());
            for s in snapshots {
                g.advance().expect("a snapshot");
                g.recording_id(s.recording_id)
                    .leadership_term_id(s.leadership_term_id)
                    .term_base_log_position(s.term_base_log_position)
                    .log_position(s.log_position)
                    .timestamp(s.timestamp)
                    .service_id(s.service_id)
                    .archive_endpoint(s.archive_endpoint.as_bytes());
            }
            let mut e = g.parent().expect("the notice");
            e.response_channel(response_channel.as_bytes()).encoded_credentials(b"");
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_decode_as_encoded() {
        let term = NewLeadershipTerm {
            log_leadership_term_id: 3,
            next_leadership_term_id: 4,
            next_term_base_log_position: 1024,
            next_log_position: -1,
            leadership_term_id: 5,
            term_base_log_position: 2048,
            log_position: 4096,
            commit_position: 3072,
            leader_recording_id: 7,
            timestamp: 99,
            leader_member_id: 2,
            log_session_id: -12,
            app_version: 1,
            is_startup: true,
        };
        assert_eq!(
            decode(&new_leadership_term(&term)),
            Ok(ConsensusMessage::NewLeadershipTerm(term))
        );
        assert_eq!(
            decode(&catchup_position(5, 4096, 1, "localhost:20113")),
            Ok(ConsensusMessage::CatchupPosition {
                leadership_term_id: 5,
                log_position: 4096,
                follower_member_id: 1,
                catchup_endpoint: "localhost:20113".into(),
            })
        );
        assert_eq!(
            decode(&vote(6, 5, 4096, 0, 1, true)),
            Ok(ConsensusMessage::Vote {
                candidate_term_id: 6,
                log_leadership_term_id: 5,
                log_position: 4096,
                candidate_member_id: 0,
                follower_member_id: 1,
                vote: true,
            })
        );
        assert_eq!(
            decode(&append_position(5, 8192, 2, APPEND_POSITION_FLAG_CATCHUP)),
            Ok(ConsensusMessage::AppendPosition {
                leadership_term_id: 5,
                log_position: 8192,
                follower_member_id: 2,
                flags: 1,
            })
        );
        assert!(
            decode(&canvass_position(1, 2, 3, 4)[..20]).is_err(),
            "a short block refused"
        );
    }
}
