//! The cluster's log as the consensus module writes and reads it: the
//! leader appends client and service messages, timers, sessions opened and
//! closed, cluster actions and new terms (Java's `LogPublisher`); every
//! member reads them back as it replays its log or follows the leader's
//! (Java's `LogAdapter`), after reassembly.
//!
//! A cluster action and a new term carry the log position just after
//! themselves: [`position_after`] of the publication's position, as Java
//! computes it before claiming the frame.

use rusteron_cluster_codecs::cluster::{
    close_reason::CloseReason, cluster_action::ClusterAction, cluster_action_request_codec,
    cluster_time_unit::ClusterTimeUnit, new_leadership_term_event_codec, session_close_event_codec,
    session_message_header_codec, session_open_event_codec, timer_event_codec,
};
use rusteron_cluster_core::position::{FRAME_ALIGNMENT, HEADER_LENGTH as FRAME_HEADER_LENGTH, align};

use crate::codec::{self, HEADER_LENGTH, SCHEMA_ID, encode};
use crate::io::{Ports, offer_result};

/// A client or service message's header before its payload: SBE header and block.
pub const SESSION_HEADER_LENGTH: usize = 32;
/// A cluster action's flags when none were sent.
pub const CLUSTER_ACTION_FLAGS_DEFAULT: i32 = 0;
/// The flag of a cluster action that asks standbys for a snapshot.
pub const CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT: i32 = 1;

/// How many times the log is offered a message before it is refused: Java's.
const SEND_ATTEMPTS: usize = 3;

/// Appends a message to the log, in parts as given, offered up to three
/// times: the position after it, `None` when the log refused it (back
/// pressure, not connected), an error when the log publication is closed or
/// full, as Java's `LogPublisher` throws.
pub fn append(ports: &mut impl Ports, parts: &[&[u8]]) -> Result<Option<i64>, String> {
    for _ in 0..SEND_ATTEMPTS {
        let position = ports.offer_log(parts);
        if position > 0 {
            return Ok(Some(position));
        }
        match position {
            offer_result::CLOSED => return Err("log publication is closed".into()),
            offer_result::MAX_POSITION_EXCEEDED => return Err("log publication at max position".into()),
            _ => {}
        }
    }
    Ok(None)
}

/// The log's position after an unfragmented message of `length` bytes
/// appended at `position`.
pub fn position_after(position: i64, length: usize) -> i64 {
    position
        + align(
            i64::from(FRAME_HEADER_LENGTH) + length as i64,
            i64::from(FRAME_ALIGNMENT),
        )
}

/// As [`position_after`], on a log of `term_length`-byte terms: a message
/// that does not fit in the rest of its term goes after a padding frame, at
/// the start of the next, as an exclusive publication appends it (Java
/// 1.54.1 takes the position from its claim). A `term_length` of 0, not
/// known, is [`position_after`].
pub fn position_after_in_term(position: i64, term_length: i32, length: usize) -> i64 {
    let aligned = align(
        i64::from(FRAME_HEADER_LENGTH) + length as i64,
        i64::from(FRAME_ALIGNMENT),
    );
    if term_length <= 0 {
        return position + aligned;
    }
    let term_length = i64::from(term_length);
    let term_offset = position & (term_length - 1);
    if term_offset + aligned > term_length {
        position + (term_length - term_offset) + aligned
    } else {
        position + aligned
    }
}

/// A client's or service's message: header, then the payload as given.
pub fn session_message_header(leadership_term_id: i64, cluster_session_id: i64, timestamp: i64) -> Vec<u8> {
    use session_message_header_codec as c;
    encode!(c::SessionMessageHeaderEncoder, 0, |e| {
        e.leadership_term_id(leadership_term_id)
            .cluster_session_id(cluster_session_id)
            .timestamp(timestamp);
    })
}

/// A session opened: what every member's services learn of the client.
#[allow(clippy::too_many_arguments)]
pub fn session_open(
    leadership_term_id: i64,
    cluster_session_id: i64,
    correlation_id: i64,
    timestamp: i64,
    response_stream_id: i32,
    response_channel: &str,
    encoded_principal: &[u8],
) -> Vec<u8> {
    use session_open_event_codec as c;
    encode!(
        c::SessionOpenEventEncoder,
        8 + response_channel.len() + encoded_principal.len(),
        |e| {
            e.leadership_term_id(leadership_term_id)
                .cluster_session_id(cluster_session_id)
                .correlation_id(correlation_id)
                .timestamp(timestamp)
                .response_stream_id(response_stream_id)
                .response_channel(response_channel.as_bytes())
                .encoded_principal(encoded_principal);
        }
    )
}

pub fn session_close(
    leadership_term_id: i64,
    cluster_session_id: i64,
    timestamp: i64,
    close_reason: CloseReason,
) -> Vec<u8> {
    use session_close_event_codec as c;
    encode!(c::SessionCloseEventEncoder, 0, |e| {
        e.leadership_term_id(leadership_term_id)
            .cluster_session_id(cluster_session_id)
            .timestamp(timestamp)
            .close_reason(close_reason);
    })
}

pub fn timer_event(leadership_term_id: i64, correlation_id: i64, timestamp: i64) -> Vec<u8> {
    use timer_event_codec as c;
    encode!(c::TimerEventEncoder, 0, |e| {
        e.leadership_term_id(leadership_term_id)
            .correlation_id(correlation_id)
            .timestamp(timestamp);
    })
}

/// The length of a cluster action, for [`position_after`].
pub const CLUSTER_ACTION_LENGTH: usize = HEADER_LENGTH + cluster_action_request_codec::SBE_BLOCK_LENGTH as usize;

pub fn cluster_action(
    leadership_term_id: i64,
    term_base_log_position: i64,
    log_position: i64,
    timestamp: i64,
    action: ClusterAction,
    flags: i32,
) -> Vec<u8> {
    use cluster_action_request_codec as c;
    encode!(c::ClusterActionRequestEncoder, 0, |e| {
        e.leadership_term_id(leadership_term_id)
            .log_position(log_position)
            .timestamp(timestamp)
            .action(action)
            .flags(flags)
            .term_base_log_position(term_base_log_position);
    })
}

/// The length of a new term's event, for [`position_after`].
pub const NEW_LEADERSHIP_TERM_EVENT_LENGTH: usize =
    HEADER_LENGTH + new_leadership_term_event_codec::SBE_BLOCK_LENGTH as usize;

#[allow(clippy::too_many_arguments)]
pub fn new_leadership_term_event(
    leadership_term_id: i64,
    log_position: i64,
    timestamp: i64,
    term_base_log_position: i64,
    leader_member_id: i32,
    log_session_id: i32,
    time_unit: ClusterTimeUnit,
    app_version: i32,
) -> Vec<u8> {
    use new_leadership_term_event_codec as c;
    encode!(c::NewLeadershipTermEventEncoder, 0, |e| {
        e.leadership_term_id(leadership_term_id)
            .log_position(log_position)
            .timestamp(timestamp)
            .term_base_log_position(term_base_log_position)
            .leader_member_id(leader_member_id)
            .log_session_id(log_session_id)
            .time_unit(time_unit)
            .app_version(app_version);
    })
}

/// A message of the log, as the consensus module reads it back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogMessage {
    SessionMessage {
        cluster_session_id: i64,
        timestamp: i64,
    },
    TimerEvent {
        correlation_id: i64,
    },
    SessionOpen {
        correlation_id: i64,
        cluster_session_id: i64,
        timestamp: i64,
        response_stream_id: i32,
        response_channel: String,
    },
    SessionClose {
        cluster_session_id: i64,
        close_reason: CloseReason,
    },
    ClusterAction {
        leadership_term_id: i64,
        log_position: i64,
        timestamp: i64,
        action: ClusterAction,
        flags: i32,
    },
    NewLeadershipTerm {
        leadership_term_id: i64,
        log_position: i64,
        timestamp: i64,
        term_base_log_position: i64,
        time_unit: ClusterTimeUnit,
        app_version: i32,
    },
    /// Another schema's message: an extension's.
    Extension,
    /// A template the module does not act on (a membership change, say).
    Ignored,
}

/// Decodes a whole message of the log, its lengths checked first.
pub fn decode(message: &[u8]) -> Result<LogMessage, String> {
    let h = codec::header(message)?;
    if h.schema_id != SCHEMA_ID {
        return Ok(LogMessage::Extension);
    }
    let header = || codec::header_decoder(message);
    Ok(match h.template_id {
        session_message_header_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 24)?;
            let d = session_message_header_codec::SessionMessageHeaderDecoder::default().header(header(), 0);
            LogMessage::SessionMessage {
                cluster_session_id: d.cluster_session_id(),
                timestamp: d.timestamp(),
            }
        }
        timer_event_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 24)?;
            let d = timer_event_codec::TimerEventDecoder::default().header(header(), 0);
            LogMessage::TimerEvent {
                correlation_id: d.correlation_id(),
            }
        }
        session_open_event_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 36)?;
            let mut d = session_open_event_codec::SessionOpenEventDecoder::default().header(header(), 0);
            codec::check_var(message, rusteron_cluster_codecs::cluster::Decoder::get_limit(&d))?;
            let c = d.response_channel_decoder();
            let response_channel = codec::ascii(d.response_channel_slice(c));
            LogMessage::SessionOpen {
                correlation_id: d.correlation_id(),
                cluster_session_id: d.cluster_session_id(),
                timestamp: d.timestamp(),
                response_stream_id: d.response_stream_id(),
                response_channel,
            }
        }
        session_close_event_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 28)?;
            let d = session_close_event_codec::SessionCloseEventDecoder::default().header(header(), 0);
            LogMessage::SessionClose {
                cluster_session_id: d.cluster_session_id(),
                close_reason: d.close_reason(),
            }
        }
        cluster_action_request_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 28)?;
            let d = cluster_action_request_codec::ClusterActionRequestDecoder::default().header(header(), 0);
            LogMessage::ClusterAction {
                leadership_term_id: d.leadership_term_id(),
                log_position: d.log_position(),
                timestamp: d.timestamp(),
                action: d.action(),
                flags: d.flags().unwrap_or(CLUSTER_ACTION_FLAGS_DEFAULT),
            }
        }
        new_leadership_term_event_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 48)?;
            let d = new_leadership_term_event_codec::NewLeadershipTermEventDecoder::default().header(header(), 0);
            LogMessage::NewLeadershipTerm {
                leadership_term_id: d.leadership_term_id(),
                log_position: d.log_position(),
                timestamp: d.timestamp(),
                term_base_log_position: d.term_base_log_position(),
                time_unit: d.time_unit(),
                app_version: d.app_version().unwrap_or(0),
            }
        }
        _ => LogMessage::Ignored,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_is_appended_reads_back() {
        let open = session_open(3, 7, 99, 1_000, 102, "aeron:udp?endpoint=c:1", b"p");
        assert_eq!(
            decode(&open).unwrap(),
            LogMessage::SessionOpen {
                correlation_id: 99,
                cluster_session_id: 7,
                timestamp: 1_000,
                response_stream_id: 102,
                response_channel: "aeron:udp?endpoint=c:1".into(),
            }
        );
        let action = cluster_action(
            3,
            0,
            position_after(1_024, CLUSTER_ACTION_LENGTH),
            5,
            ClusterAction::SNAPSHOT,
            0,
        );
        assert_eq!(action.len(), CLUSTER_ACTION_LENGTH);
        let LogMessage::ClusterAction { log_position, .. } = decode(&action).unwrap() else {
            panic!("an action");
        };
        assert_eq!(log_position, 1_024 + 96, "a 48-byte message takes a 96-byte frame");
        let term = new_leadership_term_event(4, 1_120, 6, 0, 1, -5, ClusterTimeUnit::MILLIS, 1);
        assert_eq!(term.len(), NEW_LEADERSHIP_TERM_EVENT_LENGTH);
        assert!(matches!(
            decode(&term).unwrap(),
            LogMessage::NewLeadershipTerm {
                leadership_term_id: 4,
                ..
            }
        ));
        assert_eq!(session_message_header(1, 2, 3).len(), SESSION_HEADER_LENGTH);
        assert!(matches!(
            decode(&session_close(1, 7, 3, CloseReason::TIMEOUT)).unwrap(),
            LogMessage::SessionClose {
                close_reason: CloseReason::TIMEOUT,
                ..
            }
        ));
        assert!(matches!(
            decode(&timer_event(1, 42, 3)).unwrap(),
            LogMessage::TimerEvent { correlation_id: 42 }
        ));
    }

    #[test]
    fn a_short_message_is_refused() {
        let open = session_open(3, 7, 99, 1_000, 102, "aeron:udp?endpoint=c:1", b"");
        // Cut into the response channel: the principal after it is not read.
        assert!(decode(&open[..open.len() - 10]).is_err());
        assert!(decode(&open[..10]).is_err());
    }

    #[test]
    fn an_action_that_does_not_fit_its_term_lands_after_the_padding() {
        let term = 64 * 1024;
        // Room for it: as without terms.
        assert_eq!(position_after_in_term(1_024, term, CLUSTER_ACTION_LENGTH), 1_024 + 96);
        // It ends the term exactly: still this term.
        let end = i64::from(term) - 96;
        assert_eq!(
            position_after_in_term(end, term, CLUSTER_ACTION_LENGTH),
            i64::from(term)
        );
        // 64 bytes left: a padding frame, then the action at the next term's start.
        let late = 3 * i64::from(term) - 64;
        assert_eq!(
            position_after_in_term(late, term, CLUSTER_ACTION_LENGTH),
            3 * i64::from(term) + 96
        );
        // The term length not known: as before.
        assert_eq!(position_after_in_term(late, 0, CLUSTER_ACTION_LENGTH), late + 96);
    }
}
