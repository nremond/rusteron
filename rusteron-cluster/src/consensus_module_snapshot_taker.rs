//! The consensus module's own snapshot, as Java's
//! `ConsensusModuleSnapshotTaker` writes it and its
//! `ConsensusModuleSnapshotAdapter` reads it back: between a BEGIN and an
//! END marker of type 1, the module's state (the next session id, the first
//! service's message tracker), the open and closing sessions, the timers,
//! then each service's message tracker followed by its pending messages.

use crate::codecs::cluster::{
    close_reason::CloseReason, cluster_session_codec, cluster_time_unit::ClusterTimeUnit, consensus_module_codec,
    pending_message_tracker_codec, session_message_header_codec, snapshot_mark::SnapshotMark, snapshot_marker_codec,
    timer_codec,
};

use crate::codecs::message::{self, SCHEMA_ID, encode};
use crate::pending_service_message_tracker::PendingServiceMessages;
use crate::session_manager::ClusterSession;

/// The consensus module's snapshots' type in their markers.
pub const SNAPSHOT_TYPE_ID: i64 = 1;
const NULL_VALUE: i64 = -1;

pub fn marker(
    mark: SnapshotMark,
    log_position: i64,
    leadership_term_id: i64,
    term_base_log_position: i64,
    time_unit: ClusterTimeUnit,
    app_version: i32,
) -> Vec<u8> {
    use snapshot_marker_codec as c;
    encode!(c::SnapshotMarkerEncoder, 0, |e| {
        e.type_id(SNAPSHOT_TYPE_ID)
            .log_position(log_position)
            .leadership_term_id(leadership_term_id)
            .index(0)
            .mark(mark)
            .time_unit(time_unit)
            .app_version(app_version)
            .term_base_log_position(term_base_log_position);
    })
}

/// The module's state a snapshot holds.
pub struct State<'a, S> {
    pub log_position: i64,
    pub leadership_term_id: i64,
    /// Where the snapshot's term began (Java 1.54.1's markers carry it).
    pub term_base_log_position: i64,
    pub time_unit: ClusterTimeUnit,
    pub app_version: i32,
    pub next_committed_session_id: i64,
    pub sessions: S,
    /// (correlation id, deadline).
    pub timers: &'a [(i64, i64)],
    pub trackers: &'a [PendingServiceMessages],
}

/// The module's own state message: the next session id and the first
/// service's message tracker.
pub fn module_state(
    next_session_id: i64,
    next_service_session_id: i64,
    log_service_session_id: i64,
    pending_message_capacity: i32,
) -> Vec<u8> {
    use consensus_module_codec as c;
    encode!(c::ConsensusModuleEncoder, 0, |e| {
        e.next_session_id(next_session_id)
            .next_service_session_id(next_service_session_id)
            .log_service_session_id(log_service_session_id)
            .pending_message_capacity(pending_message_capacity);
    })
}

/// A session's message: Java writes no activity time.
pub fn session(
    cluster_session_id: i64,
    correlation_id: i64,
    opened_log_position: i64,
    close_reason: CloseReason,
    response_stream_id: i32,
    response_channel: &str,
) -> Vec<u8> {
    use cluster_session_codec as c;
    encode!(c::ClusterSessionEncoder, 4 + response_channel.len(), |e| {
        e.cluster_session_id(cluster_session_id)
            .correlation_id(correlation_id)
            .opened_log_position(opened_log_position)
            .time_of_last_activity(NULL_VALUE)
            .close_reason(close_reason)
            .response_stream_id(response_stream_id)
            .response_channel(response_channel.as_bytes());
    })
}

pub fn timer(correlation_id: i64, deadline: i64) -> Vec<u8> {
    use timer_codec as c;
    encode!(c::TimerEncoder, 0, |e| {
        e.correlation_id(correlation_id).deadline(deadline);
    })
}

/// A service's message tracker, its pending messages to follow it.
pub fn tracker(
    service_id: i32,
    next_service_session_id: i64,
    log_service_session_id: i64,
    pending_message_capacity: i32,
) -> Vec<u8> {
    use pending_message_tracker_codec as c;
    encode!(c::PendingMessageTrackerEncoder, 0, |e| {
        e.next_service_session_id(next_service_session_id)
            .log_service_session_id(log_service_session_id)
            .pending_message_capacity(pending_message_capacity)
            .service_id(service_id);
    })
}

/// The snapshot's messages, in order, as Java's `snapshotState` writes them;
/// and the errors Java's error handler would see (a follower that missed
/// service messages).
pub fn take<'a, S: Iterator<Item = &'a ClusterSession>>(state: State<'a, S>) -> (Vec<Vec<u8>>, Vec<String>) {
    let mut messages = vec![marker(
        SnapshotMark::BEGIN,
        state.log_position,
        state.leadership_term_id,
        state.term_base_log_position,
        state.time_unit,
        state.app_version,
    )];
    let mut errors = Vec::new();
    let (next_service, log_service, capacity) = state.trackers.first().map_or((0, 0, 0), |t| {
        (t.next_service_session_id, t.log_service_session_id, t.size() as i32)
    });
    messages.push(module_state(
        state.next_committed_session_id,
        next_service,
        log_service,
        capacity,
    ));
    for s in state.sessions {
        messages.push(session(
            s.id,
            s.correlation_id,
            s.opened_log_position,
            s.close_reason,
            s.response_stream_id,
            &s.response_channel,
        ));
    }
    for &(correlation_id, deadline) in state.timers {
        messages.push(timer(correlation_id, deadline));
    }
    for tracker in state.trackers {
        // Java's `correctNextServiceSessionId`: with nothing pending, the
        // next id follows the log's.
        let next = if tracker.is_empty() {
            tracker.log_service_session_id + 1
        } else {
            tracker.next_service_session_id
        };
        let missed = next - tracker.next_service_session_id;
        if missed > 0 {
            errors.push(format!(
                "Follower has missed {missed} service message(s).  Please check service (id={}) determinism \
                 around the use of Cluster::offer",
                tracker.service_id
            ));
        }
        messages.push(self::tracker(
            tracker.service_id,
            next,
            tracker.log_service_session_id,
            tracker.size() as i32,
        ));
        messages.extend(tracker.messages().map(<[u8]>::to_vec));
    }
    messages.push(marker(
        SnapshotMark::END,
        state.log_position,
        state.leadership_term_id,
        state.term_base_log_position,
        state.time_unit,
        state.app_version,
    ));
    (messages, errors)
}

/// A message of the module's snapshot, read back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Loaded {
    Begin {
        app_version: i32,
        time_unit: ClusterTimeUnit,
    },
    ConsensusModule {
        next_session_id: i64,
        next_service_session_id: i64,
        log_service_session_id: i64,
    },
    ClusterSession {
        cluster_session_id: i64,
        correlation_id: i64,
        opened_log_position: i64,
        time_of_last_activity: i64,
        close_reason: CloseReason,
        response_stream_id: i32,
        response_channel: String,
    },
    Timer {
        correlation_id: i64,
        deadline: i64,
    },
    PendingMessageTracker {
        next_service_session_id: i64,
        log_service_session_id: i64,
        service_id: i32,
    },
    PendingMessage {
        cluster_session_id: i64,
        message: Vec<u8>,
    },
    End,
    Ignored,
}

/// Reads a snapshot's messages in order, as Java's adapter: a marker of
/// another type, a second BEGIN or an END without BEGIN is an error.
#[derive(Debug, Default)]
pub struct Loader {
    in_snapshot: bool,
    done: bool,
}

impl Loader {
    pub fn is_done(&self) -> bool {
        self.done
    }

    pub fn on_message(&mut self, message: &[u8]) -> Result<Loaded, String> {
        let h = message::header(message)?;
        if h.schema_id != SCHEMA_ID {
            return Err(format!("expected schemaId={SCHEMA_ID}, actual={}", h.schema_id));
        }
        let header = || message::header_decoder(message);
        Ok(match h.template_id {
            session_message_header_codec::SBE_TEMPLATE_ID => {
                message::needs(&h, 24)?;
                let d = session_message_header_codec::SessionMessageHeaderDecoder::default().header(header(), 0);
                Loaded::PendingMessage {
                    cluster_session_id: d.cluster_session_id(),
                    message: message.to_vec(),
                }
            }
            snapshot_marker_codec::SBE_TEMPLATE_ID => {
                message::needs(&h, 36)?;
                let d = snapshot_marker_codec::SnapshotMarkerDecoder::default().header(header(), 0);
                if d.type_id() != SNAPSHOT_TYPE_ID {
                    return Err(format!("unexpected snapshot type: {}", d.type_id()));
                }
                match d.mark() {
                    SnapshotMark::BEGIN => {
                        if self.in_snapshot {
                            return Err("already in snapshot".into());
                        }
                        self.in_snapshot = true;
                        Loaded::Begin {
                            app_version: d.app_version().unwrap_or(0),
                            time_unit: d.time_unit(),
                        }
                    }
                    SnapshotMark::END => {
                        if !self.in_snapshot {
                            return Err("missing begin snapshot".into());
                        }
                        self.done = true;
                        Loaded::End
                    }
                    _ => Loaded::Ignored,
                }
            }
            cluster_session_codec::SBE_TEMPLATE_ID => {
                message::needs(&h, 40)?;
                let mut d = cluster_session_codec::ClusterSessionDecoder::default().header(header(), 0);
                message::check_var(message, crate::codecs::cluster::Decoder::get_limit(&d))?;
                let c = d.response_channel_decoder();
                let response_channel = message::ascii(d.response_channel_slice(c));
                Loaded::ClusterSession {
                    cluster_session_id: d.cluster_session_id(),
                    correlation_id: d.correlation_id(),
                    opened_log_position: d.opened_log_position(),
                    time_of_last_activity: d.time_of_last_activity(),
                    close_reason: d.close_reason(),
                    response_stream_id: d.response_stream_id(),
                    response_channel,
                }
            }
            timer_codec::SBE_TEMPLATE_ID => {
                message::needs(&h, 16)?;
                let d = timer_codec::TimerDecoder::default().header(header(), 0);
                Loaded::Timer {
                    correlation_id: d.correlation_id(),
                    deadline: d.deadline(),
                }
            }
            consensus_module_codec::SBE_TEMPLATE_ID => {
                message::needs(&h, 8)?;
                let d = consensus_module_codec::ConsensusModuleDecoder::default().header(header(), 0);
                // Raw, as Java's decoder reads them: their null value is a tracker's first id
                // (`Long.MIN_VALUE`, before any service message), not an absent one.
                Loaded::ConsensusModule {
                    next_session_id: d.next_session_id(),
                    next_service_session_id: d.next_service_session_id().unwrap_or(i64::MIN),
                    log_service_session_id: d.log_service_session_id().unwrap_or(i64::MIN),
                }
            }
            pending_message_tracker_codec::SBE_TEMPLATE_ID => {
                message::needs(&h, 24)?;
                let d = pending_message_tracker_codec::PendingMessageTrackerDecoder::default().header(header(), 0);
                Loaded::PendingMessageTracker {
                    next_service_session_id: d.next_service_session_id(),
                    log_service_session_id: d.log_service_session_id(),
                    service_id: d.service_id(),
                }
            }
            _ => Loaded::Ignored,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_snapshot_reads_back_as_it_was_taken() {
        let mut tracker = PendingServiceMessages::new(0);
        let mut message = vec![0; 32];
        message[0..2].copy_from_slice(&24u16.to_le_bytes());
        message[2..4].copy_from_slice(&1u16.to_le_bytes());
        message[4..6].copy_from_slice(&111u16.to_le_bytes());
        message.extend_from_slice(b"hi");
        tracker.enqueue(&message);
        let timers = [(5, 1_000), (6, 2_000)];
        let trackers = [tracker];
        let (messages, errors) = take(State {
            log_position: 4_096,
            leadership_term_id: 2,
            term_base_log_position: 1_024,
            time_unit: ClusterTimeUnit::MILLIS,
            app_version: 1,
            next_committed_session_id: 9,
            sessions: std::iter::empty(),
            timers: &timers,
            trackers: &trackers,
        });
        assert!(errors.is_empty());
        let mut loader = Loader::default();
        let loaded: Vec<Loaded> = messages.iter().map(|m| loader.on_message(m).unwrap()).collect();
        assert!(loader.is_done());
        assert_eq!(
            loaded[0],
            Loaded::Begin {
                app_version: 1,
                time_unit: ClusterTimeUnit::MILLIS
            }
        );
        assert!(matches!(loaded[1], Loaded::ConsensusModule { next_session_id: 9, .. }));
        assert_eq!(
            loaded[2],
            Loaded::Timer {
                correlation_id: 5,
                deadline: 1_000
            }
        );
        assert!(matches!(loaded[4], Loaded::PendingMessageTracker { service_id: 0, .. }));
        assert!(matches!(loaded[5], Loaded::PendingMessage { .. }));
        assert_eq!(loaded[6], Loaded::End);
        assert!(
            Loader::default().on_message(&messages[6]).is_err(),
            "an END without BEGIN"
        );
    }

    #[test]
    fn a_tracker_before_any_service_message_reads_back_its_first_id() {
        let mut loader = Loader::default();
        let begin = marker(SnapshotMark::BEGIN, 0, 0, 0, ClusterTimeUnit::NANOS, 1);
        loader.on_message(&begin).unwrap();
        let state = module_state(3, i64::MIN + 1, i64::MIN, 0);
        assert_eq!(
            loader.on_message(&state).unwrap(),
            Loaded::ConsensusModule {
                next_session_id: 3,
                next_service_session_id: i64::MIN + 1,
                log_service_session_id: i64::MIN,
            },
            "Long.MIN_VALUE is the field's null value, and a log's first service message id"
        );
    }
}
