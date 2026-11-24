//! The consensus module's state as a follower's module keeps it, rebuilt by
//! the standby from the log and from its service's timer requests
//! (`StandbyModule`'s part of it), and written as Aeron 1.54.2's module
//! snapshot, so a member loads the standby's as one of its own
//! (`ModuleState`).

use std::collections::BTreeMap;

use rusteron_cluster_codecs::cluster::{
    close_reason::CloseReason, cluster_action::ClusterAction, cluster_time_unit::ClusterTimeUnit,
    snapshot_mark::SnapshotMark,
};
use rusteron_consensus_core::log::{self, LogMessage};
use rusteron_consensus_core::service::ServiceMessage;
use rusteron_consensus_core::snapshot::{self, Loaded, Loader};

/// A first service's log message ids, as Aeron's tracker starts them.
pub const INITIAL_LOG_SERVICE_SESSION_ID: i64 = i64::MIN;
/// A SNAPSHOT action's flags: the members', or a standby's.
pub const FLAGS_DEFAULT: i32 = 0;
pub const FLAGS_STANDBY_SNAPSHOT: i32 = 1;

/// An open session, as a module's snapshot holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Session {
    pub id: i64,
    pub correlation_id: i64,
    pub opened_log_position: i64,
    pub close_reason: CloseReason,
    pub response_stream_id: i32,
    pub response_channel: String,
}

/// A service's pending message tracker; its pending messages counted, the service sends none.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Tracker {
    pub service_id: i32,
    pub next_service_session_id: i64,
    pub log_service_session_id: i64,
    pub pending_message_capacity: i32,
}

/// A module's state as its snapshot holds it. Activity times are not in it:
/// Aeron writes none. Sessions, timers and trackers in id order, so two
/// states compare as values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleState {
    pub log_position: i64,
    pub leadership_term_id: i64,
    /// Where the snapshot's term began: Aeron 1.54.1's markers carry it (cluster schema 18), so a member can
    /// rebuild its recording log's entries from a snapshot alone.
    pub term_base_log_position: i64,
    pub time_unit: ClusterTimeUnit,
    pub app_version: i32,
    pub next_session_id: i64,
    pub next_service_session_id: i64,
    pub log_service_session_id: i64,
    pub pending_message_capacity: i32,
    pub sessions: Vec<Session>,
    /// (correlation id, deadline).
    pub timers: Vec<(i64, i64)>,
    pub trackers: Vec<Tracker>,
}

impl ModuleState {
    /// Its messages, in the order a member's module writes its own: begin
    /// marker, the module's state, sessions, timers, trackers, end marker.
    pub fn messages(&self) -> Vec<Vec<u8>> {
        let mark = |m| {
            snapshot::marker(
                m,
                self.log_position,
                self.leadership_term_id,
                self.term_base_log_position,
                self.time_unit,
                self.app_version,
            )
        };
        let mut messages = vec![
            mark(SnapshotMark::BEGIN),
            snapshot::module_state(
                self.next_session_id,
                self.next_service_session_id,
                self.log_service_session_id,
                self.pending_message_capacity,
            ),
        ];
        for s in &self.sessions {
            messages.push(snapshot::session(
                s.id,
                s.correlation_id,
                s.opened_log_position,
                s.close_reason,
                s.response_stream_id,
                &s.response_channel,
            ));
        }
        for &(correlation_id, deadline) in &self.timers {
            messages.push(snapshot::timer(correlation_id, deadline));
        }
        for t in &self.trackers {
            messages.push(snapshot::tracker(
                t.service_id,
                t.next_service_session_id,
                t.log_service_session_id,
                t.pending_message_capacity,
            ));
        }
        messages.push(mark(SnapshotMark::END));
        messages
    }

    /// Reads a module snapshot back from its messages, taken at
    /// `log_position` in term `leadership_term_id`, which began at
    /// `term_base_log_position` (its markers say so too, as the recording
    /// log's entry for it does).
    pub fn read<'a>(
        messages: impl IntoIterator<Item = &'a [u8]>,
        log_position: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
    ) -> Result<Self, String> {
        let mut state = Self {
            log_position,
            leadership_term_id,
            term_base_log_position,
            time_unit: ClusterTimeUnit::NANOS,
            app_version: 0,
            next_session_id: 1,
            next_service_session_id: INITIAL_LOG_SERVICE_SESSION_ID + 1,
            log_service_session_id: INITIAL_LOG_SERVICE_SESSION_ID,
            pending_message_capacity: 0,
            sessions: Vec::new(),
            timers: Vec::new(),
            trackers: Vec::new(),
        };
        let mut loader = Loader::default();
        for message in messages {
            match loader.on_message(message)? {
                Loaded::Begin { app_version, time_unit } => {
                    state.app_version = app_version;
                    state.time_unit = time_unit;
                }
                Loaded::ConsensusModule {
                    next_session_id,
                    next_service_session_id,
                    log_service_session_id,
                } => {
                    state.next_session_id = next_session_id;
                    state.next_service_session_id = next_service_session_id;
                    state.log_service_session_id = log_service_session_id;
                }
                Loaded::ClusterSession {
                    cluster_session_id,
                    correlation_id,
                    opened_log_position,
                    close_reason,
                    response_stream_id,
                    response_channel,
                    ..
                } => state.sessions.push(Session {
                    id: cluster_session_id,
                    correlation_id,
                    opened_log_position,
                    close_reason,
                    response_stream_id,
                    response_channel,
                }),
                Loaded::Timer {
                    correlation_id,
                    deadline,
                } => state.timers.push((correlation_id, deadline)),
                Loaded::PendingMessageTracker {
                    next_service_session_id,
                    log_service_session_id,
                    service_id,
                } => state.trackers.push(Tracker {
                    service_id,
                    next_service_session_id,
                    log_service_session_id,
                    pending_message_capacity: 0,
                }),
                Loaded::End => break,
                Loaded::PendingMessage { .. } | Loaded::Ignored => {}
            }
        }
        if !loader.is_done() {
            return Err(format!(
                "the module snapshot at {log_position} ends before its end marker"
            ));
        }
        state.sessions.sort_by_key(|s| s.id);
        state.timers.sort_unstable();
        state.trackers.sort_by_key(|t| t.service_id);
        Ok(state)
    }
}

/// A SNAPSHOT action in the log: where, whose (the members' or a standby's), and when.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotAction {
    pub log_position: i64,
    pub flags: i32,
    pub timestamp: i64,
}

/// What a log entry asks of the reader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Read {
    /// Go on reading.
    Continue,
    /// Stop after this entry, as a member's module does at every cluster
    /// action: a snapshot action of this term to be held at, if it is one.
    Break(Option<SnapshotAction>),
}

/// The module's state kept from the log and the service's requests, as a
/// follower's module keeps it.
pub struct ModuleView {
    sessions: BTreeMap<i64, Session>,
    timers: BTreeMap<i64, i64>,
    /// Timers fired before the service asked for them here: their requests, when they come, are dropped.
    expired: BTreeMap<i64, u64>,
    next_session_id: i64,
    leadership_term_id: i64,
    term_base_log_position: i64,
    time_unit: ClusterTimeUnit,
    app_version: i32,
    log_service_session_id: i64,
}

impl Default for ModuleView {
    fn default() -> Self {
        Self {
            sessions: BTreeMap::new(),
            timers: BTreeMap::new(),
            expired: BTreeMap::new(),
            next_session_id: 1,
            leadership_term_id: -1,
            term_base_log_position: 0,
            time_unit: ClusterTimeUnit::NANOS,
            app_version: 0,
            log_service_session_id: INITIAL_LOG_SERVICE_SESSION_ID,
        }
    }
}

impl ModuleView {
    /// A view taken up from the standby's own snapshot: its module's state,
    /// and the term's base position its listing gives.
    pub fn loaded(state: &ModuleState, term_base_log_position: i64) -> Self {
        Self {
            sessions: state.sessions.iter().map(|s| (s.id, s.clone())).collect(),
            timers: state.timers.iter().copied().collect(),
            expired: BTreeMap::new(),
            next_session_id: state.next_session_id,
            leadership_term_id: state.leadership_term_id,
            term_base_log_position,
            time_unit: state.time_unit,
            app_version: state.app_version,
            log_service_session_id: state.log_service_session_id,
        }
    }

    pub fn leadership_term_id(&self) -> i64 {
        self.leadership_term_id
    }

    pub fn term_base_log_position(&self) -> i64 {
        self.term_base_log_position
    }

    /// One whole entry of the log, ending at `position`. An entry this view
    /// cannot read is passed over, as a module passes over what it does not act on.
    pub fn on_log(&mut self, message: &[u8], position: i64) -> Read {
        let Ok(entry) = log::decode(message) else {
            return Read::Continue;
        };
        match entry {
            LogMessage::SessionMessage { cluster_session_id, .. } => {
                // A service's message: its id, below 0, carries the service's in its top bits; ours is service 0.
                if cluster_session_id < 0 && ((cluster_session_id as u64 >> 56) & 0x7F) == 0 {
                    self.log_service_session_id = cluster_session_id;
                }
            }
            LogMessage::TimerEvent { correlation_id } => {
                if self.timers.remove(&correlation_id).is_none() {
                    *self.expired.entry(correlation_id).or_default() += 1;
                }
            }
            LogMessage::SessionOpen {
                correlation_id,
                cluster_session_id,
                response_stream_id,
                response_channel,
                ..
            } => {
                self.sessions.insert(
                    cluster_session_id,
                    Session {
                        id: cluster_session_id,
                        correlation_id,
                        opened_log_position: position,
                        close_reason: CloseReason::NullVal,
                        response_stream_id,
                        response_channel,
                    },
                );
                if cluster_session_id >= self.next_session_id {
                    self.next_session_id = cluster_session_id + 1;
                }
            }
            LogMessage::SessionClose { cluster_session_id, .. } => {
                self.sessions.remove(&cluster_session_id);
            }
            LogMessage::ClusterAction {
                leadership_term_id,
                log_position,
                timestamp,
                action,
                flags,
            } => {
                let snapshot = (action == ClusterAction::SNAPSHOT && leadership_term_id == self.leadership_term_id)
                    .then_some(SnapshotAction {
                        log_position,
                        flags,
                        timestamp,
                    });
                return Read::Break(snapshot);
            }
            LogMessage::NewLeadershipTerm {
                leadership_term_id,
                term_base_log_position,
                time_unit,
                app_version,
                ..
            } => {
                self.leadership_term_id = leadership_term_id;
                self.term_base_log_position = term_base_log_position;
                self.time_unit = time_unit;
                self.app_version = app_version;
            }
            LogMessage::Extension | LogMessage::Ignored => {}
        }
        Read::Continue
    }

    /// What the service sent the module: its timers. Session closes and
    /// service messages a leader's module acts on; a standby's need not.
    pub fn on_service(&mut self, message: &ServiceMessage) {
        match message {
            ServiceMessage::ScheduleTimer {
                correlation_id,
                deadline,
            } => match self.expired.get_mut(correlation_id) {
                Some(n) if *n > 0 => {
                    *n -= 1;
                    if *n == 0 {
                        self.expired.remove(correlation_id);
                    }
                }
                _ => {
                    self.timers.insert(*correlation_id, *deadline);
                }
            },
            ServiceMessage::CancelTimer { correlation_id } => {
                self.timers.remove(correlation_id);
            }
            _ => {}
        }
    }

    /// The module's state now, as a member's module would write it in a
    /// snapshot at `log_position`. The service sends no service messages, so
    /// none is pending: the tracker's next id follows the log's.
    pub fn state(&self, log_position: i64) -> ModuleState {
        ModuleState {
            log_position,
            leadership_term_id: self.leadership_term_id,
            term_base_log_position: self.term_base_log_position,
            time_unit: self.time_unit,
            app_version: self.app_version,
            next_session_id: self.next_session_id,
            next_service_session_id: INITIAL_LOG_SERVICE_SESSION_ID + 1,
            log_service_session_id: self.log_service_session_id,
            pending_message_capacity: 0,
            sessions: self.sessions.values().cloned().collect(),
            timers: self.timers.iter().map(|(c, d)| (*c, *d)).collect(),
            trackers: vec![Tracker {
                service_id: 0,
                next_service_session_id: self.log_service_session_id.wrapping_add(1),
                log_service_session_id: self.log_service_session_id,
                pending_message_capacity: 0,
            }],
        }
    }
}
