//! What a clustered service sees: the trait it implements, and the context
//! its callbacks get to read the cluster and to act on it.

use std::collections::BTreeMap;

use super::outbox::Outbox;
use super::snapshot::{SnapshotSink, SnapshotSource};

/// A member's role in the cluster.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Role {
    /// The role from its code in the log and on the wire.
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(Self::Follower),
            1 => Some(Self::Candidate),
            2 => Some(Self::Leader),
            _ => None,
        }
    }
}

/// The unit of the cluster's timestamps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    Millis,
    Micros,
    Nanos,
}

/// Why a session closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CloseReason {
    ClientAction,
    ServiceAction,
    Timeout,
    Unknown,
}

/// A client session the log opened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientSession {
    pub id: i64,
    pub response_stream_id: i32,
    pub response_channel: String,
    pub encoded_principal: Vec<u8>,
    /// The service asked for the session to close; the log has not closed it yet.
    pub closing: bool,
    /// The leader's egress publication to the client is requested.
    pub(crate) connected: bool,
}

/// A message from the log for the service.
#[derive(Clone, Copy, Debug)]
pub struct SessionMessage<'a> {
    /// The session it came from, if the service knows it: a message a service
    /// sent through the consensus module comes from none.
    pub session_id: Option<i64>,
    /// The cluster session id in the message's header.
    pub cluster_session_id: i64,
    pub timestamp: i64,
    /// The log position just after the message.
    pub log_position: i64,
    pub payload: &'a [u8],
}

/// A new leadership term, as the log announces it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NewLeadershipTerm {
    pub leadership_term_id: i64,
    pub log_position: i64,
    pub timestamp: i64,
    pub term_base_log_position: i64,
    pub leader_member_id: i32,
    pub log_session_id: i32,
    pub time_unit: TimeUnit,
    pub app_version: i32,
}

/// Where in the log a snapshot is taken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotPoint {
    pub log_position: i64,
    pub leadership_term_id: i64,
    pub timestamp: i64,
}

/// The snapshot a service starts from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub recording_id: i64,
    /// Where the log continues after it.
    pub log_position: i64,
    pub leadership_term_id: i64,
    pub timestamp: i64,
}

/// What became of a message or timer the service was handed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Apply {
    /// Applied: the log moves on.
    Done,
    /// Not applied, and nothing changed: deliver it again later. Whatever the
    /// callback sent or scheduled before returning this is discarded.
    Retry,
}

/// What became of an egress offer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Egress {
    /// Queued for the session's publication.
    Queued,
    /// This member does not lead: only the leader answers clients, and every
    /// other member's answer goes nowhere.
    Mocked,
    /// The session has no publication: it is closed, or unknown, or this
    /// service does not answer clients.
    NotConnected,
}

/// Errors a service gets back from its context.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CxError {
    UnknownSession(i64),
    MessageTooLong { length: usize, max: usize },
}

impl std::fmt::Display for CxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownSession(id) => write!(f, "unknown cluster session {id}"),
            Self::MessageTooLong { length, max } => write!(f, "message of {length} bytes exceeds {max}"),
        }
    }
}

impl std::error::Error for CxError {}

/// A clustered service: deterministic, single-threaded, driven by the log.
///
/// Every member applies the same log in the same order, so a service's state
/// must follow from the log alone: no clock but [`Cx::time`], no randomness,
/// no I/O that changes what it does. Callbacks run on the container's thread
/// and must not block it.
pub trait ClusteredService {
    /// Where the snapshot the service starts from goes, as it is replayed:
    /// called before [`Self::on_start`] when there is one.
    fn snapshot_sink(&mut self, info: &SnapshotInfo) -> Box<dyn SnapshotSink>;

    /// The service starts, from the snapshot its sink took in, if any. An
    /// error fails the start: the consensus module learns of it from the
    /// start's acknowledgement, and the container stops.
    fn on_start(&mut self, cx: &ReadCx<'_>, snapshot: Option<&SnapshotInfo>) -> Result<(), String>;

    fn on_session_open(&mut self, cx: &mut Cx<'_>, session: &ClientSession, timestamp: i64);

    fn on_session_close(&mut self, cx: &mut Cx<'_>, session: &ClientSession, timestamp: i64, reason: CloseReason);

    fn on_session_message(&mut self, cx: &mut Cx<'_>, message: SessionMessage<'_>) -> Apply;

    fn on_timer_event(&mut self, cx: &mut Cx<'_>, correlation_id: i64, timestamp: i64) -> Apply;

    /// The state as of `point`, which the log has just reached and will not
    /// pass until the snapshot is recorded. The source is pulled without
    /// blocking; it may be filled from another thread.
    fn on_take_snapshot(&mut self, cx: &ReadCx<'_>, point: SnapshotPoint) -> Box<dyn SnapshotSource>;

    fn on_role_change(&mut self, _cx: &ReadCx<'_>, _role: Role) {}

    fn on_new_leadership_term(&mut self, _cx: &mut Cx<'_>, _term: &NewLeadershipTerm) {}

    /// The service is stopping: the log has reached its termination position,
    /// or the consensus module is gone.
    fn on_terminate(&mut self, _cx: &ReadCx<'_>) {}

    /// Called every duty cycle, with the monotonic clock: work outside the
    /// log, which must not change the service's state. Returns the work done.
    fn do_background_work(&mut self, _cx: &ReadCx<'_>, _now_ns: i64) -> usize {
        0
    }
}

/// The state every callback can read.
#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) service_id: i32,
    pub(crate) member_id: i32,
    pub(crate) role: Role,
    pub(crate) cluster_time: i64,
    pub(crate) log_position: i64,
    pub(crate) leadership_term_id: i64,
    pub(crate) time_unit: Option<TimeUnit>,
    pub(crate) sessions: BTreeMap<i64, ClientSession>,
    pub(crate) out: Outbox,
    pub(crate) now_ns: i64,
}

/// What a callback that may not act on the cluster can read of it.
pub struct ReadCx<'a> {
    pub(crate) shared: &'a Shared,
}

/// What a callback can read of the cluster, and do to it.
pub struct Cx<'a> {
    pub(crate) shared: &'a mut Shared,
}

macro_rules! readers {
    () => {
        pub fn service_id(&self) -> i32 {
            self.shared.service_id
        }

        /// This member's id, once the log has been joined.
        pub fn member_id(&self) -> i32 {
            self.shared.member_id
        }

        pub fn role(&self) -> Role {
            self.shared.role
        }

        /// The timestamp of the log entry being applied: the cluster's time.
        pub fn time(&self) -> i64 {
            self.shared.cluster_time
        }

        /// The log position just after the entry being applied.
        pub fn log_position(&self) -> i64 {
            self.shared.log_position
        }

        pub fn leadership_term_id(&self) -> i64 {
            self.shared.leadership_term_id
        }

        pub fn time_unit(&self) -> Option<TimeUnit> {
            self.shared.time_unit
        }

        pub fn session(&self, id: i64) -> Option<&ClientSession> {
            self.shared.sessions.get(&id)
        }

        /// The open sessions, by id.
        pub fn sessions(&self) -> impl Iterator<Item = &ClientSession> {
            self.shared.sessions.values()
        }
    };
}

impl ReadCx<'_> {
    readers!();
}

impl Cx<'_> {
    readers!();

    /// Sends `payload` to a session's client, after the session message header.
    pub fn egress(&mut self, session_id: i64, payload: &[u8]) -> Egress {
        let shared = &mut *self.shared;
        if shared.role != Role::Leader {
            return Egress::Mocked;
        }
        match shared.sessions.get(&session_id) {
            Some(session) if session.connected => {
                shared.out.stage_egress(
                    session_id,
                    shared.leadership_term_id,
                    shared.cluster_time,
                    payload,
                    shared.now_ns,
                );
                Egress::Queued
            }
            _ => Egress::NotConnected,
        }
    }

    /// Sends `payload` to every service through the log: it comes back to each
    /// as a message from no session.
    pub fn service_message(&mut self, payload: &[u8]) -> Result<(), CxError> {
        let shared = &mut *self.shared;
        shared.out.stage_service_message(
            shared.service_id,
            shared.leadership_term_id,
            shared.cluster_time,
            payload,
        )
    }

    /// A timer the log fires at `deadline`, in cluster time. A timer scheduled
    /// again under the same id replaces the first.
    pub fn schedule_timer(&mut self, correlation_id: i64, deadline: i64) {
        self.shared.out.stage_schedule_timer(correlation_id, deadline);
    }

    pub fn cancel_timer(&mut self, correlation_id: i64) {
        self.shared.out.stage_cancel_timer(correlation_id);
    }

    /// Asks the consensus module to close a session: the log closes it.
    pub fn close_session(&mut self, session_id: i64) -> Result<(), CxError> {
        let shared = &mut *self.shared;
        let session = shared
            .sessions
            .get_mut(&session_id)
            .ok_or(CxError::UnknownSession(session_id))?;
        if !session.closing {
            session.closing = true;
            shared.out.stage_close_session(session_id);
        }
        Ok(())
    }
}
