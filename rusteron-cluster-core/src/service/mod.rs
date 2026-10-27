//! The clustered service container as a state machine, wire-compatible with
//! Aeron 1.53.3's `ClusteredServiceAgent`.
//!
//! The container reads no clock, draws no random number and does no I/O.
//! The shell that drives it feeds it time ([`Container::on_tick`]), what the
//! consensus module sends ([`Container::on_cm_message`]), the log's frames
//! ([`Container::on_log_fragment`]) and the outcome of the I/O it asked for
//! ([`Container::on_io`]); it takes the I/O to do from
//! [`Container::poll_action`] and the bytes to send from the drains.
//!
//! What it owes the consensus module, which acks it out of order (the
//! consensus module stops on that), and what the service sees:
//! - **Acknowledgements**, numbered from 0: once started (carrying the Aeron
//!   client's id, by which the consensus module watches the service live),
//!   each log joined, each snapshot (carrying its recording), each requested
//!   ack, and at termination.
//! - **The log**, never past the commit position, each message once, in
//!   order; a snapshot is taken at exactly the position of its request, and
//!   the log waits for it.
//! - **Nothing dropped silently**: messages to the consensus module are
//!   queued and retried in order; a full queue stops the log instead.

pub mod api;
mod decode;
pub mod outbox;
pub mod snapshot;

use std::collections::VecDeque;

use rusteron_cluster_codecs::cluster::{
    Decoder, boolean_type::BooleanType, close_reason::CloseReason as WireCloseReason, cluster_action::ClusterAction,
    cluster_action_request_codec::ClusterActionRequestDecoder, join_log_codec::JoinLogDecoder, message_header_codec,
    new_leadership_term_event_codec::NewLeadershipTermEventDecoder,
    request_service_ack_codec::RequestServiceAckDecoder,
    service_termination_position_codec::ServiceTerminationPositionDecoder,
    session_close_event_codec::SessionCloseEventDecoder, session_message_header_codec::SessionMessageHeaderDecoder,
    session_open_event_codec::SessionOpenEventDecoder, snapshot_mark::SnapshotMark,
    timer_event_codec::TimerEventDecoder,
};

pub use api::{
    Apply, ClientSession, CloseReason, ClusteredService, Cx, CxError, Egress, NewLeadershipTerm, ReadCx, Role,
    SessionMessage, SnapshotInfo, SnapshotPoint, TimeUnit,
};
pub use outbox::{Offer, SESSION_HEADER_LENGTH};
pub use snapshot::{
    BytesSource, ChannelSource, Finish, Pull, SnapshotSink, SnapshotSource, SnapshotWriter, channel_source, vec_sink,
};

use crate::counters::{NULL_VALUE, RecoveryState};
use crate::log::{Fragment, PollAction, Reassembler};
use api::Shared;
use outbox::Outbox;
use snapshot::Loader;

const HEADER: usize = message_header_codec::ENCODED_LENGTH;
const ONE_MILLISECOND_NS: i64 = 1_000_000;
/// An ack for termination is offered this many times, then given up on.
const TERMINATION_ACK_ATTEMPTS: u32 = 5;
const CLUSTER_ACTION_FLAGS_DEFAULT: i32 = 0;
const CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT: i32 = 1;

/// How a container runs. The defaults are Aeron's.
#[derive(Clone, Debug)]
pub struct Config {
    pub service_id: i32,
    pub cluster_id: i32,
    /// The service's version, `major << 16 | minor << 8 | patch`, written in
    /// its snapshots: a log or snapshot of another major version stops the
    /// service. Java's default is 0.0.1.
    pub app_version: i32,
    /// Whether the service answers clients. A second service on the same log
    /// that only observes says no.
    pub responding: bool,
    pub snapshot_channel: String,
    pub snapshot_stream_id: i32,
    pub replay_channel: String,
    pub replay_stream_id: i32,
    /// The largest message to the consensus module: an eighth of the control
    /// channel's term.
    pub cm_max_message_length: usize,
    /// Above this many messages queued for the consensus module, the log waits.
    pub cm_high_water: usize,
    /// Above this many frames queued for clients, the log waits.
    pub egress_high_water: usize,
    /// A frame a client's publication refuses for this long is dropped.
    pub egress_timeout_ns: i64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            service_id: 0,
            cluster_id: 0,
            app_version: 1,
            responding: true,
            snapshot_channel: "aeron:ipc?alias=snapshot".into(),
            snapshot_stream_id: 106,
            replay_channel: "aeron:ipc".into(),
            replay_stream_id: 103,
            cm_max_message_length: 128 * 1024 / 8,
            cm_high_water: 1_024,
            egress_high_water: 4_096,
            egress_timeout_ns: 5_000_000_000,
        }
    }
}

/// The time, as the shell reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Now {
    /// A monotonic clock, ns.
    pub nanos: i64,
    /// The wall clock, ms since the epoch.
    pub epoch_ms: i64,
}

/// What [`Container::on_tick`] asks of the shell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Tick {
    /// Poll the consensus module's stream, then call [`Container::after_cm_poll`]:
    /// every millisecond, as Java does.
    pub poll_cm: bool,
    pub work: usize,
}

/// I/O the shell does for the container.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Subscribe to the log, and report its image ([`Io::LogImage`]).
    JoinLog {
        channel: String,
        stream_id: i32,
        session_id: i32,
    },
    /// Close the log's subscription.
    CloseLog,
    /// Replay a snapshot's recording to a new subscription, deliver its
    /// frames ([`Container::on_snapshot_fragment`]) and its end
    /// ([`Container::on_snapshot_replay_end`]).
    ReplaySnapshot {
        recording_id: i64,
        channel: String,
        stream_id: i32,
    },
    CloseSnapshotReplay,
    /// Open an exclusive publication, record it in the local archive, and
    /// report the recording ([`Io::SnapshotPublication`]); then offer the
    /// frames of [`Container::snapshot_frame`].
    RecordSnapshot {
        channel: String,
        stream_id: i32,
    },
    /// Every frame is offered: report once the archive has recorded the
    /// publication's position ([`Io::SnapshotRecorded`]).
    AwaitSnapshotRecorded,
    CloseSnapshotRecording,
    ConnectEgress {
        session_id: i64,
        channel: String,
        stream_id: i32,
    },
    DisconnectEgress {
        session_id: i64,
    },
    /// This member's id, for the mark file.
    MemberId(i32),
    /// Something went wrong that the container lives through: for the error log.
    Error {
        message: String,
    },
    /// Stop, once the consensus module's queue is drained ([`Container::is_flushed`]).
    Stop {
        expected: bool,
    },
}

/// What came of the I/O the container asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Io {
    LogImage {
        join_position: i64,
    },
    /// The log's subscription could not be made: the join is tried again.
    LogSubscriptionFailed {
        message: String,
    },
    /// The snapshot to start from cannot be replayed.
    SnapshotReplayFailed {
        message: String,
    },
    SnapshotPublication {
        recording_id: i64,
        max_payload_length: usize,
    },
    SnapshotRecorded,
    /// The snapshot could not be recorded: acknowledged without a recording.
    SnapshotFailed {
        message: String,
    },
}

/// What an image says of itself after a poll that delivered nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImageStatus {
    pub position: i64,
    pub end_of_stream: bool,
    pub closed: bool,
}

#[derive(Clone, Debug)]
struct JoinLogEvent {
    log_position: i64,
    max_log_position: i64,
    member_id: i32,
    log_session_id: i32,
    log_stream_id: i32,
    is_startup: bool,
    role: Role,
    is_standby: bool,
    log_channel: String,
}

struct ActiveLog {
    max_log_position: i64,
    reassembler: Reassembler,
}

struct Taking {
    log_position: i64,
    recording_id: Option<i64>,
    max_payload: usize,
    preamble: VecDeque<Vec<u8>>,
    source: Box<dyn SnapshotSource>,
    frame: Option<Vec<u8>>,
    buffer: Vec<u8>,
    written: bool,
}

enum Phase {
    AwaitCounters,
    Loading {
        info: SnapshotInfo,
        loader: Box<Loader>,
        reassembler: Reassembler,
        replay_ended: bool,
    },
    Running,
    Stopped,
}

/// A clustered service and its container's state.
pub struct Container<S> {
    config: Config,
    service: S,
    shared: Shared,
    phase: Phase,
    ack_id: i64,
    client_id: i64,
    last_slow_tick_ns: Option<i64>,
    termination: Option<(i64, bool)>,
    requested_ack: Option<i64>,
    pending_join: Option<JoinLogEvent>,
    joining: Option<JoinLogEvent>,
    log: Option<ActiveLog>,
    snapshot: Option<Taking>,
    standby_snapshot_flags: i32,
    actions: VecDeque<Action>,
}

fn major(version: i32) -> i32 {
    (version >> 16) & 0xFF
}

fn close_reason(reason: WireCloseReason) -> CloseReason {
    match reason {
        WireCloseReason::CLIENT_ACTION => CloseReason::ClientAction,
        WireCloseReason::SERVICE_ACTION => CloseReason::ServiceAction,
        WireCloseReason::TIMEOUT => CloseReason::Timeout,
        _ => CloseReason::Unknown,
    }
}

/// `channel` with its `alias` parameter set to `alias`, as Java's
/// `ChannelUriStringBuilder` would make it.
fn with_alias(channel: &str, alias: &str) -> String {
    let (media, params) = channel.split_once('?').unwrap_or((channel, ""));
    let mut kept: Vec<&str> = params
        .split('|')
        .filter(|p| !p.is_empty() && !p.starts_with("alias="))
        .collect();
    let alias = format!("alias={alias}");
    kept.push(&alias);
    format!("{media}?{}", kept.join("|"))
}

impl<S: ClusteredService> Container<S> {
    pub fn new(config: Config, service: S) -> Self {
        let shared = Shared {
            service_id: config.service_id,
            member_id: NULL_VALUE as i32,
            role: Role::Follower,
            cluster_time: 0,
            log_position: NULL_VALUE,
            leadership_term_id: NULL_VALUE,
            time_unit: None,
            sessions: Default::default(),
            out: Outbox::new(config.cm_max_message_length),
            now_ns: 0,
        };
        Self {
            config,
            service,
            shared,
            phase: Phase::AwaitCounters,
            ack_id: 0,
            client_id: NULL_VALUE,
            last_slow_tick_ns: None,
            termination: None,
            requested_ack: None,
            pending_join: None,
            joining: None,
            log: None,
            snapshot: None,
            standby_snapshot_flags: CLUSTER_ACTION_FLAGS_DEFAULT,
            actions: VecDeque::new(),
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn service(&self) -> &S {
        &self.service
    }

    pub fn service_mut(&mut self) -> &mut S {
        &mut self.service
    }

    pub fn role(&self) -> Role {
        self.shared.role
    }

    pub fn log_position(&self) -> i64 {
        self.shared.log_position
    }

    pub fn cluster_time(&self) -> i64 {
        self.shared.cluster_time
    }

    pub fn member_id(&self) -> i32 {
        self.shared.member_id
    }

    /// Frames for clients dropped since the start.
    pub fn egress_dropped(&self) -> u64 {
        self.shared.out.egress_dropped
    }

    /// Whether the container has stopped: drain the consensus module's queue, then exit.
    pub fn is_stopped(&self) -> bool {
        matches!(self.phase, Phase::Stopped)
    }

    /// Whether nothing is left for the consensus module.
    pub fn is_flushed(&self) -> bool {
        self.shared.out.cm_len() == 0
    }

    /// The next I/O to do.
    pub fn poll_action(&mut self) -> Option<Action> {
        self.actions.pop_front()
    }

    fn next_ack_id(&mut self) -> i64 {
        let id = self.ack_id;
        self.ack_id += 1;
        id
    }

    fn ack(&mut self, log_position: i64, relevant_id: i64, attempts: Option<u32>) {
        let id = self.next_ack_id();
        let bytes = outbox::ack(
            log_position,
            self.shared.cluster_time,
            id,
            relevant_id,
            self.config.service_id,
        );
        self.shared.out.push_ack(bytes, attempts);
    }

    fn error(&mut self, message: String) {
        self.actions.push_back(Action::Error { message });
    }

    fn fail_stop(&mut self, message: String) {
        if !self.is_stopped() {
            self.error(message);
            self.phase = Phase::Stopped;
            self.actions.push_back(Action::Stop { expected: false });
        }
    }

    // ----- time -----

    /// The duty cycle's start: the time, and background work.
    pub fn on_tick(&mut self, now: Now) -> Tick {
        self.shared.now_ns = now.nanos;
        let poll_cm = match self.last_slow_tick_ns {
            None => true,
            Some(last) => now.nanos - last > ONE_MILLISECOND_NS,
        };
        if poll_cm {
            self.last_slow_tick_ns = Some(now.nanos);
        }
        self.finish_loading();
        let work = if matches!(self.phase, Phase::Running) {
            let cx = ReadCx { shared: &self.shared };
            self.service.do_background_work(&cx, now.nanos)
        } else {
            0
        };
        Tick { poll_cm, work }
    }

    // ----- start -----

    /// The consensus module's counters are found: the recovery state's key,
    /// and this Aeron client's id, which the start's ack carries.
    pub fn on_counters(&mut self, recovery_key: &[u8], client_id: i64) {
        if !matches!(self.phase, Phase::AwaitCounters) {
            return;
        }
        self.client_id = client_id;
        let state = match RecoveryState::parse(recovery_key) {
            Ok(state) => state,
            Err(e) => return self.start_failed(e.to_string()),
        };
        self.shared.log_position = state.log_position;
        self.shared.cluster_time = state.timestamp;
        self.shared.leadership_term_id = state.leadership_term_id;
        if state.leadership_term_id == NULL_VALUE {
            return self.start(None);
        }
        let Some(recording_id) = state.snapshot_recording_id(self.config.service_id) else {
            let count = state.snapshot_recording_ids.len();
            return self.start_failed(format!(
                "invalid serviceId {} for count of {count}",
                self.config.service_id
            ));
        };
        let info = SnapshotInfo {
            recording_id,
            log_position: state.log_position,
            leadership_term_id: state.leadership_term_id,
            timestamp: state.timestamp,
        };
        let sink = self.service.snapshot_sink(&info);
        self.phase = Phase::Loading {
            info,
            loader: Box::new(Loader::new(sink)),
            reassembler: Reassembler::default(),
            replay_ended: false,
        };
        self.actions.push_back(Action::ReplaySnapshot {
            recording_id,
            channel: self.config.replay_channel.clone(),
            stream_id: self.config.replay_stream_id,
        });
    }

    /// A frame of the snapshot being loaded.
    pub fn on_snapshot_fragment(&mut self, fragment: Fragment<'_>) -> PollAction {
        let Phase::Loading {
            loader, reassembler, ..
        } = &mut self.phase
        else {
            return PollAction::Continue;
        };
        let mut failure = None;
        let action = reassembler.on_fragment(fragment, |message, _| match loader.on_message(message) {
            Ok(()) => PollAction::Continue,
            Err(e) => {
                failure = Some(e);
                PollAction::Break
            }
        });
        if let Some(e) = failure {
            self.actions.push_back(Action::CloseSnapshotReplay);
            self.start_failed(e);
        }
        action
    }

    /// The snapshot's replay has ended: its image is at its end, or closed.
    pub fn on_snapshot_replay_end(&mut self) {
        let Phase::Loading {
            loader, replay_ended, ..
        } = &mut self.phase
        else {
            return;
        };
        if *replay_ended {
            return;
        }
        *replay_ended = true;
        let complete = loader.is_after_end();
        self.actions.push_back(Action::CloseSnapshotReplay);
        if !complete {
            self.start_failed("snapshot ended unexpectedly".into());
        }
    }

    fn finish_loading(&mut self) {
        let Phase::Loading {
            info,
            loader,
            replay_ended: true,
            ..
        } = &mut self.phase
        else {
            return;
        };
        match loader.sink.finish() {
            Finish::Pending => {}
            Finish::Failed(e) => self.start_failed(e),
            Finish::Done => {
                let info = *info;
                let (app_version, time_unit) = (loader.app_version, loader.time_unit);
                let sessions = std::mem::take(&mut loader.sessions);
                if major(app_version) != major(self.config.app_version) {
                    return self.start_failed(format!(
                        "incompatible app version: {:#x} snapshot={app_version:#x}",
                        self.config.app_version
                    ));
                }
                self.shared.time_unit = Some(time_unit);
                for session in sessions {
                    self.shared.sessions.insert(session.id, session);
                }
                self.start(Some(info));
            }
        }
    }

    fn start(&mut self, snapshot: Option<SnapshotInfo>) {
        let result = {
            let cx = ReadCx { shared: &self.shared };
            self.service.on_start(&cx, snapshot.as_ref())
        };
        match result {
            Ok(()) => {
                let client_id = self.client_id;
                self.ack(self.shared.log_position, client_id, None);
                self.phase = Phase::Running;
            }
            Err(e) => self.start_failed(e),
        }
    }

    fn start_failed(&mut self, reason: String) {
        if self.is_stopped() {
            return;
        }
        self.ack(self.shared.log_position, NULL_VALUE, None);
        self.fail_stop(format!(
            "failed to start service={} leadershipTermId={} logPosition={}: {reason}",
            self.config.service_id, self.shared.leadership_term_id, self.shared.log_position
        ));
    }

    // ----- the consensus module -----

    /// A message from the consensus module's stream to its services.
    pub fn on_cm_message(&mut self, message: &[u8]) {
        if let Err(e) = self.decode_cm_message(message) {
            self.error(e);
        }
    }

    fn decode_cm_message(&mut self, message: &[u8]) -> Result<(), String> {
        let header = decode::header(message)?;
        match header.template_id {
            40 => {
                let mut d = decode::body(message, &header, 40, |b, h| {
                    JoinLogDecoder::default().wrap(b, HEADER, h.block_length, h.version)
                })?;
                let role = Role::from_code(d.role()).ok_or_else(|| format!("unknown role {}", d.role()))?;
                let (log_position, max_log_position, member_id) =
                    (d.log_position(), d.max_log_position(), d.member_id());
                let (log_session_id, log_stream_id) = (d.log_session_id(), d.log_stream_id());
                let is_startup = d.is_startup() == BooleanType::TRUE;
                let is_standby = d.is_standby() == BooleanType::TRUE;
                decode::check_var(message, d.get_limit())?;
                let c = d.log_channel_decoder();
                let log_channel = String::from_utf8_lossy(d.log_channel_slice(c)).into_owned();
                if let Some(log) = self.log.as_mut() {
                    log.max_log_position = log_position;
                }
                self.pending_join = Some(JoinLogEvent {
                    log_position,
                    max_log_position,
                    member_id,
                    log_session_id,
                    log_stream_id,
                    is_startup,
                    role,
                    is_standby,
                    log_channel,
                });
            }
            42 => {
                let d = decode::body(message, &header, 8, |b, h| {
                    ServiceTerminationPositionDecoder::default().wrap(b, HEADER, h.block_length, h.version)
                })?;
                let ack_required = if header.block_length >= 12 {
                    d.ack_required() == BooleanType::TRUE
                } else {
                    false
                };
                self.termination = Some((d.log_position(), ack_required));
            }
            108 => {
                let d = decode::body(message, &header, 8, |b, h| {
                    RequestServiceAckDecoder::default().wrap(b, HEADER, h.block_length, h.version)
                })?;
                self.requested_ack = Some(d.log_position());
            }
            _ => {}
        }
        Ok(())
    }

    /// After polling the consensus module's stream: join a new log, stop at
    /// the termination position, ack when asked.
    pub fn after_cm_poll(&mut self) {
        if !matches!(self.phase, Phase::Running) {
            return;
        }
        if self.log.is_none()
            && self.joining.is_none()
            && let Some(event) = self.pending_join.take()
        {
            self.begin_join(event);
        }
        if let Some((position, ack_required)) = self.termination {
            let log_position = self.shared.log_position;
            if log_position >= position {
                if log_position > position {
                    self.error(format!(
                        "service terminate: logPosition={log_position} > terminationPosition={position}"
                    ));
                }
                self.terminate(log_position == position, ack_required);
                return;
            }
        }
        if let Some(position) = self.requested_ack {
            let log_position = self.shared.log_position;
            if log_position >= position {
                if log_position > position {
                    self.error(format!(
                        "invalid ack request: logPosition={log_position} > requestedAckPosition={position}"
                    ));
                }
                self.ack(log_position, NULL_VALUE, None);
                self.requested_ack = None;
            }
        }
    }

    fn begin_join(&mut self, event: JoinLogEvent) {
        if event.role != Role::Leader {
            self.disconnect_egress();
        }
        self.standby_snapshot_flags = if event.is_standby {
            CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT
        } else {
            CLUSTER_ACTION_FLAGS_DEFAULT
        };
        let channel = with_alias(&event.log_channel, &format!("log-sc-{}", self.config.service_id));
        self.actions.push_back(Action::JoinLog {
            channel,
            stream_id: event.log_stream_id,
            session_id: event.log_session_id,
        });
        self.joining = Some(event);
    }

    fn terminate(&mut self, expected: bool, ack_required: bool) {
        {
            let cx = ReadCx { shared: &self.shared };
            self.service.on_terminate(&cx);
        }
        if ack_required {
            self.ack(self.shared.log_position, NULL_VALUE, Some(TERMINATION_ACK_ATTEMPTS));
        }
        self.termination = None;
        self.phase = Phase::Stopped;
        self.actions.push_back(Action::Stop { expected });
    }

    /// The commit position's counter is gone: the consensus module stopped.
    pub fn on_commit_position_closed(&mut self) {
        if matches!(self.phase, Phase::Running) {
            self.error("commit-pos counter unexpectedly closed, terminating".into());
            self.terminate(true, false);
        } else if !self.is_stopped() {
            self.phase = Phase::Stopped;
            self.actions.push_back(Action::Stop { expected: true });
        }
    }

    /// The Aeron client closed under the container: it stops at once.
    pub fn on_aeron_closed(&mut self) {
        self.fail_stop("unexpected Aeron close".into());
    }

    /// Something the container cannot live through happened outside it (a
    /// service that panicked, I/O that failed for good): it stops at once.
    pub fn on_fatal(&mut self, message: String) {
        self.fail_stop(message);
    }

    // ----- I/O -----

    pub fn on_io(&mut self, io: Io) {
        match io {
            Io::LogImage { join_position } => self.on_log_image(join_position),
            Io::LogSubscriptionFailed { message } => {
                if let Some(event) = self.joining.take() {
                    self.pending_join.get_or_insert(event);
                }
                self.error(format!("failed to join active log, will retry: {message}"));
            }
            Io::SnapshotReplayFailed { message } => self.start_failed(message),
            Io::SnapshotPublication {
                recording_id,
                max_payload_length,
            } => {
                if let Some(taking) = self.snapshot.as_mut() {
                    taking.recording_id = Some(recording_id);
                    taking.max_payload = max_payload_length;
                }
            }
            Io::SnapshotRecorded => {
                let recording_id = self.snapshot.as_ref().and_then(|t| t.recording_id);
                self.finish_snapshot(recording_id);
            }
            Io::SnapshotFailed { message } => {
                if self.snapshot.is_some() {
                    self.error(message);
                    self.finish_snapshot(None);
                }
            }
        }
    }

    fn on_log_image(&mut self, join_position: i64) {
        let Some(event) = self.joining.take() else {
            return;
        };
        let log_position = self.shared.log_position;
        if join_position != log_position || event.log_position != log_position {
            self.error(format!(
                "Cluster log must be contiguous: expectedPosition={log_position} joinPosition={join_position} \
                 eventPosition={}",
                event.log_position
            ));
            self.actions.push_back(Action::CloseLog);
            self.pending_join.get_or_insert(event);
            return;
        }
        let max_log_position = self
            .pending_join
            .as_ref()
            .map_or(event.max_log_position, |next| next.log_position);
        self.log = Some(ActiveLog {
            max_log_position,
            reassembler: Reassembler::default(),
        });
        self.ack(event.log_position, NULL_VALUE, None);
        self.shared.member_id = event.member_id;
        self.actions.push_back(Action::MemberId(event.member_id));
        if event.role == Role::Leader {
            let ids: Vec<i64> = self.shared.sessions.keys().copied().collect();
            for id in ids {
                if self.config.responding && !event.is_startup {
                    self.connect(id);
                }
                if let Some(session) = self.shared.sessions.get_mut(&id) {
                    session.closing = false;
                }
            }
        }
        self.set_role(event.role);
    }

    fn connect(&mut self, session_id: i64) {
        if let Some(session) = self.shared.sessions.get_mut(&session_id)
            && !session.connected
        {
            session.connected = true;
            self.actions.push_back(Action::ConnectEgress {
                session_id,
                channel: session.response_channel.clone(),
                stream_id: session.response_stream_id,
            });
        }
    }

    fn disconnect(&mut self, session: &mut ClientSession) {
        if session.connected {
            session.connected = false;
            self.shared.out.clear_egress(session.id);
            self.actions
                .push_back(Action::DisconnectEgress { session_id: session.id });
        }
    }

    fn disconnect_egress(&mut self) {
        let ids: Vec<i64> = self
            .shared
            .sessions
            .values()
            .filter(|s| s.connected)
            .map(|s| s.id)
            .collect();
        for id in ids {
            if let Some(mut session) = self.shared.sessions.remove(&id) {
                self.disconnect(&mut session);
                self.shared.sessions.insert(id, session);
            }
        }
    }

    fn set_role(&mut self, role: Role) {
        if role != self.shared.role {
            self.shared.role = role;
            let cx = ReadCx { shared: &self.shared };
            self.service.on_role_change(&cx, role);
        }
    }

    // ----- the log -----

    /// How far the log may be polled: `Some` the commit position, or `None`
    /// while the container cannot take more (no log, a snapshot under way,
    /// its queues full).
    pub fn log_poll_limit(&self, commit_position: i64) -> Option<i64> {
        let ready = matches!(self.phase, Phase::Running)
            && self.log.is_some()
            && self.snapshot.is_none()
            && self.shared.out.cm_len() < self.config.cm_high_water
            && self.shared.out.egress_len() < self.config.egress_high_water;
        ready.then_some(commit_position)
    }

    /// A frame of the log.
    pub fn on_log_fragment(&mut self, fragment: Fragment<'_>) -> PollAction {
        let Some(log) = self.log.as_mut() else {
            return PollAction::Abort;
        };
        let mut reassembler = std::mem::take(&mut log.reassembler);
        let action = reassembler.on_fragment(fragment, |message, position| self.on_log_message(message, position));
        if let Some(log) = self.log.as_mut() {
            log.reassembler = reassembler;
        }
        action
    }

    /// A poll of the log delivered nothing: is the log done?
    pub fn on_log_idle(&mut self, status: ImageStatus) {
        let Some(log) = self.log.as_ref() else {
            return;
        };
        if status.position >= log.max_log_position || status.end_of_stream || status.closed {
            self.shared.log_position = self.shared.log_position.max(status.position);
            self.log = None;
            self.actions.push_back(Action::CloseLog);
            self.disconnect_egress();
            self.set_role(Role::Follower);
        }
    }

    fn on_log_message(&mut self, message: &[u8], position: i64) -> PollAction {
        if !matches!(self.phase, Phase::Running) {
            return PollAction::Abort;
        }
        match self.dispatch(message, position) {
            // A message that filled a queue ends the poll: the log waits until it drains.
            Ok(PollAction::Continue)
                if self.shared.out.cm_len() >= self.config.cm_high_water
                    || self.shared.out.egress_len() >= self.config.egress_high_water =>
            {
                PollAction::Break
            }
            Ok(action) => action,
            Err(e) => {
                self.fail_stop(format!("log at {position}: {e}"));
                PollAction::Break
            }
        }
    }

    /// Runs a callback that may be asked again: what it sent is kept only if
    /// it applied its entry, and the log's position and time go back if not.
    fn apply(
        &mut self,
        log_position: i64,
        timestamp: i64,
        callback: impl FnOnce(&mut S, &mut Cx<'_>) -> Apply,
    ) -> PollAction {
        let before = (self.shared.log_position, self.shared.cluster_time);
        self.shared.log_position = log_position;
        self.shared.cluster_time = timestamp;
        let result = callback(
            &mut self.service,
            &mut Cx {
                shared: &mut self.shared,
            },
        );
        match result {
            Apply::Done => {
                self.shared.out.commit();
                PollAction::Continue
            }
            Apply::Retry => {
                self.shared.out.discard();
                (self.shared.log_position, self.shared.cluster_time) = before;
                PollAction::Abort
            }
        }
    }

    fn dispatch(&mut self, message: &[u8], position: i64) -> Result<PollAction, String> {
        let header = decode::header(message)?;
        match header.template_id {
            1 => {
                let d = decode::body(message, &header, 24, |b, h| {
                    SessionMessageHeaderDecoder::default().wrap(b, HEADER, h.block_length, h.version)
                })?;
                let (cluster_session_id, timestamp) = (d.cluster_session_id(), d.timestamp());
                let payload = message
                    .get(SESSION_HEADER_LENGTH..)
                    .ok_or("session message shorter than its header")?;
                let session_id = self
                    .shared
                    .sessions
                    .contains_key(&cluster_session_id)
                    .then_some(cluster_session_id);
                let m = SessionMessage {
                    session_id,
                    cluster_session_id,
                    timestamp,
                    log_position: position,
                    payload,
                };
                Ok(self.apply(position, timestamp, |service, cx| service.on_session_message(cx, m)))
            }
            20 => {
                let d = decode::body(message, &header, 24, |b, h| {
                    TimerEventDecoder::default().wrap(b, HEADER, h.block_length, h.version)
                })?;
                let (correlation_id, timestamp) = (d.correlation_id(), d.timestamp());
                Ok(self.apply(position, timestamp, |service, cx| {
                    service.on_timer_event(cx, correlation_id, timestamp)
                }))
            }
            21 => {
                let mut d = decode::body(message, &header, 36, |b, h| {
                    SessionOpenEventDecoder::default().wrap(b, HEADER, h.block_length, h.version)
                })?;
                let (leadership_term_id, id, timestamp) =
                    (d.leadership_term_id(), d.cluster_session_id(), d.timestamp());
                let response_stream_id = d.response_stream_id();
                decode::check_var(message, d.get_limit())?;
                let c = d.response_channel_decoder();
                let response_channel = String::from_utf8_lossy(d.response_channel_slice(c)).into_owned();
                decode::check_var(message, d.get_limit())?;
                let p = d.encoded_principal_decoder();
                let encoded_principal = d.encoded_principal_slice(p).to_vec();
                self.shared.log_position = position;
                self.shared.cluster_time = timestamp;
                if self.shared.sessions.contains_key(&id) {
                    return Err(format!(
                        "clashing open clusterSessionId={id} leadershipTermId={leadership_term_id} logPosition={position}"
                    ));
                }
                let session = ClientSession {
                    id,
                    response_stream_id,
                    response_channel,
                    encoded_principal,
                    closing: false,
                    connected: false,
                };
                self.shared.sessions.insert(id, session.clone());
                if self.shared.role == Role::Leader && self.config.responding {
                    self.connect(id);
                }
                self.service.on_session_open(
                    &mut Cx {
                        shared: &mut self.shared,
                    },
                    &session,
                    timestamp,
                );
                self.shared.out.commit();
                Ok(PollAction::Continue)
            }
            22 => {
                let d = decode::body(message, &header, 28, |b, h| {
                    SessionCloseEventDecoder::default().wrap(b, HEADER, h.block_length, h.version)
                })?;
                let (leadership_term_id, id, timestamp) =
                    (d.leadership_term_id(), d.cluster_session_id(), d.timestamp());
                let reason = close_reason(d.close_reason());
                self.shared.log_position = position;
                self.shared.cluster_time = timestamp;
                match self.shared.sessions.remove(&id) {
                    None => self.error(format!(
                        "unknown session close: clusterSessionId={id} closeReason={reason:?} \
                         leadershipTermId={leadership_term_id} logPosition={position}"
                    )),
                    Some(mut session) => {
                        self.disconnect(&mut session);
                        self.service.on_session_close(
                            &mut Cx {
                                shared: &mut self.shared,
                            },
                            &session,
                            timestamp,
                            reason,
                        );
                        self.shared.out.commit();
                    }
                }
                Ok(PollAction::Continue)
            }
            23 => {
                let d = decode::body(message, &header, 32, |b, h| {
                    ClusterActionRequestDecoder::default().wrap(b, HEADER, h.block_length, h.version)
                })?;
                let (leadership_term_id, log_position, timestamp) =
                    (d.leadership_term_id(), d.log_position(), d.timestamp());
                let flags = d.flags().unwrap_or(CLUSTER_ACTION_FLAGS_DEFAULT);
                self.shared.log_position = log_position;
                self.shared.cluster_time = timestamp;
                let take = flags == CLUSTER_ACTION_FLAGS_DEFAULT || flags & self.standby_snapshot_flags != 0;
                if d.action() == ClusterAction::SNAPSHOT && take {
                    self.begin_snapshot(log_position, leadership_term_id);
                    return Ok(PollAction::Break);
                }
                Ok(PollAction::Continue)
            }
            24 => {
                let d = decode::body(message, &header, 48, |b, h| {
                    NewLeadershipTermEventDecoder::default().wrap(b, HEADER, h.block_length, h.version)
                })?;
                let term = NewLeadershipTerm {
                    leadership_term_id: d.leadership_term_id(),
                    log_position: d.log_position(),
                    timestamp: d.timestamp(),
                    term_base_log_position: d.term_base_log_position(),
                    leader_member_id: d.leader_member_id(),
                    log_session_id: d.log_session_id(),
                    time_unit: snapshot::time_unit(d.time_unit()),
                    app_version: d.app_version().unwrap_or(0),
                };
                if major(term.app_version) != major(self.config.app_version) {
                    return Err(format!(
                        "incompatible version: {:#x} log={:#x}",
                        self.config.app_version, term.app_version
                    ));
                }
                self.shared.leadership_term_id = term.leadership_term_id;
                self.shared.log_position = term.log_position;
                self.shared.cluster_time = term.timestamp;
                self.shared.time_unit = Some(term.time_unit);
                self.service.on_new_leadership_term(
                    &mut Cx {
                        shared: &mut self.shared,
                    },
                    &term,
                );
                self.shared.out.commit();
                Ok(PollAction::Continue)
            }
            _ => Ok(PollAction::Continue),
        }
    }

    // ----- snapshots -----

    fn begin_snapshot(&mut self, log_position: i64, leadership_term_id: i64) {
        let Some(time_unit) = self.shared.time_unit else {
            self.error(format!(
                "snapshot at {log_position} requested before the log named its time unit"
            ));
            self.ack(log_position, NULL_VALUE, None);
            return;
        };
        let point = SnapshotPoint {
            log_position,
            leadership_term_id,
            timestamp: self.shared.cluster_time,
        };
        let source = {
            let cx = ReadCx { shared: &self.shared };
            self.service.on_take_snapshot(&cx, point)
        };
        let app_version = self.config.app_version;
        let mut preamble = VecDeque::with_capacity(self.shared.sessions.len() + 2);
        preamble.push_back(snapshot::marker(
            log_position,
            leadership_term_id,
            SnapshotMark::BEGIN,
            time_unit,
            app_version,
        ));
        for session in self.shared.sessions.values() {
            preamble.push_back(snapshot::client_session(session));
        }
        preamble.push_back(snapshot::marker(
            log_position,
            leadership_term_id,
            SnapshotMark::END,
            time_unit,
            app_version,
        ));
        self.snapshot = Some(Taking {
            log_position,
            recording_id: None,
            max_payload: 0,
            preamble,
            source,
            frame: None,
            buffer: Vec::new(),
            written: false,
        });
        self.actions.push_back(Action::RecordSnapshot {
            channel: self.config.snapshot_channel.clone(),
            stream_id: self.config.snapshot_stream_id,
        });
    }

    /// The next frame of the snapshot being taken, once its publication is
    /// recorded: offer it, then call [`Self::snapshot_frame_sent`]. `None`
    /// when there is nothing to offer now.
    pub fn snapshot_frame(&mut self) -> Option<&[u8]> {
        enum Next {
            Ready,
            Wait,
            Written,
            Failed(String),
        }
        let next = {
            let taking = self.snapshot.as_mut()?;
            if taking.recording_id.is_none() || taking.written {
                Next::Wait
            } else if taking.frame.is_some() {
                Next::Ready
            } else if let Some(frame) = taking.preamble.pop_front() {
                taking.frame = Some(frame);
                Next::Ready
            } else {
                taking.buffer.resize(taking.max_payload.max(1), 0);
                match taking.source.pull(&mut taking.buffer) {
                    Pull::Data(0) | Pull::Pending => Next::Wait,
                    Pull::Data(n) => {
                        taking.frame = Some(taking.buffer[..n].to_vec());
                        Next::Ready
                    }
                    Pull::Done => {
                        taking.written = true;
                        Next::Written
                    }
                    Pull::Failed(e) => Next::Failed(e),
                }
            }
        };
        match next {
            Next::Ready => self.snapshot.as_ref().and_then(|t| t.frame.as_deref()),
            Next::Wait => None,
            Next::Written => {
                self.actions.push_back(Action::AwaitSnapshotRecorded);
                None
            }
            Next::Failed(e) => {
                self.error(format!("snapshot failed: {e}"));
                self.finish_snapshot(None);
                None
            }
        }
    }

    /// The frame [`Self::snapshot_frame`] gave was taken by the publication.
    pub fn snapshot_frame_sent(&mut self) {
        if let Some(taking) = self.snapshot.as_mut() {
            taking.frame = None;
        }
    }

    fn finish_snapshot(&mut self, recording_id: Option<i64>) {
        if let Some(taking) = self.snapshot.take() {
            self.ack(taking.log_position, recording_id.unwrap_or(NULL_VALUE), None);
            self.actions.push_back(Action::CloseSnapshotRecording);
        }
    }

    // ----- output -----

    /// Offers what is queued for the consensus module, in order, until a
    /// refusal. Returns how many went.
    pub fn drain_cm(&mut self, offer: impl FnMut(&[u8]) -> Offer) -> usize {
        match self.shared.out.drain_cm(offer) {
            Ok(sent) => sent,
            Err(e) => {
                self.fail_stop(e);
                0
            }
        }
    }

    /// Offers what is queued for each session's client.
    pub fn drain_egress(&mut self, offer: impl FnMut(i64, &[u8]) -> Offer) -> usize {
        let actions = &mut self.actions;
        self.shared
            .out
            .drain_egress(self.shared.now_ns, self.config.egress_timeout_ns, offer, |message| {
                actions.push_back(Action::Error { message })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_log_alias_replaces_any_other() {
        assert_eq!(
            with_alias("aeron:udp?endpoint=localhost:20113|alias=log|session-id=7", "log-sc-0"),
            "aeron:udp?endpoint=localhost:20113|session-id=7|alias=log-sc-0"
        );
        assert_eq!(with_alias("aeron:ipc", "log-sc-1"), "aeron:ipc?alias=log-sc-1");
    }
}
