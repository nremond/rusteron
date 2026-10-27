//! The service container's shell: drives a [`Container`] on Aeron's C client,
//! one thread, the client's conductor run on that thread too.
//!
//! Each duty cycle reads the clocks once, then: every millisecond, the
//! client's conductor, the counters' liveness, the mark file and the
//! consensus module's stream; the I/O the container asked for; the log, up to
//! the commit position; a snapshot being loaded or taken; what is queued for
//! the consensus module and for clients.

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusteron_archive::*;
use rusteron_cluster_core::counters::{
    COMMIT_POSITION_TYPE_ID, RECORDING_POSITION_TYPE_ID, RECOVERY_STATE_TYPE_ID, RecordingPosition, RecoveryState,
    is_cluster_counter,
};
use rusteron_cluster_core::log::{Fragment, PollAction, Reassembler};
use rusteron_cluster_core::markfile::{ContainerHeader, ERROR_BUFFER_MIN_LENGTH, UPDATE_INTERVAL_MS};
use rusteron_cluster_core::service::{Action, ClusteredService, Config, Container, ImageStatus, Io, Now, Offer};

use crate::markfile::MarkFile;

/// Aeron's counter state for a counter in use.
const RECORD_ALLOCATED: i32 = 1;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How the container's thread waits when a duty cycle found nothing to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Idle {
    /// Never waits: lowest latency, a core of its own.
    Spin,
    /// Spins, yields, then parks up to a millisecond, as Aeron's default.
    Backoff,
}

/// Where a container finds its cluster and how it runs. The defaults are Aeron's.
#[derive(Clone, Debug)]
pub struct ContainerContext {
    pub config: Config,
    pub aeron_dir: String,
    /// The cluster's directory: the mark file goes there.
    pub cluster_dir: PathBuf,
    pub control_channel: String,
    pub service_stream_id: i32,
    pub consensus_module_stream_id: i32,
    pub archive_control_request_channel: String,
    pub archive_control_request_stream_id: i32,
    pub archive_control_response_channel: String,
    /// `None` for Java's choice: 120 + 100 × cluster id + service id + 1.
    pub archive_control_response_stream_id: Option<i32>,
    pub service_name: String,
    pub file_page_size: usize,
    pub error_buffer_length: usize,
    pub log_fragment_limit: usize,
    pub idle: Idle,
}

impl ContainerContext {
    pub fn new(aeron_dir: impl Into<String>, cluster_dir: impl Into<PathBuf>) -> Self {
        Self {
            config: Config::default(),
            aeron_dir: aeron_dir.into(),
            cluster_dir: cluster_dir.into(),
            control_channel: "aeron:ipc?term-length=128k".into(),
            service_stream_id: 104,
            consensus_module_stream_id: 105,
            archive_control_request_channel: "aeron:ipc?term-length=64k".into(),
            archive_control_request_stream_id: 10,
            archive_control_response_channel: "aeron:ipc?term-length=64k".into(),
            archive_control_response_stream_id: None,
            service_name: "rusteron-cluster-service".into(),
            file_page_size: 4_096,
            error_buffer_length: ERROR_BUFFER_MIN_LENGTH,
            log_fragment_limit: 50,
            idle: Idle::Backoff,
        }
    }

    fn archive_response_stream_id(&self) -> i32 {
        self.archive_control_response_stream_id
            .unwrap_or(120 + 100 * self.config.cluster_id + self.config.service_id + 1)
    }
}

/// Why a container could not run.
#[derive(Debug)]
pub enum ContainerError {
    Aeron(String),
    Io(std::io::Error),
}

impl std::fmt::Display for ContainerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Aeron(e) => write!(f, "aeron: {e}"),
            Self::Io(e) => write!(f, "mark file: {e}"),
        }
    }
}

impl std::error::Error for ContainerError {}

impl From<AeronCError> for ContainerError {
    fn from(e: AeronCError) -> Self {
        Self::Aeron(e.to_string())
    }
}

impl From<AeronArchiveError> for ContainerError {
    fn from(e: AeronArchiveError) -> Self {
        Self::Aeron(e.to_string())
    }
}

impl From<std::io::Error> for ContainerError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// How a container ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exit {
    /// The cluster terminated it at the position it asked for.
    Terminated,
    /// It stopped for anything else: its errors are in the mark file.
    Failed,
}

struct LogState {
    session_id: i32,
    adding: Option<AeronAsyncAddSubscription>,
    subscription: Option<AeronSubscription>,
    image: Option<AeronImage>,
}

struct ReplayState {
    _archive: AeronArchive,
    session_id: i32,
    adding: Option<AeronAsyncAddSubscription>,
    subscription: Option<AeronSubscription>,
    image: Option<AeronImage>,
}

struct RecordingState {
    archive: AeronArchive,
    channel: String,
    stream_id: i32,
    adding: Option<AeronAsyncAddExclusivePublication>,
    publication: Option<AeronExclusivePublication>,
    counter: Option<(i32, i64, i64)>,
    awaiting: bool,
}

#[derive(Default)]
struct Egress {
    adding: Option<AeronAsyncAddPublication>,
    publication: Option<AeronPublication>,
}

/// A clustered service running in its container.
pub struct ServiceContainer<S: ClusteredService> {
    core: Container<S>,
    ctx: ContainerContext,
    aeron: Aeron,
    counters: AeronCountersReader,
    cm_publication: AeronPublication,
    cm_subscription: AeronSubscription,
    cm_reassembler: Reassembler,
    commit_counter: (i32, i64),
    mark: MarkFile,
    log: Option<LogState>,
    replay: Option<ReplayState>,
    recording: Option<RecordingState>,
    egress: HashMap<i64, Egress>,
    started: Instant,
    mark_deadline_ms: i64,
    stopping: Option<bool>,
    idle_count: u32,
}

fn epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn offer_result(result: Result<i64, AeronOfferError>) -> Offer {
    match result {
        Ok(_) => Offer::Sent,
        Err(AeronOfferError::NotConnected) => Offer::NotConnected,
        Err(AeronOfferError::BackPressured | AeronOfferError::AdminAction) => Offer::Retry,
        Err(_) => Offer::Closed,
    }
}

fn controlled(action: PollAction) -> bindings::aeron_controlled_fragment_handler_action_t {
    use bindings::aeron_controlled_fragment_handler_action_en as Action;
    match action {
        PollAction::Continue => Action::AERON_ACTION_CONTINUE,
        PollAction::Break => Action::AERON_ACTION_BREAK,
        PollAction::Abort => Action::AERON_ACTION_ABORT,
    }
}

fn fragment<'a>(buffer: &'a [u8], header: &AeronHeader) -> Fragment<'a> {
    let (flags, term_offset) = header
        .get_values()
        .map(|v| (v.frame().flags(), v.frame().term_offset()))
        .unwrap_or((0, -1));
    Fragment {
        payload: buffer,
        flags,
        term_offset,
        position: header.position(),
    }
}

/// Runs `f`, a call into the service from inside a C callback: a panic there
/// must not unwind through C. It becomes a fatal error, reported once out.
fn shield(panicked: &mut Option<String>, f: impl FnOnce() -> PollAction) -> PollAction {
    if panicked.is_some() {
        return PollAction::Abort;
    }
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(action) => action,
        Err(cause) => {
            let message = cause
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| cause.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panic".into());
            *panicked = Some(format!("service panicked: {message}"));
            PollAction::Abort
        }
    }
}

fn find_counter(counters: &AeronCountersReader, matches: impl Fn(i32, &[u8]) -> bool) -> Option<(i32, Vec<u8>)> {
    let mut found = None;
    counters.foreach_counter_fn(|_value, id, type_id, key, _label| {
        if found.is_none() && matches(type_id, key) {
            found = Some((id, key.to_vec()));
        }
    });
    found
}

impl<S: ClusteredService> ServiceContainer<S> {
    /// Connects to the media driver, writes the mark file, and waits for the
    /// consensus module's counters: the container is then started, its
    /// snapshot (if any) being loaded.
    pub fn launch(ctx: ContainerContext, service: S) -> Result<Self, ContainerError> {
        let context = AeronContext::new()?;
        context.set_dir(&ctx.aeron_dir.as_str().into_c_string())?;
        context.set_use_conductor_agent_invoker(true)?;
        let aeron = Aeron::new(&context)?;
        aeron.start()?;

        let now_ms = epoch_ms();
        let header = ContainerHeader {
            start_timestamp_ms: now_ms,
            pid: std::process::id() as i64,
            archive_stream_id: ctx.archive_control_request_stream_id,
            service_stream_id: ctx.service_stream_id,
            consensus_module_stream_id: ctx.consensus_module_stream_id,
            service_id: ctx.config.service_id,
            cluster_id: ctx.config.cluster_id,
            error_buffer_length: ctx.error_buffer_length as i32,
            aeron_directory: ctx.aeron_dir.clone(),
            control_channel: ctx.control_channel.clone(),
            service_name: ctx.service_name.clone(),
        };
        let mark = MarkFile::create(&ctx.cluster_dir, &header, ctx.file_page_size, now_ms)?;

        let control = ctx.control_channel.as_str().into_c_string();
        let cm_publication = aeron.add_publication(&control, ctx.consensus_module_stream_id, CONNECT_TIMEOUT)?;
        let cm_subscription = aeron.add_subscription(
            &control,
            ctx.service_stream_id,
            Handlers::NONE,
            Handlers::NONE,
            CONNECT_TIMEOUT,
        )?;
        let counters = aeron.counters_reader();
        let cluster_id = ctx.config.cluster_id;

        let commit = loop {
            aeron.main_do_work()?;
            if let Some((id, _)) = find_counter(&counters, |t, key| {
                t == COMMIT_POSITION_TYPE_ID && is_cluster_counter(key, cluster_id)
            }) {
                break (id, counters.counter_registration_id(id)?);
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        let recovery_key = loop {
            aeron.main_do_work()?;
            if let Some((_, key)) = find_counter(&counters, |t, key| {
                t == RECOVERY_STATE_TYPE_ID && RecoveryState::is_for_cluster(key, cluster_id)
            }) {
                break key;
            }
            std::thread::sleep(Duration::from_millis(1));
        };

        let mut core = Container::new(ctx.config.clone(), service);
        core.on_counters(&recovery_key, aeron.client_id());
        Ok(Self {
            core,
            ctx,
            aeron,
            counters,
            cm_publication,
            cm_subscription,
            cm_reassembler: Reassembler::default(),
            commit_counter: commit,
            mark,
            log: None,
            replay: None,
            recording: None,
            egress: HashMap::new(),
            started: Instant::now(),
            mark_deadline_ms: 0,
            stopping: None,
            idle_count: 0,
        })
    }

    pub fn container(&self) -> &Container<S> {
        &self.core
    }

    pub fn service(&self) -> &S {
        self.core.service()
    }

    /// Runs duty cycles until the container stops, then closes it.
    pub fn run(mut self) -> Exit {
        loop {
            if let Some(exit) = self.do_work() {
                return exit;
            }
        }
    }

    /// One duty cycle, then idling if it found nothing to do. `Some` once the
    /// container has stopped and its last messages to the consensus module are out.
    pub fn do_work(&mut self) -> Option<Exit> {
        let now = Now {
            nanos: self.started.elapsed().as_nanos() as i64,
            epoch_ms: epoch_ms(),
        };
        let tick = self.core.on_tick(now);
        let mut work = tick.work;
        if tick.poll_cm {
            work += self.slow_tick(now);
        }
        work += self.execute_actions();
        work += self.poll_log();
        work += self.poll_replay();
        work += self.poll_recording();
        work += self
            .core
            .drain_cm(|bytes| offer_result(self.cm_publication.offer(bytes)));
        work += self.drain_egress();
        work += self.execute_actions();

        if let Some(expected) = self.stopping
            && (self.core.is_flushed() || self.idle_count > 1_000)
        {
            let _ = self.mark.signal_terminated();
            return Some(if expected { Exit::Terminated } else { Exit::Failed });
        }
        self.idle(work);
        None
    }

    fn idle(&mut self, work: usize) {
        if work > 0 {
            self.idle_count = 0;
            return;
        }
        self.idle_count = self.idle_count.saturating_add(1);
        match self.ctx.idle {
            Idle::Spin => std::hint::spin_loop(),
            Idle::Backoff => match self.idle_count {
                0..=10 => std::hint::spin_loop(),
                11..=20 => std::thread::yield_now(),
                n => std::thread::sleep(Duration::from_micros((1u64 << (n - 20).min(10)).min(1_000))),
            },
        }
    }

    fn record_error(&mut self, message: &str) {
        log::warn!("{message}");
        if let Err(e) = self.mark.record_error(message, epoch_ms()) {
            log::warn!("failed to record an error in the mark file: {e}");
        }
    }

    fn slow_tick(&mut self, now: Now) -> usize {
        let mut work = match self.aeron.main_do_work() {
            Ok(n) => n.max(0) as usize,
            Err(e) => {
                self.record_error(&format!("conductor: {e}"));
                0
            }
        };
        if self.aeron.is_closed() {
            self.core.on_aeron_closed();
            return work;
        }
        let (id, registration_id) = self.commit_counter;
        let alive = self.counters.counter_state(id).ok() == Some(RECORD_ALLOCATED)
            && self.counters.counter_registration_id(id).ok() == Some(registration_id);
        if !alive {
            self.core.on_commit_position_closed();
        }
        if now.epoch_ms >= self.mark_deadline_ms {
            self.mark_deadline_ms = now.epoch_ms + UPDATE_INTERVAL_MS;
            if let Err(e) = self.mark.update_activity(now.epoch_ms) {
                self.record_error(&format!("mark file: {e}"));
            }
        }
        let core = &mut self.core;
        let reassembler = &mut self.cm_reassembler;
        let polled = self.cm_subscription.poll_fn(
            |buffer, header| {
                reassembler.on_fragment(fragment(buffer, &header), |message, _| {
                    core.on_cm_message(message);
                    PollAction::Continue
                });
            },
            1,
        );
        work += polled.map(|n| n.max(0) as usize).unwrap_or(0);
        self.core.after_cm_poll();
        work
    }

    fn execute_actions(&mut self) -> usize {
        let mut work = 0;
        while let Some(action) = self.core.poll_action() {
            work += 1;
            if let Err(e) = self.execute(action) {
                self.core.on_fatal(format!("{e}"));
            }
        }
        work
    }

    fn connect_archive(&self) -> Result<AeronArchive, ContainerError> {
        let context = AeronArchiveContext::new()?;
        context.set_aeron(&self.aeron)?;
        context.set_control_request_channel(&self.ctx.archive_control_request_channel.as_str().into_c_string())?;
        context.set_control_request_stream_id(self.ctx.archive_control_request_stream_id)?;
        context.set_control_response_channel(&self.ctx.archive_control_response_channel.as_str().into_c_string())?;
        context.set_control_response_stream_id(self.ctx.archive_response_stream_id())?;
        Ok(AeronArchiveAsyncConnect::new_with_aeron(&context, &self.aeron)?.poll_blocking(CONNECT_TIMEOUT)?)
    }

    fn execute(&mut self, action: Action) -> Result<(), ContainerError> {
        match action {
            Action::JoinLog {
                channel,
                stream_id,
                session_id,
            } => {
                let adding = self.aeron.async_add_subscription(
                    &channel.as_str().into_c_string(),
                    stream_id,
                    Handlers::NONE,
                    Handlers::NONE,
                )?;
                self.log = Some(LogState {
                    session_id,
                    adding: Some(adding),
                    subscription: None,
                    image: None,
                });
            }
            Action::CloseLog => {
                if let Some(mut log) = self.log.take() {
                    log.image = None;
                }
            }
            Action::ReplaySnapshot {
                recording_id,
                channel,
                stream_id,
            } => {
                let archive = self.connect_archive()?;
                let params = AeronArchiveReplayParams::new(-1, -1, 0, -1, -1, -1)?;
                let replay_session_id =
                    archive.start_replay(recording_id, &channel.as_str().into_c_string(), stream_id, &params)?;
                let session_id = replay_session_id as i32;
                let channel = format!(
                    "{channel}{}session-id={session_id}",
                    if channel.contains('?') { "|" } else { "?" }
                );
                let adding = self.aeron.async_add_subscription(
                    &channel.as_str().into_c_string(),
                    stream_id,
                    Handlers::NONE,
                    Handlers::NONE,
                )?;
                self.replay = Some(ReplayState {
                    _archive: archive,
                    session_id,
                    adding: Some(adding),
                    subscription: None,
                    image: None,
                });
            }
            Action::CloseSnapshotReplay => {
                if let Some(mut replay) = self.replay.take() {
                    replay.image = None;
                }
            }
            Action::RecordSnapshot { channel, stream_id } => {
                let archive = self.connect_archive()?;
                let adding = self
                    .aeron
                    .async_add_exclusive_publication(&channel.as_str().into_c_string(), stream_id)?;
                self.recording = Some(RecordingState {
                    archive,
                    channel,
                    stream_id,
                    adding: Some(adding),
                    publication: None,
                    counter: None,
                    awaiting: false,
                });
            }
            Action::AwaitSnapshotRecorded => {
                if let Some(recording) = self.recording.as_mut() {
                    recording.awaiting = true;
                }
            }
            Action::CloseSnapshotRecording => {
                self.recording = None;
            }
            Action::ConnectEgress {
                session_id,
                channel,
                stream_id,
            } => {
                let adding = self
                    .aeron
                    .async_add_publication(&channel.as_str().into_c_string(), stream_id)?;
                self.egress.insert(
                    session_id,
                    Egress {
                        adding: Some(adding),
                        publication: None,
                    },
                );
            }
            Action::DisconnectEgress { session_id } => {
                self.egress.remove(&session_id);
            }
            Action::MemberId(member_id) => {
                if let Err(e) = self.mark.member_id(member_id) {
                    self.record_error(&format!("mark file: {e}"));
                }
            }
            Action::Error { message } => self.record_error(&message),
            Action::Stop { expected } => {
                self.stopping = Some(expected);
            }
        }
        Ok(())
    }

    fn poll_log(&mut self) -> usize {
        let Some(log) = self.log.as_mut() else { return 0 };
        if let Some(adding) = log.adding.as_ref() {
            match adding.poll() {
                Ok(Some(subscription)) => {
                    log.subscription = Some(subscription);
                    log.adding = None;
                }
                Ok(None) => return 0,
                Err(e) => {
                    self.log = None;
                    self.core.on_io(Io::LogSubscriptionFailed { message: e.to_string() });
                    return 1;
                }
            }
        }
        let Some(subscription) = log.subscription.as_ref() else {
            return 0;
        };
        if log.image.is_none() {
            let Some(image) = subscription.image_by_session_id(log.session_id) else {
                return 0;
            };
            let join_position = image.get_constants().map(|c| c.join_position()).unwrap_or(-1);
            log.image = Some(image);
            self.core.on_io(Io::LogImage { join_position });
            return 1;
        }
        let commit = self.counters.get_counter_value(self.commit_counter.0);
        let Some(limit) = self.core.log_poll_limit(commit) else {
            return 0;
        };
        let Some(image) = log.image.clone() else { return 0 };
        let core = &mut self.core;
        let mut panicked = None;
        let polled = image.bounded_controlled_poll_fn(
            |buffer, header| {
                controlled(shield(&mut panicked, || {
                    core.on_log_fragment(fragment(buffer, &header))
                }))
            },
            limit,
            self.ctx.log_fragment_limit,
        );
        if let Some(message) = panicked {
            self.core.on_fatal(message);
            return 1;
        }
        match polled {
            Ok(0) => {
                self.core.on_log_idle(ImageStatus {
                    position: image.position(),
                    end_of_stream: image.is_end_of_stream(),
                    closed: image.is_closed(),
                });
                0
            }
            Ok(n) => n as usize,
            Err(e) => {
                self.core.on_fatal(format!("log poll: {e}"));
                1
            }
        }
    }

    fn poll_replay(&mut self) -> usize {
        let Some(replay) = self.replay.as_mut() else { return 0 };
        if let Some(adding) = replay.adding.as_ref() {
            match adding.poll() {
                Ok(Some(subscription)) => {
                    replay.subscription = Some(subscription);
                    replay.adding = None;
                }
                Ok(None) => return 0,
                Err(e) => {
                    self.replay = None;
                    self.core.on_io(Io::SnapshotReplayFailed { message: e.to_string() });
                    return 1;
                }
            }
        }
        let Some(subscription) = replay.subscription.as_ref() else {
            return 0;
        };
        if replay.image.is_none() {
            replay.image = subscription.image_by_session_id(replay.session_id);
            return replay.image.is_some() as usize;
        }
        let Some(image) = replay.image.clone() else { return 0 };
        let core = &mut self.core;
        let mut panicked = None;
        let polled = image.controlled_poll_fn(
            |buffer, header| {
                controlled(shield(&mut panicked, || {
                    core.on_snapshot_fragment(fragment(buffer, &header))
                }))
            },
            10,
        );
        if let Some(message) = panicked {
            self.core.on_fatal(message);
            return 1;
        }
        match polled {
            Ok(0) if image.is_end_of_stream() || image.is_closed() => {
                self.core.on_snapshot_replay_end();
                1
            }
            Ok(n) => n as usize,
            Err(e) => {
                self.core.on_io(Io::SnapshotReplayFailed { message: e.to_string() });
                1
            }
        }
    }

    fn poll_recording(&mut self) -> usize {
        let Some(recording) = self.recording.as_mut() else {
            return 0;
        };
        if let Some(adding) = recording.adding.as_ref() {
            match adding.poll() {
                Ok(Some(publication)) => {
                    let session_id = publication.get_constants().map(|c| c.session_id()).unwrap_or(0);
                    recording.publication = Some(publication);
                    recording.adding = None;
                    let channel = format!(
                        "{}{}session-id={session_id}",
                        recording.channel,
                        if recording.channel.contains('?') { "|" } else { "?" }
                    );
                    if let Err(e) = recording.archive.start_recording(
                        &channel.as_str().into_c_string(),
                        recording.stream_id,
                        SOURCE_LOCATION_LOCAL,
                        true,
                    ) {
                        self.core.on_io(Io::SnapshotFailed {
                            message: format!("start recording: {e}"),
                        });
                    }
                    return 1;
                }
                Ok(None) => return 0,
                Err(e) => {
                    self.core.on_io(Io::SnapshotFailed {
                        message: format!("snapshot publication: {e}"),
                    });
                    return 1;
                }
            }
        }
        let Some(publication) = recording.publication.clone() else {
            return 0;
        };
        if recording.counter.is_none() {
            let session_id = publication.get_constants().map(|c| c.session_id()).unwrap_or(0);
            let archive_id = recording.archive.archive_id();
            let found = find_counter(&self.counters, |type_id, key| {
                type_id == RECORDING_POSITION_TYPE_ID
                    && RecordingPosition::parse(key)
                        .is_ok_and(|r| r.session_id == session_id && r.archive_id == archive_id)
            });
            let Some((counter_id, key)) = found else {
                if let Err(e) = recording.archive.check_for_error_response() {
                    self.core.on_io(Io::SnapshotFailed {
                        message: format!("recording: {e}"),
                    });
                }
                return 0;
            };
            let recording_id = RecordingPosition::parse(&key).map(|r| r.recording_id).unwrap_or(-1);
            let registration_id = self.counters.counter_registration_id(counter_id).unwrap_or(-1);
            recording.counter = Some((counter_id, recording_id, registration_id));
            let max_payload_length = publication.max_payload_length().unwrap_or(1_376);
            self.core.on_io(Io::SnapshotPublication {
                recording_id,
                max_payload_length,
            });
            return 1;
        }
        let mut work = 0;
        while let Some(frame) = self.core.snapshot_frame() {
            match offer_result(publication.offer(frame)) {
                Offer::Sent => {
                    self.core.snapshot_frame_sent();
                    work += 1;
                }
                Offer::Retry | Offer::NotConnected => break,
                Offer::Closed => {
                    self.core.on_io(Io::SnapshotFailed {
                        message: "snapshot publication closed".into(),
                    });
                    return 1;
                }
            }
        }
        let Some(recording) = self.recording.as_mut() else {
            return work;
        };
        if recording.awaiting
            && let Some((counter_id, recording_id, registration_id)) = recording.counter
        {
            let position = publication.position();
            if self.counters.get_counter_value(counter_id) >= position {
                recording.awaiting = false;
                self.core.on_io(Io::SnapshotRecorded);
                return work + 1;
            }
            // The same counter, still in use: its registration id names one allocation, as the recording id in
            // its key does for Java's RecordingPos.isActive. (rusteron's get_counter_key reads a key up to its
            // first zero byte, which cuts a binary key short.)
            let active = self.counters.counter_state(counter_id).ok() == Some(RECORD_ALLOCATED)
                && self.counters.counter_registration_id(counter_id).ok() == Some(registration_id);
            if !active {
                self.core.on_io(Io::SnapshotFailed {
                    message: format!("recording stopped unexpectedly: {recording_id}"),
                });
                return work + 1;
            }
            if let Err(e) = recording.archive.check_for_error_response() {
                self.core.on_io(Io::SnapshotFailed {
                    message: format!("recording: {e}"),
                });
                return work + 1;
            }
        }
        work
    }

    fn drain_egress(&mut self) -> usize {
        let mut work = 0;
        for egress in self.egress.values_mut() {
            if let Some(adding) = egress.adding.as_ref() {
                match adding.poll() {
                    Ok(Some(publication)) => {
                        egress.publication = Some(publication);
                        egress.adding = None;
                        work += 1;
                    }
                    Ok(None) => {}
                    Err(_) => egress.adding = None,
                }
            }
        }
        let egress = &self.egress;
        work += self.core.drain_egress(|session_id, bytes| {
            match egress.get(&session_id).and_then(|e| e.publication.as_ref()) {
                Some(publication) => offer_result(publication.offer(bytes)),
                None => Offer::NotConnected,
            }
        });
        work
    }
}

impl<S: ClusteredService> Drop for ServiceContainer<S> {
    fn drop(&mut self) {
        if let Some(mut log) = self.log.take() {
            log.image = None;
        }
        if let Some(mut replay) = self.replay.take() {
            replay.image = None;
        }
        self.recording = None;
        self.egress.clear();
    }
}
