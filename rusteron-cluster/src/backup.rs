//! The cluster backup's shell: drives a [`ClusterBackupAgent`] on Aeron's
//! C client, one thread, the client's conductor run on it, as Java's
//! `ClusterBackup` runs its agent by default.
//!
//! Each duty cycle reads the clock once, then: every millisecond, the
//! client's conductor, counters gone, the mark file; what members send on
//! the consensus stream, both archives' responses; the backup's work; what
//! it asked for; its events to the listener (Java's
//! `ClusterBackupEventsListener`), on that thread.
//!
//! The `cluster_backup` binary runs one; a process embeds one with
//! [`ClusterBackupNode::launch`] and [`ClusterBackupNode::run`] on a thread
//! of its own, or [`spawn`] for a thread the node owns.
//!
//! ```no_run
//! # use rusteron_cluster::backup::*;
//! # fn run() -> Result<(), BackupError> {
//! let mut ctx = BackupContext::new("/dev/shm/aeron-standby", "/var/lib/standby/cluster");
//! ctx.config.cluster_consensus_endpoints = "node0:20002,node1:20002,node2:20002".into();
//! ctx.config.consensus_channel = "aeron:udp?endpoint=standby:20002".into();
//! ctx.config.catchup_endpoint = "standby:20003".into();
//! let handle = spawn(ctx, ())?;
//! // ...
//! handle.stop();
//! let exit = handle.join();
//! # Ok(()) }
//! ```

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusteron_archive::*;
use rusteron_cluster_core::counters::{self as cluster_counters, RECORDING_POSITION_TYPE_ID, RecordingPosition};
use rusteron_cluster_core::log::{Fragment, Reassembler};
use rusteron_cluster_core::markfile::{self, BackupHeader, ERROR_BUFFER_MIN_LENGTH, UPDATE_INTERVAL_MS};
use rusteron_cluster_core::recording_log;
pub use rusteron_cluster_core::recording_log::{RecordingLog, Snapshot};
use rusteron_consensus_core::backup::{Action, ArchiveId, BackupCounter, CONSENSUS_FRAGMENT_LIMIT, Event, Io, Ports};
pub use rusteron_consensus_core::backup::{
    ArchiveConfig, ClusterBackupAgent, Config, FixedCredentials, ReplayStart, SourceType, State,
};
use rusteron_consensus_core::io::{Offer, RecordingCounter};
pub use rusteron_consensus_core::members::ClusterMember;

use crate::container::{Idle, find_counter};
use crate::counters::{add, get, load, store};
use crate::markfile::MarkFile;

/// Aeron's counter state for a counter in use.
const RECORD_ALLOCATED: i32 = 1;
const ADD_TIMEOUT: Duration = Duration::from_secs(10);
/// The archives' fragment limit, a poll.
const ARCHIVE_FRAGMENT_LIMIT: usize = 10;

/// Where a backup keeps its files, and how it runs. The defaults are Aeron's.
#[derive(Clone, Debug)]
pub struct BackupContext {
    pub config: Config,
    pub aeron_dir: String,
    /// The recording log's directory, a node's cluster directory once it starts from the backup.
    pub cluster_dir: PathBuf,
    /// Where the mark file goes, the cluster directory if none.
    pub mark_file_dir: Option<PathBuf>,
    /// The cluster directory deleted first: a backup from scratch.
    pub delete_dir_on_start: bool,
    pub credentials: FixedCredentials,
    pub file_page_size: usize,
    pub error_buffer_length: usize,
    pub idle: Idle,
}

impl BackupContext {
    pub fn new(aeron_dir: impl Into<String>, cluster_dir: impl Into<PathBuf>) -> Self {
        Self {
            config: Config::default(),
            aeron_dir: aeron_dir.into(),
            cluster_dir: cluster_dir.into(),
            mark_file_dir: None,
            delete_dir_on_start: false,
            credentials: FixedCredentials::default(),
            file_page_size: 4_096,
            error_buffer_length: ERROR_BUFFER_MIN_LENGTH,
            idle: Idle::Backoff,
        }
    }
}

/// Why a backup could not start.
#[derive(Debug)]
pub enum BackupError {
    Aeron(String),
    Io(io::Error),
    Config(String),
}

impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Aeron(e) => write!(f, "aeron: {e}"),
            Self::Io(e) => write!(f, "files: {e}"),
            Self::Config(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for BackupError {}

impl From<AeronCError> for BackupError {
    fn from(e: AeronCError) -> Self {
        Self::Aeron(e.to_string())
    }
}

impl From<io::Error> for BackupError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

/// How a backup ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Exit {
    /// Stopped by its owner.
    Stopped,
    /// Ended on its own, Java's `AgentTerminationException`: the archive
    /// gone or full, the recording log unwritable, the client closed.
    Terminated(String),
}

/// What a backup tells its owner, on its thread (Java's
/// `ClusterBackupEventsListener`, and its termination hook).
pub trait BackupListener {
    /// A query begins.
    fn on_backup_query(&mut self) {}
    /// Something failed or stalled: the backup starts over.
    fn on_possible_failure(&mut self, _message: &str) {}
    /// A member answered: the cluster, the member the log is copied from,
    /// and the snapshots to copy.
    fn on_backup_response(
        &mut self,
        _cluster_members: &[ClusterMember],
        _log_source_member: &ClusterMember,
        _snapshots_to_retrieve: &[Snapshot],
    ) {
    }
    /// The recording log changed, with the snapshots copied since the last change.
    fn on_updated_recording_log(&mut self, _recording_log: &RecordingLog, _snapshots_retrieved: &[Snapshot]) {}
    /// The live log's recording in the local archive grew to `log_position`.
    fn on_live_log_progress(&mut self, _recording_id: i64, _recording_pos_counter_id: i32, _log_position: i64) {}
    /// The backup ended on its own (Java's termination hooks).
    fn on_terminated(&mut self, _reason: &str) {}
}

/// No listener.
impl BackupListener for () {}

fn epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(unix)]
fn write_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, bytes, offset)
}

fn sync_dir(dir: &Path) {
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
}

/// Opens `recording.log`, or creates it and syncs its directory.
fn open_recording_log(dir: &Path) -> io::Result<(File, RecordingLog)> {
    let path = dir.join(recording_log::FILE_NAME);
    let exists = path.exists();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;
    if !exists {
        sync_dir(dir);
        return Ok((file, RecordingLog::new()));
    }
    let bytes = std::fs::read(&path)?;
    let log = RecordingLog::load(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok((file, log))
}

fn offer_result(result: Result<i64, AeronOfferError>) -> Offer {
    match result {
        Ok(_) => Offer::Sent,
        Err(AeronOfferError::NotConnected) => Offer::NotConnected,
        Err(AeronOfferError::BackPressured | AeronOfferError::AdminAction) => Offer::Retry,
        Err(AeronOfferError::MaxPositionExceeded) => Offer::MaxPositionExceeded,
        Err(_) => Offer::Closed,
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

fn registration_id(subscription: &AeronSubscription) -> i64 {
    subscription.get_constants().map(|c| c.registration_id()).unwrap_or(-1)
}

/// The counters gone, as the client's callback reports them.
#[derive(Clone, Default)]
struct CountersGone(Rc<RefCell<Vec<i32>>>);

impl AeronUnavailableCounterCallback for CountersGone {
    fn handle_aeron_on_unavailable_counter(&mut self, _: AeronCountersReader, _: i64, counter_id: i32) {
        self.0.borrow_mut().push(counter_id);
    }
}

/// The backup's counters on the driver.
struct Counters {
    errors: AeronCounter,
    state: AeronCounter,
    live_log_position: AeronCounter,
    next_query_deadline_ms: AeronCounter,
    snapshots_retrieved: AeronCounter,
}

impl Counters {
    fn get(&self, counter: BackupCounter) -> &AeronCounter {
        match counter {
            BackupCounter::State => &self.state,
            BackupCounter::LiveLogPosition => &self.live_log_position,
            BackupCounter::NextQueryDeadlineMs => &self.next_query_deadline_ms,
            BackupCounter::SnapshotsRetrieved => &self.snapshots_retrieved,
        }
    }
}

/// An archive's control publication and response subscription.
struct ArchiveLink {
    publication: AeronExclusivePublication,
    subscription: AeronSubscription,
    response_channel: String,
    /// A response channel's (`control-mode=response`): sent as given.
    is_response_channel: bool,
}

/// What the backup reaches through the shell: everything but the backup itself.
struct BackupIo {
    aeron: Aeron,
    counters_reader: AeronCountersReader,
    counters: Counters,
    consensus_publication: Option<AeronExclusivePublication>,
    local: Option<ArchiveLink>,
    cluster: Option<ArchiveLink>,
    recording_subscription: Option<AeronSubscription>,
}

impl BackupIo {
    fn link(&self, archive: ArchiveId) -> Option<&ArchiveLink> {
        match archive {
            ArchiveId::Local => self.local.as_ref(),
            ArchiveId::Cluster => self.cluster.as_ref(),
        }
    }
}

impl Ports for BackupIo {
    fn offer_consensus(&mut self, message: &[u8]) -> Offer {
        match &self.consensus_publication {
            Some(p) => offer_result(p.offer_parts(&[message])),
            None => Offer::NotConnected,
        }
    }

    fn consensus_connected(&self) -> bool {
        self.consensus_publication.as_ref().is_some_and(|p| p.is_connected())
    }

    fn offer_archive(&mut self, archive: ArchiveId, message: &[u8]) -> Offer {
        match self.link(archive) {
            Some(link) => offer_result(link.publication.offer_parts(&[message])),
            None => Offer::NotConnected,
        }
    }

    fn archive_publication_connected(&self, archive: ArchiveId) -> bool {
        self.link(archive).is_some_and(|l| l.publication.is_connected())
    }

    fn archive_subscription_connected(&self, archive: ArchiveId) -> bool {
        self.link(archive).is_some_and(|l| l.subscription.is_connected())
    }

    fn archive_response_channel(&self, archive: ArchiveId) -> Option<String> {
        let link = self.link(archive)?;
        if link.is_response_channel || !link.response_channel.starts_with("aeron:udp") {
            return Some(link.response_channel.clone());
        }
        link.subscription
            .try_resolve_channel_endpoint_port_as_string(4_096)
            .ok()
            .filter(|c| !c.is_empty() && !c.contains(":0|") && !c.ends_with(":0"))
    }

    fn next_correlation_id(&mut self) -> i64 {
        self.aeron.next_correlation_id()
    }

    fn recording_subscription_endpoint(&self) -> Option<String> {
        self.recording_subscription
            .as_ref()
            .and_then(|s| s.resolved_endpoint_as_string(256).ok())
            .filter(|e| !e.is_empty())
    }

    fn find_recording_counter(&self, session_id: i32, archive_id: i64) -> Option<RecordingCounter> {
        let (counter_id, key) = find_counter(&self.counters_reader, |type_id, key| {
            type_id == RECORDING_POSITION_TYPE_ID
                && RecordingPosition::parse(key).is_ok_and(|r| r.session_id == session_id && r.archive_id == archive_id)
        })?;
        let registration_id = self.counters_reader.counter_registration_id(counter_id).ok()?;
        if registration_id == 0 {
            return None;
        }
        let recording = RecordingPosition::parse(&key).ok()?;
        Some(RecordingCounter {
            counter_id,
            registration_id,
            recording_id: recording.recording_id,
        })
    }

    fn find_recording_counter_by_recording(&self, recording_id: i64, archive_id: i64) -> Option<RecordingCounter> {
        let (counter_id, _) = find_counter(&self.counters_reader, |type_id, key| {
            type_id == RECORDING_POSITION_TYPE_ID
                && RecordingPosition::parse(key)
                    .is_ok_and(|r| r.recording_id == recording_id && r.archive_id == archive_id)
        })?;
        let registration_id = self.counters_reader.counter_registration_id(counter_id).ok()?;
        if registration_id == 0 {
            return None;
        }
        Some(RecordingCounter {
            counter_id,
            registration_id,
            recording_id,
        })
    }

    fn counter_value(&self, counter_id: i32) -> i64 {
        load(&self.counters_reader, counter_id)
    }

    fn is_counter_active(&self, counter_id: i32, registration_id: i64) -> bool {
        self.counters_reader.counter_state(counter_id).ok() == Some(RECORD_ALLOCATED)
            && self.counters_reader.counter_registration_id(counter_id).ok() == Some(registration_id)
    }

    fn counter(&self, counter: BackupCounter) -> i64 {
        get(self.counters.get(counter))
    }

    fn set_counter(&mut self, counter: BackupCounter, value: i64) {
        store(self.counters.get(counter), value);
    }
}

/// A cluster backup running on its driver.
pub struct ClusterBackupNode<L: BackupListener = ()> {
    core: ClusterBackupAgent,
    io: BackupIo,
    listener: L,
    ctx: BackupContext,
    mark: MarkFile,
    recording_log: File,
    consensus_subscription: Option<AeronSubscription>,
    consensus_reassemblers: HashMap<i32, Reassembler>,
    local_reassembler: Reassembler,
    cluster_reassembler: Reassembler,
    counters_gone: CountersGone,
    _counter_handler: Option<Handler<CountersGone>>,
    last_slow_tick_ms: i64,
    mark_deadline_ms: i64,
    idle_count: u32,
    last_state: State,
    exit: Option<Exit>,
}

impl<L: BackupListener> ClusterBackupNode<L> {
    /// Connects to the media driver, writes the mark file, opens the
    /// recording log and allocates the backup's counters: the backup then
    /// connects to its archive, and queries the cluster once it has.
    pub fn launch(ctx: BackupContext, listener: L) -> Result<Self, BackupError> {
        if ctx.delete_dir_on_start && ctx.cluster_dir.exists() {
            std::fs::remove_dir_all(&ctx.cluster_dir)?;
        }
        std::fs::create_dir_all(&ctx.cluster_dir)?;
        let counters_gone = CountersGone::default();
        let context = AeronContext::new()?;
        context.set_dir(&ctx.aeron_dir.as_str().into_c_string())?;
        context.set_use_conductor_agent_invoker(true)?;
        context.set_client_name(&format!("cluster-backup clusterId={}", ctx.config.cluster_id).into_c_string())?;
        let counter_handler = context.set_on_unavailable_counter(Some(counters_gone.clone()))?;
        let aeron = Aeron::new(&context)?;
        aeron.start()?;

        let now_ms = epoch_ms();
        let mark_dir = ctx.mark_file_dir.clone().unwrap_or_else(|| ctx.cluster_dir.clone());
        let header = BackupHeader {
            start_timestamp_ms: now_ms,
            pid: std::process::id() as i64,
            archive_stream_id: ctx.config.local_archive.request_stream_id,
            cluster_id: ctx.config.cluster_id,
            error_buffer_length: ctx.error_buffer_length as i32,
            aeron_directory: ctx.aeron_dir.clone(),
        };
        let bytes = header.encode().map_err(|e| BackupError::Config(e.to_string()))?;
        let mark = MarkFile::create_named(
            &mark_dir,
            markfile::FILE_NAME,
            markfile::LINK_FILE_NAME,
            &bytes,
            ctx.error_buffer_length,
            ctx.file_page_size,
            now_ms,
        )?;
        // Java's `MarkFile.ensureMarkFileLink`: the cluster directory names the mark file's.
        let link = ctx.cluster_dir.join(markfile::LINK_FILE_NAME);
        if mark_dir != ctx.cluster_dir {
            std::fs::write(&link, std::path::absolute(&mark_dir)?.display().to_string())?;
        } else if link.exists() {
            std::fs::remove_file(&link)?;
        }
        let (recording_log, log) = open_recording_log(&ctx.cluster_dir)?;

        let cluster_id = ctx.config.cluster_id;
        let (key, label) = cluster_counters::backup_error_counter(cluster_id);
        let errors = aeron.add_counter(cluster_counters::BACKUP_ERROR_COUNT_TYPE_ID, &key, &label, ADD_TIMEOUT)?;
        let mut allocated = HashMap::new();
        for (type_id, name) in cluster_counters::backup_counters() {
            let (key, label) = cluster_counters::cluster_counter(name, cluster_id);
            allocated.insert(type_id, aeron.add_counter(type_id, &key, &label, ADD_TIMEOUT)?);
        }
        let mut take = |type_id: i32| allocated.remove(&type_id).expect("allocated");
        let counters = Counters {
            errors,
            state: take(cluster_counters::BACKUP_STATE_TYPE_ID),
            live_log_position: take(cluster_counters::BACKUP_LIVE_LOG_POSITION_TYPE_ID),
            next_query_deadline_ms: take(cluster_counters::BACKUP_QUERY_DEADLINE_TYPE_ID),
            snapshots_retrieved: take(cluster_counters::BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID),
        };
        let core = ClusterBackupAgent::new(ctx.config.clone(), log, Box::new(ctx.credentials.clone()), now_ms)
            .map_err(BackupError::Config)?;
        let mut node = Self {
            core,
            io: BackupIo {
                counters_reader: aeron.counters_reader(),
                counters,
                consensus_publication: None,
                local: None,
                cluster: None,
                recording_subscription: None,
                aeron,
            },
            listener,
            mark,
            recording_log,
            consensus_subscription: None,
            consensus_reassemblers: HashMap::new(),
            local_reassembler: Reassembler::default(),
            cluster_reassembler: Reassembler::default(),
            counters_gone,
            _counter_handler: counter_handler,
            last_slow_tick_ms: 0,
            mark_deadline_ms: 0,
            idle_count: 0,
            last_state: State::BackupQuery,
            exit: None,
            ctx,
        };
        node.execute_actions();
        if node.consensus_subscription.is_none() || node.io.local.is_none() {
            let reason = match node.exit.take() {
                Some(Exit::Terminated(reason)) => reason,
                _ => "the consensus subscription or the archive's streams could not be added".into(),
            };
            let _ = node.mark.signal_terminated();
            return Err(BackupError::Aeron(reason));
        }
        Ok(node)
    }

    pub fn state(&self) -> State {
        self.core.state()
    }

    pub fn backup(&self) -> &ClusterBackupAgent {
        &self.core
    }

    pub fn listener(&self) -> &L {
        &self.listener
    }

    /// Runs duty cycles until the backup ends, or `running` says to stop.
    pub fn run(mut self, running: impl Fn() -> bool) -> Exit {
        loop {
            if let Some(exit) = self.do_work() {
                return exit;
            }
            if !running() {
                self.close();
                return Exit::Stopped;
            }
        }
    }

    /// One duty cycle, then idling if it found nothing to do: `Some` once
    /// the backup ended, closed.
    pub fn do_work(&mut self) -> Option<Exit> {
        let now_ms = epoch_ms();
        self.core.on_tick(now_ms);
        let mut work = 0;
        if now_ms != self.last_slow_tick_ms {
            self.last_slow_tick_ms = now_ms;
            work += self.slow_tick(now_ms);
        }
        work += self.poll_consensus();
        work += self.poll_archive(ArchiveId::Local);
        work += self.poll_archive(ArchiveId::Cluster);
        work += self.core.do_work(&mut self.io);
        work += self.execute_actions();
        self.dispatch_events();
        if let Some(exit) = self.exit.clone() {
            self.close();
            if let Exit::Terminated(reason) = &exit {
                self.listener.on_terminated(reason);
            }
            return Some(exit);
        }
        self.idle(work);
        None
    }

    /// Java's `onClose`: the replay and recording stopped, the archives'
    /// sessions closed, the state CLOSED, the mark file let go.
    pub fn close(&mut self) {
        self.core.close(&mut self.io);
        self.execute_actions();
        self.dispatch_events();
        let _ = self.mark.signal_terminated();
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

    /// Counted, and in the mark file's error log as Java's exceptions print
    /// their message: its category first, which Aeron's tests filter on.
    fn record(&mut self, warn: bool, message: &str) {
        let category = if warn {
            log::warn!("{message}");
            "WARN"
        } else {
            log::error!("{message}");
            "ERROR"
        };
        add(&self.io.counters.errors, 1);
        let entry = format!("{category} - {message}");
        if let Err(e) = self.mark.record_error(&entry, epoch_ms()) {
            log::warn!("failed to record an error in the mark file: {e}");
        }
    }

    fn slow_tick(&mut self, now_ms: i64) -> usize {
        let mut work = match self.io.aeron.main_do_work() {
            Ok(n) => n.max(0) as usize,
            Err(e) => {
                self.record(false, &format!("conductor: {e}"));
                0
            }
        };
        if self.io.aeron.is_closed() {
            self.core.on_io(Io::ClientClosed, &mut self.io);
            return work + 1;
        }
        if now_ms >= self.mark_deadline_ms {
            self.mark_deadline_ms = now_ms + UPDATE_INTERVAL_MS;
            if let Err(e) = self.mark.update_activity(now_ms) {
                self.record(false, &format!("mark file: {e}"));
            }
        }
        let gone = std::mem::take(&mut *self.counters_gone.0.borrow_mut());
        work += gone.len();
        for counter_id in gone {
            self.core.on_io(Io::CounterUnavailable { counter_id }, &mut self.io);
        }
        work
    }

    fn poll_consensus(&mut self) -> usize {
        let Some(subscription) = self.consensus_subscription.clone() else {
            return 0;
        };
        let core = &mut self.core;
        let io = &mut self.io;
        let reassemblers = &mut self.consensus_reassemblers;
        subscription
            .poll_fn(
                |buffer, header| {
                    let session_id = header.get_values().map(|v| v.frame().session_id()).unwrap_or(0);
                    let reassembler = reassemblers.entry(session_id).or_default();
                    reassembler.on_fragment(fragment(buffer, &header), |message, _| {
                        core.on_consensus_message(message, io);
                        rusteron_cluster_core::log::PollAction::Continue
                    });
                },
                CONSENSUS_FRAGMENT_LIMIT,
            )
            .map(|n| n.max(0) as usize)
            .unwrap_or(0)
    }

    fn poll_archive(&mut self, archive: ArchiveId) -> usize {
        let Some(subscription) = self.io.link(archive).map(|l| l.subscription.clone()) else {
            return 0;
        };
        let core = &mut self.core;
        let io = &mut self.io;
        let reassembler = match archive {
            ArchiveId::Local => &mut self.local_reassembler,
            ArchiveId::Cluster => &mut self.cluster_reassembler,
        };
        subscription
            .poll_fn(
                |buffer, header| {
                    reassembler.on_fragment(fragment(buffer, &header), |message, _| {
                        core.on_archive_message(archive, message, io);
                        rusteron_cluster_core::log::PollAction::Continue
                    });
                },
                ARCHIVE_FRAGMENT_LIMIT,
            )
            .map(|n| n.max(0) as usize)
            .unwrap_or(0)
    }

    fn dispatch_events(&mut self) {
        while let Some(event) = self.core.poll_event() {
            match event {
                Event::BackupQuery => self.listener.on_backup_query(),
                Event::PossibleFailure { message } => {
                    log::info!("possible failure: {message}");
                    self.listener.on_possible_failure(&message);
                }
                Event::BackupResponse {
                    cluster_members,
                    log_source_member,
                    snapshots_to_retrieve,
                } => {
                    log::info!(
                        "backup response from member {}, {} snapshots to copy",
                        log_source_member.id,
                        snapshots_to_retrieve.len()
                    );
                    self.listener
                        .on_backup_response(&cluster_members, &log_source_member, &snapshots_to_retrieve);
                }
                Event::UpdatedRecordingLog { snapshots_retrieved } => self
                    .listener
                    .on_updated_recording_log(self.core.recording_log(), &snapshots_retrieved),
                Event::LiveLogProgress {
                    recording_id,
                    recording_pos_counter_id,
                    log_position,
                } => self
                    .listener
                    .on_live_log_progress(recording_id, recording_pos_counter_id, log_position),
            }
        }
        let state = self.core.state();
        if state != self.last_state {
            log::info!("state {:?} -> {state:?}", self.last_state);
            self.last_state = state;
        }
    }

    fn execute_actions(&mut self) -> usize {
        let mut work = 0;
        while let Some(action) = self.core.poll_action() {
            work += 1;
            if let Err(e) = self.execute(action) {
                self.core.on_io(Io::AddFailed { message: e.to_string() }, &mut self.io);
            }
        }
        work
    }

    fn add_subscription(&self, channel: &str, stream_id: i32) -> Result<AeronSubscription, BackupError> {
        Ok(self.io.aeron.add_subscription(
            &channel.into_c_string(),
            stream_id,
            Handlers::NONE,
            Handlers::NONE,
            ADD_TIMEOUT,
        )?)
    }

    fn add_publication(&self, channel: &str, stream_id: i32) -> Result<AeronExclusivePublication, BackupError> {
        Ok(self
            .io
            .aeron
            .add_exclusive_publication(&channel.into_c_string(), stream_id, ADD_TIMEOUT)?)
    }

    /// Java's `AsyncConnect` links: on a response channel the subscription
    /// first, the publication then naming it.
    fn add_archive_link(
        &self,
        request_channel: &str,
        request_stream_id: i32,
        response_channel: &str,
        response_stream_id: i32,
    ) -> Result<ArchiveLink, BackupError> {
        let is_response_channel = response_channel.contains("control-mode=response");
        let subscription = self.add_subscription(response_channel, response_stream_id)?;
        let request_channel = if is_response_channel {
            let separator = if request_channel.contains('?') { '|' } else { '?' };
            format!(
                "{request_channel}{separator}response-correlation-id={}",
                registration_id(&subscription)
            )
        } else {
            request_channel.to_owned()
        };
        let publication = self.add_publication(&request_channel, request_stream_id)?;
        Ok(ArchiveLink {
            publication,
            subscription,
            response_channel: response_channel.to_owned(),
            is_response_channel,
        })
    }

    fn execute(&mut self, action: Action) -> Result<(), BackupError> {
        match action {
            Action::AddConsensusSubscription { channel, stream_id } => {
                self.consensus_subscription = Some(self.add_subscription(&channel, stream_id)?);
            }
            Action::AddConsensusPublication { channel, stream_id } => {
                if let Some(p) = self.io.consensus_publication.take() {
                    let _ = p.close();
                }
                self.io.consensus_publication = Some(self.add_publication(&channel, stream_id)?);
            }
            Action::CloseConsensusPublication => {
                if let Some(p) = self.io.consensus_publication.take() {
                    let _ = p.close();
                }
            }
            Action::AddArchive {
                archive,
                request_channel,
                request_stream_id,
                response_channel,
                response_stream_id,
            } => {
                let link = self.add_archive_link(
                    &request_channel,
                    request_stream_id,
                    &response_channel,
                    response_stream_id,
                );
                match archive {
                    ArchiveId::Local => {
                        self.local_reassembler = Reassembler::default();
                        self.io.local = Some(link?);
                    }
                    ArchiveId::Cluster => {
                        self.cluster_reassembler = Reassembler::default();
                        self.io.cluster = Some(link?);
                    }
                }
            }
            Action::CloseArchive { archive } => {
                let link = match archive {
                    ArchiveId::Local => self.io.local.take(),
                    ArchiveId::Cluster => self.io.cluster.take(),
                };
                if let Some(link) = link {
                    let _ = link.publication.close();
                    let _ = link.subscription.close();
                }
            }
            Action::AddRecordingSubscription { channel, stream_id } => {
                self.io.recording_subscription = Some(self.add_subscription(&channel, stream_id)?);
            }
            Action::CloseRecordingSubscription => {
                if let Some(s) = self.io.recording_subscription.take() {
                    let _ = s.close();
                }
            }
            Action::WriteRecordingLog { writes, sync } => {
                let written = writes
                    .iter()
                    .try_for_each(|(offset, bytes)| write_at(&self.recording_log, bytes, *offset))
                    .and_then(|()| if sync { self.recording_log.sync_all() } else { Ok(()) });
                if let Err(e) = written {
                    // Java's force failing: the backup ends.
                    self.record(false, &format!("recording log: {e}"));
                    self.exit = Some(Exit::Terminated("failed to update recording log".into()));
                }
            }
            Action::Error { warn, message } => self.record(warn, &message),
            Action::Terminate { reason } => self.exit = Some(Exit::Terminated(reason)),
        }
        Ok(())
    }
}

/// A backup on a thread of its own: stopped by [`BackupHandle::stop`].
pub struct BackupHandle {
    running: Arc<AtomicBool>,
    thread: Option<JoinHandle<Exit>>,
}

impl BackupHandle {
    /// Asks the backup to stop: it closes at the end of its duty cycle.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Waits for the backup to end: how it did.
    pub fn join(mut self) -> Exit {
        match self.thread.take().map(JoinHandle::join) {
            Some(Ok(exit)) => exit,
            Some(Err(_)) => Exit::Terminated("the backup's thread panicked".into()),
            None => Exit::Stopped,
        }
    }
}

impl Drop for BackupHandle {
    fn drop(&mut self) {
        self.stop();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Launches a backup on a thread of its own, its listener called there:
/// returns once it has started, or why it could not.
pub fn spawn<L: BackupListener + Send + 'static>(ctx: BackupContext, listener: L) -> Result<BackupHandle, BackupError> {
    let running = Arc::new(AtomicBool::new(true));
    let flag = running.clone();
    let (started, launched) = std::sync::mpsc::channel();
    let thread =
        std::thread::Builder::new().name("aeron-cl-bu".into()).spawn(move || {
            match ClusterBackupNode::launch(ctx, listener) {
                Ok(node) => {
                    let _ = started.send(Ok(()));
                    node.run(|| flag.load(Ordering::Acquire))
                }
                Err(e) => {
                    let reason = e.to_string();
                    let _ = started.send(Err(e));
                    Exit::Terminated(reason)
                }
            }
        })?;
    match launched.recv() {
        Ok(Ok(())) => Ok(BackupHandle {
            running,
            thread: Some(thread),
        }),
        Ok(Err(e)) => {
            let _ = thread.join();
            Err(e)
        }
        Err(_) => {
            let _ = thread.join();
            Err(BackupError::Config(
                "the backup's thread ended before it started".into(),
            ))
        }
    }
}
