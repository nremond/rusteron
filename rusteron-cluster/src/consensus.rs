//! The consensus module's shell: drives a [`ConsensusModule`] on Aeron's C
//! client, one thread, the client's conductor run on it, as Java's
//! `ConsensusModule` in its default mode.
//!
//! Each duty cycle reads the cluster clock once, then: every millisecond,
//! the client's conductor, images and counters come and gone, the mark file;
//! the services' and the archive's messages; the module's work; what it
//! asked for; the images it reads (the ingress, replays).

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusteron_archive::*;
use rusteron_cluster_core::counters::{self as cluster_counters, RECORDING_POSITION_TYPE_ID, RecordingPosition};
use rusteron_cluster_core::log::{Fragment, PollAction, Reassembler};
use rusteron_cluster_core::markfile::{
    self, ConsensusModuleHeader, DEFAULT_AUTHENTICATOR, ERROR_BUFFER_MIN_LENGTH, UPDATE_INTERVAL_MS,
};
use rusteron_cluster_core::node_state::{self, NodeState};
use rusteron_cluster_core::recording_log::{self, RecordingLog};
use rusteron_consensus_core::config::{Config, TimeUnit};
use rusteron_consensus_core::io::{Action, CounterId, EgressId, Io, Offer, Ports, RecordingCounter, SubscriptionId};
use rusteron_consensus_core::module::{
    CONSENSUS_FRAGMENT_LIMIT, ConsensusModule, INGRESS_FRAGMENT_LIMIT, LOG_FRAGMENT_LIMIT, Poll,
};
use rusteron_consensus_core::sessions::IngressImage;

use crate::container::{Idle, find_counter};
use crate::counters::{add, compare_and_set, get, load, store};
use crate::markfile::MarkFile;

/// Aeron's counter state for a counter in use.
const RECORD_ALLOCATED: i32 = 1;
/// A channel endpoint's bound socket address (Java's `LocalSocketAddressStatus`).
const LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID: i32 = 14;
const ADD_TIMEOUT: Duration = Duration::from_secs(10);

/// Where a consensus module finds its node and how it runs. The defaults are Aeron's.
#[derive(Clone, Debug)]
pub struct ConsensusContext {
    pub config: Config,
    pub aeron_dir: String,
    /// The cluster's directory: recording log, node state, mark file.
    pub cluster_dir: PathBuf,
    pub archive_control_request_channel: String,
    pub archive_control_response_channel: String,
    pub file_page_size: usize,
    pub error_buffer_length: usize,
    pub cycle_threshold_ns: i64,
    pub total_snapshot_duration_threshold_ns: i64,
    pub idle: Idle,
}

impl ConsensusContext {
    pub fn new(aeron_dir: impl Into<String>, cluster_dir: impl Into<PathBuf>, config: Config) -> Self {
        Self {
            config,
            aeron_dir: aeron_dir.into(),
            cluster_dir: cluster_dir.into(),
            archive_control_request_channel: "aeron:ipc?term-length=64k".into(),
            archive_control_response_channel: "aeron:ipc?term-length=64k".into(),
            file_page_size: 4_096,
            error_buffer_length: ERROR_BUFFER_MIN_LENGTH,
            cycle_threshold_ns: 100_000_000,
            total_snapshot_duration_threshold_ns: 1_000_000_000,
            idle: Idle::Backoff,
        }
    }
}

/// Why a consensus module could not start.
#[derive(Debug)]
pub enum ConsensusError {
    Aeron(String),
    Io(io::Error),
    Config(String),
}

impl std::fmt::Display for ConsensusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Aeron(e) => write!(f, "aeron: {e}"),
            Self::Io(e) => write!(f, "files: {e}"),
            Self::Config(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ConsensusError {}

impl From<AeronCError> for ConsensusError {
    fn from(e: AeronCError) -> Self {
        Self::Aeron(e.to_string())
    }
}

impl From<io::Error> for ConsensusError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

fn epoch_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(unix)]
fn write_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, bytes, offset)
}

fn sync(file: &File, level: i32) -> io::Result<()> {
    match level {
        ..=0 => Ok(()),
        1 => file.sync_data(),
        _ => file.sync_all(),
    }
}

fn sync_dir(dir: &Path) {
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
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

fn position_result(result: Result<i64, AeronOfferError>) -> i64 {
    use rusteron_consensus_core::io::offer_result as r;
    match result {
        Ok(position) => position,
        Err(AeronOfferError::NotConnected) => r::NOT_CONNECTED,
        Err(AeronOfferError::BackPressured) => r::BACK_PRESSURED,
        Err(AeronOfferError::AdminAction) => r::ADMIN_ACTION,
        Err(AeronOfferError::MaxPositionExceeded) => r::MAX_POSITION_EXCEEDED,
        Err(_) => r::CLOSED,
    }
}

fn controlled(action: PollAction) -> bindings::aeron_controlled_fragment_handler_action_t {
    use bindings::aeron_controlled_fragment_handler_action_en as A;
    match action {
        PollAction::Continue => A::AERON_ACTION_CONTINUE,
        PollAction::Break => A::AERON_ACTION_BREAK,
        PollAction::Abort => A::AERON_ACTION_ABORT,
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

/// Images and counters as the client's callbacks report them.
enum Event {
    ImageAvailable {
        registration_id: i64,
        session_id: i32,
        correlation_id: i64,
        join_position: i64,
        term_buffer_length: i32,
        source_identity: String,
    },
    ImageUnavailable {
        registration_id: i64,
        session_id: i32,
        correlation_id: i64,
        end_of_stream: bool,
    },
    CounterUnavailable {
        registration_id: i64,
    },
}

/// Events as the client's conductor reports them, behind a lock: Aeron keeps
/// the callbacks, so they must be `Send` (rusteron 0.3).
#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<Event>>>);

impl Events {
    fn lock(&self) -> MutexGuard<'_, Vec<Event>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn registration_id<S: ResourceStorage<bindings::aeron_subscription_t>>(subscription: &AeronSubscription<S>) -> i64 {
    subscription.get_constants().map(|c| c.registration_id()).unwrap_or(-1)
}

impl AeronAvailableImageCallback for Events {
    fn handle_aeron_on_available_image(&mut self, subscription: &AeronSubscriptionRef, image: &AeronImageRef) {
        if let Ok(c) = image.get_constants() {
            self.lock().push(Event::ImageAvailable {
                registration_id: registration_id(subscription),
                session_id: c.session_id(),
                correlation_id: c.correlation_id(),
                join_position: c.join_position(),
                term_buffer_length: c.term_buffer_length() as i32,
                source_identity: c.source_identity().to_owned(),
            });
        }
    }
}

impl AeronUnavailableImageCallback for Events {
    fn handle_aeron_on_unavailable_image(&mut self, subscription: &AeronSubscriptionRef, image: &AeronImageRef) {
        if let Ok(c) = image.get_constants() {
            self.lock().push(Event::ImageUnavailable {
                registration_id: registration_id(subscription),
                session_id: c.session_id(),
                correlation_id: c.correlation_id(),
                end_of_stream: image.is_end_of_stream(),
            });
        }
    }
}

impl AeronUnavailableCounterCallback for Events {
    fn handle_aeron_on_unavailable_counter(&mut self, _: &AeronCountersReaderRef, registration_id: i64, _: i32) {
        self.lock().push(Event::CounterUnavailable { registration_id });
    }
}

/// The module's counters on the driver.
struct Counters {
    errors: AeronCounter,
    module_state: AeronCounter,
    election_state: AeronCounter,
    election_count: AeronCounter,
    leadership_term_id: AeronCounter,
    node_role: AeronCounter,
    commit_position: AeronCounter,
    control_toggle: AeronCounter,
    node_control_toggle: AeronCounter,
    snapshot_count: AeronCounter,
    timed_out_clients: AeronCounter,
    /// Allocated only when the module accepts standbys' snapshots.
    standby_snapshots: Option<AeronCounter>,
    _others: Vec<AeronCounter>,
    recovery_state: Option<AeronCounter>,
}

impl Counters {
    fn get(&self, counter: CounterId) -> Option<&AeronCounter> {
        Some(match counter {
            CounterId::ModuleState => &self.module_state,
            CounterId::NodeRole => &self.node_role,
            CounterId::CommitPosition => &self.commit_position,
            CounterId::ElectionState => &self.election_state,
            CounterId::ElectionCount => &self.election_count,
            CounterId::LeadershipTermId => &self.leadership_term_id,
            CounterId::SnapshotCount => &self.snapshot_count,
            CounterId::TimedOutClientCount => &self.timed_out_clients,
            CounterId::StandbySnapshotCount => return self.standby_snapshots.as_ref(),
        })
    }
}

#[derive(Default)]
struct Egress {
    adding: Option<AeronAsyncAddPublication>,
    publication: Option<AeronPublication>,
    adding_counter: Option<AeronAsyncAddCounter>,
    counter: Option<AeronCounter>,
    counter_value: i64,
}

/// A destination being added or removed.
enum Destination {
    Publication(AeronAsyncDestination),
    Subscription(AeronAsyncDestination),
}

/// What the module reaches through the shell: everything but the module itself.
struct Node {
    aeron: Aeron,
    counters: Counters,
    log_publication: Option<AeronExclusivePublication>,
    snapshot_publication: Option<AeronExclusivePublication>,
    service_publication: AeronPublication,
    archive_publication: AeronExclusivePublication,
    /// Shared with each poll, which cannot borrow it from the node it hands
    /// the module.
    archive_subscription: Rc<AeronSubscription>,
    archive_response_channel: String,
    egress: HashMap<EgressId, Egress>,
    /// This member's publications to the others, by member id.
    consensus: HashMap<i32, AeronExclusivePublication>,
    subscriptions: HashMap<SubscriptionId, Rc<AeronSubscription>>,
    /// The registration ids of subscriptions removed, while their sockets close.
    removed: HashMap<SubscriptionId, i64>,
    /// `node-state.dat`, written and synced before the module acts on it.
    node_state: File,
    file_sync_level: i32,
}

impl Node {
    /// The driver's counters, a view of the client's.
    fn counters_reader(&self) -> Ref<'_, AeronCountersReader> {
        self.aeron.counters_reader()
    }
}

impl Ports for Node {
    fn log_position(&self) -> i64 {
        self.log_publication.as_ref().map_or(0, |p| p.position())
    }

    fn offer_log(&mut self, parts: &[&[u8]]) -> i64 {
        match &self.log_publication {
            Some(p) => position_result(p.offer_parts(parts)),
            None => rusteron_consensus_core::io::offer_result::NOT_CONNECTED,
        }
    }

    fn egress_connected(&self, egress: EgressId) -> bool {
        self.egress
            .get(&egress)
            .and_then(|e| e.publication.as_ref())
            .is_some_and(|p| p.is_connected())
    }

    fn egress_available_window(&self, egress: EgressId) -> i64 {
        self.egress
            .get(&egress)
            .and_then(|e| e.publication.as_ref())
            .map_or(0, |p| p.position_limit() - p.position())
    }

    fn offer_egress(&mut self, egress: EgressId, message: &[u8]) -> Offer {
        match self.egress.get(&egress).and_then(|e| e.publication.as_ref()) {
            Some(p) => offer_result(p.offer_parts(&[message])),
            None => Offer::NotConnected,
        }
    }

    fn offer_consensus(&mut self, member_id: i32, message: &[u8]) -> Offer {
        match self.consensus.get(&member_id) {
            Some(p) => offer_result(p.offer_parts(&[message])),
            None => Offer::NotConnected,
        }
    }

    fn write_node_state(&mut self, offset: u64, bytes: &[u8]) -> Result<(), String> {
        write_at(&self.node_state, bytes, offset)
            .and_then(|()| sync(&self.node_state, self.file_sync_level))
            .map_err(|e| e.to_string())
    }

    fn offer_service(&mut self, message: &[u8]) -> Offer {
        offer_result(self.service_publication.offer_parts(&[message]))
    }

    fn offer_archive(&mut self, message: &[u8]) -> Offer {
        offer_result(self.archive_publication.offer_parts(&[message]))
    }

    fn archive_publication_connected(&self) -> bool {
        self.archive_publication.is_connected()
    }

    fn archive_subscription_connected(&self) -> bool {
        self.archive_subscription.is_connected()
    }

    fn archive_response_channel(&self) -> Option<String> {
        Some(self.archive_response_channel.clone())
    }

    fn snapshot_position(&self) -> i64 {
        self.snapshot_publication.as_ref().map_or(0, |p| p.position())
    }

    fn offer_snapshot(&mut self, message: &[u8]) -> Offer {
        match &self.snapshot_publication {
            Some(p) => offer_result(p.offer_parts(&[message])),
            None => Offer::NotConnected,
        }
    }

    fn find_recording_counter(&self, session_id: i32, archive_id: i64) -> Option<RecordingCounter> {
        let (counter_id, key) = find_counter(&self.counters_reader(), |type_id, key| {
            type_id == RECORDING_POSITION_TYPE_ID
                && RecordingPosition::parse(key).is_ok_and(|r| r.session_id == session_id && r.archive_id == archive_id)
        })?;
        let registration_id = self.counters_reader().counter_registration_id(counter_id).ok()?;
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
        let (counter_id, _) = find_counter(&self.counters_reader(), |type_id, key| {
            type_id == RECORDING_POSITION_TYPE_ID
                && RecordingPosition::parse(key)
                    .is_ok_and(|r| r.recording_id == recording_id && r.archive_id == archive_id)
        })?;
        let registration_id = self.counters_reader().counter_registration_id(counter_id).ok()?;
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
        load(&self.counters_reader(), counter_id)
    }

    fn is_counter_active(&self, counter_id: i32, registration_id: i64) -> bool {
        let reader = self.counters_reader();
        reader.counter_state(counter_id).ok() == Some(RECORD_ALLOCATED)
            && reader.counter_registration_id(counter_id).ok() == Some(registration_id)
    }

    fn set_counter(&mut self, counter: CounterId, value: i64) {
        if let Some(counter) = self.counters.get(counter) {
            store(counter, value);
        }
    }

    fn counter_id(&self, counter: CounterId) -> i32 {
        self.counters
            .get(counter)
            .and_then(|c| c.get_constants().ok())
            .map_or(-1, |c| c.counter_id())
    }

    fn control_toggle(&self) -> i32 {
        get(&self.counters.control_toggle) as i32
    }

    fn set_control_toggle(&mut self, value: i32) {
        store(&self.counters.control_toggle, i64::from(value));
    }

    fn compare_and_set_control_toggle(&mut self, expected: i32, new: i32) -> bool {
        compare_and_set(&self.counters.control_toggle, i64::from(expected), i64::from(new))
    }

    fn node_control_toggle(&self) -> i32 {
        get(&self.counters.node_control_toggle) as i32
    }

    fn set_node_control_toggle(&mut self, value: i32) {
        store(&self.counters.node_control_toggle, i64::from(value));
    }

    fn next_correlation_id(&mut self) -> i64 {
        self.aeron.next_correlation_id()
    }

    fn has_local_sockets(&self, subscription: SubscriptionId) -> bool {
        let Some(&registration_id) = self.removed.get(&subscription) else {
            return false;
        };
        let reader = self.counters_reader();
        let mut found = false;
        reader.foreach_counter_fn(|_, id, type_id, _, _| {
            found |= type_id == LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID
                && reader.counter_registration_id(id).ok() == Some(registration_id);
        });
        found
    }

    fn resolved_endpoint(&self, subscription: SubscriptionId) -> Option<String> {
        self.subscriptions
            .get(&subscription)
            .and_then(|s| s.resolved_endpoint_as_string(256).ok())
            .filter(|e| !e.is_empty())
    }
}

/// A consensus module running on its node.
pub struct ConsensusModuleNode {
    core: ConsensusModule,
    node: Node,
    ctx: ConsensusContext,
    mark: MarkFile,
    recording_log: File,
    service_subscription: AeronSubscription,
    service_reassembler: Reassembler,
    archive_reassembler: Reassembler,
    by_registration: HashMap<i64, SubscriptionId>,
    destinations: Vec<Destination>,
    images: HashMap<(SubscriptionId, i32), IngressImage>,
    events: Events,
    image_handler: SharedHandler<Events>,
    _counter_handler: Option<SharedHandler<Events>>,
    last_slow_tick_ms: i64,
    mark_deadline_ms: i64,
    idle_count: u32,
    exit: Option<bool>,
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

/// Opens `node-state.dat`, or creates it at its length.
fn open_node_state(dir: &Path, file_sync_level: i32) -> io::Result<(File, NodeState)> {
    let path = dir.join(node_state::FILE_NAME);
    let exists = path.exists();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;
    if exists {
        let bytes = std::fs::read(&path)?;
        let state = NodeState::load(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        return Ok((file, state));
    }
    let (state, bytes) = NodeState::create();
    file.set_len(node_state::FILE_LENGTH as u64)?;
    write_at(&file, &bytes, 0)?;
    sync(&file, file_sync_level.max(1))?;
    Ok((file, state))
}

impl ConsensusModuleNode {
    /// Connects to the media driver, opens the node's files, allocates the
    /// module's counters and its services' and archive's streams: the module
    /// is then starting, connecting to its archive.
    pub fn launch(ctx: ConsensusContext) -> Result<Self, ConsensusError> {
        let events = Events::default();
        let context = AeronContext::new()?;
        context.set_dir(&ctx.aeron_dir.as_str().into_c_string())?;
        context.set_use_conductor_agent_invoker(true)?;
        let counter_handler = context.set_on_unavailable_counter(Some(events.clone()))?;
        let aeron = Aeron::new(&context)?;
        aeron.start()?;
        let config = ctx.config.clone();
        let cluster_id = config.cluster_id;

        let now_ms = epoch_ns() / 1_000_000;
        std::fs::create_dir_all(&ctx.cluster_dir)?;
        let header = ConsensusModuleHeader {
            start_timestamp_ms: now_ms,
            pid: std::process::id() as i64,
            candidate_term_id: -1,
            archive_stream_id: 10,
            service_stream_id: config.service_stream_id,
            consensus_module_stream_id: config.consensus_module_stream_id,
            ingress_stream_id: config.ingress_stream_id,
            member_id: config.member_id,
            cluster_id,
            error_buffer_length: ctx.error_buffer_length as i32,
            aeron_directory: ctx.aeron_dir.clone(),
            control_channel: config.control_channel.clone(),
            ingress_channel: config.ingress_channel.clone(),
            authenticator: DEFAULT_AUTHENTICATOR.into(),
            services_cluster_dir: ctx.cluster_dir.display().to_string(),
        };
        let bytes = header.encode().map_err(|e| ConsensusError::Config(e.to_string()))?;
        let mark = MarkFile::create_named(
            &ctx.cluster_dir,
            markfile::FILE_NAME,
            markfile::LINK_FILE_NAME,
            &bytes,
            ctx.error_buffer_length,
            ctx.file_page_size,
            now_ms,
        )?;
        let (recording_log, log) = open_recording_log(&ctx.cluster_dir)?;
        let (node_state, state) = open_node_state(&ctx.cluster_dir, config.file_sync_level)?;

        let counter = |type_id: i32, name: &str| -> Result<AeronCounter, ConsensusError> {
            let (key, label) = cluster_counters::cluster_counter(name, cluster_id);
            Ok(aeron.add_counter(type_id, &key, &label, ADD_TIMEOUT)?)
        };
        let (key, label) = cluster_counters::error_counter(cluster_id);
        let errors = aeron.add_counter(cluster_counters::ERROR_COUNT_TYPE_ID, &key, &label, ADD_TIMEOUT)?;
        let mut allocated = HashMap::new();
        for (type_id, name) in cluster_counters::consensus_module_counters(
            ctx.cycle_threshold_ns,
            ctx.total_snapshot_duration_threshold_ns,
        ) {
            allocated.insert(type_id, counter(type_id, &name)?);
        }
        let mut take = |type_id: i32| allocated.remove(&type_id).expect("allocated");
        let counters = Counters {
            errors,
            module_state: take(cluster_counters::MODULE_STATE_TYPE_ID),
            election_state: take(cluster_counters::ELECTION_STATE_TYPE_ID),
            election_count: take(cluster_counters::ELECTION_COUNT_TYPE_ID),
            leadership_term_id: take(cluster_counters::LEADERSHIP_TERM_ID_TYPE_ID),
            node_role: take(cluster_counters::NODE_ROLE_TYPE_ID),
            commit_position: take(cluster_counters::COMMIT_POSITION_TYPE_ID),
            control_toggle: take(cluster_counters::CONTROL_TOGGLE_TYPE_ID),
            node_control_toggle: take(cluster_counters::NODE_CONTROL_TOGGLE_TYPE_ID),
            snapshot_count: take(cluster_counters::SNAPSHOT_COUNTER_TYPE_ID),
            timed_out_clients: take(cluster_counters::CLIENT_TIMEOUT_COUNT_TYPE_ID),
            standby_snapshots: if config.accept_standby_snapshots {
                Some(counter(
                    cluster_counters::STANDBY_SNAPSHOT_COUNTER_TYPE_ID,
                    "Cluster standby snapshots received",
                )?)
            } else {
                None
            },
            _others: allocated.into_values().collect(),
            recovery_state: None,
        };
        store(&counters.leadership_term_id, -1);

        let control = config.control_channel.as_str().into_c_string();
        let service_publication = aeron.add_publication(&control, config.service_stream_id, ADD_TIMEOUT)?;
        let service_subscription = aeron.add_subscription(
            &control,
            config.consensus_module_stream_id,
            Handlers::NONE,
            Handlers::NONE,
            ADD_TIMEOUT,
        )?;
        let archive_publication = aeron.add_exclusive_publication(
            &ctx.archive_control_request_channel.as_str().into_c_string(),
            config.archive_control_request_stream_id,
            ADD_TIMEOUT,
        )?;
        let response_stream_id = cluster_id * 100 + 100 + 20;
        let archive_subscription = aeron.add_subscription(
            &ctx.archive_control_response_channel.as_str().into_c_string(),
            response_stream_id,
            Handlers::NONE,
            Handlers::NONE,
            ADD_TIMEOUT,
        )?;
        let cluster_time = cluster_time(config.time_unit);
        let file_sync_level = config.file_sync_level;
        let core = ConsensusModule::new(config, log, state, cluster_time).map_err(ConsensusError::Config)?;
        let image_handler = SharedHandler::new_shared(events.clone());
        Ok(Self {
            core,
            node: Node {
                counters,
                log_publication: None,
                snapshot_publication: None,
                service_publication,
                archive_publication,
                archive_subscription: Rc::new(archive_subscription),
                archive_response_channel: ctx.archive_control_response_channel.clone(),
                egress: HashMap::new(),
                consensus: HashMap::new(),
                subscriptions: HashMap::new(),
                removed: HashMap::new(),
                node_state,
                file_sync_level,
                aeron,
            },
            mark,
            recording_log,
            service_subscription,
            service_reassembler: Reassembler::default(),
            archive_reassembler: Reassembler::default(),
            by_registration: HashMap::new(),
            destinations: Vec::new(),
            images: HashMap::new(),
            events,
            image_handler,
            _counter_handler: counter_handler,
            last_slow_tick_ms: 0,
            mark_deadline_ms: 0,
            idle_count: 0,
            exit: None,
            ctx,
        })
    }

    pub fn module(&self) -> &ConsensusModule {
        &self.core
    }

    /// Runs duty cycles until the module terminates: whether as expected.
    pub fn run(mut self, running: impl Fn() -> bool) -> bool {
        loop {
            if let Some(expected) = self.do_work() {
                return expected;
            }
            if !running() {
                self.close();
                return true;
            }
        }
    }

    /// One duty cycle, then idling if it found nothing to do: `Some` once
    /// the module terminated, whether as expected.
    pub fn do_work(&mut self) -> Option<bool> {
        let time = cluster_time(self.ctx.config.time_unit);
        self.core.on_tick(time);
        let now_ms = epoch_ns() / 1_000_000;
        let mut work = 0;
        if now_ms != self.last_slow_tick_ms {
            self.last_slow_tick_ms = now_ms;
            work += self.slow_tick(now_ms);
            if let Some(expected) = self.exit {
                let _ = self.mark.signal_terminated();
                return Some(expected);
            }
        }
        work += self.poll_services();
        work += self.poll_archive();
        work += self.core.do_work(&mut self.node);
        work += self.execute_actions();
        work += self.poll_adds();
        work += self.poll_images();
        work += self.execute_actions();
        if let Some(expected) = self.exit {
            let _ = self.mark.signal_terminated();
            return Some(expected);
        }
        self.idle(work);
        None
    }

    /// Stops at the operator's request: the services told, the log's recording stopped.
    pub fn close(&mut self) {
        self.core.close(&mut self.node);
        self.execute_actions();
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

    fn record_error(&mut self, message: &str) {
        self.record(false, message);
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
        add(&self.node.counters.errors, 1);
        let entry = format!("{category} - {message}");
        if let Err(e) = self.mark.record_error(&entry, epoch_ns() / 1_000_000) {
            log::warn!("failed to record an error in the mark file: {e}");
        }
    }

    fn slow_tick(&mut self, now_ms: i64) -> usize {
        let mut work = match self.node.aeron.main_do_work() {
            Ok(n) => n.max(0) as usize,
            Err(e) => {
                self.record_error(&format!("conductor: {e}"));
                0
            }
        };
        if self.node.aeron.is_closed() {
            // The client's conductor gave up (the driver gone, or this process
            // stalled past its service interval): Java's agent terminates too.
            self.record_error("unexpected Aeron close");
            self.exit = Some(false);
            return work;
        }
        if now_ms >= self.mark_deadline_ms {
            self.mark_deadline_ms = now_ms + UPDATE_INTERVAL_MS;
            if let Err(e) = self.mark.update_activity(now_ms) {
                self.record_error(&format!("mark file: {e}"));
            }
        }
        let events = std::mem::take(&mut *self.events.lock());
        work += events.len();
        for event in events {
            match event {
                Event::ImageAvailable {
                    registration_id,
                    session_id,
                    correlation_id,
                    join_position,
                    term_buffer_length,
                    source_identity,
                } => {
                    if let Some(&subscription) = self.by_registration.get(&registration_id) {
                        self.images.insert(
                            (subscription, session_id),
                            IngressImage {
                                correlation_id,
                                session_id,
                                source_identity: source_identity.clone(),
                            },
                        );
                        self.core.on_io(
                            Io::ImageAvailable {
                                subscription,
                                session_id,
                                correlation_id,
                                join_position,
                                term_buffer_length,
                                source_identity,
                            },
                            &mut self.node,
                        );
                    }
                }
                Event::ImageUnavailable {
                    registration_id,
                    session_id,
                    correlation_id,
                    end_of_stream,
                } => {
                    if let Some(&subscription) = self.by_registration.get(&registration_id) {
                        self.images.remove(&(subscription, session_id));
                        self.core.on_io(
                            Io::ImageUnavailable {
                                subscription,
                                session_id,
                                correlation_id,
                                end_of_stream,
                            },
                            &mut self.node,
                        );
                    }
                }
                Event::CounterUnavailable { registration_id } => self
                    .core
                    .on_io(Io::CounterUnavailable { registration_id }, &mut self.node),
            }
        }
        work
    }

    fn poll_services(&mut self) -> usize {
        let core = &mut self.core;
        let node = &mut self.node;
        let reassembler = &mut self.service_reassembler;
        self.service_subscription
            .poll_fn(
                |buffer, header| {
                    reassembler.on_fragment(fragment(buffer, header), |message, _| {
                        core.on_service_message(message, node);
                        PollAction::Continue
                    });
                },
                25,
            )
            .map(|n| n.max(0) as usize)
            .unwrap_or(0)
    }

    fn poll_archive(&mut self) -> usize {
        let core = &mut self.core;
        let node = &mut self.node;
        let reassembler = &mut self.archive_reassembler;
        let subscription = Rc::clone(&node.archive_subscription);
        subscription
            .poll_fn(
                |buffer, header| {
                    reassembler.on_fragment(fragment(buffer, header), |message, _| {
                        core.on_archive_message(message, node);
                        PollAction::Continue
                    });
                },
                10,
            )
            .map(|n| n.max(0) as usize)
            .unwrap_or(0)
    }

    fn poll_images(&mut self) -> usize {
        let mut work = 0;
        for (subscription_id, poll) in self.core.polls(&self.node) {
            let Some(subscription) = self.node.subscriptions.get(&subscription_id).cloned() else {
                continue;
            };
            match poll {
                Poll::Ingress => {
                    let core = &mut self.core;
                    let node = &mut self.node;
                    let images = &self.images;
                    work += subscription
                        .controlled_poll_fn(
                            |buffer, header| {
                                let session_id = header.get_values().map(|v| v.frame().session_id()).unwrap_or(0);
                                let image =
                                    images
                                        .get(&(subscription_id, session_id))
                                        .cloned()
                                        .unwrap_or(IngressImage {
                                            correlation_id: -1,
                                            session_id,
                                            source_identity: String::new(),
                                        });
                                controlled(core.on_ingress_fragment(fragment(buffer, header), &image, node))
                            },
                            INGRESS_FRAGMENT_LIMIT,
                        )
                        .map(|n| n.max(0) as usize)
                        .unwrap_or(0);
                }
                Poll::Consensus => {
                    let core = &mut self.core;
                    let node = &mut self.node;
                    work += subscription
                        .poll_fn(
                            |buffer, header| {
                                let session_id = header.get_values().map(|v| v.frame().session_id()).unwrap_or(0);
                                core.on_consensus_fragment(fragment(buffer, header), session_id, node);
                            },
                            CONSENSUS_FRAGMENT_LIMIT,
                        )
                        .map(|n| n.max(0) as usize)
                        .unwrap_or(0);
                }
                Poll::Image { session_id, limit } => {
                    let Some(image) = subscription.image_by_session_id(session_id) else {
                        continue;
                    };
                    let core = &mut self.core;
                    let node = &mut self.node;
                    work += image
                        .bounded_controlled_poll_fn(
                            |buffer, header| {
                                controlled(core.on_image_fragment(subscription_id, fragment(buffer, header), node))
                            },
                            limit,
                            LOG_FRAGMENT_LIMIT,
                        )
                        .map(|n| n.max(0) as usize)
                        .unwrap_or(0);
                    core.on_image_position(subscription_id, image.position());
                }
            }
        }
        work
    }

    fn execute_actions(&mut self) -> usize {
        let mut work = 0;
        while let Some(action) = self.core.poll_action() {
            work += 1;
            if let Err(e) = self.execute(action) {
                self.record_error(&format!("{e}"));
            }
        }
        work
    }

    fn execute(&mut self, action: Action) -> Result<(), ConsensusError> {
        let aeron = &self.node.aeron;
        match action {
            Action::ConnectEgress {
                egress,
                channel,
                stream_id,
                counter,
            } => {
                let entry = self.node.egress.entry(egress).or_default();
                entry.adding = Some(aeron.async_add_publication(&channel.as_str().into_c_string(), stream_id)?);
                if let Some((key, label, value)) = counter {
                    entry.counter_value = value;
                    entry.adding_counter =
                        Some(aeron.async_add_counter(cluster_counters::SESSION_TYPE_ID, &key, &label)?);
                }
            }
            Action::DisconnectEgress { egress } => {
                if let Some(e) = self.node.egress.remove(&egress) {
                    if let Some(p) = e.publication {
                        let _ = p.close();
                    }
                    if let Some(c) = e.counter {
                        let _ = c.close();
                    }
                }
            }
            Action::AddLogPublication { channel, stream_id } => {
                let publication =
                    aeron.add_exclusive_publication(&channel.as_str().into_c_string(), stream_id, ADD_TIMEOUT)?;
                let session_id = publication.get_constants().map(|c| c.session_id()).unwrap_or(0);
                self.node.log_publication = Some(publication);
                self.core.on_io(Io::LogPublicationAdded { session_id }, &mut self.node);
            }
            Action::AddLogDestination { destination } => {
                if let Some(p) = &self.node.log_publication {
                    let d = p.async_add_destination(aeron, &destination.as_str().into_c_string())?;
                    self.destinations.push(Destination::Publication(d));
                }
            }
            Action::AddConsensusPublication {
                member_id,
                channel,
                stream_id,
            } => {
                let publication =
                    aeron.add_exclusive_publication(&channel.as_str().into_c_string(), stream_id, ADD_TIMEOUT)?;
                self.node.consensus.insert(member_id, publication);
            }
            Action::AddDestination {
                subscription,
                destination,
            } => {
                if let Some(s) = self.node.subscriptions.get(&subscription) {
                    let d = s.async_add_destination(aeron, &destination.as_str().into_c_string())?;
                    self.destinations.push(Destination::Subscription(d));
                }
            }
            Action::RemoveDestination {
                subscription,
                destination,
            } => {
                if let Some(s) = self.node.subscriptions.get(&subscription) {
                    let d = s.async_remove_destination(aeron, &destination.as_str().into_c_string())?;
                    self.destinations.push(Destination::Subscription(d));
                }
            }
            Action::RemoveLogPublication => {
                if let Some(p) = self.node.log_publication.take() {
                    let _ = p.close();
                }
            }
            Action::AddSubscription {
                subscription,
                channel,
                stream_id,
            } => {
                let s = aeron.add_subscription(
                    &channel.as_str().into_c_string(),
                    stream_id,
                    Some(&self.image_handler),
                    Some(&self.image_handler),
                    ADD_TIMEOUT,
                )?;
                self.by_registration.insert(registration_id(&s), subscription);
                self.node.subscriptions.insert(subscription, Rc::new(s));
            }
            Action::RemoveSubscription { subscription } => {
                if let Some(s) = self.node.subscriptions.remove(&subscription) {
                    let id = registration_id(&s);
                    self.by_registration.remove(&id);
                    self.node.removed.insert(subscription, id);
                    self.images.retain(|(id, _), _| *id != subscription);
                    if let Ok(s) = Rc::try_unwrap(s) {
                        let _ = s.close();
                    }
                }
            }
            Action::AddSnapshotPublication { channel, stream_id } => {
                let publication =
                    aeron.add_exclusive_publication(&channel.as_str().into_c_string(), stream_id, ADD_TIMEOUT)?;
                let session_id = publication.get_constants().map(|c| c.session_id()).unwrap_or(0);
                self.node.snapshot_publication = Some(publication);
                self.core
                    .on_io(Io::SnapshotPublicationAdded { session_id }, &mut self.node);
            }
            Action::RemoveSnapshotPublication => {
                if let Some(p) = self.node.snapshot_publication.take() {
                    let _ = p.close();
                }
            }
            Action::AddRecoveryStateCounter { key, label } => {
                let counter = aeron.add_counter(cluster_counters::RECOVERY_STATE_TYPE_ID, &key, &label, ADD_TIMEOUT)?;
                self.node.counters.recovery_state = Some(counter);
            }
            Action::RemoveRecoveryStateCounter => {
                if let Some(c) = self.node.counters.recovery_state.take() {
                    let _ = c.close();
                }
            }
            Action::WriteRecordingLog { writes, sync: synced } => {
                for (offset, bytes) in writes {
                    write_at(&self.recording_log, &bytes, offset)?;
                }
                if synced {
                    sync(&self.recording_log, self.ctx.config.file_sync_level)?;
                }
            }
            Action::Error { warn, message } => self.record(warn, &message),
            Action::Terminate { expected } => self.exit = Some(expected),
        }
        Ok(())
    }

    /// Clients' publications and counters, and destinations, being added.
    fn poll_adds(&mut self) -> usize {
        for d in std::mem::take(&mut self.destinations) {
            let polled = match &d {
                Destination::Publication(a) => a.aeron_exclusive_publication_async_destination_poll(),
                Destination::Subscription(a) => a.aeron_subscription_async_destination_poll(),
            };
            match polled {
                Ok(0) => self.destinations.push(d),
                // The client freed the command as it completed: its handle is let go unclosed.
                Ok(_) => std::mem::forget(d),
                Err(e) => {
                    std::mem::forget(d);
                    self.record_error(&format!("destination: {e}"));
                }
            }
        }
        let mut done = Vec::new();
        for (&egress, e) in self.node.egress.iter_mut() {
            if let Some(adding) = &e.adding_counter {
                match adding.poll() {
                    Ok(Some(counter)) => {
                        store(&counter, e.counter_value);
                        e.counter = Some(counter);
                        e.adding_counter = None;
                    }
                    Ok(None) => {}
                    Err(_) => e.adding_counter = None,
                }
            }
            if let Some(adding) = &e.adding {
                match adding.poll() {
                    Ok(Some(publication)) => {
                        e.publication = Some(publication);
                        e.adding = None;
                        done.push((egress, Ok(())));
                    }
                    Ok(None) => {}
                    Err(error) => {
                        e.adding = None;
                        done.push((egress, Err(error.to_string())));
                    }
                }
            }
        }
        let work = done.len();
        for (egress, result) in done {
            self.core.on_io(Io::EgressAdded { egress, result }, &mut self.node);
        }
        work
    }
}

/// The cluster clock in the cluster's unit: Java's `ClusterClock.time()`.
fn cluster_time(unit: TimeUnit) -> i64 {
    unit.from_nanos(epoch_ns())
}
