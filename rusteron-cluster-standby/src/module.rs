//! The consensus module's part, played for a standby's service: the
//! service container runs the service on the log the standby's backup
//! holds, as a member's runs it on the log its module replays. Not a
//! consensus module: it votes, appends and fires nothing.
//!
//! - It allocates the two counters a container waits for at its start: the
//!   commit position, and a recovery state naming no term (the service
//!   starts empty, at log position 0) or the standby's newest snapshot.
//! - Once the service acks its start, it replays the local log recording
//!   from the service's position, following it as the backup extends it,
//!   and sends the container a `JoinLog` as a standby's. The replay is
//!   bounded by a counter of its own, the recording's position, which waits
//!   through the backup's changes of source instead of ending. It reads the
//!   replay only once the service has acked joining it: on IPC a late
//!   subscriber joins where the slowest reader is, not where it asked.
//! - It reads the replay itself, ahead of the service, keeps the module's
//!   state ([`ModuleView`]), and moves the commit position up to what it
//!   has read: the backup records committed entries only.
//! - At a members' snapshot action it holds the log there until the
//!   service acks there: its state as of the ack is what the members'
//!   modules write there ([`StandbyModule::state_at`]). The service acks
//!   unasked: Aeron's container snapshots at a members' snapshot too, a
//!   standby's included, and acks with its recording, which nothing lists:
//!   the module deletes it.
//! - At a standby snapshot action the service snapshots into the standby's
//!   archive and acks with its recording; the module writes its own state
//!   beside it as a module snapshot, lists the set in its own file, synced,
//!   and offers every member the notice, for 10 s, on the members'
//!   consensus stream: Aeron's `StandbySnapshot` message, which members
//!   started with standby snapshots accepted list in their recording log.
//! - It keeps its newest sets only, [`KEEP_SNAPSHOTS`] by default: an
//!   older set is listed no more, then its recordings are deleted, as are
//!   the members' sets the backup copied before its newest
//!   ([`StandbyModule::purge_later`]). A member restarts from the newest
//!   set it knows of; one down so long that its set is gone counts a warning
//!   and loads its own snapshot instead. What it deletes must be a snapshot
//!   recording, no longer recorded. What a kill or a copy given up leaves,
//!   listed nowhere, the standby finds at its next start
//!   ([`StandbyModule::sweep`]).
//! - A replay that ends anyway, on an error, has the service join again
//!   where the module read if it had joined that replay, else where it
//!   joined last.
//!
//! It speaks the module's internal protocol with the container and writes
//! its snapshot format, Aeron 1.54.2's. It serves one service, id 0, which
//! sends no messages to the cluster, so none is pending: a module snapshot
//! holds a follower's pending service messages, which a standby cannot
//! know. The module fails at once on a second service or on a service
//! message rather than write a snapshot that differs from the members'.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use rusteron_archive::*;
use rusteron_cluster::container::find_counter;
use rusteron_cluster::counters::{compare_and_set, get, load, store};
use rusteron_cluster_core::counters::{
    COMMIT_POSITION_TYPE_ID, RECORDING_POSITION_TYPE_ID, RECOVERY_STATE_TYPE_ID, RecordingPosition, RecoveryState,
    cluster_counter, recovery_state_label,
};
use rusteron_cluster_core::log::{Fragment, PollAction, Reassembler};
use rusteron_cluster_core::recording_log::Entry;
use rusteron_consensus_core::consensus::{self, StandbySnapshotEntry};
use rusteron_consensus_core::service::{self, ServiceMessage};

use crate::snapshots::{Recording, Snapshot, unlisted};
use crate::state::{FLAGS_DEFAULT, FLAGS_STANDBY_SNAPSHOT, ModuleState, ModuleView, Read, SnapshotAction};
use crate::watch::{self, LiveLogWatch, Verdict, clock};

const NULL_VALUE: i64 = -1;
const FRAGMENT_LIMIT: usize = 64;
/// How long a notice is offered to a member that does not take it, one that is down.
const NOTICE_PATIENCE: Duration = Duration::from_secs(10);
const TAIL_FRAGMENT_LIMIT: usize = 256;
/// A frame's flag: the first fragment of a message.
const BEGIN_FRAG_FLAG: u8 = 0x80;
/// How long the module snapshot may take to load, or to be recorded.
const SNAPSHOT_PATIENCE: Duration = Duration::from_secs(10);
/// `AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION`, 0.3.0.
const PROTOCOL_SEMANTIC_VERSION: i32 = 3 << 8;
/// `Cluster.Role.FOLLOWER`.
const FOLLOWER: i32 = 0;
/// `AeronArchive.REPLAY_ALL_AND_FOLLOW`.
const REPLAY_ALL_AND_FOLLOW: i64 = -1;
/// Aeron's counter state for a counter in use.
const RECORD_ALLOCATED: i32 = 1;
const ADD_TIMEOUT: Duration = Duration::from_secs(10);
/// How often the module reads its archive session's answers.
const ARCHIVE_POLL_INTERVAL: Duration = Duration::from_millis(10);
/// How long after a failed connection to the archive the module tries again.
const ARCHIVE_RECONNECT_INTERVAL: Duration = Duration::from_secs(1);
/// How many of its newest snapshots a standby keeps by default: a member restarts from the newest it knows.
pub const KEEP_SNAPSHOTS: usize = 3;
/// How soon a recording to delete is looked at again while it is still recorded.
const PURGE_RECORDING: Duration = Duration::from_millis(10);
/// How soon, and how many times, a deletion its archive refused is asked again: a member may be copying it.
const PURGE_REFUSED: Duration = Duration::from_secs(1);
const PURGE_REFUSALS: u32 = 5;
/// `AeronArchive.NULL_POSITION`: a recording's stop position while it is recorded.
const NULL_POSITION: i64 = -1;

/// Connects a client of the local archive on the module's Aeron client.
pub type ArchiveConnect = Box<dyn Fn(&Aeron) -> Result<AeronArchive, String>>;

/// Where the module reaches its service, its archive and the members:
/// Aeron's container and module defaults, and the standby's own streams.
#[derive(Clone, Debug)]
pub struct ModuleConfig {
    /// The standby's slot: the member id it gives the container.
    pub member_id: i32,
    pub cluster_id: i32,
    /// The host members answer the notices to.
    pub host: String,
    /// Where the members' archives copy the standby's snapshots from: its archive's control endpoint.
    pub archive_endpoint: String,
    /// Every member's consensus endpoint, where the notices go.
    pub member_consensus_endpoints: Vec<String>,
    /// The standby's own list of its snapshots.
    pub snapshots_file: PathBuf,
    /// How many of its newest snapshots the standby keeps listed, and their recordings; 0: all.
    pub keep_snapshots: usize,
    /// A test's aid: the module's state at each members' snapshot recorded in the local archive as a module
    /// snapshot, and listed in this file (`logPosition recordingId` a line), for a check against the members'.
    pub member_states_file: Option<PathBuf>,
    pub control_channel: String,
    pub service_stream_id: i32,
    pub consensus_module_stream_id: i32,
    pub replay_channel: String,
    pub replay_stream_id: i32,
    pub snapshot_channel: String,
    pub snapshot_stream_id: i32,
    /// The service's snapshots' stream: what the module deletes must be a snapshot, the service's or a module's.
    pub service_snapshot_stream_id: i32,
    pub consensus_stream_id: i32,
    /// The standby's own streams, on its driver, apart from its service's: where members reach it back when
    /// they take its notice (they write nothing to it), where its own module snapshot is replayed from at its
    /// start, and where it reads the log's tail ahead of the service.
    pub response_stream_id: i32,
    pub load_stream_id: i32,
    pub tail_channel: String,
    pub tail_stream_id: i32,
    /// The type of the counter that bounds the service's replay: past Aeron's own.
    pub replay_limit_type_id: i32,
    /// The watch on the backup's live log ([`LiveLogWatch::new`]).
    pub live_log_timeout: Duration,
    pub snapshot_hold: Duration,
}

impl ModuleConfig {
    /// Standby slot `member_id`'s, Aeron's defaults for the channels and streams.
    pub fn new(member_id: i32, host: impl Into<String>, archive_endpoint: impl Into<String>) -> Self {
        Self {
            member_id,
            cluster_id: 0,
            host: host.into(),
            archive_endpoint: archive_endpoint.into(),
            member_consensus_endpoints: Vec::new(),
            snapshots_file: PathBuf::from("standby-snapshots"),
            keep_snapshots: KEEP_SNAPSHOTS,
            member_states_file: None,
            control_channel: "aeron:ipc?term-length=128k".into(),
            service_stream_id: 104,
            consensus_module_stream_id: 105,
            replay_channel: "aeron:ipc".into(),
            replay_stream_id: 103,
            snapshot_channel: "aeron:ipc?alias=snapshot".into(),
            snapshot_stream_id: 107,
            service_snapshot_stream_id: 106,
            consensus_stream_id: 108,
            response_stream_id: 111,
            load_stream_id: 112,
            tail_channel: "aeron:ipc".into(),
            tail_stream_id: 113,
            replay_limit_type_id: 1_100,
            live_log_timeout: watch::TIMEOUT,
            snapshot_hold: watch::SNAPSHOT_HOLD,
        }
    }
}

/// A notice offered to the members until each takes it, or its time is up.
struct Notice {
    message: Vec<u8>,
    taken: Vec<bool>,
    deadline: Instant,
}

/// The replay the service reads, and its image once it has one.
struct Replay {
    session_id: i64,
    subscription: AeronSubscription,
    image: Option<AeronImage>,
}

/// The local log recording's position counter: the recording, the counter,
/// and its registration, which names one allocation of it, as the recording
/// id in its key does for Java's `RecordingPos.isActive` (rusteron's
/// `get_counter_key` cuts a binary key short).
#[derive(Clone, Copy)]
struct RecordingCounter {
    recording_id: i64,
    counter_id: i32,
    registration_id: i64,
}

/// The module: run [`Self::do_work`] on one thread, with the local log
/// recording's id as the backup reports it.
pub struct StandbyModule {
    aeron: Aeron,
    archive: AeronArchive,
    connect: ArchiveConnect,
    next_archive_poll: Instant,
    config: ModuleConfig,
    archive_id: i64,
    commit_position: AeronCounter,
    _recovery_state: AeronCounter,
    replay_limit: AeronCounter,
    replay_limit_id: i32,
    to_service: AeronExclusivePublication,
    from_service: AeronSubscription,
    from_service_reassembler: Reassembler,
    to_members: Vec<AeronExclusivePublication>,
    responses: AeronSubscription,
    view: ModuleView,
    loaded: Option<Snapshot>,
    snapshots: Vec<Snapshot>,
    states: BTreeMap<i64, ModuleState>,
    service_started: bool,
    last_ack_position: i64,
    replay: Option<Replay>,
    log_reassembler: Reassembler,
    /// Where the service's log ends so far: where the next replay begins.
    service_position: i64,
    /// Whether the service acked joining the replay: it then reads the replay to its end.
    service_joined: bool,
    recording_counter: Option<RecordingCounter>,
    /// The watch on the backup's live log recording, the replay of it it reads the log's tail on, and the
    /// recording counter (its registration) that replay reads, and the clock it is watched on.
    watch: LiveLogWatch,
    tail: Option<Replay>,
    tail_recording: Option<i64>,
    epoch: Instant,
    /// A snapshot action the service is held at until it acks there.
    snapshot_at: Option<SnapshotAction>,
    notice: Option<Notice>,
    next_correlation_id: i64,
    joins: u32,
    /// Snapshot recordings nothing lists, to delete, and how many times their archive refused.
    purges: VecDeque<(i64, u32)>,
    next_purge: Instant,
    purged: usize,
}

fn aeron_error<E: std::fmt::Display>(what: &'static str) -> impl Fn(E) -> String {
    move |e| format!("{what}: {e}")
}

fn c_string(s: &str) -> std::ffi::CString {
    s.into_c_string()
}

/// `ChannelUri.addSessionId`.
fn with_session_id(channel: &str, session_id: i32) -> String {
    format!(
        "{channel}{}session-id={session_id}",
        if channel.contains('?') { "|" } else { "?" }
    )
}

impl StandbyModule {
    /// The module on `aeron` and its local archive, which `connect` reaches: its counters, its
    /// streams to the service and the members, and the standby's newest
    /// snapshot taken up, if it can be read back.
    pub fn new(aeron: Aeron, connect: ArchiveConnect, config: ModuleConfig) -> Result<Self, String> {
        let archive = connect(&aeron)?;
        let archive_id = archive.archive_id();
        let (key, label) = cluster_counter("Cluster commit-pos:", config.cluster_id);
        let commit_position = aeron
            .add_counter(COMMIT_POSITION_TYPE_ID, &key, &label, ADD_TIMEOUT)
            .map_err(aeron_error("commit position"))?;
        let replay_limit = aeron
            .add_counter(
                config.replay_limit_type_id,
                &[],
                "standby log replay limit",
                ADD_TIMEOUT,
            )
            .map_err(aeron_error("replay limit"))?;
        let replay_limit_id = replay_limit
            .get_constants()
            .map_err(aeron_error("replay limit"))?
            .counter_id();

        let mut view = ModuleView::default();
        let mut service_position = 0;
        let latest = Snapshot::latest(&config.snapshots_file).map_err(|e| e.to_string())?;
        let loaded = latest.and_then(|snapshot| match load_state(&aeron, &archive, &config, &snapshot) {
            Ok(state) => {
                view = ModuleView::loaded(&state, snapshot.term_base_log_position);
                service_position = snapshot.log_position;
                store(&commit_position, snapshot.log_position);
                Some(snapshot)
            }
            Err(e) => {
                println!(
                    "standby: its snapshot at {} cannot be read back ({e}): it applies the log from its start",
                    snapshot.log_position
                );
                None
            }
        });
        let recovery = match &loaded {
            Some(s) => RecoveryState {
                leadership_term_id: s.leadership_term_id,
                log_position: s.log_position,
                timestamp: s.timestamp,
                cluster_id: config.cluster_id,
                snapshot_recording_ids: vec![s.service_recording_id],
            },
            None => RecoveryState {
                leadership_term_id: NULL_VALUE,
                log_position: 0,
                timestamp: 0,
                cluster_id: config.cluster_id,
                snapshot_recording_ids: Vec::new(),
            },
        };
        let label = recovery_state_label(recovery.leadership_term_id, recovery.log_position, config.cluster_id);
        let recovery_state = aeron
            .add_counter(RECOVERY_STATE_TYPE_ID, &recovery.to_key(), &label, ADD_TIMEOUT)
            .map_err(aeron_error("recovery state"))?;
        let control = c_string(&config.control_channel);
        let to_service = aeron
            .add_exclusive_publication(&control, config.service_stream_id, ADD_TIMEOUT)
            .map_err(aeron_error("to the service"))?;
        let from_service = aeron
            .add_subscription(
                &control,
                config.consensus_module_stream_id,
                Handlers::NONE,
                Handlers::NONE,
                ADD_TIMEOUT,
            )
            .map_err(aeron_error("from the service"))?;
        let mut to_members = Vec::new();
        for endpoint in &config.member_consensus_endpoints {
            let channel = c_string(&format!("aeron:udp?term-length=64k|endpoint={endpoint}"));
            to_members.push(
                aeron
                    .add_exclusive_publication(&channel, config.consensus_stream_id, ADD_TIMEOUT)
                    .map_err(aeron_error("to the members"))?,
            );
        }
        let responses = aeron
            .add_subscription(
                &c_string(&format!("aeron:udp?endpoint={}:0", config.host)),
                config.response_stream_id,
                Handlers::NONE,
                Handlers::NONE,
                ADD_TIMEOUT,
            )
            .map_err(aeron_error("responses"))?;
        let watch = LiveLogWatch::new(config.live_log_timeout, config.snapshot_hold);
        Ok(Self {
            aeron,
            archive,
            connect,
            next_archive_poll: Instant::now(),
            config,
            archive_id,
            commit_position,
            _recovery_state: recovery_state,
            replay_limit,
            replay_limit_id,
            to_service,
            from_service,
            from_service_reassembler: Reassembler::default(),
            to_members,
            responses,
            view,
            loaded,
            snapshots: Vec::new(),
            states: BTreeMap::new(),
            service_started: false,
            last_ack_position: NULL_VALUE,
            replay: None,
            log_reassembler: Reassembler::default(),
            service_position,
            service_joined: false,
            recording_counter: None,
            watch,
            tail: None,
            tail_recording: None,
            epoch: Instant::now(),
            snapshot_at: None,
            notice: None,
            next_correlation_id: 1,
            joins: 0,
            purges: VecDeque::new(),
            next_purge: Instant::now(),
            purged: 0,
        })
    }

    /// One duty cycle, the local log recording being `recording_id` (-1
    /// before the backup has one): the work done.
    pub fn do_work(&mut self, recording_id: i64) -> Result<usize, String> {
        let mut work = self.poll_service()?;
        work += self.send_notice();
        work += self.keep_archive();
        work += self.purge();
        if !self.service_started || recording_id == NULL_VALUE {
            return Ok(work);
        }
        work += self.follow_recording(recording_id);
        if self.replay.is_none() {
            // Join once the recording runs past where the service's log ends.
            if get(&self.replay_limit) > self.service_position {
                self.join(recording_id)?;
                work += 1;
            }
            return Ok(work);
        }
        if self.snapshot_at.is_some() {
            // Held at a snapshot until the service acks there.
            return Ok(work);
        }
        Ok(work + self.read_log()?)
    }

    /// Moves the replay limit up to the recording's position, while the
    /// recording is active. By the archive too: another archive on the
    /// standby's driver numbers its recordings from 0 as well.
    fn follow_recording(&mut self, recording_id: i64) -> usize {
        let counters = self.aeron.counters_reader();
        let active = |c: &RecordingCounter| {
            c.recording_id == recording_id
                && counters.counter_state(c.counter_id).ok() == Some(RECORD_ALLOCATED)
                && counters.counter_registration_id(c.counter_id).ok() == Some(c.registration_id)
        };
        if !self.recording_counter.as_ref().is_some_and(active) {
            let archive_id = self.archive_id;
            self.recording_counter = find_counter(&counters, |type_id, key| {
                type_id == RECORDING_POSITION_TYPE_ID
                    && RecordingPosition::parse(key)
                        .is_ok_and(|r| r.recording_id == recording_id && r.archive_id == archive_id)
            })
            .and_then(|(counter_id, _)| {
                // Not yet registered: a driver marks a counter allocated before it sets its registration id.
                let registration_id = counters
                    .counter_registration_id(counter_id)
                    .ok()
                    .filter(|&id| id != 0)?;
                Some(RecordingCounter {
                    recording_id,
                    counter_id,
                    registration_id,
                })
            });
        }
        let Some(counter) = self.recording_counter else {
            return 0;
        };
        let recorded = load(&counters, counter.counter_id);
        if !active(&counter) {
            return 0;
        }
        let mut work = 0;
        // Only ever up, as a member's module moves its own commit position.
        loop {
            let limit = get(&self.replay_limit);
            if recorded <= limit {
                break;
            }
            if compare_and_set(&self.replay_limit, limit, recorded) {
                work += 1;
                break;
            }
        }
        work += self.read_tail(recording_id, counter.registration_id, recorded);
        work + self.watch_live_log(recording_id, counter.registration_id, recorded)
    }

    /// Stops the backup's live log recording once it has stood still for
    /// the watch's timeout, unless the log ends with the members' snapshot
    /// action: the backup, its recording's counter gone, resets and asks a
    /// member again ([`LiveLogWatch`]).
    fn watch_live_log(&mut self, recording_id: i64, registration_id: i64, recorded: i64) -> usize {
        let now = self.epoch.elapsed().as_nanos() as i64;
        match self.watch.check(registration_id, recorded, now) {
            Verdict::HoldBegins => {
                println!(
                    "standby: its live log stood at {recorded} for {} s, ending with the members' snapshot action: \
                     held, as they take their snapshot, {} s at most, at {} UTC",
                    self.config.live_log_timeout.as_secs(),
                    self.config.snapshot_hold.as_secs(),
                    clock()
                );
                0
            }
            Verdict::Stop => {
                if self
                    .archive
                    .try_stop_recording_by_identity(recording_id)
                    .unwrap_or(false)
                {
                    println!(
                        "standby: its live log stood at {recorded} for {} s, its recording stopped for the backup to \
                         start again, at {} UTC",
                        self.config.live_log_timeout.as_secs(),
                        clock()
                    );
                }
                1
            }
            Verdict::Wait | Verdict::Hold => 0,
        }
    }

    /// Reads the backup's recording on a replay of its own, from where the
    /// service's log ends and ahead of the service, for the watch to know
    /// what the log ends with. Begun again on each new recording counter; a
    /// replay the archive refuses leaves the watch unable to hold off at
    /// snapshots, as it then knows nothing of what the log ends with.
    fn read_tail(&mut self, recording_id: i64, registration_id: i64, recorded: i64) -> usize {
        if self.tail_recording != Some(registration_id) {
            self.end_tail();
            self.tail_recording = Some(registration_id);
            self.watch.tail_restarted();
            match self.start_tail(recording_id, self.service_position.min(recorded)) {
                Ok(tail) => self.tail = Some(tail),
                Err(e) => println!(
                    "standby: its log's tail is not read, so its watch does not hold off at the members' snapshots: {e}"
                ),
            }
            return 1;
        }
        let Some(tail) = self.tail.as_mut() else {
            return 0;
        };
        let watch = &mut self.watch;
        tail.subscription
            .poll_fn(
                |buffer: &[u8], header: &AeronHeader| {
                    let flags = header.get_values().map_or(0, |v| v.frame().flags());
                    watch.on_tail(buffer, flags & BEGIN_FRAG_FLAG != 0, header.position());
                },
                TAIL_FRAGMENT_LIMIT,
            )
            .map_or(0, |n| n.max(0) as usize)
    }

    fn start_tail(&self, recording_id: i64, position: i64) -> Result<Replay, String> {
        let params = AeronArchiveReplayParams::new(self.replay_limit_id, -1, position, REPLAY_ALL_AND_FOLLOW, -1, -1)
            .map_err(aeron_error("replay params"))?;
        let session_id = self
            .archive
            .start_replay(
                recording_id,
                &c_string(&self.config.tail_channel),
                self.config.tail_stream_id,
                &params,
            )
            .map_err(aeron_error("the tail's replay"))?;
        let channel = with_session_id(&self.config.tail_channel, session_id as i32);
        let subscription = self
            .aeron
            .add_subscription(
                &c_string(&channel),
                self.config.tail_stream_id,
                Handlers::NONE,
                Handlers::NONE,
                ADD_TIMEOUT,
            )
            .map_err(aeron_error("the tail's subscription"))?;
        Ok(Replay {
            session_id,
            subscription,
            image: None,
        })
    }

    fn end_tail(&mut self) {
        if let Some(tail) = self.tail.take() {
            // It may have ended with its recording.
            let _ = self.archive.stop_replay(tail.session_id);
        }
    }

    /// Reads the replay ahead of the service: the module's state, the
    /// commit position, held at snapshot actions.
    fn read_log(&mut self) -> Result<usize, String> {
        let Some(replay) = self.replay.as_mut() else {
            return Ok(0);
        };
        if replay.image.is_none() {
            replay.image = replay.subscription.image_by_session_id(replay.session_id as i32);
        }
        let Some(image) = replay.image.clone() else {
            return Ok(0);
        };
        if !self.service_joined {
            // Not a fragment before the service has joined: the replay is IPC, where a subscription joins at the
            // slowest reader's position, so the module reading ahead would have the service join past the
            // position it asked for, and refuse the log ("Cluster log must be contiguous") each time it tried.
            if image.is_end_of_stream() || image.is_closed() {
                self.end_replay();
                return Ok(1);
            }
            return Ok(0);
        }
        let view = &mut self.view;
        let reassembler = &mut self.log_reassembler;
        let snapshot_at = &mut self.snapshot_at;
        let polled = image
            .controlled_poll_fn(
                |buffer, header| {
                    controlled(reassembler.on_fragment(fragment(buffer, header), |message, position| {
                        match view.on_log(message, position) {
                            Read::Continue => PollAction::Continue,
                            Read::Break(held) => {
                                *snapshot_at = held;
                                PollAction::Break
                            }
                        }
                    }))
                },
                FRAGMENT_LIMIT,
            )
            .map_err(aeron_error("the log replay"))?;
        let mut work = polled.max(0) as usize;
        let read = image.position();
        if read > get(&self.commit_position) {
            store(&self.commit_position, read);
        }
        // Held at a snapshot, the service acks there unasked: Aeron's container snapshots at a members'
        // snapshot too, a standby's included. It is not asked as well: a container sometimes read the request
        // only once past the snapshot, warned "invalid ack request" and acked again where it was.
        if self.snapshot_at.is_none() && (image.is_end_of_stream() || image.is_closed()) {
            // The replay failed. The service reads it to the end if it joined it; if not, its log ends where
            // it was.
            if self.service_joined {
                self.service_position = read;
            }
            self.end_replay();
            work += 1;
        }
        Ok(work)
    }

    /// Reads the archive's answers, pings and recording signals, every 10 ms: the archive pings each session
    /// every 100 ms and drops one that has not read its answers for 5 s, which an idle module would not
    /// between snapshots. A session dropped all the same is connected again, a second apart.
    fn keep_archive(&mut self) -> usize {
        let now = Instant::now();
        if now < self.next_archive_poll {
            return 0;
        }
        self.next_archive_poll = now + ARCHIVE_POLL_INTERVAL;
        match self.archive.poll_for_recording_signals() {
            Ok(n) => n.max(0) as usize,
            Err(e) => {
                println!("standby: its archive session ended ({e}): connecting again");
                self.next_archive_poll = now + ARCHIVE_RECONNECT_INTERVAL;
                match (self.connect)(&self.aeron) {
                    Ok(archive) => {
                        self.archive = archive;
                        1
                    }
                    Err(e) => {
                        println!("standby: its archive did not answer: {e}");
                        0
                    }
                }
            }
        }
    }

    fn end_replay(&mut self) {
        if let Some(replay) = self.replay.take() {
            // It may have ended with its recording.
            let _ = self.archive.stop_replay(replay.session_id);
        }
        self.log_reassembler.reset();
    }

    /// Replays the recording from where the service's log ends, and has the
    /// container join it as a standby's.
    fn join(&mut self, recording_id: i64) -> Result<(), String> {
        let params = AeronArchiveReplayParams::new(
            self.replay_limit_id,
            -1,
            self.service_position,
            REPLAY_ALL_AND_FOLLOW,
            -1,
            -1,
        )
        .map_err(aeron_error("replay params"))?;
        let session_id = self
            .archive
            .start_replay(
                recording_id,
                &c_string(&self.config.replay_channel),
                self.config.replay_stream_id,
                &params,
            )
            .map_err(aeron_error("the log replay"))?;
        let channel = with_session_id(&self.config.replay_channel, session_id as i32);
        let subscription = self
            .aeron
            .add_subscription(
                &c_string(&channel),
                self.config.replay_stream_id,
                Handlers::NONE,
                Handlers::NONE,
                ADD_TIMEOUT,
            )
            .map_err(aeron_error("the log replay's subscription"))?;
        self.replay = Some(Replay {
            session_id,
            subscription,
            image: None,
        });
        self.service_joined = false;
        self.send(&service::join_log_as(
            self.service_position,
            i64::MAX,
            self.config.member_id,
            session_id as i32,
            self.config.replay_stream_id,
            true,
            FOLLOWER,
            true,
            &channel,
        ))?;
        self.joins += 1;
        Ok(())
    }

    /// A message to the service, offered until it goes: the control stream is IPC.
    fn send(&self, message: &[u8]) -> Result<(), String> {
        loop {
            match self.to_service.offer(message) {
                Ok(_) => return Ok(()),
                Err(AeronOfferError::NotConnected | AeronOfferError::BackPressured | AeronOfferError::AdminAction) => {
                    std::hint::spin_loop()
                }
                Err(e) => return Err(format!("the service's control channel cannot take a message: {e:?}")),
            }
        }
    }

    /// The service's messages: its acks, and its timer requests the module's state keeps. A second service's
    /// ack, or a message to the cluster, fails the module: its state would no longer be a follower's.
    fn poll_service(&mut self) -> Result<usize, String> {
        let mut messages = Vec::new();
        let reassembler = &mut self.from_service_reassembler;
        let polled = self
            .from_service
            .poll_fn(
                |buffer, header| {
                    reassembler.on_fragment(fragment(buffer, header), |message, _| {
                        messages.push(message.to_vec());
                        PollAction::Continue
                    });
                },
                FRAGMENT_LIMIT,
            )
            .map_err(aeron_error("from the service"))?;
        for message in messages {
            match service::decode(&message) {
                Ok(ServiceMessage::Ack { service_id, .. }) if service_id != 0 => {
                    return Err(format!(
                        "service {service_id} acked: a standby runs one service, id 0, and its module snapshot \
                         would hold no other's"
                    ));
                }
                Ok(ServiceMessage::Message { cluster_session_id, .. }) => {
                    return Err(format!(
                        "its service sent a message to the cluster (session {cluster_session_id}): a standby \
                         supports a service that sends none, as its module snapshot cannot hold a follower's \
                         pending service messages"
                    ));
                }
                Ok(ServiceMessage::Ack {
                    log_position,
                    ack_id,
                    relevant_id,
                    ..
                }) => self.on_ack(log_position, ack_id, relevant_id)?,
                Ok(other) => self.view.on_service(&other),
                Err(e) => println!("standby: an unreadable message from the service: {e}"),
            }
        }
        Ok(polled.max(0) as usize)
    }

    fn on_ack(&mut self, log_position: i64, ack_id: i64, relevant_id: i64) -> Result<(), String> {
        if ack_id == 0 {
            if relevant_id == NULL_VALUE {
                println!("standby: the service failed to start");
            }
            self.service_started = true;
        }
        self.last_ack_position = log_position;
        if ack_id != 0 && relevant_id == NULL_VALUE && self.replay.is_some() && log_position == self.service_position {
            // The service acks each join, at the position it joined.
            self.service_joined = true;
        }
        if let Some(action) = self.snapshot_at
            && log_position == action.log_position
        {
            // The service has read the log to the snapshot, and asked for every timer up to it.
            let state = self.view.state(action.log_position);
            self.states.insert(action.log_position, state.clone());
            // A snapshot that fails is said and passed over: the service goes on, and the next snapshot is
            // another chance.
            if action.flags == FLAGS_DEFAULT
                && self.config.member_states_file.is_some()
                && let Err(e) = self.record_member_state(&state)
            {
                println!(
                    "standby: its module state at {} was not recorded: {e}",
                    action.log_position
                );
            }
            if action.flags == FLAGS_STANDBY_SNAPSHOT {
                if relevant_id == NULL_VALUE {
                    println!("standby: the service's snapshot at {} failed", action.log_position);
                } else if let Err(e) = self.take_snapshot(&state, relevant_id, action.timestamp) {
                    println!("standby: its snapshot at {} failed: {e}", action.log_position);
                }
            } else if relevant_id != NULL_VALUE {
                // The service's own snapshot at a members' snapshot: nothing lists it, nor loads it.
                self.purge_later([relevant_id]);
            }
            self.snapshot_at = None;
        }
        Ok(())
    }

    /// Records the module's state at a members' snapshot, and lists it in the member states file.
    fn record_member_state(&mut self, state: &ModuleState) -> Result<(), String> {
        let Some(file) = self.config.member_states_file.clone() else {
            return Ok(());
        };
        let recording_id = self.record_state(state)?;
        let line = format!("{} {recording_id}\n", state.log_position);
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file)
            .and_then(|mut f| std::io::Write::write_all(&mut f, line.as_bytes()))
            .map_err(|e| format!("{}: {e}", file.display()))
    }

    /// Writes the module's state as a module snapshot into the standby's
    /// archive, beside the service's, lists the set, and makes the notice.
    fn take_snapshot(&mut self, state: &ModuleState, service_recording_id: i64, timestamp: i64) -> Result<(), String> {
        let module_recording_id = self.record_state(state)?;
        let snapshot = Snapshot {
            log_position: state.log_position,
            leadership_term_id: state.leadership_term_id,
            term_base_log_position: self.view.term_base_log_position(),
            timestamp,
            service_recording_id,
            module_recording_id,
        };
        // Listed before the members hear of it: a standby restarting starts from it, not before it.
        snapshot
            .append(&self.config.snapshots_file)
            .map_err(|e| e.to_string())?;
        self.snapshots.push(snapshot);
        println!(
            "standby: snapshot at {} of term {}, service recording {service_recording_id}, module recording \
             {module_recording_id}",
            snapshot.log_position, snapshot.leadership_term_id
        );
        // The older sets listed no more, then deleted: a member restarts from the newest set it knows of, and
        // one whose set is gone, down that long, loads its own snapshot instead and replays more log.
        match Snapshot::keep_newest(&self.config.snapshots_file, self.config.keep_snapshots) {
            Ok(dropped) => {
                for d in dropped {
                    println!("standby: its snapshot at {} is listed no more", d.log_position);
                    self.purge_later([d.service_recording_id, d.module_recording_id]);
                }
            }
            Err(e) => println!("standby: its older snapshots stay listed: {e}"),
        }

        // The service's snapshot first, the module's last, as a member's recording log lists a set.
        let entry = |recording_id, service_id| StandbySnapshotEntry {
            recording_id,
            leadership_term_id: snapshot.leadership_term_id,
            term_base_log_position: snapshot.term_base_log_position,
            log_position: snapshot.log_position,
            timestamp,
            service_id,
            archive_endpoint: self.config.archive_endpoint.clone(),
        };
        let entries = [entry(service_recording_id, 0), entry(module_recording_id, -1)];
        let response_channel = self.response_channel()?;
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id += 1;
        self.notice = Some(Notice {
            message: consensus::standby_snapshot(
                correlation_id,
                PROTOCOL_SEMANTIC_VERSION,
                self.config.response_stream_id,
                &entries,
                &response_channel,
            ),
            taken: vec![false; self.to_members.len()],
            deadline: Instant::now() + NOTICE_PATIENCE,
        });
        Ok(())
    }

    /// Records `state` as a module snapshot in the local archive: its recording.
    fn record_state(&self, state: &ModuleState) -> Result<i64, String> {
        let publication = self
            .aeron
            .add_exclusive_publication(
                &c_string(&self.config.snapshot_channel),
                self.config.snapshot_stream_id,
                ADD_TIMEOUT,
            )
            .map_err(aeron_error("the module snapshot's publication"))?;
        let session_id = publication
            .get_constants()
            .map_err(aeron_error("the module snapshot"))?
            .session_id();
        self.archive
            .start_recording(
                &c_string(&with_session_id(&self.config.snapshot_channel, session_id)),
                self.config.snapshot_stream_id,
                SOURCE_LOCATION_LOCAL,
                true,
            )
            .map_err(aeron_error("the module snapshot's recording"))?;
        let counters = self.aeron.counters_reader();
        let archive_id = self.archive_id;
        let deadline = Instant::now() + SNAPSHOT_PATIENCE;
        let (counter_id, key, registration_id) = loop {
            if let Some((counter_id, key)) = find_counter(&counters, |type_id, key| {
                type_id == RECORDING_POSITION_TYPE_ID
                    && RecordingPosition::parse(key)
                        .is_ok_and(|r| r.session_id == session_id && r.archive_id == archive_id)
            }) && let Some(registration_id) = counters.counter_registration_id(counter_id).ok().filter(|&id| id != 0)
            {
                break (counter_id, key, registration_id);
            }
            if Instant::now() > deadline {
                return Err("the module snapshot's recording did not start".into());
            }
            std::thread::yield_now();
        };
        let recording_id = RecordingPosition::parse(&key)
            .map_err(|e| format!("{e:?}"))?
            .recording_id;
        for message in state.messages() {
            loop {
                match publication.offer(&message) {
                    Ok(_) => break,
                    Err(
                        AeronOfferError::NotConnected | AeronOfferError::BackPressured | AeronOfferError::AdminAction,
                    ) if Instant::now() < deadline => std::thread::yield_now(),
                    Err(e) => return Err(format!("the module snapshot: {e:?}")),
                }
            }
        }
        let end = publication.position();
        while load(&counters, counter_id) < end {
            let active = counters.counter_state(counter_id).ok() == Some(RECORD_ALLOCATED)
                && counters.counter_registration_id(counter_id).ok() == Some(registration_id);
            if !active || Instant::now() > deadline {
                return Err(format!("the module snapshot's recording stopped: {recording_id}"));
            }
            std::thread::yield_now();
        }
        Ok(recording_id)
    }

    /// Deletes these snapshot recordings, which nothing lists, once they are no longer recorded.
    pub fn purge_later(&mut self, recording_ids: impl IntoIterator<Item = i64>) {
        self.purges.extend(recording_ids.into_iter().map(|id| (id, 0)));
    }

    /// Finds the snapshot recordings in the standby's archive that nothing
    /// lists ([`unlisted`]), its backup's recording log `backup_log`, and
    /// deletes them as the rest, one a duty cycle. Called at the standby's
    /// start, before its backup runs, when nothing records a snapshot that
    /// will be listed. The recordings, and the archive's newest.
    pub fn sweep(&mut self, backup_log: &[Entry]) -> Result<(Vec<i64>, i64), String> {
        let mut recordings = Vec::new();
        let mut count = 0;
        self.archive
            .list_recordings_fn(&mut count, 0, i32::MAX, |d| {
                recordings.push(Recording {
                    recording_id: d.recording_id(),
                    stream_id: d.stream_id(),
                    stop_position: d.stop_position(),
                })
            })
            .map_err(|e| format!("its archive did not list its recordings: {e}"))?;
        let sets = Snapshot::all(&self.config.snapshots_file).map_err(|e| e.to_string())?;
        // The module states a test records are listed in a file of their own, a line `logPosition recording` each.
        let kept: Vec<i64> = match &self.config.member_states_file {
            Some(file) => match std::fs::read_to_string(file) {
                Ok(text) => text
                    .lines()
                    .filter_map(|l| l.split_whitespace().nth(1)?.parse().ok())
                    .collect(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(e) => return Err(format!("{}: {e}", file.display())),
            },
            None => Vec::new(),
        };
        let streams = [self.config.service_snapshot_stream_id, self.config.snapshot_stream_id];
        let ids = unlisted(&recordings, &sets, backup_log, &kept, streams);
        self.purge_later(ids.iter().copied());
        Ok((ids, recordings.iter().map(|r| r.recording_id).max().unwrap_or(-1)))
    }

    /// Deletes the next snapshot recording nothing lists, one a duty cycle: once it is no longer recorded, if its
    /// archive still describes it as a snapshot. A deletion refused is asked again a second on, a few times.
    fn purge(&mut self) -> usize {
        let Some(&(recording_id, refusals)) = self.purges.front() else {
            return 0;
        };
        let now = Instant::now();
        if now < self.next_purge {
            return 0;
        }
        let mut described = None;
        let mut count = 0;
        if self
            .archive
            .list_recording_fn(&mut count, recording_id, |d| {
                described = Some((d.stream_id(), d.stop_position()))
            })
            .is_err()
        {
            // The archive's session may be down; keep_archive connects again.
            self.next_purge = now + PURGE_REFUSED;
            return 0;
        }
        self.purges.pop_front();
        match described {
            // Gone already.
            None => {}
            Some((stream_id, _))
                if stream_id != self.config.service_snapshot_stream_id
                    && stream_id != self.config.snapshot_stream_id =>
            {
                println!("standby: recording {recording_id} is no snapshot (stream {stream_id}), kept");
            }
            Some((_, NULL_POSITION)) => {
                // Still recorded: the service acks its snapshot as the recording ends.
                self.purges.push_back((recording_id, refusals));
                self.next_purge = now + PURGE_RECORDING;
                return 0;
            }
            Some(_) => match self.archive.purge_recording(recording_id) {
                Ok(_) => self.purged += 1,
                Err(_) if refusals + 1 < PURGE_REFUSALS => {
                    self.purges.push_back((recording_id, refusals + 1));
                    self.next_purge = now + PURGE_REFUSED;
                }
                Err(e) => println!("standby: recording {recording_id} was not deleted: {e}"),
            },
        }
        1
    }

    /// How many snapshot recordings the module deleted since it started.
    pub fn purged(&self) -> usize {
        self.purged
    }

    /// The notices' response channel, its port resolved.
    fn response_channel(&self) -> Result<String, String> {
        let deadline = Instant::now() + ADD_TIMEOUT;
        loop {
            if let Some(channel) = self
                .responses
                .try_resolve_channel_endpoint_port_as_string(4_096)
                .ok()
                .filter(|c| !c.is_empty() && !c.contains(":0|") && !c.ends_with(":0"))
            {
                return Ok(channel);
            }
            if Instant::now() > deadline {
                return Err("the notices' response channel did not resolve".into());
            }
            std::thread::yield_now();
        }
    }

    /// Offers the latest notice to the members that have not taken it, until
    /// all have or its time is up.
    fn send_notice(&mut self) -> usize {
        let _ = self.responses.poll_fn(|_, _| {}, FRAGMENT_LIMIT);
        let Some(notice) = self.notice.as_mut() else {
            return 0;
        };
        let mut work = 0;
        let mut pending = false;
        for (member, taken) in self.to_members.iter().zip(notice.taken.iter_mut()) {
            if !*taken {
                if member.offer(&notice.message).is_ok() {
                    *taken = true;
                    work += 1;
                } else {
                    pending = true;
                }
            }
        }
        if !pending || Instant::now() > notice.deadline {
            self.notice = None;
        }
        work
    }

    /// The standby's own snapshot it started from, if any.
    pub fn loaded(&self) -> Option<Snapshot> {
        self.loaded
    }

    /// The standby snapshots taken since it started, oldest first.
    pub fn snapshots(&self) -> &[Snapshot] {
        &self.snapshots
    }

    /// The module's state at a members' snapshot's log position, once the standby has passed it.
    pub fn state_at(&self, log_position: i64) -> Option<&ModuleState> {
        self.states.get(&log_position)
    }

    pub fn service_started(&self) -> bool {
        self.service_started
    }

    /// The commit position the service reads to.
    pub fn commit_position(&self) -> i64 {
        get(&self.commit_position)
    }

    /// The log position of the service's last ack: its start, a join, or a snapshot.
    pub fn last_ack_position(&self) -> i64 {
        self.last_ack_position
    }

    /// How many times the container has joined a replay of the log.
    pub fn joins(&self) -> u32 {
        self.joins
    }

    /// The snapshot action the service is held at until it acks there, if any.
    pub fn held_at(&self) -> Option<SnapshotAction> {
        self.snapshot_at
    }

    /// How far the replay may read: the local log recording's position.
    pub fn replay_limit(&self) -> i64 {
        get(&self.replay_limit)
    }

    /// Where the service's log ends so far: where the next replay begins.
    pub fn service_position(&self) -> i64 {
        self.service_position
    }

    /// Stops the replay. The counters go with the module, and the container
    /// terminates once its commit position is gone.
    pub fn close(mut self) {
        self.end_replay();
        self.end_tail();
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

/// Reads the module's state back from the standby's own snapshot set: its
/// module recording replayed from the start, to its end marker.
fn load_state(
    aeron: &Aeron,
    archive: &AeronArchive,
    config: &ModuleConfig,
    snapshot: &Snapshot,
) -> Result<ModuleState, String> {
    let params = AeronArchiveReplayParams::new(-1, -1, 0, -1, -1, -1).map_err(aeron_error("replay params"))?;
    let session_id = archive
        .start_replay(
            snapshot.module_recording_id,
            &c_string(&config.replay_channel),
            config.load_stream_id,
            &params,
        )
        .map_err(aeron_error("the snapshot's replay"))?;
    let channel = with_session_id(&config.replay_channel, session_id as i32);
    let subscription = aeron
        .add_subscription(
            &c_string(&channel),
            config.load_stream_id,
            Handlers::NONE,
            Handlers::NONE,
            ADD_TIMEOUT,
        )
        .map_err(aeron_error("the snapshot's subscription"))?;
    let mut messages: Vec<Vec<u8>> = Vec::new();
    let mut reassembler = Reassembler::default();
    let deadline = Instant::now() + SNAPSHOT_PATIENCE;
    let read = |messages: &[Vec<u8>]| {
        ModuleState::read(
            messages.iter().map(Vec::as_slice),
            snapshot.log_position,
            snapshot.leadership_term_id,
            snapshot.term_base_log_position,
        )
    };
    loop {
        let polled = subscription
            .poll_fn(
                |buffer, header| {
                    reassembler.on_fragment(fragment(buffer, header), |message, _| {
                        messages.push(message.to_vec());
                        PollAction::Continue
                    });
                },
                FRAGMENT_LIMIT,
            )
            .map_err(aeron_error("the snapshot's replay"))?;
        if polled > 0
            && let Ok(state) = read(&messages)
        {
            let _ = archive.stop_replay(session_id);
            return Ok(state);
        }
        if polled <= 0 {
            if Instant::now() > deadline {
                let _ = archive.stop_replay(session_id);
                return read(&messages)
                    .map_err(|e| format!("the module snapshot ended at message {}: {e}", messages.len()));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}
