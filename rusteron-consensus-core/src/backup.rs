//! Aeron Cluster's backup (Java's `ClusterBackupAgent`, with its
//! `SnapshotReplication`) as a state machine: it asks a member where the
//! cluster's log and snapshots are, copies the snapshots into its own
//! archive, records the live log there as the member's archive replays it
//! up to the cluster's commit position, and keeps a recording log from
//! which a node can start on what it holds.
//!
//! Inputs: the time ([`ClusterBackupAgent::on_tick`]), what members send it
//! on its consensus stream, both archives' responses, and what came of the
//! I/O it asked for ([`ClusterBackupAgent::on_io`]). Outputs: offers and
//! counters through [`Ports`], [`Action`]s for its shell, and [`Event`]s for
//! a listener (Java's `ClusterBackupEventsListener`).
//!
//! The states are Java's, in its order: [`State::BackupQuery`],
//! [`State::SnapshotRetrieve`], [`State::LiveLogRecord`],
//! [`State::LiveLogReplay`], [`State::UpdateRecordingLog`],
//! [`State::BackingUp`], and [`State::ResetBackup`] on an error or a stall.
//! Java blocks on its local archive (a recording started, a stop position
//! read, a replication accepted); here each is a request whose answer the
//! state waits for. Java 1.53.3's backup sends members no heartbeat
//! request: its liveness is its mark file's activity, which the shell keeps.

use std::collections::{HashMap, VecDeque};

use rusteron_archive_core::channel::ChannelUri;
use rusteron_archive_core::protocol::{Signal, error_code};
use rusteron_archive_core::session::{self as archive, ClientSession, Event as ArchiveEvent, Link};
use rusteron_cluster_codecs::cluster::backup_response_codec;
use rusteron_cluster_core::client::EventCode;
use rusteron_cluster_core::client::codec::{self as client_codec, Egress, PROTOCOL_SEMANTIC_VERSION};
use rusteron_cluster_core::recording_log::{RecordingLog, Snapshot};

use crate::codec::{self, SCHEMA_ID};
use crate::config::SERVICE_ID;
use crate::consensus;
use crate::io::{Offer, RecordingCounter};
use crate::members::{self, ClusterMember};

/// Aeron's null for ids, positions and timestamps.
pub const NULL_VALUE: i64 = -1;
const NULL_POSITION: i64 = -1;
const NULL_TIMESTAMP: i64 = -1;
/// A replay's length to the end of the recording, and on as it grows.
const NULL_LENGTH: i64 = -1;
/// How often the mark file's activity is refreshed (Java's `MARK_FILE_UPDATE_INTERVAL_MS`).
pub const MARK_FILE_UPDATE_INTERVAL_MS: i64 = 1_000;
const SLOW_TICK_INTERVAL_MS: i64 = 10;
/// The consensus stream's fragment limit, a poll (Java's `ConsensusAdapter`).
pub const CONSENSUS_FRAGMENT_LIMIT: usize = 10;
/// How many times the consensus publication is offered a message: Java's.
const SEND_ATTEMPTS: usize = 3;

/// `ClusterBackup.State`, as its counter shows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// The cluster asked where its log and snapshots are.
    BackupQuery = 0,
    /// The latest snapshots copied from the member's archive.
    SnapshotRetrieve = 1,
    /// The live log's recording set up in the local archive.
    LiveLogRecord = 2,
    /// The member's archive replaying the log into that recording.
    LiveLogReplay = 3,
    /// The recording log updated, for a node to start from.
    UpdateRecordingLog = 4,
    /// The live log followed until the next query.
    BackingUp = 5,
    /// After an error or a stall: everything let go, and a cool-down.
    ResetBackup = 6,
    Closed = 7,
}

impl State {
    pub fn code(self) -> i64 {
        self as i64
    }

    /// The state a counter's value names.
    pub fn from_code(code: i64) -> Option<State> {
        Some(match code {
            0 => State::BackupQuery,
            1 => State::SnapshotRetrieve,
            2 => State::LiveLogRecord,
            3 => State::LiveLogReplay,
            4 => State::UpdateRecordingLog,
            5 => State::BackingUp,
            6 => State::ResetBackup,
            7 => State::Closed,
            _ => return None,
        })
    }
}

/// Which member the log is copied from (`ClusterBackup.SourceType`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SourceType {
    #[default]
    Any,
    Leader,
    /// Any member but the leader: a follower's archive holds the committed
    /// log too, and spares the leader.
    Follower,
}

impl SourceType {
    /// Java's names: `ANY`, `LEADER`, `FOLLOWER`.
    pub fn parse(name: &str) -> Result<Self, String> {
        match name {
            "ANY" => Ok(Self::Any),
            "LEADER" => Ok(Self::Leader),
            "FOLLOWER" => Ok(Self::Follower),
            other => Err(format!(
                "ClusterBackup.Context.sourceType={other} is not valid. Must be one of: [ANY, LEADER, FOLLOWER]"
            )),
        }
    }

    /// Java's `LogSourceValidator.isAcceptable`: whether `member_id` may supply
    /// the log, the leader being `leader_member_id` ([`NULL_VALUE`] unknown).
    pub fn is_acceptable(self, leader_member_id: i32, member_id: i32) -> bool {
        let leader = i64::from(leader_member_id);
        match self {
            Self::Leader => leader != NULL_VALUE && leader_member_id == member_id,
            Self::Follower => leader == NULL_VALUE || leader_member_id != member_id,
            Self::Any => true,
        }
    }
}

/// Where a backup with no log of its own starts the log's replay
/// (`ClusterBackup.Configuration.ReplayStart`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReplayStart {
    /// The log's start: 0, unless the cluster's log was truncated.
    #[default]
    Beginning,
    /// The latest snapshot of the consensus module retrieved.
    LatestSnapshot,
}

impl ReplayStart {
    /// Java's names: `BEGINNING`, `LATEST_SNAPSHOT`.
    pub fn parse(name: &str) -> Result<Self, String> {
        match name {
            "BEGINNING" => Ok(Self::Beginning),
            "LATEST_SNAPSHOT" => Ok(Self::LatestSnapshot),
            other => Err(format!("unknown replay start: {other}")),
        }
    }
}

/// Where the live log's replay starts (Java's `replayStartPosition`): where
/// the local log's recording stopped (`last_term_stop_position`), when the
/// recording log has a term; else [`NULL_POSITION`], the log's start, or
/// the latest consensus module snapshot's position among those retrieved.
pub fn replay_start_position(
    last_term_stop_position: Option<i64>,
    snapshots_retrieved: &[Snapshot],
    replay_start: ReplayStart,
) -> i64 {
    if let Some(position) = last_term_stop_position {
        return position;
    }
    if replay_start == ReplayStart::Beginning {
        return NULL_POSITION;
    }
    snapshots_retrieved
        .iter()
        .filter(|s| s.service_id == SERVICE_ID)
        .fold(NULL_POSITION, |position, s| position.max(s.log_position))
}

/// What a backup tells an authenticator (Java's `CredentialsSupplier`).
pub trait CredentialsSupplier {
    /// Sent with each backup query.
    fn encoded_credentials(&self) -> Vec<u8>;
    /// The answer to a member's challenge.
    fn on_challenge(&mut self, encoded_challenge: &[u8]) -> Vec<u8>;
}

/// Fixed credentials, and a fixed answer to any challenge; both empty by
/// default, Java's `NullCredentialsSupplier`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FixedCredentials {
    pub credentials: Vec<u8>,
    pub challenge_response: Vec<u8>,
}

impl CredentialsSupplier for FixedCredentials {
    fn encoded_credentials(&self) -> Vec<u8> {
        self.credentials.clone()
    }

    fn on_challenge(&mut self, _: &[u8]) -> Vec<u8> {
        self.challenge_response.clone()
    }
}

/// An archive's control channels: its request publication, its response
/// subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveConfig {
    pub request_channel: String,
    pub request_stream_id: i32,
    pub response_channel: String,
    pub response_stream_id: i32,
}

/// How a backup runs: Java's `ClusterBackup.Context`, with its defaults.
#[derive(Clone, Debug)]
pub struct Config {
    pub cluster_id: i32,
    /// Where members answer the backup: its own endpoint on the consensus
    /// stream, also the template of its publications to them.
    pub consensus_channel: String,
    pub consensus_stream_id: i32,
    /// The members' consensus endpoints, comma-separated.
    pub cluster_consensus_endpoints: String,
    pub log_stream_id: i32,
    /// Where members' archives replay the log and copy snapshots to, `host:0`
    /// for any port: required.
    pub catchup_endpoint: String,
    pub catchup_channel: String,
    /// How long between queries while backing up.
    pub backup_interval_ns: i64,
    /// How long a member has to answer a query before the next is asked.
    pub response_timeout_ns: i64,
    /// How long without progress before a reset.
    pub progress_timeout_ns: i64,
    /// How long a reset waits before querying again.
    pub cool_down_interval_ns: i64,
    /// How long a snapshot's copy may go without progress.
    pub replication_progress_timeout_ns: i64,
    /// How often that progress is checked: [`NULL_VALUE`], a tenth of the timeout.
    pub replication_progress_interval_ns: i64,
    pub source_type: SourceType,
    pub initial_replay_start: ReplayStart,
    /// The backup's own archive, on its driver: IPC.
    pub local_archive: ArchiveConfig,
    /// A template for the source member's archive: its archive endpoint is
    /// set on the request channel, and its archive response endpoint, if it
    /// has one, makes the response channel a response channel.
    pub cluster_archive: ArchiveConfig,
    /// Seeds the endpoints' shuffle and the recording's session id.
    pub random_seed: u64,
    /// How long the live log may stand still while backing up before the
    /// backup resets and asks again; 0, never, as Java's. Not Java's
    /// (docs/backup.md, "Two departures"): Java's resets on a live log
    /// recording that ends, never on one left open that stops growing.
    /// Only for a cluster whose log moves on its own, by timers: an idle
    /// one's would reset each time.
    pub live_log_progress_timeout_ns: i64,
    /// Whether the members' newest snapshots are copied, before the live log
    /// is followed after each reset; false: never copied, for a standby that
    /// takes its own. Not Java's, which always copies. With
    /// [`ReplayStart::LatestSnapshot`] and no term filed, the log is then
    /// replayed from its start.
    pub retrieve_snapshots: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cluster_id: 0,
            consensus_channel: "aeron:udp?term-length=64k".into(),
            consensus_stream_id: 108,
            cluster_consensus_endpoints: String::new(),
            log_stream_id: 100,
            catchup_endpoint: String::new(),
            catchup_channel: "aeron:udp?alias=backup|cc=cubic|so-sndbuf=512k|so-rcvbuf=512k|rcv-wnd=512k".into(),
            backup_interval_ns: 3_600_000_000_000,
            response_timeout_ns: 5_000_000_000,
            progress_timeout_ns: 10_000_000_000,
            cool_down_interval_ns: 30_000_000_000,
            replication_progress_timeout_ns: 10_000_000_000,
            replication_progress_interval_ns: NULL_VALUE,
            source_type: SourceType::Any,
            initial_replay_start: ReplayStart::Beginning,
            local_archive: ArchiveConfig {
                request_channel: "aeron:ipc?term-length=64k".into(),
                request_stream_id: 10,
                response_channel: "aeron:ipc?term-length=64k".into(),
                response_stream_id: 20,
            },
            cluster_archive: ArchiveConfig {
                request_channel: "aeron:udp?endpoint=localhost:8010".into(),
                request_stream_id: 10,
                response_channel: "aeron:udp?endpoint=localhost:0".into(),
                response_stream_id: 20,
            },
            random_seed: 1,
            live_log_progress_timeout_ns: 0,
            retrieve_snapshots: true,
        }
    }
}

/// The backup's two archives.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ArchiveId {
    /// The backup's own, where the snapshots and the log are recorded.
    Local,
    /// The source member's, which replays the log.
    Cluster,
}

/// The backup's counters, as the shell holds them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BackupCounter {
    State,
    LiveLogPosition,
    /// Read back each duty cycle: `ClusterTool` may set it to ask for a query now.
    NextQueryDeadlineMs,
    SnapshotsRetrieved,
}

/// The shell's synchronous side.
pub trait Ports {
    /// Offers on the publication to the member queried.
    fn offer_consensus(&mut self, message: &[u8]) -> Offer;
    fn consensus_connected(&self) -> bool;

    /// Offers a request to an archive, once.
    fn offer_archive(&mut self, archive: ArchiveId, message: &[u8]) -> Offer;
    fn archive_publication_connected(&self, archive: ArchiveId) -> bool;
    fn archive_subscription_connected(&self, archive: ArchiveId) -> bool;
    /// The archive's response channel, its port resolved.
    fn archive_response_channel(&self, archive: ArchiveId) -> Option<String>;

    /// A correlation id unique on the driver: Aeron's.
    fn next_correlation_id(&mut self) -> i64;

    /// The endpoint the recording subscription bound, its port resolved.
    fn recording_subscription_endpoint(&self) -> Option<String>;

    /// The local archive's recording-position counter of the recording of
    /// `session_id`, once it is allocated and names its recording.
    fn find_recording_counter(&self, session_id: i32, archive_id: i64) -> Option<RecordingCounter>;
    /// The local archive's recording-position counter of `recording_id`, while it records.
    fn find_recording_counter_by_recording(&self, recording_id: i64, archive_id: i64) -> Option<RecordingCounter>;
    fn counter_value(&self, counter_id: i32) -> i64;
    /// Whether the counter is still the one registered as `registration_id`.
    fn is_counter_active(&self, counter_id: i32, registration_id: i64) -> bool;

    fn counter(&self, counter: BackupCounter) -> i64;
    fn set_counter(&mut self, counter: BackupCounter, value: i64);
}

/// What the backup asks of its shell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Where members answer: added once, at the start.
    AddConsensusSubscription {
        channel: String,
        stream_id: i32,
    },
    /// The publication to the next member to query, in place of any other.
    AddConsensusPublication {
        channel: String,
        stream_id: i32,
    },
    CloseConsensusPublication,
    /// An archive's request publication and response subscription; on a
    /// response channel (`control-mode=response`), the subscription first,
    /// the publication then naming it (`response-correlation-id`).
    AddArchive {
        archive: ArchiveId,
        request_channel: String,
        request_stream_id: i32,
        response_channel: String,
        response_stream_id: i32,
    },
    /// The archive's publication and subscription closed, its session over.
    CloseArchive {
        archive: ArchiveId,
    },
    /// A subscription holding the catch-up endpoint, so its wildcard port
    /// resolves before the archive records there.
    AddRecordingSubscription {
        channel: String,
        stream_id: i32,
    },
    CloseRecordingSubscription,
    /// Writes to `recording.log`, in order, then a sync when `sync` (Java's `force(2)`).
    WriteRecordingLog {
        writes: Vec<(u64, Vec<u8>)>,
        sync: bool,
    },
    /// An error for the mark file's error log and the errors counter, a
    /// warning as Java's `ClusterEvent`s are.
    Error {
        warn: bool,
        message: String,
    },
    /// The backup is done (Java's `AgentTerminationException`): the shell
    /// closes it and runs its termination hooks.
    Terminate {
        reason: String,
    },
}

/// What the shell reports back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Io {
    /// A publication or subscription could not be added: a member's host
    /// not resolved, say.
    AddFailed { message: String },
    /// A counter went: the live log recording's, when its image goes.
    CounterUnavailable { counter_id: i32 },
    /// The Aeron client closed under the backup.
    ClientClosed,
}

/// What the backup tells a listener (Java's `ClusterBackupEventsListener`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A query begins.
    BackupQuery,
    /// Something failed, or stalled: the backup starts over.
    PossibleFailure { message: String },
    /// A member answered: the cluster, the member the log comes from, and
    /// the snapshots to copy.
    BackupResponse {
        cluster_members: Vec<ClusterMember>,
        log_source_member: Box<ClusterMember>,
        snapshots_to_retrieve: Vec<Snapshot>,
    },
    /// The recording log changed ([`ClusterBackupAgent::recording_log`]),
    /// with the snapshots copied since the last change.
    UpdatedRecordingLog { snapshots_retrieved: Vec<Snapshot> },
    /// The live log's recording grew.
    LiveLogProgress {
        recording_id: i64,
        recording_pos_counter_id: i32,
        log_position: i64,
    },
}

/// What Java throws.
#[derive(Debug)]
enum Fault {
    /// The backup resets.
    Error(String),
    /// As `Error`, recorded as a warning.
    Warning(String),
    /// The backup ends (Java's `AgentTerminationException`).
    Terminate(String),
}

type Outcome<T> = Result<T, Fault>;

/// An archive's link, on the shell's ports.
struct ArchiveLink<'a, P: Ports> {
    ports: &'a mut P,
    archive: ArchiveId,
}

impl<P: Ports> Link for ArchiveLink<'_, P> {
    fn offer(&mut self, message: &[u8]) -> Offer {
        self.ports.offer_archive(self.archive, message)
    }
    fn publication_connected(&self) -> bool {
        self.ports.archive_publication_connected(self.archive)
    }
    fn subscription_connected(&self) -> bool {
        self.ports.archive_subscription_connected(self.archive)
    }
    fn resolved_response_channel(&self) -> Option<String> {
        self.ports.archive_response_channel(self.archive)
    }
    fn next_correlation_id(&mut self) -> i64 {
        self.ports.next_correlation_id()
    }
}

/// A term the member named, to append to the recording log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TermEntry {
    leadership_term_id: i64,
    term_base_log_position: i64,
}

/// The publication to a member, as Java's `PublicationGroup` keeps it: once
/// closed it is still the current one, and the next member is asked for
/// only when there is none or a query timed out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Current {
    None,
    Open,
    Closed,
}

/// Java's `PublicationGroup`: the members' consensus endpoints, shuffled
/// once and tried in turn, but one excluded, the last to redirect the
/// backup or to be no acceptable source.
#[derive(Debug)]
struct PublicationGroup {
    endpoints: Vec<String>,
    cursor: usize,
    excluded: Option<usize>,
    current: Current,
}

impl PublicationGroup {
    /// The next endpoint, its publication current. With a single endpoint
    /// its exclusion is ignored, where Java's loop would spin for good.
    fn next(&mut self) -> &str {
        loop {
            self.cursor += 1;
            if self.cursor >= self.endpoints.len() {
                self.cursor = 0;
            }
            if Some(self.cursor) != self.excluded || self.endpoints.len() == 1 {
                break;
            }
        }
        self.current = Current::Open;
        &self.endpoints[self.cursor]
    }

    /// Closes the current publication: whether it was open.
    fn close(&mut self) -> bool {
        let open = self.current == Current::Open;
        if open {
            self.current = Current::Closed;
        }
        open
    }

    fn close_and_exclude_current(&mut self) -> bool {
        self.excluded = Some(self.cursor);
        self.close()
    }
}

/// A snapshot's recording being copied into the local archive (Java's
/// `RecordingReplication`), its progress the local recording's counter.
#[derive(Debug)]
struct RecordingReplication {
    correlation_id: i64,
    /// The archive's id for it, which its signals carry.
    replication_id: Option<i64>,
    counter: Option<RecordingCounter>,
    recording_id: i64,
    position: i64,
    progress_deadline_ns: i64,
    progress_check_deadline_ns: i64,
    has_replication_ended: bool,
    has_synced: bool,
}

impl RecordingReplication {
    /// The destination's position, every `interval_ns`; the work done, or
    /// Java's error once it has not progressed within `timeout_ns`.
    fn poll(
        &mut self,
        now_ns: i64,
        interval_ns: i64,
        timeout_ns: i64,
        archive_id: i64,
        ports: &impl Ports,
    ) -> Outcome<usize> {
        if self.has_replication_ended {
            return Ok(0);
        }
        let mut work = 0;
        if now_ns >= self.progress_check_deadline_ns {
            self.progress_check_deadline_ns = now_ns + interval_ns;
            if self.counter.is_none() && self.recording_id != NULL_VALUE {
                // Found on EXTEND unless the driver had yet to register it then.
                self.counter = ports.find_recording_counter_by_recording(self.recording_id, archive_id);
            }
            if let Some(counter) = self.counter
                && ports.is_counter_active(counter.counter_id, counter.registration_id)
            {
                let position = ports.counter_value(counter.counter_id);
                if position > self.position {
                    self.position = position;
                    self.progress_deadline_ns = now_ns + timeout_ns;
                }
            }
            work += 1;
        }
        if now_ns >= self.progress_deadline_ns {
            // Snapshots are copied without a stop position.
            return Err(Fault::Warning("log replication has not progressed".into()));
        }
        Ok(work)
    }

    /// Java's `onSignal`, for a signal of this replication.
    fn on_signal(
        &mut self,
        recording_id: i64,
        position: i64,
        signal: Signal,
        archive_id: i64,
        ports: &impl Ports,
    ) -> Outcome<()> {
        if signal == Signal::EXTEND {
            self.counter = ports.find_recording_counter_by_recording(recording_id, archive_id);
        } else if signal == Signal::SYNC {
            self.has_synced = true;
        } else if signal == Signal::REPLICATE_END {
            self.has_replication_ended = true;
        } else if signal == Signal::DELETE {
            return Err(Fault::Error(format!(
                "recording was deleted during replication: recordingId={recording_id}"
            )));
        }
        if recording_id != NULL_VALUE {
            self.recording_id = recording_id;
        }
        if position != NULL_POSITION {
            self.position = position;
        }
        Ok(())
    }
}

/// Java's `SnapshotReplication` on its `MultipleRecordingReplication`: the
/// snapshots' recordings copied from the member's archive one at a time,
/// each again until its copy ends synced.
#[derive(Debug)]
struct SnapshotReplication {
    snapshots: Vec<Snapshot>,
    src_control_channel: String,
    src_control_stream_id: i32,
    replication_channel: String,
    cursor: usize,
    /// Source recording to local recording, of those copied.
    completed: HashMap<i64, i64>,
    current: Option<RecordingReplication>,
}

impl SnapshotReplication {
    fn is_complete(&self) -> bool {
        self.cursor >= self.snapshots.len()
    }

    /// The snapshots as copied: each with its local recording.
    fn snapshots_retrieved(&self) -> Vec<Snapshot> {
        self.snapshots
            .iter()
            .map(|s| Snapshot {
                recording_id: self.completed.get(&s.recording_id).copied().unwrap_or(NULL_VALUE),
                ..*s
            })
            .collect()
    }
}

/// Where the live log's replay starts, while the local recording's stop
/// position is asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplayStartQuery {
    NotAsked,
    Pending,
    Known(i64),
}

/// What the backup waits on its local archive for, by correlation id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalRequest {
    /// The live log's recording started or extended: its subscription.
    LogRecording,
    /// A recording asked for before a reset: stopped once it answers.
    LogRecordingAbandoned,
    /// The local log's stop position, where the replay starts.
    StopPosition,
    Replicate,
    /// A replication given up before the archive answered: stopped once it does.
    ReplicateAbandoned,
    /// A tryStop: whatever comes back is fine.
    TryStop,
}

/// Java's `ArchiveException.errorCodeAsString`.
fn error_code_name(code: i64) -> String {
    let name = match code {
        error_code::GENERIC => "GENERIC",
        error_code::ACTIVE_LISTING => "ACTIVE_LISTING",
        error_code::ACTIVE_RECORDING => "ACTIVE_RECORDING",
        error_code::ACTIVE_SUBSCRIPTION => "ACTIVE_SUBSCRIPTION",
        error_code::UNKNOWN_SUBSCRIPTION => "UNKNOWN_SUBSCRIPTION",
        error_code::UNKNOWN_RECORDING => "UNKNOWN_RECORDING",
        error_code::UNKNOWN_REPLAY => "UNKNOWN_REPLAY",
        error_code::MAX_REPLAYS => "MAX_REPLAYS",
        error_code::MAX_RECORDINGS => "MAX_RECORDINGS",
        error_code::INVALID_EXTENSION => "INVALID_EXTENSION",
        error_code::AUTHENTICATION_REJECTED => "AUTHENTICATION_REJECTED",
        error_code::STORAGE_SPACE => "STORAGE_SPACE",
        error_code::UNKNOWN_REPLICATION => "UNKNOWN_REPLICATION",
        error_code::UNAUTHORISED_ACTION => "UNAUTHORISED_ACTION",
        error_code::REPLICATION_CONNECTION_FAILURE => "REPLICATION_CONNECTION_FAILURE",
        error_code::EMPTY_RECORDING => "EMPTY_RECORDING",
        error_code::INVALID_POSITION => "INVALID_POSITION",
        other => return format!("unknown error code: {other}"),
    };
    name.into()
}

/// A channel from `template`: its `remove` parameters dropped, then each of
/// `put` set.
fn channel(template: &str, remove: &[&str], put: &[(&str, &str)]) -> Outcome<String> {
    let (media, params) = template.split_once('?').unwrap_or((template, ""));
    let kept: Vec<&str> = params
        .split('|')
        .filter(|p| !p.is_empty() && !remove.iter().any(|k| p.split('=').next() == Some(*k)))
        .collect();
    let text = if kept.is_empty() {
        media.to_owned()
    } else {
        format!("{media}?{}", kept.join("|"))
    };
    let mut uri = ChannelUri::parse(&text).map_err(Fault::Error)?;
    for (key, value) in put {
        uri.put(key, value);
    }
    Ok(uri.to_string())
}

/// The backup.
pub struct ClusterBackupAgent {
    config: Config,
    credentials: Box<dyn CredentialsSupplier + Send>,
    backup_response_timeout_ms: i64,
    backup_query_interval_ms: i64,
    backup_progress_timeout_ms: i64,
    cool_down_interval_ms: i64,
    replication_progress_interval_ns: i64,
    state: State,
    now_ms: i64,
    local: ClientSession,
    /// The local archive connected: Java's `onStart` done.
    started: bool,
    cluster: Option<ClientSession>,
    /// The source member's archive's request channel, which a replication names.
    cluster_request_channel: String,
    publications: PublicationGroup,
    snapshots_to_retrieve: Vec<Snapshot>,
    snapshots_retrieved: Vec<Snapshot>,
    snapshot_replication: Option<SnapshotReplication>,
    cluster_members: Vec<ClusterMember>,
    log_supplier_member: Option<ClusterMember>,
    recording_log: RecordingLog,
    leader_log_entry: Option<TermEntry>,
    leader_last_term_entry: Option<TermEntry>,
    recording_subscription: bool,
    replay_channel: Option<String>,
    recording_channel: Option<String>,
    slow_tick_deadline_ms: i64,
    time_of_last_backup_query_ms: i64,
    time_of_last_progress_ms: i64,
    cool_down_deadline_ms: Option<i64>,
    correlation_id: i64,
    cluster_log_recording_id: i64,
    live_log_recording_subscription_id: i64,
    live_log_recording_id: i64,
    live_log_replay_session_id: i64,
    leader_commit_position_counter_id: i32,
    live_log_recording_counter: Option<RecordingCounter>,
    live_log_recording_session_id: Option<i32>,
    /// The live log position counter's value, which only rises while backing up.
    live_log_position: i64,
    /// When the live log last moved on, or began: for [`Config::live_log_progress_timeout_ns`].
    live_log_moved_ms: i64,
    live_log_progress_timeout_ms: i64,
    replay_start: ReplayStartQuery,
    snapshots_retrieved_count: i64,
    local_requests: HashMap<i64, LocalRequest>,
    random_state: u64,
    terminated: bool,
    actions: VecDeque<Action>,
    events: VecDeque<Event>,
}

impl ClusterBackupAgent {
    /// A backup starting on its recording log at `now_ms` (epoch
    /// milliseconds): it subscribes where members answer it and connects to
    /// its archive.
    pub fn new(
        config: Config,
        recording_log: RecordingLog,
        credentials: Box<dyn CredentialsSupplier + Send>,
        now_ms: i64,
    ) -> Result<Self, String> {
        if config.catchup_endpoint.is_empty() {
            return Err("ClusterBackup.Context.catchupEndpoint must be set".into());
        }
        let local = &config.local_archive;
        if !local.request_channel.starts_with("aeron:ipc") || !local.response_channel.starts_with("aeron:ipc") {
            return Err("local archive control must be IPC".into());
        }
        let endpoints: Vec<String> = config
            .cluster_consensus_endpoints
            .split(',')
            .map(str::trim)
            .filter(|e| !e.is_empty())
            .map(str::to_owned)
            .collect();
        if endpoints.is_empty() {
            return Err("ClusterBackup.Context.clusterConsensusEndpoints must be set".into());
        }
        let ms = |ns: i64| ns / 1_000_000;
        let replication_progress_interval_ns = if config.replication_progress_interval_ns == NULL_VALUE {
            (config.replication_progress_timeout_ns / 10).max(1)
        } else {
            config.replication_progress_interval_ns
        };
        let archive_config = archive::Config {
            response_stream_id: local.response_stream_id,
            client_name: format!("cluster-backup clusterId={}", config.cluster_id),
            ..archive::Config::default()
        };
        let mut actions = VecDeque::new();
        actions.push_back(Action::AddConsensusSubscription {
            channel: config.consensus_channel.clone(),
            stream_id: config.consensus_stream_id,
        });
        actions.push_back(Action::AddArchive {
            archive: ArchiveId::Local,
            request_channel: local.request_channel.clone(),
            request_stream_id: local.request_stream_id,
            response_channel: local.response_channel.clone(),
            response_stream_id: local.response_stream_id,
        });
        let mut agent = Self {
            backup_response_timeout_ms: ms(config.response_timeout_ns),
            backup_query_interval_ms: ms(config.backup_interval_ns),
            backup_progress_timeout_ms: ms(config.progress_timeout_ns),
            cool_down_interval_ms: ms(config.cool_down_interval_ns),
            replication_progress_interval_ns,
            credentials,
            state: State::BackupQuery,
            now_ms,
            local: ClientSession::new(archive_config, now_ms.saturating_mul(1_000_000)),
            started: false,
            cluster: None,
            cluster_request_channel: String::new(),
            publications: PublicationGroup {
                endpoints,
                cursor: 0,
                excluded: None,
                current: Current::None,
            },
            snapshots_to_retrieve: Vec::new(),
            snapshots_retrieved: Vec::new(),
            snapshot_replication: None,
            cluster_members: Vec::new(),
            log_supplier_member: None,
            recording_log,
            leader_log_entry: None,
            leader_last_term_entry: None,
            recording_subscription: false,
            replay_channel: None,
            recording_channel: None,
            slow_tick_deadline_ms: 0,
            time_of_last_backup_query_ms: 0,
            time_of_last_progress_ms: now_ms,
            cool_down_deadline_ms: None,
            correlation_id: NULL_VALUE,
            cluster_log_recording_id: NULL_VALUE,
            live_log_recording_subscription_id: NULL_VALUE,
            live_log_recording_id: NULL_VALUE,
            live_log_replay_session_id: NULL_VALUE,
            leader_commit_position_counter_id: NULL_VALUE as i32,
            live_log_recording_counter: None,
            live_log_recording_session_id: None,
            live_log_position: 0,
            live_log_moved_ms: now_ms,
            live_log_progress_timeout_ms: ms(config.live_log_progress_timeout_ns),
            replay_start: ReplayStartQuery::NotAsked,
            snapshots_retrieved_count: 0,
            local_requests: HashMap::new(),
            random_state: config.random_seed,
            terminated: false,
            actions,
            events: VecDeque::new(),
            config,
        };
        agent.shuffle();
        Ok(agent)
    }

    pub fn state(&self) -> State {
        self.state
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The backup's recording log, as a node started from the backup reads it.
    pub fn recording_log(&self) -> &RecordingLog {
        &self.recording_log
    }

    /// How far the live log is recorded.
    pub fn live_log_position(&self) -> i64 {
        self.live_log_position
    }

    /// The live log's local recording, once its replay is joined.
    pub fn live_log_recording_id(&self) -> i64 {
        self.live_log_recording_id
    }

    /// The member the log is copied from, once one answered.
    pub fn log_supplier_member(&self) -> Option<&ClusterMember> {
        self.log_supplier_member.as_ref()
    }

    pub fn is_terminated(&self) -> bool {
        self.terminated
    }

    pub fn poll_action(&mut self) -> Option<Action> {
        self.actions.pop_front()
    }

    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    /// The time, in epoch milliseconds: read once a duty cycle.
    pub fn on_tick(&mut self, now_ms: i64) {
        self.now_ms = now_ms;
        let now_ns = now_ms.saturating_mul(1_000_000);
        self.local.on_tick(now_ns);
        if let Some(cluster) = self.cluster.as_mut() {
            cluster.on_tick(now_ns);
        }
    }

    /// A duty cycle (Java's `doWork`): the archives' sessions stepped, the
    /// slow tick, the state's work, a stall checked; then the requests the
    /// work made sent at once, as Java's calls send theirs.
    pub fn do_work(&mut self, ports: &mut impl Ports) -> usize {
        if self.terminated {
            return 0;
        }
        let mut work = self.step_archives(ports);
        self.drain_archive_events(ports);
        if !self.started || self.terminated {
            return work;
        }
        match self.work(ports) {
            Ok(n) => work += n,
            Err(fault) => self.handle(Err(fault), ports),
        }
        work + self.step_archives(ports)
    }

    /// Both archives' sessions: their connects' steps, their requests sent.
    fn step_archives(&mut self, ports: &mut impl Ports) -> usize {
        let mut work = self.local.do_work(&mut ArchiveLink {
            ports,
            archive: ArchiveId::Local,
        });
        if let Some(cluster) = self.cluster.as_mut() {
            work += cluster.do_work(&mut ArchiveLink {
                ports,
                archive: ArchiveId::Cluster,
            });
        }
        work
    }

    /// A message on the consensus stream, assembled.
    pub fn on_consensus_message(&mut self, message: &[u8], ports: &mut impl Ports) {
        if self.terminated || !self.started {
            return;
        }
        let result = self.on_consensus(message, ports);
        self.handle(result, ports);
    }

    /// A message from one of the archives, assembled.
    pub fn on_archive_message(&mut self, archive: ArchiveId, message: &[u8], ports: &mut impl Ports) {
        match archive {
            ArchiveId::Local => self.local.on_response(message),
            ArchiveId::Cluster => match self.cluster.as_mut() {
                Some(cluster) => cluster.on_response(message),
                None => return,
            },
        }
        self.drain_archive_events(ports);
    }

    /// What came of the shell's I/O.
    pub fn on_io(&mut self, io: Io, ports: &mut impl Ports) {
        if self.terminated {
            return;
        }
        let result = match io {
            Io::AddFailed { message } => Err(Fault::Error(message)),
            Io::CounterUnavailable { counter_id } => {
                if self
                    .live_log_recording_counter
                    .is_some_and(|c| c.counter_id == counter_id)
                {
                    self.events.push_back(Event::PossibleFailure {
                        message: "log recording counter unexpectedly unavailable".into(),
                    });
                    self.set_state(State::ResetBackup, ports);
                }
                Ok(())
            }
            Io::ClientClosed => Err(Fault::Terminate("unexpected Aeron close".into())),
        };
        self.handle(result, ports);
    }

    /// Java's `onClose`: the snapshots' copy, the replay and the recording
    /// stopped, both archives' sessions closed, the state CLOSED.
    pub fn close(&mut self, ports: &mut impl Ports) {
        if self.state == State::Closed {
            return;
        }
        self.close_snapshot_replication(ports);
        self.stop_replay(ports);
        self.stop_recording(ports);
        if self.publications.close() {
            self.actions.push_back(Action::CloseConsensusPublication);
        }
        if self.recording_subscription {
            self.recording_subscription = false;
            self.actions.push_back(Action::CloseRecordingSubscription);
        }
        self.set_state(State::Closed, ports);
        self.local.do_work(&mut ArchiveLink {
            ports,
            archive: ArchiveId::Local,
        });
        if let Some(close) = self.local.close() {
            ports.offer_archive(ArchiveId::Local, &close);
        }
        self.close_cluster_archive(ports);
        self.terminated = true;
    }

    // ---------------------------------------------------------------- faults

    /// Java's `doWork` catch: an error starts the backup over; a termination
    /// ends it, as does any error before it started.
    fn handle(&mut self, result: Outcome<()>, ports: &mut impl Ports) {
        let (warn, message) = match result {
            Ok(()) => return,
            Err(Fault::Terminate(reason)) => return self.terminate(reason),
            Err(Fault::Error(message)) => (false, message),
            Err(Fault::Warning(message)) => (true, message),
        };
        if !self.started {
            return self.terminate(message);
        }
        self.events.push_back(Event::PossibleFailure {
            message: message.clone(),
        });
        self.set_state(State::ResetBackup, ports);
        self.actions.push_back(Action::Error { warn, message });
    }

    fn terminate(&mut self, reason: String) {
        if self.terminated {
            return;
        }
        self.terminated = true;
        self.actions.push_back(Action::Error {
            warn: false,
            message: reason.clone(),
        });
        self.actions.push_back(Action::Terminate { reason });
    }

    fn error(&mut self, warn: bool, message: String) {
        self.actions.push_back(Action::Error { warn, message });
    }

    fn set_state(&mut self, state: State, ports: &mut impl Ports) {
        if state == State::BackupQuery {
            self.events.push_back(Event::BackupQuery);
        }
        ports.set_counter(BackupCounter::State, state.code());
        self.state = state;
        self.correlation_id = NULL_VALUE;
    }

    // ---------------------------------------------------------------- randomness

    fn next_random(&mut self) -> u64 {
        self.random_state = self.random_state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.random_state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Java's `PublicationGroup.shuffle`.
    fn shuffle(&mut self) {
        for i in (0..self.publications.endpoints.len()).rev() {
            let j = (self.next_random() % (i as u64 + 1)) as usize;
            self.publications.endpoints.swap(i, j);
        }
    }

    // ---------------------------------------------------------------- work

    fn work(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let now = self.now_ms;
        let mut work = 0;
        if now > self.slow_tick_deadline_ms {
            self.slow_tick_deadline_ms = now + SLOW_TICK_INTERVAL_MS;
            self.slow_tick(ports)?;
        }
        work += match self.state {
            State::BackupQuery => self.backup_query(ports)?,
            State::SnapshotRetrieve => self.snapshot_retrieve(ports)?,
            State::LiveLogRecord => self.live_log_record(ports)?,
            State::LiveLogReplay => self.live_log_replay(ports)?,
            State::UpdateRecordingLog => self.update_recording_log(ports)?,
            State::BackingUp => self.backing_up(ports),
            State::ResetBackup => self.reset_backup(ports),
            State::Closed => return Ok(work),
        };
        if self.live_log_recording_counter.is_none()
            && now > self.time_of_last_progress_ms + self.backup_progress_timeout_ms
        {
            self.events.push_back(Event::PossibleFailure {
                message: "progress has stalled".into(),
            });
            self.set_state(State::ResetBackup, ports);
        }
        Ok(work)
    }

    /// Java's `slowTick`, but for the client's conductor and the mark file,
    /// the shell's: the local archive must still answer.
    fn slow_tick(&mut self, ports: &mut impl Ports) -> Outcome<()> {
        if !ports.archive_subscription_connected(ArchiveId::Local) {
            return Err(Fault::Terminate("local archive not connected".into()));
        }
        Ok(())
    }

    fn reset_backup(&mut self, ports: &mut impl Ports) -> usize {
        let now = self.now_ms;
        self.time_of_last_progress_ms = now;
        match self.cool_down_deadline_ms {
            None => {
                self.cool_down_deadline_ms = Some(now + self.cool_down_interval_ms);
                self.reset(ports);
                1
            }
            Some(deadline) if now > deadline => {
                self.cool_down_deadline_ms = None;
                self.set_state(State::BackupQuery, ports);
                1
            }
            Some(_) => 0,
        }
    }

    /// Java's `reset`: what the last source gave let go.
    fn reset(&mut self, ports: &mut impl Ports) {
        self.cluster_members.clear();
        self.log_supplier_member = None;
        self.leader_log_entry = None;
        self.leader_last_term_entry = None;
        self.live_log_recording_counter = None;
        self.live_log_recording_id = NULL_VALUE;
        self.live_log_recording_session_id = None;
        self.snapshots_to_retrieve.clear();
        self.snapshots_retrieved.clear();
        self.close_snapshot_replication(ports);
        self.stop_recording(ports);
        self.stop_replay(ports);
        if self.publications.close() {
            self.actions.push_back(Action::CloseConsensusPublication);
        }
        self.close_cluster_archive(ports);
        if self.recording_subscription {
            self.recording_subscription = false;
            self.actions.push_back(Action::CloseRecordingSubscription);
        }
        self.replay_start = ReplayStartQuery::NotAsked;
        self.local_requests.retain(|_, r| *r != LocalRequest::StopPosition);
        for request in self.local_requests.values_mut() {
            if *request == LocalRequest::LogRecording {
                *request = LocalRequest::LogRecordingAbandoned;
            }
        }
    }

    fn stop_replay(&mut self, ports: &mut impl Ports) {
        if self.live_log_replay_session_id != NULL_VALUE {
            let replay_session_id = self.live_log_replay_session_id;
            self.live_log_replay_session_id = NULL_VALUE;
            if let Some(cluster) = self.cluster.as_mut()
                && cluster.is_connected()
            {
                let correlation_id = ports.next_correlation_id();
                let _ = cluster.request(correlation_id, &archive::Request::StopReplay { replay_session_id });
            }
        }
    }

    fn stop_recording(&mut self, ports: &mut impl Ports) {
        if self.live_log_recording_subscription_id != NULL_VALUE {
            let subscription_id = self.live_log_recording_subscription_id;
            self.live_log_recording_subscription_id = NULL_VALUE;
            if let Err(e) = self.local_request(
                ports,
                archive::Request::StopRecordingSubscription { subscription_id },
                LocalRequest::TryStop,
            ) {
                let message = match e {
                    Fault::Error(m) | Fault::Warning(m) | Fault::Terminate(m) => m,
                };
                self.error(true, format!("failed to stop log recording: {message}"));
            }
        }
    }

    /// The source member's archive let go: what was asked of it sent, then
    /// its session closed.
    fn close_cluster_archive(&mut self, ports: &mut impl Ports) {
        if let Some(mut cluster) = self.cluster.take() {
            cluster.do_work(&mut ArchiveLink {
                ports,
                archive: ArchiveId::Cluster,
            });
            if let Some(close) = cluster.close() {
                ports.offer_archive(ArchiveId::Cluster, &close);
            }
            self.actions.push_back(Action::CloseArchive {
                archive: ArchiveId::Cluster,
            });
        }
    }

    fn close_snapshot_replication(&mut self, ports: &mut impl Ports) {
        if let Some(mut replication) = self.snapshot_replication.take()
            && let Some(current) = replication.current.take()
        {
            self.stop_replication(current, ports);
        }
    }

    /// Java's `RecordingReplication.close`: a replication not ended stopped,
    /// or stopped once the archive gives its id.
    fn stop_replication(&mut self, replication: RecordingReplication, ports: &mut impl Ports) {
        if replication.has_replication_ended {
            return;
        }
        match replication.replication_id {
            Some(replication_id) => {
                let _ = self.local_request(
                    ports,
                    archive::Request::StopReplication { replication_id },
                    LocalRequest::TryStop,
                );
            }
            None => {
                self.local_requests
                    .insert(replication.correlation_id, LocalRequest::ReplicateAbandoned);
            }
        }
    }

    fn local_request(&mut self, ports: &mut impl Ports, request: archive::Request, kind: LocalRequest) -> Outcome<i64> {
        let correlation_id = ports.next_correlation_id();
        self.local
            .request(correlation_id, &request)
            .map_err(|e| Fault::Error(e.to_string()))?;
        self.local_requests.insert(correlation_id, kind);
        Ok(correlation_id)
    }

    fn cluster_connected(&self) -> bool {
        self.cluster.as_ref().is_some_and(ClientSession::is_connected)
    }

    /// Offers on the publication to the member up to three times: whether it
    /// went; an error when it is closed or full, as Java's `checkResult` throws.
    fn send_consensus(&mut self, ports: &mut impl Ports, message: &[u8]) -> Outcome<bool> {
        if self.publications.current != Current::Open {
            return Err(Fault::Error("publication is closed".into()));
        }
        for _ in 0..SEND_ATTEMPTS {
            match ports.offer_consensus(message) {
                Offer::Sent => return Ok(true),
                Offer::Closed => return Err(Fault::Error("publication is closed".into())),
                Offer::MaxPositionExceeded => return Err(Fault::Error("publication at max position".into())),
                Offer::Retry | Offer::NotConnected => {}
            }
        }
        Ok(false)
    }

    // ---------------------------------------------------------------- states

    fn backup_query(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let now = self.now_ms;
        if self.publications.current == Current::None
            || now > self.time_of_last_backup_query_ms + self.backup_response_timeout_ms
        {
            let endpoint = self.publications.next().to_owned();
            let channel = channel(&self.config.consensus_channel, &[], &[("endpoint", &endpoint)])?;
            self.actions.push_back(Action::AddConsensusPublication {
                channel,
                stream_id: self.config.consensus_stream_id,
            });
            self.correlation_id = NULL_VALUE;
            self.time_of_last_backup_query_ms = now;
            return Ok(1);
        }
        if self.correlation_id == NULL_VALUE
            && self.publications.current == Current::Open
            && ports.consensus_connected()
        {
            let correlation_id = ports.next_correlation_id();
            let message = consensus::backup_query(
                correlation_id,
                self.config.consensus_stream_id,
                PROTOCOL_SEMANTIC_VERSION,
                i64::MAX,
                &self.config.consensus_channel,
                &self.credentials.encoded_credentials(),
            );
            if self.send_consensus(ports, &message)? {
                self.time_of_last_backup_query_ms = now;
                self.correlation_id = correlation_id;
                return Ok(1);
            }
        }
        Ok(0)
    }

    fn snapshot_retrieve(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        if !self.cluster_connected() {
            return Ok(0);
        }
        let mut work = 0;
        if self.snapshot_replication.is_none() {
            let replication_channel = channel(
                &self.config.catchup_channel,
                &[],
                &[("endpoint", &self.config.catchup_endpoint)],
            )?;
            self.snapshot_replication = Some(SnapshotReplication {
                snapshots: self.snapshots_to_retrieve.clone(),
                src_control_channel: self.cluster_request_channel.clone(),
                src_control_stream_id: self.config.cluster_archive.request_stream_id,
                replication_channel,
                cursor: 0,
                completed: HashMap::new(),
                current: None,
            });
            work += 1;
        }
        work += self.poll_snapshot_replication(ports)?;
        self.time_of_last_progress_ms = self.now_ms;
        if self
            .snapshot_replication
            .as_ref()
            .is_some_and(SnapshotReplication::is_complete)
        {
            let replication = self.snapshot_replication.take().expect("checked");
            self.snapshots_retrieved.extend(replication.snapshots_retrieved());
            self.set_state(State::LiveLogRecord, ports);
            work += 1;
        }
        Ok(work)
    }

    /// Java's `MultipleRecordingReplication.poll`.
    fn poll_snapshot_replication(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let Some(mut replication) = self.snapshot_replication.take() else {
            return Ok(0);
        };
        let result = self.step_snapshot_replication(&mut replication, ports);
        if result.is_err()
            && let Some(current) = replication.current.take()
        {
            self.stop_replication(current, ports);
        }
        self.snapshot_replication = Some(replication);
        result
    }

    fn step_snapshot_replication(&mut self, r: &mut SnapshotReplication, ports: &mut impl Ports) -> Outcome<usize> {
        if r.is_complete() {
            return Ok(0);
        }
        let now_ns = self.now_ms.saturating_mul(1_000_000);
        let Some(current) = r.current.as_mut() else {
            self.replicate_current(r, now_ns, ports)?;
            return Ok(1);
        };
        current.poll(
            now_ns,
            self.replication_progress_interval_ns,
            self.config.replication_progress_timeout_ns,
            self.local.archive_id(),
            &*ports,
        )?;
        if !current.has_replication_ended {
            return Ok(0);
        }
        let ended = r.current.take().expect("current");
        if ended.has_synced {
            r.completed
                .insert(r.snapshots[r.cursor].recording_id, ended.recording_id);
            r.cursor += 1;
        } else {
            self.replicate_current(r, now_ns, ports)?;
        }
        Ok(1)
    }

    fn replicate_current(&mut self, r: &mut SnapshotReplication, now_ns: i64, ports: &mut impl Ports) -> Outcome<()> {
        let request = archive::Request::Replicate {
            src_recording_id: r.snapshots[r.cursor].recording_id,
            dst_recording_id: NULL_VALUE,
            stop_position: NULL_POSITION,
            src_control_stream_id: r.src_control_stream_id,
            src_control_channel: r.src_control_channel.clone(),
            replication_channel: r.replication_channel.clone(),
            replication_session_id: ports.next_correlation_id() as i32,
        };
        let correlation_id = self.local_request(ports, request, LocalRequest::Replicate)?;
        r.current = Some(RecordingReplication {
            correlation_id,
            replication_id: None,
            counter: None,
            recording_id: NULL_VALUE,
            position: NULL_POSITION,
            progress_deadline_ns: now_ns + self.config.replication_progress_timeout_ns,
            progress_check_deadline_ns: now_ns + self.replication_progress_interval_ns,
            has_replication_ended: false,
            has_synced: false,
        });
        Ok(())
    }

    fn live_log_record(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let now = self.now_ms;
        if self.live_log_recording_subscription_id == NULL_VALUE {
            if self.local_requests.values().any(|r| *r == LocalRequest::LogRecording) {
                return Ok(0);
            }
            let session_id = match self.live_log_recording_session_id {
                Some(id) => id,
                None => {
                    let id = self.next_random() as i32;
                    self.live_log_recording_session_id = Some(id);
                    id
                }
            };
            let session = session_id.to_string();
            let catchup_endpoint = self.config.catchup_endpoint.clone();
            let catchup_channel = self.config.catchup_channel.clone();
            if catchup_endpoint.ends_with(":0") {
                if !self.recording_subscription {
                    let tags = format!("{},{}", ports.next_correlation_id(), ports.next_correlation_id());
                    let recording_channel = channel(
                        &catchup_channel,
                        &["endpoint"],
                        &[("tags", &tags), ("session-id", &session)],
                    )?;
                    let subscription_channel = channel(&recording_channel, &[], &[("endpoint", &catchup_endpoint)])?;
                    self.recording_channel = Some(recording_channel);
                    self.recording_subscription = true;
                    self.actions.push_back(Action::AddRecordingSubscription {
                        channel: subscription_channel,
                        stream_id: self.config.log_stream_id,
                    });
                    self.time_of_last_progress_ms = now;
                    return Ok(1);
                }
                let Some(resolved) = ports.recording_subscription_endpoint() else {
                    return Ok(0);
                };
                let mut uri = ChannelUri::parse(&channel(&catchup_channel, &[], &[("endpoint", &catchup_endpoint)])?)
                    .map_err(Fault::Error)?;
                uri.replace_endpoint_wildcard_port(&resolved).map_err(Fault::Error)?;
                uri.put("session-id", &session);
                self.replay_channel = Some(uri.to_string());
            } else {
                let replay_channel = channel(
                    &catchup_channel,
                    &[],
                    &[("endpoint", &catchup_endpoint), ("session-id", &session)],
                )?;
                self.recording_channel = Some(replay_channel.clone());
                self.replay_channel = Some(replay_channel);
            }
            self.start_log_recording(ports)?;
            return Ok(1);
        }
        self.time_of_last_progress_ms = now;
        self.set_state(State::LiveLogReplay, ports);
        Ok(1)
    }

    /// Java's `startLogRecording`: the live log recorded from the source's
    /// replay, extending the last term's recording if there is one.
    fn start_log_recording(&mut self, ports: &mut impl Ports) -> Outcome<()> {
        let channel = self.recording_channel.clone().unwrap_or_default();
        let stream_id = self.config.log_stream_id;
        let request = match self.recording_log.find_last_term() {
            None => archive::Request::StartRecording {
                channel,
                stream_id,
                local: false,
                auto_stop: true,
            },
            Some(term) => archive::Request::ExtendRecording {
                recording_id: term.recording_id,
                channel,
                stream_id,
                local: false,
                auto_stop: true,
            },
        };
        self.local_request(ports, request, LocalRequest::LogRecording)?;
        Ok(())
    }

    fn live_log_replay(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let now = self.now_ms;
        if self.live_log_recording_id != NULL_VALUE {
            self.time_of_last_progress_ms = now;
            self.set_state(State::UpdateRecordingLog, ports);
            return Ok(0);
        }
        if !self.cluster_connected() {
            return Ok(0);
        }
        if self.correlation_id == NULL_VALUE {
            let start_position = match self.replay_start {
                ReplayStartQuery::Pending => return Ok(0),
                ReplayStartQuery::Known(position) => position,
                ReplayStartQuery::NotAsked => match self.recording_log.find_last_term() {
                    Some(term) => {
                        let recording_id = term.recording_id;
                        self.local_request(
                            ports,
                            archive::Request::StopPosition { recording_id },
                            LocalRequest::StopPosition,
                        )?;
                        self.replay_start = ReplayStartQuery::Pending;
                        return Ok(1);
                    }
                    None => replay_start_position(None, &self.snapshots_retrieved, self.config.initial_replay_start),
                },
            };
            let replay_id = ports.next_correlation_id();
            let request = archive::Request::BoundedReplay {
                recording_id: self.cluster_log_recording_id,
                position: start_position,
                length: NULL_LENGTH,
                limit_counter_id: self.leader_commit_position_counter_id,
                channel: self.replay_channel.take().unwrap_or_default(),
                stream_id: self.config.log_stream_id,
            };
            let cluster = self.cluster.as_mut().expect("connected");
            cluster
                .request(replay_id, &request)
                .map_err(|e| Fault::Error(e.to_string()))?;
            self.replay_start = ReplayStartQuery::NotAsked;
            self.correlation_id = replay_id;
            self.time_of_last_progress_ms = now;
            return Ok(1);
        }
        if self.live_log_replay_session_id == NULL_VALUE {
            return Ok(0);
        }
        if self.live_log_recording_counter.is_none() {
            let session_id = self.live_log_replay_session_id as i32;
            if let Some(counter) = ports.find_recording_counter(session_id, self.local.archive_id()) {
                self.live_log_position = ports.counter_value(counter.counter_id);
                ports.set_counter(BackupCounter::LiveLogPosition, self.live_log_position);
                self.live_log_recording_id = counter.recording_id;
                self.live_log_recording_counter = Some(counter);
                self.live_log_moved_ms = now;
                self.time_of_last_backup_query_ms = now;
                self.time_of_last_progress_ms = now;
                self.set_state(State::UpdateRecordingLog, ports);
                return Ok(1);
            }
        }
        Ok(0)
    }

    fn update_recording_log(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let now = self.now_ms;
        let snapshot_leadership_term_id = self
            .snapshots_retrieved
            .first()
            .map_or(NULL_VALUE, |s| s.leadership_term_id);
        let recording_id = self.live_log_recording_id;
        let mut updated = false;
        let mut update = || -> Result<(), String> {
            if let Some(e) = self.leader_log_entry
                && self.recording_log.is_unknown(e.leadership_term_id)
                && e.leadership_term_id <= snapshot_leadership_term_id
            {
                self.recording_log
                    .append_term(
                        recording_id,
                        e.leadership_term_id,
                        e.term_base_log_position,
                        NULL_TIMESTAMP,
                    )
                    .map_err(|e| e.0)?;
                updated = true;
                self.leader_log_entry = None;
            }
            if !self.snapshots_retrieved.is_empty() {
                for s in self.snapshots_retrieved.iter().rev() {
                    self.recording_log
                        .append_snapshot(
                            s.recording_id,
                            s.leadership_term_id,
                            s.term_base_log_position,
                            s.log_position,
                            s.timestamp,
                            s.service_id,
                        )
                        .map_err(|e| e.0)?;
                }
                updated = true;
            }
            if let Some(e) = self.leader_last_term_entry
                && self.recording_log.is_unknown(e.leadership_term_id)
            {
                self.recording_log
                    .append_term(
                        recording_id,
                        e.leadership_term_id,
                        e.term_base_log_position,
                        NULL_TIMESTAMP,
                    )
                    .map_err(|e| e.0)?;
                updated = true;
                self.leader_last_term_entry = None;
            }
            Ok(())
        };
        let result = update();
        if let Err(message) = result {
            self.error(false, message);
            return Err(Fault::Terminate("failed to update recording log".into()));
        }
        if updated {
            self.actions.push_back(Action::WriteRecordingLog {
                writes: self.recording_log.take_writes(),
                sync: true,
            });
            if !self.snapshots_retrieved.is_empty() {
                self.snapshots_retrieved_count += 1;
                ports.set_counter(BackupCounter::SnapshotsRetrieved, self.snapshots_retrieved_count);
            }
            self.events.push_back(Event::UpdatedRecordingLog {
                snapshots_retrieved: self.snapshots_retrieved.clone(),
            });
        }
        self.snapshots_retrieved.clear();
        self.snapshots_to_retrieve.clear();
        self.time_of_last_progress_ms = now;
        ports.set_counter(BackupCounter::NextQueryDeadlineMs, now + self.backup_query_interval_ms);
        self.set_state(State::BackingUp, ports);
        Ok(1)
    }

    fn backing_up(&mut self, ports: &mut impl Ports) -> usize {
        let now = self.now_ms;
        let mut work = 0;
        if now > ports.counter(BackupCounter::NextQueryDeadlineMs) {
            self.time_of_last_backup_query_ms = now;
            self.time_of_last_progress_ms = now;
            self.set_state(State::BackupQuery, ports);
            work += 1;
        }
        if let Some(counter) = self.live_log_recording_counter {
            let position = ports.counter_value(counter.counter_id);
            if position > self.live_log_position {
                self.live_log_position = position;
                self.live_log_moved_ms = now;
                ports.set_counter(BackupCounter::LiveLogPosition, position);
                self.events.push_back(Event::LiveLogProgress {
                    recording_id: self.live_log_recording_id,
                    recording_pos_counter_id: counter.counter_id,
                    log_position: position,
                });
                work += 1;
            }
        }
        // A source whose replay stops sending with its image kept: nothing else ends the backup's wait.
        if self.state == State::BackingUp
            && self.live_log_progress_timeout_ms > 0
            && now > self.live_log_moved_ms + self.live_log_progress_timeout_ms
        {
            self.events.push_back(Event::PossibleFailure {
                message: "the live log has not progressed".into(),
            });
            self.set_state(State::ResetBackup, ports);
            work += 1;
        }
        work
    }

    // ---------------------------------------------------------------- members

    fn on_consensus(&mut self, message: &[u8], ports: &mut impl Ports) -> Outcome<()> {
        let header = codec::header(message).map_err(Fault::Error)?;
        if header.schema_id != SCHEMA_ID {
            return Err(Fault::Error(format!(
                "expected schemaId={SCHEMA_ID}, actual={}",
                header.schema_id
            )));
        }
        if header.template_id == backup_response_codec::SBE_TEMPLATE_ID {
            let response = consensus::decode_backup_response(message).map_err(Fault::Error)?;
            return self.on_backup_response(response, ports);
        }
        match client_codec::decode_egress(message).map_err(Fault::Error)? {
            Egress::Challenge {
                cluster_session_id,
                encoded_challenge,
                ..
            } => self.on_challenge(cluster_session_id, encoded_challenge, ports),
            Egress::SessionEvent {
                correlation_id,
                code,
                detail,
                ..
            } => self.on_session_event(correlation_id, code, &detail, ports),
            _ => Ok(()),
        }
    }

    fn on_backup_response(&mut self, r: consensus::BackupResponse, ports: &mut impl Ports) -> Outcome<()> {
        if i64::from(r.member_id) == NULL_VALUE {
            self.error(
                true,
                "onBackupResponse(): memberId is null, retrying for compatible node".into(),
            );
            self.set_state(State::ResetBackup, ports);
            return Ok(());
        }
        if !self.config.source_type.is_acceptable(r.leader_member_id, r.member_id) {
            if self.publications.close_and_exclude_current() {
                self.actions.push_back(Action::CloseConsensusPublication);
            }
            self.set_state(State::ResetBackup, ports);
            return Ok(());
        }
        if self.log_supplier_member.as_ref().is_some_and(|m| m.id != r.member_id) {
            self.set_state(State::ResetBackup, ports);
            return Ok(());
        }
        if self.state != State::BackupQuery || r.correlation_id != self.correlation_id {
            return Ok(());
        }
        let listed = if self.config.retrieve_snapshots {
            r.snapshots.as_slice()
        } else {
            &[]
        };
        for s in listed {
            if self
                .recording_log
                .get_latest_snapshot(s.service_id)
                .is_some_and(|e| e.log_position == s.log_position)
            {
                continue;
            }
            self.snapshots_to_retrieve.push(Snapshot {
                recording_id: s.recording_id,
                leadership_term_id: s.leadership_term_id,
                term_base_log_position: s.term_base_log_position,
                log_position: s.log_position,
                timestamp: s.timestamp,
                service_id: s.service_id,
            });
        }
        if self.log_supplier_member.as_ref().is_none_or(|m| m.id != r.member_id)
            || r.log_recording_id != self.cluster_log_recording_id
        {
            self.cluster_log_recording_id = r.log_recording_id;
            self.leader_log_entry = Some(TermEntry {
                leadership_term_id: r.log_leadership_term_id,
                term_base_log_position: r.log_term_base_log_position,
            });
        }
        if self
            .recording_log
            .find_last_term()
            .is_none_or(|last| last.leadership_term_id != r.last_leadership_term_id)
        {
            self.leader_last_term_entry = Some(TermEntry {
                leadership_term_id: r.last_leadership_term_id,
                term_base_log_position: r.last_term_base_log_position,
            });
        }
        self.time_of_last_backup_query_ms = 0;
        self.correlation_id = NULL_VALUE;
        self.leader_commit_position_counter_id = r.commit_position_counter_id;

        let mut cluster_members = members::parse(&r.cluster_members).map_err(|e| Fault::Error(e.0))?;
        members::set_is_leader(&mut cluster_members, r.leader_member_id);
        let Some(mut log_supplier_member) = members::find_member(&cluster_members, r.member_id).cloned() else {
            let ids: Vec<i32> = cluster_members.iter().map(|m| m.id).collect();
            return Err(Fault::Error(format!("{} not found in {ids:?}", r.member_id)));
        };
        log_supplier_member.leadership_term_id = r.log_leadership_term_id;
        self.events.push_back(Event::BackupResponse {
            cluster_members: cluster_members.clone(),
            log_source_member: Box::new(log_supplier_member.clone()),
            snapshots_to_retrieve: self.snapshots_to_retrieve.clone(),
        });
        self.cluster_members = cluster_members;

        if self.cluster.is_none() {
            let template = &self.config.cluster_archive;
            let request_channel = channel(
                &template.request_channel,
                &[],
                &[("endpoint", &log_supplier_member.archive_endpoint)],
            )?;
            let response_channel = match &log_supplier_member.archive_response_endpoint {
                Some(endpoint) => channel(
                    &template.response_channel,
                    &["endpoint"],
                    &[("control-mode", "response"), ("control", endpoint)],
                )?,
                None => template.response_channel.clone(),
            };
            let config = archive::Config {
                response_stream_id: template.response_stream_id,
                client_name: format!("cluster-backup clusterId={}", self.config.cluster_id),
                ..archive::Config::default()
            };
            self.actions.push_back(Action::AddArchive {
                archive: ArchiveId::Cluster,
                request_channel: request_channel.clone(),
                request_stream_id: template.request_stream_id,
                response_channel,
                response_stream_id: template.response_stream_id,
            });
            self.cluster = Some(ClientSession::new(config, self.now_ms.saturating_mul(1_000_000)));
            self.cluster_request_channel = request_channel;
        }
        self.log_supplier_member = Some(log_supplier_member);
        self.time_of_last_progress_ms = self.now_ms;
        let next = if self.snapshots_to_retrieve.is_empty() {
            State::LiveLogRecord
        } else {
            State::SnapshotRetrieve
        };
        self.set_state(next, ports);
        Ok(())
    }

    fn on_challenge(
        &mut self,
        cluster_session_id: i64,
        encoded_challenge: &[u8],
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        let response = self.credentials.on_challenge(encoded_challenge);
        self.correlation_id = ports.next_correlation_id();
        let message = client_codec::challenge_response(self.correlation_id, cluster_session_id, &response);
        self.send_consensus(ports, &message)?;
        Ok(())
    }

    fn on_session_event(
        &mut self,
        correlation_id: i64,
        code: EventCode,
        detail: &str,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        if correlation_id != self.correlation_id {
            return Ok(());
        }
        match code {
            EventCode::ERROR | EventCode::AUTHENTICATION_REJECTED => Err(Fault::Error(format!("{code}: {detail}"))),
            EventCode::REDIRECT => {
                if self.publications.close_and_exclude_current() {
                    self.actions.push_back(Action::CloseConsensusPublication);
                }
                self.set_state(State::ResetBackup, ports);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    // ---------------------------------------------------------------- archives

    fn drain_archive_events(&mut self, ports: &mut impl Ports) {
        while let Some(event) = self.local.poll_event() {
            let result = self.on_local_event(event, ports);
            self.handle(result, ports);
        }
        while let Some(event) = self.cluster.as_mut().and_then(ClientSession::poll_event) {
            let result = self.on_cluster_event(event, ports);
            self.handle(result, ports);
        }
    }

    fn on_local_event(&mut self, event: ArchiveEvent, ports: &mut impl Ports) -> Outcome<()> {
        match event {
            ArchiveEvent::Connected { .. } => {
                if !self.started {
                    // Java's `onStart`, once its archive connected.
                    self.started = true;
                    ports.set_counter(BackupCounter::NextQueryDeadlineMs, self.now_ms - 1);
                    self.time_of_last_progress_ms = self.now_ms;
                }
            }
            ArchiveEvent::ConnectFailed { message } => return Err(Fault::Terminate(message)),
            ArchiveEvent::Disconnected => return Err(Fault::Terminate("local archive not connected".into())),
            ArchiveEvent::Ok {
                correlation_id,
                relevant_id,
            } => match self.local_requests.remove(&correlation_id) {
                Some(LocalRequest::LogRecording) => {
                    self.live_log_recording_subscription_id = relevant_id;
                    self.recording_channel = None;
                    if self.recording_subscription {
                        self.recording_subscription = false;
                        self.actions.push_back(Action::CloseRecordingSubscription);
                    }
                }
                Some(LocalRequest::LogRecordingAbandoned) => {
                    let _ = self.local_request(
                        ports,
                        archive::Request::StopRecordingSubscription {
                            subscription_id: relevant_id,
                        },
                        LocalRequest::TryStop,
                    );
                }
                Some(LocalRequest::StopPosition) => self.replay_start = ReplayStartQuery::Known(relevant_id),
                Some(LocalRequest::Replicate) => {
                    if let Some(current) = self.snapshot_replication.as_mut().and_then(|r| r.current.as_mut())
                        && current.correlation_id == correlation_id
                    {
                        current.replication_id = Some(relevant_id);
                    }
                }
                Some(LocalRequest::ReplicateAbandoned) => {
                    let _ = self.local_request(
                        ports,
                        archive::Request::StopReplication {
                            replication_id: relevant_id,
                        },
                        LocalRequest::TryStop,
                    );
                }
                Some(LocalRequest::TryStop) | None => {}
            },
            ArchiveEvent::Error {
                correlation_id,
                error_code: code,
                message,
            } => {
                let request = self.local_requests.remove(&correlation_id);
                if matches!(
                    request,
                    Some(
                        LocalRequest::TryStop | LocalRequest::LogRecordingAbandoned | LocalRequest::ReplicateAbandoned
                    )
                ) {
                    return Ok(());
                }
                if code == error_code::STORAGE_SPACE {
                    return Err(Fault::Terminate(message));
                }
                return Err(Fault::Error(message));
            }
            ArchiveEvent::RecordingUnknown {
                correlation_id,
                recording_id,
            } => {
                if self.local_requests.remove(&correlation_id).is_some() {
                    return Err(Fault::Error(format!("unknown recording id: {recording_id}")));
                }
            }
            ArchiveEvent::TimedOut { correlation_id } => match self.local_requests.remove(&correlation_id) {
                Some(
                    LocalRequest::TryStop | LocalRequest::LogRecordingAbandoned | LocalRequest::ReplicateAbandoned,
                )
                | None => {}
                Some(_) => {
                    return Err(Fault::Error(format!(
                        "archive response timeout: correlationId={correlation_id}"
                    )));
                }
            },
            ArchiveEvent::Signal {
                correlation_id,
                recording_id,
                position,
                signal,
                ..
            } => {
                let archive_id = self.local.archive_id();
                if let Some(current) = self.snapshot_replication.as_mut().and_then(|r| r.current.as_mut())
                    && current.replication_id == Some(correlation_id)
                {
                    current.on_signal(recording_id, position, signal, archive_id, &*ports)?;
                }
            }
            ArchiveEvent::Descriptor { .. } => {}
        }
        Ok(())
    }

    fn on_cluster_event(&mut self, event: ArchiveEvent, ports: &mut impl Ports) -> Outcome<()> {
        let awaiting_replay = self.state == State::LiveLogReplay && self.correlation_id != NULL_VALUE;
        match event {
            ArchiveEvent::ConnectFailed { message } => return Err(Fault::Error(message)),
            ArchiveEvent::Ok {
                correlation_id,
                relevant_id,
            } => {
                if awaiting_replay
                    && correlation_id == self.correlation_id
                    && self.live_log_replay_session_id == NULL_VALUE
                {
                    self.live_log_replay_session_id = relevant_id;
                    self.time_of_last_progress_ms = self.now_ms;
                }
            }
            ArchiveEvent::Error {
                correlation_id,
                error_code: code,
                message,
            } => {
                // Java polls the replay's answer, then any error while its
                // counter is awaited; outside, any error the archive sends.
                if awaiting_replay
                    && (correlation_id == self.correlation_id || self.live_log_replay_session_id != NULL_VALUE)
                {
                    return Err(Fault::Error(format!(
                        "Live log replay failed (replaySessionId={}): errorMessage={message}, errorCode={}",
                        self.live_log_replay_session_id,
                        error_code_name(code)
                    )));
                }
                if self.correlation_id == NULL_VALUE {
                    self.error(true, format!("cluster archive error: {message}"));
                    self.set_state(State::ResetBackup, ports);
                }
            }
            ArchiveEvent::RecordingUnknown { correlation_id, .. } => {
                if awaiting_replay && correlation_id == self.correlation_id {
                    return Err(Fault::Error("Live log replay failed: RECORDING_UNKNOWN".into()));
                }
            }
            ArchiveEvent::TimedOut { correlation_id } => {
                if awaiting_replay && correlation_id == self.correlation_id {
                    return Err(Fault::Error(format!(
                        "archive response timeout: correlationId={correlation_id}"
                    )));
                }
            }
            ArchiveEvent::Connected { .. }
            | ArchiveEvent::Disconnected
            | ArchiveEvent::Signal { .. }
            | ArchiveEvent::Descriptor { .. } => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channels_are_built_from_templates() {
        assert_eq!(
            channel("aeron:udp?endpoint=a:1|alias=x", &["endpoint"], &[("tags", "1,2")]).unwrap(),
            "aeron:udp?alias=x|tags=1,2"
        );
        assert_eq!(
            channel("aeron:udp?term-length=64k", &[], &[("endpoint", "h:9")]).unwrap(),
            "aeron:udp?term-length=64k|endpoint=h:9"
        );
        assert_eq!(
            channel(
                "aeron:udp?endpoint=localhost:0",
                &["endpoint"],
                &[("control-mode", "response")]
            )
            .unwrap(),
            "aeron:udp?control-mode=response"
        );
    }

    #[test]
    fn sources_are_acceptable_as_java_validates_them() {
        assert!(SourceType::Any.is_acceptable(-1, 1));
        assert!(SourceType::Leader.is_acceptable(1, 1));
        assert!(!SourceType::Leader.is_acceptable(0, 1));
        assert!(!SourceType::Leader.is_acceptable(-1, 1));
        assert!(SourceType::Follower.is_acceptable(0, 1));
        assert!(SourceType::Follower.is_acceptable(-1, 1));
        assert!(!SourceType::Follower.is_acceptable(1, 1));
    }

    #[test]
    fn a_group_skips_its_excluded_endpoint() {
        let mut group = PublicationGroup {
            endpoints: vec!["a".into(), "b".into(), "c".into()],
            cursor: 0,
            excluded: None,
            current: Current::None,
        };
        assert_eq!(group.next(), "b");
        assert!(group.close_and_exclude_current());
        assert_eq!(group.current, Current::Closed);
        assert_eq!(group.next(), "c");
        assert_eq!(group.next(), "a");
        assert_eq!(group.next(), "c", "b is excluded");
    }
}
