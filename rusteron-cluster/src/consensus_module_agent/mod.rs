//! The consensus module: Java's `ConsensusModuleAgent` and its `Election`,
//! as a state machine.
//!
//! Inputs: the time ([`ConsensusModule::on_tick`]), messages from the
//! archive, the services, clients and the images it reads, and what came of
//! the I/O it asked for ([`ConsensusModule::on_io`]). Outputs: what goes
//! out synchronously through [`Ports`], and [`Action`]s, taken with
//! [`ConsensusModule::poll_action`].
//!
//! Java waits in place in several places (for the archive to answer, for a
//! recording's counter, for services' acks, for a snapshot to be recorded);
//! here each wait is a state the duty cycle checks.
//!
//! The election is in the `election` submodule, what members say to each
//! other outside it and each one's part after it in `peers`.

use std::collections::{HashMap, VecDeque};

use crate::cluster_counters::{self as cluster_counters, RecoveryState};
use crate::codecs::cluster::cluster_action::ClusterAction;
use crate::node_state_file::NodeState;
use crate::recording_log::{RecordingExtent, RecordingLog, RecoveryPlan, Snapshot};
use rusteron_archive::catalog::Descriptor;
use rusteron_archive::channel_uri::ChannelUri;
use rusteron_archive::client::aeron_archive::{self as archive, ClientSession, Event as ArchiveEvent, Link};
use rusteron_archive::control_session_adapter::{Signal, error_code};
use rusteron_archive::fragment_assembler::{Fragment, PollAction, Reassembler};

use crate::cluster_member::{self, ClusterMember};
use crate::consensus_adapter::{self, BackupResponse, BackupSnapshot, ConsensusMessage, NewLeadershipTerm};
use crate::consensus_module::context::{Config, SERVICE_ID};
use crate::consensus_module_agent::ports::{Action, CounterId, Io, Offer, Ports, RecordingCounter, SubscriptionId};
use crate::consensus_module_snapshot_taker::{self, Loaded, Loader};
use crate::ingress_adapter::{self, IngressMessage};
use crate::log_publisher::{self, LogMessage};
use crate::pending_service_message_tracker::{PendingServiceMessages, service_id_from_log_message};
use crate::service_proxy::{self, ServiceAck, ServiceAcks, ServiceMessage};
use crate::session_manager::{self, IngressImage, SessionAction, SessionManager, Term};
use crate::timer_service::Timers;

mod election;
mod peers;
pub mod ports;
mod standby_snapshot_replicator;

pub use standby_snapshot_replicator::node_toggle;

/// Aeron's null for ids and positions.
pub const NULL_VALUE: i64 = -1;
const NULL_POSITION: i64 = -1;
/// The slow tick: sessions, the control toggle, liveness.
const SLOW_TICK_INTERVAL_NS: i64 = 10_000_000;
/// How many times the services' publication is offered a message.
const SERVICE_SEND_ATTEMPTS: usize = 5;
/// The ingress' fragment limit, a poll.
pub const INGRESS_FRAGMENT_LIMIT: usize = 50;
/// The log's fragment limit, a poll.
pub const LOG_FRAGMENT_LIMIT: usize = 50;
/// The consensus stream's fragment limit, a poll (Java's `ConsensusAdapter`).
pub const CONSENSUS_FRAGMENT_LIMIT: usize = 10;
const IPC_CHANNEL: &str = "aeron:ipc";
const UDP_CHANNEL: &str = "aeron:udp";
const SPY_PREFIX: &str = "aeron-spy:";

/// `ConsensusModule.State`, as its counter shows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModuleState {
    Init = 0,
    Active = 1,
    Suspended = 2,
    Snapshot = 3,
    Quitting = 4,
    Terminating = 5,
    Closed = 6,
}

/// `Cluster.Role`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower = 0,
    Candidate = 1,
    Leader = 2,
}

/// `ElectionState`, as its counter shows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ElectionState {
    Init = 0,
    Canvass = 1,
    Nominate = 2,
    CandidateBallot = 3,
    FollowerBallot = 4,
    LeaderLogReplication = 5,
    LeaderReplay = 6,
    LeaderInit = 7,
    LeaderReady = 8,
    FollowerLogReplication = 9,
    FollowerReplay = 10,
    FollowerCatchupInit = 11,
    FollowerCatchupAwait = 12,
    FollowerCatchup = 13,
    FollowerLogInit = 14,
    FollowerLogAwait = 15,
    FollowerReady = 16,
    Closed = 17,
}

/// The control toggle's states (`ClusterControl.ToggleState`).
pub mod toggle {
    pub const INACTIVE: i32 = 0;
    pub const NEUTRAL: i32 = 1;
    pub const SUSPEND: i32 = 2;
    pub const RESUME: i32 = 3;
    pub const SNAPSHOT: i32 = 4;
    pub const SHUTDOWN: i32 = 5;
    pub const ABORT: i32 = 6;
    pub const STANDBY_SNAPSHOT: i32 = 7;
}

/// A subscription's images the shell polls for the module, and how.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Poll {
    /// Every image, up to [`INGRESS_FRAGMENT_LIMIT`] fragments.
    Ingress,
    /// The image of `session_id`, bounded by `limit`, up to [`LOG_FRAGMENT_LIMIT`].
    Image { session_id: i32, limit: i64 },
    /// Every image, assembled, up to [`CONSENSUS_FRAGMENT_LIMIT`]: what
    /// other members send, to [`ConsensusModule::on_consensus_fragment`].
    Consensus,
}

/// What the module wants done because Java would have thrown.
#[derive(Debug)]
enum Fault {
    /// Recorded; inside an election, the election starts again.
    Error(String),
    /// As `Error`, recorded as a warning: what Java throws with the WARN
    /// category, its `ClusterEvent`s and some timeouts.
    Warning(String),
    /// The archive's storage is full, or a fatal error: the node stops.
    Terminate(String),
}

impl From<String> for Fault {
    fn from(message: String) -> Self {
        Fault::Error(message)
    }
}

type Outcome<T> = Result<T, Fault>;

/// The archive link, on the shell's ports.
struct ArchiveLink<'a, P: Ports>(&'a mut P);

impl<P: Ports> Link for ArchiveLink<'_, P> {
    fn offer(&mut self, message: &[u8]) -> Offer {
        self.0.offer_archive(message)
    }
    fn publication_connected(&self) -> bool {
        self.0.archive_publication_connected()
    }
    fn subscription_connected(&self) -> bool {
        self.0.archive_subscription_connected()
    }
    fn resolved_response_channel(&self) -> Option<String> {
        self.0.archive_response_channel()
    }
    fn next_correlation_id(&mut self) -> i64 {
        self.0.next_correlation_id()
    }
}

/// An image the module reads: a snapshot's replay, the log's replay, the
/// leader's log a follower follows.
#[derive(Debug)]
struct ImageReader {
    subscription: SubscriptionId,
    session_id: i32,
    /// Its position, once its image is joined.
    position: Option<i64>,
    /// Where the last whole message read ended, or where the image joined:
    /// the services' log position follows messages, so not past a term's
    /// padding frame nor inside a fragmented message, as `position` may be.
    message_position: Option<i64>,
    reassembler: Reassembler,
    /// Gone: its publication closed it, or it timed out.
    closed: bool,
    end_of_stream: bool,
    term_buffer_length: i32,
}

impl ImageReader {
    fn new(subscription: SubscriptionId, session_id: i32) -> Self {
        Self {
            subscription,
            session_id,
            position: None,
            message_position: None,
            reassembler: Reassembler::default(),
            closed: false,
            end_of_stream: false,
            term_buffer_length: 0,
        }
    }
}

/// An image on the follower's log subscription, available to join.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LogImage {
    session_id: i32,
    join_position: i64,
    term_buffer_length: i32,
}

/// Recovery as the node starts (Java's `onStart`).
#[derive(Debug)]
enum Startup {
    ConnectArchive,
    /// Standbys' snapshots newer than the member's own, copied before the
    /// recovery plan is made.
    ReplicateStandby,
    /// The recording the plan needs, being listed.
    ListLogRecording {
        correlation_id: i64,
    },
    /// The module's snapshot, replayed.
    LoadSnapshot(SnapshotLoad),
    /// The services start from their snapshots, then ack.
    AwaitServices,
}

#[derive(Debug)]
enum SnapshotLoad {
    Replay { correlation_id: i64 },
    Read { reader: ImageReader, loader: Loader },
}

/// A replay of the local log to the services (Java's `LogReplay`).
#[derive(Debug)]
struct LogReplay {
    start: i64,
    stop: i64,
    replay_session_id: i64,
    /// Whether the services were told to join the replay.
    services_asked: bool,
    step: ReplayStep,
}

#[derive(Debug)]
enum ReplayStep {
    Replay {
        correlation_id: i64,
    },
    AwaitImage {
        reader: ImageReader,
    },
    AwaitServices {
        reader: ImageReader,
    },
    /// The image read as the module's log (`reader`).
    Read,
}

/// A member joins the new term's log (Java's `joinLogAsLeader`,
/// `tryJoinLogAsFollower`): recorded, its recording's counter found, the
/// services told to join it and their acks awaited.
#[derive(Debug)]
enum JoinLog {
    StartRecording,
    AwaitCounter,
    AwaitServices,
}

/// The waits of Java's `prepareForNewLeadership` as an election starts
/// again: the log's recording stopped and listed, its counter gone, the
/// follower's log sockets closed.
#[derive(Debug)]
struct Prepare {
    /// Where the log is rolled back to.
    log_position: i64,
    step: PrepareStep,
    /// The append position's counter as the recording was stopped.
    counter: Option<RecordingCounter>,
    sockets: Vec<SubscriptionId>,
    /// Whether anything was prepared: at a node's start nothing is.
    ran: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum PrepareStep {
    /// The recording's stop position, asked again until it has one.
    StopPosition {
        correlation_id: Option<i64>,
    },
    /// The recovery plan's recording, listed.
    RecoveryPlan {
        correlation_id: i64,
    },
    CounterGone,
    SocketsClosed,
    Done,
}

/// A follower's replication of the leader's recording into its own
/// (Java's `RecordingReplication`).
#[derive(Debug)]
struct Replication {
    correlation_id: i64,
    /// The archive's id for it, which its signals carry.
    replication_id: Option<i64>,
    stop_position: i64,
    /// The destination recording's counter, once it is extended.
    counter: Option<RecordingCounter>,
    recording_id: i64,
    position: i64,
    progress_deadline_ns: i64,
    progress_check_deadline_ns: i64,
    has_replication_ended: bool,
    has_synced: bool,
    has_stopped: bool,
}

/// An election, as Java's `Election` runs it for this node.
#[derive(Debug)]
struct Election {
    state: ElectionState,
    is_node_startup: bool,
    is_first_init: bool,
    is_extended_canvass: bool,
    /// A leader that closed its log: not waited for in the canvass.
    graceful_closed_leader_id: i32,
    log_position: i64,
    append_position: i64,
    notified_commit_position: i64,
    catchup_join_position: i64,
    replication_leadership_term_id: i64,
    replication_stop_position: i64,
    replication_deadline_ns: i64,
    /// Where the leader followed began its term, as its new term said.
    leader_term_base_log_position: i64,
    replication_term_base_log_position: i64,
    leader_recording_id: i64,
    log_leadership_term_id: i64,
    initial_log_leadership_term_id: i64,
    initial_term_base_log_position: i64,
    initial_time_of_last_update_ns: i64,
    leadership_term_id: i64,
    candidate_term_id: i64,
    time_of_last_state_change_ns: i64,
    time_of_last_update_ns: i64,
    time_of_last_commit_position_update_ns: i64,
    nomination_deadline_ns: i64,
    /// Before this the member does not stand: it has just taken a newer
    /// term from a candidate it refused, and waits a canvass round for a
    /// leader of that term to answer.
    stand_not_before_ns: i64,
    last_published_commit_position: i64,
    last_published_append_position: i64,
    log_session_id: Option<i32>,
    is_leader_startup: bool,
    /// The leader's index in the members, once known.
    leader: Option<usize>,
    /// When that leader was last heard from: its new term or commit position.
    time_of_last_leader_message_ns: i64,
    /// A replay given up and held until the services ack this position.
    replay_drain: Option<i64>,
    /// When the election last said it stalls.
    time_of_last_stall_warning_ns: i64,
    log_replay: Option<LogReplay>,
    log_replication: Option<Replication>,
    join_log: Option<JoinLog>,
    prepare: Option<Prepare>,
}

/// The module's snapshot under way (Java's `takeSnapshot`).
#[derive(Debug)]
struct TakeSnapshot {
    log_position: i64,
    timestamp: i64,
    acks: Vec<ServiceAck>,
    step: SnapshotStep,
}

#[derive(Debug)]
enum SnapshotStep {
    /// A service failed to take its snapshot: none is taken.
    Incomplete,
    AddPublication,
    AwaitPublication,
    StartRecording,
    AwaitCounter {
        session_id: i32,
    },
    Offer {
        counter: RecordingCounter,
        messages: Vec<Vec<u8>>,
        next: usize,
    },
    AwaitRecorded {
        counter: RecordingCounter,
        position: i64,
    },
}

/// A shutdown or abort under way (Java's `ClusterTermination`).
#[derive(Debug)]
struct ClusterTermination {
    deadline_ns: i64,
    is_awaiting_services: bool,
}

/// What the module waits on the archive for, by correlation id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Request {
    /// A tryStop: whatever comes back is fine.
    TryStop,
    /// A recording started or extended for the log: its subscription.
    LogRecording,
    SnapshotRecording,
    /// The recovery plan's recording, listed after the start.
    PlanRefresh {
        recording_id: i64,
    },
    StopReplay,
    /// The log recording's stop position, as an election starts again.
    PrepareStopPosition,
    PrepareRecoveryPlan,
    /// A follower's replication of the leader's log.
    Replicate,
    /// A standby's snapshot copied from its archive.
    ReplicateStandby,
    /// A replication given up before the archive answered: stopped once it does.
    ReplicateAbandoned,
    /// A leader's replay of its log to a follower catching up.
    CatchupReplay {
        member_id: i32,
    },
}

/// The consensus module.
pub struct ConsensusModule {
    config: Config,
    members: Vec<ClusterMember>,
    this: usize,
    leader: usize,
    ingress_endpoints: String,
    state: ModuleState,
    role: Role,
    recording_log: RecordingLog,
    node_state: NodeState,
    archive: ClientSession,
    sessions: SessionManager,
    timers: Timers,
    expired_timer_counts: HashMap<i64, i64>,
    /// (append position, correlation id) of timers fired, not committed.
    uncommitted_timers: VecDeque<(i64, i64)>,
    /// (append position, state before) of actions appended, not committed.
    uncommitted_previous_state: VecDeque<(i64, ModuleState)>,
    trackers: Vec<PendingServiceMessages>,
    service_acks: ServiceAcks,
    service_ack_id: i64,
    expected_ack_position: i64,
    service_client_ids: Vec<i64>,

    leadership_term_id: i64,
    commit_position: i64,
    notified_commit_position: i64,
    last_quorum_backtrack_commit_position: i64,
    log_recording_id: i64,
    log_subscription_id: i64,
    log_recording_stop_position: i64,
    log_session_id: Option<i32>,
    log_publication_channel_tag: i64,
    append_counter: Option<RecordingCounter>,
    recovery_plan: Option<RecoveryPlan>,

    cluster_time: i64,
    now_ns: i64,
    slow_tick_deadline_ns: i64,
    time_of_last_log_update_ns: i64,

    startup: Option<Startup>,
    election: Option<Election>,
    take_snapshot: Option<TakeSnapshot>,
    termination: Option<ClusterTermination>,
    termination_position: i64,
    termination_leadership_term_id: i64,

    requests: HashMap<i64, Request>,
    ingress: Option<SubscriptionId>,
    ingress_reassemblers: HashMap<i32, Reassembler>,
    consensus: Option<SubscriptionId>,
    consensus_reassemblers: HashMap<i32, Reassembler>,
    /// Each consensus image's source, as a backup's session names it.
    consensus_sources: HashMap<i32, String>,
    /// Standbys' notices filed in the recording log.
    standby_snapshots_received: i64,
    /// The log's image as the module reads it (Java's `LogAdapter`): a
    /// replay, or the leader's log a follower follows.
    reader: Option<ImageReader>,
    /// Where the log's image was when it went.
    log_adapter_position: i64,
    /// A follower's log subscription, its channel, and the images on it.
    log_subscription: Option<SubscriptionId>,
    log_subscription_channel: String,
    log_subscription_images: Vec<LogImage>,
    live_log_destination: Option<String>,
    catchup_log_destination: Option<String>,
    time_of_last_leader_update_ns: i64,
    time_of_last_append_position_update_ns: i64,
    time_of_last_append_position_send_ns: i64,
    last_append_position: i64,
    random_state: u64,
    next_subscription: u64,
    snapshot_count: i64,
    /// Standbys' snapshots being copied, at a start or on the node toggle.
    standby_replication: Option<standby_snapshot_replicator::StandbyReplication>,
    election_count: i64,
    terminated: bool,
    actions: VecDeque<Action>,
}

impl ConsensusModule {
    /// A module starting from its files, at cluster time `cluster_time`.
    pub fn new(
        config: Config,
        recording_log: RecordingLog,
        node_state: NodeState,
        cluster_time: i64,
    ) -> Result<Self, String> {
        let members = cluster_member::parse(&config.cluster_members).map_err(|e| e.to_string())?;
        let this = members
            .iter()
            .position(|m| m.id == config.member_id)
            .ok_or_else(|| format!("memberId={} not found in clusterMembers", config.member_id))?;
        if config.ingress_channel.is_empty() {
            return Err("ingressChannel must be specified".into());
        }
        let now_ns = config.time_unit.to_nanos(cluster_time);
        let archive_config = archive::Config {
            response_stream_id: config.cluster_id * 100 + 100 + 20,
            client_name: format!("consensus-module-{}-{}", config.cluster_id, config.member_id),
            ..archive::Config::default()
        };
        let sessions = SessionManager::new(
            config.member_id,
            config.cluster_id,
            config.session_timeout_ns,
            config.max_concurrent_sessions,
            config.leader_heartbeat_timeout_ns,
            config.accept_standby_snapshots,
            config.standby_snapshot_notification_delay_ns,
        );
        let ingress_endpoints = cluster_member::ingress_endpoints(&members);
        let mut actions = VecDeque::new();
        let mut consensus_uri =
            ChannelUri::parse(&config.consensus_channel).map_err(|e| format!("consensusChannel: {e:?}"))?;
        if consensus_uri.get("endpoint").is_none() {
            consensus_uri.put("endpoint", &members[this].consensus_endpoint);
        }
        let consensus = SubscriptionId(1);
        actions.push_back(Action::AddSubscription {
            subscription: consensus,
            channel: consensus_uri.to_string(),
            stream_id: config.consensus_stream_id,
        });
        Ok(Self {
            trackers: (0..config.service_count as i32)
                .map(PendingServiceMessages::new)
                .collect(),
            service_acks: ServiceAcks::new(config.service_count),
            service_client_ids: vec![NULL_VALUE; config.service_count],
            archive: ClientSession::new(archive_config, now_ns),
            members,
            this,
            leader: this,
            ingress_endpoints,
            state: ModuleState::Init,
            role: Role::Follower,
            recording_log,
            node_state,
            sessions,
            timers: Timers::new(),
            expired_timer_counts: HashMap::new(),
            uncommitted_timers: VecDeque::new(),
            uncommitted_previous_state: VecDeque::new(),
            service_ack_id: 0,
            expected_ack_position: 0,
            leadership_term_id: NULL_VALUE,
            commit_position: 0,
            notified_commit_position: 0,
            last_quorum_backtrack_commit_position: NULL_POSITION,
            log_recording_id: NULL_VALUE,
            log_subscription_id: NULL_VALUE,
            log_recording_stop_position: 0,
            log_session_id: None,
            log_publication_channel_tag: NULL_VALUE,
            append_counter: None,
            recovery_plan: None,
            cluster_time,
            now_ns,
            slow_tick_deadline_ns: 0,
            time_of_last_log_update_ns: 0,
            startup: Some(Startup::ConnectArchive),
            election: None,
            take_snapshot: None,
            termination: None,
            termination_position: NULL_POSITION,
            termination_leadership_term_id: NULL_VALUE,
            requests: HashMap::new(),
            ingress: None,
            ingress_reassemblers: HashMap::new(),
            consensus: Some(consensus),
            consensus_reassemblers: HashMap::new(),
            consensus_sources: HashMap::new(),
            standby_snapshots_received: 0,
            reader: None,
            log_adapter_position: 0,
            log_subscription: None,
            log_subscription_channel: String::new(),
            log_subscription_images: Vec::new(),
            live_log_destination: None,
            catchup_log_destination: None,
            time_of_last_leader_update_ns: 0,
            time_of_last_append_position_update_ns: 0,
            time_of_last_append_position_send_ns: 0,
            last_append_position: NULL_POSITION,
            random_state: config.random_seed,
            next_subscription: 2,
            snapshot_count: 0,
            standby_replication: None,
            election_count: 0,
            terminated: false,
            actions,
            config,
        })
    }

    pub fn state(&self) -> ModuleState {
        self.state
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn commit_position(&self) -> i64 {
        self.commit_position
    }

    pub fn leadership_term_id(&self) -> i64 {
        self.leadership_term_id
    }

    pub fn election_state(&self) -> Option<ElectionState> {
        self.election.as_ref().map(|e| e.state)
    }

    pub fn is_terminated(&self) -> bool {
        self.terminated
    }

    pub fn poll_action(&mut self) -> Option<Action> {
        self.actions.pop_front()
    }

    /// The time, in the cluster's unit: read once a duty cycle.
    pub fn on_tick(&mut self, cluster_time: i64) {
        self.cluster_time = cluster_time;
        self.now_ns = self.config.time_unit.to_nanos(cluster_time);
        self.archive.on_tick(self.now_ns);
    }

    /// The subscriptions whose images the shell polls for the module, and
    /// how far: the log's bound reads counters, as Java's does before a poll.
    pub fn polls(&self, ports: &impl Ports) -> Vec<(SubscriptionId, Poll)> {
        let mut polls = Vec::new();
        // Java polls the consensus stream once the node started: what other
        // members sent meanwhile waits for the election.
        if let Some(consensus) = self.consensus
            && self.startup.is_none()
        {
            polls.push((consensus, Poll::Consensus));
        }
        if let Some(ingress) = self.ingress
            && self.election.is_none()
            && self.startup.is_none()
        {
            let polled = match self.role {
                Role::Leader => self.state == ModuleState::Active,
                _ => {
                    (self.state == ModuleState::Active || self.state == ModuleState::Suspended)
                        && !(self.termination_position != NULL_POSITION
                            && self.log_adapter_position() >= self.termination_position)
                }
            };
            if polled {
                polls.push((ingress, Poll::Ingress));
            }
        }
        if let Some(reader) = &self.reader
            && reader.position.is_some()
            && !reader.closed
            && let Some(limit) = self.log_poll_limit(ports)
        {
            polls.push((
                reader.subscription,
                Poll::Image {
                    session_id: reader.session_id,
                    limit,
                },
            ));
        }
        polls
    }

    /// How far the log's image is read: a snapshot to its end, a replay to
    /// its stop, a follower's log to the commit position the leader
    /// notified and its own recording reached.
    fn log_poll_limit(&self, ports: &impl Ports) -> Option<i64> {
        if self.startup.is_some() {
            return Some(i64::MAX);
        }
        let active = self.state == ModuleState::Active || self.state == ModuleState::Suspended;
        let recorded = || self.append_counter.map(|c| ports.counter_value(c.counter_id));
        match &self.election {
            Some(election) => {
                if let Some(replay) = &election.log_replay
                    && matches!(replay.step, ReplayStep::Read)
                {
                    // Given up, it is held for the services, read no further
                    // than the end of a message (`replay_drained`).
                    let reading = election.state != ElectionState::Init
                        || (election.replay_drain.is_none()
                            && self.reader.as_ref().is_some_and(|r| r.message_position != r.position));
                    return (active && reading).then_some(replay.stop);
                }
                if election.state == ElectionState::FollowerCatchup && active {
                    return recorded().map(|append| append.min(election.notified_commit_position));
                }
                None
            }
            None => {
                if self.role == Role::Leader || !active {
                    return None;
                }
                if self.termination_position != NULL_POSITION
                    && self.log_adapter_position() >= self.termination_position
                {
                    return None;
                }
                let limit = recorded().unwrap_or(self.log_recording_stop_position);
                Some(self.notified_commit_position.min(limit))
            }
        }
    }

    // ---------------------------------------------------------------- duty cycle

    /// One duty cycle's own work: the slow tick when due, the startup or
    /// election under way, or the leader's work.
    pub fn do_work(&mut self, ports: &mut impl Ports) -> usize {
        if self.terminated {
            return 0;
        }
        let mut work = self.archive.do_work(&mut ArchiveLink(ports));
        self.drain_archive_events(ports);
        let result = self.work(ports, &mut work);
        self.handle(result, ports);
        self.actions.extend(self.sessions.take_actions());
        work
    }

    fn work(&mut self, ports: &mut impl Ports, work: &mut usize) -> Outcome<()> {
        // Java's `pollArchiveEvents`, from its slow tick, its idle and an
        // election's log replication: the local archive's responses gone,
        // the node ends. It would no longer record its log.
        if self.archive.is_connected() && !ports.archive_subscription_connected() {
            self.archive.on_disconnected();
            self.drain_archive_events(ports);
            return Ok(());
        }
        if self.startup.is_some() {
            *work += self.startup_work(ports)?;
            return Ok(());
        }
        if self.now_ns >= self.slow_tick_deadline_ns {
            let slow = self.slow_tick_work(ports)?;
            *work += slow;
            self.slow_tick_deadline_ns = if slow > 0 {
                self.now_ns + 1
            } else {
                self.now_ns + SLOW_TICK_INTERVAL_NS
            };
        }
        if self.election.is_some() {
            *work += self.election_work(ports)?;
        } else {
            *work += self.consensus_work(ports)?;
        }
        if self.take_snapshot.is_some() {
            *work += self.snapshot_work(ports)?;
        }
        Ok(())
    }

    /// Java's `doWork` catch: an error inside an election starts it again;
    /// outside, it is recorded and the module goes on; the archive's storage
    /// full ends the node.
    fn handle(&mut self, result: Outcome<()>, ports: &mut impl Ports) {
        let (warn, message) = match result {
            Ok(()) => return,
            Err(Fault::Terminate(reason)) => {
                self.error(reason.clone());
                self.unexpected_termination(&reason, ports);
                return;
            }
            Err(Fault::Error(message)) => (false, message),
            Err(Fault::Warning(message)) => (true, message),
        };
        self.actions.push_back(Action::Error {
            warn,
            message: message.clone(),
        });
        if self.startup.is_some() {
            self.unexpected_termination(&message, ports);
        } else if let Some(election) = self.election.as_mut() {
            // Java's `Election.handleError`: the election starts over.
            election.log_position = self.commit_position;
            election.prepare = None;
            self.set_election_state(ElectionState::Init, ports);
        }
    }

    fn error(&mut self, message: String) {
        self.actions.push_back(Action::Error { warn: false, message });
    }

    /// What Java records with the WARN category, a `ClusterEvent` mostly.
    fn warn(&mut self, message: String) {
        self.actions.push_back(Action::Error { warn: true, message });
    }

    fn set_state(&mut self, state: ModuleState, ports: &mut impl Ports) {
        if state != self.state {
            self.state = state;
            ports.set_counter(CounterId::ModuleState, state as i64);
        }
    }

    fn set_role(&mut self, role: Role, ports: &mut impl Ports) {
        if role != self.role {
            self.role = role;
            ports.set_counter(CounterId::NodeRole, role as i64);
        }
    }

    fn set_leadership_term_id(&mut self, leadership_term_id: i64, ports: &mut impl Ports) {
        self.leadership_term_id = leadership_term_id;
        ports.set_counter(CounterId::LeadershipTermId, leadership_term_id);
    }

    fn set_commit_position(&mut self, position: i64, ports: &mut impl Ports) {
        self.commit_position = position;
        ports.set_counter(CounterId::CommitPosition, position);
    }

    /// Java's `proposeMaxRelease`: the commit position only rises.
    fn propose_commit_position(&mut self, position: i64, ports: &mut impl Ports) -> bool {
        if position > self.commit_position {
            self.set_commit_position(position, ports);
            return true;
        }
        false
    }

    fn timestamp(&self) -> i64 {
        self.cluster_time
    }

    fn term(&self) -> Term {
        Term {
            leadership_term_id: self.leadership_term_id,
            leader_member_id: self.members[self.leader].id,
            timestamp: self.timestamp(),
            now_ns: self.now_ns,
        }
    }

    fn new_subscription(&mut self, channel: String, stream_id: i32) -> SubscriptionId {
        let subscription = SubscriptionId(self.next_subscription);
        self.next_subscription += 1;
        self.actions.push_back(Action::AddSubscription {
            subscription,
            channel,
            stream_id,
        });
        subscription
    }

    fn archive_request(
        &mut self,
        ports: &mut impl Ports,
        request: archive::Request,
        kind: Option<Request>,
    ) -> Outcome<i64> {
        let correlation_id = ports.next_correlation_id();
        self.archive
            .request(correlation_id, &request)
            .map_err(|e| Fault::Error(e.to_string()))?;
        if let Some(kind) = kind {
            self.requests.insert(correlation_id, kind);
        }
        Ok(correlation_id)
    }

    fn offer_service(&mut self, ports: &mut impl Ports, message: &[u8]) -> bool {
        (0..SERVICE_SEND_ATTEMPTS).any(|_| ports.offer_service(message) == Offer::Sent)
    }

    // ---------------------------------------------------------------- startup

    fn startup_work(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        match self.startup.as_mut().expect("starting") {
            Startup::ConnectArchive
            | Startup::ListLogRecording { .. }
            | Startup::LoadSnapshot(SnapshotLoad::Replay { .. }) => Ok(0),
            Startup::LoadSnapshot(SnapshotLoad::Read { .. }) => Ok(0),
            Startup::ReplicateStandby => {
                let (work, done) = self.standby_replication_work(ports);
                if done {
                    self.plan_recovery(ports)?;
                }
                Ok(work)
            }
            Startup::AwaitServices => {
                if !self
                    .service_acks
                    .has_reached(self.expected_ack_position, self.service_ack_id)?
                {
                    return Ok(0);
                }
                let acks = self.service_acks.poll(self.expected_ack_position)?;
                let mut complete = true;
                for (service_id, ack) in acks.iter().enumerate().rev() {
                    if ack.relevant_id == NULL_VALUE {
                        self.warn(format!("service={service_id} failed to start clustered service"));
                        complete = false;
                    }
                }
                if !complete {
                    return Err(Fault::Terminate("failed to start clustered service(s)".into()));
                }
                for (service_id, ack) in acks.iter().enumerate() {
                    self.service_client_ids[service_id] = ack.relevant_id;
                }
                self.service_ack_id += 1;
                self.actions.push_back(Action::RemoveRecoveryStateCounter);
                self.startup = None;
                self.start_election(ports);
                self.election_work(ports)?;
                self.set_state(ModuleState::Active, ports);
                Ok(1)
            }
        }
    }

    /// The archive connected: the last term's recording stopped in case it
    /// still records, then the recovery plan.
    fn on_archive_connected(&mut self, ports: &mut impl Ports) -> Outcome<()> {
        let last = self.recording_log.find_last_term_recording_id();
        if last != NULL_VALUE {
            self.archive_request(
                ports,
                archive::Request::StopRecordingByIdentity { recording_id: last },
                Some(Request::TryStop),
            )?;
        }
        if self.start_standby_replication() {
            self.startup = Some(Startup::ReplicateStandby);
            return Ok(());
        }
        self.plan_recovery(ports)
    }

    /// The recovery plan, once its log recording is listed.
    fn plan_recovery(&mut self, ports: &mut impl Ports) -> Outcome<()> {
        match self.recording_log.plan_recording_id(self.log_recording_id) {
            Some(recording_id) => {
                let correlation_id =
                    self.archive_request(ports, archive::Request::ListRecording { recording_id }, None)?;
                self.startup = Some(Startup::ListLogRecording { correlation_id });
                Ok(())
            }
            None => self.on_recovery_plan(None, ports),
        }
    }

    /// Java's `recoverFromSnapshotAndLog`, the plan made.
    fn on_recovery_plan(&mut self, extent: Option<RecordingExtent>, ports: &mut impl Ports) -> Outcome<()> {
        let plan = self
            .recording_log
            .create_recovery_plan(self.config.service_count as i32, self.log_recording_id, extent)
            .map_err(|e| Fault::Terminate(e.to_string()))?;
        if let Some(log) = plan.log {
            self.log_recording_id = log.recording_id;
        }
        let (key, label) = recovery_state_counter(&plan, self.leadership_term_id, self.config.cluster_id);
        self.actions.push_back(Action::AddRecoveryStateCounter { key, label });
        let snapshot = plan.snapshots.first().copied();
        self.recovery_plan = Some(plan);
        match snapshot {
            Some(snapshot) => {
                let correlation_id = self.archive_request(
                    ports,
                    archive::Request::Replay {
                        recording_id: snapshot.recording_id,
                        position: 0,
                        length: NULL_VALUE,
                        channel: self.config.replay_channel.clone(),
                        stream_id: self.config.replay_stream_id,
                    },
                    None,
                )?;
                self.startup = Some(Startup::LoadSnapshot(SnapshotLoad::Replay { correlation_id }));
            }
            None => self.startup = Some(Startup::AwaitServices),
        }
        Ok(())
    }

    /// The module's snapshot replayed to its END: its state taken.
    fn on_snapshot_loaded(&mut self, snapshot: Snapshot, ports: &mut impl Ports) -> Outcome<()> {
        for tracker in &mut self.trackers {
            tracker.verify().map_err(|e| Fault::Terminate(e.to_string()))?;
            tracker.reset();
        }
        self.set_commit_position(snapshot.log_position, ports);
        self.set_leadership_term_id(snapshot.leadership_term_id, ports);
        self.expected_ack_position = snapshot.log_position;
        self.startup = Some(Startup::AwaitServices);
        Ok(())
    }

    fn apply_loaded(&mut self, loaded: Loaded) -> Outcome<()> {
        match loaded {
            Loaded::Begin { app_version, time_unit } => {
                if (app_version >> 16) != (self.config.app_version >> 16) {
                    return Err(Fault::Terminate(format!(
                        "incompatible version: {} snapshot={}",
                        crate::session_manager::semantic_version_string(self.config.app_version),
                        crate::session_manager::semantic_version_string(app_version)
                    )));
                }
                if time_unit != self.config.time_unit.codec() {
                    return Err(Fault::Terminate(format!(
                        "incompatible time unit: {} snapshot={time_unit:?}",
                        self.config.time_unit.name()
                    )));
                }
            }
            Loaded::ConsensusModule {
                next_session_id,
                next_service_session_id,
                log_service_session_id,
                ..
            } => {
                self.sessions.load_next_session_id(next_session_id);
                if let Some(tracker) = self.trackers.first_mut() {
                    tracker.load_state(next_service_session_id, log_service_session_id);
                }
            }
            Loaded::ClusterSession {
                cluster_session_id,
                correlation_id,
                opened_log_position,
                time_of_last_activity,
                close_reason,
                response_stream_id,
                response_channel,
            } => self.sessions.on_load_cluster_session(
                cluster_session_id,
                correlation_id,
                opened_log_position,
                time_of_last_activity,
                close_reason,
                response_stream_id,
                response_channel,
            ),
            Loaded::Timer {
                correlation_id,
                deadline,
            } => self.on_schedule_timer(correlation_id, deadline),
            Loaded::PendingMessageTracker {
                next_service_session_id,
                log_service_session_id,
                service_id,
                ..
            } => {
                let count = self.trackers.len();
                let tracker = usize::try_from(service_id)
                    .ok()
                    .and_then(|i| self.trackers.get_mut(i))
                    .ok_or_else(|| {
                        Fault::Terminate(format!("serviceId={service_id} invalid for serviceCount={count}"))
                    })?;
                tracker.load_state(next_service_session_id, log_service_session_id);
            }
            Loaded::PendingMessage {
                cluster_session_id,
                message,
            } => {
                let service_id = service_id_from_log_message(cluster_session_id) as usize;
                if let Some(tracker) = self.trackers.get_mut(service_id) {
                    tracker.append_message(&message);
                }
            }
            Loaded::End | Loaded::Ignored => {}
        }
        Ok(())
    }

    // ---------------------------------------------------------------- archive

    fn drain_archive_events(&mut self, ports: &mut impl Ports) {
        while let Some(event) = self.archive.poll_event() {
            let result = self.on_archive_event(event, ports);
            self.handle(result, ports);
        }
    }

    /// A message from the local archive.
    pub fn on_archive_message(&mut self, message: &[u8], ports: &mut impl Ports) {
        self.archive.on_response(message);
        self.drain_archive_events(ports);
        self.actions.extend(self.sessions.take_actions());
    }

    fn on_archive_event(&mut self, event: ArchiveEvent, ports: &mut impl Ports) -> Outcome<()> {
        match event {
            ArchiveEvent::Connected { .. } => {
                if matches!(self.startup, Some(Startup::ConnectArchive)) {
                    self.on_archive_connected(ports)?;
                }
            }
            ArchiveEvent::ConnectFailed { message } => return Err(Fault::Terminate(message)),
            ArchiveEvent::Disconnected => return Err(Fault::Terminate("local archive is not connected".into())),
            ArchiveEvent::TimedOut { correlation_id } => match self.requests.remove(&correlation_id) {
                Some(Request::TryStop | Request::StopReplay | Request::ReplicateAbandoned) => {}
                Some(Request::ReplicateStandby) => {
                    self.on_standby_replication_failed("archive response timeout", ports);
                }
                _ => {
                    return Err(Fault::Error(format!(
                        "archive response timeout: correlationId={correlation_id}"
                    )));
                }
            },
            ArchiveEvent::Descriptor {
                correlation_id,
                descriptor,
            } => self.on_descriptor(correlation_id, &descriptor, ports)?,
            ArchiveEvent::RecordingUnknown {
                correlation_id,
                recording_id,
            } => {
                if let Some(Startup::ListLogRecording { correlation_id: c, .. }) = self.startup
                    && c == correlation_id
                {
                    return Err(Fault::Terminate(format!("unknown recording id: {recording_id}")));
                }
                if self.requests.remove(&correlation_id) == Some(Request::PrepareRecoveryPlan) {
                    return Err(Fault::Error(format!("unknown recording id: {recording_id}")));
                }
            }
            ArchiveEvent::Ok {
                correlation_id,
                relevant_id,
            } => self.on_archive_ok(correlation_id, relevant_id, ports)?,
            ArchiveEvent::Error {
                correlation_id,
                error_code: code,
                message,
            } => {
                // A standby copy's error after its archive took it, under the replication's id: Java's
                // replicator, on an archive client of its own, catches any and tries the next archive, else
                // the member goes on with its own snapshots. A request pending under the same id comes first.
                if !self.requests.contains_key(&correlation_id) && self.is_standby_replication(correlation_id) {
                    self.on_standby_replication_failed(&message, ports);
                    return Ok(());
                }
                let request = self.requests.remove(&correlation_id);
                if matches!(request, Some(Request::TryStop | Request::ReplicateAbandoned)) {
                    return Ok(());
                }
                if let Some(Request::CatchupReplay { member_id }) = request {
                    self.on_catchup_replay_failed(member_id, correlation_id, &message);
                    return Ok(());
                }
                if request == Some(Request::ReplicateStandby) {
                    self.on_standby_replication_failed(&message, ports);
                    return Ok(());
                }
                if code == error_code::STORAGE_SPACE {
                    return Err(Fault::Terminate(message));
                }
                if code == error_code::UNKNOWN_REPLAY {
                    self.warn(format!("replay no longer relevant - {message}"));
                    return Ok(());
                }
                return Err(Fault::Error(message));
            }
            ArchiveEvent::Signal {
                correlation_id,
                recording_id,
                position,
                signal,
                ..
            } => {
                if signal == Signal::STOP && recording_id == self.log_recording_id {
                    self.log_recording_stop_position = position;
                    if self.election.is_none() && self.state == ModuleState::Active {
                        let is_eos = self.reader.as_ref().is_some_and(|r| r.end_of_stream);
                        self.enter_election(is_eos, &format!("log recording stopped: eos={is_eos}"), ports)?;
                        return Ok(());
                    }
                }
                if !self.on_standby_replication_signal(correlation_id, recording_id, position, signal, ports) {
                    self.on_replication_signal(correlation_id, recording_id, position, signal, ports)?;
                }
            }
        }
        Ok(())
    }

    fn on_descriptor(&mut self, correlation_id: i64, d: &Descriptor, ports: &mut impl Ports) -> Outcome<()> {
        let extent = RecordingExtent {
            start_position: d.start_position,
            stop_position: d.stop_position,
            initial_term_id: d.initial_term_id,
            term_buffer_length: d.term_buffer_length,
            mtu_length: d.mtu_length,
            session_id: d.session_id,
        };
        if let Some(Startup::ListLogRecording { correlation_id: c, .. }) = self.startup
            && c == correlation_id
        {
            return self.on_recovery_plan(Some(extent), ports);
        }
        if self.is_prepare_plan(correlation_id) {
            self.requests.remove(&correlation_id);
            return self.on_prepare_recovery_plan(Some(extent), ports);
        }
        if let Some(Request::PlanRefresh { .. }) = self.requests.remove(&correlation_id)
            && let Ok(plan) = self.recording_log.create_recovery_plan(
                self.config.service_count as i32,
                self.log_recording_id,
                Some(extent),
            )
        {
            self.recovery_plan = Some(plan);
        }
        Ok(())
    }

    fn on_archive_ok(&mut self, correlation_id: i64, relevant_id: i64, ports: &mut impl Ports) -> Outcome<()> {
        if let Some(Startup::LoadSnapshot(SnapshotLoad::Replay { correlation_id: c })) = self.startup
            && c == correlation_id
        {
            let session_id = relevant_id as i32;
            let channel = add_session_id(&self.config.replay_channel, session_id);
            let subscription = self.new_subscription(channel, self.config.replay_stream_id);
            self.reader = Some(ImageReader::new(subscription, session_id));
            self.startup = Some(Startup::LoadSnapshot(SnapshotLoad::Read {
                reader: ImageReader::new(subscription, session_id),
                loader: Loader::default(),
            }));
            return Ok(());
        }
        if let Some(Election {
            log_replay: Some(replay),
            ..
        }) = self.election.as_mut()
            && let ReplayStep::Replay { correlation_id: c } = replay.step
            && c == correlation_id
        {
            replay.replay_session_id = relevant_id;
            let session_id = relevant_id as i32;
            let channel = add_session_id(&self.config.replay_channel, session_id);
            let subscription = SubscriptionId(self.next_subscription);
            self.next_subscription += 1;
            self.actions.push_back(Action::AddSubscription {
                subscription,
                channel,
                stream_id: self.config.replay_stream_id,
            });
            replay.step = ReplayStep::AwaitImage {
                reader: ImageReader::new(subscription, session_id),
            };
            return Ok(());
        }
        match self.requests.remove(&correlation_id) {
            Some(Request::LogRecording) => self.log_subscription_id = relevant_id,
            Some(Request::PrepareStopPosition) => self.on_prepare_stop_position(correlation_id, relevant_id, ports)?,
            Some(Request::Replicate) => self.on_replication_started(correlation_id, relevant_id),
            Some(Request::ReplicateStandby) => self.on_standby_replication_started(correlation_id, relevant_id),
            Some(Request::ReplicateAbandoned) => {
                let _ = self.archive_request(
                    ports,
                    archive::Request::StopReplication {
                        replication_id: relevant_id,
                    },
                    Some(Request::TryStop),
                );
            }
            Some(Request::CatchupReplay { member_id }) => {
                self.on_catchup_replay_started(member_id, correlation_id, relevant_id, ports)
            }
            Some(
                Request::SnapshotRecording
                | Request::TryStop
                | Request::StopReplay
                | Request::PrepareRecoveryPlan
                | Request::PlanRefresh { .. },
            )
            | None => {}
        }
        Ok(())
    }

    // ---------------------------------------------------------------- images

    /// A fragment of an image the module reads (a replay); `PollAction` as
    /// Java's adapters answer.
    pub fn on_image_fragment(
        &mut self,
        subscription: SubscriptionId,
        fragment: Fragment<'_>,
        ports: &mut impl Ports,
    ) -> PollAction {
        let result = self.image_fragment(subscription, fragment, ports);
        let action = match result {
            Ok(action) => action,
            Err(fault) => {
                self.handle(Err(fault), ports);
                PollAction::Break
            }
        };
        self.actions.extend(self.sessions.take_actions());
        action
    }

    /// Where a poll left the log's image: Java's `LogAdapter.position` is the
    /// image's. Past a term's padding frame, which a poll steps over with no
    /// fragment, so a replay stopping at a term's end reaches its stop, and
    /// the commit position, the services' bound, follows it there.
    pub fn on_image_position(&mut self, subscription: SubscriptionId, position: i64) {
        if let Some(reader) = self.reader.as_mut()
            && reader.subscription == subscription
            && reader.position.is_some_and(|p| p < position)
        {
            reader.position = Some(position);
        }
    }

    fn image_fragment(
        &mut self,
        subscription: SubscriptionId,
        fragment: Fragment<'_>,
        ports: &mut impl Ports,
    ) -> Outcome<PollAction> {
        if let Some(Startup::LoadSnapshot(SnapshotLoad::Read { .. })) = self.startup {
            let Some(Startup::LoadSnapshot(SnapshotLoad::Read { reader, loader })) = self.startup.as_mut() else {
                unreachable!()
            };
            if reader.subscription != subscription {
                return Ok(PollAction::Continue);
            }
            let mut loaded = Vec::new();
            let mut failure = None;
            let action = reader
                .reassembler
                .on_fragment(fragment, |message, _| match loader.on_message(message) {
                    Ok(l) => {
                        loaded.push(l);
                        if loader.is_done() {
                            PollAction::Break
                        } else {
                            PollAction::Continue
                        }
                    }
                    Err(e) => {
                        failure = Some(e);
                        PollAction::Break
                    }
                });
            reader.position = Some(fragment.position);
            let done = loader.is_done();
            if let Some(e) = failure {
                return Err(Fault::Terminate(e));
            }
            for l in loaded {
                self.apply_loaded(l)?;
            }
            if done {
                self.actions.push_back(Action::RemoveSubscription { subscription });
                self.reader = None;
                let snapshot = self
                    .recovery_plan
                    .as_ref()
                    .and_then(|p| p.snapshots.first().copied())
                    .expect("a snapshot loading");
                self.on_snapshot_loaded(snapshot, ports)?;
            }
            return Ok(action);
        }
        let Some(reader) = self.reader.as_mut() else {
            return Ok(PollAction::Continue);
        };
        if reader.subscription != subscription {
            return Ok(PollAction::Continue);
        }
        let mut messages = Vec::new();
        let action = reader.reassembler.on_fragment(fragment, |message, position| {
            messages.push((message.to_vec(), position));
            // A cluster action is acted on before the next message: Java breaks the poll there.
            match log_publisher::decode(message) {
                Ok(LogMessage::ClusterAction { .. }) => PollAction::Break,
                _ => PollAction::Continue,
            }
        });
        reader.position = Some(fragment.position);
        if let Some(&(_, position)) = messages.last() {
            reader.message_position = Some(position);
        }
        for (message, position) in messages {
            self.on_log_message(&message, position, ports)?;
        }
        // Java proposes the commit position right after the poll, whatever
        // state a cluster action in it moved the module to: the services
        // then reach the action.
        self.propose_commit_position(fragment.position, ports);
        Ok(action)
    }

    /// A message of the log, replayed or followed (Java's `LogAdapter`).
    fn on_log_message(&mut self, message: &[u8], position: i64, ports: &mut impl Ports) -> Outcome<()> {
        match log_publisher::decode(message)? {
            LogMessage::SessionMessage {
                cluster_session_id,
                timestamp,
            } => {
                if let Some(session) = self.sessions.find_by_session_id(cluster_session_id) {
                    session.time_of_last_activity_ns = self.config.time_unit.to_nanos(timestamp);
                } else if cluster_session_id < 0 {
                    let service_id = service_id_from_log_message(cluster_session_id) as usize;
                    if let Some(tracker) = self.trackers.get_mut(service_id) {
                        tracker.sweep_follower_messages(cluster_session_id);
                    }
                }
            }
            LogMessage::TimerEvent { correlation_id } => {
                if !self.timers.cancel(correlation_id) {
                    *self.expired_timer_counts.entry(correlation_id).or_insert(0) += 1;
                }
            }
            LogMessage::SessionOpen {
                correlation_id,
                cluster_session_id,
                timestamp,
                response_stream_id,
                response_channel,
            } => self.sessions.on_replay_session_open(
                position,
                correlation_id,
                cluster_session_id,
                self.config.time_unit.to_nanos(timestamp),
                response_stream_id,
                response_channel,
            ),
            LogMessage::SessionClose {
                cluster_session_id,
                close_reason,
            } => self.sessions.on_replay_session_close(cluster_session_id, close_reason),
            LogMessage::ClusterAction {
                leadership_term_id,
                log_position,
                timestamp,
                action,
                flags,
            } => {
                if leadership_term_id == self.leadership_term_id {
                    match action {
                        ClusterAction::SUSPEND => self.set_state(ModuleState::Suspended, ports),
                        ClusterAction::RESUME => self.set_state(ModuleState::Active, ports),
                        ClusterAction::SNAPSHOT if flags == log_publisher::CLUSTER_ACTION_FLAGS_DEFAULT => {
                            self.set_state(ModuleState::Snapshot, ports);
                            if self.config.service_count == 0 {
                                self.snapshot_on_service_ack(log_position, timestamp, Vec::new());
                            }
                        }
                        _ => {}
                    }
                }
            }
            LogMessage::NewLeadershipTerm {
                leadership_term_id,
                log_position,
                timestamp,
                term_base_log_position,
                time_unit,
                app_version,
            } => {
                if time_unit != self.config.time_unit.codec() {
                    return Err(Fault::Terminate(format!(
                        "incompatible timestamp units: {} log={time_unit:?}",
                        self.config.time_unit.name()
                    )));
                }
                if (app_version >> 16) != (self.config.app_version >> 16) {
                    return Err(Fault::Terminate(format!(
                        "incompatible version: {} log={}",
                        crate::session_manager::semantic_version_string(self.config.app_version),
                        crate::session_manager::semantic_version_string(app_version)
                    )));
                }
                self.set_leadership_term_id(leadership_term_id, ports);
                if self.election.is_some() {
                    self.election_on_replay_new_leadership_term_event(
                        leadership_term_id,
                        log_position,
                        timestamp,
                        term_base_log_position,
                    )?;
                }
            }
            LogMessage::Extension => {
                return Err(Fault::Error("expected schemaId=111, actual=other".into()));
            }
            LogMessage::Ignored => {}
        }
        Ok(())
    }

    /// What the shell reports.
    pub fn on_io(&mut self, io: Io, ports: &mut impl Ports) {
        let result = self.io(io, ports);
        self.handle(result, ports);
        self.actions.extend(self.sessions.take_actions());
    }

    fn io(&mut self, io: Io, ports: &mut impl Ports) -> Outcome<()> {
        match io {
            Io::EgressAdded { egress, result } => self.sessions.on_egress_added(egress, &result, self.now_ns),
            Io::LogPublicationAdded { session_id } => {
                self.log_session_id = Some(session_id);
                if let Some(election) = self.election.as_mut() {
                    election.log_session_id = Some(session_id);
                }
            }
            Io::SnapshotPublicationAdded { session_id } => {
                if let Some(take) = self.take_snapshot.as_mut()
                    && matches!(take.step, SnapshotStep::AwaitPublication)
                {
                    let channel = add_session_id(&self.config.snapshot_channel, session_id);
                    take.step = SnapshotStep::StartRecording;
                    let request = archive::Request::StartRecording {
                        channel,
                        stream_id: self.config.snapshot_stream_id,
                        local: true,
                        auto_stop: true,
                    };
                    self.archive_request(ports, request, Some(Request::SnapshotRecording))?;
                    if let Some(take) = self.take_snapshot.as_mut() {
                        take.step = SnapshotStep::AwaitCounter { session_id };
                    }
                }
            }
            Io::ImageAvailable {
                subscription,
                session_id,
                correlation_id: _,
                join_position,
                term_buffer_length,
                source_identity,
            } => {
                if Some(subscription) == self.consensus {
                    // Backups' and standbys' sessions name the image they came on.
                    self.consensus_sources.insert(
                        session_id,
                        format!("sourceIdentity={source_identity} sessionId={session_id}"),
                    );
                }
                if Some(subscription) == self.log_subscription {
                    self.log_subscription_images.retain(|i| i.session_id != session_id);
                    self.log_subscription_images.push(LogImage {
                        session_id,
                        join_position,
                        term_buffer_length,
                    });
                } else {
                    self.on_image_available(subscription, session_id, join_position, term_buffer_length)?;
                }
            }
            Io::ImageUnavailable {
                subscription,
                session_id,
                correlation_id,
                end_of_stream,
            } => {
                if Some(subscription) == self.ingress {
                    self.ingress_reassemblers.remove(&session_id);
                    if self.role == Role::Leader && self.state == ModuleState::Active {
                        self.sessions.timeout_on_unavailable_image(correlation_id);
                    }
                }
                if Some(subscription) == self.consensus {
                    self.consensus_reassemblers.remove(&session_id);
                    self.consensus_sources.remove(&session_id);
                }
                if Some(subscription) == self.log_subscription {
                    self.log_subscription_images.retain(|i| i.session_id != session_id);
                }
                if let Some(reader) = self.reader.as_mut()
                    && reader.subscription == subscription
                    && reader.session_id == session_id
                {
                    reader.closed = true;
                    reader.end_of_stream = end_of_stream;
                }
            }
            Io::CounterUnavailable { registration_id } => {
                if self.state != ModuleState::Terminating && self.state != ModuleState::Quitting {
                    if let Some(service_id) = self.service_client_ids.iter().position(|&id| id == registration_id) {
                        let message = format!("Aeron client in service closed unexpectedly: serviceId={service_id}");
                        self.warn(message);
                        self.set_state(ModuleState::Closed, ports);
                        return Ok(());
                    }
                    if self
                        .append_counter
                        .is_some_and(|c| c.registration_id == registration_id)
                    {
                        self.append_counter = None;
                        self.log_subscription_id = NULL_VALUE;
                    }
                }
            }
        }
        Ok(())
    }

    fn on_image_available(
        &mut self,
        subscription: SubscriptionId,
        session_id: i32,
        join_position: i64,
        term_buffer_length: i32,
    ) -> Outcome<()> {
        if let Some(Startup::LoadSnapshot(SnapshotLoad::Read { reader, .. })) = self.startup.as_mut()
            && reader.subscription == subscription
            && reader.session_id == session_id
        {
            reader.position = Some(join_position);
            if let Some(r) = self.reader.as_mut() {
                r.position = Some(join_position);
                r.message_position = Some(join_position);
            }
            return Ok(());
        }
        if let Some(Election {
            log_replay: Some(replay),
            ..
        }) = self.election.as_mut()
            && let ReplayStep::AwaitImage { reader } = &replay.step
            && reader.subscription == subscription
            && reader.session_id == session_id
        {
            if join_position != replay.start {
                return Err(Fault::Error(format!(
                    "joinPosition={join_position} expected startPosition={}",
                    replay.start
                )));
            }
            let ReplayStep::AwaitImage { mut reader } = std::mem::replace(
                &mut replay.step,
                ReplayStep::Replay {
                    correlation_id: NULL_VALUE,
                },
            ) else {
                unreachable!()
            };
            reader.position = Some(join_position);
            reader.message_position = Some(join_position);
            reader.term_buffer_length = term_buffer_length;
            replay.step = ReplayStep::AwaitServices { reader };
        }
        Ok(())
    }

    // ---------------------------------------------------------------- services

    /// A message from a service (Java's `ConsensusModuleAdapter`).
    pub fn on_service_message(&mut self, message: &[u8], ports: &mut impl Ports) {
        let result = match service_proxy::decode(message) {
            Ok(m) => self.service_message(m, ports),
            Err(e) => Err(Fault::Error(e)),
        };
        self.handle(result, ports);
        self.actions.extend(self.sessions.take_actions());
    }

    fn service_message(&mut self, message: ServiceMessage, ports: &mut impl Ports) -> Outcome<()> {
        match message {
            ServiceMessage::Message {
                service_id,
                cluster_session_id,
                message,
            } => match usize::try_from(service_id).ok().and_then(|i| self.trackers.get_mut(i)) {
                Some(tracker) => tracker.enqueue(&message),
                // Reported and dropped, as Java 1.54 does.
                None => self.warn(format!(
                    "service message dropped, unknown serviceId={service_id} clusterSessionId={cluster_session_id} \
                     serviceCount={}",
                    self.trackers.len()
                )),
            },
            ServiceMessage::CloseSession { cluster_session_id } => {
                let is_active_leader = self.role == Role::Leader && self.state == ModuleState::Active;
                let term = self.term();
                self.sessions
                    .on_service_close_session(cluster_session_id, is_active_leader, &term, ports)?;
            }
            ServiceMessage::ScheduleTimer {
                correlation_id,
                deadline,
            } => self.on_schedule_timer(correlation_id, deadline),
            ServiceMessage::CancelTimer { correlation_id } => {
                self.timers.cancel(correlation_id);
            }
            ServiceMessage::Ack {
                log_position,
                timestamp,
                ack_id,
                relevant_id,
                service_id,
            } => self.on_service_ack(log_position, timestamp, ack_id, relevant_id, service_id, ports)?,
            ServiceMessage::ClusterMembersQuery {
                correlation_id,
                extended,
            } => {
                let leader = self.members[self.leader].id;
                let response = if extended {
                    service_proxy::cluster_members_extended_response(
                        correlation_id,
                        self.now_ns,
                        leader,
                        self.config.member_id,
                        &self.members,
                    )
                } else {
                    service_proxy::cluster_members_response(
                        correlation_id,
                        leader,
                        &cluster_member::encode_as_string(&self.members),
                    )
                };
                if !self.offer_service(ports, &response) {
                    self.warn("failed to send cluster members response".into());
                }
            }
            ServiceMessage::Ignored => {}
        }
        Ok(())
    }

    fn on_schedule_timer(&mut self, correlation_id: i64, deadline: i64) {
        match self.expired_timer_counts.get_mut(&correlation_id) {
            Some(count) if *count > 0 => {
                *count -= 1;
                if *count == 0 {
                    self.expired_timer_counts.remove(&correlation_id);
                }
            }
            _ => self.timers.schedule(correlation_id, deadline),
        }
    }

    fn on_service_ack(
        &mut self,
        log_position: i64,
        timestamp: i64,
        ack_id: i64,
        relevant_id: i64,
        service_id: i32,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        let service_count = self.service_acks.service_count();
        let Some(index) = usize::try_from(service_id).ok().filter(|&i| i < service_count) else {
            // Reported and dropped, as Java 1.54 does: no election started over for it.
            self.warn(format!(
                "service ack dropped, unknown serviceId={service_id} ackId={ack_id} serviceCount={service_count}"
            ));
            return Ok(());
        };
        if ack_id == 0 && self.service_client_ids.get(index).is_some_and(|&id| id != NULL_VALUE) {
            return Err(Fault::Error(format!(
                "initial ack already received from service: possible duplicate serviceId={service_id}"
            )));
        }
        self.service_acks.push(
            index,
            ServiceAck {
                ack_id,
                log_position,
                relevant_id,
            },
        )?;
        // A snapshot's acks are taken even while services are being joined:
        // left in the queue, they would come before the join's.
        if self.startup.is_some() || (self.awaiting_services() && self.state != ModuleState::Snapshot) {
            return Ok(());
        }
        if matches!(self.state, ModuleState::Snapshot | ModuleState::Terminating)
            && let Some(ack_id) = self.service_acks.drop_before(log_position)
        {
            // Acks of a join an election gave up on dropped, as in
            // `services_joined`.
            self.service_ack_id = ack_id;
        }
        if self.service_acks.has_reached(log_position, self.service_ack_id)? {
            match self.state {
                ModuleState::Snapshot => {
                    self.service_ack_id += 1;
                    let acks = self.service_acks.poll(log_position)?;
                    self.snapshot_on_service_ack(log_position, timestamp, acks);
                }
                ModuleState::Terminating => {
                    self.service_ack_id += 1;
                    self.service_acks.poll(log_position)?;
                    if let Some(termination) = self.termination.as_mut() {
                        termination.is_awaiting_services = false;
                    }
                    self.do_termination(log_position, ports);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Whether a join of the log waits for the services' acks.
    fn awaiting_services(&self) -> bool {
        matches!(
            &self.election,
            Some(Election {
                join_log: Some(JoinLog::AwaitServices),
                ..
            }) | Some(Election {
                log_replay: Some(LogReplay {
                    step: ReplayStep::AwaitServices { .. },
                    ..
                }),
                ..
            })
        )
    }

    /// Java's `awaitServicesReady`: the services told to join a log, then
    /// their acks awaited.
    #[allow(clippy::too_many_arguments)]
    fn join_services(
        &mut self,
        ports: &mut impl Ports,
        channel: &str,
        stream_id: i32,
        log_session_id: i32,
        log_position: i64,
        max_log_position: i64,
        is_startup: bool,
        role: Role,
    ) -> Outcome<()> {
        if self.config.service_count == 0 {
            return Ok(());
        }
        let message = service_proxy::join_log(
            log_position,
            max_log_position,
            self.config.member_id,
            log_session_id,
            stream_id,
            is_startup,
            role as i32,
            channel,
        );
        if !self.offer_service(ports, &message) {
            return Err(Fault::Error("failed to send join log request".into()));
        }
        self.expected_ack_position = log_position;
        Ok(())
    }

    /// Whether the services acked the join of a log: their acks taken.
    fn services_joined(&mut self, log_position: i64) -> Outcome<bool> {
        if self.config.service_count == 0 {
            return Ok(true);
        }
        if self.state != ModuleState::Snapshot {
            // Services ack a join an election gave up on all the same, at the
            // position that join began: those acks dropped, and the count taken
            // from this join's. Java takes acks strictly in sequence, so one
            // left over puts every later one out of sequence for good.
            match self.service_acks.drop_before(log_position) {
                Some(ack_id) => self.service_ack_id = ack_id,
                None => return Ok(false),
            }
        }
        if !self.service_acks.has_reached(log_position, self.service_ack_id)? {
            return Ok(false);
        }
        self.service_acks.poll(log_position)?;
        self.service_ack_id += 1;
        Ok(true)
    }

    // ---------------------------------------------------------------- clients

    /// A fragment on the ingress (Java's `IngressAdapter`, reassembled per image).
    pub fn on_ingress_fragment(
        &mut self,
        fragment: Fragment<'_>,
        image: &IngressImage,
        ports: &mut impl Ports,
    ) -> PollAction {
        let mut reassembler = self.ingress_reassemblers.remove(&image.session_id).unwrap_or_default();
        let mut result = Ok(());
        let action = reassembler.on_fragment(fragment, |message, _| {
            match self.ingress_message(message, image, ports) {
                Ok(action) => action,
                Err(fault) => {
                    result = Err(fault);
                    PollAction::Continue
                }
            }
        });
        self.ingress_reassemblers.insert(image.session_id, reassembler);
        self.handle(result, ports);
        self.actions.extend(self.sessions.take_actions());
        action
    }

    fn ingress_message(&mut self, message: &[u8], image: &IngressImage, ports: &mut impl Ports) -> Outcome<PollAction> {
        let is_leader = self.role == Role::Leader;
        match ingress_adapter::decode(message)? {
            IngressMessage::Message {
                leadership_term_id,
                cluster_session_id,
            } => {
                if leadership_term_id == self.leadership_term_id && is_leader {
                    let timestamp = self.timestamp();
                    let now_ns = self.config.time_unit.to_nanos(timestamp);
                    let open = self
                        .sessions
                        .find_by_session_id(cluster_session_id)
                        .is_some_and(|s| s.is_open());
                    if open {
                        let header =
                            log_publisher::session_message_header(leadership_term_id, cluster_session_id, timestamp);
                        match log_publisher::append(
                            ports,
                            &[&header, &message[log_publisher::SESSION_HEADER_LENGTH..]],
                        )? {
                            Some(_) => {
                                if let Some(session) = self.sessions.find_by_session_id(cluster_session_id) {
                                    session.time_of_last_activity_ns = now_ns;
                                }
                            }
                            None => return Ok(PollAction::Abort),
                        }
                    }
                }
            }
            IngressMessage::Connect {
                correlation_id,
                response_stream_id,
                version,
                response_channel,
                client_info,
            } => self.sessions.on_session_connect(
                correlation_id,
                response_stream_id,
                version,
                response_channel,
                &client_info,
                image,
                is_leader,
                &self.ingress_endpoints,
                self.now_ns,
            ),
            IngressMessage::Close {
                leadership_term_id,
                cluster_session_id,
            } => {
                if leadership_term_id == self.leadership_term_id && is_leader {
                    let term = self.term();
                    self.sessions.on_session_close(cluster_session_id, &term, ports)?;
                }
            }
            IngressMessage::KeepAlive {
                leadership_term_id,
                cluster_session_id,
            } => {
                if leadership_term_id == self.leadership_term_id && is_leader {
                    self.sessions
                        .on_session_keep_alive(cluster_session_id, image.correlation_id, self.now_ns);
                }
            }
            IngressMessage::Admin {
                leadership_term_id,
                cluster_session_id,
                correlation_id,
                request_type,
            } => {
                if is_leader && leadership_term_id == self.leadership_term_id {
                    self.sessions
                        .on_admin_request(cluster_session_id, correlation_id, request_type, ports);
                }
            }
            IngressMessage::Ignored => {}
        }
        Ok(PollAction::Continue)
    }

    // ---------------------------------------------------------------- the leader's work

    fn slow_tick_work(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let mut work = 0;
        if self.state == ModuleState::Closed {
            // Java's `unexpectedTermination`, no error recorded: what closed
            // the module recorded why.
            self.unexpected_termination("State.CLOSED == state", ports);
            return Ok(work);
        }
        let term = self.term();
        work += self.sessions.send_redirects(&term, ports);
        work += self.sessions.send_rejections(&term, ports);
        if self.election.is_none() && self.role == Role::Leader {
            work += self.check_cluster_control_toggle(ports)?;
            if self.state == ModuleState::Active {
                let term = self.term();
                work += self.sessions.process_pending_sessions(&term, ports)?;
                work += self.process_backup_sessions(ports);
                work += self
                    .sessions
                    .check_sessions(&term, &self.ingress_endpoints.clone(), ports)?;
                ports.set_counter(CounterId::TimedOutClientCount, self.sessions.timed_out_count);
                if self.members.len() > 1
                    && !cluster_member::has_active_quorum(
                        &self.members,
                        self.now_ns,
                        self.config.leader_heartbeat_timeout_ns,
                    )
                {
                    self.enter_election(false, "inactive follower quorum", ports)?;
                    work += 1;
                }
            } else if self.state == ModuleState::Terminating {
                let now_ns = self.now_ns;
                if self.termination.as_ref().is_some_and(|t| self.can_terminate(t, now_ns)) {
                    let position = self.termination_position;
                    self.commit_log_position(self.leadership_term_id, position)?;
                    self.close_and_terminate(ports);
                }
            }
        } else if self.election.is_none() {
            if self.role == Role::Follower && self.state == ModuleState::Active {
                work += self.process_backup_sessions(ports);
            }
            if (self.state == ModuleState::Active || self.state == ModuleState::Suspended)
                && self.now_ns >= self.time_of_last_log_update_ns + self.config.leader_heartbeat_timeout_ns
                && self.termination_position == NULL_POSITION
            {
                self.enter_election(false, "leader heartbeat timeout", ports)?;
                work += 1;
            }
        }
        if self.election.is_none() && self.state == ModuleState::Active {
            work += self.check_node_control_toggle(ports);
        }
        work += self.process_standby_snapshot_notifications(ports);
        Ok(work)
    }

    fn consensus_work(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let mut work = 0;
        if self.standby_replication.is_some() {
            let (copied, done) = self.standby_replication_work(ports);
            work += copied;
            if done {
                self.refresh_recovery_plan(ports)?;
            }
        }
        if self.role == Role::Leader {
            if self.state == ModuleState::Active {
                work += self.poll_timers(ports)?;
                let term_id = self.leadership_term_id;
                let timestamp = self.timestamp();
                for i in 0..self.trackers.len() {
                    let mut failure = None;
                    let tracker = &mut self.trackers[i];
                    work += tracker.poll(|cluster_session_id, payload| {
                        let header = log_publisher::session_message_header(term_id, cluster_session_id, timestamp);
                        match log_publisher::append(ports, &[&header, payload]) {
                            Ok(Some(position)) => position,
                            Ok(None) => -1,
                            Err(e) => {
                                failure = Some(e);
                                -1
                            }
                        }
                    });
                    if let Some(e) = failure {
                        return Err(Fault::Error(e));
                    }
                }
            } else if self.state == ModuleState::Snapshot
                && self.config.service_count == 0
                && ports.log_position() <= self.commit_position
                && self.take_snapshot.is_none()
            {
                // As soon as its action is committed, not at the next slow tick (Java 1.54).
                let position = self.commit_position;
                self.snapshot_on_service_ack(position, self.timestamp(), Vec::new());
                work += 1;
            }
            work += self.update_leader_position(ports)?;
        } else {
            work += self.follower_work(ports)?;
        }
        Ok(work)
    }

    fn poll_timers(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let term_id = self.leadership_term_id;
        let timestamp = self.timestamp();
        let mut appended = Vec::new();
        let mut failure = None;
        let fired = self.timers.poll(timestamp, |correlation_id| {
            let message = log_publisher::timer_event(term_id, correlation_id, timestamp);
            match log_publisher::append(ports, &[&message]) {
                Ok(Some(position)) => {
                    appended.push((position, correlation_id));
                    true
                }
                Ok(None) => false,
                Err(e) => {
                    failure = Some(e);
                    false
                }
            }
        });
        self.uncommitted_timers.extend(appended);
        if let Some(e) = failure {
            return Err(Fault::Error(e));
        }
        Ok(fired)
    }

    /// Java's `updateLeaderPosition`: the quorum's position, bounded by the
    /// leader's own recording, becomes the commit position.
    fn update_leader_position(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let Some(counter) = self.append_counter else {
            return Ok(0);
        };
        let append_position = ports.counter_value(counter.counter_id);
        let quorum = self.quorum_position_bounded_by_leader_log(append_position);
        self.update_leader_position_to(append_position, quorum, ports)
    }

    /// Raft's commit rule: a majority's position in this leader's log, each
    /// member counting only as far as its log is known to be the leader's.
    fn quorum_position_bounded_by_leader_log(&self, append_position: i64) -> i64 {
        let timeout_ns = self.config.leader_heartbeat_timeout_ns;
        cluster_member::quorum_position(&self.members, self.now_ns, timeout_ns, |i, member| {
            if i == self.this {
                member.log_position
            } else {
                self.matched_position(member)
            }
        })
        .min(append_position)
    }

    /// The terms of this member's log that hold entries, in order. Java's
    /// recording log also has a term it skipped, filled in as an empty one
    /// where the next began: none of that term's entries is in this log.
    fn terms_with_entries(&self) -> impl Iterator<Item = &crate::recording_log::Entry> {
        self.recording_log.entries().iter().filter(|e| {
            e.is_valid
                && e.entry_type == crate::recording_log::ENTRY_TYPE_TERM
                && (e.log_position == NULL_POSITION || e.log_position > e.term_base_log_position)
        })
    }

    /// Raft's log matching, to answer a member whose log ends in
    /// `log_leadership_term_id`: the latest term at or before it of which this
    /// member's log holds entries, and the next one after that. Java names
    /// the member's term whatever its own log holds, and looks for the next
    /// by number: a member whose last term this one never had then keeps that
    /// term's entries, and goes on from them as if they were the leader's. An
    /// empty term begins where the next one with entries does, so naming
    /// that one instead moves the member as far.
    fn matching_term(&self, log_leadership_term_id: i64) -> (i64, Option<crate::recording_log::Entry>) {
        let matching = self
            .terms_with_entries()
            .filter(|e| e.leadership_term_id <= log_leadership_term_id)
            .map(|e| e.leadership_term_id)
            .last()
            .unwrap_or(NULL_VALUE);
        let next = self
            .terms_with_entries()
            .find(|e| e.leadership_term_id > matching)
            .cloned();
        (matching, next)
    }

    /// How much of this leader's log `member` is known to hold: all of what
    /// it reported in the leader's term. Of a log ending in an older term the
    /// leader holds entries of, as far as the leader's log stays in that term,
    /// since a term's entries come from its one leader. Of any other, nothing.
    /// Java counts any position, so a canvassing member's log gone another
    /// way can commit entries it does not hold.
    fn matched_position(&self, member: &ClusterMember) -> i64 {
        let term = self
            .election
            .as_ref()
            .map_or(self.leadership_term_id, |e| e.leadership_term_id);
        if member.leadership_term_id == term {
            return member.log_position;
        }
        if !self
            .terms_with_entries()
            .any(|e| e.leadership_term_id == member.leadership_term_id)
        {
            return 0;
        }
        let end = self
            .recording_log
            .entries()
            .iter()
            .find(|e| {
                e.is_valid
                    && e.entry_type == crate::recording_log::ENTRY_TYPE_TERM
                    && e.leadership_term_id > member.leadership_term_id
            })
            .map_or(i64::MAX, |e| e.term_base_log_position);
        member.log_position.min(end)
    }

    fn update_leader_position_to(
        &mut self,
        append_position: i64,
        quorum: i64,
        ports: &mut impl Ports,
    ) -> Outcome<usize> {
        let now_ns = self.now_ns;
        let this = &mut self.members[self.this];
        this.log_position = append_position;
        this.time_of_last_append_position_ns = now_ns;
        let commit = self.commit_position;
        if quorum > commit || now_ns >= self.time_of_last_log_update_ns + self.config.leader_heartbeat_interval_ns {
            if quorum < commit && commit > self.last_quorum_backtrack_commit_position {
                self.last_quorum_backtrack_commit_position = commit;
                self.warn(format!(
                    "quorum position went backwards: leaderCommitPosition={commit} quorumPosition={quorum}"
                ));
            }
            self.publish_commit_position(quorum, self.leadership_term_id, ports)?;
            self.propose_commit_position(quorum, ports);
            self.time_of_last_log_update_ns = now_ns;
            self.sweep_uncommitted_entries_to(quorum);
            return Ok(1);
        }
        Ok(0)
    }

    fn sweep_uncommitted_entries_to(&mut self, commit_position: i64) {
        for tracker in &mut self.trackers {
            tracker.sweep_leader_messages(commit_position);
        }
        while self
            .uncommitted_timers
            .front()
            .is_some_and(|&(p, _)| p <= commit_position)
        {
            self.uncommitted_timers.pop_front();
        }
        self.sessions.sweep_uncommitted_sessions(commit_position);
        while self
            .uncommitted_previous_state
            .front()
            .is_some_and(|&(p, _)| p <= commit_position)
        {
            self.uncommitted_previous_state.pop_front();
        }
    }

    fn append_action(&mut self, action: ClusterAction, flags: i32, ports: &mut impl Ports) -> Outcome<bool> {
        let log_position = log_publisher::position_after_in_term(
            ports.log_position(),
            ports.log_term_length(),
            log_publisher::CLUSTER_ACTION_LENGTH,
        );
        let message = log_publisher::cluster_action(
            self.leadership_term_id,
            self.term_base_log_position(),
            log_position,
            self.timestamp(),
            action,
            flags,
        );
        Ok(log_publisher::append(ports, &[&message])?.is_some())
    }

    /// Where this term began: its entry in the recording log, else the
    /// recovery plan's last term's, as Java's `termBaseLogPosition()` has it
    /// for a cluster action's and a snapshot's metadata (1.54.1).
    fn term_base_log_position(&self) -> i64 {
        self.recording_log
            .find_term_entry(self.leadership_term_id)
            .map(|e| e.term_base_log_position)
            .or_else(|| self.recovery_plan.as_ref().map(|p| p.last_term_base_log_position))
            .unwrap_or(NULL_VALUE)
    }

    fn check_cluster_control_toggle(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        match self.state {
            ModuleState::Active => {
                match ports.control_toggle() {
                    toggle::SUSPEND => {
                        if self.append_action(
                            ClusterAction::SUSPEND,
                            log_publisher::CLUSTER_ACTION_FLAGS_DEFAULT,
                            ports,
                        )? {
                            self.uncommitted_previous_state
                                .push_back((ports.log_position(), self.state));
                            self.set_state(ModuleState::Suspended, ports);
                        }
                    }
                    toggle::SNAPSHOT => {
                        if self.append_action(
                            ClusterAction::SNAPSHOT,
                            log_publisher::CLUSTER_ACTION_FLAGS_DEFAULT,
                            ports,
                        )? {
                            self.uncommitted_previous_state
                                .push_back((ports.log_position(), self.state));
                            self.set_state(ModuleState::Snapshot, ports);
                        }
                    }
                    toggle::STANDBY_SNAPSHOT => {
                        if self.append_action(
                            ClusterAction::SNAPSHOT,
                            log_publisher::CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT,
                            ports,
                        )? {
                            ports.set_control_toggle(toggle::NEUTRAL);
                        }
                    }
                    toggle::SHUTDOWN => {
                        if self.append_action(
                            ClusterAction::SNAPSHOT,
                            log_publisher::CLUSTER_ACTION_FLAGS_DEFAULT,
                            ports,
                        )? {
                            let position = ports.log_position();
                            self.start_termination(position, ports)?;
                            self.uncommitted_previous_state.push_back((position, self.state));
                            self.set_state(ModuleState::Snapshot, ports);
                        }
                    }
                    toggle::ABORT => {
                        let position = ports.log_position();
                        self.start_termination(position, ports)?;
                        if self.config.service_count > 0 {
                            self.send_termination_position(position, true, ports);
                        } else if let Some(t) = self.termination.as_mut() {
                            t.is_awaiting_services = false;
                        }
                        self.set_state(ModuleState::Terminating, ports);
                    }
                    _ => return Ok(0),
                }
                Ok(1)
            }
            // A leader with no service snapshots in its consensus work (Java 1.54).
            ModuleState::Snapshot => Ok(0),
            ModuleState::Suspended => {
                if ports.control_toggle() == toggle::RESUME {
                    if self.append_action(
                        ClusterAction::RESUME,
                        log_publisher::CLUSTER_ACTION_FLAGS_DEFAULT,
                        ports,
                    )? {
                        self.uncommitted_previous_state
                            .push_back((ports.log_position(), self.state));
                        self.set_state(ModuleState::Active, ports);
                        ports.set_control_toggle(toggle::NEUTRAL);
                    }
                    return Ok(1);
                }
                Ok(0)
            }
            _ => Ok(0),
        }
    }

    // ---------------------------------------------------------------- snapshots

    /// Java's `snapshotOnServiceAck`: the module's snapshot taken once
    /// every service named its own.
    fn snapshot_on_service_ack(&mut self, log_position: i64, timestamp: i64, acks: Vec<ServiceAck>) {
        let mut complete = true;
        for (service_id, ack) in acks.iter().enumerate().rev() {
            if ack.relevant_id == NULL_VALUE {
                self.warn(format!("service={service_id} failed to take snapshot"));
                complete = false;
            }
        }
        if complete {
            self.take_snapshot = Some(TakeSnapshot {
                log_position,
                timestamp,
                acks,
                step: SnapshotStep::AddPublication,
            });
        } else {
            self.take_snapshot = Some(TakeSnapshot {
                log_position,
                timestamp,
                acks,
                step: SnapshotStep::Incomplete,
            });
        }
    }

    fn snapshot_work(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let Some(take) = self.take_snapshot.as_mut() else {
            return Ok(0);
        };
        match &mut take.step {
            SnapshotStep::Incomplete => {
                self.take_snapshot = None;
                self.after_snapshot(ports);
                Ok(1)
            }
            SnapshotStep::AddPublication => {
                take.step = SnapshotStep::AwaitPublication;
                self.actions.push_back(Action::AddSnapshotPublication {
                    channel: self.config.snapshot_channel.clone(),
                    stream_id: self.config.snapshot_stream_id,
                });
                Ok(1)
            }
            SnapshotStep::AwaitPublication | SnapshotStep::StartRecording => Ok(0),
            SnapshotStep::AwaitCounter { session_id } => {
                let session_id = *session_id;
                let Some(counter) = ports.find_recording_counter(session_id, self.archive.archive_id()) else {
                    return Ok(0);
                };
                let (log_position, term) = (take.log_position, self.leadership_term_id);
                let timers = self.timers.snapshot();
                let (messages, errors) =
                    consensus_module_snapshot_taker::take(consensus_module_snapshot_taker::State {
                        log_position,
                        leadership_term_id: term,
                        term_base_log_position: self.term_base_log_position(),
                        time_unit: self.config.time_unit.codec(),
                        app_version: self.config.app_version,
                        next_committed_session_id: self.sessions.next_committed_session_id(),
                        sessions: self.sessions.snapshot_sessions(),
                        timers: &timers,
                        trackers: &self.trackers,
                    });
                for e in errors {
                    self.error(e);
                }
                if let Some(take) = self.take_snapshot.as_mut() {
                    take.step = SnapshotStep::Offer {
                        counter,
                        messages,
                        next: 0,
                    };
                }
                Ok(1)
            }
            SnapshotStep::Offer {
                counter,
                messages,
                next,
            } => {
                let mut work = 0;
                while *next < messages.len() {
                    match ports.offer_snapshot(&messages[*next]) {
                        Offer::Sent => {
                            *next += 1;
                            work += 1;
                        }
                        Offer::Retry => break,
                        other => return Err(Fault::Error(format!("snapshot publication: {other:?}"))),
                    }
                }
                if *next == messages.len() {
                    let (counter, position) = (*counter, ports.snapshot_position());
                    take.step = SnapshotStep::AwaitRecorded { counter, position };
                }
                Ok(work)
            }
            SnapshotStep::AwaitRecorded { counter, position } => {
                let (counter, position) = (*counter, *position);
                if ports.counter_value(counter.counter_id) < position {
                    if !ports.is_counter_active(counter.counter_id, counter.registration_id) {
                        self.take_snapshot = None;
                        self.actions.push_back(Action::RemoveSnapshotPublication);
                        self.error(format!(
                            "failed to take snapshot: recording has stopped unexpectedly: {}",
                            counter.recording_id
                        ));
                        self.after_snapshot(ports);
                    }
                    return Ok(0);
                }
                let take = self.take_snapshot.take().expect("a snapshot");
                self.actions.push_back(Action::RemoveSnapshotPublication);
                self.record_snapshot(&take, counter.recording_id, ports)?;
                self.after_snapshot(ports);
                Ok(1)
            }
        }
    }

    /// The snapshot recorded: its entries, the services' then the module's,
    /// in the recording log.
    fn record_snapshot(&mut self, take: &TakeSnapshot, recording_id: i64, ports: &mut impl Ports) -> Outcome<()> {
        let term = self.leadership_term_id;
        let base = self
            .recording_log
            .get_term_entry(term)
            .map_err(|e| e.to_string())?
            .term_base_log_position;
        for (service_id, ack) in take.acks.iter().enumerate().rev() {
            self.recording_log
                .append_snapshot(
                    ack.relevant_id,
                    term,
                    base,
                    take.log_position,
                    take.timestamp,
                    service_id as i32,
                )
                .map_err(|e| e.to_string())?;
        }
        self.recording_log
            .append_snapshot(recording_id, term, base, take.log_position, take.timestamp, SERVICE_ID)
            .map_err(|e| e.to_string())?;
        self.flush_recording_log(true);
        self.refresh_recovery_plan(ports)?;
        self.snapshot_count += 1;
        ports.set_counter(CounterId::SnapshotCount, self.snapshot_count);
        Ok(())
    }

    /// After a snapshot, taken or not: termination if one waits, else active again.
    fn after_snapshot(&mut self, ports: &mut impl Ports) {
        self.sessions.update_time_of_last_activity(self.now_ns);
        if self.termination.is_some() {
            if self.config.service_count > 0 {
                self.send_termination_position(self.termination_position, true, ports);
            } else if let Some(t) = self.termination.as_mut() {
                t.is_awaiting_services = false;
            }
            let deadline = self.now_ns + self.config.termination_timeout_ns;
            if let Some(t) = self.termination.as_mut() {
                t.deadline_ns = deadline;
            }
            self.set_state(ModuleState::Terminating, ports);
        } else {
            self.set_state(ModuleState::Active, ports);
            if self.role == Role::Leader {
                ports.set_control_toggle(toggle::NEUTRAL);
            }
        }
    }

    fn flush_recording_log(&mut self, sync: bool) {
        let writes = self.recording_log.take_writes();
        if !writes.is_empty() || sync {
            self.actions.push_back(Action::WriteRecordingLog { writes, sync });
        }
    }

    fn commit_log_position(&mut self, leadership_term_id: i64, position: i64) -> Outcome<()> {
        self.recording_log
            .commit_log_position(leadership_term_id, position)
            .map_err(|e| e.to_string())?;
        self.flush_recording_log(false);
        Ok(())
    }

    /// The recovery plan made again, from the archive's listing of the log's recording.
    fn refresh_recovery_plan(&mut self, ports: &mut impl Ports) -> Outcome<()> {
        if let Some(recording_id) = self.recording_log.plan_recording_id(self.log_recording_id) {
            self.archive_request(
                ports,
                archive::Request::ListRecording { recording_id },
                Some(Request::PlanRefresh { recording_id }),
            )?;
        }
        Ok(())
    }

    // ---------------------------------------------------------------- termination

    /// A shutdown or abort on the leader: every other member told where the
    /// log ends.
    fn start_termination(&mut self, position: i64, ports: &mut impl Ports) -> Outcome<()> {
        self.termination = Some(ClusterTermination {
            deadline_ns: self.now_ns + self.config.termination_timeout_ns,
            is_awaiting_services: true,
        });
        self.send_member_termination_positions(position, ports)?;
        self.termination_position = position;
        self.termination_leadership_term_id = self.leadership_term_id;
        Ok(())
    }

    fn can_terminate(&self, termination: &ClusterTermination, now_ns: i64) -> bool {
        if termination.is_awaiting_services {
            return false;
        }
        self.members.iter().all(|m| m.is_leader || m.has_terminated) || now_ns >= termination.deadline_ns
    }

    fn send_termination_position(&mut self, position: i64, ack_required: bool, ports: &mut impl Ports) {
        let message = service_proxy::termination_position(position, ack_required);
        if !self.offer_service(ports, &message) {
            self.warn("failed to send service termination position".into());
        }
    }

    fn do_termination(&mut self, log_position: i64, ports: &mut impl Ports) {
        let now_ns = self.now_ns;
        match self.termination.as_ref() {
            None => {
                if self.termination_leadership_term_id == self.leadership_term_id {
                    let leader = self.members[self.leader].id;
                    let ack = consensus_adapter::termination_ack(
                        self.leadership_term_id,
                        log_position,
                        self.config.member_id,
                    );
                    if let Err(e) = consensus_adapter::send(ports, leader, &ack) {
                        self.error(e);
                    }
                } else {
                    self.error("termination ack not sent - different leadership term to request".into());
                }
                let term = self.leadership_term_id;
                if let Err(Fault::Error(e) | Fault::Warning(e) | Fault::Terminate(e)) =
                    self.commit_log_position(term, log_position)
                {
                    self.error(e);
                }
                self.close_and_terminate(ports);
            }
            Some(t) if self.can_terminate(t, now_ns) => {
                let term = self.leadership_term_id;
                if let Err(Fault::Error(e) | Fault::Warning(e) | Fault::Terminate(e)) =
                    self.commit_log_position(term, log_position)
                {
                    self.error(e);
                }
                self.close_and_terminate(ports);
            }
            Some(_) => {}
        }
    }

    fn try_stop_log_recording(&mut self, ports: &mut impl Ports) {
        self.append_counter = None;
        if !self.archive.is_connected() {
            return;
        }
        let request = if self.log_subscription_id != NULL_VALUE {
            let subscription_id = std::mem::replace(&mut self.log_subscription_id, NULL_VALUE);
            archive::Request::StopRecordingSubscription { subscription_id }
        } else if self.log_recording_id != NULL_VALUE {
            archive::Request::StopRecordingByIdentity {
                recording_id: self.log_recording_id,
            }
        } else {
            return;
        };
        let _ = self.archive_request(ports, request, Some(Request::TryStop));
        self.archive.do_work(&mut ArchiveLink(ports));
    }

    fn close_and_terminate(&mut self, ports: &mut impl Ports) {
        self.try_stop_log_recording(ports);
        self.set_state(ModuleState::Closed, ports);
        self.terminated = true;
        self.actions.push_back(Action::Terminate { expected: true });
    }

    fn unexpected_termination(&mut self, reason: &str, ports: &mut impl Ports) {
        if self.terminated {
            return;
        }
        if self.config.service_count > 0 {
            self.send_termination_position(0, true, ports);
        }
        self.try_stop_log_recording(ports);
        let _ = reason;
        self.set_state(ModuleState::Closed, ports);
        self.terminated = true;
        self.actions.push_back(Action::Terminate { expected: false });
    }

    /// The node stops at its operator's request (Java's `onClose`): the
    /// services told where to stop, the log recording stopped.
    pub fn close(&mut self, ports: &mut impl Ports) {
        if self.config.service_count > 0 {
            let position = self.commit_position;
            self.send_termination_position(position, false, ports);
        }
        self.actions.push_back(Action::RemoveLogPublication);
        self.try_stop_log_recording(ports);
        self.sessions.close_sessions();
        self.actions.extend(self.sessions.take_actions());
        self.set_state(ModuleState::Closed, ports);
        self.terminated = true;
        if let Some(close) = self.archive.close() {
            ports.offer_archive(&close);
        }
    }

    // ---------------------------------------------------------------- the log

    fn stop_replay(&mut self, replay: LogReplay, ports: &mut impl Ports) {
        if replay.replay_session_id != NULL_VALUE {
            let replay_session_id = replay.replay_session_id;
            let _ = self.archive_request(
                ports,
                archive::Request::StopReplay { replay_session_id },
                Some(Request::StopReplay),
            );
        }
        match replay.step {
            ReplayStep::Read => {
                self.disconnect_log();
            }
            ReplayStep::AwaitImage { reader } | ReplayStep::AwaitServices { reader } => {
                self.actions.push_back(Action::RemoveSubscription {
                    subscription: reader.subscription,
                });
            }
            ReplayStep::Replay { .. } => {}
        }
    }

    /// The position the log's image reached (Java's `LogAdapter.position`).
    fn log_adapter_position(&self) -> i64 {
        self.reader
            .as_ref()
            .and_then(|r| r.position)
            .unwrap_or(self.log_adapter_position)
    }

    /// Java's `addLogPublication`: the leader's log publication, tagged,
    /// from where the log is.
    fn add_log_publication(&mut self, append_position: i64, ports: &mut impl Ports) {
        let publication_tag = ports.next_correlation_id();
        self.log_publication_channel_tag = ports.next_correlation_id();
        let mut uri = ChannelUri::parse(&self.config.log_channel)
            .unwrap_or_else(|_| ChannelUri::parse("aeron:udp").expect("a channel"));
        uri.put("alias", "log");
        uri.put(
            "tags",
            &format!("{},{}", self.log_publication_channel_tag, publication_tag),
        );
        if uri.is_udp() {
            if self.config.is_log_mdc() {
                uri.put("control-mode", "manual");
                if self.config.enable_control_on_log_control
                    && let Some(control) =
                        cluster_member::replace_port_with_wildcard(&self.members[self.this].log_endpoint)
                {
                    uri.put("control", &control);
                }
            }
            uri.put("ssc", if self.members.len() == 1 { "true" } else { "false" });
        }
        match self.recovery_plan.as_ref().and_then(|p| p.log) {
            Some(log) => {
                let term_length = log.term_buffer_length;
                let bits = term_length.trailing_zeros();
                let term_id = (append_position >> bits) as i32 + log.initial_term_id;
                let term_offset = (append_position & (i64::from(term_length) - 1)) as i32;
                uri.put("init-term-id", &log.initial_term_id.to_string());
                uri.put("term-id", &term_id.to_string());
                uri.put("term-offset", &term_offset.to_string());
                uri.put("term-length", &term_length.to_string());
                uri.put("mtu", &log.mtu_length.to_string());
            }
            None => {
                uri.put("init-term-id", "0");
                uri.put("term-id", "0");
                uri.put("term-offset", "0");
            }
        }
        self.log_session_id = None;
        self.actions.push_back(Action::AddLogPublication {
            channel: uri.to_string(),
            stream_id: self.config.log_stream_id,
        });
        if self.config.is_log_mdc() {
            for i in 0..self.members.len() {
                if i != self.this {
                    let destination =
                        election::create_destination_uri(&self.config.log_channel, &self.members[i].log_endpoint);
                    self.actions.push_back(Action::AddLogDestination { destination });
                }
            }
        }
    }

    fn restore_uncommitted_entries(&mut self, commit_position: i64, ports: &mut impl Ports) {
        for &(position, correlation_id) in &self.uncommitted_timers {
            if position > commit_position {
                self.timers.schedule(correlation_id, 0);
            }
        }
        self.uncommitted_timers.clear();
        for tracker in &mut self.trackers {
            tracker.restore_uncommitted_messages(commit_position);
        }
        self.sessions.restore_uncommitted_sessions(commit_position);
        while self
            .uncommitted_previous_state
            .front()
            .is_some_and(|&(p, _)| p <= commit_position)
        {
            self.uncommitted_previous_state.pop_front();
        }
        if let Some((_, state)) = self.uncommitted_previous_state.pop_front()
            && self.state != ModuleState::Closed
        {
            self.set_state(state, ports);
        }
        self.uncommitted_previous_state.clear();
    }
}

/// `ChannelUri.addSessionId`.
fn add_session_id(channel: &str, session_id: i32) -> String {
    match ChannelUri::parse(channel) {
        Ok(mut uri) => {
            uri.put("session-id", &session_id.to_string());
            uri.to_string()
        }
        Err(_) => format!("{channel}?session-id={session_id}"),
    }
}

/// The recovery state counter's key and label (Java's `addRecoveryStateCounter`).
fn recovery_state_counter(plan: &RecoveryPlan, leadership_term_id: i64, cluster_id: i32) -> (Vec<u8>, String) {
    let state = match plan.snapshots.first() {
        Some(snapshot) => {
            let mut ids = vec![NULL_VALUE; plan.snapshots.len() - 1];
            for s in &plan.snapshots[1..] {
                if let Some(slot) = usize::try_from(s.service_id).ok().and_then(|i| ids.get_mut(i)) {
                    *slot = s.recording_id;
                }
            }
            RecoveryState {
                leadership_term_id: snapshot.leadership_term_id,
                log_position: snapshot.log_position,
                timestamp: snapshot.timestamp,
                cluster_id,
                snapshot_recording_ids: ids,
            }
        }
        None => RecoveryState {
            leadership_term_id,
            log_position: 0,
            timestamp: 0,
            cluster_id,
            snapshot_recording_ids: Vec::new(),
        },
    };
    let label = cluster_counters::recovery_state_label(state.leadership_term_id, state.log_position, cluster_id);
    (state.to_key(), label)
}
