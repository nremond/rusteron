//! How a consensus module runs: what Java's `ConsensusModule.Context` sets
//! for a plain cluster, with Java's defaults.

use rusteron_archive::channel_uri::ChannelUri;
use rusteron_cluster_codecs::cluster::cluster_time_unit::ClusterTimeUnit;

/// The service id the module's own entries take.
pub const SERVICE_ID: i32 = -1;

/// A cluster's time unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeUnit {
    Millis,
    Micros,
    Nanos,
}

impl TimeUnit {
    pub fn to_nanos(self, time: i64) -> i64 {
        match self {
            TimeUnit::Millis => time.saturating_mul(1_000_000),
            TimeUnit::Micros => time.saturating_mul(1_000),
            TimeUnit::Nanos => time,
        }
    }

    pub fn from_nanos(self, nanos: i64) -> i64 {
        match self {
            TimeUnit::Millis => nanos / 1_000_000,
            TimeUnit::Micros => nanos / 1_000,
            TimeUnit::Nanos => nanos,
        }
    }

    pub fn codec(self) -> ClusterTimeUnit {
        match self {
            TimeUnit::Millis => ClusterTimeUnit::MILLIS,
            TimeUnit::Micros => ClusterTimeUnit::MICROS,
            TimeUnit::Nanos => ClusterTimeUnit::NANOS,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            TimeUnit::Millis => "MILLISECONDS",
            TimeUnit::Micros => "MICROSECONDS",
            TimeUnit::Nanos => "NANOSECONDS",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub cluster_id: i32,
    pub member_id: i32,
    /// `aeron.cluster.members`.
    pub cluster_members: String,
    pub service_count: usize,
    /// Written in snapshots and new terms; 0.0.1 as Java's default.
    pub app_version: i32,
    pub time_unit: TimeUnit,
    /// 0 none, 1 data, 2 data and metadata: for the recording log and node state.
    pub file_sync_level: i32,

    /// Required: where clients connect.
    pub ingress_channel: String,
    pub ingress_stream_id: i32,
    pub log_channel: String,
    pub log_stream_id: i32,
    pub consensus_channel: String,
    pub consensus_stream_id: i32,
    /// The services' control channel, streams 104 to them and 105 from them.
    pub control_channel: String,
    pub service_stream_id: i32,
    pub consensus_module_stream_id: i32,
    pub snapshot_channel: String,
    pub snapshot_stream_id: i32,
    pub replay_channel: String,
    pub replay_stream_id: i32,
    /// Where this member's archive replicates from a leader's, required for
    /// clusters of more than one.
    pub replication_channel: String,
    pub leader_archive_control_channel: String,
    pub archive_control_request_stream_id: i32,
    /// The channel a leader replays its log on to a follower catching up,
    /// the follower's catch-up endpoint added.
    pub follower_catchup_channel: String,
    /// A member appointed leader, or -1: the others never nominate themselves.
    pub appointed_leader_id: i32,
    /// Binds the log's MDC control to this member's log endpoint's host.
    pub enable_control_on_log_control: bool,
    pub enable_control_on_consensus_channel: bool,

    pub max_concurrent_sessions: usize,
    pub session_timeout_ns: i64,
    pub leader_heartbeat_timeout_ns: i64,
    pub leader_heartbeat_interval_ns: i64,
    pub election_timeout_ns: i64,
    pub election_status_interval_ns: i64,
    pub startup_canvass_timeout_ns: i64,
    pub termination_timeout_ns: i64,

    /// `aeron.cluster.accept.standby.snapshots`: standbys' notices of their
    /// snapshots filed in the recording log, for a restart to copy.
    pub accept_standby_snapshots: bool,
    /// `aeron.cluster.standby.snapshot.notification.processing.delay`: how
    /// long a notice waits once the commit position has passed it.
    pub standby_snapshot_notification_delay_ns: i64,

    /// The seed of the nomination delay's generator; the shell draws one.
    pub random_seed: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cluster_id: 0,
            member_id: 0,
            cluster_members: "0,localhost:20000,localhost:20001,localhost:20002,localhost:0,localhost:8010".into(),
            service_count: 1,
            app_version: 1,
            time_unit: TimeUnit::Millis,
            file_sync_level: 0,
            ingress_channel: String::new(),
            ingress_stream_id: 101,
            log_channel: "aeron:udp?term-length=64m".into(),
            log_stream_id: 100,
            consensus_channel: "aeron:udp".into(),
            consensus_stream_id: 108,
            control_channel: "aeron:ipc?term-length=128k".into(),
            service_stream_id: 104,
            consensus_module_stream_id: 105,
            snapshot_channel: "aeron:ipc?alias=snapshot".into(),
            snapshot_stream_id: 107,
            replay_channel: "aeron:ipc".into(),
            replay_stream_id: 103,
            replication_channel: String::new(),
            leader_archive_control_channel: "aeron:udp?term-length=64k".into(),
            archive_control_request_stream_id: 10,
            follower_catchup_channel: "aeron:udp".into(),
            appointed_leader_id: -1,
            enable_control_on_log_control: true,
            enable_control_on_consensus_channel: false,
            max_concurrent_sessions: 10,
            session_timeout_ns: 10_000_000_000,
            leader_heartbeat_timeout_ns: 10_000_000_000,
            leader_heartbeat_interval_ns: 200_000_000,
            election_timeout_ns: 1_000_000_000,
            election_status_interval_ns: 100_000_000,
            startup_canvass_timeout_ns: 60_000_000_000,
            termination_timeout_ns: 10_000_000_000,
            accept_standby_snapshots: false,
            standby_snapshot_notification_delay_ns: 0,
            random_seed: 1,
        }
    }
}

impl Config {
    /// Whether the log goes to followers through multi-destination-cast:
    /// UDP without an endpoint, as Java's `isLogMdc`.
    pub fn is_log_mdc(&self) -> bool {
        ChannelUri::parse(&self.log_channel).is_ok_and(|uri| uri.is_udp() && uri.get("endpoint").is_none())
    }
}
