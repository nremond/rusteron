//! What the consensus module asks of its shell, and what the shell tells
//! it back.
//!
//! - [`Ports`]: what Java does synchronously and branches on at once:
//!   offers on the log, a member's consensus publication, a client's
//!   egress, the services, the archive and a snapshot; counters read and
//!   written.
//! - [`Action`]: what Java does asynchronously, or what may take time:
//!   publications, subscriptions and counters added and removed, files
//!   written and synced, errors recorded, termination. The shell reports
//!   what came of them as an [`Io`].

/// What an offer came to.
pub use rusteron_archive::archive_conductor::Offer;

/// A client's egress publication, the module's handle for it: sessions
/// redirected or rejected have no cluster session id yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EgressId(pub u64);

/// A subscription the module asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriptionId(pub u64);

/// The module's counters, as the shell holds them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CounterId {
    ModuleState,
    NodeRole,
    CommitPosition,
    ElectionState,
    ElectionCount,
    LeadershipTermId,
    SnapshotCount,
    TimedOutClientCount,
    /// Standbys' snapshots filed, allocated only when they are accepted.
    StandbySnapshotCount,
}

/// An archive's recording-position counter, found by its recording's session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordingCounter {
    pub counter_id: i32,
    pub registration_id: i64,
    pub recording_id: i64,
}

/// Aeron's result codes for an offer that fails, as a position.
pub mod offer_result {
    pub const NOT_CONNECTED: i64 = -1;
    pub const BACK_PRESSURED: i64 = -2;
    pub const ADMIN_ACTION: i64 = -3;
    pub const CLOSED: i64 = -4;
    pub const MAX_POSITION_EXCEEDED: i64 = -5;
}

/// The shell's synchronous side.
pub trait Ports {
    /// The log publication's position, 0 without one.
    fn log_position(&self) -> i64;
    /// The log publication's term length, 0 without one or if not known:
    /// where a message lands when it does not fit the rest of a term.
    fn log_term_length(&self) -> i32 {
        0
    }
    /// Offers a message on the log publication, in parts as Aeron's
    /// vectored offer takes them: the new position, or Aeron's negative
    /// result ([`offer_result`]).
    fn offer_log(&mut self, parts: &[&[u8]]) -> i64;

    /// Whether a client's egress is connected.
    fn egress_connected(&self, egress: EgressId) -> bool;
    /// How far a client's egress may be offered before back pressure.
    fn egress_available_window(&self, egress: EgressId) -> i64;
    fn offer_egress(&mut self, egress: EgressId, message: &[u8]) -> Offer;

    /// Offers on this member's consensus publication to `member_id`.
    fn offer_consensus(&mut self, member_id: i32, message: &[u8]) -> Offer;
    /// Writes `node-state.dat` at `offset`, then syncs it as the file sync
    /// level asks, before the module acts on what it holds: a vote goes out
    /// only once its term is on disk, as Java's `NodeStateFile` has it, so
    /// a crash cannot leave a member free to vote twice in one term.
    fn write_node_state(&mut self, offset: u64, bytes: &[u8]) -> Result<(), String>;
    /// Offers to the services, on the service control stream.
    fn offer_service(&mut self, message: &[u8]) -> Offer;

    /// Offers a request to the local archive.
    fn offer_archive(&mut self, message: &[u8]) -> Offer;
    fn archive_publication_connected(&self) -> bool;
    fn archive_subscription_connected(&self) -> bool;
    /// The archive response channel, its port resolved.
    fn archive_response_channel(&self) -> Option<String>;

    /// The snapshot publication's position.
    fn snapshot_position(&self) -> i64;
    fn offer_snapshot(&mut self, message: &[u8]) -> Offer;

    /// The local archive's recording-position counter of the recording of
    /// `session_id`, once it is allocated and names its recording.
    fn find_recording_counter(&self, session_id: i32, archive_id: i64) -> Option<RecordingCounter>;
    /// The local archive's recording-position counter of `recording_id`, while it records.
    fn find_recording_counter_by_recording(&self, recording_id: i64, archive_id: i64) -> Option<RecordingCounter>;
    fn counter_value(&self, counter_id: i32) -> i64;
    /// Whether the counter is still the one registered as `registration_id`.
    fn is_counter_active(&self, counter_id: i32, registration_id: i64) -> bool;

    fn set_counter(&mut self, counter: CounterId, value: i64);
    /// One of the module's counters' id on the driver: a backup bounds its
    /// replay of this member's log with the commit position's.
    fn counter_id(&self, counter: CounterId) -> i32;
    /// The control toggle's state.
    fn control_toggle(&self) -> i32;
    fn set_control_toggle(&mut self, value: i32);
    /// Sets the control toggle if it is `expected`: whether it was.
    fn compare_and_set_control_toggle(&mut self, expected: i32, new: i32) -> bool;
    /// The node control toggle's state (Java's `NodeControl`).
    fn node_control_toggle(&self) -> i32;
    fn set_node_control_toggle(&mut self, value: i32);

    /// A correlation id unique on the driver: Aeron's.
    fn next_correlation_id(&mut self) -> i64;

    /// Whether a subscription the module removed still holds local sockets
    /// (Java's `awaitLocalSocketsClosed`): a follower's log endpoint is bound again.
    fn has_local_sockets(&self, subscription: SubscriptionId) -> bool;
    /// The endpoint a subscription's socket bound, its port resolved.
    fn resolved_endpoint(&self, subscription: SubscriptionId) -> Option<String>;
}

/// What the module asks of its shell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// A client's response publication, and its session counter (key,
    /// label, the session id as its value).
    ConnectEgress {
        egress: EgressId,
        channel: String,
        stream_id: i32,
        counter: Option<(Vec<u8>, String, i64)>,
    },
    DisconnectEgress {
        egress: EgressId,
    },
    /// The leader's log publication, exclusive; reported as
    /// [`Io::LogPublicationAdded`].
    AddLogPublication {
        channel: String,
        stream_id: i32,
    },
    /// A follower's endpoint on the leader's multi-destination log, once
    /// the log publication is added.
    AddLogDestination {
        destination: String,
    },
    /// This member's publication to another, on the consensus stream.
    AddConsensusPublication {
        member_id: i32,
        channel: String,
        stream_id: i32,
    },
    RemoveLogPublication,
    /// A subscription whose images the module reads (a replay, the
    /// ingress); its images are reported as [`Io::ImageAvailable`].
    AddSubscription {
        subscription: SubscriptionId,
        channel: String,
        stream_id: i32,
    },
    RemoveSubscription {
        subscription: SubscriptionId,
    },
    /// A destination added to a multi-destination subscription: a
    /// follower's catch-up and live log endpoints.
    AddDestination {
        subscription: SubscriptionId,
        destination: String,
    },
    RemoveDestination {
        subscription: SubscriptionId,
        destination: String,
    },
    /// The publication a snapshot is written on, exclusive; reported as
    /// [`Io::SnapshotPublicationAdded`].
    AddSnapshotPublication {
        channel: String,
        stream_id: i32,
    },
    RemoveSnapshotPublication,
    /// The recovery state counter the services start from, until they acked.
    AddRecoveryStateCounter {
        key: Vec<u8>,
        label: String,
    },
    RemoveRecoveryStateCounter,
    /// Writes to `recording.log`, in order, then a sync as the file sync
    /// level asks when `sync`.
    WriteRecordingLog {
        writes: Vec<(u64, Vec<u8>)>,
        sync: bool,
    },
    /// An error for the mark file's error log and the errors counter. `warn`
    /// when Java gives it the WARN category, as its `ClusterEvent`s: Aeron's
    /// tests and tools tell a warning from an error by it.
    Error {
        warn: bool,
        message: String,
    },
    /// The module is done: `expected` as a shutdown is, not as a failure.
    Terminate {
        expected: bool,
    },
}

/// What the shell reports back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Io {
    /// A client's response publication added, or not.
    EgressAdded {
        egress: EgressId,
        result: Result<(), String>,
    },
    /// The log publication added: its session id.
    LogPublicationAdded { session_id: i32 },
    /// The snapshot publication added: its session id.
    SnapshotPublicationAdded { session_id: i32 },
    /// An image on a subscription the module asked for.
    ImageAvailable {
        subscription: SubscriptionId,
        session_id: i32,
        correlation_id: i64,
        join_position: i64,
        term_buffer_length: i32,
        source_identity: String,
    },
    /// An image gone; `end_of_stream` when its publication closed it.
    ImageUnavailable {
        subscription: SubscriptionId,
        session_id: i32,
        correlation_id: i64,
        end_of_stream: bool,
    },
    /// A counter went: a client's heartbeat, a recording's position.
    CounterUnavailable { registration_id: i64 },
}
