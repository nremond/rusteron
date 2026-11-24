//! The Aeron counters of a cluster: those a clustered service finds and
//! reads, those a consensus module allocates, and the layout of their keys
//! and labels (`ClusterCounters`, `RecoveryState`, `RecordingPos` in Java).

pub mod io;

/// The consensus module's state: `ConsensusModule.State`.
pub const MODULE_STATE_TYPE_ID: i32 = 200;
/// The node's role: follower, candidate, leader.
pub const NODE_ROLE_TYPE_ID: i32 = 201;
/// The control toggle an operator sets (snapshot, suspend, shutdown...).
pub const CONTROL_TOGGLE_TYPE_ID: i32 = 202;
/// The consensus module's commit position: a service never reads the log past it.
pub const COMMIT_POSITION_TYPE_ID: i32 = 203;
/// What a service recovers from at start: the latest snapshot, and where the log continues.
pub const RECOVERY_STATE_TYPE_ID: i32 = 204;
/// How many snapshots the module took.
pub const SNAPSHOT_COUNTER_TYPE_ID: i32 = 205;
/// The election's state, `ElectionState`.
pub const ELECTION_STATE_TYPE_ID: i32 = 207;
/// The module's errors.
pub const ERROR_COUNT_TYPE_ID: i32 = 212;
/// Clients whose sessions timed out.
pub const CLIENT_TIMEOUT_COUNT_TYPE_ID: i32 = 213;
/// The longest duty cycle.
pub const MAX_CYCLE_TIME_TYPE_ID: i32 = 216;
/// Duty cycles longer than the threshold.
pub const CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID: i32 = 217;
/// Standbys' snapshots a member filed, allocated only when it accepts them.
pub const STANDBY_SNAPSHOT_COUNTER_TYPE_ID: i32 = 232;
/// The node control toggle (standby snapshots, recording log validation).
pub const NODE_CONTROL_TOGGLE_TYPE_ID: i32 = 233;
/// The longest snapshot, all services included.
pub const TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID: i32 = 234;
/// Snapshots longer than the threshold.
pub const TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID: i32 = 235;
/// How many elections the module ran.
pub const ELECTION_COUNT_TYPE_ID: i32 = 238;
/// The current leadership term.
pub const LEADERSHIP_TERM_ID_TYPE_ID: i32 = 239;
/// One for each client session.
pub const SESSION_TYPE_ID: i32 = 241;
/// A cluster backup's state, `ClusterBackup.State`.
pub const BACKUP_STATE_TYPE_ID: i32 = 208;
/// How far a cluster backup has recorded the live log.
pub const BACKUP_LIVE_LOG_POSITION_TYPE_ID: i32 = 209;
/// When a cluster backup queries the cluster next, in epoch milliseconds.
pub const BACKUP_QUERY_DEADLINE_TYPE_ID: i32 = 210;
/// A cluster backup's errors.
pub const BACKUP_ERROR_COUNT_TYPE_ID: i32 = 211;
/// The snapshots a cluster backup copied.
pub const BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID: i32 = 240;
/// An archive's position in one of its recordings.
pub const RECORDING_POSITION_TYPE_ID: i32 = 100;

/// The version the module's errors counter names, Aeron's.
pub const VERSION: &str = "1.54.2";
/// The commit it names.
pub const GIT_SHA: &str = "481d4271ad";
/// A counter's label suffix naming its cluster.
const CLUSTER_ID_LABEL_SUFFIX: &str = " - clusterId=";

/// A duration as Agrona's `SystemUtil.formatDuration` writes it: in the
/// largest unit that divides it.
fn format_duration(ns: i64) -> String {
    for (unit, suffix) in [(1_000_000_000, "s"), (1_000_000, "ms"), (1_000, "us")] {
        if ns >= unit && ns % unit == 0 {
            return format!("{}{suffix}", ns / unit);
        }
    }
    format!("{ns}ns")
}

/// A cluster counter's key (the cluster id) and label: `ClusterCounters.allocate`.
pub fn cluster_counter(name: &str, cluster_id: i32) -> (Vec<u8>, String) {
    (
        cluster_id.to_le_bytes().to_vec(),
        format!("{name}{CLUSTER_ID_LABEL_SUFFIX}{cluster_id}"),
    )
}

/// The module's errors counter, with its version: `ClusterCounters.allocateVersioned`.
pub fn error_counter(cluster_id: i32) -> (Vec<u8>, String) {
    (
        cluster_id.to_le_bytes().to_vec(),
        format!("Cluster Errors{CLUSTER_ID_LABEL_SUFFIX}{cluster_id} version={VERSION} commit={GIT_SHA}"),
    )
}

/// A cluster backup's errors counter, with its version.
pub fn backup_error_counter(cluster_id: i32) -> (Vec<u8>, String) {
    (
        cluster_id.to_le_bytes().to_vec(),
        format!("ClusterBackup Errors{CLUSTER_ID_LABEL_SUFFIX}{cluster_id} version={VERSION} commit={GIT_SHA}"),
    )
}

/// The counters a cluster backup allocates after its errors counter, in
/// Java's order (`ClusterBackup.Context.conclude`).
pub fn backup_counters() -> Vec<(i32, &'static str)> {
    vec![
        (BACKUP_STATE_TYPE_ID, "ClusterBackup State"),
        (BACKUP_LIVE_LOG_POSITION_TYPE_ID, "ClusterBackup live log position"),
        (BACKUP_QUERY_DEADLINE_TYPE_ID, "ClusterBackup next query deadline in ms"),
        (
            BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID,
            "ClusterBackup snapshots retrieved",
        ),
    ]
}

/// The counters a consensus module allocates as it starts, after its errors
/// counter, in Java's order (`ConsensusModule.Context.conclude`), without
/// the standby snapshots' it allocates only when it accepts them.
pub fn consensus_module_counters(
    cycle_threshold_ns: i64,
    total_snapshot_duration_threshold_ns: i64,
) -> Vec<(i32, String)> {
    vec![
        (MODULE_STATE_TYPE_ID, "Consensus Module state".into()),
        (ELECTION_STATE_TYPE_ID, "Cluster election state".into()),
        (ELECTION_COUNT_TYPE_ID, "Cluster election count".into()),
        (LEADERSHIP_TERM_ID_TYPE_ID, "Cluster leadership term id".into()),
        (NODE_ROLE_TYPE_ID, "Cluster node role".into()),
        (COMMIT_POSITION_TYPE_ID, "Cluster commit-pos:".into()),
        (CONTROL_TOGGLE_TYPE_ID, "Cluster control toggle".into()),
        (NODE_CONTROL_TOGGLE_TYPE_ID, "Node control toggle".into()),
        (SNAPSHOT_COUNTER_TYPE_ID, "Cluster snapshot count".into()),
        (CLIENT_TIMEOUT_COUNT_TYPE_ID, "Cluster timed out client count".into()),
        (MAX_CYCLE_TIME_TYPE_ID, "Cluster max cycle time in ns".into()),
        (
            CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
            format!(
                "Cluster work cycle time exceeded count: threshold={}",
                format_duration(cycle_threshold_ns)
            ),
        ),
        (
            TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID,
            "Total max snapshot duration in ns".into(),
        ),
        (
            TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID,
            format!(
                "Total max snapshot duration exceeded count: threshold={}",
                format_duration(total_snapshot_duration_threshold_ns)
            ),
        ),
    ]
}

/// A client session's counter: its key the cluster id and the session id,
/// its label what the client said of itself (`ClusterSession.addSessionCounter`).
pub fn session_counter(cluster_id: i32, cluster_session_id: i64, session_info: &str) -> (Vec<u8>, String) {
    let mut key = cluster_id.to_le_bytes().to_vec();
    key.extend_from_slice(&cluster_session_id.to_le_bytes());
    (
        key,
        format!("cluster-session: {session_info}{CLUSTER_ID_LABEL_SUFFIX}{cluster_id}"),
    )
}

/// The recovery-state counter's label (`RecoveryState.allocate`).
pub fn recovery_state_label(leadership_term_id: i64, log_position: i64, cluster_id: i32) -> String {
    format!("Cluster recovery: leadershipTermId={leadership_term_id} logPosition={log_position} clusterId={cluster_id}")
}

/// Aeron's null for ids and positions.
pub const NULL_VALUE: i64 = -1;

/// A key too short for what its type says it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShortKey {
    pub type_id: i32,
    pub length: usize,
}

impl std::fmt::Display for ShortKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "counter key of type {} too short: {} bytes",
            self.type_id, self.length
        )
    }
}

impl std::error::Error for ShortKey {}

fn i32_at(key: &[u8], offset: usize) -> Option<i32> {
    key.get(offset..offset + 4)
        .map(|b| i32::from_le_bytes(b.try_into().expect("4 bytes")))
}

fn i64_at(key: &[u8], offset: usize) -> Option<i64> {
    key.get(offset..offset + 8)
        .map(|b| i64::from_le_bytes(b.try_into().expect("8 bytes")))
}

/// Whether a commit-position counter's key names this cluster: its first
/// four bytes are the cluster id, as for every cluster counter.
pub fn is_cluster_counter(key: &[u8], cluster_id: i32) -> bool {
    i32_at(key, 0) == Some(cluster_id)
}

/// The recovery state the consensus module leaves for its services while
/// they start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryState {
    /// The term of the latest snapshot, [`NULL_VALUE`] when there is none.
    pub leadership_term_id: i64,
    /// Where the log continues: the snapshot's position, or 0.
    pub log_position: i64,
    /// The cluster time at that position.
    pub timestamp: i64,
    pub cluster_id: i32,
    /// The snapshot's recording for each service, by service id.
    pub snapshot_recording_ids: Vec<i64>,
}

impl RecoveryState {
    const LEADERSHIP_TERM_ID: usize = 0;
    const LOG_POSITION: usize = 8;
    const TIMESTAMP: usize = 16;
    const CLUSTER_ID: usize = 24;
    const SERVICE_COUNT: usize = 28;
    const SNAPSHOT_RECORDING_IDS: usize = 32;

    /// Whether a recovery-state counter's key belongs to this cluster.
    pub fn is_for_cluster(key: &[u8], cluster_id: i32) -> bool {
        i32_at(key, Self::CLUSTER_ID) == Some(cluster_id)
    }

    /// Reads a recovery-state counter's key.
    pub fn parse(key: &[u8]) -> Result<Self, ShortKey> {
        let short = ShortKey {
            type_id: RECOVERY_STATE_TYPE_ID,
            length: key.len(),
        };
        let service_count = i32_at(key, Self::SERVICE_COUNT).ok_or(short)?;
        let service_count = usize::try_from(service_count).map_err(|_| short)?;
        let snapshot_recording_ids = (0..service_count)
            .map(|i| i64_at(key, Self::SNAPSHOT_RECORDING_IDS + i * 8))
            .collect::<Option<Vec<_>>>()
            .ok_or(short)?;
        Ok(Self {
            leadership_term_id: i64_at(key, Self::LEADERSHIP_TERM_ID).ok_or(short)?,
            log_position: i64_at(key, Self::LOG_POSITION).ok_or(short)?,
            timestamp: i64_at(key, Self::TIMESTAMP).ok_or(short)?,
            cluster_id: i32_at(key, Self::CLUSTER_ID).ok_or(short)?,
            snapshot_recording_ids,
        })
    }

    /// The snapshot service `service_id` starts from, if there is one.
    pub fn snapshot_recording_id(&self, service_id: i32) -> Option<i64> {
        if self.leadership_term_id == NULL_VALUE {
            return None;
        }
        usize::try_from(service_id)
            .ok()
            .and_then(|i| self.snapshot_recording_ids.get(i))
            .copied()
    }

    /// The key as the consensus module writes it, for tests.
    pub fn to_key(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(Self::SNAPSHOT_RECORDING_IDS + 8 * self.snapshot_recording_ids.len());
        key.extend_from_slice(&self.leadership_term_id.to_le_bytes());
        key.extend_from_slice(&self.log_position.to_le_bytes());
        key.extend_from_slice(&self.timestamp.to_le_bytes());
        key.extend_from_slice(&self.cluster_id.to_le_bytes());
        key.extend_from_slice(&(self.snapshot_recording_ids.len() as i32).to_le_bytes());
        for id in &self.snapshot_recording_ids {
            key.extend_from_slice(&id.to_le_bytes());
        }
        key
    }
}

/// An archive's recording-position counter, as its key describes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordingPosition {
    pub recording_id: i64,
    pub session_id: i32,
    pub archive_id: i64,
}

impl RecordingPosition {
    const RECORDING_ID: usize = 0;
    const SESSION_ID: usize = 8;
    const SOURCE_IDENTITY_LENGTH: usize = 12;
    const SOURCE_IDENTITY: usize = 16;

    /// Reads a recording-position counter's key: the archive id follows the
    /// source identity, unaligned.
    pub fn parse(key: &[u8]) -> Result<Self, ShortKey> {
        let short = ShortKey {
            type_id: RECORDING_POSITION_TYPE_ID,
            length: key.len(),
        };
        let identity_length = i32_at(key, Self::SOURCE_IDENTITY_LENGTH).ok_or(short)?;
        let identity_length = usize::try_from(identity_length).map_err(|_| short)?;
        Ok(Self {
            recording_id: i64_at(key, Self::RECORDING_ID).ok_or(short)?,
            session_id: i32_at(key, Self::SESSION_ID).ok_or(short)?,
            archive_id: i64_at(key, Self::SOURCE_IDENTITY + identity_length).ok_or(short)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_state_round_trips_and_names_each_services_snapshot() {
        let state = RecoveryState {
            leadership_term_id: 3,
            log_position: 4_096,
            timestamp: 1_700_000_000_000,
            cluster_id: 7,
            snapshot_recording_ids: vec![11, 12],
        };
        let key = state.to_key();
        assert_eq!(key.len(), 48);
        assert!(RecoveryState::is_for_cluster(&key, 7));
        assert!(!RecoveryState::is_for_cluster(&key, 0));
        let parsed = RecoveryState::parse(&key).expect("parses");
        assert_eq!(parsed, state);
        assert_eq!(parsed.snapshot_recording_id(1), Some(12));
        assert_eq!(parsed.snapshot_recording_id(2), None);
    }

    #[test]
    fn no_snapshot_without_a_term() {
        let state = RecoveryState {
            leadership_term_id: NULL_VALUE,
            log_position: 0,
            timestamp: 0,
            cluster_id: 0,
            snapshot_recording_ids: vec![],
        };
        assert_eq!(
            RecoveryState::parse(&state.to_key())
                .expect("parses")
                .snapshot_recording_id(0),
            None
        );
    }

    #[test]
    fn a_short_key_is_refused() {
        let key = RecoveryState {
            leadership_term_id: 1,
            log_position: 0,
            timestamp: 0,
            cluster_id: 0,
            snapshot_recording_ids: vec![5],
        }
        .to_key();
        assert!(RecoveryState::parse(&key[..key.len() - 1]).is_err());
        assert!(RecoveryState::parse(&key[..20]).is_err());
    }

    #[test]
    fn recording_position_finds_the_archive_id_after_the_identity() {
        let identity = b"aeron:ipc";
        let mut key = Vec::new();
        key.extend_from_slice(&42i64.to_le_bytes());
        key.extend_from_slice(&(-7i32).to_le_bytes());
        key.extend_from_slice(&(identity.len() as i32).to_le_bytes());
        key.extend_from_slice(identity);
        key.extend_from_slice(&3i64.to_le_bytes());
        let parsed = RecordingPosition::parse(&key).expect("parses");
        assert_eq!(
            parsed,
            RecordingPosition {
                recording_id: 42,
                session_id: -7,
                archive_id: 3
            }
        );
        assert!(RecordingPosition::parse(&key[..key.len() - 1]).is_err());
    }
}
