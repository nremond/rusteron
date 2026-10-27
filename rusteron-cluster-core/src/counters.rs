//! The Aeron counters a clustered service finds and reads, and the layout of
//! their keys (`ClusterCounters`, `RecoveryState`, `RecordingPos` in Java).

/// The consensus module's commit position: a service never reads the log past it.
pub const COMMIT_POSITION_TYPE_ID: i32 = 203;
/// What a service recovers from at start: the latest snapshot, and where the log continues.
pub const RECOVERY_STATE_TYPE_ID: i32 = 204;
/// An archive's position in one of its recordings.
pub const RECORDING_POSITION_TYPE_ID: i32 = 100;

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
