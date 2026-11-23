//! A consensus module's `recording.log`, as Java's `RecordingLog` keeps it:
//! the leadership terms of the log's one recording, and the snapshots taken,
//! each an entry of 64 bytes (more for a standby snapshot's endpoint),
//! little-endian, appended and never moved; a position committed and an
//! entry invalidated in place.
//!
//! The file is held as its entries, sorted as Java sorts them; every change
//! queues the bytes Java would write at the offset it would write them, for
//! the shell to write ([`RecordingLog::take_writes`]) and sync as the
//! cluster's file sync level asks. Opening, creating and syncing the file
//! and its directory are the shell's.

use std::cmp::Ordering;
use std::collections::BTreeMap;

/// The file in the cluster directory.
pub const FILE_NAME: &str = "recording.log";
/// An entry for a leadership term of the log.
pub const ENTRY_TYPE_TERM: i32 = 0;
/// An entry for a snapshot taken at a position in the log.
pub const ENTRY_TYPE_SNAPSHOT: i32 = 1;
/// An entry for a snapshot held by another machine's archive.
pub const ENTRY_TYPE_STANDBY_SNAPSHOT: i32 = 2;
/// Set in the type of an entry no longer to be used.
pub const ENTRY_TYPE_INVALID_FLAG: i32 = 1 << 31;
/// The service id of the consensus module's own snapshot, and of a term.
pub const SERVICE_ID: i32 = -1;
/// Aeron's null for ids and positions.
pub const NULL_VALUE: i64 = -1;

const RECORDING_ID_OFFSET: usize = 0;
const LEADERSHIP_TERM_ID_OFFSET: usize = 8;
const TERM_BASE_LOG_POSITION_OFFSET: usize = 16;
const LOG_POSITION_OFFSET: usize = 24;
const TIMESTAMP_OFFSET: usize = 32;
const SERVICE_ID_OFFSET: usize = 40;
const ENTRY_TYPE_OFFSET: usize = 44;
const ENDPOINT_OFFSET: usize = 48;
const RECORD_ALIGNMENT: usize = 64;
/// The longest entry Java reads.
pub const MAX_ENTRY_LENGTH: usize = 4096;
/// The longest archive endpoint a standby snapshot's entry holds.
pub const MAX_ENDPOINT_LENGTH: usize = MAX_ENTRY_LENGTH - ENDPOINT_OFFSET - 4;

/// What Java's `RecordingLog` would throw, its message Java's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

fn error<T>(message: impl Into<String>) -> Result<T, Error> {
    Err(Error(message.into()))
}

/// An entry in the log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub recording_id: i64,
    pub leadership_term_id: i64,
    /// The log's position where the term starts.
    pub term_base_log_position: i64,
    /// Where the term ends once committed ([`NULL_VALUE`] before), or the
    /// snapshot's position.
    pub log_position: i64,
    /// Cluster time, in the cluster's unit.
    pub timestamp: i64,
    pub service_id: i32,
    pub entry_type: i32,
    pub is_valid: bool,
    /// Its place in the file, in entries, removed ones counted.
    pub entry_index: i32,
    /// A standby snapshot's archive.
    pub archive_endpoint: Option<String>,
    /// Its offset in the file.
    pub position: u64,
}

impl Entry {
    /// Its length in the file, aligned.
    pub fn length(&self) -> usize {
        let unaligned = match (&self.archive_endpoint, self.entry_type) {
            (Some(endpoint), ENTRY_TYPE_STANDBY_SNAPSHOT) => ENDPOINT_OFFSET + 4 + endpoint.len(),
            _ => ENDPOINT_OFFSET,
        };
        unaligned.next_multiple_of(RECORD_ALIGNMENT)
    }

    fn is_valid_term(&self) -> bool {
        self.is_valid && self.entry_type == ENTRY_TYPE_TERM
    }

    fn is_valid_snapshot(&self) -> bool {
        self.is_valid && self.entry_type == ENTRY_TYPE_SNAPSHOT
    }

    fn is_valid_any_snapshot(&self) -> bool {
        self.is_valid && (self.entry_type == ENTRY_TYPE_SNAPSHOT || self.entry_type == ENTRY_TYPE_STANDBY_SNAPSHOT)
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot {
            recording_id: self.recording_id,
            leadership_term_id: self.leadership_term_id,
            term_base_log_position: self.term_base_log_position,
            log_position: self.log_position,
            timestamp: self.timestamp,
            service_id: self.service_id,
        }
    }

    /// The bytes Java writes for it: `length()` of them, the padding zero.
    fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![0; self.length()];
        put_i64(&mut bytes, RECORDING_ID_OFFSET, self.recording_id);
        put_i64(&mut bytes, LEADERSHIP_TERM_ID_OFFSET, self.leadership_term_id);
        put_i64(&mut bytes, TERM_BASE_LOG_POSITION_OFFSET, self.term_base_log_position);
        put_i64(&mut bytes, LOG_POSITION_OFFSET, self.log_position);
        put_i64(&mut bytes, TIMESTAMP_OFFSET, self.timestamp);
        bytes[SERVICE_ID_OFFSET..SERVICE_ID_OFFSET + 4].copy_from_slice(&self.service_id.to_le_bytes());
        bytes[ENTRY_TYPE_OFFSET..ENTRY_TYPE_OFFSET + 4].copy_from_slice(&self.entry_type.to_le_bytes());
        if let Some(endpoint) = self.archive_endpoint.as_deref().filter(|e| !e.is_empty()) {
            bytes[ENDPOINT_OFFSET..ENDPOINT_OFFSET + 4].copy_from_slice(&(endpoint.len() as i32).to_le_bytes());
            bytes[ENDPOINT_OFFSET + 4..ENDPOINT_OFFSET + 4 + endpoint.len()].copy_from_slice(endpoint.as_bytes());
        }
        bytes
    }
}

/// As Java's `Entry.toString`, which ClusterTool prints.
impl std::fmt::Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Entry{{recordingId={}, leadershipTermId={}, termBaseLogPosition={}, logPosition={}, timestamp={}, \
             serviceId={}, type={}, isValid={}, entryIndex={}",
            self.recording_id,
            self.leadership_term_id,
            self.term_base_log_position,
            self.log_position,
            self.timestamp,
            self.service_id,
            type_as_str(self.entry_type),
            self.is_valid,
            self.entry_index
        )?;
        if self.entry_type == ENTRY_TYPE_STANDBY_SNAPSHOT {
            write!(
                f,
                ", archiveEndpoint={}",
                self.archive_endpoint.as_deref().unwrap_or("null")
            )?;
        }
        f.write_str("}")
    }
}

fn type_as_str(entry_type: i32) -> &'static str {
    match entry_type {
        ENTRY_TYPE_TERM => "TERM",
        ENTRY_TYPE_SNAPSHOT => "SNAPSHOT",
        ENTRY_TYPE_STANDBY_SNAPSHOT => "STANDBY_SNAPSHOT",
        _ => "UNKNOWN",
    }
}

fn put_i64(bytes: &mut [u8], offset: usize, value: i64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn i64_at(bytes: &[u8], offset: usize) -> i64 {
    let mut b = [0; 8];
    let end = bytes.len().min(offset + 8);
    if offset < end {
        b[..end - offset].copy_from_slice(&bytes[offset..end]);
    }
    i64::from_le_bytes(b)
}

fn i32_at(bytes: &[u8], offset: usize) -> i32 {
    let mut b = [0; 4];
    let end = bytes.len().min(offset + 4);
    if offset < end {
        b[..end - offset].copy_from_slice(&bytes[offset..end]);
    }
    i32::from_le_bytes(b)
}

/// Java's `ENTRY_COMPARATOR`: by term, then term base; a term before its
/// snapshots, terms in file order; snapshots by position, a standby one
/// before a local one, services from the highest id down to the consensus
/// module's.
fn compare(e1: &Entry, e2: &Entry) -> Ordering {
    e1.leadership_term_id
        .cmp(&e2.leadership_term_id)
        .then(e1.term_base_log_position.cmp(&e2.term_base_log_position))
        .then_with(|| {
            if e1.entry_type != e2.entry_type {
                if e1.entry_type == ENTRY_TYPE_TERM {
                    return Ordering::Less;
                }
                if e2.entry_type == ENTRY_TYPE_TERM {
                    return Ordering::Greater;
                }
            } else if e1.entry_type == ENTRY_TYPE_TERM {
                return e1.entry_index.cmp(&e2.entry_index);
            }
            e1.log_position
                .cmp(&e2.log_position)
                .then(e2.entry_type.cmp(&e1.entry_type))
                .then(e2.service_id.cmp(&e1.service_id))
        })
}

/// A snapshot to recover from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub recording_id: i64,
    pub leadership_term_id: i64,
    pub term_base_log_position: i64,
    pub log_position: i64,
    pub timestamp: i64,
    pub service_id: i32,
}

/// What the archive says of a recording: its descriptor's extent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordingExtent {
    pub start_position: i64,
    pub stop_position: i64,
    pub initial_term_id: i32,
    pub term_buffer_length: i32,
    pub mtu_length: i32,
    pub session_id: i32,
}

/// The local log to replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Log {
    pub recording_id: i64,
    pub leadership_term_id: i64,
    pub term_base_log_position: i64,
    pub log_position: i64,
    pub start_position: i64,
    pub stop_position: i64,
    pub initial_term_id: i32,
    pub term_buffer_length: i32,
    pub mtu_length: i32,
    pub session_id: i32,
}

/// The snapshots and the log a consensus module recovers from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryPlan {
    pub last_leadership_term_id: i64,
    pub last_term_base_log_position: i64,
    pub appended_log_position: i64,
    pub committed_log_position: i64,
    /// The consensus module's first, then each service's by id.
    pub snapshots: Vec<Snapshot>,
    pub log: Option<Log>,
}

/// As Java's records print themselves: what ClusterTool's `recovery-plan` shows.
impl std::fmt::Display for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Snapshot[recordingId={}, leadershipTermId={}, termBaseLogPosition={}, logPosition={}, timestamp={}, \
             serviceId={}]",
            self.recording_id,
            self.leadership_term_id,
            self.term_base_log_position,
            self.log_position,
            self.timestamp,
            self.service_id
        )
    }
}

impl std::fmt::Display for Log {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Log[recordingId={}, leadershipTermId={}, termBaseLogPosition={}, logPosition={}, startPosition={}, \
             stopPosition={}, initialTermId={}, termBufferLength={}, mtuLength={}, sessionId={}]",
            self.recording_id,
            self.leadership_term_id,
            self.term_base_log_position,
            self.log_position,
            self.start_position,
            self.stop_position,
            self.initial_term_id,
            self.term_buffer_length,
            self.mtu_length,
            self.session_id
        )
    }
}

impl std::fmt::Display for RecoveryPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshots: Vec<String> = self.snapshots.iter().map(Snapshot::to_string).collect();
        write!(
            f,
            "RecoveryPlan[lastLeadershipTermId={}, lastTermBaseLogPosition={}, appendedLogPosition={}, \
             committedLogPosition={}, snapshots=[{}], log=",
            self.last_leadership_term_id,
            self.last_term_base_log_position,
            self.appended_log_position,
            self.committed_log_position,
            snapshots.join(", ")
        )?;
        match &self.log {
            Some(log) => write!(f, "{log}]"),
            None => f.write_str("null]"),
        }
    }
}

/// The log, as Java's `RecordingLog` holds it.
#[derive(Clone, Debug, Default)]
pub struct RecordingLog {
    entries: Vec<Entry>,
    /// A valid term's index in `entries`.
    term_index: BTreeMap<i64, usize>,
    /// The indexes of invalid snapshots, in the order Java keeps them.
    invalid_snapshots: Vec<usize>,
    next_entry_index: i32,
    term_recording_id: i64,
    file_length: u64,
    writes: Vec<(u64, Vec<u8>)>,
}

impl RecordingLog {
    /// A new, empty file's.
    pub fn new() -> Self {
        Self {
            term_recording_id: NULL_VALUE,
            ..Self::default()
        }
    }

    /// An existing file's, read as Java's `reload` reads it: a removed entry
    /// (type -1) skipped but counted, a standby entry cut short ending it.
    pub fn load(bytes: &[u8]) -> Result<Self, Error> {
        let mut log = Self::new();
        let mut consumed = 0;
        while consumed + ENDPOINT_OFFSET < bytes.len() {
            let raw_type = i32_at(bytes, consumed + ENTRY_TYPE_OFFSET);
            let entry_type = raw_type & !ENTRY_TYPE_INVALID_FLAG;
            let endpoint_offset = consumed + ENDPOINT_OFFSET;
            let archive_endpoint = if entry_type == ENTRY_TYPE_STANDBY_SNAPSHOT {
                let length = i32_at(bytes, endpoint_offset);
                let end = endpoint_offset + 4 + length.max(0) as usize;
                if endpoint_offset + 4 > bytes.len() || end > bytes.len() {
                    break;
                }
                Some(String::from_utf8_lossy(&bytes[endpoint_offset + 4..end]).into_owned())
            } else {
                None
            };
            let entry = Entry {
                recording_id: i64_at(bytes, consumed + RECORDING_ID_OFFSET),
                leadership_term_id: i64_at(bytes, consumed + LEADERSHIP_TERM_ID_OFFSET),
                term_base_log_position: i64_at(bytes, consumed + TERM_BASE_LOG_POSITION_OFFSET),
                log_position: i64_at(bytes, consumed + LOG_POSITION_OFFSET),
                timestamp: i64_at(bytes, consumed + TIMESTAMP_OFFSET),
                service_id: i32_at(bytes, consumed + SERVICE_ID_OFFSET),
                entry_type,
                is_valid: raw_type & ENTRY_TYPE_INVALID_FLAG == 0,
                entry_index: log.next_entry_index,
                archive_endpoint,
                position: consumed as u64,
            };
            let length = entry.length();
            if raw_type != NULL_VALUE as i32 {
                if entry_type == ENTRY_TYPE_TERM {
                    log.validate_term_recording_id(entry.recording_id)?;
                }
                log.entries.push(entry);
            }
            consumed += length;
            log.next_entry_index += 1;
        }
        log.file_length = bytes.len() as u64;
        log.entries.sort_by(compare);
        for (i, entry) in log.entries.iter().enumerate() {
            if entry.is_valid_term() {
                log.term_index.insert(entry.leadership_term_id, i);
            }
            if !entry.is_valid
                && (entry.entry_type == ENTRY_TYPE_SNAPSHOT || entry.entry_type == ENTRY_TYPE_STANDBY_SNAPSHOT)
            {
                log.invalid_snapshots.push(i);
            }
        }
        Ok(log)
    }

    /// The writes queued since the last call, in order: each an offset and
    /// the bytes to write there.
    pub fn take_writes(&mut self) -> Vec<(u64, Vec<u8>)> {
        std::mem::take(&mut self.writes)
    }

    /// The entries, sorted.
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// The index the next entry appended gets.
    pub fn next_entry_index(&self) -> i32 {
        self.next_entry_index
    }

    /// The log's recording, from its last term, or [`NULL_VALUE`].
    pub fn find_last_term_recording_id(&self) -> i64 {
        self.find_last_term().map_or(NULL_VALUE, |e| e.recording_id)
    }

    pub fn find_last_term(&self) -> Option<&Entry> {
        self.entries.iter().rev().find(|e| e.is_valid_term())
    }

    pub fn get_term_entry(&self, leadership_term_id: i64) -> Result<&Entry, Error> {
        self.find_term_entry(leadership_term_id)
            .ok_or_else(|| Error(format!("unknown leadershipTermId={leadership_term_id}")))
    }

    pub fn find_term_entry(&self, leadership_term_id: i64) -> Option<&Entry> {
        self.term_index.get(&leadership_term_id).map(|&i| &self.entries[i])
    }

    /// The latest valid snapshot of the consensus module ([`SERVICE_ID`]) or
    /// of a service, taken with the module's.
    pub fn get_latest_snapshot(&self, service_id: i32) -> Option<&Entry> {
        for (i, entry) in self.entries.iter().enumerate().rev() {
            if entry.is_valid_snapshot() && entry.service_id == SERVICE_ID {
                if service_id == SERVICE_ID {
                    return Some(entry);
                }
                let index = i as i64 - (i64::from(service_id) + 1);
                if index >= 0 {
                    let snapshot = &self.entries[index as usize];
                    if snapshot.is_valid_snapshot() && snapshot.service_id == service_id {
                        return Some(snapshot);
                    }
                }
            }
        }
        None
    }

    /// Invalidates the latest snapshots, the module's and its services', so
    /// a restart recovers from the ones before: whether there were any.
    pub fn invalidate_latest_snapshot(&mut self) -> Result<bool, Error> {
        let mut indices = Vec::new();
        let mut high_log_position = NULL_VALUE;
        for idx in (0..self.entries.len()).rev() {
            let entry = &self.entries[idx];
            if entry.is_valid_any_snapshot() && entry.service_id == SERVICE_ID {
                if entry.log_position >= high_log_position {
                    if !self.term_index.contains_key(&entry.leadership_term_id) {
                        return error(format!(
                            "no matching term for snapshot: leadershipTermId={}",
                            entry.leadership_term_id
                        ));
                    }
                    if entry.log_position > high_log_position {
                        indices.clear();
                        high_log_position = entry.log_position;
                    }
                    indices.push(idx);
                } else {
                    break;
                }
            }
        }
        let found = !indices.is_empty();
        while let Some(starting_index) = indices.pop() {
            let mut service_id = SERVICE_ID;
            for idx in (0..=starting_index).rev() {
                let entry = &self.entries[idx];
                if entry.is_valid_any_snapshot() && entry.service_id == service_id {
                    self.invalidate_entry(idx);
                    service_id += 1;
                } else {
                    break;
                }
            }
        }
        Ok(found)
    }

    /// A term's timestamp, or [`NULL_VALUE`].
    pub fn get_term_timestamp(&self, leadership_term_id: i64) -> i64 {
        self.find_term_entry(leadership_term_id)
            .map_or(NULL_VALUE, |e| e.timestamp)
    }

    /// Whether a term is not in the log yet.
    pub fn is_unknown(&self, leadership_term_id: i64) -> bool {
        !self.term_index.contains_key(&leadership_term_id)
    }

    /// The recording a recovery plan needs the archive to describe: the
    /// last term's, or the leader's being replicated into an empty log.
    pub fn plan_recording_id(&self, replicated_recording_id: i64) -> Option<i64> {
        if self.entries.is_empty() {
            return (replicated_recording_id != NULL_VALUE).then_some(replicated_recording_id);
        }
        self.plan_indexes().1.map(|i| self.entries[i].recording_id)
    }

    /// The latest valid module snapshot and latest valid term with a
    /// recording, as Java's `planRecovery` finds them.
    fn plan_indexes(&self) -> (Option<usize>, Option<usize>) {
        let (mut snapshot, mut log) = (None, None);
        for i in (0..self.entries.len()).rev() {
            let entry = &self.entries[i];
            if snapshot.is_none() && entry.is_valid_snapshot() && entry.service_id == SERVICE_ID {
                snapshot = Some(i);
            } else if log.is_none() && entry.is_valid_term() && entry.recording_id != NULL_VALUE {
                log = Some(i);
            } else if snapshot.is_some() && log.is_some() {
                break;
            }
        }
        (snapshot, log)
    }

    /// Java's `createRecoveryPlan`, the archive's answer for
    /// [`plan_recording_id`](Self::plan_recording_id) given: `None` when it
    /// knows no such recording.
    pub fn create_recovery_plan(
        &self,
        service_count: i32,
        replicated_recording_id: i64,
        extent: Option<RecordingExtent>,
    ) -> Result<RecoveryPlan, Error> {
        let mut snapshots = Vec::new();
        let mut log = None;
        if self.entries.is_empty() {
            if replicated_recording_id != NULL_VALUE {
                let Some(e) = extent else {
                    return error(format!("unknown recording id: {replicated_recording_id}"));
                };
                log = Some(Log {
                    recording_id: replicated_recording_id,
                    leadership_term_id: NULL_VALUE,
                    term_base_log_position: 0,
                    log_position: 0,
                    start_position: e.start_position,
                    stop_position: e.stop_position,
                    initial_term_id: e.initial_term_id,
                    term_buffer_length: e.term_buffer_length,
                    mtu_length: e.mtu_length,
                    session_id: e.session_id,
                });
            }
        } else {
            let (snapshot_index, log_index) = self.plan_indexes();
            if let Some(i) = snapshot_index {
                snapshots = self.add_snapshots(service_count, i)?;
            }
            if let Some(i) = log_index {
                let entry = &self.entries[i];
                let Some(e) = extent else {
                    return error(format!("unknown recording id: {}", entry.recording_id));
                };
                log = Some(Log {
                    recording_id: entry.recording_id,
                    leadership_term_id: entry.leadership_term_id,
                    term_base_log_position: entry.term_base_log_position,
                    log_position: entry.log_position,
                    start_position: if snapshot_index.is_none() {
                        e.start_position
                    } else {
                        snapshots[0].log_position
                    },
                    stop_position: e.stop_position,
                    initial_term_id: e.initial_term_id,
                    term_buffer_length: e.term_buffer_length,
                    mtu_length: e.mtu_length,
                    session_id: e.session_id,
                });
            }
        }

        let mut plan = RecoveryPlan {
            last_leadership_term_id: NULL_VALUE,
            last_term_base_log_position: 0,
            appended_log_position: 0,
            committed_log_position: 0,
            snapshots,
            log,
        };
        if let Some(snapshot) = plan.snapshots.first() {
            plan.last_leadership_term_id = snapshot.leadership_term_id;
            plan.last_term_base_log_position = snapshot.term_base_log_position;
            plan.appended_log_position = snapshot.log_position;
            plan.committed_log_position = snapshot.log_position;
        }
        if let Some(log) = plan.log {
            plan.last_leadership_term_id = log.leadership_term_id;
            plan.last_term_base_log_position = log.term_base_log_position;
            plan.appended_log_position = log.stop_position;
            if log.log_position != NULL_VALUE {
                plan.committed_log_position = log.log_position;
            }
        }
        Ok(plan)
    }

    /// Java's `addSnapshots`: the module's snapshot, then each service's
    /// below it at its id + 1, validity not checked.
    fn add_snapshots(&self, service_count: i32, snapshot_index: usize) -> Result<Vec<Snapshot>, Error> {
        let snapshot = &self.entries[snapshot_index];
        let mut snapshots = vec![snapshot.snapshot()];
        for i in 1..=service_count as usize {
            if snapshot_index < i {
                let entries: Vec<String> = self.entries.iter().map(Entry::to_string).collect();
                return error(format!(
                    "snapshot missing for service at index {i} in [{}]",
                    entries.join(", ")
                ));
            }
            let entry = &self.entries[snapshot_index - i];
            if entry.entry_type == ENTRY_TYPE_SNAPSHOT
                && entry.leadership_term_id == snapshot.leadership_term_id
                && entry.log_position == snapshot.log_position
            {
                let at = (entry.service_id + 1) as usize;
                if at > snapshots.len() {
                    return error(format!("Index: {at}, Size: {}", snapshots.len()));
                }
                snapshots.insert(at, entry.snapshot());
            }
        }
        Ok(snapshots)
    }

    /// Appends a term, committing the previous one at its base, and ending
    /// this one where a later term already starts.
    pub fn append_term(
        &mut self,
        recording_id: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
        timestamp: i64,
    ) -> Result<(), Error> {
        self.validate_term_recording_id(recording_id)?;
        let mut log_position = NULL_VALUE;
        if !self.entries.is_empty() {
            if self.term_index.contains_key(&leadership_term_id) {
                return error(format!(
                    "duplicate TERM entry for leadershipTermId={leadership_term_id}"
                ));
            }
            if self.term_index.contains_key(&(leadership_term_id - 1)) {
                self.commit_log_position(leadership_term_id - 1, term_base_log_position)?;
            }
            if let Some(next) = self.find_term_entry(leadership_term_id + 1) {
                log_position = next.term_base_log_position;
            }
        }
        let index = self.append(
            ENTRY_TYPE_TERM,
            recording_id,
            leadership_term_id,
            term_base_log_position,
            log_position,
            timestamp,
            NULL_VALUE as i32,
            None,
        );
        self.term_index.insert(leadership_term_id, index);
        Ok(())
    }

    /// Appends a snapshot, or makes valid again the invalid entry it matches.
    pub fn append_snapshot(
        &mut self,
        recording_id: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        timestamp: i64,
        service_id: i32,
    ) -> Result<(), Error> {
        validate_recording_id(recording_id)?;
        if !self.restore_invalid_snapshot(
            ENTRY_TYPE_SNAPSHOT,
            recording_id,
            leadership_term_id,
            term_base_log_position,
            log_position,
            timestamp,
            service_id,
            None,
        ) {
            self.append(
                ENTRY_TYPE_SNAPSHOT,
                recording_id,
                leadership_term_id,
                term_base_log_position,
                log_position,
                timestamp,
                service_id,
                None,
            );
        }
        Ok(())
    }

    /// Appends a snapshot a standby took, held by the archive at
    /// `archive_endpoint`, or makes valid again the invalid entry it matches.
    /// Java's restore builds that entry without its endpoint, and throws; this
    /// one keeps it.
    #[allow(clippy::too_many_arguments)]
    pub fn append_standby_snapshot(
        &mut self,
        recording_id: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        timestamp: i64,
        service_id: i32,
        archive_endpoint: &str,
    ) -> Result<(), Error> {
        validate_recording_id(recording_id)?;
        if archive_endpoint.is_empty() {
            return error("Remote snapshots must have a valid endpoint");
        }
        if archive_endpoint.len() > MAX_ENDPOINT_LENGTH {
            return error(format!(
                "Endpoint is too long: {} vs {MAX_ENDPOINT_LENGTH}",
                archive_endpoint.len()
            ));
        }
        if !self.restore_invalid_snapshot(
            ENTRY_TYPE_STANDBY_SNAPSHOT,
            recording_id,
            leadership_term_id,
            term_base_log_position,
            log_position,
            timestamp,
            service_id,
            Some(archive_endpoint),
        ) {
            self.append(
                ENTRY_TYPE_STANDBY_SNAPSHOT,
                recording_id,
                leadership_term_id,
                term_base_log_position,
                log_position,
                timestamp,
                service_id,
                Some(archive_endpoint),
            );
        }
        Ok(())
    }

    /// The newest complete set of standby snapshots each archive holds
    /// (Java's `latestStandbySnapshots`): per endpoint, the highest position
    /// with exactly one entry per service and the module's, listed from the
    /// file's end. Two sets at one position count as none there.
    pub fn latest_standby_snapshots(&self, service_count: usize) -> BTreeMap<String, Vec<Entry>> {
        let mut by_endpoint: BTreeMap<String, BTreeMap<i64, Vec<Entry>>> = BTreeMap::new();
        for entry in self.entries.iter().rev() {
            if entry.entry_type == ENTRY_TYPE_STANDBY_SNAPSHOT && entry.is_valid {
                let endpoint = entry.archive_endpoint.clone().unwrap_or_default();
                by_endpoint
                    .entry(endpoint)
                    .or_default()
                    .entry(entry.log_position)
                    .or_default()
                    .push(entry.clone());
            }
        }
        by_endpoint
            .into_iter()
            .filter_map(|(endpoint, by_position)| {
                by_position
                    .into_values()
                    .rev()
                    .find(|set| set.len() == service_count + 1)
                    .map(|set| (endpoint, set))
            })
            .collect()
    }

    /// The local snapshots a backup starts from (Java's
    /// `findSnapshotAtOrBeforeOrLowest`): the latest complete set at or before
    /// `log_position`, else the lowest complete set, else none; the services'
    /// first, the module's last.
    pub fn find_snapshot_at_or_before_or_lowest(&self, log_position: i64, service_count: usize) -> Vec<Snapshot> {
        let complete = |i: usize| self.is_complete_snapshot_set(i, service_count);
        let module = |e: &Entry| e.is_valid_snapshot() && e.service_id == SERVICE_ID;
        let found = (0..self.entries.len())
            .rev()
            .find(|&i| module(&self.entries[i]) && self.entries[i].log_position <= log_position && complete(i))
            .or_else(|| (0..self.entries.len()).find(|&i| module(&self.entries[i]) && complete(i)));
        found.map_or_else(Vec::new, |i| {
            self.entries[i - service_count..=i]
                .iter()
                .map(Entry::snapshot)
                .collect()
        })
    }

    /// Whether the `service_count` entries before the module's snapshot at
    /// `index` are its services' (Java's `validateSnapshotEntries`, which
    /// reads before the first entry when they are missing).
    fn is_complete_snapshot_set(&self, index: usize, service_count: usize) -> bool {
        if index < service_count {
            return false;
        }
        let module = &self.entries[index];
        (1..=service_count).all(|i| {
            let entry = &self.entries[index - i];
            entry.entry_type == ENTRY_TYPE_SNAPSHOT
                && entry.leadership_term_id == module.leadership_term_id
                && entry.log_position == module.log_position
        })
    }

    /// Where a term ends, written in place when it changes.
    pub fn commit_log_position(&mut self, leadership_term_id: i64, log_position: i64) -> Result<(), Error> {
        let Some(&index) = self.term_index.get(&leadership_term_id) else {
            return error(format!("unknown leadershipTermId={leadership_term_id}"));
        };
        let entry = &mut self.entries[index];
        if entry.log_position != log_position {
            self.writes.push((
                entry.position + LOG_POSITION_OFFSET as u64,
                log_position.to_le_bytes().to_vec(),
            ));
            entry.log_position = log_position;
        }
        Ok(())
    }

    /// Marks the entry at `index` in [`entries`](Self::entries) invalid.
    pub fn invalidate_entry(&mut self, index: usize) {
        let entry = &mut self.entries[index];
        entry.is_valid = false;
        let (entry_type, term, position) = (entry.entry_type, entry.leadership_term_id, entry.position);
        if entry_type == ENTRY_TYPE_TERM {
            self.term_index.remove(&term);
        } else if entry_type == ENTRY_TYPE_SNAPSHOT || entry_type == ENTRY_TYPE_STANDBY_SNAPSHOT {
            self.invalid_snapshots.push(index);
        }
        self.writes.push((
            position + ENTRY_TYPE_OFFSET as u64,
            (ENTRY_TYPE_INVALID_FLAG | entry_type).to_le_bytes().to_vec(),
        ));
    }

    /// Java's `ensureCoherent`: the terms from the log's first (or the last
    /// one known) up to `leadership_term_id` appended, the previous committed,
    /// and `log_position` committed when given. Java syncs the file after:
    /// the caller does.
    #[allow(clippy::too_many_arguments)]
    pub fn ensure_coherent(
        &mut self,
        recording_id: i64,
        initial_log_leadership_term_id: i64,
        initial_term_base_log_position: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        now_ns: i64,
        timestamp: i64,
    ) -> Result<(), Error> {
        let Some(mut last_term) = self.find_last_term().cloned() else {
            for term in initial_log_leadership_term_id.max(0)..leadership_term_id {
                self.append_term(recording_id, term, initial_term_base_log_position, timestamp)?;
            }
            self.append_term(recording_id, leadership_term_id, term_base_log_position, timestamp)?;
            if log_position != NULL_VALUE {
                self.commit_log_position(leadership_term_id, log_position)?;
            }
            return Ok(());
        };
        if last_term.leadership_term_id < leadership_term_id {
            if last_term.log_position == NULL_VALUE {
                if term_base_log_position == NULL_VALUE {
                    return error(format!(
                        "Prior term was not committed: {last_term} and logTermBasePosition was not specified: \
                         leadershipTermId = {leadership_term_id}, logTermBasePosition = {term_base_log_position}, \
                         logPosition = {log_position}, nowNs = {now_ns}"
                    ));
                }
                self.commit_log_position(last_term.leadership_term_id, term_base_log_position)?;
                last_term = self.find_last_term().cloned().expect("the term just committed");
            }
            for term in last_term.leadership_term_id + 1..leadership_term_id {
                self.append_term(recording_id, term, last_term.log_position, timestamp)?;
            }
            self.append_term(recording_id, leadership_term_id, last_term.log_position, timestamp)?;
        }
        if log_position != NULL_VALUE {
            self.commit_log_position(leadership_term_id, log_position)?;
        }
        Ok(())
    }

    fn validate_term_recording_id(&mut self, recording_id: i64) -> Result<(), Error> {
        validate_recording_id(recording_id)?;
        if recording_id != self.term_recording_id {
            // With every term invalidated, the log's recording is free to be
            // another: Java never invalidates them all.
            if self.term_recording_id != NULL_VALUE && self.find_last_term().is_some() {
                return error(format!(
                    "invalid TERM recordingId={recording_id}, expected recordingId={}",
                    self.term_recording_id
                ));
            }
            self.term_recording_id = recording_id;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn restore_invalid_snapshot(
        &mut self,
        entry_type: i32,
        recording_id: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        timestamp: i64,
        service_id: i32,
        archive_endpoint: Option<&str>,
    ) -> bool {
        for i in (0..self.invalid_snapshots.len()).rev() {
            let index = self.invalid_snapshots[i];
            let entry = &self.entries[index];
            if entry.entry_type == entry_type
                && entry.leadership_term_id == leadership_term_id
                && entry.term_base_log_position == term_base_log_position
                && entry.log_position == log_position
                && entry.service_id == service_id
            {
                let restored = Entry {
                    recording_id,
                    leadership_term_id,
                    term_base_log_position,
                    log_position,
                    timestamp,
                    service_id,
                    entry_type,
                    is_valid: true,
                    entry_index: entry.entry_index,
                    archive_endpoint: archive_endpoint.map(str::to_string),
                    position: entry.position,
                };
                self.writes.push((restored.position, restored.encode()));
                self.entries[index] = restored;
                self.invalid_snapshots.swap_remove(i);
                return true;
            }
        }
        false
    }

    /// Writes a new entry at the end of the file and puts it in order: a
    /// later term's index moves up, as do the invalid snapshots' Java finds
    /// above it, scanning down.
    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        entry_type: i32,
        recording_id: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        timestamp: i64,
        service_id: i32,
        archive_endpoint: Option<&str>,
    ) -> usize {
        let entry = Entry {
            recording_id,
            leadership_term_id,
            term_base_log_position,
            log_position,
            timestamp,
            service_id,
            entry_type,
            is_valid: true,
            entry_index: self.next_entry_index,
            archive_endpoint: archive_endpoint.map(str::to_string),
            position: self.file_length.next_multiple_of(RECORD_ALIGNMENT as u64),
        };
        let bytes = entry.encode();
        self.file_length = entry.position + bytes.len() as u64;
        self.writes.push((entry.position, bytes));
        self.next_entry_index += 1;

        let size = self.entries.len();
        let mut index = size;
        for e in self.entries.iter().rev() {
            if compare(&entry, e) == Ordering::Less {
                index -= 1;
            } else {
                break;
            }
        }
        self.entries.insert(index, entry);
        if index < size {
            for (&term, i) in self.term_index.iter_mut() {
                if term > leadership_term_id {
                    *i += 1;
                }
            }
            for i in self.invalid_snapshots.iter_mut().rev() {
                if *i >= index {
                    *i += 1;
                } else {
                    break;
                }
            }
        }
        index
    }
}

fn validate_recording_id(recording_id: i64) -> Result<(), Error> {
    if recording_id == NULL_VALUE {
        return error(format!("invalid recordingId={recording_id}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The file the queued writes make.
    fn apply(file: &mut Vec<u8>, log: &mut RecordingLog) {
        for (offset, bytes) in log.take_writes() {
            let end = offset as usize + bytes.len();
            if file.len() < end {
                file.resize(end, 0);
            }
            file[offset as usize..end].copy_from_slice(&bytes);
        }
    }

    #[test]
    fn terms_commit_the_previous_and_reload_alike() {
        let mut file = Vec::new();
        let mut log = RecordingLog::new();
        log.append_term(7, 0, 0, 100).unwrap();
        log.append_term(7, 1, 4_096, 200).unwrap();
        log.append_snapshot(8, 1, 4_096, 6_144, 250, 0).unwrap();
        log.append_snapshot(9, 1, 4_096, 6_144, 250, SERVICE_ID).unwrap();
        apply(&mut file, &mut log);
        assert_eq!(file.len(), 4 * 64);
        assert_eq!(log.get_term_entry(0).unwrap().log_position, 4_096);
        assert_eq!(log.find_last_term_recording_id(), 7);
        assert_eq!(log.get_latest_snapshot(0).unwrap().recording_id, 8);
        assert_eq!(log.get_latest_snapshot(SERVICE_ID).unwrap().recording_id, 9);

        let reloaded = RecordingLog::load(&file).unwrap();
        assert_eq!(reloaded.entries(), log.entries());
        assert_eq!(reloaded.next_entry_index(), 4);
        assert!(log.append_term(8, 2, 8_192, 300).is_err(), "one recording for the log");
        assert!(log.append_term(7, 1, 4_096, 300).is_err(), "no duplicate term");
    }

    #[test]
    fn an_invalidated_snapshot_is_restored_in_place() {
        let mut file = Vec::new();
        let mut log = RecordingLog::new();
        log.append_term(7, 0, 0, 100).unwrap();
        log.append_snapshot(8, 0, 0, 1_024, 150, 0).unwrap();
        log.append_snapshot(9, 0, 0, 1_024, 150, SERVICE_ID).unwrap();
        assert!(log.invalidate_latest_snapshot().unwrap());
        assert!(log.get_latest_snapshot(SERVICE_ID).is_none());
        apply(&mut file, &mut log);
        assert_eq!(
            i32_at(&file, 64 + ENTRY_TYPE_OFFSET),
            ENTRY_TYPE_INVALID_FLAG | ENTRY_TYPE_SNAPSHOT
        );

        log.append_snapshot(10, 0, 0, 1_024, 160, SERVICE_ID).unwrap();
        apply(&mut file, &mut log);
        assert_eq!(file.len(), 3 * 64, "restored where it was");
        let reloaded = RecordingLog::load(&file).unwrap();
        assert_eq!(reloaded.get_latest_snapshot(SERVICE_ID).unwrap().recording_id, 10);
        assert!(
            reloaded.get_latest_snapshot(0).is_none(),
            "the service's is still invalid"
        );
    }

    #[test]
    fn a_plan_recovers_from_the_latest_snapshot_and_the_log() {
        let mut log = RecordingLog::new();
        log.append_term(7, 0, 0, 100).unwrap();
        log.append_snapshot(8, 0, 0, 1_024, 150, 0).unwrap();
        log.append_snapshot(9, 0, 0, 1_024, 150, SERVICE_ID).unwrap();
        assert_eq!(log.plan_recording_id(NULL_VALUE), Some(7));
        let extent = RecordingExtent {
            start_position: 0,
            stop_position: 2_048,
            initial_term_id: 3,
            term_buffer_length: 65_536,
            mtu_length: 1_408,
            session_id: -5,
        };
        let plan = log.create_recovery_plan(1, NULL_VALUE, Some(extent)).unwrap();
        assert_eq!(
            plan.snapshots.iter().map(|s| s.recording_id).collect::<Vec<_>>(),
            [9, 8]
        );
        assert_eq!(plan.log.unwrap().start_position, 1_024);
        assert_eq!(plan.appended_log_position, 2_048);
        assert_eq!(plan.committed_log_position, 1_024);
        assert!(log.create_recovery_plan(1, NULL_VALUE, None).is_err());

        let empty = RecordingLog::new();
        assert_eq!(empty.plan_recording_id(NULL_VALUE), None);
        let plan = empty.create_recovery_plan(1, NULL_VALUE, None).unwrap();
        assert_eq!((plan.last_leadership_term_id, plan.log), (NULL_VALUE, None));
    }

    #[test]
    fn ensure_coherent_fills_in_the_missing_terms() {
        let mut log = RecordingLog::new();
        log.ensure_coherent(7, NULL_VALUE, 0, 2, 0, NULL_VALUE, 1, 100).unwrap();
        let terms: Vec<_> = log
            .entries()
            .iter()
            .map(|e| (e.leadership_term_id, e.log_position))
            .collect();
        assert_eq!(terms, [(0, 0), (1, 0), (2, NULL_VALUE)]);
        assert!(
            log.ensure_coherent(7, NULL_VALUE, 0, 3, NULL_VALUE, NULL_VALUE, 1, 100)
                .is_err(),
            "term 2 not committed, and no base given"
        );
        log.ensure_coherent(7, NULL_VALUE, 0, 4, 2_048, 3_072, 1, 100).unwrap();
        let terms: Vec<_> = log
            .entries()
            .iter()
            .map(|e| (e.leadership_term_id, e.log_position))
            .collect();
        assert_eq!(terms, [(0, 0), (1, 0), (2, 2_048), (3, 2_048), (4, 3_072)]);
    }

    #[test]
    fn the_entry_prints_as_java_prints_it() {
        let mut log = RecordingLog::new();
        log.append_term(7, 0, 0, 100).unwrap();
        assert_eq!(
            log.entries()[0].to_string(),
            "Entry{recordingId=7, leadershipTermId=0, termBaseLogPosition=0, logPosition=-1, timestamp=100, \
             serviceId=-1, type=TERM, isValid=true, entryIndex=0}"
        );
    }

    /// Java's `RecordingLogTest.shouldInsertStandbySnapshotInRecordingLog`
    /// and `shouldHandleEntriesStraddlingPageBoundary`: standby entries keep
    /// their endpoint through a reload, a long one taking several records.
    #[test]
    fn standby_snapshots_reload_with_their_endpoint() {
        let long_endpoint = format!("{}x", "a".repeat(3_078));
        for endpoint in ["remotehost.aeron.io:20002", long_endpoint.as_str()] {
            let mut file = Vec::new();
            let mut log = RecordingLog::new();
            log.append_snapshot(1, 1, 0, 1_000, 1_000_000_000, SERVICE_ID).unwrap();
            log.append_snapshot(2, 1, 0, 1_000, 1_000_000_000, 0).unwrap();
            log.append_standby_snapshot(3, 2, 1_000, 2_000, 1_000_000_000, SERVICE_ID, endpoint)
                .unwrap();
            log.append_standby_snapshot(4, 2, 1_000, 2_000, 1_000_000_000, 0, endpoint)
                .unwrap();
            log.append_snapshot(5, 3, 2_000, 3_000, 1_000_000_000, SERVICE_ID)
                .unwrap();
            log.append_snapshot(6, 3, 2_000, 3_000, 1_000_000_000, 0).unwrap();
            apply(&mut file, &mut log);

            let reloaded = RecordingLog::load(&file).unwrap();
            assert_eq!(reloaded.entries(), log.entries());
            assert_eq!(reloaded.next_entry_index(), 6);
            for entry in reloaded.entries() {
                let standby = entry.recording_id == 3 || entry.recording_id == 4;
                assert_eq!(entry.entry_type == ENTRY_TYPE_STANDBY_SNAPSHOT, standby);
                assert_eq!(entry.archive_endpoint.as_deref(), standby.then_some(endpoint));
            }
            let plan = reloaded.create_recovery_plan(1, NULL_VALUE, None).unwrap();
            let mut recordings: Vec<i64> = plan.snapshots.iter().map(|s| s.recording_id).collect();
            recordings.sort_unstable();
            assert_eq!(recordings, [5, 6], "a plan ignores standby entries");
            assert_eq!(reloaded.get_latest_snapshot(SERVICE_ID).unwrap().recording_id, 5);
        }
    }

    /// Java's `shouldRejectSnapshotEntryIfEndpointIsTooLong`, and the other
    /// refusals of `appendStandbySnapshot`.
    #[test]
    fn a_standby_snapshot_needs_a_recording_and_an_endpoint_that_fits() {
        let mut log = RecordingLog::new();
        let endpoint = format!("{}x", "a".repeat(4_999));
        assert_eq!(
            log.append_standby_snapshot(1, 2, 1_000, 2_000, 0, SERVICE_ID, &endpoint),
            error(format!("Endpoint is too long: 5000 vs {MAX_ENDPOINT_LENGTH}"))
        );
        assert!(
            log.append_standby_snapshot(1, 2, 1_000, 2_000, 0, SERVICE_ID, "")
                .is_err()
        );
        assert!(
            log.append_standby_snapshot(NULL_VALUE, 2, 1_000, 2_000, 0, SERVICE_ID, "h:1")
                .is_err()
        );
        let fits = "a".repeat(MAX_ENDPOINT_LENGTH);
        log.append_standby_snapshot(1, 2, 1_000, 2_000, 0, SERVICE_ID, &fits)
            .unwrap();
        assert_eq!(log.entries()[0].length(), MAX_ENTRY_LENGTH);
    }

    /// Java's `shouldGetLatestStandbySnapshotsGroupedByEndpoint`, with a
    /// position holding two sets, which counts as none.
    #[test]
    fn the_latest_standby_snapshots_are_each_archives_newest_complete_set() {
        let (h0, h1) = ("remotehost0.aeron.io:20002", "remotehost1.aeron.io:20002");
        let mut log = RecordingLog::new();
        log.append_snapshot(1, 1, 0, 1_000, 1_000_000_000, SERVICE_ID).unwrap();
        log.append_snapshot(2, 1, 0, 1_000, 1_000_000_000, 0).unwrap();
        log.append_standby_snapshot(5, 2, 500, 800, 1_000_000_000, SERVICE_ID, h0)
            .unwrap();
        log.append_standby_snapshot(6, 2, 500, 800, 1_000_000_000, 0, h0)
            .unwrap();
        log.append_standby_snapshot(3, 2, 1_000, 2_000, 1_000_000_000, SERVICE_ID, h0)
            .unwrap();
        log.append_standby_snapshot(4, 2, 1_000, 2_000, 1_000_000_000, 0, h0)
            .unwrap();
        log.append_standby_snapshot(3, 2, 1_000, 2_000, 1_000_000_000, SERVICE_ID, h1)
            .unwrap();
        log.append_standby_snapshot(4, 2, 1_000, 2_000, 1_000_000_000, 0, h1)
            .unwrap();
        log.append_standby_snapshot(10, 2, 3_000, 4_000, 1_000_000_000, 0, h0)
            .unwrap();
        log.append_standby_snapshot(11, 2, 3_000, 4_000, 1_000_000_000, 0, h1)
            .unwrap();

        let latest = log.latest_standby_snapshots(1);
        assert_eq!(latest.len(), 2);
        for endpoint in [h0, h1] {
            let set: Vec<(i64, i32)> = latest[endpoint]
                .iter()
                .map(|e| (e.recording_id, e.service_id))
                .collect();
            assert_eq!(set, [(3, SERVICE_ID), (4, 0)], "{endpoint}");
        }

        log.append_standby_snapshot(12, 2, 1_000, 2_000, 1_000_000_000, SERVICE_ID, h1)
            .unwrap();
        log.append_standby_snapshot(13, 2, 1_000, 2_000, 1_000_000_000, 0, h1)
            .unwrap();
        assert!(
            !log.latest_standby_snapshots(1).contains_key(h1),
            "two sets at one position"
        );
        assert_eq!(log.latest_standby_snapshots(1)[h0][0].log_position, 2_000);
    }

    /// Java's `shouldInvalidateLatestSnapshotIgnoringStandbySnapshots`.
    #[test]
    fn invalidating_the_latest_snapshot_leaves_an_older_standby_set() {
        let h0 = "remotehost0.aeron.io:20002";
        let mut log = RecordingLog::new();
        log.append_term(1, 0, 0, 1_000_000_000).unwrap();
        log.append_snapshot(2, 0, 0, 1_000, 1_000_000_000, SERVICE_ID).unwrap();
        log.append_snapshot(3, 0, 0, 1_000, 1_000_000_000, 0).unwrap();
        log.append_snapshot(4, 0, 2, 2_000, 1_000_000_000, SERVICE_ID).unwrap();
        log.append_snapshot(5, 0, 2, 2_000, 1_000_000_000, 0).unwrap();
        log.append_standby_snapshot(15, 0, 500, 800, 1_000_000_000, SERVICE_ID, h0)
            .unwrap();
        log.append_standby_snapshot(16, 0, 500, 800, 1_000_000_000, 0, h0)
            .unwrap();
        assert_eq!(log.get_latest_snapshot(SERVICE_ID).unwrap().recording_id, 4);
        let before = log.latest_standby_snapshots(1);

        assert!(log.invalidate_latest_snapshot().unwrap());
        assert_eq!(log.get_latest_snapshot(SERVICE_ID).unwrap().recording_id, 2);
        assert_eq!(log.latest_standby_snapshots(1), before);
    }

    /// An invalidated standby entry restored with its endpoint, which Java's
    /// restore drops, and throws.
    #[test]
    fn an_invalidated_standby_snapshot_is_restored_with_its_endpoint() {
        let h0 = "remotehost0.aeron.io:20002";
        let mut file = Vec::new();
        let mut log = RecordingLog::new();
        log.append_term(1, 0, 0, 1_000_000_000).unwrap();
        log.append_standby_snapshot(15, 0, 500, 800, 1_000_000_000, 0, h0)
            .unwrap();
        log.append_standby_snapshot(16, 0, 500, 800, 1_000_000_000, SERVICE_ID, h0)
            .unwrap();
        assert!(log.invalidate_latest_snapshot().unwrap());
        assert!(log.latest_standby_snapshots(1).is_empty());
        log.append_standby_snapshot(17, 0, 500, 800, 1_000_000_000, SERVICE_ID, h0)
            .unwrap();
        log.append_standby_snapshot(18, 0, 500, 800, 1_000_000_000, 0, h0)
            .unwrap();
        apply(&mut file, &mut log);
        let reloaded = RecordingLog::load(&file).unwrap();
        assert_eq!(reloaded.next_entry_index(), 3, "restored in place");
        let set = &reloaded.latest_standby_snapshots(1)[h0];
        assert_eq!(set.iter().map(|e| e.recording_id).collect::<Vec<_>>(), [17, 18]);
    }

    /// Java's `shouldFindSnapshotAtOrBeforeOrLowest`: local sets only, a set
    /// missing a service skipped, the lowest when none is at or before.
    #[test]
    fn a_backup_starts_from_the_latest_complete_set_at_or_before_its_position() {
        let mut log = RecordingLog::new();
        let mut recording_id = 0;
        let mut next = || {
            recording_id += 1;
            recording_id
        };
        for service_id in [0, 1, SERVICE_ID] {
            log.append_snapshot(next(), 1, 500, 777, 0, service_id).unwrap();
        }
        for service_id in [0, 1, SERVICE_ID] {
            log.append_standby_snapshot(next(), 1, 500, 888, 0, service_id, "localhost:8080")
                .unwrap();
        }
        for service_id in [0, 1, SERVICE_ID] {
            log.append_snapshot(next(), 1, 500, 888, 0, service_id).unwrap();
        }
        for service_id in [0, SERVICE_ID] {
            log.append_snapshot(next(), 1, 500, 890, 0, service_id).unwrap();
        }
        for service_id in [0, 1, SERVICE_ID] {
            log.append_standby_snapshot(next(), 1, 500, 990, 0, service_id, "localhost:8080")
                .unwrap();
        }
        for service_id in [0, 1, SERVICE_ID] {
            log.append_snapshot(next(), 1, 500, 999, 0, service_id).unwrap();
        }
        for (asked, found) in [
            (777, 777),
            (888, 888),
            (999, 999),
            (890, 888),
            (990, 888),
            (750, 777),
            (1_000, 999),
        ] {
            let set = log.find_snapshot_at_or_before_or_lowest(asked, 2);
            assert_eq!(set.len(), 3, "at {asked}");
            assert!(set.iter().all(|s| s.log_position == found), "at {asked}: {set:?}");
            let services: Vec<i32> = set.iter().map(|s| s.service_id).collect();
            assert_eq!(services, [1, 0, SERVICE_ID]);
        }
        assert!(
            RecordingLog::new()
                .find_snapshot_at_or_before_or_lowest(i64::MAX, 1)
                .is_empty()
        );
        let mut lone = RecordingLog::new();
        lone.append_snapshot(1, 1, 0, 64, 0, SERVICE_ID).unwrap();
        assert!(
            lone.find_snapshot_at_or_before_or_lowest(i64::MAX, 1).is_empty(),
            "no service's before it"
        );
    }
}
