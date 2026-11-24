//! The standby's own list of its snapshots (`StandbyModule.Snapshot`): a
//! line each, `logPosition term termBase timestamp serviceRecording
//! moduleRecording`, appended and synced before the members hear of the
//! set, so a standby restarting starts from its newest, not before it. It
//! keeps its newest few, written again without the older ones, whose
//! recordings then go; and of the members' sets its backup copied, only the
//! newest is kept ([`superseded_snapshots`]). What nothing lists, which a
//! kill or a copy given up leaves, is found at the standby's start
//! ([`unlisted`]).

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

use rusteron_cluster_core::recording_log::{ENTRY_TYPE_SNAPSHOT, Entry};

/// A standby snapshot: where in the log, which term and its base, when, and
/// the service's and the module's recordings in the standby's archive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub log_position: i64,
    pub leadership_term_id: i64,
    pub term_base_log_position: i64,
    pub timestamp: i64,
    pub service_recording_id: i64,
    pub module_recording_id: i64,
}

impl Snapshot {
    /// The newest snapshot listed in `file`, or none: no file, or no whole line.
    pub fn latest(file: &Path) -> io::Result<Option<Self>> {
        Ok(Self::all(file)?.pop())
    }

    /// The snapshots listed in `file`, oldest first: none with no file, a line not whole passed over.
    pub fn all(file: &Path) -> io::Result<Vec<Self>> {
        let text = match std::fs::read_to_string(file) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        Ok(text.lines().filter_map(Self::parse).collect())
    }

    /// Keeps the newest `keep` snapshots listed in `file`, all of them if 0:
    /// the file written again with those only, synced, and moved over the old
    /// one. The snapshots no longer listed, oldest first, whose recordings may
    /// then go.
    pub fn keep_newest(file: &Path, keep: usize) -> io::Result<Vec<Self>> {
        let mut all = Self::all(file)?;
        if keep == 0 || all.len() <= keep {
            return Ok(Vec::new());
        }
        let kept = all.split_off(all.len() - keep);
        let mut next_name = file.file_name().unwrap_or_default().to_os_string();
        next_name.push(".next");
        let next = file.with_file_name(next_name);
        {
            let mut f = File::create(&next)?;
            for snapshot in &kept {
                f.write_all(snapshot.line().as_bytes())?;
            }
            f.sync_all()?;
        }
        std::fs::rename(&next, file)?;
        // The move synced too: else a power cut could bring the old list back, the sets appended since lost.
        if let Some(directory) = std::path::absolute(file)?.parent()
            && let Ok(directory) = File::open(directory)
        {
            let _ = directory.sync_all();
        }
        Ok(all)
    }

    fn parse(line: &str) -> Option<Self> {
        let f: Vec<i64> = line.split_whitespace().map(str::parse).collect::<Result<_, _>>().ok()?;
        let [
            log_position,
            leadership_term_id,
            term_base_log_position,
            timestamp,
            service,
            module,
        ] = f[..]
        else {
            return None;
        };
        Some(Self {
            log_position,
            leadership_term_id,
            term_base_log_position,
            timestamp,
            service_recording_id: service,
            module_recording_id: module,
        })
    }

    fn line(&self) -> String {
        format!(
            "{} {} {} {} {} {}\n",
            self.log_position,
            self.leadership_term_id,
            self.term_base_log_position,
            self.timestamp,
            self.service_recording_id,
            self.module_recording_id
        )
    }

    /// Lists this snapshot in `file`, synced.
    pub fn append(&self, file: &Path) -> io::Result<()> {
        let mut f = OpenOptions::new().create(true).append(true).open(file)?;
        f.write_all(self.line().as_bytes())?;
        f.sync_all()
    }
}

/// The recordings of the snapshots a recording log lists before its newest
/// the members' sets a backup copied that a
/// newer one replaced. The standby restarts from its own sets, and the backup
/// compares only the newest's position with the members'.
pub fn superseded_snapshots(entries: &[Entry]) -> Vec<i64> {
    let snapshots = || {
        entries
            .iter()
            .filter(|e| e.entry_type == ENTRY_TYPE_SNAPSHOT && e.is_valid)
    };
    let Some(newest) = snapshots().map(|e| e.log_position).max() else {
        return Vec::new();
    };
    snapshots()
        .filter(|e| e.log_position < newest)
        .map(|e| e.recording_id)
        .collect()
}

/// A recording in the standby's archive, as its start looks at it: its id,
/// its stream, and where it stopped (-1 while it is still recorded).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Recording {
    pub recording_id: i64,
    pub stream_id: i32,
    pub stop_position: i64,
}

/// Of `recordings`, the snapshot recordings nothing lists, for the module to
/// delete: stopped, on one of `snapshot_streams`, and
/// in neither the standby's own `sets`, nor its backup's newest copy of the
/// members' in `backup_log` (a copy before that is superseded), nor `kept`.
/// A standby killed while it wrote a set, before it listed it, leaves one;
/// so does a backup that gave up a copy when its source changed, and a kill
/// between a set's drop and its deletion. Looked for at the standby's
/// start, before its backup and its service: nothing then records a
/// snapshot that will be listed.
pub fn unlisted(
    recordings: &[Recording],
    sets: &[Snapshot],
    backup_log: &[Entry],
    kept: &[i64],
    snapshot_streams: [i32; 2],
) -> Vec<i64> {
    let superseded: HashSet<i64> = superseded_snapshots(backup_log).into_iter().collect();
    let listed: HashSet<i64> = sets
        .iter()
        .flat_map(|s| [s.service_recording_id, s.module_recording_id])
        .chain(
            backup_log
                .iter()
                .filter(|e| e.entry_type == ENTRY_TYPE_SNAPSHOT && e.is_valid && !superseded.contains(&e.recording_id))
                .map(|e| e.recording_id),
        )
        .chain(kept.iter().copied())
        .collect();
    recordings
        .iter()
        .filter(|r| {
            snapshot_streams.contains(&r.stream_id) && r.stop_position != -1 && !listed.contains(&r.recording_id)
        })
        .map(|r| r.recording_id)
        .collect()
}
