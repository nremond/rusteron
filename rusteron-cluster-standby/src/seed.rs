//! A new member's recording log from what a standby holds: the
//! disaster-recovery runbook starts a new cluster from the standby's
//! archive, each new member given copies of the standby's log recording and
//! of its newest snapshot set (`standby_seed`).
//!
//! A member starts from its recording log: the last term's log recording in
//! its own archive, and the newest snapshot set, its service's entry then its
//! module's. It loads the set, replays the log after it to the recording's
//! end, then elects, and the next term extends that recording: the layout a
//! cluster backup's recording log has, which the standby's backup keeps
//! with its terms. The standby's own sets are listed in `standby-snapshots`
//! instead, and the backup's file still names copies of the members' sets
//! since deleted: neither is carried over, but the newest set, in its
//! copies.

use rusteron_cluster_core::recording_log::{ENTRY_TYPE_TERM, Entry, RecordingLog, SERVICE_ID};

use crate::snapshots::Snapshot;

/// The service whose snapshot a standby set holds: the standby runs one.
pub const SERVICE: i32 = 0;

/// A standby set and the recordings it was copied to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CopiedSet {
    pub set: Snapshot,
    pub service_recording_id: i64,
    pub module_recording_id: i64,
}

/// The terms of a standby's recording log, by term: the valid ones.
fn terms(standby_log: &[Entry]) -> Vec<&Entry> {
    let mut terms: Vec<&Entry> = standby_log
        .iter()
        .filter(|e| e.entry_type == ENTRY_TYPE_TERM && e.is_valid)
        .collect();
    terms.sort_by_key(|e| e.leadership_term_id);
    terms
}

/// The standby's log recording, in its archive: its last term's.
pub fn log_recording_id(standby_log: &[Entry]) -> Option<i64> {
    terms(standby_log).last().map(|e| e.recording_id)
}

/// A new member's recording log: the standby's terms, their log in
/// `log_copy`, then the set copied if any. With none, the member replays the
/// whole log. Every term must be in the one recording the last is: a standby
/// keeps its log in one, which its restarts extend.
pub fn member_log(standby_log: &[Entry], log_copy: i64, set: Option<CopiedSet>) -> Result<RecordingLog, String> {
    let terms = terms(standby_log);
    let Some(last) = terms.last() else {
        return Err("the standby's recording log has no term: it has recorded no log".into());
    };
    let mut log = RecordingLog::new();
    for term in &terms {
        if term.recording_id != last.recording_id {
            return Err(format!(
                "term {} is in recording {}, the log's last term in {}: the standby's log is in more than one recording",
                term.leadership_term_id, term.recording_id, last.recording_id
            ));
        }
        log.append_term(
            log_copy,
            term.leadership_term_id,
            term.term_base_log_position,
            term.timestamp,
        )
        .map_err(|e| e.to_string())?;
    }
    if let Some(copied) = set {
        let s = copied.set;
        // A term the backup never filed, begun and ended between two of its queries: the set's needs one.
        if log.is_unknown(s.leadership_term_id) {
            log.append_term(log_copy, s.leadership_term_id, s.term_base_log_position, s.timestamp)
                .map_err(|e| e.to_string())?;
        }
        for (recording_id, service_id) in [
            (copied.service_recording_id, SERVICE),
            (copied.module_recording_id, SERVICE_ID),
        ] {
            log.append_snapshot(
                recording_id,
                s.leadership_term_id,
                s.term_base_log_position,
                s.log_position,
                s.timestamp,
                service_id,
            )
            .map_err(|e| e.to_string())?;
        }
    }
    Ok(log)
}
