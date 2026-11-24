//! A new member's recording log from a standby's,
//! checked as the member's module reads it at its start: the recovery plan
//! loads the set copied, then replays the copied log to its end.

use rusteron_cluster_core::recording_log::{self, Entry, RecordingExtent, RecordingLog, SERVICE_ID};
use rusteron_cluster_standby::seed::{CopiedSet, SERVICE, log_recording_id, member_log};
use rusteron_cluster_standby::snapshots::Snapshot;

/// The standby's log, in its archive's recording 7.
const STANDBY_LOG: i64 = 7;
/// Where the copies went in the new member's archive.
const LOG_COPY: i64 = 0;
const SERVICE_COPY: i64 = 1;
const MODULE_COPY: i64 = 2;
/// Where the standby's log, so its copy, ends.
const LOG_END: i64 = 9_000;

/// A standby backup's recording log: terms 0, 1 and 2 of its one log
/// recording, and a members' set it copied in term 1, since deleted.
fn standby_log(terms: &[(i64, i64)]) -> Vec<Entry> {
    let mut log = RecordingLog::new();
    for &(term, base) in terms {
        log.append_term(STANDBY_LOG, term, base, 10 + term).unwrap();
    }
    for service_id in [SERVICE, SERVICE_ID] {
        log.append_snapshot(3, 1, 1_000, 2_000, 21, service_id).unwrap();
    }
    log.entries().to_vec()
}

fn set(term: i64, term_base: i64, log_position: i64) -> CopiedSet {
    CopiedSet {
        set: Snapshot {
            log_position,
            leadership_term_id: term,
            term_base_log_position: term_base,
            timestamp: 31,
            service_recording_id: 40,
            module_recording_id: 41,
        },
        service_recording_id: SERVICE_COPY,
        module_recording_id: MODULE_COPY,
    }
}

/// The member's recovery plan, the archive describing the log's copy.
fn plan(log: &RecordingLog) -> recording_log::RecoveryPlan {
    let extent = RecordingExtent {
        start_position: 0,
        stop_position: LOG_END,
        initial_term_id: 5,
        term_buffer_length: 64 << 20,
        mtu_length: 1_408,
        session_id: 9,
    };
    assert_eq!(log.plan_recording_id(-1), Some(LOG_COPY));
    log.create_recovery_plan(1, -1, Some(extent)).expect("a recovery plan")
}

#[test]
fn a_member_seeded_with_the_newest_set_loads_it_then_replays_the_copied_log() {
    let standby = standby_log(&[(0, 0), (1, 1_000), (2, 5_000)]);
    assert_eq!(log_recording_id(&standby), Some(STANDBY_LOG));
    let member = member_log(&standby, LOG_COPY, Some(set(2, 5_000, 6_000))).unwrap();

    let plan = plan(&member);
    let mut snapshots: Vec<(i64, i32, i64, i64, i64)> = plan
        .snapshots
        .iter()
        .map(|s| {
            (
                s.recording_id,
                s.service_id,
                s.leadership_term_id,
                s.term_base_log_position,
                s.log_position,
            )
        })
        .collect();
    snapshots.sort();
    assert_eq!(
        snapshots,
        [
            (SERVICE_COPY, SERVICE, 2, 5_000, 6_000),
            (MODULE_COPY, SERVICE_ID, 2, 5_000, 6_000)
        ]
    );
    let log = plan.log.expect("the log to replay");
    assert_eq!(
        (log.recording_id, log.leadership_term_id, log.term_base_log_position),
        (LOG_COPY, 2, 5_000)
    );
    // Replayed from the set to the copy's end.
    assert_eq!((log.start_position, log.stop_position), (6_000, LOG_END));
    assert_eq!((plan.last_leadership_term_id, plan.appended_log_position), (2, LOG_END));
}

#[test]
fn nothing_of_the_standby_s_archive_is_named_in_the_member_s_log() {
    let standby = standby_log(&[(0, 0), (1, 1_000), (2, 5_000)]);
    let member = member_log(&standby, LOG_COPY, Some(set(2, 5_000, 6_000))).unwrap();
    let named: Vec<i64> = member.entries().iter().map(|e| e.recording_id).collect();
    assert!(
        named
            .iter()
            .all(|id| [LOG_COPY, SERVICE_COPY, MODULE_COPY].contains(id)),
        "{named:?}"
    );
    // Its terms committed as the standby's were: each ends where the next begins.
    let ends: Vec<(i64, i64)> = member
        .entries()
        .iter()
        .filter(|e| e.entry_type == recording_log::ENTRY_TYPE_TERM)
        .map(|e| (e.leadership_term_id, e.log_position))
        .collect();
    assert_eq!(ends, [(0, 1_000), (1, 5_000), (2, recording_log::NULL_VALUE)]);
}

#[test]
fn a_member_seeded_without_a_set_replays_the_whole_log() {
    let standby = standby_log(&[(0, 0), (1, 1_000), (2, 5_000)]);
    let member = member_log(&standby, LOG_COPY, None).unwrap();

    let plan = plan(&member);
    assert!(plan.snapshots.is_empty(), "{:?}", plan.snapshots);
    let log = plan.log.expect("the log to replay");
    assert_eq!(
        (log.recording_id, log.leadership_term_id, log.start_position),
        (LOG_COPY, 2, 0)
    );
}

#[test]
fn a_set_in_a_term_the_backup_never_filed_gets_its_term() {
    // The backup queried in terms 0 and 2 only: term 1 began and ended between.
    let standby = standby_log(&[(0, 0), (2, 5_000)]);
    let member = member_log(&standby, LOG_COPY, Some(set(1, 1_000, 3_000))).unwrap();

    assert!(!member.is_unknown(1));
    let plan = plan(&member);
    assert!(
        plan.snapshots
            .iter()
            .all(|s| s.leadership_term_id == 1 && s.log_position == 3_000)
    );
    let log = plan.log.expect("the log to replay");
    assert_eq!(
        (log.leadership_term_id, log.term_base_log_position, log.start_position),
        (2, 5_000, 3_000)
    );
}

#[test]
fn a_log_in_more_than_one_recording_is_refused() {
    // A recording log refuses such a term itself; a file another tool wrote may hold one.
    let mut entries = standby_log(&[(0, 0), (1, 1_000)]);
    let term = entries
        .iter_mut()
        .find(|e| e.entry_type == recording_log::ENTRY_TYPE_TERM && e.leadership_term_id == 1);
    term.expect("term 1").recording_id = STANDBY_LOG + 1;
    let error = member_log(&entries, LOG_COPY, None).unwrap_err();
    assert!(error.contains("more than one recording"), "{error}");
}

#[test]
fn a_standby_that_recorded_no_log_seeds_nothing() {
    let error = member_log(&[], LOG_COPY, None).unwrap_err();
    assert!(error.contains("no term"), "{error}");
    assert_eq!(log_recording_id(&[]), None);
}
