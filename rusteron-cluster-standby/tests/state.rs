//! The standby's module state on crafted logs: sessions, timers (one fired
//! before its request),
//! terms, the service's message ids, snapshot actions; the snapshot written
//! and read back; the standby's list of its snapshots.

use std::io::Write;

use rusteron_cluster_codecs::cluster::{
    close_reason::CloseReason, cluster_action::ClusterAction, cluster_time_unit::ClusterTimeUnit, snapshot_marker_codec,
};
use rusteron_cluster_standby::snapshots::Snapshot;
use rusteron_cluster_standby::state::{
    FLAGS_DEFAULT, FLAGS_STANDBY_SNAPSHOT, INITIAL_LOG_SERVICE_SESSION_ID, ModuleState, ModuleView, Read, Session,
    SnapshotAction, Tracker,
};
use rusteron_consensus_core::service::ServiceMessage;
use rusteron_consensus_core::{codec, log};

/// A view that has read term 0's event at position 0, then `entries`, each 100 further on.
fn read(entries: &[Vec<u8>]) -> (ModuleView, Vec<Read>) {
    let mut view = ModuleView::default();
    let term = log::new_leadership_term_event(0, 0, 1_000, 0, 1, 7, ClusterTimeUnit::NANOS, 65_536);
    assert_eq!(view.on_log(&term, 96), Read::Continue);
    let reads = entries
        .iter()
        .enumerate()
        .map(|(i, e)| view.on_log(e, 196 + 100 * i as i64))
        .collect();
    (view, reads)
}

#[test]
fn sessions_open_and_close_and_the_next_id_follows_the_highest() {
    let (view, _) = read(&[
        log::session_open(0, 1, 11, 1_100, 102, "aeron:udp?endpoint=a:1", b""),
        log::session_open(0, 5, 15, 1_200, 102, "aeron:udp?endpoint=b:2", b""),
        log::session_close(0, 1, 1_300, CloseReason::CLIENT_ACTION),
    ]);
    let state = view.state(500);
    assert_eq!(
        state.sessions,
        [Session {
            id: 5,
            correlation_id: 15,
            opened_log_position: 296,
            close_reason: CloseReason::NullVal,
            response_stream_id: 102,
            response_channel: "aeron:udp?endpoint=b:2".into(),
        }]
    );
    assert_eq!(state.next_session_id, 6);
    assert_eq!(
        (state.leadership_term_id, state.app_version, state.time_unit),
        (0, 65_536, ClusterTimeUnit::NANOS)
    );
    assert_eq!(view.term_base_log_position(), 0);
}

#[test]
fn a_timer_fired_before_its_request_drops_the_request() {
    let (mut view, _) = read(&[log::timer_event(0, 41, 1_100)]);
    view.on_service(&ServiceMessage::ScheduleTimer {
        correlation_id: 41,
        deadline: 9,
    });
    view.on_service(&ServiceMessage::ScheduleTimer {
        correlation_id: 42,
        deadline: 10,
    });
    view.on_service(&ServiceMessage::ScheduleTimer {
        correlation_id: 43,
        deadline: 11,
    });
    view.on_service(&ServiceMessage::CancelTimer { correlation_id: 43 });
    assert_eq!(
        view.state(0).timers,
        [(42, 10)],
        "41 fired before it was asked for, 43 cancelled"
    );
    view.on_log(&log::timer_event(0, 42, 1_200), 400);
    assert!(view.state(0).timers.is_empty(), "42 fired after its request");
}

#[test]
fn the_service_message_id_is_the_last_one_logged_of_service_0() {
    let (view, _) = read(&[]);
    assert_eq!(view.state(0).log_service_session_id, INITIAL_LOG_SERVICE_SESSION_ID);
    let service0 = INITIAL_LOG_SERVICE_SESSION_ID + 7;
    let service1 = service0 | (1 << 56);
    let (view, _) = read(&[
        log::session_message_header(0, service0, 1_100),
        log::session_message_header(0, service1, 1_200),
        log::session_message_header(0, 3, 1_300),
    ]);
    let state = view.state(0);
    assert_eq!(
        state.log_service_session_id, service0,
        "service 1's and a client's are not"
    );
    assert_eq!(
        state.trackers,
        [Tracker {
            service_id: 0,
            next_service_session_id: service0 + 1,
            log_service_session_id: service0,
            pending_message_capacity: 0
        }]
    );
    assert_eq!(state.next_service_session_id, INITIAL_LOG_SERVICE_SESSION_ID + 1);
}

#[test]
fn every_cluster_action_breaks_and_a_snapshot_of_this_term_is_held_at() {
    let (_, reads) = read(&[
        log::cluster_action(0, 0, 1_000, 1_100, ClusterAction::SNAPSHOT, FLAGS_DEFAULT),
        log::cluster_action(0, 0, 2_000, 1_200, ClusterAction::SNAPSHOT, FLAGS_STANDBY_SNAPSHOT),
        log::cluster_action(3, 0, 3_000, 1_300, ClusterAction::SNAPSHOT, FLAGS_DEFAULT),
        log::cluster_action(0, 0, 4_000, 1_400, ClusterAction::SUSPEND, FLAGS_DEFAULT),
    ]);
    assert_eq!(
        reads,
        [
            Read::Break(Some(SnapshotAction {
                log_position: 1_000,
                flags: FLAGS_DEFAULT,
                timestamp: 1_100
            })),
            Read::Break(Some(SnapshotAction {
                log_position: 2_000,
                flags: FLAGS_STANDBY_SNAPSHOT,
                timestamp: 1_200
            })),
            Read::Break(None),
            Read::Break(None),
        ]
    );
}

#[test]
fn a_state_written_reads_back_the_same_and_a_view_takes_it_up() {
    let (mut view, _) = read(&[
        log::session_open(0, 3, 13, 1_100, 102, "aeron:udp?endpoint=c:3", b""),
        log::session_open(0, 2, 12, 1_200, 102, "aeron:udp?endpoint=d:4", b""),
        log::session_message_header(0, INITIAL_LOG_SERVICE_SESSION_ID + 2, 1_300),
    ]);
    view.on_service(&ServiceMessage::ScheduleTimer {
        correlation_id: 8,
        deadline: 80,
    });
    view.on_service(&ServiceMessage::ScheduleTimer {
        correlation_id: 4,
        deadline: 40,
    });
    let state = view.state(4_096);
    let messages = state.messages();
    assert_eq!(
        messages.len(),
        1 + 1 + 2 + 2 + 1 + 1,
        "begin, module, sessions, timers, tracker, end"
    );
    let back = ModuleState::read(messages.iter().map(Vec::as_slice), 4_096, 0, 0).unwrap();
    assert_eq!(back, state);
    let again = ModuleView::loaded(&back, 0);
    assert_eq!(again.state(4_096), state);
    assert!(
        ModuleState::read(messages[..3].iter().map(Vec::as_slice), 4_096, 0, 0).is_err(),
        "no end marker"
    );
}

#[test]
fn the_markers_carry_where_the_snapshots_term_began() {
    let mut view = ModuleView::default();
    let term = log::new_leadership_term_event(3, 2_048, 1_000, 2_048, 1, 7, ClusterTimeUnit::NANOS, 65_536);
    assert_eq!(view.on_log(&term, 2_048), Read::Continue);
    let state = view.state(4_096);
    assert_eq!(state.term_base_log_position, 2_048);
    let messages = state.messages();
    for marker in [messages.first().unwrap(), messages.last().unwrap()] {
        let decoder = snapshot_marker_codec::SnapshotMarkerDecoder::default().header(codec::header_decoder(marker), 0);
        assert_eq!(decoder.leadership_term_id(), 3);
        assert_eq!(
            decoder.term_base_log_position(),
            Some(2_048),
            "Aeron 1.54.1's members rebuild the term from it"
        );
    }
}

#[test]
fn the_standbys_list_gives_its_newest_snapshot() {
    let dir = std::env::temp_dir().join(format!("standby-snapshots-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("standby-snapshots");
    let _ = std::fs::remove_file(&file);
    assert_eq!(Snapshot::latest(&file).unwrap(), None);
    let first = Snapshot {
        log_position: 4_096,
        leadership_term_id: 0,
        term_base_log_position: 0,
        timestamp: 1_700,
        service_recording_id: 4,
        module_recording_id: 5,
    };
    let second = Snapshot {
        log_position: 8_192,
        service_recording_id: 6,
        module_recording_id: 7,
        ..first
    };
    first.append(&file).unwrap();
    second.append(&file).unwrap();
    // A line cut short by a crash is passed over.
    std::fs::OpenOptions::new()
        .append(true)
        .open(&file)
        .unwrap()
        .write_all(b"12288 0 0")
        .unwrap();
    assert_eq!(Snapshot::latest(&file).unwrap(), Some(second));
    assert_eq!(
        std::fs::read_to_string(&file).unwrap().lines().next(),
        Some("4096 0 0 1700 4 5")
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
