//! Backups and standbys at a one-member cluster's consensus module, run in
//! memory by the testkit: a backup's query answered with the log and the
//! snapshots to start from, a heartbeat answered, a request of another
//! protocol refused and the refusal recorded, a request before the member
//! is active dropped, a standby's notice filed once committed and its delay
//! over, or refused by a member that does not take them; the newest
//! standby set copied from the standby's archive and filed as the member's
//! snapshots on the node control toggle, and a start going on with its own
//! snapshots when the standby's archive does not deliver, or no longer has
//! the set.

use rusteron_archive::control_session_adapter::{Replicate, Request, Signal};
use rusteron_cluster::codecs::cluster::{
    ReadBuf, event_code::EventCode, message_header_codec::MessageHeaderDecoder, session_event_codec,
};
use rusteron_cluster::consensus_adapter::{self, StandbySnapshotEntry, decode_backup_response};
use rusteron_cluster::consensus_module::context::Config;
use rusteron_cluster::consensus_module_agent::ports::CounterId;
use rusteron_cluster::consensus_module_agent::{ModuleState, Role, node_toggle, toggle};
use rusteron_cluster::recording_log::{ENTRY_TYPE_SNAPSHOT, ENTRY_TYPE_STANDBY_SNAPSHOT, RecordingLog};
use rusteron_cluster::session_manager::PROTOCOL_SEMANTIC_VERSION;
use rusteron_cluster::testkit::consensus::FakeNode;

const MEMBERS: &str = "0,localhost:20110,localhost:20111,localhost:20112,localhost:20113,localhost:20114";
const RESPONSE_CHANNEL: &str = "aeron:udp?endpoint=localhost:20199";
const STANDBY_ARCHIVE: &str = "localhost:21301";

fn config() -> Config {
    Config {
        cluster_members: MEMBERS.into(),
        ingress_channel: "aeron:udp?term-length=64k".into(),
        ..Config::default()
    }
}

fn started(config: Config) -> FakeNode {
    let mut node = FakeNode::new(config);
    node.run(20);
    assert_eq!(node.module.state(), ModuleState::Active, "errors: {:?}", node.errors);
    assert_eq!(node.module.role(), Role::Leader);
    node
}

/// Everything the member sent on its egress publications since the last call.
fn replies(node: &mut FakeNode) -> Vec<Vec<u8>> {
    node.ports.egress.drain().flat_map(|(_, messages)| messages).collect()
}

/// A session event's code, and whether its bytes hold `detail`.
fn event(message: &[u8], detail: &str) -> (EventCode, bool) {
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(message), 0);
    assert_eq!(header.template_id(), session_event_codec::SBE_TEMPLATE_ID);
    let d = session_event_codec::SessionEventDecoder::default().header(header, 0);
    (d.code(), String::from_utf8_lossy(message).contains(detail))
}

fn standby_entries(log_position: i64) -> Vec<StandbySnapshotEntry> {
    [(21, 0), (22, -1)]
        .into_iter()
        .map(|(recording_id, service_id)| StandbySnapshotEntry {
            recording_id,
            leadership_term_id: 0,
            term_base_log_position: 0,
            log_position,
            timestamp: 450,
            service_id,
            archive_endpoint: STANDBY_ARCHIVE.into(),
        })
        .collect()
}

fn notice(correlation_id: i64, log_position: i64) -> Vec<u8> {
    consensus_adapter::standby_snapshot(
        correlation_id,
        PROTOCOL_SEMANTIC_VERSION,
        111,
        &standby_entries(log_position),
        RESPONSE_CHANNEL,
    )
}

/// The recording log's standby entries: recording and position.
fn standby_entries_filed(node: &FakeNode) -> Vec<(i64, i64)> {
    RecordingLog::load(&node.recording_log_file)
        .expect("the recording log")
        .entries()
        .iter()
        .filter(|e| e.entry_type == ENTRY_TYPE_STANDBY_SNAPSHOT)
        .map(|e| (e.recording_id, e.log_position))
        .collect()
}

#[test]
fn a_backup_is_told_the_log_and_the_latest_snapshot() {
    let mut node = started(config());
    node.ports.toggle = toggle::SNAPSHOT;
    node.run(30);
    assert_eq!(node.ports.counter(CounterId::SnapshotCount), 1);
    let snapshot_position = node.module.commit_position();
    replies(&mut node);

    node.consensus_in(&consensus_adapter::backup_query(
        5,
        108,
        PROTOCOL_SEMANTIC_VERSION,
        -1,
        RESPONSE_CHANNEL,
        &[],
    ));
    node.run(15);
    let replies = replies(&mut node);
    assert_eq!(replies.len(), 1, "errors: {:?}", node.errors);
    let response = decode_backup_response(&replies[0]).expect("a backup response");
    let log = RecordingLog::load(&node.recording_log_file).expect("the recording log");
    let term = log.find_last_term().expect("a term");
    assert_eq!(response.correlation_id, 5);
    assert_eq!(
        (
            response.log_recording_id,
            response.log_leadership_term_id,
            response.log_term_base_log_position
        ),
        (term.recording_id, term.leadership_term_id, term.term_base_log_position)
    );
    assert_eq!(
        (response.last_leadership_term_id, response.last_term_base_log_position),
        (term.leadership_term_id, term.term_base_log_position)
    );
    assert_eq!(
        response.commit_position_counter_id,
        100 + CounterId::CommitPosition as i32
    );
    assert_eq!((response.leader_member_id, response.member_id), (0, 0));
    assert!(
        response.cluster_members.starts_with("0,localhost:20110,"),
        "{}",
        response.cluster_members
    );
    let snapshots: Vec<(i32, i64)> = response
        .snapshots
        .iter()
        .map(|s| (s.service_id, s.log_position))
        .collect();
    assert_eq!(snapshots, [(0, snapshot_position), (-1, snapshot_position)]);
    assert!(
        node.ports.egress_connected.is_empty(),
        "its session closed once answered"
    );
}

#[test]
fn a_heartbeat_is_answered() {
    let mut node = started(config());
    node.consensus_in(&consensus_adapter::heartbeat_request(7, 108, RESPONSE_CHANNEL));
    node.run(15);
    assert_eq!(replies(&mut node), [consensus_adapter::heartbeat_response(7)]);
    assert!(node.ports.egress_connected.is_empty());
}

#[test]
fn a_backup_of_another_protocol_is_refused_and_the_refusal_recorded() {
    let mut node = started(config());
    node.consensus_in(&consensus_adapter::backup_query(
        8,
        108,
        1 << 16,
        -1,
        RESPONSE_CHANNEL,
        &[],
    ));
    node.run(15);
    let replies = replies(&mut node);
    assert_eq!(replies.len(), 1);
    assert_eq!(
        event(&replies[0], "invalid client version 1.0.0, cluster=0.3.0"),
        (EventCode::ERROR, true)
    );
    assert!(
        node.errors
            .iter()
            .any(|e| e == "ERROR invalid client version 1.0.0, cluster=0.3.0, clusterMemberId=0, id=-1"),
        "{:?}",
        node.errors
    );
}

#[test]
fn a_request_before_the_member_is_active_is_dropped() {
    let mut node = FakeNode::new(config());
    node.consensus_in(&consensus_adapter::heartbeat_request(7, 108, RESPONSE_CHANNEL));
    node.run(20);
    assert_eq!(node.module.state(), ModuleState::Active);
    assert!(replies(&mut node).is_empty());
}

#[test]
fn a_standby_snapshot_is_filed_once_committed() {
    let mut node = started(Config {
        accept_standby_snapshots: true,
        ..config()
    });
    let commit = node.module.commit_position();
    node.consensus_in(&notice(9, commit + 4_096));
    node.consensus_in(&notice(10, commit));
    node.run(15);
    assert!(replies(&mut node).is_empty(), "a notice gets no answer");
    assert_eq!(
        standby_entries_filed(&node),
        [(21, commit), (22, commit)],
        "the notice past the commit position still waits"
    );
    assert_eq!(node.ports.counter(CounterId::StandbySnapshotCount), 1);
    let log = RecordingLog::load(&node.recording_log_file).expect("the recording log");
    assert_eq!(log.latest_standby_snapshots(1)[STANDBY_ARCHIVE].len(), 2);
}

#[test]
fn a_standby_snapshot_waits_out_the_processing_delay() {
    let mut node = started(Config {
        accept_standby_snapshots: true,
        standby_snapshot_notification_delay_ns: 50_000_000,
        ..config()
    });
    let commit = node.module.commit_position();
    node.consensus_in(&notice(9, commit));
    node.run(15);
    assert!(standby_entries_filed(&node).is_empty());
    node.advance(60).run(15);
    assert_eq!(standby_entries_filed(&node), [(21, commit), (22, commit)]);
}

#[test]
fn a_member_that_takes_no_standby_snapshots_refuses_a_notice() {
    let mut node = started(config());
    let commit = node.module.commit_position();
    node.consensus_in(&notice(9, commit));
    node.run(15);
    let replies = replies(&mut node);
    assert_eq!(replies.len(), 1);
    assert_eq!(
        event(&replies[0], "Not configured to accept StandbySnapshot"),
        (EventCode::ERROR, true)
    );
    assert!(standby_entries_filed(&node).is_empty());
}

/// The replications the module asked its archive for, in order.
fn replications(node: &FakeNode) -> Vec<Replicate> {
    node.ports
        .archive
        .requests
        .iter()
        .filter_map(|r| match r {
            Request::Replicate(r) => Some(r.as_ref().clone()),
            _ => None,
        })
        .collect()
}

/// The standby's archive copying a recording into `dst_recording_id`, as
/// the local archive signals it.
fn copied(node: &mut FakeNode, replicate: &Replicate, dst_recording_id: i64) {
    let replication_id = 3_000 + replicate.correlation_id;
    for (signal, position) in [
        (Signal::REPLICATE, 0),
        (Signal::EXTEND, 0),
        (Signal::SYNC, 1024),
        (Signal::REPLICATE_END, 1024),
        (Signal::STOP, 1024),
    ] {
        node.ports
            .archive
            .signal(replication_id, dst_recording_id, 0, position, signal);
    }
    node.run(15);
}

/// The recording log's snapshots of the member's own: recording, service, position.
fn local_snapshots(node: &FakeNode) -> Vec<(i64, i32, i64)> {
    RecordingLog::load(&node.recording_log_file)
        .expect("the recording log")
        .entries()
        .iter()
        .filter(|e| e.entry_type == ENTRY_TYPE_SNAPSHOT && e.is_valid)
        .map(|e| (e.recording_id, e.service_id, e.log_position))
        .collect()
}

/// Java's `StandbySnapshotReplicatorTest`: the newest set copied, recording
/// by recording, from the archive its entries name, then filed as the
/// member's own.
#[test]
fn the_node_toggle_copies_the_newest_standby_set_and_files_it() {
    let mut node = started(Config {
        accept_standby_snapshots: true,
        ..config()
    });
    assert_eq!(
        node.ports.node_toggle,
        node_toggle::NEUTRAL,
        "activated by the election"
    );
    let commit = node.module.commit_position();
    node.consensus_in(&notice(9, commit));
    node.run(15);
    assert_eq!(standby_entries_filed(&node).len(), 2);

    node.ports.node_toggle = node_toggle::REPLICATE_STANDBY_SNAPSHOT;
    node.run(15);
    assert_eq!(node.ports.node_toggle, node_toggle::NEUTRAL);
    let first = replications(&node);
    assert_eq!(first.len(), 1, "one recording at a time");
    assert_eq!(
        (
            first[0].src_recording_id,
            first[0].dst_recording_id,
            first[0].stop_position,
            first[0].src_control_channel.as_str()
        ),
        (22, -1, -1, "aeron:udp?endpoint=localhost:21301"),
        "the module's first, from the standby's archive, whole"
    );
    copied(&mut node, &first[0], 100);
    let second = replications(&node);
    assert_eq!(second.len(), 2);
    assert_eq!(second[1].src_recording_id, 21);
    copied(&mut node, &second[1], 101);

    assert_eq!(local_snapshots(&node), [(101, 0, commit), (100, -1, commit)]);
    assert_eq!(node.ports.counter(CounterId::SnapshotCount), 1);
    assert!(node.errors.is_empty(), "{:?}", node.errors);

    node.ports.node_toggle = node_toggle::REPLICATE_STANDBY_SNAPSHOT;
    node.run(15);
    assert_eq!(
        replications(&node).len(),
        2,
        "a set no newer than the member's own is not copied"
    );
}

/// A copy that ends without its sync is copied again.
#[test]
fn a_recording_copied_without_its_sync_is_copied_again() {
    let mut node = started(Config {
        accept_standby_snapshots: true,
        ..config()
    });
    let commit = node.module.commit_position();
    node.consensus_in(&notice(9, commit));
    node.run(15);
    node.ports.node_toggle = node_toggle::REPLICATE_STANDBY_SNAPSHOT;
    node.run(15);
    let first = replications(&node).remove(0);
    let replication_id = 3_000 + first.correlation_id;
    for (signal, position) in [(Signal::EXTEND, 0), (Signal::REPLICATE_END, 512)] {
        node.ports.archive.signal(replication_id, 100, 0, position, signal);
    }
    node.run(15);
    let again = replications(&node);
    assert_eq!(again.len(), 2);
    assert_eq!(again[1].src_recording_id, first.src_recording_id);
}

/// Java's `replicateStandbySnapshotsForStartup`: a standby archive that does
/// not deliver is given up after twice the archive's message timeout, with
/// a warning, and the member starts on its own snapshots.
#[test]
fn a_start_goes_on_with_its_own_snapshots_when_the_standby_does_not_deliver() {
    let config = Config {
        accept_standby_snapshots: true,
        ..config()
    };
    let mut node = started(config.clone());
    let commit = node.module.commit_position();
    node.consensus_in(&notice(9, commit));
    node.run(15);
    assert_eq!(standby_entries_filed(&node).len(), 2);

    let mut node = node.restart(config);
    node.run(20);
    assert_ne!(node.module.state(), ModuleState::Active, "copying first");
    assert_eq!(replications(&node).len(), 1);
    node.advance(21_000).run(30);
    assert_eq!(node.module.state(), ModuleState::Active, "errors: {:?}", node.errors);
    assert!(
        node.errors.iter().any(|e| e
            == "failed to replicate any standby snapshots, errors: {localhost:21301=log replication has not progressed}"),
        "{:?}",
        node.errors
    );
}

/// A standby that no longer has the set, deleted as it keeps its newest
/// only: once the local archive took the copy, it answers the source's
/// refusal under the replication's id (Java's `ReplicationSession.error`).
/// Java's replicator, on an archive client of its own, catches it, and the
/// member starts on its own snapshots, as here, not ending.
#[test]
fn a_start_goes_on_with_its_own_snapshots_when_the_standby_set_is_gone() {
    let config = Config {
        accept_standby_snapshots: true,
        ..config()
    };
    let mut node = started(config.clone());
    let commit = node.module.commit_position();
    node.consensus_in(&notice(9, commit));
    node.run(15);
    assert_eq!(standby_entries_filed(&node).len(), 2);

    let mut node = node.restart(config);
    node.run(20);
    let first = replications(&node);
    assert_eq!(first.len(), 1, "copying first");
    let replication_id = 3_000 + first[0].correlation_id;
    node.ports
        .archive
        .error(replication_id, 5, "unknown src recording id 22");
    node.ports
        .archive
        .signal(replication_id, -1, 0, -1, Signal::REPLICATE_END);
    node.run(30);
    assert_eq!(node.module.state(), ModuleState::Active, "errors: {:?}", node.errors);
    assert!(
        node.errors
            .iter()
            .any(|e| e
                == "failed to replicate any standby snapshots, errors: {localhost:21301=unknown src recording id 22}"),
        "{:?}",
        node.errors
    );
    assert_eq!(replications(&node).len(), 1, "the set not copied again");
}
