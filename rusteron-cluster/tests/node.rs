//! A one-member cluster's consensus module, run in memory by the testkit:
//! started on an empty log, its clients' sessions and messages in the log,
//! a snapshot, a restart from it, a shutdown, a service gone.

use rusteron_archive::fragment_assembler::Fragment;
use rusteron_archive::logbuffer::UNFRAGMENTED;
use rusteron_cluster::codecs::cluster::{
    WriteBuf, message_header_codec, service_ack_codec, session_connect_request_codec, session_message_header_codec,
};
use rusteron_cluster::consensus_module::context::Config;
use rusteron_cluster::consensus_module_agent::ports::{CounterId, Io};
use rusteron_cluster::consensus_module_agent::{ElectionState, ModuleState, Poll, Role, toggle};
use rusteron_cluster::log_publisher::LogMessage;
use rusteron_cluster::recording_log::{ENTRY_TYPE_SNAPSHOT, ENTRY_TYPE_TERM, RecordingLog};
use rusteron_cluster::session_manager::{IngressImage, PROTOCOL_SEMANTIC_VERSION};
use rusteron_cluster::testkit::consensus::FakeNode;

fn config() -> Config {
    Config {
        cluster_members: "0,localhost:20110,localhost:20111,localhost:20112,localhost:20113,localhost:20114".into(),
        ingress_channel: "aeron:udp?term-length=64k".into(),
        ..Config::default()
    }
}

fn started() -> FakeNode {
    let mut node = FakeNode::new(config());
    node.run(20);
    assert_eq!(node.module.state(), ModuleState::Active, "errors: {:?}", node.errors);
    assert_eq!(node.module.role(), Role::Leader);
    node
}

fn image() -> IngressImage {
    IngressImage {
        correlation_id: 31,
        session_id: 99,
        source_identity: "127.0.0.1:40000".into(),
    }
}

fn ingress(node: &mut FakeNode, message: &[u8]) {
    let (subscription, _) = node
        .module
        .polls(&node.ports)
        .into_iter()
        .find(|(_, p)| *p == Poll::Ingress)
        .expect("the ingress polled");
    let _ = subscription;
    let fragment = Fragment {
        payload: message,
        flags: UNFRAGMENTED,
        term_offset: 0,
        position: 0,
    };
    node.module.on_ingress_fragment(fragment, &image(), &mut node.ports);
    node.carry();
}

fn connect_request(correlation_id: i64) -> Vec<u8> {
    let channel = b"aeron:udp?endpoint=localhost:20121";
    let mut out = vec![0; 8 + 16 + 4 + channel.len() + 4 + 4];
    let mut e = session_connect_request_codec::SessionConnectRequestEncoder::default()
        .wrap(WriteBuf::new(&mut out), 8)
        .header(0)
        .parent()
        .unwrap();
    e.correlation_id(correlation_id)
        .response_stream_id(102)
        .version(PROTOCOL_SEMANTIC_VERSION)
        .response_channel(channel)
        .encoded_credentials(&[])
        .client_info(b"");
    out
}

fn client_message(leadership_term_id: i64, cluster_session_id: i64, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0; 32];
    let mut e = session_message_header_codec::SessionMessageHeaderEncoder::default()
        .wrap(WriteBuf::new(&mut out), 8)
        .header(0)
        .parent()
        .unwrap();
    e.leadership_term_id(leadership_term_id)
        .cluster_session_id(cluster_session_id)
        .timestamp(0);
    out.extend_from_slice(payload);
    out
}

fn recording_log(node: &FakeNode) -> RecordingLog {
    RecordingLog::load(&node.recording_log_file).expect("the recording log")
}

#[test]
fn a_node_on_an_empty_log_leads_term_0() {
    let node = started();
    assert_eq!(node.module.leadership_term_id(), 0);
    let log = node.ports.log_messages();
    assert!(
        matches!(
            log.first(),
            Some((
                LogMessage::NewLeadershipTerm {
                    leadership_term_id: 0,
                    ..
                },
                96
            ))
        ),
        "{log:?}"
    );
    assert_eq!(node.module.commit_position(), 96, "the new term committed");
    assert_eq!(node.ports.counter(CounterId::CommitPosition), 96);
    assert_eq!(node.ports.toggle, toggle::NEUTRAL);
    let entries = recording_log(&node).entries().to_vec();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        (
            entries[0].entry_type,
            entries[0].leadership_term_id,
            entries[0].term_base_log_position
        ),
        (ENTRY_TYPE_TERM, 0, 0)
    );
    assert_eq!(
        node.module
            .polls(&node.ports)
            .iter()
            .map(|(_, p)| *p)
            .collect::<Vec<_>>(),
        [Poll::Consensus, Poll::Ingress]
    );
    assert!(node.errors.is_empty(), "{:?}", node.errors);
}

#[test]
fn a_client_is_opened_in_the_log_and_its_messages_committed() {
    let mut node = started();
    ingress(&mut node, &connect_request(77));
    node.run(20);
    let log = node.ports.log_messages();
    assert!(
        log.iter().any(|(m, _)| matches!(
            m,
            LogMessage::SessionOpen {
                cluster_session_id: 1,
                correlation_id: 77,
                ..
            }
        )),
        "{log:?}"
    );
    ingress(&mut node, &client_message(0, 1, b"hello"));
    node.run(2);
    let log = node.ports.log_messages();
    let (_, position) = log
        .iter()
        .find(|(m, _)| {
            matches!(
                m,
                LogMessage::SessionMessage {
                    cluster_session_id: 1,
                    ..
                }
            )
        })
        .expect("the message appended");
    assert_eq!(node.module.commit_position(), *position, "and committed");
    ingress(&mut node, &client_message(5, 1, b"stale term"));
    assert_eq!(
        node.ports.log_messages().len(),
        log.len(),
        "a message of another term is dropped"
    );
}

#[test]
fn a_snapshot_is_taken_on_the_toggle_and_entered_in_the_recording_log() {
    let mut node = started();
    node.ports.toggle = toggle::SNAPSHOT;
    node.run(30);
    assert_eq!(node.module.state(), ModuleState::Active, "errors: {:?}", node.errors);
    assert_eq!(node.ports.toggle, toggle::NEUTRAL);
    assert_eq!(node.ports.counter(CounterId::SnapshotCount), 1);
    let entries = recording_log(&node).entries().to_vec();
    let snapshots: Vec<_> = entries.iter().filter(|e| e.entry_type == ENTRY_TYPE_SNAPSHOT).collect();
    assert_eq!(snapshots.len(), 2, "the service's and the module's: {entries:?}");
    assert_eq!((snapshots[0].service_id, snapshots[1].service_id), (0, -1));
    assert_eq!(snapshots[1].log_position, 96 + 96, "after the new term and the action");
}

#[test]
fn a_node_restarts_from_its_snapshot_and_log_in_the_next_term() {
    let mut node = started();
    ingress(&mut node, &connect_request(77));
    node.run(20);
    node.ports.toggle = toggle::SNAPSHOT;
    node.run(30);
    ingress(&mut node, &client_message(0, 1, b"after the snapshot"));
    node.run(5);
    let before = node.module.commit_position();

    let mut node = node.restart(config());
    node.run(40);
    assert_eq!(node.module.state(), ModuleState::Active, "errors: {:?}", node.errors);
    assert_eq!(node.module.leadership_term_id(), 1);
    assert!(node.module.commit_position() > before, "the new term after the old log");
    let terms: Vec<_> = recording_log(&node)
        .entries()
        .iter()
        .filter(|e| e.entry_type == ENTRY_TYPE_TERM)
        .map(|e| (e.leadership_term_id, e.term_base_log_position, e.log_position))
        .collect();
    assert_eq!(terms, [(0, 0, before), (1, before, -1)]);
}

#[test]
fn a_restarted_leader_waits_for_its_recording_counter_to_hold_its_start_position() {
    let mut node = started();
    ingress(&mut node, &connect_request(77));
    node.run(20);
    let before = node.module.commit_position();
    assert!(before > 0);

    let mut node = node.restart(config());
    node.ports.archive.counters_unset = true;
    node.run(40);
    assert_eq!(
        node.module.election_state(),
        Some(ElectionState::LeaderInit),
        "led on a counter still at 0"
    );
    node.ports.archive.counters_unset = false;
    node.run(40);
    assert_eq!(node.module.election_state(), None, "errors: {:?}", node.errors);
    assert_eq!(node.module.state(), ModuleState::Active, "errors: {:?}", node.errors);
    assert_eq!(node.module.leadership_term_id(), 1);
    assert!(
        !node.errors.iter().any(|e| e.contains("quorum position went backwards")),
        "{:?}",
        node.errors
    );
}

#[test]
fn a_shutdown_snapshots_then_terminates() {
    let mut node = started();
    node.ports.toggle = toggle::SHUTDOWN;
    node.run(40);
    assert_eq!(node.terminated, Some(true), "errors: {:?}", node.errors);
    assert_eq!(node.module.state(), ModuleState::Closed);
    assert_eq!(node.ports.counter(CounterId::SnapshotCount), 1);
}

#[test]
fn a_service_gone_ends_the_node() {
    let mut node = started();
    let client = node.services.client_ids[0];
    node.module.on_io(
        Io::CounterUnavailable {
            registration_id: client,
        },
        &mut node.ports,
    );
    node.run(20);
    assert_eq!(node.terminated, Some(false));
    assert!(
        node.errors
            .iter()
            .any(|e| e == "Aeron client in service closed unexpectedly: serviceId=0")
    );
}

/// Java 1.54: a leader with no service takes its snapshot as soon as the
/// snapshot's action is committed, in its consensus work, not at its next
/// slow tick (10 ms on).
#[test]
fn a_node_without_services_snapshots_once_its_action_is_committed() {
    let mut node = FakeNode::new(Config {
        service_count: 0,
        ..config()
    });
    node.run(20);
    assert_eq!(node.module.state(), ModuleState::Active, "errors: {:?}", node.errors);
    node.ports.toggle = toggle::SNAPSHOT;
    let mut cycles = 0;
    while node.module.state() != ModuleState::Snapshot {
        node.run(1);
        cycles += 1;
        assert!(cycles < 20, "no snapshot action: {:?}", node.errors);
    }
    let mut after = 0;
    while node.ports.counter(CounterId::SnapshotCount) == 0 {
        node.run(1);
        after += 1;
        assert!(after < 30, "no snapshot: {:?}", node.errors);
    }
    assert!(after < 10, "the snapshot waited {after} cycles, as for a slow tick");
    assert!(node.errors.is_empty(), "{:?}", node.errors);
}

/// Java 1.54: a service message or ack naming a service the node does not
/// have is reported and dropped; the node carries on, its election not
/// started over.
#[test]
fn a_message_or_ack_from_an_unknown_service_is_dropped() {
    let mut node = started();
    // A service message sent as service 3 of a one-service node.
    let message = rusteron_cluster::log_publisher::session_message_header(0, 3, 0);
    node.module.on_service_message(&message, &mut node.ports);
    let mut ack = vec![0; message_header_codec::ENCODED_LENGTH + service_ack_codec::SBE_BLOCK_LENGTH as usize];
    let mut e = service_ack_codec::ServiceAckEncoder::default()
        .wrap(WriteBuf::new(&mut ack), message_header_codec::ENCODED_LENGTH)
        .header(0)
        .parent()
        .unwrap();
    e.log_position(0).timestamp(0).ack_id(7).relevant_id(-1).service_id(3);
    node.module.on_service_message(&ack, &mut node.ports);
    node.run(20);
    assert_eq!(
        node.errors,
        [
            "service message dropped, unknown serviceId=3 clusterSessionId=3 serviceCount=1",
            "service ack dropped, unknown serviceId=3 ackId=7 serviceCount=1",
        ]
    );
    assert_eq!(node.terminated, None);
    assert_eq!(node.module.state(), ModuleState::Active);
    assert_eq!(node.module.role(), Role::Leader);
}

/// Java's `pollArchiveEvents`: a node whose archive died would no longer
/// record its log, so it ends, for its supervisor to start it again.
#[test]
fn the_local_archive_gone_ends_the_node() {
    let mut node = started();
    node.ports.archive_gone = true;
    node.run(20);
    assert_eq!(node.terminated, Some(false), "errors: {:?}", node.errors);
    assert_eq!(node.module.state(), ModuleState::Closed);
    assert_eq!(node.errors, ["local archive is not connected"]);
}
