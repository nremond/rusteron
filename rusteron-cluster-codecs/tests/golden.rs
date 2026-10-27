//! The Rust codecs against bytes written by Aeron 1.53.3's Java codecs
//! (`java/GoldenCodecs.java`, which writes `tests/fixtures`): each message
//! decodes to the values Java set, and encodes back to the same bytes.

use rusteron_cluster_codecs::cluster::{
    ReadBuf, WriteBuf, boolean_type::BooleanType, cancel_timer_codec::*, change_type::ChangeType,
    client_session_codec::*, close_reason::CloseReason, close_session_codec::*, cluster_action::ClusterAction,
    cluster_action_request_codec::*, cluster_time_unit::ClusterTimeUnit, join_log_codec::*,
    membership_change_event_codec::*, message_header_codec, message_header_codec::MessageHeaderDecoder,
    new_leadership_term_event_codec::*, request_service_ack_codec::*, schedule_timer_codec::*, service_ack_codec::*,
    service_termination_position_codec::*, session_close_event_codec::*, session_message_header_codec::*,
    session_open_event_codec::*, snapshot_mark::SnapshotMark, snapshot_marker_codec::*, timer_event_codec::*,
};
use rusteron_cluster_codecs::mark;

const HEADER: usize = message_header_codec::ENCODED_LENGTH;
const LOG_CHANNEL: &str = "aeron:udp?endpoint=localhost:20113|alias=log|session-id=-123456";
const RESPONSE_CHANNEL: &str = "aeron:udp?endpoint=localhost:20121";
const MEMBERS: &str = "0,localhost:20110,localhost:20111,localhost:20112,localhost:20113,localhost:20114|";

fn fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}.hex", env!("CARGO_MANIFEST_DIR"));
    let hex = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let hex = hex.trim();
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex"))
        .collect()
}

/// Semantic version as Agrona composes it: major, minor, patch in bytes 2, 1, 0.
fn version(major: i32, minor: i32, patch: i32) -> i32 {
    (major << 16) | (minor << 8) | patch
}

fn header(bytes: &[u8], template_id: u16) -> MessageHeaderDecoder<ReadBuf<'_>> {
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(bytes), 0);
    assert_eq!(header.template_id(), template_id);
    assert_eq!(header.schema_id(), 111);
    assert_eq!(header.version(), 17);
    header
}

#[test]
fn join_log() {
    let bytes = fixture("join_log");
    let mut d = JoinLogDecoder::default().header(header(&bytes, 40), 0);
    assert_eq!(d.log_position(), 1_024);
    assert_eq!(d.max_log_position(), 4_096);
    assert_eq!(d.member_id(), 2);
    assert_eq!(d.log_session_id(), -123_456);
    assert_eq!(d.log_stream_id(), 100);
    assert_eq!(d.is_startup(), BooleanType::TRUE);
    assert_eq!(d.role(), 1);
    assert_eq!(d.is_standby(), BooleanType::FALSE);
    let channel = d.log_channel_decoder();
    assert_eq!(d.log_channel_slice(channel), LOG_CHANNEL.as_bytes());
    assert_eq!(HEADER + d.encoded_length(), bytes.len());

    let mut out = vec![0; bytes.len()];
    let mut e = JoinLogEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.log_position(1_024)
        .max_log_position(4_096)
        .member_id(2)
        .log_session_id(-123_456)
        .log_stream_id(100)
        .is_startup(BooleanType::TRUE)
        .role(1)
        .is_standby(BooleanType::FALSE)
        .log_channel(LOG_CHANNEL.as_bytes());
    assert_eq!(HEADER + e.encoded_length(), bytes.len());
    assert_eq!(out, bytes);
}

#[test]
fn service_termination_position() {
    let bytes = fixture("service_termination_position");
    let d = ServiceTerminationPositionDecoder::default().header(header(&bytes, 42), 0);
    assert_eq!(d.log_position(), 8_192);
    assert_eq!(d.ack_required(), BooleanType::TRUE);

    let mut out = vec![0; bytes.len()];
    let mut e = ServiceTerminationPositionEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.log_position(8_192).ack_required(BooleanType::TRUE);
    assert_eq!(out, bytes);
}

/// `ackRequired` is new in version 17: from an older consensus module the
/// message ends before it, and the decoder must not read past its block.
#[test]
fn service_termination_position_before_version_17() {
    let mut bytes = fixture("service_termination_position");
    bytes.truncate(HEADER + 8);
    bytes[0..2].copy_from_slice(&8u16.to_le_bytes()); // block length
    bytes[6..8].copy_from_slice(&16u16.to_le_bytes()); // version
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
    let d = ServiceTerminationPositionDecoder::default().header(header, 0);
    assert_eq!(d.log_position(), 8_192);
    assert_ne!(d.ack_required(), BooleanType::TRUE);
}

#[test]
fn request_service_ack() {
    let bytes = fixture("request_service_ack");
    let d = RequestServiceAckDecoder::default().header(header(&bytes, 108), 0);
    assert_eq!(d.log_position(), 12_288);

    let mut out = vec![0; bytes.len()];
    let mut e = RequestServiceAckEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.log_position(12_288);
    assert_eq!(out, bytes);
}

#[test]
fn service_ack() {
    let bytes = fixture("service_ack");
    let d = ServiceAckDecoder::default().header(header(&bytes, 33), 0);
    assert_eq!(d.log_position(), 2_048);
    assert_eq!(d.timestamp(), 1_700_000_000_123);
    assert_eq!(d.ack_id(), 3);
    assert_eq!(d.relevant_id(), 77);
    assert_eq!(d.service_id(), 1);

    let mut out = vec![0; bytes.len()];
    let mut e = ServiceAckEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.log_position(2_048)
        .timestamp(1_700_000_000_123)
        .ack_id(3)
        .relevant_id(77)
        .service_id(1);
    assert_eq!(out, bytes);
}

#[test]
fn schedule_timer() {
    let bytes = fixture("schedule_timer");
    let d = ScheduleTimerDecoder::default().header(header(&bytes, 31), 0);
    assert_eq!(d.correlation_id(), 42);
    assert_eq!(d.deadline(), 123_456_789);

    let mut out = vec![0; bytes.len()];
    let mut e = ScheduleTimerEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.correlation_id(42).deadline(123_456_789);
    assert_eq!(out, bytes);
}

#[test]
fn cancel_timer() {
    let bytes = fixture("cancel_timer");
    let d = CancelTimerDecoder::default().header(header(&bytes, 32), 0);
    assert_eq!(d.correlation_id(), 43);

    let mut out = vec![0; bytes.len()];
    let mut e = CancelTimerEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.correlation_id(43);
    assert_eq!(out, bytes);
}

#[test]
fn close_session() {
    let bytes = fixture("close_session");
    let d = CloseSessionDecoder::default().header(header(&bytes, 30), 0);
    assert_eq!(d.cluster_session_id(), 7);

    let mut out = vec![0; bytes.len()];
    let mut e = CloseSessionEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.cluster_session_id(7);
    assert_eq!(out, bytes);
}

#[test]
fn session_message_header() {
    let bytes = fixture("session_message_header");
    assert_eq!(bytes.len(), 32, "the header a session message's payload follows");
    let d = SessionMessageHeaderDecoder::default().header(header(&bytes, 1), 0);
    assert_eq!(d.leadership_term_id(), 5);
    assert_eq!(d.cluster_session_id(), 9);
    assert_eq!(d.timestamp(), 111);

    let mut out = vec![0; bytes.len()];
    let mut e = SessionMessageHeaderEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.leadership_term_id(5).cluster_session_id(9).timestamp(111);
    assert_eq!(out, bytes);
}

#[test]
fn timer_event() {
    let bytes = fixture("timer_event");
    let d = TimerEventDecoder::default().header(header(&bytes, 20), 0);
    assert_eq!(d.leadership_term_id(), 6);
    assert_eq!(d.correlation_id(), 44);
    assert_eq!(d.timestamp(), 222);

    let mut out = vec![0; bytes.len()];
    let mut e = TimerEventEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.leadership_term_id(6).correlation_id(44).timestamp(222);
    assert_eq!(out, bytes);
}

#[test]
fn session_open_event() {
    let bytes = fixture("session_open_event");
    let mut d = SessionOpenEventDecoder::default().header(header(&bytes, 21), 0);
    assert_eq!(d.leadership_term_id(), 7);
    assert_eq!(d.correlation_id(), 45);
    assert_eq!(d.cluster_session_id(), 10);
    assert_eq!(d.timestamp(), 333);
    assert_eq!(d.response_stream_id(), 102);
    let channel = d.response_channel_decoder();
    assert_eq!(d.response_channel_slice(channel), RESPONSE_CHANNEL.as_bytes());
    let principal = d.encoded_principal_decoder();
    assert_eq!(d.encoded_principal_slice(principal), &[1, 2, 3]);
    assert_eq!(HEADER + d.encoded_length(), bytes.len());

    let mut out = vec![0; bytes.len()];
    let mut e = SessionOpenEventEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.leadership_term_id(7)
        .correlation_id(45)
        .cluster_session_id(10)
        .timestamp(333)
        .response_stream_id(102)
        .response_channel(RESPONSE_CHANNEL.as_bytes())
        .encoded_principal(&[1, 2, 3]);
    assert_eq!(out, bytes);
}

#[test]
fn session_close_event() {
    let bytes = fixture("session_close_event");
    let d = SessionCloseEventDecoder::default().header(header(&bytes, 22), 0);
    assert_eq!(d.leadership_term_id(), 8);
    assert_eq!(d.cluster_session_id(), 11);
    assert_eq!(d.timestamp(), 444);
    assert_eq!(d.close_reason(), CloseReason::TIMEOUT);

    let mut out = vec![0; bytes.len()];
    let mut e = SessionCloseEventEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.leadership_term_id(8)
        .cluster_session_id(11)
        .timestamp(444)
        .close_reason(CloseReason::TIMEOUT);
    assert_eq!(out, bytes);
}

#[test]
fn cluster_action_request() {
    let bytes = fixture("cluster_action_request");
    let d = ClusterActionRequestDecoder::default().header(header(&bytes, 23), 0);
    assert_eq!(d.leadership_term_id(), 9);
    assert_eq!(d.log_position(), 16_384);
    assert_eq!(d.timestamp(), 555);
    assert_eq!(d.action(), ClusterAction::SNAPSHOT);
    assert_eq!(d.flags(), Some(0));

    let mut out = vec![0; bytes.len()];
    let mut e = ClusterActionRequestEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.leadership_term_id(9)
        .log_position(16_384)
        .timestamp(555)
        .action(ClusterAction::SNAPSHOT)
        .flags(0);
    assert_eq!(out, bytes);
}

#[test]
fn new_leadership_term_event() {
    let bytes = fixture("new_leadership_term_event");
    let d = NewLeadershipTermEventDecoder::default().header(header(&bytes, 24), 0);
    assert_eq!(d.leadership_term_id(), 10);
    assert_eq!(d.log_position(), 20_480);
    assert_eq!(d.timestamp(), 666);
    assert_eq!(d.term_base_log_position(), 18_432);
    assert_eq!(d.leader_member_id(), 1);
    assert_eq!(d.log_session_id(), -654_321);
    assert_eq!(d.time_unit(), ClusterTimeUnit::NANOS);
    assert_eq!(d.app_version(), Some(version(1, 2, 3)));

    let mut out = vec![0; bytes.len()];
    let mut e = NewLeadershipTermEventEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.leadership_term_id(10)
        .log_position(20_480)
        .timestamp(666)
        .term_base_log_position(18_432)
        .leader_member_id(1)
        .log_session_id(-654_321)
        .time_unit(ClusterTimeUnit::NANOS)
        .app_version(version(1, 2, 3));
    assert_eq!(out, bytes);
}

#[test]
fn membership_change_event() {
    let bytes = fixture("membership_change_event");
    let mut d = MembershipChangeEventDecoder::default().header(header(&bytes, 25), 0);
    assert_eq!(d.leadership_term_id(), 11);
    assert_eq!(d.log_position(), 24_576);
    assert_eq!(d.timestamp(), 777);
    assert_eq!(d.leader_member_id(), 2);
    assert_eq!(d.cluster_size(), 3);
    assert_eq!(d.change_type(), ChangeType::QUIT);
    assert_eq!(d.member_id(), 0);
    let members = d.cluster_members_decoder();
    assert_eq!(d.cluster_members_slice(members), MEMBERS.as_bytes());

    let mut out = vec![0; bytes.len()];
    let mut e = MembershipChangeEventEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.leadership_term_id(11)
        .log_position(24_576)
        .timestamp(777)
        .leader_member_id(2)
        .cluster_size(3)
        .change_type(ChangeType::QUIT)
        .member_id(0)
        .cluster_members(MEMBERS.as_bytes());
    assert_eq!(out, bytes);
}

#[test]
fn snapshot_marker() {
    let bytes = fixture("snapshot_marker");
    let d = SnapshotMarkerDecoder::default().header(header(&bytes, 100), 0);
    assert_eq!(d.type_id(), 2);
    assert_eq!(d.log_position(), 28_672);
    assert_eq!(d.leadership_term_id(), 12);
    assert_eq!(d.index(), 0);
    assert_eq!(d.mark(), SnapshotMark::BEGIN);
    assert_eq!(d.time_unit(), ClusterTimeUnit::NANOS);
    assert_eq!(d.app_version(), Some(version(0, 0, 1)));

    let mut out = vec![0; bytes.len()];
    let mut e = SnapshotMarkerEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.type_id(2)
        .log_position(28_672)
        .leadership_term_id(12)
        .index(0)
        .mark(SnapshotMark::BEGIN)
        .time_unit(ClusterTimeUnit::NANOS)
        .app_version(version(0, 0, 1));
    assert_eq!(out, bytes);
}

#[test]
fn client_session() {
    let bytes = fixture("client_session");
    let mut d = ClientSessionDecoder::default().header(header(&bytes, 102), 0);
    assert_eq!(d.cluster_session_id(), 12);
    assert_eq!(d.response_stream_id(), 103);
    let channel = d.response_channel_decoder();
    assert_eq!(d.response_channel_slice(channel), RESPONSE_CHANNEL.as_bytes());
    let principal = d.encoded_principal_decoder();
    assert!(d.encoded_principal_slice(principal).is_empty());
    assert_eq!(HEADER + d.encoded_length(), bytes.len());

    let mut out = vec![0; bytes.len()];
    let mut e = ClientSessionEncoder::default()
        .wrap(WriteBuf::new(&mut out), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.cluster_session_id(12)
        .response_stream_id(103)
        .response_channel(RESPONSE_CHANNEL.as_bytes())
        .encoded_principal(&[]);
    assert_eq!(out, bytes);
}

#[test]
fn mark_file_header() {
    use mark::{
        cluster_component_type::ClusterComponentType, mark_file_header_codec::*,
        message_header_codec::MessageHeaderDecoder,
    };
    const STRINGS: [&str; 6] = [
        "/tmp/aeron-node",
        "aeron:ipc?term-length=128k",
        "",
        "rusteron-service",
        "",
        "/tmp/cluster",
    ];

    let bytes = fixture("mark_file_header");
    let header = MessageHeaderDecoder::default().wrap(mark::ReadBuf::new(&bytes), 0);
    assert_eq!(
        (header.template_id(), header.schema_id(), header.version()),
        (200, 110, 2)
    );
    assert_eq!(header.block_length(), 128);
    let mut d = MarkFileHeaderDecoder::default().header(header, 0);
    assert_eq!(d.version(), version(0, 3, 0));
    assert_eq!(d.component_type(), ClusterComponentType::CONTAINER);
    assert_eq!(d.activity_timestamp(), 1_700_000_000_456);
    assert_eq!(d.start_timestamp(), 1_700_000_000_000);
    assert_eq!(d.pid(), 4_242);
    assert_eq!(d.candidate_term_id(), -1);
    assert_eq!(d.archive_stream_id(), 10);
    assert_eq!(d.service_stream_id(), 104);
    assert_eq!(d.consensus_module_stream_id(), 105);
    assert_eq!(d.ingress_stream_id(), -1);
    assert_eq!(d.member_id(), 2);
    assert_eq!(d.service_id(), 1);
    assert_eq!(d.header_length(), 8_192);
    assert_eq!(d.error_buffer_length(), 1_048_576);
    assert_eq!(d.cluster_id(), 0);
    let c = d.aeron_directory_decoder();
    assert_eq!(d.aeron_directory_slice(c), STRINGS[0].as_bytes());
    let c = d.control_channel_decoder();
    assert_eq!(d.control_channel_slice(c), STRINGS[1].as_bytes());
    let c = d.ingress_channel_decoder();
    assert_eq!(d.ingress_channel_slice(c), STRINGS[2].as_bytes());
    let c = d.service_name_decoder();
    assert_eq!(d.service_name_slice(c), STRINGS[3].as_bytes());
    let c = d.authenticator_decoder();
    assert_eq!(d.authenticator_slice(c), STRINGS[4].as_bytes());
    let c = d.services_cluster_dir_decoder();
    assert_eq!(d.services_cluster_dir_slice(c), STRINGS[5].as_bytes());

    let mut out = vec![0; bytes.len()];
    let mut e = MarkFileHeaderEncoder::default()
        .wrap(
            mark::WriteBuf::new(&mut out),
            mark::message_header_codec::ENCODED_LENGTH,
        )
        .header(0)
        .parent()
        .unwrap();
    e.version(version(0, 3, 0))
        .component_type(ClusterComponentType::CONTAINER)
        .activity_timestamp(1_700_000_000_456)
        .start_timestamp(1_700_000_000_000)
        .pid(4_242)
        .candidate_term_id(-1)
        .archive_stream_id(10)
        .service_stream_id(104)
        .consensus_module_stream_id(105)
        .ingress_stream_id(-1)
        .member_id(2)
        .service_id(1)
        .header_length(8_192)
        .error_buffer_length(1_048_576)
        .cluster_id(0)
        .aeron_directory(STRINGS[0].as_bytes())
        .control_channel(STRINGS[1].as_bytes())
        .ingress_channel(STRINGS[2].as_bytes())
        .service_name(STRINGS[3].as_bytes())
        .authenticator(STRINGS[4].as_bytes())
        .services_cluster_dir(STRINGS[5].as_bytes());
    assert_eq!(out, bytes);
}
