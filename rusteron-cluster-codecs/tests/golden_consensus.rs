//! The consensus module's messages against bytes written by Aeron 1.54.2's
//! Java codecs (`java/GoldenCodecs.java`): what it exchanges with clients,
//! with the other members, with services and ClusterTool, and keeps in its
//! snapshot. Each encodes to Java's bytes with the values Java set, and
//! decodes back to them.

use rusteron_cluster_codecs::cluster::cluster_members_extended_response_codec;
use rusteron_cluster_codecs::cluster::{
    ReadBuf, WriteBuf, admin_request_codec::*, admin_request_type::AdminRequestType,
    admin_response_code::AdminResponseCode, admin_response_codec::*, append_position_codec::*,
    boolean_type::BooleanType, canvass_position_codec::*, catchup_position_codec::*, challenge_codec::*,
    close_reason::CloseReason, cluster_members_extended_response_codec::*, cluster_members_query_codec::*,
    cluster_members_response_codec::*, cluster_session_codec::*, commit_position_codec::*, consensus_module_codec::*,
    event_code::EventCode, message_header_codec, message_header_codec::MessageHeaderDecoder, new_leader_event_codec::*,
    new_leadership_term_codec::*, pending_message_tracker_codec::*, request_vote_codec::*,
    session_close_request_codec::*, session_connect_request_codec::*, session_event_codec::*,
    session_keep_alive_codec::*, stop_catchup_codec::*, termination_ack_codec::*, termination_position_codec::*,
    timer_codec::*, vote_codec::*,
};
use rusteron_cluster_codecs::mark;

const HEADER: usize = message_header_codec::ENCODED_LENGTH;
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
    assert_eq!(header.version(), 18);
    header
}

/// Encodes a message as `$body` sets it and checks the bytes are Java's;
/// gives Java's bytes back for the decoder's checks.
macro_rules! encodes_as_java {
    ($name:literal, $encoder:ty, |$e:ident| $body:block) => {{
        let bytes = fixture($name);
        let mut out = vec![0; bytes.len()];
        {
            #[allow(unused_mut)]
            let mut $e = <$encoder>::default()
                .wrap(WriteBuf::new(&mut out), HEADER)
                .header(0)
                .parent()
                .unwrap();
            $body
        }
        assert_eq!(out, bytes, $name);
        bytes
    }};
}

#[test]
fn session_event() {
    let bytes = encodes_as_java!("session_event", SessionEventEncoder, |e| {
        e.cluster_session_id(13)
            .correlation_id(46)
            .leadership_term_id(14)
            .leader_member_id(1)
            .code(EventCode::REDIRECT)
            .version(version(0, 3, 0))
            .leader_heartbeat_timeout_ns(10_000_000_000)
            .detail(b"0=localhost:20110,1=localhost:20210");
    });
    let mut d = SessionEventDecoder::default().header(header(&bytes, 2), 0);
    assert_eq!(d.cluster_session_id(), 13);
    assert_eq!(d.code(), EventCode::REDIRECT);
    assert_eq!(d.leader_heartbeat_timeout_ns(), Some(10_000_000_000));
    let detail = d.detail_decoder();
    assert_eq!(d.detail_slice(detail), b"0=localhost:20110,1=localhost:20210");
}

#[test]
fn session_connect_request() {
    let bytes = encodes_as_java!("session_connect_request", SessionConnectRequestEncoder, |e| {
        e.correlation_id(47)
            .response_stream_id(102)
            .version(version(0, 3, 0))
            .response_channel(RESPONSE_CHANNEL.as_bytes())
            .encoded_credentials(&[4, 5])
            .client_info(b"name=bench");
    });
    let mut d = SessionConnectRequestDecoder::default().header(header(&bytes, 3), 0);
    assert_eq!(d.correlation_id(), 47);
    assert_eq!(d.response_stream_id(), 102);
    assert_eq!(d.version(), Some(version(0, 3, 0)));
    let channel = d.response_channel_decoder();
    assert_eq!(d.response_channel_slice(channel), RESPONSE_CHANNEL.as_bytes());
    let credentials = d.encoded_credentials_decoder();
    assert_eq!(d.encoded_credentials_slice(credentials), &[4, 5]);
    let info = d.client_info_decoder();
    assert_eq!(d.client_info_slice(info), b"name=bench");
}

#[test]
fn session_close_request_and_keep_alive() {
    let bytes = encodes_as_java!("session_close_request", SessionCloseRequestEncoder, |e| {
        e.leadership_term_id(15).cluster_session_id(16);
    });
    let d = SessionCloseRequestDecoder::default().header(header(&bytes, 4), 0);
    assert_eq!((d.leadership_term_id(), d.cluster_session_id()), (15, 16));

    let bytes = encodes_as_java!("session_keep_alive", SessionKeepAliveEncoder, |e| {
        e.leadership_term_id(17).cluster_session_id(18);
    });
    let d = SessionKeepAliveDecoder::default().header(header(&bytes, 5), 0);
    assert_eq!((d.leadership_term_id(), d.cluster_session_id()), (17, 18));
}

#[test]
fn new_leader_event() {
    let endpoints = "0=localhost:20110,1=localhost:20210,2=localhost:20310";
    let bytes = encodes_as_java!("new_leader_event", NewLeaderEventEncoder, |e| {
        e.leadership_term_id(19)
            .cluster_session_id(20)
            .leader_member_id(2)
            .ingress_endpoints(endpoints.as_bytes());
    });
    let mut d = NewLeaderEventDecoder::default().header(header(&bytes, 6), 0);
    assert_eq!(d.leader_member_id(), 2);
    let e = d.ingress_endpoints_decoder();
    assert_eq!(d.ingress_endpoints_slice(e), endpoints.as_bytes());
}

#[test]
fn challenge() {
    let bytes = encodes_as_java!("challenge", ChallengeEncoder, |e| {
        e.correlation_id(51)
            .cluster_session_id(24)
            .encoded_challenge(&[7, 8, 9]);
    });
    let mut d = ChallengeDecoder::default().header(header(&bytes, 7), 0);
    assert_eq!((d.correlation_id(), d.cluster_session_id()), (51, 24));
    let c = d.encoded_challenge_decoder();
    assert_eq!(d.encoded_challenge_slice(c), &[7, 8, 9]);
}

#[test]
fn admin_request_and_response() {
    let bytes = encodes_as_java!("admin_request", AdminRequestEncoder, |e| {
        e.leadership_term_id(21)
            .cluster_session_id(22)
            .correlation_id(48)
            .request_type(AdminRequestType::SNAPSHOT)
            .payload(&[]);
    });
    let d = AdminRequestDecoder::default().header(header(&bytes, 26), 0);
    assert_eq!(d.request_type(), AdminRequestType::SNAPSHOT);
    assert_eq!(d.correlation_id(), 48);

    let message = "Execution of the SNAPSHOT request was not authorised";
    let bytes = encodes_as_java!("admin_response", AdminResponseEncoder, |e| {
        e.cluster_session_id(23)
            .correlation_id(49)
            .request_type(AdminRequestType::SNAPSHOT)
            .response_code(AdminResponseCode::UNAUTHORISED_ACCESS)
            .message(message.as_bytes())
            .payload(&[]);
    });
    let mut d = AdminResponseDecoder::default().header(header(&bytes, 27), 0);
    assert_eq!(d.response_code(), AdminResponseCode::UNAUTHORISED_ACCESS);
    let m = d.message_decoder();
    assert_eq!(d.message_slice(m), message.as_bytes());
}

#[test]
fn cluster_members_query_and_responses() {
    let bytes = encodes_as_java!("cluster_members_query", ClusterMembersQueryEncoder, |e| {
        e.correlation_id(50).extended(BooleanType::TRUE);
    });
    let d = ClusterMembersQueryDecoder::default().header(header(&bytes, 34), 0);
    assert_eq!(d.extended(), BooleanType::TRUE);

    let bytes = encodes_as_java!("cluster_members_response", ClusterMembersResponseEncoder, |e| {
        e.correlation_id(51)
            .leader_member_id(1)
            .active_members(MEMBERS.as_bytes())
            .passive_followers(b"");
    });
    let mut d = ClusterMembersResponseDecoder::default().header(header(&bytes, 41), 0);
    let active = d.active_members_decoder();
    assert_eq!(d.active_members_slice(active), MEMBERS.as_bytes());

    let bytes = encodes_as_java!(
        "cluster_members_extended_response",
        ClusterMembersExtendedResponseEncoder,
        |e| {
            e.correlation_id(52)
                .current_time_ns(1_700_000_002_000_000_000)
                .leader_member_id(0)
                .member_id(1);
            let mut active = e.active_members_encoder(
                2,
                cluster_members_extended_response_codec::encoder::ActiveMembersEncoder::default(),
            );
            for i in 0..2i32 {
                active.advance().unwrap();
                active
                    .leadership_term_id(26 + i64::from(i))
                    .log_position(36_864 + i64::from(i) * 1_024)
                    .time_of_last_append_ns(1_700_000_001_000_000_000 + i64::from(i))
                    .member_id(i)
                    .ingress_endpoint(format!("localhost:2{i}110").as_bytes())
                    .consensus_endpoint(format!("localhost:2{i}111").as_bytes())
                    .log_endpoint(format!("localhost:2{i}112").as_bytes())
                    .catchup_endpoint(format!("localhost:2{i}113").as_bytes())
                    .archive_endpoint(format!("localhost:2{i}114").as_bytes());
            }
            let e = active.parent().unwrap();
            let mut passive = e.passive_members_encoder(
                0,
                cluster_members_extended_response_codec::encoder::PassiveMembersEncoder::default(),
            );
            passive.parent().unwrap();
        }
    );
    let d = ClusterMembersExtendedResponseDecoder::default().header(header(&bytes, 43), 0);
    assert_eq!(d.member_id(), 1);
    let mut active = d.active_members_decoder();
    assert_eq!(active.count(), 2);
    active.advance().unwrap();
    assert_eq!(active.log_position(), 36_864);
}

#[test]
fn canvass_vote_and_ballot() {
    let bytes = encodes_as_java!("canvass_position", CanvassPositionEncoder, |e| {
        e.log_leadership_term_id(24)
            .log_position(32_768)
            .leadership_term_id(25)
            .follower_member_id(2)
            .protocol_version(version(1, 0, 0));
    });
    let d = CanvassPositionDecoder::default().header(header(&bytes, 50), 0);
    assert_eq!(d.log_position(), 32_768);
    assert_eq!(d.protocol_version(), Some(version(1, 0, 0)));

    let bytes = encodes_as_java!("request_vote", RequestVoteEncoder, |e| {
        e.log_leadership_term_id(26)
            .log_position(36_864)
            .candidate_term_id(27)
            .candidate_member_id(1)
            .protocol_version(version(1, 0, 0));
    });
    let d = RequestVoteDecoder::default().header(header(&bytes, 51), 0);
    assert_eq!(d.candidate_term_id(), 27);

    let bytes = encodes_as_java!("vote", VoteEncoder, |e| {
        e.candidate_term_id(28)
            .log_leadership_term_id(29)
            .log_position(40_960)
            .candidate_member_id(1)
            .follower_member_id(2)
            .vote(BooleanType::TRUE);
    });
    let d = VoteDecoder::default().header(header(&bytes, 52), 0);
    assert_eq!(d.vote(), BooleanType::TRUE);
    assert_eq!(d.follower_member_id(), 2);
}

#[test]
fn new_leadership_term() {
    let bytes = encodes_as_java!("new_leadership_term", NewLeadershipTermEncoder, |e| {
        e.log_leadership_term_id(30)
            .next_leadership_term_id(31)
            .next_term_base_log_position(45_056)
            .next_log_position(49_152)
            .leadership_term_id(32)
            .term_base_log_position(53_248)
            .log_position(57_344)
            .leader_recording_id(33)
            .timestamp(1_700_000_003_000)
            .leader_member_id(0)
            .log_session_id(-777)
            .app_version(version(0, 0, 1))
            .is_startup(BooleanType::FALSE)
            .commit_position(55_296);
    });
    let d = NewLeadershipTermDecoder::default().header(header(&bytes, 53), 0);
    assert_eq!(d.next_log_position(), 49_152);
    assert_eq!(d.log_session_id(), -777);
    assert_eq!(d.commit_position(), 55_296);
}

#[test]
fn append_commit_and_catchup_positions() {
    let bytes = encodes_as_java!("append_position", AppendPositionEncoder, |e| {
        e.leadership_term_id(34)
            .log_position(61_440)
            .follower_member_id(2)
            .flags(1);
    });
    let d = AppendPositionDecoder::default().header(header(&bytes, 54), 0);
    assert_eq!(d.flags(), 1);

    let bytes = encodes_as_java!("commit_position", CommitPositionEncoder, |e| {
        e.leadership_term_id(35).log_position(65_536).leader_member_id(1);
    });
    let d = CommitPositionDecoder::default().header(header(&bytes, 55), 0);
    assert_eq!(d.log_position(), 65_536);

    let bytes = encodes_as_java!("catchup_position", CatchupPositionEncoder, |e| {
        e.leadership_term_id(36)
            .log_position(69_632)
            .follower_member_id(2)
            .catchup_endpoint(b"localhost:20313");
    });
    let mut d = CatchupPositionDecoder::default().header(header(&bytes, 56), 0);
    let endpoint = d.catchup_endpoint_decoder();
    assert_eq!(d.catchup_endpoint_slice(endpoint), b"localhost:20313");

    let bytes = encodes_as_java!("stop_catchup", StopCatchupEncoder, |e| {
        e.leadership_term_id(37).follower_member_id(2);
    });
    let d = StopCatchupDecoder::default().header(header(&bytes, 57), 0);
    assert_eq!(d.leadership_term_id(), 37);
}

#[test]
fn termination_position_and_ack() {
    let bytes = encodes_as_java!("termination_position", TerminationPositionEncoder, |e| {
        e.leadership_term_id(38).log_position(73_728);
    });
    let d = TerminationPositionDecoder::default().header(header(&bytes, 75), 0);
    assert_eq!(d.log_position(), 73_728);

    let bytes = encodes_as_java!("termination_ack", TerminationAckEncoder, |e| {
        e.leadership_term_id(39).log_position(77_824).member_id(2);
    });
    let d = TerminationAckDecoder::default().header(header(&bytes, 76), 0);
    assert_eq!(d.member_id(), 2);
}

#[test]
fn the_consensus_modules_snapshot() {
    let bytes = encodes_as_java!("cluster_session", ClusterSessionEncoder, |e| {
        e.cluster_session_id(40)
            .correlation_id(53)
            .opened_log_position(81_920)
            .time_of_last_activity(-1)
            .close_reason(CloseReason::NullVal)
            .response_stream_id(102)
            .response_channel(RESPONSE_CHANNEL.as_bytes());
    });
    let d = ClusterSessionDecoder::default().header(header(&bytes, 103), 0);
    assert_eq!(d.close_reason(), CloseReason::NullVal);
    assert_eq!(d.opened_log_position(), 81_920);

    let bytes = encodes_as_java!("timer", TimerEncoder, |e| {
        e.correlation_id(54).deadline(1_700_000_004_000);
    });
    let d = TimerDecoder::default().header(header(&bytes, 104), 0);
    assert_eq!(d.deadline(), 1_700_000_004_000);

    let bytes = encodes_as_java!("consensus_module", ConsensusModuleEncoder, |e| {
        e.next_session_id(41)
            .next_service_session_id(i64::MIN + 3)
            .log_service_session_id(i64::MIN + 2)
            .pending_message_capacity(128);
    });
    let d = ConsensusModuleDecoder::default().header(header(&bytes, 105), 0);
    assert_eq!(d.next_session_id(), 41);
    assert_eq!(d.log_service_session_id(), Some(i64::MIN + 2));

    let base = (1i64 << 56) | i64::MIN;
    let bytes = encodes_as_java!("pending_message_tracker", PendingMessageTrackerEncoder, |e| {
        e.next_service_session_id(base | 5)
            .log_service_session_id(base | 4)
            .pending_message_capacity(64)
            .service_id(1);
    });
    let d = PendingMessageTrackerDecoder::default().header(header(&bytes, 107), 0);
    assert_eq!(d.next_service_session_id(), base | 5);
    assert_eq!(d.service_id(), 1);
}

#[test]
fn the_consensus_modules_mark_file_header() {
    use mark::{
        cluster_component_type::ClusterComponentType,
        mark_file_header_codec::{MarkFileHeaderDecoder, MarkFileHeaderEncoder},
        message_header_codec::MessageHeaderDecoder,
    };
    let bytes = fixture("mark_file_header_consensus_module");
    let mut out = vec![0; bytes.len()];
    {
        let mut e = MarkFileHeaderEncoder::default()
            .wrap(
                mark::WriteBuf::new(&mut out),
                mark::message_header_codec::ENCODED_LENGTH,
            )
            .header(0)
            .parent()
            .unwrap();
        e.version(version(0, 3, 0))
            .component_type(ClusterComponentType::CONSENSUS_MODULE)
            .activity_timestamp(1_700_000_001_456)
            .start_timestamp(1_700_000_001_000)
            .pid(4_343)
            .candidate_term_id(6)
            .archive_stream_id(10)
            .service_stream_id(104)
            .consensus_module_stream_id(105)
            .ingress_stream_id(101)
            .member_id(1)
            .service_id(-1)
            .header_length(8_192)
            .error_buffer_length(1_048_576)
            .cluster_id(3)
            .aeron_directory(b"/tmp/aeron-node1")
            .control_channel(b"aeron:ipc?term-length=128k")
            .ingress_channel(b"aeron:udp?term-length=64k")
            .service_name(b"")
            .authenticator(b"io.aeron.security.DefaultAuthenticatorSupplier")
            .services_cluster_dir(b"/tmp/cluster1");
    }
    assert_eq!(out, bytes);
    let d =
        MarkFileHeaderDecoder::default().header(MessageHeaderDecoder::default().wrap(mark::ReadBuf::new(&bytes), 0), 0);
    assert_eq!(d.component_type(), ClusterComponentType::CONSENSUS_MODULE);
    assert_eq!(d.ingress_stream_id(), 101);
}
