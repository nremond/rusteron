//! The Rust codecs against bytes written by Aeron 1.53.3's Java codecs
//! (`java/GoldenCodecs.java`, which writes `tests/fixtures`): each message
//! decodes to the values Java set, and encodes back to the same bytes. The
//! requests are a client's, which the archive server decodes; the responses,
//! the signal, the catalog's structures and the mark file's header are what
//! it encodes.

use rusteron_archive_codecs::archive::{
    ReadBuf, WriteBuf, archive_id_request_codec::*, auth_connect_request_codec::*, boolean_type::BooleanType,
    bounded_replay_request_codec::*, catalog_header_codec::*, close_session_request_codec::*,
    control_response_code::ControlResponseCode, control_response_codec::*, extend_recording_request_2_codec::*,
    keep_alive_request_codec::*, list_recording_request_codec::*, list_recordings_for_uri_request_codec::*,
    message_header_codec, message_header_codec::MessageHeaderDecoder, ping_codec::*, purge_recording_request_codec::*,
    recording_descriptor_codec::*, recording_descriptor_header_codec::*, recording_position_request_codec::*,
    recording_signal::RecordingSignal, recording_signal_event_codec::*, recording_state::RecordingState,
    replay_request_codec::*, replicate_request_2_codec::*, source_location::SourceLocation,
    start_position_request_codec::*, start_recording_request_2_codec::*, start_recording_request_codec::*,
    stop_position_request_codec::*, stop_recording_by_identity_request_codec::*, stop_recording_request_codec::*,
    stop_recording_subscription_request_codec::*, stop_replay_request_codec::*, stop_replication_request_codec::*,
    truncate_recording_request_codec::*,
};
use rusteron_archive_codecs::mark;

const HEADER: usize = message_header_codec::ENCODED_LENGTH;
const CHANNEL: &str = "aeron:udp?endpoint=localhost:3333|alias=log|tags=7,8";
const REPLAY_CHANNEL: &str = "aeron:ipc?session-id=-77|alias=replay";
const RESPONSE_CHANNEL: &str = "aeron:udp?endpoint=localhost:0";
const SRC_CONTROL: &str = "aeron:udp?endpoint=localhost:8010";
const REPLICATION_CHANNEL: &str = "aeron:udp?endpoint=localhost:8012";
const ERROR: &str = "unknown recording id: 3";
const PROTOCOL: i32 = 0x01_0C_00;

fn fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}.hex", env!("CARGO_MANIFEST_DIR"));
    let hex = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let hex = hex.trim();
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex"))
        .collect()
}

fn header(bytes: &[u8], template_id: u16) -> MessageHeaderDecoder<ReadBuf<'_>> {
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(bytes), 0);
    assert_eq!(header.template_id(), template_id);
    assert_eq!(header.schema_id(), 101);
    assert_eq!(header.version(), 14);
    header
}

/// A buffer the length of `bytes`, for an encoder to fill.
fn out(bytes: &[u8]) -> Vec<u8> {
    vec![0; bytes.len()]
}

#[test]
fn auth_connect() {
    let bytes = fixture("auth_connect");
    let mut d = AuthConnectRequestDecoder::default().header(header(&bytes, 58), 0);
    assert_eq!(
        (d.correlation_id(), d.response_stream_id(), d.version()),
        (101, 20, Some(PROTOCOL))
    );
    let channel = d.response_channel_decoder();
    assert_eq!(d.response_channel_slice(channel), RESPONSE_CHANNEL.as_bytes());
    let credentials = d.encoded_credentials_decoder();
    assert_eq!(d.encoded_credentials_slice(credentials), b"");
    let info = d.client_info_decoder();
    assert_eq!(d.client_info_slice(info), b"cm-1");
    assert_eq!(HEADER + d.encoded_length(), bytes.len());

    let mut buf = out(&bytes);
    let mut e = AuthConnectRequestEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.correlation_id(101)
        .response_stream_id(20)
        .version(PROTOCOL)
        .response_channel(RESPONSE_CHANNEL.as_bytes())
        .encoded_credentials(b"")
        .client_info(b"cm-1");
    assert_eq!(buf, bytes);
}

#[test]
fn session_requests() {
    let bytes = fixture("archive_id");
    let d = ArchiveIdRequestDecoder::default().header(header(&bytes, 68), 0);
    assert_eq!((d.control_session_id(), d.correlation_id()), (11, 102));
    let bytes = fixture("keep_alive");
    let d = KeepAliveRequestDecoder::default().header(header(&bytes, 61), 0);
    assert_eq!((d.control_session_id(), d.correlation_id()), (11, 103));
    let bytes = fixture("close_session");
    let d = CloseSessionRequestDecoder::default().header(header(&bytes, 3), 0);
    assert_eq!(d.control_session_id(), 11);
}

#[test]
fn start_and_stop_recording() {
    let bytes = fixture("start_recording");
    let mut d = StartRecordingRequestDecoder::default().header(header(&bytes, 4), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.stream_id()),
        (11, 104, 33)
    );
    assert_eq!(d.source_location(), SourceLocation::REMOTE);
    let channel = d.channel_decoder();
    assert_eq!(d.channel_slice(channel), CHANNEL.as_bytes());

    let bytes = fixture("stop_recording");
    let mut d = StopRecordingRequestDecoder::default().header(header(&bytes, 5), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.stream_id()),
        (11, 105, 33)
    );
    let channel = d.channel_decoder();
    assert_eq!(d.channel_slice(channel), CHANNEL.as_bytes());

    let bytes = fixture("start_recording2");
    let mut d = StartRecordingRequest2Decoder::default().header(header(&bytes, 63), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.stream_id()),
        (11, 106, 34)
    );
    assert_eq!(
        (d.source_location(), d.auto_stop()),
        (SourceLocation::LOCAL, BooleanType::TRUE)
    );
    let channel = d.channel_decoder();
    assert_eq!(d.channel_slice(channel), CHANNEL.as_bytes());
    assert_eq!(HEADER + d.encoded_length(), bytes.len());
    let mut buf = out(&bytes);
    let mut e = StartRecordingRequest2Encoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.control_session_id(11)
        .correlation_id(106)
        .stream_id(34)
        .source_location(SourceLocation::LOCAL)
        .auto_stop(BooleanType::TRUE)
        .channel(CHANNEL.as_bytes());
    assert_eq!(buf, bytes);

    let bytes = fixture("extend_recording2");
    let mut d = ExtendRecordingRequest2Decoder::default().header(header(&bytes, 64), 0);
    assert_eq!(
        (
            d.control_session_id(),
            d.correlation_id(),
            d.recording_id(),
            d.stream_id()
        ),
        (11, 107, 5, 35)
    );
    assert_eq!(
        (d.source_location(), d.auto_stop()),
        (SourceLocation::REMOTE, BooleanType::FALSE)
    );
    let channel = d.channel_decoder();
    assert_eq!(d.channel_slice(channel), CHANNEL.as_bytes());

    let bytes = fixture("stop_recording_subscription");
    let d = StopRecordingSubscriptionRequestDecoder::default().header(header(&bytes, 14), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.subscription_id()),
        (11, 108, 4_242)
    );
    let bytes = fixture("stop_recording_by_identity");
    let d = StopRecordingByIdentityRequestDecoder::default().header(header(&bytes, 65), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.recording_id()),
        (11, 109, 6)
    );
}

#[test]
fn replays() {
    let bytes = fixture("replay");
    let mut d = ReplayRequestDecoder::default().header(header(&bytes, 6), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.recording_id()),
        (11, 110, 7)
    );
    assert_eq!((d.position(), d.length(), d.replay_stream_id()), (4_096, i64::MAX, 66));
    assert_eq!((d.file_io_max_length(), d.replay_token()), (1 << 20, -9));
    let channel = d.replay_channel_decoder();
    assert_eq!(d.replay_channel_slice(channel), REPLAY_CHANNEL.as_bytes());

    let bytes = fixture("bounded_replay");
    let mut d = BoundedReplayRequestDecoder::default().header(header(&bytes, 18), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.recording_id()),
        (11, 111, 8)
    );
    assert_eq!(
        (d.position(), d.length(), d.limit_counter_id(), d.replay_stream_id()),
        (-1, -1, 23, 67)
    );
    assert_eq!((d.file_io_max_length(), d.replay_token()), (65_536, -10));
    let channel = d.replay_channel_decoder();
    assert_eq!(d.replay_channel_slice(channel), REPLAY_CHANNEL.as_bytes());

    let bytes = fixture("stop_replay");
    let d = StopReplayRequestDecoder::default().header(header(&bytes, 7), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.replay_session_id()),
        (11, 112, (3 << 32) | 0x7777)
    );
}

#[test]
fn queries_and_changes() {
    let bytes = fixture("list_recording");
    let d = ListRecordingRequestDecoder::default().header(header(&bytes, 10), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.recording_id()),
        (11, 113, 9)
    );

    let bytes = fixture("list_recordings_for_uri");
    let mut d = ListRecordingsForUriRequestDecoder::default().header(header(&bytes, 9), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.from_recording_id()),
        (11, 114, 2)
    );
    assert_eq!((d.record_count(), d.stream_id()), (i32::MAX, 200));
    let channel = d.channel_decoder();
    assert_eq!(d.channel_slice(channel), b"alias=demo-events");

    let bytes = fixture("start_position");
    let d = StartPositionRequestDecoder::default().header(header(&bytes, 52), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.recording_id()),
        (11, 115, 10)
    );
    let bytes = fixture("stop_position");
    let d = StopPositionRequestDecoder::default().header(header(&bytes, 15), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.recording_id()),
        (11, 116, 11)
    );
    let bytes = fixture("recording_position");
    let d = RecordingPositionRequestDecoder::default().header(header(&bytes, 12), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.recording_id()),
        (11, 117, 12)
    );

    let bytes = fixture("truncate_recording");
    let d = TruncateRecordingRequestDecoder::default().header(header(&bytes, 13), 0);
    assert_eq!(
        (
            d.control_session_id(),
            d.correlation_id(),
            d.recording_id(),
            d.position()
        ),
        (11, 118, 13, 8_192)
    );
    let bytes = fixture("purge_recording");
    let d = PurgeRecordingRequestDecoder::default().header(header(&bytes, 104), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.recording_id()),
        (11, 119, 14)
    );
}

#[test]
fn replication() {
    let bytes = fixture("replicate2");
    let mut d = ReplicateRequest2Decoder::default().header(header(&bytes, 66), 0);
    assert_eq!((d.control_session_id(), d.correlation_id()), (11, 120));
    assert_eq!(
        (d.src_recording_id(), d.dst_recording_id(), d.stop_position()),
        (15, -1, 1_048_576)
    );
    assert_eq!(
        (d.channel_tag_id(), d.subscription_tag_id(), d.src_control_stream_id()),
        (-1, -1, 10)
    );
    assert_eq!((d.file_io_max_length(), d.replication_session_id()), (8 << 20, -55));
    let src = d.src_control_channel_decoder();
    assert_eq!(d.src_control_channel_slice(src), SRC_CONTROL.as_bytes());
    let live = d.live_destination_decoder();
    assert_eq!(d.live_destination_slice(live), b"");
    let channel = d.replication_channel_decoder();
    assert_eq!(d.replication_channel_slice(channel), REPLICATION_CHANNEL.as_bytes());
    let credentials = d.encoded_credentials_decoder();
    assert_eq!(d.encoded_credentials_slice(credentials), b"");
    let response = d.src_response_channel_decoder();
    assert_eq!(d.src_response_channel_slice(response), b"");
    assert_eq!(HEADER + d.encoded_length(), bytes.len());

    let bytes = fixture("stop_replication");
    let d = StopReplicationRequestDecoder::default().header(header(&bytes, 51), 0);
    assert_eq!(
        (d.control_session_id(), d.correlation_id(), d.replication_id()),
        (11, 121, 16)
    );
}

#[test]
fn control_responses() {
    for (name, correlation_id, relevant_id, code, message) in [
        ("control_response_ok", 101, 11, ControlResponseCode::OK, ""),
        ("control_response_error", 113, 5, ControlResponseCode::ERROR, ERROR),
    ] {
        let bytes = fixture(name);
        header(&bytes, 1);
        let mut buf = out(&bytes);
        let mut e = ControlResponseEncoder::default()
            .wrap(WriteBuf::new(&mut buf), HEADER)
            .header(0)
            .parent()
            .unwrap();
        e.control_session_id(11)
            .correlation_id(correlation_id)
            .relevant_id(relevant_id)
            .code(code)
            .version(PROTOCOL)
            .error_message(message.as_bytes());
        assert_eq!(buf, bytes, "{name}");
    }
}

#[test]
fn recording_signal_and_ping() {
    let bytes = fixture("recording_signal");
    header(&bytes, 24);
    let mut buf = out(&bytes);
    let mut e = RecordingSignalEventEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.control_session_id(11)
        .correlation_id(106)
        .recording_id(17)
        .subscription_id(4_243)
        .position(65_536)
        .signal(RecordingSignal::REPLICATE_END);
    assert_eq!(buf, bytes);

    let bytes = fixture("ping");
    header(&bytes, 106);
    let mut buf = out(&bytes);
    let mut e = PingEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .unwrap();
    e.control_session_id(11);
    assert_eq!(buf, bytes);
}

/// The descriptor of `GoldenCodecs.descriptor`.
fn descriptor<'a>(e: &mut RecordingDescriptorEncoder<'a>) {
    e.control_session_id(0)
        .correlation_id(0)
        .recording_id(17)
        .start_timestamp(1_700_000_000_000)
        .stop_timestamp(1_700_000_001_000)
        .start_position(1_024)
        .stop_position(1_048_576)
        .initial_term_id(-5)
        .segment_file_length(128 << 20)
        .term_buffer_length(64 << 10)
        .mtu_length(1_408)
        .session_id(-123_456)
        .stream_id(100)
        .stripped_channel(b"aeron:udp?endpoint=localhost:3333|alias=log")
        .original_channel(CHANNEL.as_bytes())
        .source_identity(b"127.0.0.1:52341");
}

#[test]
fn recording_descriptor() {
    let bytes = fixture("recording_descriptor");
    let mut d = RecordingDescriptorDecoder::default().header(header(&bytes, 22), 0);
    assert_eq!(
        (d.recording_id(), d.start_position(), d.stop_position()),
        (17, 1_024, 1_048_576)
    );
    assert_eq!(
        (d.initial_term_id(), d.term_buffer_length(), d.mtu_length()),
        (-5, 64 << 10, 1_408)
    );
    let stripped = d.stripped_channel_decoder();
    assert_eq!(
        d.stripped_channel_slice(stripped),
        b"aeron:udp?endpoint=localhost:3333|alias=log"
    );
    let original = d.original_channel_decoder();
    assert_eq!(d.original_channel_slice(original), CHANNEL.as_bytes());
    let source = d.source_identity_decoder();
    assert_eq!(d.source_identity_slice(source), b"127.0.0.1:52341");
    let mut buf = out(&bytes);
    let mut e = RecordingDescriptorEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER)
        .header(0)
        .parent()
        .unwrap();
    descriptor(&mut e);
    assert_eq!(buf, bytes);
}

#[test]
fn catalog_structures() {
    let bytes = fixture("catalog_header");
    assert_eq!(bytes.len(), 32, "the header's block, its reserved byte last");
    let d = CatalogHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0, 32, 14);
    assert_eq!(
        (d.version(), d.length(), d.next_recording_id(), d.alignment()),
        (0x03_01_00, 32, 18, 64)
    );

    let bytes = fixture("catalog_entry");
    let mut buf = out(&bytes);
    let mut e = RecordingDescriptorEncoder::default().wrap(WriteBuf::new(&mut buf), 32);
    descriptor(&mut e);
    let length = e.encoded_length();
    let mut h = RecordingDescriptorHeaderEncoder::default().wrap(WriteBuf::new(&mut buf), 0);
    h.length(length as i32)
        .state(RecordingState::VALID)
        .checksum(0x1234_5678);
    assert_eq!(buf, bytes);
    let d = RecordingDescriptorHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0, 32, 14);
    assert_eq!(
        (d.length(), d.state(), d.checksum()),
        (length as i32, RecordingState::VALID, 0x1234_5678)
    );
}

#[test]
fn mark_file_header() {
    let bytes = fixture("mark_file_header");
    let h = mark::message_header_codec::MessageHeaderDecoder::default().wrap(mark::ReadBuf::new(&bytes), 0);
    assert_eq!(
        (h.block_length(), h.template_id(), h.schema_id(), h.version()),
        (128, 200, 100, 2)
    );
    let mut buf = out(&bytes);
    let mut e = mark::mark_file_header_codec::MarkFileHeaderEncoder::default()
        .wrap(
            mark::WriteBuf::new(&mut buf),
            mark::message_header_codec::ENCODED_LENGTH,
        )
        .header(0)
        .parent()
        .unwrap();
    e.version(0x03_01_00)
        .activity_timestamp(1_700_000_000_123)
        .start_timestamp(1_700_000_000_000)
        .pid(4_321)
        .control_stream_id(10)
        .local_control_stream_id(11)
        .events_stream_id(30)
        .header_length(8_192)
        .error_buffer_length(1 << 20)
        .archive_id(1_001)
        .control_channel(b"aeron:udp?endpoint=localhost:8010")
        .local_control_channel(b"aeron:ipc?term-length=64k")
        .events_channel(b"aeron:udp?control-mode=dynamic")
        .aeron_directory(b"/dev/shm/aeron-node0");
    assert_eq!(buf, bytes);
}
