//! The archive's control protocol on the wire: the requests a client sends,
//! decoded with every length checked first (the generated readers panic on a
//! short buffer), and the messages the archive sends back.

#![forbid(unsafe_code)]
// The crate allows these lints for its generated bindings; this module is checked.
#![warn(
    clippy::all,
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    unsafe_op_in_unsafe_fn,
    unused_unsafe,
    unused_variables
)]

use crate::codecs::archive::{
    Decoder, ReadBuf, WriteBuf, archive_id_request_codec as archive_id, auth_connect_request_codec as auth_connect,
    boolean_type::BooleanType, close_session_request_codec as close_session,
    control_response_code::ControlResponseCode, control_response_codec, control_response_codec::ControlResponseEncoder,
    extend_recording_request_2_codec as extend_recording2, extend_recording_request_codec as extend_recording,
    keep_alive_request_codec as keep_alive, list_recording_request_codec as list_recording,
    list_recordings_for_uri_request_codec as list_for_uri, message_header_codec,
    message_header_codec::MessageHeaderDecoder, ping_codec, ping_codec::PingEncoder, recording_descriptor_codec,
    recording_position_request_codec as recording_position, recording_signal::RecordingSignal,
    recording_signal_event_codec, recording_signal_event_codec::RecordingSignalEventEncoder,
    source_location::SourceLocation, start_position_request_codec as start_position,
    start_recording_request_2_codec as start_recording2, start_recording_request_codec as start_recording,
    stop_position_request_codec as stop_position, stop_recording_by_identity_request_codec as stop_by_identity,
    stop_recording_request_codec as stop_recording, stop_recording_subscription_request_codec as stop_subscription,
};
use crate::codecs::archive::{
    bounded_replay_request_codec as bounded_replay, find_last_matching_recording_request_codec as find_last,
    list_recordings_request_codec as list_recordings, max_recorded_position_request_codec as max_recorded_position,
    purge_recording_request_codec as purge_recording, replay_request_codec as replay,
    stop_all_replays_request_codec as stop_all_replays, stop_replay_request_codec as stop_replay,
    truncate_recording_request_codec as truncate_recording,
};
use crate::codecs::archive::{
    replicate_request_2_codec as replicate2, replicate_request_codec as replicate,
    stop_replication_request_codec as stop_replication,
};

pub use crate::codecs::archive::control_response_code::ControlResponseCode as Code;
pub use crate::codecs::archive::recording_signal::RecordingSignal as Signal;

pub const HEADER_LENGTH: usize = message_header_codec::ENCODED_LENGTH;
pub const SCHEMA_ID: u16 = 101;
/// The schema version the archive writes.
pub const SCHEMA_VERSION: u16 = 14;
/// The control protocol's version, 1.12.0, in every control response.
pub const PROTOCOL_SEMANTIC_VERSION: i32 = 0x01_0C_00;
/// The protocol's major version a client must speak.
pub const PROTOCOL_MAJOR_VERSION: i32 = 1;
const CLIENT_INFO_SINCE_VERSION: u16 = 12;

/// The archive's error codes, sent as a control response's relevant id
/// (`ArchiveException`).
pub mod error_code {
    pub const GENERIC: i64 = 0;
    pub const ACTIVE_LISTING: i64 = 1;
    pub const ACTIVE_RECORDING: i64 = 2;
    pub const ACTIVE_SUBSCRIPTION: i64 = 3;
    pub const UNKNOWN_SUBSCRIPTION: i64 = 4;
    pub const UNKNOWN_RECORDING: i64 = 5;
    pub const UNKNOWN_REPLAY: i64 = 6;
    pub const MAX_REPLAYS: i64 = 7;
    pub const MAX_RECORDINGS: i64 = 8;
    pub const INVALID_EXTENSION: i64 = 9;
    pub const AUTHENTICATION_REJECTED: i64 = 10;
    pub const STORAGE_SPACE: i64 = 11;
    pub const UNKNOWN_REPLICATION: i64 = 12;
    pub const UNAUTHORISED_ACTION: i64 = 13;
    pub const REPLICATION_CONNECTION_FAILURE: i64 = 14;
    pub const EMPTY_RECORDING: i64 = 15;
    pub const INVALID_POSITION: i64 = 16;
}

/// Where a recording's publication is: on this archive's driver (recorded
/// through a spy) or not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Location {
    Local,
    Remote,
}

/// A request a client sends, as this archive serves it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    AuthConnect {
        correlation_id: i64,
        response_stream_id: i32,
        version: i32,
        response_channel: String,
        client_info: String,
    },
    CloseSession {
        control_session_id: i64,
    },
    KeepAlive {
        control_session_id: i64,
        correlation_id: i64,
    },
    ArchiveId {
        control_session_id: i64,
        correlation_id: i64,
    },
    /// v1 never stops on its own; v2 says. So for an extension.
    StartRecording {
        control_session_id: i64,
        correlation_id: i64,
        stream_id: i32,
        location: Location,
        auto_stop: bool,
        channel: String,
    },
    StopRecording {
        control_session_id: i64,
        correlation_id: i64,
        stream_id: i32,
        channel: String,
    },
    StopRecordingSubscription {
        control_session_id: i64,
        correlation_id: i64,
        subscription_id: i64,
    },
    StopRecordingByIdentity {
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    },
    ExtendRecording {
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        stream_id: i32,
        location: Location,
        auto_stop: bool,
        channel: String,
    },
    ListRecording {
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    },
    MaxRecordedPosition {
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    },
    FindLastMatchingRecording {
        control_session_id: i64,
        correlation_id: i64,
        min_recording_id: i64,
        session_id: i32,
        stream_id: i32,
        channel_fragment: Vec<u8>,
    },
    ListRecordings {
        control_session_id: i64,
        correlation_id: i64,
        from_recording_id: i64,
        record_count: i32,
    },
    ListRecordingsForUri {
        control_session_id: i64,
        correlation_id: i64,
        from_recording_id: i64,
        record_count: i32,
        stream_id: i32,
        channel_fragment: Vec<u8>,
    },
    StartPosition {
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    },
    StopPosition {
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    },
    RecordingPosition {
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    },
    /// A replay; bounded by a counter when it names one.
    Replay {
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        position: i64,
        length: i64,
        replay_stream_id: i32,
        /// -1: the archive's.
        file_io_max_length: i32,
        /// -1: none.
        replay_token: i64,
        replay_channel: String,
        limit_counter_id: Option<i32>,
    },
    StopReplay {
        control_session_id: i64,
        correlation_id: i64,
        replay_session_id: i64,
    },
    /// -1: every recording's replays.
    StopAllReplays {
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    },
    TruncateRecording {
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        position: i64,
    },
    PurgeRecording {
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    },
    /// Copy a recording from another archive (v1 and v2).
    Replicate(Box<Replicate>),
    StopReplication {
        control_session_id: i64,
        correlation_id: i64,
        replication_id: i64,
    },
    /// A request of the protocol this archive does not serve: answered
    /// with an error.
    Unsupported {
        template_id: u16,
        control_session_id: i64,
        correlation_id: i64,
    },
}

impl Request {
    /// The control session a request is for; `None` for a connect.
    pub fn control_session_id(&self) -> Option<i64> {
        match *self {
            Request::AuthConnect { .. } => None,
            Request::Replicate(ref r) => Some(r.control_session_id),
            Request::CloseSession { control_session_id }
            | Request::KeepAlive { control_session_id, .. }
            | Request::ArchiveId { control_session_id, .. }
            | Request::StartRecording { control_session_id, .. }
            | Request::StopRecording { control_session_id, .. }
            | Request::StopRecordingSubscription { control_session_id, .. }
            | Request::StopRecordingByIdentity { control_session_id, .. }
            | Request::ExtendRecording { control_session_id, .. }
            | Request::ListRecording { control_session_id, .. }
            | Request::ListRecordingsForUri { control_session_id, .. }
            | Request::ListRecordings { control_session_id, .. }
            | Request::MaxRecordedPosition { control_session_id, .. }
            | Request::FindLastMatchingRecording { control_session_id, .. }
            | Request::StartPosition { control_session_id, .. }
            | Request::StopPosition { control_session_id, .. }
            | Request::RecordingPosition { control_session_id, .. }
            | Request::Replay { control_session_id, .. }
            | Request::StopReplay { control_session_id, .. }
            | Request::StopAllReplays { control_session_id, .. }
            | Request::TruncateRecording { control_session_id, .. }
            | Request::PurgeRecording { control_session_id, .. }
            | Request::StopReplication { control_session_id, .. }
            | Request::Unsupported { control_session_id, .. } => Some(control_session_id),
        }
    }
}

/// A replication request's fields, as Java's adapter hands them on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replicate {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub src_recording_id: i64,
    /// -1: a new recording.
    pub dst_recording_id: i64,
    /// -1: as far as the source goes.
    pub stop_position: i64,
    pub channel_tag_id: i64,
    pub subscription_tag_id: i64,
    pub src_control_stream_id: i32,
    pub file_io_max_length: i32,
    pub replication_session_id: i32,
    pub src_control_channel: String,
    pub live_destination: String,
    /// Empty: the archive's.
    pub replication_channel: String,
    pub encoded_credentials: Vec<u8>,
    /// Empty: the archive client's.
    pub src_response_channel: String,
}

/// What a message that cannot be served is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Malformed {
    /// Not the archive's schema: Java throws, and counts it.
    Schema(u16),
    /// A template the archive does not know: Java ignores it.
    UnknownTemplate(u16),
    /// Too short for what it says it holds, or a value no client sends.
    Invalid(String),
}

impl std::fmt::Display for Malformed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Malformed::Schema(id) => write!(f, "expected schemaId={SCHEMA_ID}, actual={id}"),
            Malformed::UnknownTemplate(id) => write!(f, "unknown templateId={id}"),
            Malformed::Invalid(why) => f.write_str(why),
        }
    }
}

fn u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn i64_at(bytes: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("8 bytes"))
}

/// Whether the variable-length field at `limit` fits in the message.
fn check_var(message: &[u8], limit: usize) -> Result<(), Malformed> {
    let length = message
        .get(limit..limit + 4)
        .map(|b| u32::from_le_bytes(b.try_into().expect("4 bytes")) as usize)
        .ok_or_else(|| Malformed::Invalid(format!("variable-length field at {limit} past the message's end")))?;
    if limit + 4 + length > message.len() {
        return Err(Malformed::Invalid(format!(
            "variable-length field of {length} bytes at {limit} past the message's end"
        )));
    }
    Ok(())
}

/// A replay request's buffer size and token: absent from older clients'.
const FILE_IO_MAX_LENGTH_SINCE_VERSION: u16 = 7;
const REPLAY_TOKEN_SINCE_VERSION: u16 = 10;
/// A replication request's fields, by the version that brought them.
const REPLICATION_SESSION_ID_SINCE_VERSION: u16 = 8;
const SRC_RESPONSE_CHANNEL_SINCE_VERSION: u16 = 10;

/// Java decodes ASCII; a byte past it is kept as the replacement character.
fn ascii(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn location(value: SourceLocation) -> Location {
    match value {
        SourceLocation::LOCAL => Location::Local,
        _ => Location::Remote,
    }
}

/// The templates of the protocol this archive does not serve, each with its
/// control session and correlation ids first.
const UNSUPPORTED: [u16; 10] = [17, 53, 54, 55, 56, 57, 60, 62, 105, 107];

/// Decodes a control request.
pub fn decode_request(message: &[u8]) -> Result<Request, Malformed> {
    if message.len() < HEADER_LENGTH {
        return Err(Malformed::Invalid(format!(
            "message of {} bytes is shorter than its header",
            message.len()
        )));
    }
    let schema_id = u16_at(message, 4);
    if schema_id != SCHEMA_ID {
        return Err(Malformed::Schema(schema_id));
    }
    let block_length = u16_at(message, 0);
    let template_id = u16_at(message, 2);
    let version = u16_at(message, 6);
    if message.len() < HEADER_LENGTH + block_length as usize {
        return Err(Malformed::Invalid(format!(
            "message {template_id} of {} bytes is shorter than its block of {block_length}",
            message.len()
        )));
    }
    let needs = |min: u16| {
        if block_length < min {
            Err(Malformed::Invalid(format!(
                "message {template_id} has a block of {block_length}, expected at least {min}"
            )))
        } else {
            Ok(())
        }
    };
    let header = || MessageHeaderDecoder::default().wrap(ReadBuf::new(message), 0);

    macro_rules! session_and_recording {
        ($codec:ident :: $decoder:ident, $variant:ident) => {{
            needs($codec::SBE_BLOCK_LENGTH)?;
            let d = $codec::$decoder::default().header(header(), 0);
            Request::$variant {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                recording_id: d.recording_id(),
            }
        }};
    }

    let request = match template_id {
        auth_connect::SBE_TEMPLATE_ID => {
            needs(auth_connect::SBE_BLOCK_LENGTH)?;
            let mut d = auth_connect::AuthConnectRequestDecoder::default().header(header(), 0);
            check_var(message, d.get_limit())?;
            let c = d.response_channel_decoder();
            let response_channel = ascii(d.response_channel_slice(c));
            check_var(message, d.get_limit())?;
            let _credentials = d.encoded_credentials_decoder();
            let client_info = if version >= CLIENT_INFO_SINCE_VERSION {
                check_var(message, d.get_limit())?;
                let c = d.client_info_decoder();
                ascii(d.client_info_slice(c))
            } else {
                String::new()
            };
            Request::AuthConnect {
                correlation_id: d.correlation_id(),
                response_stream_id: d.response_stream_id(),
                version: d.version().unwrap_or(0),
                response_channel,
                client_info,
            }
        }
        close_session::SBE_TEMPLATE_ID => {
            needs(close_session::SBE_BLOCK_LENGTH)?;
            let d = close_session::CloseSessionRequestDecoder::default().header(header(), 0);
            Request::CloseSession {
                control_session_id: d.control_session_id(),
            }
        }
        keep_alive::SBE_TEMPLATE_ID => {
            needs(keep_alive::SBE_BLOCK_LENGTH)?;
            let d = keep_alive::KeepAliveRequestDecoder::default().header(header(), 0);
            Request::KeepAlive {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
            }
        }
        archive_id::SBE_TEMPLATE_ID => {
            needs(archive_id::SBE_BLOCK_LENGTH)?;
            let d = archive_id::ArchiveIdRequestDecoder::default().header(header(), 0);
            Request::ArchiveId {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
            }
        }
        start_recording::SBE_TEMPLATE_ID => {
            needs(start_recording::SBE_BLOCK_LENGTH)?;
            let mut d = start_recording::StartRecordingRequestDecoder::default().header(header(), 0);
            check_var(message, d.get_limit())?;
            let c = d.channel_decoder();
            Request::StartRecording {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                stream_id: d.stream_id(),
                location: location(d.source_location()),
                auto_stop: false,
                channel: ascii(d.channel_slice(c)),
            }
        }
        start_recording2::SBE_TEMPLATE_ID => {
            needs(start_recording2::SBE_BLOCK_LENGTH)?;
            let mut d = start_recording2::StartRecordingRequest2Decoder::default().header(header(), 0);
            check_var(message, d.get_limit())?;
            let c = d.channel_decoder();
            Request::StartRecording {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                stream_id: d.stream_id(),
                location: location(d.source_location()),
                auto_stop: d.auto_stop() == BooleanType::TRUE,
                channel: ascii(d.channel_slice(c)),
            }
        }
        stop_recording::SBE_TEMPLATE_ID => {
            needs(stop_recording::SBE_BLOCK_LENGTH)?;
            let mut d = stop_recording::StopRecordingRequestDecoder::default().header(header(), 0);
            check_var(message, d.get_limit())?;
            let c = d.channel_decoder();
            Request::StopRecording {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                stream_id: d.stream_id(),
                channel: ascii(d.channel_slice(c)),
            }
        }
        stop_subscription::SBE_TEMPLATE_ID => {
            needs(stop_subscription::SBE_BLOCK_LENGTH)?;
            let d = stop_subscription::StopRecordingSubscriptionRequestDecoder::default().header(header(), 0);
            Request::StopRecordingSubscription {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                subscription_id: d.subscription_id(),
            }
        }
        stop_by_identity::SBE_TEMPLATE_ID => {
            needs(stop_by_identity::SBE_BLOCK_LENGTH)?;
            let d = stop_by_identity::StopRecordingByIdentityRequestDecoder::default().header(header(), 0);
            Request::StopRecordingByIdentity {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                recording_id: d.recording_id(),
            }
        }
        extend_recording::SBE_TEMPLATE_ID => {
            needs(extend_recording::SBE_BLOCK_LENGTH)?;
            let mut d = extend_recording::ExtendRecordingRequestDecoder::default().header(header(), 0);
            check_var(message, d.get_limit())?;
            let c = d.channel_decoder();
            Request::ExtendRecording {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                recording_id: d.recording_id(),
                stream_id: d.stream_id(),
                location: location(d.source_location()),
                auto_stop: false,
                channel: ascii(d.channel_slice(c)),
            }
        }
        extend_recording2::SBE_TEMPLATE_ID => {
            needs(extend_recording2::SBE_BLOCK_LENGTH)?;
            let mut d = extend_recording2::ExtendRecordingRequest2Decoder::default().header(header(), 0);
            check_var(message, d.get_limit())?;
            let c = d.channel_decoder();
            Request::ExtendRecording {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                recording_id: d.recording_id(),
                stream_id: d.stream_id(),
                location: location(d.source_location()),
                auto_stop: d.auto_stop() == BooleanType::TRUE,
                channel: ascii(d.channel_slice(c)),
            }
        }
        list_recording::SBE_TEMPLATE_ID => {
            needs(list_recording::SBE_BLOCK_LENGTH)?;
            let d = list_recording::ListRecordingRequestDecoder::default().header(header(), 0);
            Request::ListRecording {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                recording_id: d.recording_id(),
            }
        }
        max_recorded_position::SBE_TEMPLATE_ID => {
            session_and_recording!(
                max_recorded_position::MaxRecordedPositionRequestDecoder,
                MaxRecordedPosition
            )
        }
        find_last::SBE_TEMPLATE_ID => {
            needs(find_last::SBE_BLOCK_LENGTH)?;
            let mut d = find_last::FindLastMatchingRecordingRequestDecoder::default().header(header(), 0);
            check_var(message, d.get_limit())?;
            let c = d.channel_decoder();
            Request::FindLastMatchingRecording {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                min_recording_id: d.min_recording_id(),
                session_id: d.session_id(),
                stream_id: d.stream_id(),
                channel_fragment: d.channel_slice(c).to_vec(),
            }
        }
        list_recordings::SBE_TEMPLATE_ID => {
            needs(list_recordings::SBE_BLOCK_LENGTH)?;
            let d = list_recordings::ListRecordingsRequestDecoder::default().header(header(), 0);
            Request::ListRecordings {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                from_recording_id: d.from_recording_id(),
                record_count: d.record_count(),
            }
        }
        list_for_uri::SBE_TEMPLATE_ID => {
            needs(list_for_uri::SBE_BLOCK_LENGTH)?;
            let mut d = list_for_uri::ListRecordingsForUriRequestDecoder::default().header(header(), 0);
            check_var(message, d.get_limit())?;
            let c = d.channel_decoder();
            Request::ListRecordingsForUri {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                from_recording_id: d.from_recording_id(),
                record_count: d.record_count(),
                stream_id: d.stream_id(),
                channel_fragment: d.channel_slice(c).to_vec(),
            }
        }
        start_position::SBE_TEMPLATE_ID => {
            session_and_recording!(start_position::StartPositionRequestDecoder, StartPosition)
        }
        stop_position::SBE_TEMPLATE_ID => {
            session_and_recording!(stop_position::StopPositionRequestDecoder, StopPosition)
        }
        recording_position::SBE_TEMPLATE_ID => {
            session_and_recording!(recording_position::RecordingPositionRequestDecoder, RecordingPosition)
        }
        replay::SBE_TEMPLATE_ID => {
            needs(replay::SBE_BLOCK_LENGTH)?;
            let mut d = replay::ReplayRequestDecoder::default().header(header(), 0);
            check_var(message, d.get_limit())?;
            let c = d.replay_channel_decoder();
            Request::Replay {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                recording_id: d.recording_id(),
                position: d.position(),
                length: d.length(),
                replay_stream_id: d.replay_stream_id(),
                file_io_max_length: if version >= FILE_IO_MAX_LENGTH_SINCE_VERSION {
                    d.file_io_max_length()
                } else {
                    -1
                },
                replay_token: if version >= REPLAY_TOKEN_SINCE_VERSION {
                    d.replay_token()
                } else {
                    -1
                },
                replay_channel: ascii(d.replay_channel_slice(c)),
                limit_counter_id: None,
            }
        }
        bounded_replay::SBE_TEMPLATE_ID => {
            needs(bounded_replay::SBE_BLOCK_LENGTH)?;
            let mut d = bounded_replay::BoundedReplayRequestDecoder::default().header(header(), 0);
            check_var(message, d.get_limit())?;
            let c = d.replay_channel_decoder();
            Request::Replay {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                recording_id: d.recording_id(),
                position: d.position(),
                length: d.length(),
                replay_stream_id: d.replay_stream_id(),
                file_io_max_length: if version >= FILE_IO_MAX_LENGTH_SINCE_VERSION {
                    d.file_io_max_length()
                } else {
                    -1
                },
                replay_token: if version >= REPLAY_TOKEN_SINCE_VERSION {
                    d.replay_token()
                } else {
                    -1
                },
                replay_channel: ascii(d.replay_channel_slice(c)),
                limit_counter_id: Some(d.limit_counter_id()),
            }
        }
        stop_replay::SBE_TEMPLATE_ID => {
            needs(stop_replay::SBE_BLOCK_LENGTH)?;
            let d = stop_replay::StopReplayRequestDecoder::default().header(header(), 0);
            Request::StopReplay {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                replay_session_id: d.replay_session_id(),
            }
        }
        stop_all_replays::SBE_TEMPLATE_ID => {
            session_and_recording!(stop_all_replays::StopAllReplaysRequestDecoder, StopAllReplays)
        }
        truncate_recording::SBE_TEMPLATE_ID => {
            needs(truncate_recording::SBE_BLOCK_LENGTH)?;
            let d = truncate_recording::TruncateRecordingRequestDecoder::default().header(header(), 0);
            Request::TruncateRecording {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                recording_id: d.recording_id(),
                position: d.position(),
            }
        }
        purge_recording::SBE_TEMPLATE_ID => {
            session_and_recording!(purge_recording::PurgeRecordingRequestDecoder, PurgeRecording)
        }
        replicate2::SBE_TEMPLATE_ID => {
            needs(replicate2::SBE_BLOCK_LENGTH)?;
            let mut d = replicate2::ReplicateRequest2Decoder::default().header(header(), 0);
            check_var(message, d.get_limit())?;
            let c = d.src_control_channel_decoder();
            let src_control_channel = ascii(d.src_control_channel_slice(c));
            check_var(message, d.get_limit())?;
            let c = d.live_destination_decoder();
            let live_destination = ascii(d.live_destination_slice(c));
            check_var(message, d.get_limit())?;
            let c = d.replication_channel_decoder();
            let replication_channel = ascii(d.replication_channel_slice(c));
            let encoded_credentials = if version >= REPLICATION_SESSION_ID_SINCE_VERSION {
                check_var(message, d.get_limit())?;
                let c = d.encoded_credentials_decoder();
                d.encoded_credentials_slice(c).to_vec()
            } else {
                Vec::new()
            };
            let src_response_channel = if version >= SRC_RESPONSE_CHANNEL_SINCE_VERSION {
                check_var(message, d.get_limit())?;
                let c = d.src_response_channel_decoder();
                ascii(d.src_response_channel_slice(c))
            } else {
                String::new()
            };
            Request::Replicate(Box::new(Replicate {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                src_recording_id: d.src_recording_id(),
                dst_recording_id: d.dst_recording_id(),
                stop_position: d.stop_position(),
                channel_tag_id: d.channel_tag_id(),
                subscription_tag_id: d.subscription_tag_id(),
                src_control_stream_id: d.src_control_stream_id(),
                file_io_max_length: if version >= FILE_IO_MAX_LENGTH_SINCE_VERSION {
                    d.file_io_max_length()
                } else {
                    -1
                },
                replication_session_id: if version >= REPLICATION_SESSION_ID_SINCE_VERSION {
                    d.replication_session_id()
                } else {
                    -1
                },
                src_control_channel,
                live_destination,
                replication_channel,
                encoded_credentials,
                src_response_channel,
            }))
        }
        replicate::SBE_TEMPLATE_ID => {
            needs(replicate::SBE_BLOCK_LENGTH)?;
            let mut d = replicate::ReplicateRequestDecoder::default().header(header(), 0);
            check_var(message, d.get_limit())?;
            let c = d.src_control_channel_decoder();
            let src_control_channel = ascii(d.src_control_channel_slice(c));
            check_var(message, d.get_limit())?;
            let c = d.live_destination_decoder();
            let live_destination = ascii(d.live_destination_slice(c));
            Request::Replicate(Box::new(Replicate {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                src_recording_id: d.src_recording_id(),
                dst_recording_id: d.dst_recording_id(),
                stop_position: -1,
                channel_tag_id: -1,
                subscription_tag_id: -1,
                src_control_stream_id: d.src_control_stream_id(),
                file_io_max_length: -1,
                replication_session_id: -1,
                src_control_channel,
                live_destination,
                replication_channel: String::new(),
                encoded_credentials: Vec::new(),
                src_response_channel: String::new(),
            }))
        }
        stop_replication::SBE_TEMPLATE_ID => {
            needs(stop_replication::SBE_BLOCK_LENGTH)?;
            let d = stop_replication::StopReplicationRequestDecoder::default().header(header(), 0);
            Request::StopReplication {
                control_session_id: d.control_session_id(),
                correlation_id: d.correlation_id(),
                replication_id: d.replication_id(),
            }
        }
        id if UNSUPPORTED.contains(&id) => {
            needs(16)?;
            Request::Unsupported {
                template_id: id,
                control_session_id: i64_at(message, HEADER_LENGTH),
                correlation_id: i64_at(message, HEADER_LENGTH + 8),
            }
        }
        id => return Err(Malformed::UnknownTemplate(id)),
    };
    Ok(request)
}

/// A control response (`ControlResponseProxy.sendResponse`).
pub fn control_response(
    control_session_id: i64,
    correlation_id: i64,
    relevant_id: i64,
    code: ControlResponseCode,
    error_message: &str,
) -> Vec<u8> {
    let mut buf = vec![0; HEADER_LENGTH + control_response_codec::SBE_BLOCK_LENGTH as usize + 4 + error_message.len()];
    let mut e = ControlResponseEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER_LENGTH)
        .header(0)
        .parent()
        .expect("encoder");
    e.control_session_id(control_session_id)
        .correlation_id(correlation_id)
        .relevant_id(relevant_id)
        .code(code)
        .version(PROTOCOL_SEMANTIC_VERSION)
        .error_message(error_message.as_bytes());
    buf
}

/// A recording signal (`ControlResponseProxy.sendSignal`).
pub fn recording_signal(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
    subscription_id: i64,
    position: i64,
    signal: RecordingSignal,
) -> Vec<u8> {
    let mut buf = vec![0; HEADER_LENGTH + recording_signal_event_codec::SBE_BLOCK_LENGTH as usize];
    let mut e = RecordingSignalEventEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER_LENGTH)
        .header(0)
        .parent()
        .expect("encoder");
    e.control_session_id(control_session_id)
        .correlation_id(correlation_id)
        .recording_id(recording_id)
        .subscription_id(subscription_id)
        .position(position)
        .signal(signal);
    buf
}

/// A ping, that the response path is alive.
pub fn ping(control_session_id: i64) -> Vec<u8> {
    let mut buf = vec![0; HEADER_LENGTH + ping_codec::SBE_BLOCK_LENGTH as usize];
    let mut e = PingEncoder::default()
        .wrap(WriteBuf::new(&mut buf), HEADER_LENGTH)
        .header(0)
        .parent()
        .expect("encoder");
    e.control_session_id(control_session_id);
    buf
}

/// A recording's descriptor for a listing (`ControlResponseProxy.sendDescriptor`):
/// the catalog's own bytes after a message header, the request's session
/// and correlation ids in place of the catalog's, the catalog's padding sent
/// as it is.
pub fn recording_descriptor(control_session_id: i64, correlation_id: i64, catalog_descriptor: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HEADER_LENGTH + catalog_descriptor.len());
    buf.extend_from_slice(&(recording_descriptor_codec::SBE_BLOCK_LENGTH).to_le_bytes());
    buf.extend_from_slice(&recording_descriptor_codec::SBE_TEMPLATE_ID.to_le_bytes());
    buf.extend_from_slice(&SCHEMA_ID.to_le_bytes());
    buf.extend_from_slice(&SCHEMA_VERSION.to_le_bytes());
    buf.extend_from_slice(&control_session_id.to_le_bytes());
    buf.extend_from_slice(&correlation_id.to_le_bytes());
    buf.extend_from_slice(&catalog_descriptor[16..]);
    buf
}
