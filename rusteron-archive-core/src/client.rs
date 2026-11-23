//! Aeron Archive's control protocol from the client's side: the requests a
//! client sends, as Aeron's client encodes them, and the messages an archive
//! sends back, decoded with every length checked first. The archive is a
//! client itself when it replicates a recording from another archive.

use crate::catalog::Descriptor;
use crate::protocol::{Code, HEADER_LENGTH, Malformed, SCHEMA_ID, Signal};
use rusteron_archive_codecs::archive::{
    Decoder, ReadBuf, WriteBuf, boolean_type::BooleanType, control_response_codec,
    control_response_codec::ControlResponseDecoder, message_header_codec::MessageHeaderDecoder, ping_codec,
    ping_codec::PingDecoder, recording_descriptor_codec, recording_descriptor_codec::RecordingDescriptorDecoder,
    recording_signal_event_codec, recording_signal_event_codec::RecordingSignalEventDecoder,
    source_location::SourceLocation,
};
use rusteron_archive_codecs::archive::{
    archive_id_request_codec as archive_id, auth_connect_request_codec as auth_connect,
    bounded_replay_request_codec as bounded_replay, close_session_request_codec as close_session,
    extend_recording_request_2_codec as extend2, find_last_matching_recording_request_codec as find_last,
    keep_alive_request_codec as keep_alive, list_recording_request_codec as list_recording,
    list_recordings_for_uri_request_codec as list_for_uri, list_recordings_request_codec as list_recordings,
    max_recorded_position_request_codec as max_recorded, purge_recording_request_codec as purge,
    recording_position_request_codec as recording_position, replay_request_codec as replay,
    replicate_request_2_codec as replicate2, start_position_request_codec as start_position,
    start_recording_request_2_codec as start2, start_recording_request_codec as start1,
    stop_all_replays_request_codec as stop_all_replays, stop_position_request_codec as stop_position,
    stop_recording_by_identity_request_codec as stop_identity, stop_recording_request_codec as stop1,
    stop_recording_subscription_request_codec as stop_subscription, stop_replay_request_codec as stop_replay,
    stop_replication_request_codec as stop_replication, truncate_recording_request_codec as truncate,
};

/// The protocol version Aeron 1.54.2's client sends.
pub const VERSION: i32 = 0x01_0C_00;

macro_rules! encode {
    ($codec:ident :: $encoder:ident, $var:expr, |$e:ident| $body:block) => {{
        let mut buf = vec![0u8; HEADER_LENGTH + $codec::SBE_BLOCK_LENGTH as usize + $var];
        {
            let mut $e = $codec::$encoder::default()
                .wrap(WriteBuf::new(&mut buf), HEADER_LENGTH)
                .header(0)
                .parent()
                .expect("encoder");
            $body
        }
        buf
    }};
}

fn location(local: bool) -> SourceLocation {
    if local {
        SourceLocation::LOCAL
    } else {
        SourceLocation::REMOTE
    }
}

fn boolean(value: bool) -> BooleanType {
    if value { BooleanType::TRUE } else { BooleanType::FALSE }
}

pub fn auth_connect(
    correlation_id: i64,
    response_stream_id: i32,
    version: i32,
    response_channel: &str,
    client_info: &str,
) -> Vec<u8> {
    auth_connect_with(
        correlation_id,
        response_stream_id,
        version,
        response_channel,
        b"",
        client_info,
    )
}

/// A connect with credentials, for an archive with an authenticator.
pub fn auth_connect_with(
    correlation_id: i64,
    response_stream_id: i32,
    version: i32,
    response_channel: &str,
    encoded_credentials: &[u8],
    client_info: &str,
) -> Vec<u8> {
    encode!(
        auth_connect::AuthConnectRequestEncoder,
        12 + response_channel.len() + encoded_credentials.len() + client_info.len(),
        |e| {
            e.correlation_id(correlation_id)
                .response_stream_id(response_stream_id)
                .version(version)
                .response_channel(response_channel.as_bytes())
                .encoded_credentials(encoded_credentials)
                .client_info(client_info.as_bytes());
        }
    )
}

pub fn close_session(control_session_id: i64) -> Vec<u8> {
    encode!(close_session::CloseSessionRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id);
    })
}

pub fn keep_alive(control_session_id: i64, correlation_id: i64) -> Vec<u8> {
    encode!(keep_alive::KeepAliveRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id).correlation_id(correlation_id);
    })
}

pub fn archive_id(control_session_id: i64, correlation_id: i64) -> Vec<u8> {
    encode!(archive_id::ArchiveIdRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id).correlation_id(correlation_id);
    })
}

/// v1: never stops on its own.
pub fn start_recording(
    control_session_id: i64,
    correlation_id: i64,
    stream_id: i32,
    local: bool,
    channel: &str,
) -> Vec<u8> {
    encode!(start1::StartRecordingRequestEncoder, 4 + channel.len(), |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .stream_id(stream_id)
            .source_location(location(local))
            .channel(channel.as_bytes());
    })
}

pub fn start_recording2(
    control_session_id: i64,
    correlation_id: i64,
    stream_id: i32,
    local: bool,
    auto_stop: bool,
    channel: &str,
) -> Vec<u8> {
    encode!(start2::StartRecordingRequest2Encoder, 4 + channel.len(), |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .stream_id(stream_id)
            .source_location(location(local))
            .auto_stop(boolean(auto_stop))
            .channel(channel.as_bytes());
    })
}

pub fn stop_recording(control_session_id: i64, correlation_id: i64, stream_id: i32, channel: &str) -> Vec<u8> {
    encode!(stop1::StopRecordingRequestEncoder, 4 + channel.len(), |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .stream_id(stream_id)
            .channel(channel.as_bytes());
    })
}

pub fn stop_recording_subscription(control_session_id: i64, correlation_id: i64, subscription_id: i64) -> Vec<u8> {
    encode!(stop_subscription::StopRecordingSubscriptionRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .subscription_id(subscription_id);
    })
}

pub fn stop_recording_by_identity(control_session_id: i64, correlation_id: i64, recording_id: i64) -> Vec<u8> {
    encode!(stop_identity::StopRecordingByIdentityRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id);
    })
}

#[allow(clippy::too_many_arguments)]
pub fn extend_recording2(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
    stream_id: i32,
    local: bool,
    auto_stop: bool,
    channel: &str,
) -> Vec<u8> {
    encode!(extend2::ExtendRecordingRequest2Encoder, 4 + channel.len(), |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id)
            .stream_id(stream_id)
            .source_location(location(local))
            .auto_stop(boolean(auto_stop))
            .channel(channel.as_bytes());
    })
}

pub fn list_recording(control_session_id: i64, correlation_id: i64, recording_id: i64) -> Vec<u8> {
    encode!(list_recording::ListRecordingRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id);
    })
}

pub fn list_recordings_for_uri(
    control_session_id: i64,
    correlation_id: i64,
    from_recording_id: i64,
    record_count: i32,
    stream_id: i32,
    channel_fragment: &str,
) -> Vec<u8> {
    encode!(
        list_for_uri::ListRecordingsForUriRequestEncoder,
        4 + channel_fragment.len(),
        |e| {
            e.control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .from_recording_id(from_recording_id)
                .record_count(record_count)
                .stream_id(stream_id)
                .channel(channel_fragment.as_bytes());
        }
    )
}

pub fn start_position(control_session_id: i64, correlation_id: i64, recording_id: i64) -> Vec<u8> {
    encode!(start_position::StartPositionRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id);
    })
}

pub fn stop_position(control_session_id: i64, correlation_id: i64, recording_id: i64) -> Vec<u8> {
    encode!(stop_position::StopPositionRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id);
    })
}

pub fn recording_position(control_session_id: i64, correlation_id: i64, recording_id: i64) -> Vec<u8> {
    encode!(recording_position::RecordingPositionRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id);
    })
}
#[allow(clippy::too_many_arguments)]
pub fn replay(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
    position: i64,
    length: i64,
    replay_stream_id: i32,
    file_io_max_length: i32,
    channel: &str,
) -> Vec<u8> {
    encode!(replay::ReplayRequestEncoder, 4 + channel.len(), |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id)
            .position(position)
            .length(length)
            .replay_stream_id(replay_stream_id)
            .file_io_max_length(file_io_max_length)
            .replay_token(-1)
            .replay_channel(channel.as_bytes());
    })
}

#[allow(clippy::too_many_arguments)]
pub fn bounded_replay(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
    position: i64,
    length: i64,
    limit_counter_id: i32,
    replay_stream_id: i32,
    channel: &str,
) -> Vec<u8> {
    encode!(bounded_replay::BoundedReplayRequestEncoder, 4 + channel.len(), |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id)
            .position(position)
            .length(length)
            .limit_counter_id(limit_counter_id)
            .replay_stream_id(replay_stream_id)
            .file_io_max_length(-1)
            .replay_token(-1)
            .replay_channel(channel.as_bytes());
    })
}

pub fn stop_replay(control_session_id: i64, correlation_id: i64, replay_session_id: i64) -> Vec<u8> {
    encode!(stop_replay::StopReplayRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .replay_session_id(replay_session_id);
    })
}

pub fn stop_all_replays(control_session_id: i64, correlation_id: i64, recording_id: i64) -> Vec<u8> {
    encode!(stop_all_replays::StopAllReplaysRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id);
    })
}

pub fn truncate_recording(control_session_id: i64, correlation_id: i64, recording_id: i64, position: i64) -> Vec<u8> {
    encode!(truncate::TruncateRecordingRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id)
            .position(position);
    })
}

pub fn purge_recording(control_session_id: i64, correlation_id: i64, recording_id: i64) -> Vec<u8> {
    encode!(purge::PurgeRecordingRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id);
    })
}

/// A listing of every recording, which this archive does not serve.
pub fn list_recordings(control_session_id: i64, correlation_id: i64, from: i64, count: i32) -> Vec<u8> {
    encode!(list_recordings::ListRecordingsRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .from_recording_id(from)
            .record_count(count);
    })
}

#[allow(clippy::too_many_arguments)]
pub fn replicate2(
    control_session_id: i64,
    correlation_id: i64,
    src_recording_id: i64,
    dst_recording_id: i64,
    stop_position: i64,
    src_control_stream_id: i32,
    replication_session_id: i32,
    src_control_channel: &str,
    replication_channel: &str,
) -> Vec<u8> {
    encode!(
        replicate2::ReplicateRequest2Encoder,
        5 * 4 + src_control_channel.len() + replication_channel.len(),
        |e| {
            e.control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .src_recording_id(src_recording_id)
                .dst_recording_id(dst_recording_id)
                .stop_position(stop_position)
                .channel_tag_id(-1)
                .subscription_tag_id(-1)
                .src_control_stream_id(src_control_stream_id)
                .file_io_max_length(-1)
                .replication_session_id(replication_session_id)
                .src_control_channel(src_control_channel.as_bytes())
                .live_destination(b"")
                .replication_channel(replication_channel.as_bytes())
                .encoded_credentials(b"")
                .src_response_channel(b"");
        }
    )
}

pub fn max_recorded_position(control_session_id: i64, correlation_id: i64, recording_id: i64) -> Vec<u8> {
    encode!(max_recorded::MaxRecordedPositionRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id);
    })
}

pub fn find_last_matching_recording(
    control_session_id: i64,
    correlation_id: i64,
    min_recording_id: i64,
    session_id: i32,
    stream_id: i32,
    channel_fragment: &str,
) -> Vec<u8> {
    encode!(
        find_last::FindLastMatchingRecordingRequestEncoder,
        4 + channel_fragment.len(),
        |e| {
            e.control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .min_recording_id(min_recording_id)
                .session_id(session_id)
                .stream_id(stream_id)
                .channel(channel_fragment.as_bytes());
        }
    )
}

pub fn stop_replication(control_session_id: i64, correlation_id: i64, replication_id: i64) -> Vec<u8> {
    encode!(stop_replication::StopReplicationRequestEncoder, 0, |e| {
        e.control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .replication_id(replication_id);
    })
}

/// A message the archive sends a client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    Control {
        control_session_id: i64,
        correlation_id: i64,
        relevant_id: i64,
        code: Code,
        version: i32,
        message: String,
    },
    Signal {
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        subscription_id: i64,
        position: i64,
        signal: Signal,
    },
    Descriptor {
        control_session_id: i64,
        correlation_id: i64,
        descriptor: Descriptor,
    },
    Ping {
        control_session_id: i64,
    },
}

impl Response {
    /// A message from a trusted archive: panics on a malformed one.
    pub fn decode(bytes: &[u8]) -> Response {
        decode_response(bytes).expect("a well-formed response")
    }

    fn decode_unchecked(bytes: &[u8]) -> Response {
        let header = || MessageHeaderDecoder::default().wrap(ReadBuf::new(bytes), 0);
        match header().template_id() {
            1 => {
                let mut d = ControlResponseDecoder::default().header(header(), 0);
                let c = d.error_message_decoder();
                let message = String::from_utf8_lossy(d.error_message_slice(c)).into_owned();
                Response::Control {
                    control_session_id: d.control_session_id(),
                    correlation_id: d.correlation_id(),
                    relevant_id: d.relevant_id(),
                    code: d.code(),
                    version: d.version().unwrap_or(0),
                    message,
                }
            }
            24 => {
                let d = RecordingSignalEventDecoder::default().header(header(), 0);
                Response::Signal {
                    control_session_id: d.control_session_id(),
                    correlation_id: d.correlation_id(),
                    recording_id: d.recording_id(),
                    subscription_id: d.subscription_id(),
                    position: d.position(),
                    signal: d.signal(),
                }
            }
            22 => {
                let mut d = RecordingDescriptorDecoder::default().header(header(), 0);
                let (control_session_id, correlation_id) = (d.control_session_id(), d.correlation_id());
                let mut descriptor = Descriptor {
                    control_session_id,
                    correlation_id,
                    recording_id: d.recording_id(),
                    start_timestamp: d.start_timestamp(),
                    stop_timestamp: d.stop_timestamp(),
                    start_position: d.start_position(),
                    stop_position: d.stop_position(),
                    initial_term_id: d.initial_term_id(),
                    segment_file_length: d.segment_file_length(),
                    term_buffer_length: d.term_buffer_length(),
                    mtu_length: d.mtu_length(),
                    session_id: d.session_id(),
                    stream_id: d.stream_id(),
                    stripped_channel: String::new(),
                    original_channel: String::new(),
                    source_identity: String::new(),
                };
                let c = d.stripped_channel_decoder();
                descriptor.stripped_channel = String::from_utf8_lossy(d.stripped_channel_slice(c)).into_owned();
                let c = d.original_channel_decoder();
                descriptor.original_channel = String::from_utf8_lossy(d.original_channel_slice(c)).into_owned();
                let c = d.source_identity_decoder();
                descriptor.source_identity = String::from_utf8_lossy(d.source_identity_slice(c)).into_owned();
                Response::Descriptor {
                    control_session_id,
                    correlation_id,
                    descriptor,
                }
            }
            106 => Response::Ping {
                control_session_id: PingDecoder::default().header(header(), 0).control_session_id(),
            },
            other => panic!("not a response: template {other}"),
        }
    }
}

fn u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
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

/// Decodes a message from an archive, its lengths checked first.
pub fn decode_response(bytes: &[u8]) -> Result<Response, Malformed> {
    if bytes.len() < HEADER_LENGTH {
        return Err(Malformed::Invalid(format!(
            "message of {} bytes is shorter than its header",
            bytes.len()
        )));
    }
    let schema_id = u16_at(bytes, 4);
    if schema_id != SCHEMA_ID {
        return Err(Malformed::Schema(schema_id));
    }
    let block_length = u16_at(bytes, 0);
    let template_id = u16_at(bytes, 2);
    if bytes.len() < HEADER_LENGTH + block_length as usize {
        return Err(Malformed::Invalid(format!(
            "message {template_id} of {} bytes is shorter than its block of {block_length}",
            bytes.len()
        )));
    }
    let header = || MessageHeaderDecoder::default().wrap(ReadBuf::new(bytes), 0);
    let needs = |min: u16| {
        if block_length < min {
            Err(Malformed::Invalid(format!(
                "message {template_id} has a block of {block_length}, expected at least {min}"
            )))
        } else {
            Ok(())
        }
    };
    match template_id {
        control_response_codec::SBE_TEMPLATE_ID => {
            needs(control_response_codec::SBE_BLOCK_LENGTH)?;
            let d = ControlResponseDecoder::default().header(header(), 0);
            check_var(bytes, d.get_limit())?;
        }
        recording_signal_event_codec::SBE_TEMPLATE_ID => {
            needs(recording_signal_event_codec::SBE_BLOCK_LENGTH)?;
        }
        recording_descriptor_codec::SBE_TEMPLATE_ID => {
            needs(recording_descriptor_codec::SBE_BLOCK_LENGTH)?;
            let mut d = RecordingDescriptorDecoder::default().header(header(), 0);
            check_var(bytes, d.get_limit())?;
            let _ = d.stripped_channel_decoder();
            check_var(bytes, d.get_limit())?;
            let _ = d.original_channel_decoder();
            check_var(bytes, d.get_limit())?;
        }
        ping_codec::SBE_TEMPLATE_ID => needs(ping_codec::SBE_BLOCK_LENGTH)?,
        other => return Err(Malformed::UnknownTemplate(other)),
    }
    Ok(Response::decode_unchecked(bytes))
}
