//! What a client sends the cluster, and what it reads back on its egress,
//! as Aeron 1.54.2's `AeronCluster` encodes and decodes it. Every length is
//! checked before a field is read: the generated readers check nothing but
//! the buffer's length, and panic when it is too short.

use rusteron_cluster_codecs::cluster::{
    Decoder, ReadBuf, WriteBuf, admin_request_codec, admin_request_type::AdminRequestType,
    admin_response_code::AdminResponseCode, admin_response_codec, challenge_codec, challenge_response_codec,
    event_code::EventCode, message_header_codec, message_header_codec::MessageHeaderDecoder, new_leader_event_codec,
    session_close_request_codec, session_connect_request_codec, session_event_codec, session_keep_alive_codec,
    session_message_header_codec,
};

/// An SBE message header's length.
pub const HEADER_LENGTH: usize = message_header_codec::ENCODED_LENGTH;
/// The cluster protocol's schema.
pub const SCHEMA_ID: u16 = 111;
/// A session message's header before its payload: SBE's 8 bytes and a block of 24.
pub const SESSION_HEADER_LENGTH: usize = HEADER_LENGTH + session_message_header_codec::SBE_BLOCK_LENGTH as usize;
/// The client protocol's version, 0.3.0 (`AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION`).
pub const PROTOCOL_SEMANTIC_VERSION: i32 = 3 << 8;

/// Encodes a message of `$codec`, `$var` bytes of variable-length data
/// after its block, as `$body` sets it, header included.
macro_rules! encode {
    ($codec:ident :: $encoder:ident, $var:expr, |$e:ident| $body:block) => {{
        let mut buf = vec![0u8; HEADER_LENGTH + $codec::SBE_BLOCK_LENGTH as usize + $var];
        {
            #[allow(unused_mut)]
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

/// A session message's header, as `AeronCluster` keeps it encoded ahead of
/// every message it offers: its timestamp 0, which the cluster ignores.
pub fn session_message_header(leadership_term_id: i64, cluster_session_id: i64) -> [u8; SESSION_HEADER_LENGTH] {
    let mut header = [0; SESSION_HEADER_LENGTH];
    let mut e = session_message_header_codec::SessionMessageHeaderEncoder::default()
        .wrap(WriteBuf::new(&mut header), HEADER_LENGTH)
        .header(0)
        .parent()
        .expect("encoder");
    e.leadership_term_id(leadership_term_id)
        .cluster_session_id(cluster_session_id)
        .timestamp(0);
    header
}

/// A client's request for a session, answered on `response_channel`.
pub fn session_connect_request(
    correlation_id: i64,
    response_stream_id: i32,
    version: i32,
    response_channel: &str,
    encoded_credentials: &[u8],
    client_info: &str,
) -> Vec<u8> {
    let var = 12 + response_channel.len() + encoded_credentials.len() + client_info.len();
    encode!(session_connect_request_codec::SessionConnectRequestEncoder, var, |e| {
        e.correlation_id(correlation_id)
            .response_stream_id(response_stream_id)
            .version(version)
            .response_channel(response_channel.as_bytes())
            .encoded_credentials(encoded_credentials)
            .client_info(client_info.as_bytes());
    })
}

/// The credentials a challenge asked for.
pub fn challenge_response(correlation_id: i64, cluster_session_id: i64, encoded_credentials: &[u8]) -> Vec<u8> {
    encode!(
        challenge_response_codec::ChallengeResponseEncoder,
        4 + encoded_credentials.len(),
        |e| {
            e.correlation_id(correlation_id)
                .cluster_session_id(cluster_session_id)
                .encoded_credentials(encoded_credentials);
        }
    )
}

pub fn session_keep_alive(leadership_term_id: i64, cluster_session_id: i64) -> Vec<u8> {
    encode!(session_keep_alive_codec::SessionKeepAliveEncoder, 0, |e| {
        e.leadership_term_id(leadership_term_id)
            .cluster_session_id(cluster_session_id);
    })
}

pub fn session_close_request(leadership_term_id: i64, cluster_session_id: i64) -> Vec<u8> {
    encode!(session_close_request_codec::SessionCloseRequestEncoder, 0, |e| {
        e.leadership_term_id(leadership_term_id)
            .cluster_session_id(cluster_session_id);
    })
}

/// An admin request with an empty payload, as `AeronCluster` sends one.
pub fn admin_request(
    leadership_term_id: i64,
    cluster_session_id: i64,
    correlation_id: i64,
    request_type: AdminRequestType,
) -> Vec<u8> {
    encode!(admin_request_codec::AdminRequestEncoder, 4, |e| {
        e.leadership_term_id(leadership_term_id)
            .cluster_session_id(cluster_session_id)
            .correlation_id(correlation_id)
            .request_type(request_type)
            .payload(&[]);
    })
}

/// What the cluster sends a client on its egress.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Egress<'a> {
    /// A service's message: its payload follows the session header.
    Message {
        leadership_term_id: i64,
        cluster_session_id: i64,
        timestamp: i64,
        payload: &'a [u8],
    },
    SessionEvent {
        cluster_session_id: i64,
        correlation_id: i64,
        leadership_term_id: i64,
        leader_member_id: i32,
        code: EventCode,
        /// 0 when the sender sent none.
        version: i32,
        /// Aeron's null value, -1, when the sender sent none.
        leader_heartbeat_timeout_ns: i64,
        detail: String,
    },
    NewLeader {
        leadership_term_id: i64,
        cluster_session_id: i64,
        leader_member_id: i32,
        ingress_endpoints: String,
    },
    Challenge {
        correlation_id: i64,
        cluster_session_id: i64,
        encoded_challenge: &'a [u8],
    },
    AdminResponse {
        cluster_session_id: i64,
        correlation_id: i64,
        request_type: AdminRequestType,
        response_code: AdminResponseCode,
        message: String,
        payload: &'a [u8],
    },
    /// A template of the cluster's schema a client does not read.
    Other { template_id: u16 },
    /// Another schema's message.
    Foreign { schema_id: u16 },
}

fn u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

/// Whether the variable-length field at `limit` fits in the message.
fn check_var(message: &[u8], limit: usize) -> Result<(), String> {
    let length = message
        .get(limit..limit + 4)
        .map(|b| u32::from_le_bytes(b.try_into().expect("4 bytes")) as usize)
        .ok_or_else(|| format!("variable-length field at {limit} past the message's end"))?;
    if limit + 4 + length > message.len() {
        return Err(format!(
            "variable-length field of {length} bytes at {limit} past the message's end"
        ));
    }
    Ok(())
}

fn needs(template_id: u16, block_length: u16, min_block: u16) -> Result<(), String> {
    if block_length < min_block {
        return Err(format!(
            "message {template_id} has a block of {block_length}, expected at least {min_block}"
        ));
    }
    Ok(())
}

fn ascii(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A message read from a client's egress, every length checked.
pub fn decode_egress(message: &[u8]) -> Result<Egress<'_>, String> {
    if message.len() < HEADER_LENGTH {
        return Err(format!("message of {} bytes is shorter than its header", message.len()));
    }
    let (block_length, template_id, schema_id) = (u16_at(message, 0), u16_at(message, 2), u16_at(message, 4));
    if schema_id != SCHEMA_ID {
        return Ok(Egress::Foreign { schema_id });
    }
    if message.len() < HEADER_LENGTH + block_length as usize {
        return Err(format!(
            "message {template_id} of {} bytes is shorter than its block of {block_length}",
            message.len()
        ));
    }
    let header = || MessageHeaderDecoder::default().wrap(ReadBuf::new(message), 0);
    Ok(match template_id {
        session_message_header_codec::SBE_TEMPLATE_ID => {
            needs(template_id, block_length, 24)?;
            if message.len() < SESSION_HEADER_LENGTH {
                return Err("session message shorter than its header".into());
            }
            let d = session_message_header_codec::SessionMessageHeaderDecoder::default().header(header(), 0);
            Egress::Message {
                leadership_term_id: d.leadership_term_id(),
                cluster_session_id: d.cluster_session_id(),
                timestamp: d.timestamp(),
                payload: &message[HEADER_LENGTH + block_length as usize..],
            }
        }
        session_event_codec::SBE_TEMPLATE_ID => {
            needs(template_id, block_length, 32)?;
            let mut d = session_event_codec::SessionEventDecoder::default().header(header(), 0);
            let version = if block_length >= 36 {
                d.version().unwrap_or(0)
            } else {
                0
            };
            let leader_heartbeat_timeout_ns = if block_length >= 44 {
                d.leader_heartbeat_timeout_ns().unwrap_or(-1)
            } else {
                -1
            };
            let (cluster_session_id, correlation_id, leadership_term_id, leader_member_id, code) = (
                d.cluster_session_id(),
                d.correlation_id(),
                d.leadership_term_id(),
                d.leader_member_id(),
                d.code(),
            );
            check_var(message, d.get_limit())?;
            let c = d.detail_decoder();
            Egress::SessionEvent {
                cluster_session_id,
                correlation_id,
                leadership_term_id,
                leader_member_id,
                code,
                version,
                leader_heartbeat_timeout_ns,
                detail: ascii(d.detail_slice(c)),
            }
        }
        new_leader_event_codec::SBE_TEMPLATE_ID => {
            needs(template_id, block_length, 20)?;
            let mut d = new_leader_event_codec::NewLeaderEventDecoder::default().header(header(), 0);
            let (leadership_term_id, cluster_session_id, leader_member_id) =
                (d.leadership_term_id(), d.cluster_session_id(), d.leader_member_id());
            check_var(message, d.get_limit())?;
            let c = d.ingress_endpoints_decoder();
            Egress::NewLeader {
                leadership_term_id,
                cluster_session_id,
                leader_member_id,
                ingress_endpoints: ascii(d.ingress_endpoints_slice(c)),
            }
        }
        challenge_codec::SBE_TEMPLATE_ID => {
            needs(template_id, block_length, 16)?;
            let mut d = challenge_codec::ChallengeDecoder::default().header(header(), 0);
            let (correlation_id, cluster_session_id) = (d.correlation_id(), d.cluster_session_id());
            check_var(message, d.get_limit())?;
            let (offset, length) = d.encoded_challenge_decoder();
            Egress::Challenge {
                correlation_id,
                cluster_session_id,
                encoded_challenge: &message[offset..offset + length],
            }
        }
        admin_response_codec::SBE_TEMPLATE_ID => {
            needs(template_id, block_length, 24)?;
            let mut d = admin_response_codec::AdminResponseDecoder::default().header(header(), 0);
            let (cluster_session_id, correlation_id, request_type, response_code) = (
                d.cluster_session_id(),
                d.correlation_id(),
                d.request_type(),
                d.response_code(),
            );
            check_var(message, d.get_limit())?;
            let c = d.message_decoder();
            let text = ascii(d.message_slice(c));
            check_var(message, d.get_limit())?;
            let (offset, length) = d.payload_decoder();
            Egress::AdminResponse {
                cluster_session_id,
                correlation_id,
                request_type,
                response_code,
                message: text,
                payload: &message[offset..offset + length],
            }
        }
        template_id => Egress::Other { template_id },
    })
}
