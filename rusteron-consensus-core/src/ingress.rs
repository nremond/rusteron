//! What clients send the cluster on its ingress, decoded with every length
//! checked (Java's `IngressAdapter`).

use rusteron_cluster_codecs::cluster::{
    admin_request_codec, admin_request_type::AdminRequestType, session_close_request_codec,
    session_connect_request_codec, session_keep_alive_codec, session_message_header_codec,
};

use crate::codec::{self, SCHEMA_ID};
use crate::log::SESSION_HEADER_LENGTH;

/// A client's message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngressMessage {
    /// For the services: its payload follows the session header.
    Message {
        leadership_term_id: i64,
        cluster_session_id: i64,
    },
    Connect {
        correlation_id: i64,
        response_stream_id: i32,
        version: i32,
        response_channel: String,
        client_info: String,
    },
    Close {
        leadership_term_id: i64,
        cluster_session_id: i64,
    },
    KeepAlive {
        leadership_term_id: i64,
        cluster_session_id: i64,
    },
    Admin {
        leadership_term_id: i64,
        cluster_session_id: i64,
        correlation_id: i64,
        request_type: AdminRequestType,
    },
    /// Another schema's, or a template the module does not serve.
    Ignored,
}

pub fn decode(message: &[u8]) -> Result<IngressMessage, String> {
    let h = codec::header(message)?;
    if h.schema_id != SCHEMA_ID {
        return Ok(IngressMessage::Ignored);
    }
    let header = || codec::header_decoder(message);
    Ok(match h.template_id {
        session_message_header_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 24)?;
            if message.len() < SESSION_HEADER_LENGTH {
                return Err("session message shorter than its header".into());
            }
            let d = session_message_header_codec::SessionMessageHeaderDecoder::default().header(header(), 0);
            IngressMessage::Message {
                leadership_term_id: d.leadership_term_id(),
                cluster_session_id: d.cluster_session_id(),
            }
        }
        session_connect_request_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 12)?;
            let mut d = session_connect_request_codec::SessionConnectRequestDecoder::default().header(header(), 0);
            let (correlation_id, response_stream_id) = (d.correlation_id(), d.response_stream_id());
            let version = d.version().unwrap_or(0);
            use rusteron_cluster_codecs::cluster::Decoder;
            codec::check_var(message, d.get_limit())?;
            let c = d.response_channel_decoder();
            let response_channel = codec::ascii(d.response_channel_slice(c));
            codec::check_var(message, d.get_limit())?;
            let _ = d.encoded_credentials_decoder();
            let client_info = if h.version >= 14 && d.get_limit() < message.len() {
                codec::check_var(message, d.get_limit())?;
                let c = d.client_info_decoder();
                codec::ascii(d.client_info_slice(c))
            } else {
                String::new()
            };
            IngressMessage::Connect {
                correlation_id,
                response_stream_id,
                version,
                response_channel,
                client_info,
            }
        }
        session_close_request_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 16)?;
            let d = session_close_request_codec::SessionCloseRequestDecoder::default().header(header(), 0);
            IngressMessage::Close {
                leadership_term_id: d.leadership_term_id(),
                cluster_session_id: d.cluster_session_id(),
            }
        }
        session_keep_alive_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 16)?;
            let d = session_keep_alive_codec::SessionKeepAliveDecoder::default().header(header(), 0);
            IngressMessage::KeepAlive {
                leadership_term_id: d.leadership_term_id(),
                cluster_session_id: d.cluster_session_id(),
            }
        }
        admin_request_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 28)?;
            let d = admin_request_codec::AdminRequestDecoder::default().header(header(), 0);
            IngressMessage::Admin {
                leadership_term_id: d.leadership_term_id(),
                cluster_session_id: d.cluster_session_id(),
                correlation_id: d.correlation_id(),
                request_type: d.request_type(),
            }
        }
        _ => IngressMessage::Ignored,
    })
}
