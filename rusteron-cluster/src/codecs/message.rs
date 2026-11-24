//! Encoding the cluster's messages, and the checks before decoding one:
//! the generated readers check nothing but the buffer's length, and panic
//! when it is too short. The consensus module decodes what clients, other
//! members and services send it, so a message's block and each of its
//! variable-length fields are checked against its length first.

use crate::codecs::cluster::{ReadBuf, message_header_codec, message_header_codec::MessageHeaderDecoder};

/// An SBE message header's length.
pub const HEADER_LENGTH: usize = message_header_codec::ENCODED_LENGTH;
/// The cluster protocol's schema.
pub const SCHEMA_ID: u16 = 111;

/// Encodes a message of `$codec`, `$var` bytes of variable-length data
/// after its block, as `$body` sets it, header included.
macro_rules! encode {
    ($codec:ident :: $encoder:ident, $var:expr, |$e:ident| $body:block) => {{
        let mut buf = vec![0u8; $crate::codecs::message::HEADER_LENGTH + $codec::SBE_BLOCK_LENGTH as usize + $var];
        {
            #[allow(unused_mut)]
            let mut $e = $codec::$encoder::default()
                .wrap(
                    crate::codecs::cluster::WriteBuf::new(&mut buf),
                    $crate::codecs::message::HEADER_LENGTH,
                )
                .header(0)
                .parent()
                .expect("encoder");
            $body
        }
        buf
    }};
}
pub(crate) use encode;

/// A message's header, checked against the message's length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub schema_id: u16,
    pub template_id: u16,
    pub block_length: u16,
    pub version: u16,
}

fn u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

/// The header of a message whose block fits in it.
pub fn header(message: &[u8]) -> Result<Header, String> {
    if message.len() < HEADER_LENGTH {
        return Err(format!("message of {} bytes is shorter than its header", message.len()));
    }
    let header = Header {
        block_length: u16_at(message, 0),
        template_id: u16_at(message, 2),
        schema_id: u16_at(message, 4),
        version: u16_at(message, 6),
    };
    if message.len() < HEADER_LENGTH + header.block_length as usize {
        return Err(format!(
            "message {} of {} bytes is shorter than its block of {}",
            header.template_id,
            message.len(),
            header.block_length
        ));
    }
    Ok(header)
}

/// The SBE header decoder over a message already checked.
pub fn header_decoder(message: &[u8]) -> MessageHeaderDecoder<ReadBuf<'_>> {
    MessageHeaderDecoder::default().wrap(ReadBuf::new(message), 0)
}

/// Refuses a block shorter than the fields read from it.
pub fn needs(header: &Header, min_block: u16) -> Result<(), String> {
    if header.block_length < min_block {
        return Err(format!(
            "message {} has a block of {}, expected at least {min_block}",
            header.template_id, header.block_length
        ));
    }
    Ok(())
}

/// Whether the variable-length field at `limit` fits in the message.
pub fn check_var(message: &[u8], limit: usize) -> Result<(), String> {
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

/// Whether the repeating group's header at `limit` fits in the message, its
/// entries' blocks holding the `min_block` bytes read from each (each entry is
/// checked against the message's end as it is reached).
pub fn check_group(message: &[u8], limit: usize, min_block: u16) -> Result<(), String> {
    let block = message
        .get(limit..limit + 4)
        .map(|b| u16::from_le_bytes([b[0], b[1]]))
        .ok_or_else(|| format!("group header at {limit} past the message's end"))?;
    if block < min_block {
        return Err(format!("group entries of {block} bytes, expected at least {min_block}"));
    }
    Ok(())
}

/// ASCII text from a message, as Java reads it.
pub fn ascii(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}
