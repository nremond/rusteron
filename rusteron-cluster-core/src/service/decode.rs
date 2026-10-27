//! Checks before decoding: the generated readers check nothing but the
//! buffer's length, and panic when it is too short.

use rusteron_cluster_codecs::cluster::{ReadBuf, message_header_codec};

const HEADER: usize = message_header_codec::ENCODED_LENGTH;
const SCHEMA_ID: u16 = 111;

/// A message's SBE header, checked against the message's length.
pub(crate) struct Header {
    pub(crate) template_id: u16,
    pub(crate) block_length: u16,
    pub(crate) version: u16,
}

fn u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

/// The header of a cluster message whose block fits in it.
pub(crate) fn header(message: &[u8]) -> Result<Header, String> {
    if message.len() < HEADER {
        return Err(format!("message of {} bytes is shorter than its header", message.len()));
    }
    let schema_id = u16_at(message, 4);
    if schema_id != SCHEMA_ID {
        return Err(format!("expected schemaId={SCHEMA_ID}, actual={schema_id}"));
    }
    let header = Header {
        block_length: u16_at(message, 0),
        template_id: u16_at(message, 2),
        version: u16_at(message, 6),
    };
    if message.len() < HEADER + header.block_length as usize {
        return Err(format!(
            "message {} of {} bytes is shorter than its block of {}",
            header.template_id,
            message.len(),
            header.block_length
        ));
    }
    Ok(header)
}

/// A decoder over a message whose block holds at least `min_block` bytes:
/// every fixed field this crate reads.
pub(crate) fn body<'a, D>(
    message: &'a [u8],
    header: &Header,
    min_block: u16,
    wrap: impl FnOnce(ReadBuf<'a>, &Header) -> D,
) -> Result<D, String> {
    if header.block_length < min_block {
        return Err(format!(
            "message {} has a block of {}, expected at least {min_block}",
            header.template_id, header.block_length
        ));
    }
    Ok(wrap(ReadBuf::new(message), header))
}

/// Whether the variable-length field at `limit` fits in the message.
pub(crate) fn check_var(message: &[u8], limit: usize) -> Result<(), String> {
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
