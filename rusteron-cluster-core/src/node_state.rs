//! A consensus module's `node-state.dat`, as Java's `NodeStateFile` lays it
//! out: a file of 1 MiB, its header's block (the mark file's version) at 0,
//! then SBE frames aligned to 8: the candidate term (the highest term this
//! node voted or stood in, written before it votes), then a footer.
//!
//! This module builds and reads the bytes; creating the file, writing and
//! syncing it are the shell's.

use rusteron_cluster_codecs::node::{
    ReadBuf, WriteBuf, candidate_term_codec, candidate_term_codec::CandidateTermEncoder, message_header_codec,
    message_header_codec::MessageHeaderDecoder, node_state_footer_codec,
    node_state_footer_codec::NodeStateFooterEncoder, node_state_header_codec,
};

use crate::markfile::SEMANTIC_VERSION;

/// The file in the cluster directory.
pub const FILE_NAME: &str = "node-state.dat";
/// Its length when created.
pub const FILE_LENGTH: usize = 1 << 20;

const ALIGNMENT: usize = 8;
const HEADER: usize = message_header_codec::ENCODED_LENGTH;
/// The candidate term's frame, right after the header's block.
const CANDIDATE_TERM_FRAME: usize = node_state_header_codec::SBE_BLOCK_LENGTH as usize;
/// Offsets in the candidate term's block.
const CANDIDATE_TERM_ID: usize = 0;
const TIMESTAMP: usize = 8;
const LOG_POSITION: usize = 16;

/// What Java's `NodeStateFile` would throw.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

/// The candidate term, and where it lives in the file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeState {
    pub candidate_term_id: i64,
    /// Epoch ms when it was last set.
    pub timestamp: i64,
    pub log_position: i64,
    /// Offset of the candidate term's block.
    body: usize,
}

impl NodeState {
    /// A new file's state, and the bytes to write at its start: the rest of
    /// its [`FILE_LENGTH`] is zero.
    pub fn create() -> (Self, Vec<u8>) {
        let footer = CANDIDATE_TERM_FRAME
            + (HEADER + candidate_term_codec::SBE_BLOCK_LENGTH as usize).next_multiple_of(ALIGNMENT);
        let mut bytes = vec![0; footer + HEADER];
        bytes[..4].copy_from_slice(&SEMANTIC_VERSION.to_le_bytes());

        let mut e = CandidateTermEncoder::default()
            .wrap(WriteBuf::new(&mut bytes), CANDIDATE_TERM_FRAME + HEADER)
            .header(CANDIDATE_TERM_FRAME)
            .parent()
            .expect("a header always has its parent");
        e.log_position(-1).timestamp(-1).candidate_term_id(-1);
        let frame_length = (HEADER + candidate_term_codec::SBE_BLOCK_LENGTH as usize) as i32;
        bytes[CANDIDATE_TERM_FRAME + 8..CANDIDATE_TERM_FRAME + 12].copy_from_slice(&frame_length.to_le_bytes());

        NodeStateFooterEncoder::default()
            .wrap(WriteBuf::new(&mut bytes), footer + HEADER)
            .header(footer)
            .parent()
            .expect("a header always has its parent");
        bytes[footer + 8..footer + 12].copy_from_slice(&(HEADER as i32).to_le_bytes());

        let state = Self {
            candidate_term_id: -1,
            timestamp: -1,
            log_position: -1,
            body: CANDIDATE_TERM_FRAME + HEADER,
        };
        (state, bytes)
    }

    /// An existing file's state: its major version checked, then its frames
    /// scanned for the footer and the candidate term.
    pub fn load(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < CANDIDATE_TERM_FRAME {
            return Err(Error(format!("node state file of {} bytes", bytes.len())));
        }
        let version = i32::from_le_bytes(bytes[..4].try_into().expect("4 bytes"));
        let major = (version >> 16) & 0xFF;
        if major != (SEMANTIC_VERSION >> 16) & 0xFF {
            return Err(Error(format!(
                "mark file major version {major} does not match software: {}",
                (SEMANTIC_VERSION >> 16) & 0xFF
            )));
        }
        if scan(bytes, node_state_footer_codec::SBE_TEMPLATE_ID).is_none() {
            return Err(Error("failed to find NodeStateFooter entry, file corrupt?".into()));
        }
        let Some(frame) = scan(bytes, candidate_term_codec::SBE_TEMPLATE_ID) else {
            return Err(Error("failed to find CandidateTerm entry".into()));
        };
        let body = frame + HEADER;
        if bytes.len() < body + candidate_term_codec::SBE_BLOCK_LENGTH as usize {
            return Err(Error("failed to find CandidateTerm entry".into()));
        }
        let at =
            |offset: usize| i64::from_le_bytes(bytes[body + offset..body + offset + 8].try_into().expect("8 bytes"));
        Ok(Self {
            candidate_term_id: at(CANDIDATE_TERM_ID),
            timestamp: at(TIMESTAMP),
            log_position: at(LOG_POSITION),
            body,
        })
    }

    /// Sets the candidate term: the bytes to write, and where, the term id
    /// in the same write as Java's ordered store follows its other fields.
    pub fn update_candidate_term_id(
        &mut self,
        candidate_term_id: i64,
        log_position: i64,
        timestamp_ms: i64,
    ) -> (u64, Vec<u8>) {
        self.candidate_term_id = candidate_term_id;
        self.log_position = log_position;
        self.timestamp = timestamp_ms;
        let mut bytes = Vec::with_capacity(24);
        bytes.extend_from_slice(&candidate_term_id.to_le_bytes());
        bytes.extend_from_slice(&timestamp_ms.to_le_bytes());
        bytes.extend_from_slice(&log_position.to_le_bytes());
        (self.body as u64, bytes)
    }

    /// Raises the candidate term to `candidate_term_id` if higher: the term
    /// in force, and the write to make when it changed.
    pub fn propose_max_candidate_term_id(
        &mut self,
        candidate_term_id: i64,
        log_position: i64,
        timestamp_ms: i64,
    ) -> (i64, Option<(u64, Vec<u8>)>) {
        if candidate_term_id > self.candidate_term_id {
            let write = self.update_candidate_term_id(candidate_term_id, log_position, timestamp_ms);
            return (candidate_term_id, Some(write));
        }
        (self.candidate_term_id, None)
    }
}

/// Java's `scanForMessageTypeOffset`: the offset of the first frame of a
/// template, frames from after the header's block, each its frame length
/// aligned to 8.
fn scan(bytes: &[u8], template_id: u16) -> Option<usize> {
    let mut offset = CANDIDATE_TERM_FRAME;
    while offset + HEADER <= bytes.len() {
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes[offset..]), 0);
        if header.template_id() == template_id {
            return Some(offset);
        }
        let frame_length = i32::from_le_bytes(bytes[offset + 8..offset + 12].try_into().expect("4 bytes"));
        if frame_length <= 0 {
            return None;
        }
        offset += (frame_length as usize).next_multiple_of(ALIGNMENT);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_file_holds_no_candidate_term_and_reads_back() {
        let (state, bytes) = NodeState::create();
        assert_eq!(state.candidate_term_id, -1);
        assert_eq!(bytes.len(), 64);
        let mut file = bytes.clone();
        file.resize(FILE_LENGTH, 0);
        assert_eq!(NodeState::load(&file).unwrap(), state);
    }

    #[test]
    fn the_candidate_term_only_rises() {
        let (mut state, bytes) = NodeState::create();
        let mut file = bytes;
        file.resize(FILE_LENGTH, 0);
        let (term, write) = state.propose_max_candidate_term_id(3, 4_096, 1_700_000_000_000);
        assert_eq!(term, 3);
        let (offset, bytes) = write.expect("raised");
        file[offset as usize..offset as usize + bytes.len()].copy_from_slice(&bytes);
        assert_eq!(state.propose_max_candidate_term_id(2, 0, 0), (3, None));
        let loaded = NodeState::load(&file).unwrap();
        assert_eq!((loaded.candidate_term_id, loaded.log_position), (3, 4_096));
    }
}
