//! The segment files a recording is written to, as Aeron Archive 1.54.2
//! writes them (`RecordingWriter.java`, `Catalog.java`):
//!
//! - one file a segment, `<recordingId>-<basePosition>.rec`, the full
//!   segment's length from its creation, holes where nothing was written;
//! - segments counted from the term base of the recording's start position,
//!   not from multiples of the segment's length: the first file starts at
//!   that base, its bytes before the start position a hole;
//! - frames as the image held them; of a padding frame only its header,
//!   the rest a hole;
//! - a recording's segment length the larger of the archive's and its
//!   term's.
//!
//! And the stop position of a recording the archive did not stop, recovered
//! from its last segment after a crash.

use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};

use rusteron_cluster_core::position::{FRAME_ALIGNMENT, HEADER_LENGTH};

/// A segment file's suffix.
pub const SEGMENT_SUFFIX: &str = ".rec";
/// The page whose boundary a last frame may straddle half written.
pub const PAGE_SIZE: usize = 4_096;
/// A padding frame's type.
pub const PADDING_FRAME_TYPE: u16 = 0;

const TYPE_OFFSET: usize = 6;

/// The file of the segment at `base_position`.
pub fn segment_file_name(recording_id: i64, base_position: i64) -> String {
    format!("{recording_id}-{base_position}{SEGMENT_SUFFIX}")
}

/// A segment file's recording id and base position, if `name` is one.
pub fn parse_segment_file_name(name: &str) -> Option<(i64, i64)> {
    let stem = name.strip_suffix(SEGMENT_SUFFIX)?;
    let (id, base) = stem.split_once('-')?;
    Some((id.parse().ok()?, base.parse().ok()?))
}

/// The base position of the segment holding `position`, in a recording that
/// starts at `start_position` (`AeronArchive.segmentFileBasePosition`).
pub fn segment_file_base_position(start_position: i64, position: i64, term_length: i32, segment_length: i32) -> i64 {
    let start_term_base = start_position - (start_position & (i64::from(term_length) - 1));
    let from_base = position - start_term_base;
    start_term_base + (from_base - (from_base & (i64::from(segment_length) - 1)))
}

/// A recording's segment length: the archive's, or its term's if larger.
pub fn recording_segment_length(archive_segment_length: i32, term_length: i32) -> i32 {
    archive_segment_length.max(term_length)
}

/// A frame's length, at `offset` in `bytes`; 0 past their end.
pub fn frame_length(bytes: &[u8], offset: usize) -> i32 {
    bytes
        .get(offset..offset + 4)
        .map_or(0, |b| i32::from_le_bytes(b.try_into().expect("4 bytes")))
}

/// Whether the frame at `offset` is padding.
pub fn is_padding(bytes: &[u8], offset: usize) -> bool {
    bytes
        .get(offset + TYPE_OFFSET..offset + TYPE_OFFSET + 2)
        .is_some_and(|b| u16::from_le_bytes([b[0], b[1]]) == PADDING_FRAME_TYPE)
}

fn align(value: usize) -> usize {
    let a = FRAME_ALIGNMENT as usize;
    (value + a - 1) & !(a - 1)
}

/// Where a recording writes: its segment, and the offset in it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentWriter {
    pub recording_id: i64,
    pub segment_length: i32,
    pub segment_base: i64,
    pub segment_offset: i64,
}

/// One block's write: `length` bytes of the block at `file_offset` in the
/// current segment; then, if `roll_over`, the next segment's file is opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockWrite {
    pub file_offset: u64,
    pub length: usize,
    pub roll_over: bool,
}

impl SegmentWriter {
    /// A recording from `start_position`, written from `join_position` (the
    /// start itself, or where an extension joins).
    pub fn new(
        recording_id: i64,
        start_position: i64,
        join_position: i64,
        term_length: i32,
        segment_length: i32,
    ) -> Self {
        let segment_base = segment_file_base_position(start_position, join_position, term_length, segment_length);
        SegmentWriter {
            recording_id,
            segment_length,
            segment_base,
            segment_offset: join_position - segment_base,
        }
    }

    pub fn file_name(&self) -> String {
        segment_file_name(self.recording_id, self.segment_base)
    }

    /// The recorded position: what the recording's counter shows.
    pub fn position(&self) -> i64 {
        self.segment_base + self.segment_offset
    }

    /// A block of `length` bytes from the image, its first frame padding or
    /// not (a block is data frames, or one padding frame): what to write,
    /// and the position it moves to.
    pub fn on_block(&mut self, length: usize, padding: bool) -> BlockWrite {
        let write = BlockWrite {
            file_offset: self.segment_offset as u64,
            length: if padding { HEADER_LENGTH as usize } else { length },
            roll_over: false,
        };
        self.segment_offset += length as i64;
        if self.segment_offset >= i64::from(self.segment_length) {
            self.segment_offset = 0;
            self.segment_base += i64::from(self.segment_length);
            return BlockWrite {
                roll_over: true,
                ..write
            };
        }
        write
    }
}

/// Why a stop position cannot be recovered without the operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recovery {
    /// The last fragment straddles a page boundary, with a page after it
    /// empty: it may be half written. Java refuses to start ("Run `ArchiveTool
    /// verify` for corrective action!").
    PageStraddle { fragment_offset: usize },
}

/// The stop position of a recording left active, from its last segment,
/// based at `segment_base` and holding `segment` (the file's bytes, up to
/// the segment's length); the start position if it has no segment
/// (`Catalog.computeStopPosition`).
pub fn compute_stop_position(
    start_position: i64,
    term_length: i32,
    segment_length: i32,
    last_segment: Option<(i64, &[u8])>,
) -> Result<i64, Recovery> {
    let Some((segment_base, segment)) = last_segment else {
        return Ok(start_position);
    };
    let start_term_offset = start_position & (i64::from(term_length) - 1);
    let start_term_base = start_position - start_term_offset;
    let file_offset = if segment_base == start_term_base {
        start_term_offset as usize
    } else {
        0
    };
    let stop_offset = recover_stop_offset(segment, file_offset, segment_length as usize)?;
    Ok((segment_base + stop_offset as i64).max(start_position))
}

/// Cuts off a segment's last fragment that a crash left half written across
/// a page boundary, as `ArchiveTool verify` does when told to
/// (`Catalog.recoverStopOffset`): the file keeps its length, the fragment and
/// what follows it zeros. Synced, unlike Java's. The recording then stops at
/// the fragment.
pub fn truncate_torn_fragment(file: &File, fragment_offset: usize, segment_length: usize) -> io::Result<()> {
    file.set_len(fragment_offset as u64)?;
    let mut file = file;
    file.seek(SeekFrom::Start(segment_length as u64 - 1))?;
    file.write_all(&[0])?;
    file.sync_all()
}

/// The offset after the last complete frame from `offset` (`recoverStopOffset`).
fn recover_stop_offset(segment: &[u8], offset: usize, segment_length: usize) -> Result<usize, Recovery> {
    let limit = segment_length.min(segment.len());
    let mut next = offset;
    let mut last_length = 0;
    while next < limit {
        let length = frame_length(segment, next);
        if length <= 0 {
            break;
        }
        last_length = align(length as usize);
        next += last_length;
    }
    let last = next - last_length;
    if last_length > 0
        && straddles_page_boundary(last, last_length)
        && !has_data_in_all_pages_after_straddle(segment, last, last_length)
    {
        return Err(Recovery::PageStraddle { fragment_offset: last });
    }
    Ok(next)
}

fn straddles_page_boundary(offset: usize, length: usize) -> bool {
    offset / PAGE_SIZE != (offset + length - 1) / PAGE_SIZE
}

fn has_data_in_all_pages_after_straddle(segment: &[u8], offset: usize, length: usize) -> bool {
    let end = offset + length;
    let mut page = (offset / PAGE_SIZE + 1) * PAGE_SIZE;
    while page < end {
        let page_end = (page + PAGE_SIZE).min(end);
        let empty = segment.get(page..page_end).is_none_or(|b| b.iter().all(|&x| x == 0));
        if empty {
            return false;
        }
        page += PAGE_SIZE;
    }
    true
}
