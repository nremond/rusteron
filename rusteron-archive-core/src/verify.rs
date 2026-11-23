//! What `ArchiveTool verify` checks of a recording (Aeron 1.53.3,
//! `ArchiveTool.verifyRecording`, without options: the last segment file
//! only, no checksums), for an archive with no JVM beside it to run the tool.
//!
//! A recording is damaged if its positions break the catalog's invariants,
//! run past its last segment file, or if that file's frames are not the
//! recording's from where it starts: Java then marks it invalid, and the
//! archive no longer lists it. A whole recording's stop position is where
//! its last segment's frames end, which the tool writes if the catalog's
//! differs; an invalid recording found whole, with segment files, is valid
//! again.
//!
//! Two departures from the tool, each where Java's archive itself reads the
//! file otherwise:
//! - a last segment file shorter than its segment (a crash between creating
//!   the file and sizing it leaves it empty) holds frames to its end, as the
//!   archive's recovery reads it ([`compute_stop_position`]). The tool reads
//!   a header past that end, fails, and marks the whole recording invalid;
//! - a frame of any length is read. The tool reads a frame's data into a
//!   buffer of 1 MiB and throws on a longer one, a padding frame to a large
//!   term's end too, which ends its verification of every recording.

use crate::catalog::{Descriptor, NULL_POSITION};
use crate::segments::{Recovery, compute_stop_position, frame_length};

use rusteron_cluster_core::position::{FRAME_ALIGNMENT, HEADER_LENGTH, position_bits_to_shift};

const TERM_OFFSET_OFFSET: usize = 8;
const STREAM_ID_OFFSET: usize = 16;
const TERM_ID_OFFSET: usize = 20;

/// What the tool finds of a recording.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Whole: its last segment's frames end at `stop_position`.
    Whole { stop_position: i64 },
    /// Its last segment's last fragment straddles a page boundary at
    /// `fragment_offset`, with a page after the boundary empty: it may be
    /// half written. The tool truncates it before it when told to, and checks
    /// the recording again; else the recording is damaged.
    Torn { fragment_offset: usize },
    /// Damaged, in the tool's words.
    Damaged(String),
}

/// Checks recording `d`, its segment file of the highest position based at
/// `.0` holding `.1` (the file's bytes from its start, at least to its
/// frames' end), if it has any segment file.
pub fn verify_recording(d: &Descriptor, last_segment: Option<(i64, &[u8])>) -> Verdict {
    if let Some(message) = position_invariant_violation(d.start_position, d.stop_position) {
        return Verdict::Damaged(message);
    }
    if let Some((base, _)) = last_segment {
        let max_segment_position = base + i64::from(d.segment_file_length) - 1;
        if d.start_position > max_segment_position || d.stop_position > max_segment_position {
            return Verdict::Damaged(format!(
                "Invariant violation: startPosition={} and/or stopPosition={} exceed max segment file \
                 position={max_segment_position}",
                d.start_position, d.stop_position
            ));
        }
    }
    let stop_position = match compute_stop_position(
        d.start_position,
        d.term_buffer_length,
        d.segment_file_length,
        last_segment,
    ) {
        Ok(stop) => stop,
        Err(Recovery::PageStraddle { fragment_offset }) => return Verdict::Torn { fragment_offset },
    };
    if let Some((base, segment)) = last_segment
        && let Some(message) = invalid_segment(d, base, segment)
    {
        return Verdict::Damaged(message);
    }
    Verdict::Whole { stop_position }
}

/// The tool's `isPositionInvariantViolated`, in its words.
fn position_invariant_violation(start: i64, stop: i64) -> Option<String> {
    let aligned = |position: i64| position & (i64::from(FRAME_ALIGNMENT) - 1) == 0;
    if start < 0 {
        Some(format!("Negative startPosition={start}"))
    } else if !aligned(start) {
        Some(format!("Non-aligned startPosition={start}"))
    } else if stop != NULL_POSITION && stop < start {
        Some(format!(
            "Invariant violation stopPosition={stop} is before startPosition={start}"
        ))
    } else if stop != NULL_POSITION && !aligned(stop) {
        Some(format!("Non-aligned stopPosition={stop}"))
    } else {
        None
    }
}

/// The tool's `isInvalidSegmentFile`: from the recording's start in its term
/// if the segment holds that term's start, else from the segment's start, to
/// the first frame of length 0, every frame's header names the recording's
/// stream, and the term and offset of its position; and its data is there.
fn invalid_segment(d: &Descriptor, base: i64, segment: &[u8]) -> Option<String> {
    let header = HEADER_LENGTH as usize;
    let term_length = i64::from(d.term_buffer_length);
    let bits = position_bits_to_shift(d.term_buffer_length);
    let start_term_offset = d.start_position & (term_length - 1);
    let start_term_base = d.start_position - start_term_offset;
    let limit = (d.segment_file_length as usize).min(segment.len());
    let mut offset = if base == start_term_base {
        start_term_offset as usize
    } else {
        0
    };
    let mut position = base + offset as i64;
    while offset < limit {
        if offset + header > segment.len() {
            return Some("failed to read fragment header".into());
        }
        let length = frame_length(segment, offset);
        if length == 0 {
            break;
        }
        let read = |at: usize| i32::from_le_bytes(segment[offset + at..offset + at + 4].try_into().expect("4 bytes"));
        let term_id = d.initial_term_id.wrapping_add((position >> bits) as i32);
        let term_offset = (position & (term_length - 1)) as i32;
        let (actual_term_offset, actual_term_id, actual_stream_id) =
            (read(TERM_OFFSET_OFFSET), read(TERM_ID_OFFSET), read(STREAM_ID_OFFSET));
        if actual_term_offset != term_offset || actual_term_id != term_id || actual_stream_id != d.stream_id {
            return Some(format!(
                "fragment termOffset={actual_term_offset} (expected={term_offset}), termId={actual_term_id} \
                 (expected={term_id}), streamId={actual_stream_id} (expected={})",
                d.stream_id
            ));
        }
        if length < 0 {
            return Some(format!("fragment frameLength={length} at offset {offset}"));
        }
        let aligned = (length as usize + FRAME_ALIGNMENT as usize - 1) & !(FRAME_ALIGNMENT as usize - 1);
        if offset + aligned > segment.len() {
            return Some(format!(
                "failed to read {} byte(s) of data at offset {}",
                aligned - header,
                offset + header
            ));
        }
        offset += aligned;
        position += aligned as i64;
    }
    None
}

/// How much of a segment file [`verify_recording`] reads, from its first
/// `prefix` bytes: `None` if its frames from `offset` end within them, a
/// frame of length 0 next or the segment's end reached; else the length to
/// read to, at least to the end of the last frame begun. The archive's
/// segments are their full length from their creation, mostly a hole past
/// their frames: reading what is needed only, the tool reads them so too.
pub fn wants_more(prefix: &[u8], offset: usize, segment_length: usize) -> Option<usize> {
    let header = HEADER_LENGTH as usize;
    let mut next = offset;
    while next < segment_length {
        if next + header > prefix.len() {
            return Some(next + header);
        }
        let length = frame_length(prefix, next);
        if length <= 0 {
            return None;
        }
        let end = next + ((length as usize + FRAME_ALIGNMENT as usize - 1) & !(FRAME_ALIGNMENT as usize - 1));
        if end > prefix.len() {
            return Some(end);
        }
        next = end;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const TERM: i32 = 64 * 1024;
    const SEGMENT: i32 = 128 * 1024;

    fn descriptor(start: i64, stop: i64) -> Descriptor {
        Descriptor {
            control_session_id: 0,
            correlation_id: 0,
            recording_id: 7,
            start_timestamp: 0,
            stop_timestamp: 0,
            start_position: start,
            stop_position: stop,
            initial_term_id: 5,
            segment_file_length: SEGMENT,
            term_buffer_length: TERM,
            mtu_length: 1408,
            session_id: 9,
            stream_id: 100,
            stripped_channel: String::new(),
            original_channel: String::new(),
            source_identity: String::new(),
        }
    }

    /// A segment file at `base` of frames of `lengths` from `offset`, their headers the recording's, their
    /// data non-zero, sized to the segment.
    fn segment(base: i64, offset: usize, lengths: &[i32]) -> Vec<u8> {
        let mut bytes = vec![0u8; SEGMENT as usize];
        let mut at = offset;
        for &length in lengths {
            let position = base + at as i64;
            bytes[at..at + 4].copy_from_slice(&length.to_le_bytes());
            bytes[at + 6..at + 8].copy_from_slice(&1u16.to_le_bytes());
            bytes[at + 8..at + 12].copy_from_slice(&((position & i64::from(TERM - 1)) as i32).to_le_bytes());
            bytes[at + 16..at + 20].copy_from_slice(&100i32.to_le_bytes());
            bytes[at + 20..at + 24].copy_from_slice(&(5 + (position >> 16) as i32).to_le_bytes());
            for b in &mut bytes[at + 32..at + length as usize] {
                *b = 0x5a;
            }
            at += (length as usize + 31) & !31;
        }
        bytes
    }

    #[test]
    fn a_recording_whose_frames_end_at_its_stop_is_whole() {
        let file = segment(0, 1024, &[100, 2000, 64]);
        let end = 1024 + 128 + 2016 + 64;
        assert_eq!(
            verify_recording(&descriptor(1024, end), Some((0, &file))),
            Verdict::Whole { stop_position: end }
        );
        assert_eq!(
            verify_recording(&descriptor(1024, 1024), Some((0, &file))),
            Verdict::Whole { stop_position: end },
            "the stop the frames say, whatever the catalog's"
        );
    }

    #[test]
    fn positions_breaking_the_invariants_are_damage() {
        let damaged = |start, stop| match verify_recording(&descriptor(start, stop), None) {
            Verdict::Damaged(message) => message,
            other => panic!("{other:?}"),
        };
        assert_eq!(damaged(-64, NULL_POSITION), "Negative startPosition=-64");
        assert_eq!(damaged(100, NULL_POSITION), "Non-aligned startPosition=100");
        assert_eq!(
            damaged(128, 64),
            "Invariant violation stopPosition=64 is before startPosition=128"
        );
        assert_eq!(damaged(128, 200), "Non-aligned stopPosition=200");
    }

    #[test]
    fn a_stop_past_the_last_segment_is_damage() {
        let file = segment(0, 0, &[64]);
        assert_eq!(
            verify_recording(&descriptor(0, i64::from(SEGMENT)), Some((0, &file))),
            Verdict::Damaged(format!(
                "Invariant violation: startPosition=0 and/or stopPosition={SEGMENT} exceed max segment file \
                 position={}",
                SEGMENT - 1
            ))
        );
    }

    #[test]
    fn a_frame_of_another_term_or_stream_is_damage() {
        let mut file = segment(0, 0, &[64, 64]);
        file[64 + 20..64 + 24].copy_from_slice(&99i32.to_le_bytes());
        assert_eq!(
            verify_recording(&descriptor(0, 128), Some((0, &file))),
            Verdict::Damaged(
                "fragment termOffset=64 (expected=64), termId=99 (expected=5), streamId=100 (expected=100)".into()
            )
        );
        let mut file = segment(0, 0, &[64]);
        file[16..20].copy_from_slice(&101i32.to_le_bytes());
        assert!(matches!(
            verify_recording(&descriptor(0, 64), Some((0, &file))),
            Verdict::Damaged(m) if m.ends_with("streamId=101 (expected=100)")
        ));
    }

    #[test]
    fn a_later_segment_is_checked_from_its_start_in_its_terms() {
        let base = i64::from(SEGMENT);
        let file = segment(base, 0, &[4000, 4000]);
        assert_eq!(
            verify_recording(&descriptor(1024, base + 8000), Some((base, &file))),
            Verdict::Whole {
                stop_position: base + 8000
            }
        );
    }

    #[test]
    fn a_frame_cut_by_the_file_s_end_is_damage() {
        let file = segment(0, 0, &[64, 2000]);
        assert_eq!(
            verify_recording(&descriptor(0, 64), Some((0, &file[..64 + 40]))),
            Verdict::Damaged("failed to read 1984 byte(s) of data at offset 96".into())
        );
        assert_eq!(
            verify_recording(&descriptor(0, 64), Some((0, &file[..64 + 16]))),
            Verdict::Damaged("failed to read fragment header".into())
        );
    }

    #[test]
    fn a_segment_file_left_empty_holds_no_frames() {
        let base = i64::from(SEGMENT);
        assert_eq!(
            verify_recording(&descriptor(0, base), Some((base, &[]))),
            Verdict::Whole { stop_position: base }
        );
    }

    #[test]
    fn a_half_written_last_fragment_is_torn() {
        let mut file = segment(0, 0, &[4000, 3000]);
        for b in &mut file[4096..4000 + 3008] {
            *b = 0;
        }
        assert_eq!(
            verify_recording(&descriptor(0, NULL_POSITION), Some((0, &file))),
            Verdict::Torn { fragment_offset: 4000 }
        );
    }

    #[test]
    fn what_is_read_ends_with_the_frames() {
        let file = segment(0, 1024, &[100, 2000, 64]);
        let end = 1024 + 128 + 2016 + 64;
        assert_eq!(wants_more(&file[..10], 1024, SEGMENT as usize), Some(1024 + 32));
        assert_eq!(wants_more(&file[..1024 + 32], 1024, SEGMENT as usize), Some(1024 + 128));
        assert_eq!(
            wants_more(&file[..1024 + 200], 1024, SEGMENT as usize),
            Some(1024 + 128 + 2016)
        );
        assert_eq!(wants_more(&file[..end], 1024, SEGMENT as usize), Some(end + 32));
        assert_eq!(wants_more(&file[..end + 32], 1024, SEGMENT as usize), None);
        assert_eq!(wants_more(&file[..end + 32], 1024, end), None, "the segment's end");
    }
}
