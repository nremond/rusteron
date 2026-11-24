//! A replay session, as Java's `ReplaySession`, without its I/O: which
//! segment file to open and where to read in it, the frames of a read made
//! ready to offer (their session and stream ids the replay's), where the
//! replay is after an offer, and how it follows a recording still being
//! written. The shell opens and reads the files, offers the blocks and reads
//! the limit counter.
//!
//! ```text
//! Init:   the segment file exists (the connect timeout, else) · the frame at the position checked
//!         (unless the recording's start or stop) · the client told OK · the subscriber connects
//! Replay: each cycle, one read of at most the buffer, never across a term; its data frames
//!         offered as one block, a padding frame appended after them; following the limit
//!         counter once the stop is reached
//! ```
//!
//! Where it differs from Java: a frame of an unknown type, and a block the
//! publication refuses as malformed, end the replay with an error (Java's
//! loops on them).

#![deny(unsafe_code)]
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

/// The I/O half, on Aeron's C client.
#[cfg(feature = "server")]
pub mod io;

use crate::control_session_adapter::error_code;
use crate::logbuffer::{FRAME_ALIGNMENT, HEADER_LENGTH};
use crate::segments::{PADDING_FRAME_TYPE, segment_file_base_position, segment_file_name};

/// Aeron's longest term: a replay offers no more than this at once.
pub const TERM_MAX_LENGTH: i64 = 1024 * 1024 * 1024;
const DATA_FRAME_TYPE: u16 = 1;
const TERM_OFFSET_OFFSET: usize = 8;
const SESSION_ID_OFFSET: usize = 12;
const STREAM_ID_OFFSET: usize = 16;
const TERM_ID_OFFSET: usize = 20;
const TYPE_OFFSET: usize = 6;

fn i32_at(bytes: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("4 bytes"))
}

fn align(value: i64) -> i64 {
    (value + i64::from(FRAME_ALIGNMENT) - 1) & !(i64::from(FRAME_ALIGNMENT) - 1)
}

/// What a replay replays, as the conductor accepted it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayParams {
    pub replay_session_id: i64,
    pub recording_id: i64,
    /// The recording's start.
    pub start_position: i64,
    pub replay_position: i64,
    /// Where the replay stops unless its limit counter moves it.
    pub stop_position: i64,
    pub replay_length: i64,
    pub term_length: i32,
    pub segment_length: i32,
    pub initial_term_id: i32,
    /// The recording's stream: the frame at the position must carry it.
    pub stream_id: i32,
    /// The replay follows a counter: a bounded replay's, or the recording's own.
    pub follows_counter: bool,
    /// The most read at once.
    pub buffer_length: usize,
    pub connect_deadline_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayState {
    Init,
    Replay,
    /// Done: its publication closed (`revoke`: without lingering).
    Inactive {
        revoke: bool,
    },
}

/// Why a replay failed: sent to its client with its correlation id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayError {
    pub code: i64,
    pub message: String,
}

/// What the limit counter shows once the replay has caught up with its stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limit {
    pub value: i64,
    pub closed: bool,
    /// Its id still names the counter the replay followed.
    pub same_registration: bool,
}

/// A read's frames made ready: `data` bytes to offer as a block, then a
/// padding frame of `padding` bytes (its header included) to append.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Batch {
    pub data: usize,
    pub padding: usize,
}

/// A replay session's state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaySession {
    p: ReplayParams,
    segment_base: i64,
    term_base_segment_offset: i64,
    term_offset: i64,
    term_id: i32,
    replay_position: i64,
    stop_position: i64,
    replay_limit: i64,
    file_open: bool,
    state: ReplayState,
}

impl ReplaySession {
    pub fn new(p: ReplayParams) -> Self {
        ReplaySession {
            segment_base: segment_file_base_position(
                p.start_position,
                p.replay_position,
                p.term_length,
                p.segment_length,
            ),
            term_base_segment_offset: 0,
            term_offset: 0,
            term_id: 0,
            replay_position: p.replay_position,
            stop_position: p.stop_position,
            replay_limit: p.replay_position.saturating_add(p.replay_length),
            file_open: false,
            state: ReplayState::Init,
            p,
        }
    }

    pub fn params(&self) -> &ReplayParams {
        &self.p
    }

    pub fn state(&self) -> ReplayState {
        self.state
    }

    pub fn replay_position(&self) -> i64 {
        self.replay_position
    }

    /// The segment file the replay reads now.
    pub fn segment_file_name(&self) -> String {
        segment_file_name(self.p.recording_id, self.segment_base)
    }

    /// Where the next read starts in the segment file.
    pub fn file_offset(&self) -> u64 {
        (self.term_base_segment_offset + self.term_offset) as u64
    }

    /// Stopped as its recording goes, by its client, or as the archive
    /// closes: the publication revoked.
    pub fn abort(&mut self) {
        self.state = ReplayState::Inactive { revoke: true };
    }

    /// Stopped by its client, on a channel that does not signal the end of
    /// stream (`eos=false`): the publication closed as at the replay's end,
    /// not revoked, which would signal it anyway. Java's 1.54 archive revokes
    /// it (`ReplaySession` aborted), and a revoked image is dropped at once
    /// wherever its readers are: a cluster follower whose leader stops a
    /// catch-up replay it still reads loses what its service had not read,
    /// and the service refuses the next log for good (aeron-io/aeron#2259;
    /// #2260 closes it).
    pub fn stop(&mut self) {
        self.state = ReplayState::Inactive { revoke: false };
    }

    /// The subscriber is gone, or the publication closed.
    pub fn on_disconnected(&mut self) {
        self.state = ReplayState::Inactive { revoke: true };
    }

    /// `raiseError`: the replay ends, its client told why.
    pub fn fail(&mut self, code: i64, message: &str) -> ReplayError {
        self.state = ReplayState::Inactive { revoke: true };
        ReplayError {
            code,
            message: format!(
                "{message}, recordingId={}, replaySessionId={}, segmentFile={}",
                self.p.recording_id,
                self.p.replay_session_id,
                self.segment_file_name()
            ),
        }
    }

    /// The segment file is not there yet: an error once the connect timeout passed.
    pub fn on_file_missing(&mut self, now_ms: i64) -> Option<ReplayError> {
        (now_ms > self.p.connect_deadline_ms)
            .then(|| self.fail(error_code::GENERIC, "recording segment file not created"))
    }

    /// The segment file opened: where the frame at the replay's position is
    /// to be checked, if it is (`None`: at the recording's start or stop).
    pub fn on_file_opened(&mut self) -> Option<u64> {
        let p = &self.p;
        let term_mask = i64::from(p.term_length) - 1;
        let start_term_base = p.start_position - (p.start_position & term_mask);
        let segment_offset = (self.replay_position - start_term_base) & (i64::from(p.segment_length) - 1);
        let bits = p.term_length.trailing_zeros();
        self.term_id = p.initial_term_id.wrapping_add((self.replay_position >> bits) as i32);
        self.term_offset = self.replay_position & term_mask;
        self.term_base_segment_offset = segment_offset - self.term_offset;
        self.file_open = true;
        (self.replay_position > p.start_position && self.replay_position != self.stop_position)
            .then_some(segment_offset as u64)
    }

    pub fn is_file_open(&self) -> bool {
        self.file_open
    }

    /// `framePosition`: a frame's place, for errors.
    fn frame_position(&self, frame_offset: i64) -> String {
        format!(
            "{} (segmentFilePosition={}, segmentOffset={}, termOffset={}, frameOffset={frame_offset})",
            self.segment_base + self.term_base_segment_offset + self.term_offset + frame_offset,
            self.segment_base,
            self.term_base_segment_offset,
            self.term_offset
        )
    }

    /// The header read at the replay's position (shorter if the file ended):
    /// a frame of this term, at this offset, of the recording's stream.
    pub fn check_header(&mut self, header: &[u8]) -> Result<(), ReplayError> {
        if header.len() < HEADER_LENGTH as usize {
            return Err(self.fail(
                error_code::GENERIC,
                "java.io.IOException: failed to read fragment header",
            ));
        }
        let valid = i64::from(i32_at(header, TERM_OFFSET_OFFSET)) == self.term_offset
            && i32_at(header, TERM_ID_OFFSET) == self.term_id
            && i32_at(header, STREAM_ID_OFFSET) == self.p.stream_id;
        if !valid {
            let message = format!(
                "replayPosition={} does not point to a valid frame",
                self.frame_position(0)
            );
            return Err(self.fail(error_code::INVALID_POSITION, &message));
        }
        Ok(())
    }

    /// The subscriber is not there yet: an error once the connect timeout passed.
    pub fn on_not_connected(&mut self, now_ms: i64, channel: &str, stream_id: i32) -> Option<ReplayError> {
        (now_ms > self.p.connect_deadline_ms).then(|| {
            let message = format!("no connection established for replayChannel={channel}, replayStreamId={stream_id}");
            self.fail(error_code::GENERIC, &message)
        })
    }

    /// Connected: the replay starts.
    pub fn on_connected(&mut self) {
        if self.state == ReplayState::Init {
            self.state = ReplayState::Replay;
        }
    }

    /// Java's "empty replay": nothing will ever come.
    pub fn is_empty(&mut self) -> bool {
        if self.p.start_position == self.stop_position && self.replay_limit == 0 {
            self.state = ReplayState::Inactive { revoke: false };
            return true;
        }
        false
    }

    /// Whether the replay waits on its limit counter (read it, then
    /// [`ReplaySession::on_limit`]).
    pub fn awaits_limit(&self) -> bool {
        self.p.follows_counter && self.replay_position >= self.stop_position
    }

    /// `notExtended`: whether the replay still waits, after its counter
    /// moved or closed; it may end here.
    pub fn on_limit(&mut self, limit: Limit) -> bool {
        let old_stop = self.stop_position;
        let mut new_stop = old_stop;
        if limit.closed {
            if limit.same_registration {
                self.replay_limit = limit.value;
                new_stop = old_stop.max(limit.value);
            } else if self.replay_limit >= old_stop {
                self.replay_limit = old_stop;
            }
        } else {
            new_stop = limit.value;
        }
        if self.replay_position >= self.replay_limit {
            self.state = ReplayState::Inactive { revoke: false };
            true
        } else if new_stop > old_stop {
            self.stop_position = new_stop;
            false
        } else {
            true
        }
    }

    /// At a term's end: the next term, in the next segment file if this one
    /// is done (`Some`: open it).
    pub fn next_term_if_due(&mut self) -> Option<String> {
        if self.term_offset != i64::from(self.p.term_length) {
            return None;
        }
        self.term_offset = 0;
        self.term_base_segment_offset += i64::from(self.p.term_length);
        if self.term_base_segment_offset == i64::from(self.p.segment_length) {
            self.segment_base += i64::from(self.p.segment_length);
            self.term_base_segment_offset = 0;
            return Some(self.segment_file_name());
        }
        None
    }

    /// The next segment file is not there.
    pub fn on_segment_missing(&mut self) -> ReplayError {
        self.fail(error_code::GENERIC, "recording segment not found")
    }

    /// How much to read now: up to the stop, the buffer and the term's end.
    pub fn read_length(&self) -> usize {
        let to_stop = (self.stop_position - self.replay_position).max(0);
        let to_term_end = i64::from(self.p.term_length) - self.term_offset;
        to_stop.min(self.p.buffer_length as i64).min(to_term_end) as usize
    }

    /// The bytes read (as many as asked; the file's end reads as zeros): the
    /// whole data frames, their session and stream ids rewritten, then a
    /// padding frame if one follows them.
    pub fn prepare(&mut self, read: &mut [u8], session_id: i32, stream_id: i32) -> Result<Batch, ReplayError> {
        let remaining = (self.replay_limit - self.replay_position).min(TERM_MAX_LENGTH);
        let bytes_read = read.len() as i64;
        let mut batch_offset = 0i64;
        let mut padding = 0;
        while batch_offset < bytes_read && batch_offset < remaining {
            let at = batch_offset as usize;
            if at + HEADER_LENGTH as usize > read.len() {
                break;
            }
            let length = i32_at(read, at);
            if length <= 0 {
                let message = format!(
                    "unexpected end of recording at position={}",
                    self.frame_position(batch_offset)
                );
                return Err(self.fail(error_code::GENERIC, &message));
            }
            let frame_type = u16::from_le_bytes([read[at + TYPE_OFFSET], read[at + TYPE_OFFSET + 1]]);
            let aligned = align(i64::from(length));
            match frame_type {
                DATA_FRAME_TYPE => {
                    if batch_offset + aligned > bytes_read {
                        break;
                    }
                    read[at + SESSION_ID_OFFSET..at + SESSION_ID_OFFSET + 4].copy_from_slice(&session_id.to_le_bytes());
                    read[at + STREAM_ID_OFFSET..at + STREAM_ID_OFFSET + 4].copy_from_slice(&stream_id.to_le_bytes());
                    batch_offset += aligned;
                }
                PADDING_FRAME_TYPE => {
                    padding = length as usize;
                    break;
                }
                other => {
                    let message = format!(
                        "unexpected frame type={other} at position={}",
                        self.frame_position(batch_offset)
                    );
                    return Err(self.fail(error_code::GENERIC, &message));
                }
            }
        }
        Ok(Batch {
            data: batch_offset as usize,
            padding,
        })
    }

    /// `hasPublicationAdvanced`: an offer of `length` bytes came to
    /// `position` (Aeron's result: negative when not taken). Whether it did.
    pub fn on_offered(&mut self, position: i64, length: usize) -> bool {
        const NOT_CONNECTED: i64 = -1;
        const CLOSED: i64 = -4;
        if position > 0 {
            let aligned = align(length as i64);
            self.term_offset += aligned;
            self.replay_position += aligned;
            if self.replay_position >= self.replay_limit {
                self.state = ReplayState::Inactive { revoke: false };
            }
            true
        } else {
            if position == CLOSED || position == NOT_CONNECTED {
                self.on_disconnected();
            }
            false
        }
    }
}
