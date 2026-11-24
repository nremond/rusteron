//! The catalog of recordings, `archive.catalog`, as Aeron Archive 1.54.2
//! writes it (`Catalog.java`), held as an image of the file: the shell reads
//! the file into it, and writes back the ranges a change dirtied
//! ([`Catalog::take_writes`]), syncing as the archive's `catalogFileSyncLevel`
//! asks. Java maps the file; the bytes are the same.
//!
//! ```text
//! file:   header (32 bytes) · entries, each aligned (64 bytes in a catalog this version writes) · zeros
//! header: version i32 · length i32 (32) · nextRecordingId i64 · alignment i32 · reserved to byte 31
//! entry:  descriptor header (32 bytes): length i32 (of what follows, padding included) · state i32 ·
//!         checksum i32 · reserved to byte 31
//!         descriptor (80-byte block, then strippedChannel, originalChannel, sourceIdentity,
//!         each a u32 length and ASCII) · zero padding to the alignment
//! ```
//!
//! A recording's entry never moves while this archive runs: space is not
//! reused, a purged recording's entry stays, marked deleted. Only VALID
//! entries are indexed; a length of 0 ends the entries.

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

use std::collections::BTreeMap;
use std::fmt;

use crate::codecs::archive::{WriteBuf, recording_descriptor_codec::RecordingDescriptorEncoder};

/// The file's name in the archive's directory.
pub const CATALOG_FILE_NAME: &str = "archive.catalog";
/// The archive's semantic version, 3.1.0: the catalog's and the mark file's.
pub const SEMANTIC_VERSION: i32 = 0x03_01_00;
const MAJOR_VERSION: i32 = 3;
/// The catalog's header, and a descriptor's.
pub const HEADER_LENGTH: usize = 32;
pub const DESCRIPTOR_HEADER_LENGTH: usize = 32;
/// The descriptor's fixed block.
pub const DESCRIPTOR_BLOCK_LENGTH: usize = 80;
/// Java's default capacity, and the smallest it accepts.
pub const DEFAULT_CAPACITY: usize = 1 << 20;
pub const MIN_CAPACITY: usize = HEADER_LENGTH;
/// The largest catalog Java maps.
const MAX_CAPACITY: usize = i32::MAX as usize;
/// Entries' alignment in a catalog this version creates.
const ALIGNMENT: usize = 64;
/// A catalog older than version 3 has no alignment in its header: its
/// entries are 1 KiB each, from 1 KiB.
const LEGACY_ALIGNMENT: usize = 1024;

pub const NULL_POSITION: i64 = -1;
pub const NULL_TIMESTAMP: i64 = -1;

// The catalog header's fields.
const VERSION_OFFSET: usize = 0;
const LENGTH_OFFSET: usize = 4;
const NEXT_RECORDING_ID_OFFSET: usize = 8;
const ALIGNMENT_OFFSET: usize = 16;
// A descriptor header's.
const ENTRY_LENGTH_OFFSET: usize = 0;
const STATE_OFFSET: usize = 4;
const CHECKSUM_OFFSET: usize = 8;
// A descriptor's, from the start of its block.
const CONTROL_SESSION_ID_OFFSET: usize = 0;
const CORRELATION_ID_OFFSET: usize = 8;
const RECORDING_ID_OFFSET: usize = 16;
const START_TIMESTAMP_OFFSET: usize = 24;
const STOP_TIMESTAMP_OFFSET: usize = 32;
const START_POSITION_OFFSET: usize = 40;
const STOP_POSITION_OFFSET: usize = 48;
const INITIAL_TERM_ID_OFFSET: usize = 56;
const SEGMENT_FILE_LENGTH_OFFSET: usize = 60;
const TERM_BUFFER_LENGTH_OFFSET: usize = 64;
const MTU_LENGTH_OFFSET: usize = 68;
const SESSION_ID_OFFSET: usize = 72;
const STREAM_ID_OFFSET: usize = 76;

/// A recording's state in its entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordingState {
    Invalid = 0,
    Valid = 1,
    Deleted = 2,
}

/// A recording, as its entry describes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Descriptor {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub recording_id: i64,
    pub start_timestamp: i64,
    pub stop_timestamp: i64,
    pub start_position: i64,
    pub stop_position: i64,
    pub initial_term_id: i32,
    pub segment_file_length: i32,
    pub term_buffer_length: i32,
    pub mtu_length: i32,
    pub session_id: i32,
    pub stream_id: i32,
    pub stripped_channel: String,
    pub original_channel: String,
    pub source_identity: String,
}

/// Why a catalog cannot be opened or changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogError {
    /// Another major version than this archive's.
    Version(i32),
    /// A capacity Java does not accept.
    Capacity(usize),
    /// The header's next recording id is behind the entries'.
    NextRecordingId {
        expected: i64,
        actual: i64,
    },
    /// An entry runs past the file.
    Corrupt {
        offset: usize,
    },
    /// No room left: the catalog is at Java's largest.
    Full,
    UnknownRecording(i64),
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Version(v) => write!(
                f,
                "incompatible catalog file version {}.{}.{}, archive software is 3.1.0",
                (v >> 16) & 0xff,
                (v >> 8) & 0xff,
                v & 0xff
            ),
            Self::Capacity(c) => write!(f, "invalid catalog capacity: {c}"),
            Self::NextRecordingId { expected, actual } => write!(
                f,
                "invalid nextRecordingId: expected value greater or equal to {expected}, was {actual}"
            ),
            Self::Corrupt { offset } => write!(f, "catalog entry at {offset} runs past the file"),
            Self::Full => write!(f, "catalog is full, max capacity reached: {MAX_CAPACITY}"),
            Self::UnknownRecording(id) => write!(f, "unknown recording id: {id}"),
        }
    }
}

impl std::error::Error for CatalogError {}

/// The catalog: its file's image, its index of valid recordings, and the
/// ranges changed since the last [`Catalog::take_writes`].
#[derive(Debug)]
pub struct Catalog {
    image: Vec<u8>,
    alignment: usize,
    /// The first entry's offset: after the header, or at 1 KiB in a legacy catalog.
    first_offset: usize,
    next_offset: usize,
    next_recording_id: i64,
    /// Valid recordings, by id: their entry's offset.
    index: BTreeMap<i64, usize>,
    dirty: Vec<(usize, usize)>,
}

fn get_i32(b: &[u8], at: usize) -> i32 {
    i32::from_le_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}

fn get_i64(b: &[u8], at: usize) -> i64 {
    i64::from_le_bytes(b[at..at + 8].try_into().expect("8 bytes"))
}

const fn align(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

impl Catalog {
    /// A new, empty catalog of `capacity` bytes: the file's whole length,
    /// as Java maps it.
    pub fn create(capacity: usize) -> Result<Self, CatalogError> {
        if !(MIN_CAPACITY..=MAX_CAPACITY).contains(&capacity) {
            return Err(CatalogError::Capacity(capacity));
        }
        let mut catalog = Catalog {
            image: vec![0; capacity],
            alignment: ALIGNMENT,
            first_offset: HEADER_LENGTH,
            next_offset: HEADER_LENGTH,
            next_recording_id: 0,
            index: BTreeMap::new(),
            dirty: Vec::new(),
        };
        catalog.put_i32(VERSION_OFFSET, SEMANTIC_VERSION);
        catalog.put_i32(LENGTH_OFFSET, HEADER_LENGTH as i32);
        catalog.put_i64(NEXT_RECORDING_ID_OFFSET, 0);
        catalog.put_i32(ALIGNMENT_OFFSET, ALIGNMENT as i32);
        catalog.dirty = vec![(0, capacity)];
        Ok(catalog)
    }

    /// The catalog in `file`, the file's bytes, mapped as Java maps them: to
    /// `capacity` at least. Its entries indexed, its next recording id
    /// checked against them. Recordings left active by a crash still have no
    /// stop position: [`Self::unstopped`] lists them for the shell to recover.
    pub fn open(mut file: Vec<u8>, capacity: usize) -> Result<Self, CatalogError> {
        if !(MIN_CAPACITY..=MAX_CAPACITY).contains(&capacity) {
            return Err(CatalogError::Capacity(capacity));
        }
        if file.len() < capacity {
            file.resize(capacity, 0);
        }
        let version = get_i32(&file, VERSION_OFFSET);
        if version >> 16 != MAJOR_VERSION {
            return Err(CatalogError::Version(version));
        }
        let alignment = get_i32(&file, ALIGNMENT_OFFSET);
        let (alignment, first) = if alignment > 0 {
            (alignment as usize, HEADER_LENGTH)
        } else {
            (LEGACY_ALIGNMENT, LEGACY_ALIGNMENT)
        };
        let mut catalog = Catalog {
            next_recording_id: if first == HEADER_LENGTH {
                get_i64(&file, NEXT_RECORDING_ID_OFFSET)
            } else {
                0
            },
            image: file,
            alignment,
            first_offset: first,
            next_offset: first,
            index: BTreeMap::new(),
            dirty: Vec::new(),
        };
        let mut offset = first;
        let mut last_id = -1;
        while offset + DESCRIPTOR_HEADER_LENGTH <= catalog.image.len() {
            let length = get_i32(&catalog.image, offset + ENTRY_LENGTH_OFFSET);
            if length <= 0 {
                break;
            }
            let frame = align(length as usize + DESCRIPTOR_HEADER_LENGTH, alignment);
            if offset + frame > catalog.image.len() {
                return Err(CatalogError::Corrupt { offset });
            }
            last_id = get_i64(&catalog.image, offset + DESCRIPTOR_HEADER_LENGTH + RECORDING_ID_OFFSET);
            if get_i32(&catalog.image, offset + STATE_OFFSET) == RecordingState::Valid as i32 {
                catalog.index.insert(last_id, offset);
            }
            offset += frame;
        }
        catalog.next_offset = offset;
        if catalog.next_recording_id == 0 {
            catalog.next_recording_id = last_id + 1;
        } else if catalog.next_recording_id < last_id + 1 {
            return Err(CatalogError::NextRecordingId {
                expected: last_id + 1,
                actual: catalog.next_recording_id,
            });
        }
        Ok(catalog)
    }

    /// The file's length: its image's.
    pub fn len(&self) -> usize {
        self.image.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn next_recording_id(&self) -> i64 {
        self.next_recording_id
    }

    /// The ranges changed since the last call, as `(offset, bytes)`, merged
    /// and in order; the file grows to the image's length.
    pub fn take_writes(&mut self) -> Vec<(u64, Vec<u8>)> {
        let mut ranges = std::mem::take(&mut self.dirty);
        ranges.sort_unstable();
        let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
        for (start, end) in ranges {
            match merged.last_mut() {
                Some(last) if start <= last.1 => last.1 = last.1.max(end),
                _ => merged.push((start, end)),
            }
        }
        merged
            .into_iter()
            .map(|(start, end)| (start as u64, self.image[start..end].to_vec()))
            .collect()
    }

    fn mark(&mut self, at: usize, length: usize) {
        self.dirty.push((at, at + length));
    }

    fn put_i32(&mut self, at: usize, value: i32) {
        self.image[at..at + 4].copy_from_slice(&value.to_le_bytes());
        self.mark(at, 4);
    }

    fn put_i64(&mut self, at: usize, value: i64) {
        self.image[at..at + 8].copy_from_slice(&value.to_le_bytes());
        self.mark(at, 8);
    }

    /// Adds a recording with `descriptor`'s fields, its id the next one (the
    /// descriptor's own id, control session and correlation ids are not
    /// written, as Java's). Returns the id.
    pub fn add_recording(&mut self, descriptor: &Descriptor) -> Result<i64, CatalogError> {
        let length = DESCRIPTOR_BLOCK_LENGTH
            + 12
            + descriptor.stripped_channel.len()
            + descriptor.original_channel.len()
            + descriptor.source_identity.len();
        let frame = align(DESCRIPTOR_HEADER_LENGTH + length, self.alignment);
        if self.next_offset + frame > self.image.len() {
            self.grow(frame)?;
        }
        let offset = self.next_offset;
        let id = self.next_recording_id;
        {
            let entry = &mut self.image[offset..offset + frame];
            let mut e = RecordingDescriptorEncoder::default().wrap(WriteBuf::new(entry), DESCRIPTOR_HEADER_LENGTH);
            e.recording_id(id)
                .start_timestamp(descriptor.start_timestamp)
                .stop_timestamp(descriptor.stop_timestamp)
                .start_position(descriptor.start_position)
                .stop_position(descriptor.stop_position)
                .initial_term_id(descriptor.initial_term_id)
                .segment_file_length(descriptor.segment_file_length)
                .term_buffer_length(descriptor.term_buffer_length)
                .mtu_length(descriptor.mtu_length)
                .session_id(descriptor.session_id)
                .stream_id(descriptor.stream_id)
                .stripped_channel(descriptor.stripped_channel.as_bytes())
                .original_channel(descriptor.original_channel.as_bytes())
                .source_identity(descriptor.source_identity.as_bytes());
        }
        self.mark(offset + DESCRIPTOR_HEADER_LENGTH, frame - DESCRIPTOR_HEADER_LENGTH);
        // The header's length counts the padding, as Java's.
        self.put_i32(offset + ENTRY_LENGTH_OFFSET, (frame - DESCRIPTOR_HEADER_LENGTH) as i32);
        self.put_i32(offset + CHECKSUM_OFFSET, 0);
        self.put_i32(offset + STATE_OFFSET, RecordingState::Valid as i32);
        self.put_i64(NEXT_RECORDING_ID_OFFSET, id + 1);
        self.next_recording_id = id + 1;
        self.next_offset = offset + frame;
        self.index.insert(id, offset);
        Ok(id)
    }

    /// Rewrites a recording's descriptor with `descriptor`'s fields, its
    /// control session and correlation ids kept, as `Catalog.replaceRecording`:
    /// in place if it fits (the frame's old tail zeroed, its length kept);
    /// else the entries after it moved along to make room.
    pub fn replace_recording(&mut self, id: i64, descriptor: &Descriptor) -> Result<(), CatalogError> {
        let offset = self.entry(id)?;
        let old_length = get_i32(&self.image, offset + ENTRY_LENGTH_OFFSET) as usize;
        let old_frame = align(old_length + DESCRIPTOR_HEADER_LENGTH, self.alignment);
        let strings =
            descriptor.stripped_channel.len() + descriptor.original_channel.len() + descriptor.source_identity.len();
        let new_frame = align(
            DESCRIPTOR_HEADER_LENGTH + DESCRIPTOR_BLOCK_LENGTH + 12 + strings,
            self.alignment,
        );
        let length = if new_frame > old_frame {
            let shift = new_frame - old_frame;
            let end = self.next_offset;
            if end + shift > self.image.len() {
                self.grow(shift)?;
            }
            self.image.copy_within(offset + old_frame..end, offset + new_frame);
            self.mark(offset + new_frame, end + shift - (offset + new_frame));
            for at in self.index.values_mut() {
                if *at > offset {
                    *at += shift;
                }
            }
            self.next_offset += shift;
            new_frame - DESCRIPTOR_HEADER_LENGTH
        } else {
            self.image[offset + new_frame..offset + old_frame].fill(0);
            self.mark(offset + new_frame, old_frame - new_frame);
            old_frame - DESCRIPTOR_HEADER_LENGTH
        };
        {
            let entry = &mut self.image[offset..offset + new_frame];
            let mut e = RecordingDescriptorEncoder::default().wrap(WriteBuf::new(entry), DESCRIPTOR_HEADER_LENGTH);
            e.recording_id(id)
                .start_timestamp(descriptor.start_timestamp)
                .stop_timestamp(descriptor.stop_timestamp)
                .start_position(descriptor.start_position)
                .stop_position(descriptor.stop_position)
                .initial_term_id(descriptor.initial_term_id)
                .segment_file_length(descriptor.segment_file_length)
                .term_buffer_length(descriptor.term_buffer_length)
                .mtu_length(descriptor.mtu_length)
                .session_id(descriptor.session_id)
                .stream_id(descriptor.stream_id)
                .stripped_channel(descriptor.stripped_channel.as_bytes())
                .original_channel(descriptor.original_channel.as_bytes())
                .source_identity(descriptor.source_identity.as_bytes());
        }
        self.mark(offset + DESCRIPTOR_HEADER_LENGTH, new_frame - DESCRIPTOR_HEADER_LENGTH);
        self.put_i32(offset + ENTRY_LENGTH_OFFSET, length as i32);
        self.put_i32(offset + CHECKSUM_OFFSET, 0);
        self.put_i32(offset + STATE_OFFSET, RecordingState::Valid as i32);
        Ok(())
    }

    /// Grows the image by half again until `frame` more bytes fit, as Java
    /// remaps its file.
    fn grow(&mut self, frame: usize) -> Result<(), CatalogError> {
        let target = self.next_offset + frame;
        if target > MAX_CAPACITY {
            return Err(CatalogError::Full);
        }
        let mut capacity = self.image.len();
        while capacity < target {
            capacity = (capacity + (capacity >> 1)).min(MAX_CAPACITY);
        }
        let old = self.image.len();
        self.image.resize(capacity, 0);
        self.mark(old, capacity - old);
        Ok(())
    }

    fn entry(&self, id: i64) -> Result<usize, CatalogError> {
        self.index.get(&id).copied().ok_or(CatalogError::UnknownRecording(id))
    }

    fn field(&self, id: i64, offset: usize) -> Result<usize, CatalogError> {
        Ok(self.entry(id)? + DESCRIPTOR_HEADER_LENGTH + offset)
    }

    /// A recording stopped at `position`, at `timestamp` (ms).
    pub fn recording_stopped(&mut self, id: i64, position: i64, timestamp: i64) -> Result<(), CatalogError> {
        let at = self.field(id, 0)?;
        self.put_i64(at + STOP_TIMESTAMP_OFFSET, timestamp);
        self.put_i64(at + STOP_POSITION_OFFSET, position);
        Ok(())
    }

    /// A recording's stop position moved: truncated, or its segments migrated.
    pub fn set_stop_position(&mut self, id: i64, position: i64) -> Result<(), CatalogError> {
        let at = self.field(id, STOP_POSITION_OFFSET)?;
        self.put_i64(at, position);
        Ok(())
    }

    /// A recording's start position moved: segments detached or attached.
    pub fn set_start_position(&mut self, id: i64, position: i64) -> Result<(), CatalogError> {
        let at = self.field(id, START_POSITION_OFFSET)?;
        self.put_i64(at, position);
        Ok(())
    }

    /// A recording extended: active again, from a new session.
    pub fn extend_recording(
        &mut self,
        id: i64,
        control_session_id: i64,
        correlation_id: i64,
        session_id: i32,
    ) -> Result<(), CatalogError> {
        let at = self.field(id, 0)?;
        self.put_i64(at + CONTROL_SESSION_ID_OFFSET, control_session_id);
        self.put_i64(at + CORRELATION_ID_OFFSET, correlation_id);
        self.put_i64(at + STOP_TIMESTAMP_OFFSET, NULL_TIMESTAMP);
        self.put_i32(at + SESSION_ID_OFFSET, session_id);
        self.put_i64(at + STOP_POSITION_OFFSET, NULL_POSITION);
        Ok(())
    }

    /// A recording's stop position recovered after a crash, at `timestamp`.
    pub fn recover_stop_position(&mut self, id: i64, position: i64, timestamp: i64) -> Result<(), CatalogError> {
        let at = self.field(id, 0)?;
        self.put_i64(at + STOP_POSITION_OFFSET, position);
        self.put_i64(at + STOP_TIMESTAMP_OFFSET, timestamp);
        Ok(())
    }

    /// Sets a recording's state, and drops it from the index: no longer a
    /// recording of this archive's. Whether it was one.
    pub fn change_state(&mut self, id: i64, state: RecordingState) -> bool {
        match self.index.remove(&id) {
            Some(offset) => {
                self.put_i32(offset + STATE_OFFSET, state as i32);
                true
            }
            None => false,
        }
    }

    pub fn has_recording(&self, id: i64) -> bool {
        self.index.contains_key(&id)
    }

    /// The valid recordings' ids, in order.
    pub fn recording_ids(&self) -> impl Iterator<Item = i64> + '_ {
        self.index.keys().copied()
    }

    /// The last recording from `min_id` of this session and stream whose
    /// original channel holds `fragment` (`Catalog.findLast`); -1 if none.
    pub fn find_last(&self, min_id: i64, session_id: i32, stream_id: i32, fragment: &[u8]) -> i64 {
        if min_id < 0 {
            return NULL_POSITION;
        }
        let holds =
            |channel: &str| fragment.is_empty() || channel.as_bytes().windows(fragment.len()).any(|w| w == fragment);
        self.index
            .range(min_id..)
            .rev()
            .map(|(&id, _)| id)
            .find(|&id| {
                self.descriptor(id)
                    .is_ok_and(|d| d.session_id == session_id && d.stream_id == stream_id && holds(&d.original_channel))
            })
            .unwrap_or(NULL_POSITION)
    }

    /// The valid recordings' ids from `from`, in order.
    pub fn recording_ids_from(&self, from: i64) -> impl Iterator<Item = i64> + '_ {
        self.index.range(from..).map(|(&id, _)| id)
    }

    pub fn start_position(&self, id: i64) -> Result<i64, CatalogError> {
        Ok(get_i64(&self.image, self.field(id, START_POSITION_OFFSET)?))
    }

    pub fn stop_position(&self, id: i64) -> Result<i64, CatalogError> {
        Ok(get_i64(&self.image, self.field(id, STOP_POSITION_OFFSET)?))
    }

    /// A recording's descriptor, its block and its strings with the padding
    /// after them: what Java sends a client for it, after a message header.
    pub fn descriptor_bytes(&self, id: i64) -> Result<&[u8], CatalogError> {
        let offset = self.entry(id)?;
        let length = get_i32(&self.image, offset + ENTRY_LENGTH_OFFSET) as usize;
        let start = offset + DESCRIPTOR_HEADER_LENGTH;
        Ok(&self.image[start..start + length])
    }

    pub fn descriptor(&self, id: i64) -> Result<Descriptor, CatalogError> {
        let offset = self.entry(id)?;
        decode(&self.image, offset).ok_or(CatalogError::Corrupt { offset })
    }

    /// Valid recordings whose stop position is not known: active when the
    /// archive stopped, or still so.
    pub fn unstopped(&self) -> Vec<Descriptor> {
        self.index
            .values()
            .filter_map(|&offset| decode(&self.image, offset))
            .filter(|d| d.stop_position == NULL_POSITION)
            .collect()
    }

    /// Every entry, valid or not, in the file's order: its state's value in
    /// the entry (a [`RecordingState`]'s, or another a damaged file holds),
    /// and its descriptor. What `ArchiveTool verify` walks.
    pub fn entries(&self) -> Vec<(i32, Descriptor)> {
        self.entry_offsets()
            .into_iter()
            .filter_map(|offset| {
                Some((
                    get_i32(&self.image, offset + STATE_OFFSET),
                    decode(&self.image, offset)?,
                ))
            })
            .collect()
    }

    /// What `ArchiveTool verify` writes of a recording, valid or not: its
    /// stop position and stop timestamp if `stop` (recovered from its frames),
    /// and its state, a valid recording indexed, any other not. Whether the
    /// catalog has the recording.
    pub fn verified(&mut self, id: i64, stop: Option<(i64, i64)>, state: RecordingState) -> bool {
        let offset = match self.index.get(&id) {
            Some(&offset) => offset,
            None => match self
                .entry_offsets()
                .into_iter()
                .find(|&offset| get_i64(&self.image, offset + DESCRIPTOR_HEADER_LENGTH + RECORDING_ID_OFFSET) == id)
            {
                Some(offset) => offset,
                None => return false,
            },
        };
        if let Some((position, timestamp)) = stop {
            let at = offset + DESCRIPTOR_HEADER_LENGTH;
            self.put_i64(at + STOP_POSITION_OFFSET, position);
            self.put_i64(at + STOP_TIMESTAMP_OFFSET, timestamp);
        }
        self.put_i32(offset + STATE_OFFSET, state as i32);
        if state == RecordingState::Valid {
            self.index.insert(id, offset);
        } else {
            self.index.remove(&id);
        }
        true
    }

    /// Every entry's offset, in the file's order.
    fn entry_offsets(&self) -> Vec<usize> {
        let mut offsets = Vec::new();
        let mut offset = self.first_offset;
        while offset < self.next_offset {
            let length = get_i32(&self.image, offset + ENTRY_LENGTH_OFFSET);
            if length <= 0 {
                break;
            }
            offsets.push(offset);
            offset += align(length as usize + DESCRIPTOR_HEADER_LENGTH, self.alignment);
        }
        offsets
    }
}

/// The descriptor of the entry at `offset`, its strings checked against the
/// entry's length.
fn decode(image: &[u8], offset: usize) -> Option<Descriptor> {
    let length = usize::try_from(get_i32(image, offset + ENTRY_LENGTH_OFFSET)).ok()?;
    let block = offset + DESCRIPTOR_HEADER_LENGTH;
    let end = block.checked_add(length)?.min(image.len());
    if block + DESCRIPTOR_BLOCK_LENGTH > end {
        return None;
    }
    let mut at = block + DESCRIPTOR_BLOCK_LENGTH;
    let mut string = || {
        let n = u32::from_le_bytes(image.get(at..at + 4)?.try_into().ok()?) as usize;
        let s = image.get(at + 4..at + 4 + n).filter(|_| at + 4 + n <= end)?;
        at += 4 + n;
        Some(String::from_utf8_lossy(s).into_owned())
    };
    let (stripped_channel, original_channel, source_identity) = (string()?, string()?, string()?);
    Some(Descriptor {
        control_session_id: get_i64(image, block + CONTROL_SESSION_ID_OFFSET),
        correlation_id: get_i64(image, block + CORRELATION_ID_OFFSET),
        recording_id: get_i64(image, block + RECORDING_ID_OFFSET),
        start_timestamp: get_i64(image, block + START_TIMESTAMP_OFFSET),
        stop_timestamp: get_i64(image, block + STOP_TIMESTAMP_OFFSET),
        start_position: get_i64(image, block + START_POSITION_OFFSET),
        stop_position: get_i64(image, block + STOP_POSITION_OFFSET),
        initial_term_id: get_i32(image, block + INITIAL_TERM_ID_OFFSET),
        segment_file_length: get_i32(image, block + SEGMENT_FILE_LENGTH_OFFSET),
        term_buffer_length: get_i32(image, block + TERM_BUFFER_LENGTH_OFFSET),
        mtu_length: get_i32(image, block + MTU_LENGTH_OFFSET),
        session_id: get_i32(image, block + SESSION_ID_OFFSET),
        stream_id: get_i32(image, block + STREAM_ID_OFFSET),
        stripped_channel,
        original_channel,
        source_identity,
    })
}
