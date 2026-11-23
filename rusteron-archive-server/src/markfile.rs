//! The archive's mark file, `archive-mark.dat`, as Aeron Archive 1.53.3
//! writes it (`ArchiveMarkFile`): an SBE message header and the archive's
//! header (schema 100 version 2) in the first 8 KiB, the distinct error log
//! after them. Written with positional writes; Java's tools map the file and
//! read the same page cache. The version, written last, says the archive is
//! ready; the activity timestamp, every second, that it is alive.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use rusteron_archive_codecs::mark::{
    WriteBuf, mark_file_header_codec, mark_file_header_codec::MarkFileHeaderEncoder, message_header_codec,
};
use rusteron_archive_core::catalog::SEMANTIC_VERSION;
use rusteron_cluster_core::markfile::error_log;

pub const FILE_NAME: &str = "archive-mark.dat";
pub const LINK_FILE_NAME: &str = "archive-mark.lnk";
/// The header's room; the error log follows it.
pub const HEADER_LENGTH: usize = 8 * 1024;
const HEADER_OFFSET: usize = message_header_codec::ENCODED_LENGTH;
const VERSION_OFFSET: usize = HEADER_OFFSET;
const ACTIVITY_TIMESTAMP_OFFSET: usize = HEADER_OFFSET + 8;
/// An archive whose activity is younger than this is alive.
pub const LIVENESS_TIMEOUT_MS: i64 = 10_000;
/// How often the activity timestamp moves.
pub const UPDATE_INTERVAL_MS: i64 = 1_000;

#[cfg(unix)]
fn write_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, bytes, offset)
}

#[cfg(unix)]
fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, bytes, offset)
}

/// What the header says of the archive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveHeader {
    pub start_timestamp_ms: i64,
    pub pid: i64,
    pub control_stream_id: i32,
    pub local_control_stream_id: i32,
    pub events_stream_id: i32,
    pub error_buffer_length: i32,
    pub archive_id: i64,
    pub control_channel: String,
    pub local_control_channel: String,
    pub events_channel: String,
    pub aeron_directory: String,
}

impl ArchiveHeader {
    /// The header's bytes, its version and activity still 0: written last.
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let strings = [
            &self.control_channel,
            &self.local_control_channel,
            &self.events_channel,
            &self.aeron_directory,
        ];
        let length = HEADER_OFFSET
            + mark_file_header_codec::SBE_BLOCK_LENGTH as usize
            + strings.iter().map(|s| 4 + s.len()).sum::<usize>();
        if length > HEADER_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("ArchiveMarkFile headerLength={length} > headerLengthCapacity={HEADER_LENGTH}"),
            ));
        }
        let mut bytes = vec![0; length];
        let mut e = MarkFileHeaderEncoder::default()
            .wrap(WriteBuf::new(&mut bytes), HEADER_OFFSET)
            .header(0)
            .parent()
            .map_err(|e| io::Error::other(format!("{e:?}")))?;
        e.version(0)
            .activity_timestamp(0)
            .start_timestamp(self.start_timestamp_ms)
            .pid(self.pid)
            .control_stream_id(self.control_stream_id)
            .local_control_stream_id(self.local_control_stream_id)
            .events_stream_id(self.events_stream_id)
            .header_length(HEADER_LENGTH as i32)
            .error_buffer_length(self.error_buffer_length)
            .archive_id(self.archive_id)
            .control_channel(self.control_channel.as_bytes())
            .local_control_channel(self.local_control_channel.as_bytes())
            .events_channel(self.events_channel.as_bytes())
            .aeron_directory(self.aeron_directory.as_bytes());
        Ok(bytes)
    }
}

/// The file's length: the header's room and the error log, to the page.
pub fn file_length(error_buffer_length: usize, file_page_size: usize) -> usize {
    (HEADER_LENGTH + error_buffer_length).div_ceil(file_page_size) * file_page_size
}

/// An archive's mark file, open for as long as the archive runs.
pub struct ArchiveMarkFile {
    file: File,
    path: PathBuf,
    error_capacity: usize,
    next_error: usize,
    errors: HashMap<String, (usize, i32)>,
}

impl ArchiveMarkFile {
    /// Creates the mark file in `dir`, refusing to if an archive is alive
    /// there: one ready, whose activity is younger than the liveness timeout.
    pub fn create(dir: &Path, header: &ArchiveHeader, file_page_size: usize, now_ms: i64) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(FILE_NAME);
        if let Ok(existing) = File::open(&path) {
            let mut version = [0; 4];
            let mut activity = [0; 8];
            if read_at(&existing, &mut version, VERSION_OFFSET as u64).is_ok()
                && read_at(&existing, &mut activity, ACTIVITY_TIMESTAMP_OFFSET as u64).is_ok()
            {
                let version = i32::from_le_bytes(version);
                let activity = i64::from_le_bytes(activity);
                if version > 0 && activity > 0 && now_ms - activity < LIVENESS_TIMEOUT_MS {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!(
                            "active mark file detected: {} ({}ms ago)",
                            path.display(),
                            now_ms - activity
                        ),
                    ));
                }
            }
        }
        let bytes = header.encode()?;
        let error_capacity = header.error_buffer_length as usize;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(file_length(error_capacity, file_page_size) as u64)?;
        write_at(&file, &bytes, 0)?;
        write_at(&file, &now_ms.to_le_bytes(), ACTIVITY_TIMESTAMP_OFFSET as u64)?;
        write_at(&file, &SEMANTIC_VERSION.to_le_bytes(), VERSION_OFFSET as u64)?;
        file.sync_all()?;
        let link = dir.join(LINK_FILE_NAME);
        if link.exists() {
            std::fs::remove_file(link)?;
        }
        Ok(Self {
            file,
            path,
            error_capacity,
            next_error: 0,
            errors: HashMap::new(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn update_activity(&self, now_ms: i64) -> io::Result<()> {
        write_at(&self.file, &now_ms.to_le_bytes(), ACTIVITY_TIMESTAMP_OFFSET as u64)
    }

    /// Records an error in the distinct error log; false when the log is full.
    pub fn record_error(&mut self, text: &str, now_ms: i64) -> io::Result<bool> {
        if let Some((offset, count)) = self.errors.get_mut(text) {
            *count += 1;
            let at = (HEADER_LENGTH + *offset) as u64;
            write_at(
                &self.file,
                &now_ms.to_le_bytes(),
                at + error_log::LAST_OBSERVATION_TIMESTAMP_OFFSET as u64,
            )?;
            write_at(
                &self.file,
                &count.to_le_bytes(),
                at + error_log::OBSERVATION_COUNT_OFFSET as u64,
            )?;
            return Ok(true);
        }
        let record = error_log::new_record(text, now_ms);
        let aligned = error_log::aligned_length(&record);
        if self.next_error + aligned > self.error_capacity {
            return Ok(false);
        }
        let at = (HEADER_LENGTH + self.next_error) as u64;
        write_at(&self.file, &record, at)?;
        write_at(
            &self.file,
            &error_log::record_length(&record).to_le_bytes(),
            at + error_log::LENGTH_OFFSET as u64,
        )?;
        self.errors.insert(text.to_string(), (self.next_error, 1));
        self.next_error += aligned;
        Ok(true)
    }

    /// The archive stopped: no longer alive, for whoever starts next.
    pub fn signal_terminated(&self) -> io::Result<()> {
        write_at(&self.file, &(-1i64).to_le_bytes(), ACTIVITY_TIMESTAMP_OFFSET as u64)?;
        self.file.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusteron_archive_codecs::mark::{
        ReadBuf, mark_file_header_codec::MarkFileHeaderDecoder, message_header_codec::MessageHeaderDecoder,
    };

    fn header() -> ArchiveHeader {
        ArchiveHeader {
            start_timestamp_ms: 1_700_000_000_000,
            pid: 4_242,
            control_stream_id: 10,
            local_control_stream_id: 10,
            events_stream_id: 30,
            error_buffer_length: 1024 * 1024,
            archive_id: 42,
            control_channel: "aeron:udp?endpoint=localhost:8010".into(),
            local_control_channel: "aeron:ipc?term-length=64k".into(),
            events_channel: String::new(),
            aeron_directory: "/dev/shm/aeron".into(),
        }
    }

    #[test]
    fn the_header_decodes_as_java_writes_it() {
        let bytes = header().encode().unwrap();
        let mut d =
            MarkFileHeaderDecoder::default().header(MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0), 0);
        assert_eq!(d.version(), 0, "the version comes last");
        assert_eq!(d.header_length(), Some(8_192));
        assert_eq!(d.archive_id(), Some(42));
        assert_eq!(d.local_control_stream_id(), 10);
        let c = d.control_channel_decoder();
        assert_eq!(d.control_channel_slice(c), b"aeron:udp?endpoint=localhost:8010");
        let c = d.local_control_channel_decoder();
        assert_eq!(d.local_control_channel_slice(c), b"aeron:ipc?term-length=64k");
        let c = d.events_channel_decoder();
        assert_eq!(d.events_channel_slice(c), b"");
        let c = d.aeron_directory_decoder();
        assert_eq!(d.aeron_directory_slice(c), b"/dev/shm/aeron");
    }

    #[test]
    fn a_live_archive_is_not_started_over_and_a_stopped_one_is() {
        let dir = std::env::temp_dir().join(format!("rusteron-archive-markfile-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mark = ArchiveMarkFile::create(&dir, &header(), 4_096, 100_000).unwrap();
        assert_eq!(std::fs::metadata(mark.path()).unwrap().len(), 8_192 + 1_048_576);
        let bytes = std::fs::read(mark.path()).unwrap();
        assert_eq!(i32::from_le_bytes(bytes[8..12].try_into().unwrap()), 0x03_01_00);
        assert_eq!(i64::from_le_bytes(bytes[16..24].try_into().unwrap()), 100_000);
        let err = ArchiveMarkFile::create(&dir, &header(), 4_096, 100_500)
            .err()
            .expect("refused");
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        mark.signal_terminated().unwrap();
        ArchiveMarkFile::create(&dir, &header(), 4_096, 100_600).expect("a stopped archive is replaced");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
