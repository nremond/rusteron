//! The service container's mark file on disk, written with positional
//! writes: Java's readers map the file and read the same page cache, so no
//! `unsafe` mapping is needed here. Fields are written in Java's order: the
//! header, the activity timestamp, then the version that says it is ready.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use rusteron_cluster_core::markfile::{
    self, ACTIVITY_TIMESTAMP_OFFSET, ContainerHeader, HEADER_LENGTH, LIVENESS_TIMEOUT_MS, MEMBER_ID_OFFSET,
    SEMANTIC_VERSION, VERSION_OFFSET, error_log,
};

#[cfg(unix)]
fn write_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, bytes, offset)
}

#[cfg(unix)]
fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, bytes, offset)
}

#[cfg(windows)]
fn write_at(file: &File, mut bytes: &[u8], mut offset: u64) -> io::Result<()> {
    while !bytes.is_empty() {
        let n = std::os::windows::fs::FileExt::seek_write(file, bytes, offset)?;
        bytes = &bytes[n..];
        offset += n as u64;
    }
    Ok(())
}

#[cfg(windows)]
fn read_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !bytes.is_empty() {
        let n = std::os::windows::fs::FileExt::seek_read(file, bytes, offset)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        bytes = &mut bytes[n..];
        offset += n as u64;
    }
    Ok(())
}

/// A container's mark file, open for as long as the container runs.
pub struct MarkFile {
    file: File,
    path: PathBuf,
    error_capacity: usize,
    next_error: usize,
    /// Errors recorded, by text: where their record is, and how often seen.
    errors: HashMap<String, (usize, i32)>,
}

impl MarkFile {
    /// Creates the mark file of service `header.service_id` in `dir`,
    /// refusing to if a container is still alive there: one whose mark file
    /// is ready and whose activity is younger than the liveness timeout.
    pub fn create(dir: &Path, header: &ContainerHeader, file_page_size: usize, now_ms: i64) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(markfile::service_file_name(header.service_id));
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
        let bytes = header
            .encode()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let error_buffer_length = header.error_buffer_length as usize;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(markfile::file_length(error_buffer_length, file_page_size) as u64)?;
        write_at(&file, &bytes, 0)?;
        write_at(&file, &now_ms.to_le_bytes(), ACTIVITY_TIMESTAMP_OFFSET as u64)?;
        write_at(&file, &SEMANTIC_VERSION.to_le_bytes(), VERSION_OFFSET as u64)?;
        file.sync_all()?;
        let link = dir.join(markfile::service_link_file_name(header.service_id));
        if link.exists() {
            std::fs::remove_file(link)?;
        }
        Ok(Self {
            file,
            path,
            error_capacity: error_buffer_length,
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

    pub fn member_id(&self, member_id: i32) -> io::Result<()> {
        write_at(&self.file, &member_id.to_le_bytes(), MEMBER_ID_OFFSET as u64)
    }

    /// Records an error in the distinct error log: a repeat counts, a new one
    /// takes a record, written before its length so a reader never sees half
    /// of it. Returns false when the log is full.
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

    /// The container stopped: its activity is no more, for whoever starts next.
    pub fn signal_terminated(&self) -> io::Result<()> {
        write_at(&self.file, &(-1i64).to_le_bytes(), ACTIVITY_TIMESTAMP_OFFSET as u64)?;
        self.file.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> ContainerHeader {
        ContainerHeader {
            start_timestamp_ms: 1_000,
            pid: 1,
            archive_stream_id: 10,
            service_stream_id: 104,
            consensus_module_stream_id: 105,
            service_id: 0,
            cluster_id: 0,
            error_buffer_length: markfile::ERROR_BUFFER_MIN_LENGTH as i32,
            aeron_directory: "/tmp/aeron".into(),
            control_channel: "aeron:ipc?term-length=128k".into(),
            service_name: "test".into(),
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rusteron-cluster-markfile-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn a_live_container_is_not_started_over_and_a_stopped_one_is() {
        let dir = temp_dir("live");
        let mark = MarkFile::create(&dir, &header(), 4_096, 100_000).unwrap();
        assert_eq!(std::fs::metadata(mark.path()).unwrap().len(), 8_192 + 1_048_576);
        let err = MarkFile::create(&dir, &header(), 4_096, 100_500)
            .err()
            .expect("refused");
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        mark.signal_terminated().unwrap();
        MarkFile::create(&dir, &header(), 4_096, 100_600).expect("a stopped container is replaced");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn an_error_seen_again_is_counted_in_its_record() {
        let dir = temp_dir("errors");
        let mut mark = MarkFile::create(&dir, &header(), 4_096, 1).unwrap();
        assert!(mark.record_error("boom", 5).unwrap());
        assert!(mark.record_error("boom", 6).unwrap());
        assert!(mark.record_error("bang", 7).unwrap());
        let bytes = std::fs::read(mark.path()).unwrap();
        let record = &bytes[HEADER_LENGTH..];
        assert_eq!(i32::from_le_bytes(record[0..4].try_into().unwrap()), 28);
        assert_eq!(i32::from_le_bytes(record[4..8].try_into().unwrap()), 2);
        assert_eq!(i64::from_le_bytes(record[8..16].try_into().unwrap()), 6);
        assert_eq!(&record[24..28], b"boom");
        assert_eq!(&record[32 + 24..32 + 28], b"bang");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
