//! The service container's mark file on disk, written with positional
//! writes: Java's readers map the file and read the same page cache, so no
//! `unsafe` mapping is needed here. Fields are written in Java's order: the
//! header, the activity timestamp, then the version that says it is ready.

use std::collections::HashMap;
use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rusteron_cluster_core::markfile::owner::{self, Header, Lock, Owner, Record};
use rusteron_cluster_core::markfile::{
    self, ACTIVITY_TIMESTAMP_OFFSET, ContainerHeader, HEADER_LENGTH, MEMBER_ID_OFFSET, SEMANTIC_VERSION,
    VERSION_OFFSET, error_log,
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
    /// Its lock file, locked while the component runs ([`owner`]).
    lock: File,
    terminated: AtomicBool,
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
        let bytes = header
            .encode()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        Self::create_named(
            dir,
            &markfile::service_file_name(header.service_id),
            &markfile::service_link_file_name(header.service_id),
            &bytes,
            header.error_buffer_length as usize,
            file_page_size,
            now_ms,
        )
    }

    /// A mark file named `file_name` with the header `bytes` (a consensus
    /// module's, say), its link file `link_name` removed: refused while
    /// another process may still run there ([`owner`]). Its lock is held
    /// until [`signal_terminated`](Self::signal_terminated), or the process
    /// ends.
    pub fn create_named(
        dir: &Path,
        file_name: &str,
        link_name: &str,
        bytes: &[u8],
        error_buffer_length: usize,
        file_page_size: usize,
        now_ms: i64,
    ) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(file_name);
        let lock = take_lock(dir, file_name, &path, bytes, now_ms)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(markfile::file_length(error_buffer_length, file_page_size) as u64)?;
        write_at(&file, bytes, 0)?;
        write_at(&file, &now_ms.to_le_bytes(), ACTIVITY_TIMESTAMP_OFFSET as u64)?;
        write_at(&file, &SEMANTIC_VERSION.to_le_bytes(), VERSION_OFFSET as u64)?;
        file.sync_all()?;
        let link = dir.join(link_name);
        if link.exists() {
            std::fs::remove_file(link)?;
        }
        Ok(Self {
            file,
            lock,
            terminated: AtomicBool::new(false),
            path,
            error_capacity: error_buffer_length,
            next_error: 0,
            errors: HashMap::new(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The activity timestamp refreshed, unless the component has signalled
    /// that it stopped: a successor may be starting over it.
    pub fn update_activity(&self, now_ms: i64) -> io::Result<()> {
        if self.terminated.load(Ordering::Acquire) {
            return Ok(());
        }
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

    /// The component stopped: its activity is no more and its lock let go,
    /// for whoever starts next.
    pub fn signal_terminated(&self) -> io::Result<()> {
        self.terminated.store(true, Ordering::Release);
        write_at(&self.file, &(-1i64).to_le_bytes(), ACTIVITY_TIMESTAMP_OFFSET as u64)?;
        self.file.sync_all()?;
        self.lock.unlock()
    }
}

/// How long a start tries for its lock while a tool looks at it
/// ([`probe_lock`] holds it a moment).
const LOCK_PATIENCE: Duration = Duration::from_millis(250);

/// The lock of mark file `file_name` in `dir`, taken for a component about
/// to write the mark file at `path` with header `bytes` ([`owner`]). Refused
/// if a live process holds it, or if the mark file's writer may still run by
/// its activity timestamp and nothing proves it gone. Taken, it records this
/// writer.
fn take_lock(dir: &Path, file_name: &str, path: &Path, bytes: &[u8], now_ms: i64) -> io::Result<File> {
    let lock_name = owner::lock_file_name(file_name);
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join(&lock_name))?;
    let deadline = Instant::now() + LOCK_PATIENCE;
    loop {
        match lock.try_lock() {
            Ok(()) => break,
            Err(TryLockError::WouldBlock) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(5)),
            Err(TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "active mark file detected: {} (a live process holds {lock_name})",
                        path.display()
                    ),
                ));
            }
            Err(TryLockError::Error(e)) => return Err(e),
        }
    }
    let found = Lock::Free(read_record(&lock));
    if let Owner::Active { activity_timestamp_ms } = owner::owner(read_header(path).as_ref(), &found, now_ms) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "active mark file detected: {} ({}ms ago)",
                path.display(),
                now_ms - activity_timestamp_ms
            ),
        ));
    }
    let writer = Header::read(bytes)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "the mark file's header is too short"))?
        .record();
    lock.set_len(0)?;
    write_at(&lock, writer.encode().as_bytes(), 0)?;
    lock.sync_data()?;
    Ok(lock)
}

/// What the lock of mark file `file_name` in `dir` shows, for a tool that
/// only looks ([`owner`]): held by a live process, or free and the writer it
/// records, or absent. A free lock is taken and let go at once; a start that
/// meets it held meanwhile tries again ([`LOCK_PATIENCE`]).
pub fn probe_lock(dir: &Path, file_name: &str) -> Lock {
    let Ok(lock) = File::open(dir.join(owner::lock_file_name(file_name))) else {
        return Lock::Absent;
    };
    match lock.try_lock() {
        Ok(()) => {
            let record = read_record(&lock);
            let _ = lock.unlock();
            Lock::Free(record)
        }
        Err(TryLockError::WouldBlock) => Lock::Held,
        Err(TryLockError::Error(_)) => Lock::Absent,
    }
}

/// The writer a lock file records, if it records one.
fn read_record(lock: &File) -> Option<Record> {
    let length = usize::try_from(lock.metadata().ok()?.len()).ok()?.min(64);
    let mut bytes = vec![0; length];
    read_at(lock, &mut bytes, 0).ok()?;
    Record::parse(std::str::from_utf8(&bytes).ok()?)
}

/// The header of the mark file at `path`, if it is there and long enough.
pub fn read_header(path: &Path) -> Option<Header> {
    let file = File::open(path).ok()?;
    let mut bytes = [0; owner::HEADER_READ_LENGTH];
    read_at(&file, &mut bytes, 0).ok()?;
    Header::read(&bytes)
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

    const LOCK_NAME: &str = "cluster-mark-service-0.lck";

    #[test]
    fn a_killed_container_is_replaced_at_once() {
        let dir = temp_dir("killed");
        let mark = MarkFile::create(&dir, &header(), 4_096, 100_000).unwrap();
        // A kill: the lock goes with the process, the activity stays fresh.
        drop(mark);
        MarkFile::create(&dir, &header(), 4_096, 100_500).expect("its lock proves the writer gone");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_paused_container_is_not_started_over_however_stale_its_activity() {
        let dir = temp_dir("paused");
        let _mark = MarkFile::create(&dir, &header(), 4_096, 100_000).unwrap();
        let err = MarkFile::create(&dir, &header(), 4_096, 100_000 + 3 * markfile::LIVENESS_TIMEOUT_MS)
            .err()
            .expect("refused");
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert!(err.to_string().contains("a live process holds"), "{err}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn without_a_lock_naming_its_writer_a_mark_file_is_judged_by_its_activity() {
        for lock in [None, Some("2 1000\n"), Some("not a record")] {
            let dir = temp_dir("unlocked");
            drop(MarkFile::create(&dir, &header(), 4_096, 100_000).unwrap());
            // As a Java component, or an earlier Rust one, leaves it: no lock file, or another writer's.
            match lock {
                None => std::fs::remove_file(dir.join(LOCK_NAME)).unwrap(),
                Some(text) => std::fs::write(dir.join(LOCK_NAME), text).unwrap(),
            }
            let err = MarkFile::create(&dir, &header(), 4_096, 100_500)
                .err()
                .expect("refused");
            assert!(err.to_string().contains("(500ms ago)"), "{lock:?}: {err}");
            MarkFile::create(&dir, &header(), 4_096, 100_000 + markfile::LIVENESS_TIMEOUT_MS)
                .expect("past the liveness timeout");
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn a_probe_sees_the_lock_held_free_or_absent() {
        let dir = temp_dir("probe");
        let name = markfile::service_file_name(0);
        assert_eq!(probe_lock(&dir, &name), Lock::Absent);
        let mark = MarkFile::create(&dir, &header(), 4_096, 100_000).unwrap();
        assert_eq!(probe_lock(&dir, &name), Lock::Held);
        drop(mark);
        let record = Record {
            pid: 1,
            start_timestamp_ms: 1_000,
        };
        assert_eq!(probe_lock(&dir, &name), Lock::Free(Some(record)));
        assert_eq!(read_header(&dir.join(&name)).map(|h| h.record()), Some(record));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_stopped_container_refreshes_its_activity_no_more() {
        let dir = temp_dir("stopped");
        let mark = MarkFile::create(&dir, &header(), 4_096, 100_000).unwrap();
        mark.signal_terminated().unwrap();
        mark.update_activity(100_100).unwrap();
        assert_eq!(read_header(mark.path()).unwrap().activity_timestamp_ms, -1);
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
