//! A recording's writer: an image's blocks into its segment files, as Java's
//! `RecordingWriter`, where [`SegmentWriter`] places them.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use rusteron_archive::{AeronCounter, AeronImage};
use rusteron_archive_core::conductor::RecordingError;
use rusteron_archive_core::protocol::error_code;
use rusteron_archive_core::segments::{SegmentWriter, is_padding};

use crate::counters::store;

/// What the archive's `fileSyncLevel` asks after each write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sync {
    None,
    /// The data (`fdatasync`): Java's level 1.
    Data,
    /// The data and the metadata (`fsync`): Java's level 2.
    All,
}

impl Sync {
    pub fn from_level(level: i32) -> Self {
        match level {
            ..=0 => Sync::None,
            1 => Sync::Data,
            _ => Sync::All,
        }
    }
}

/// The bytes and time a cycle's writes took, for the archive's counters.
#[derive(Clone, Copy, Debug, Default)]
pub struct WriteStats {
    pub bytes: i64,
    pub time_ns: i64,
    pub max_time_ns: i64,
}

#[cfg(unix)]
fn write_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, bytes, offset)
}

fn sync_dir(dir: &Path, sync: Sync) -> io::Result<()> {
    match sync {
        Sync::None => Ok(()),
        _ => File::open(dir)?.sync_all(),
    }
}

/// Opens a segment file at its full length, as `openRecordingSegmentFile`.
fn open_segment(path: &Path, length: i32, create_new: bool) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(!create_new)
        .create_new(create_new)
        .truncate(false)
        .open(path)?;
    file.set_len(length as u64)?;
    Ok(file)
}

/// `RecordingWriter.checkErrorType`: a full disk is a storage error.
fn io_error(e: &io::Error) -> RecordingError {
    let code = if e.kind() == io::ErrorKind::StorageFull {
        error_code::STORAGE_SPACE
    } else {
        error_code::GENERIC
    };
    RecordingError {
        code,
        message: format!("ERROR - java.io.IOException - {e}"),
    }
}

pub struct Recorder {
    pub recording_id: i64,
    pub subscription: u64,
    image: AeronImage,
    counter: AeronCounter,
    writer: SegmentWriter,
    file: File,
    dir: PathBuf,
    sync: Sync,
    block_length_limit: usize,
}

/// What a recorder's poll came to.
pub enum Polled {
    /// Bytes recorded, the position after them.
    Recorded(i64),
    Idle,
    /// The image ended, or a write failed.
    Ended(Option<RecordingError>),
}

impl Recorder {
    /// Opens the writer's segment file (`RecordingWriter.init`); the counter
    /// already shows its position.
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        recording_id: i64,
        subscription: u64,
        image: AeronImage,
        counter: AeronCounter,
        writer: SegmentWriter,
        dir: &Path,
        sync: Sync,
        block_length_limit: usize,
    ) -> Result<Self, RecordingError> {
        let path = dir.join(writer.file_name());
        let file = open_segment(&path, writer.segment_length, false)
            .and_then(|f| sync_dir(dir, sync).map(|()| f))
            .map_err(|e| RecordingError {
                code: error_code::GENERIC,
                message: format!("java.io.IOException: {}: {e}", path.display()),
            })?;
        Ok(Recorder {
            recording_id,
            subscription,
            image,
            counter,
            writer,
            file,
            dir: dir.to_path_buf(),
            sync,
            block_length_limit,
        })
    }

    pub fn position(&self) -> i64 {
        self.writer.position()
    }

    /// The position counter's id and registration id, for a replay that follows it.
    pub fn counter_constants(&self) -> Option<(i32, i64)> {
        let c = self.counter.get_constants().ok()?;
        Some((c.counter_id(), c.registration_id()))
    }

    /// One block from the image, written; the counter moves after the write
    /// (and its sync), never before.
    pub fn poll(&mut self, stats: &mut WriteStats) -> Polled {
        let Recorder {
            image,
            writer,
            file,
            dir,
            sync,
            block_length_limit,
            ..
        } = self;
        let mut failure = None;
        let polled = image.block_poll_fn(
            |block: &[u8], _session_id, _term_id| {
                let started = Instant::now();
                let mut next = *writer;
                let write = next.on_block(block.len(), is_padding(block, 0));
                let written = write_at(file, &block[..write.length], write.file_offset).and_then(|()| match sync {
                    Sync::None => Ok(()),
                    Sync::Data => file.sync_data(),
                    Sync::All => file.sync_all(),
                });
                if let Err(e) = written {
                    failure = Some(io_error(&e));
                    return;
                }
                *writer = next;
                if write.roll_over {
                    let path = dir.join(writer.file_name());
                    match open_segment(&path, writer.segment_length, true)
                        .and_then(|f| sync_dir(dir, *sync).map(|()| f))
                    {
                        Ok(f) => *file = f,
                        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                            failure = Some(RecordingError {
                                code: error_code::GENERIC,
                                message: format!("ERROR - segment file already exists: {}", path.display()),
                            });
                        }
                        Err(e) => failure = Some(io_error(&e)),
                    }
                }
                let ns = started.elapsed().as_nanos() as i64;
                stats.bytes += write.length as i64;
                stats.time_ns += ns;
                stats.max_time_ns = stats.max_time_ns.max(ns);
            },
            *block_length_limit,
        );
        if let Some(error) = failure {
            return Polled::Ended(Some(error));
        }
        match polled {
            Ok(n) if n > 0 => {
                let position = self.writer.position();
                store(&self.counter, position);
                Polled::Recorded(position)
            }
            Ok(_) if self.image.is_end_of_stream() || self.image.is_closed() => Polled::Ended(None),
            Ok(_) => Polled::Idle,
            Err(e) => Polled::Ended(Some(RecordingError {
                code: error_code::GENERIC,
                message: format!("ERROR - {e}"),
            })),
        }
    }
}
