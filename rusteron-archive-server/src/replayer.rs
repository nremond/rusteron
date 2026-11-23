//! A replay's I/O, as Java's `ReplaySession` does it: the segment files
//! read, the blocks offered on the replay's exclusive publication, the
//! limit counter read; where to read and what to offer is the core's
//! [`ReplaySession`].

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use rusteron_archive::{AeronCountersReader, AeronExclusivePublication};
use rusteron_archive_core::protocol::error_code;
use rusteron_archive_core::replay::{Limit, ReplayError, ReplaySession, ReplayState};

use crate::counters::load;

/// Aeron's state of a counter in use.
const RECORD_ALLOCATED: i32 = 1;
/// What an offer returns on a refused block.
const PUBLICATION_ERROR: i64 = -6;

#[cfg(unix)]
fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, bytes, offset)
}

/// Reads as much as there is, up to `bytes`' length: what Java's loop of
/// positional reads gets.
fn read_fully(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
    let mut read = 0;
    while read < bytes.len() {
        let n = read_at(file, &mut bytes[read..], offset + read as u64)?;
        if n == 0 {
            break;
        }
        read += n;
    }
    Ok(read)
}

/// The counter a replay follows once it reaches its stop.
pub enum LimitCounter {
    None,
    /// By id, and its registration: a reused id is another counter.
    Counter {
        id: i32,
        registration_id: i64,
    },
    /// The recording ended before the replay started: its final position.
    Ended(i64),
}

/// The bytes and time a cycle's reads took, for the archive's counters.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadStats {
    pub bytes: i64,
    pub time_ns: i64,
    pub max_time_ns: i64,
}

/// What a replay's cycle came to.
pub enum Replayed {
    Idle,
    Worked,
    /// The client is to be told OK; the replay goes on.
    Started,
    /// Done; its publication to be revoked (no linger) or closed. A replay
    /// that started and ended in the same cycle still has its client told OK
    /// first, as Java's tells it as it starts.
    Ended {
        error: Option<ReplayError>,
        revoke: bool,
        started: bool,
    },
}

pub struct Replayer {
    pub replay_session_id: i64,
    pub recording_id: i64,
    publication: AeronExclusivePublication,
    session: ReplaySession,
    file: Option<File>,
    dir: PathBuf,
    limit: LimitCounter,
    channel: String,
    session_id: i32,
    stream_id: i32,
}

fn io_error(session: &mut ReplaySession, e: &io::Error) -> ReplayError {
    session.fail(error_code::GENERIC, &format!("java.io.IOException: {e}"))
}

impl Replayer {
    pub fn new(
        publication: AeronExclusivePublication,
        session: ReplaySession,
        limit: LimitCounter,
        dir: &Path,
    ) -> Result<Self, String> {
        let constants = publication.get_constants().map_err(|e| e.to_string())?;
        Ok(Replayer {
            replay_session_id: session.params().replay_session_id,
            recording_id: session.params().recording_id,
            channel: constants.channel().to_owned(),
            session_id: constants.session_id(),
            stream_id: constants.stream_id(),
            publication,
            session,
            file: None,
            dir: dir.to_path_buf(),
            limit,
        })
    }

    /// Stopped as its recording goes: the publication revoked.
    pub fn abort(&mut self) {
        self.session.abort();
    }

    /// Stopped by its client on an `eos=false` channel: the publication
    /// closed, not revoked ([`ReplaySession::stop`]).
    pub fn stop(&mut self) {
        self.session.stop();
    }

    /// Closes the publication: revoked when the replay did not complete, so
    /// no linger; closed, to linger and send its end, when it did.
    pub fn close(self, revoke: bool) {
        if revoke {
            self.publication.revoke_on_close();
        }
        let _ = self.publication.close();
    }

    fn ended(&self, error: Option<ReplayError>) -> Replayed {
        let revoke = !matches!(self.session.state(), ReplayState::Inactive { revoke: false });
        Replayed::Ended {
            error,
            revoke,
            started: false,
        }
    }

    fn limit(&self, counters: &AeronCountersReader) -> Limit {
        match self.limit {
            LimitCounter::None => Limit {
                value: self.session.params().stop_position,
                closed: true,
                same_registration: false,
            },
            LimitCounter::Counter { id, registration_id } => {
                let same_registration = counters.counter_registration_id(id).ok() == Some(registration_id);
                let state = counters.counter_state(id);
                let allocated = state.as_ref().ok() == Some(&RECORD_ALLOCATED);
                Limit {
                    // An id the reader does not know is not read.
                    value: if state.is_ok() {
                        load(counters, id)
                    } else {
                        self.session.params().stop_position
                    },
                    closed: !(allocated && same_registration),
                    same_registration,
                }
            }
            LimitCounter::Ended(value) => Limit {
                value,
                closed: true,
                same_registration: true,
            },
        }
    }

    /// One cycle of the replay: at most one read, one block and one padding.
    pub fn poll(
        &mut self,
        now_ms: i64,
        counters: &AeronCountersReader,
        buffer: &mut [u8],
        stats: &mut ReadStats,
    ) -> Replayed {
        if matches!(self.session.state(), ReplayState::Inactive { .. }) {
            return self.ended(None);
        }
        let mut started = false;
        if self.session.state() == ReplayState::Init {
            if self.file.is_none() {
                let path = self.dir.join(self.session.segment_file_name());
                if !path.exists() {
                    return match self.session.on_file_missing(now_ms) {
                        Some(error) => self.ended(Some(error)),
                        None => Replayed::Idle,
                    };
                }
                let file = match File::open(&path) {
                    Ok(file) => file,
                    Err(e) => {
                        let error = io_error(&mut self.session, &e);
                        return self.ended(Some(error));
                    }
                };
                if let Some(offset) = self.session.on_file_opened() {
                    let mut header = [0u8; 32];
                    let n = match read_fully(&file, &mut header, offset) {
                        Ok(n) => n,
                        Err(e) => {
                            let error = io_error(&mut self.session, &e);
                            return self.ended(Some(error));
                        }
                    };
                    if let Err(error) = self.session.check_header(&header[..n]) {
                        return self.ended(Some(error));
                    }
                }
                self.file = Some(file);
                started = true;
            }
            if !self.publication.is_connected() {
                if let Some(error) = self.session.on_not_connected(now_ms, &self.channel, self.stream_id) {
                    return self.ended(Some(error));
                }
                return if started { Replayed::Started } else { Replayed::Idle };
            }
            self.session.on_connected();
        }
        let worked = self.replay(counters, buffer, stats);
        match worked {
            Err(error) => match self.ended(error) {
                Replayed::Ended { error, revoke, .. } => Replayed::Ended { error, revoke, started },
                other => other,
            },
            Ok(_) if started => Replayed::Started,
            Ok(true) => Replayed::Worked,
            Ok(false) => Replayed::Idle,
        }
    }

    /// `replay()`: whether it did anything; `Err` when it ended.
    fn replay(
        &mut self,
        counters: &AeronCountersReader,
        buffer: &mut [u8],
        stats: &mut ReadStats,
    ) -> Result<bool, Option<ReplayError>> {
        if !self.publication.is_connected() {
            self.session.on_disconnected();
            return Err(None);
        }
        if self.session.is_empty() {
            return Err(None);
        }
        if self.session.awaits_limit() {
            let limit = self.limit(counters);
            if self.session.on_limit(limit) {
                return match self.session.state() {
                    ReplayState::Inactive { .. } => Err(None),
                    _ => Ok(false),
                };
            }
        }
        if let Some(name) = self.session.next_term_if_due() {
            self.file = None;
            match File::open(self.dir.join(&name)) {
                Ok(file) => self.file = Some(file),
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(Some(self.session.on_segment_missing())),
                Err(e) => return Err(Some(io_error(&mut self.session, &e))),
            }
        }
        let window = self.publication.position_limit() - self.publication.position();
        if window <= 0 {
            return Ok(false);
        }
        let length = self.session.read_length();
        if length == 0 {
            return Ok(false);
        }
        let started = Instant::now();
        let file = self.file.as_ref().expect("the segment file is open");
        let read = &mut buffer[..length];
        match read_fully(file, read, self.session.file_offset()) {
            Ok(n) => read[n..].fill(0),
            Err(e) => return Err(Some(io_error(&mut self.session, &e))),
        }
        let batch = self
            .session
            .prepare(read, self.session_id, self.stream_id)
            .map_err(Some)?;
        let ns = started.elapsed().as_nanos() as i64;
        stats.bytes += length as i64;
        stats.time_ns += ns;
        stats.max_time_ns = stats.max_time_ns.max(ns);

        let mut padding = batch.padding;
        if batch.data > 0 {
            let position = self.publication.offer_block(&read[..batch.data]);
            if position == PUBLICATION_ERROR {
                let message = "the publication refused the block as malformed";
                return Err(Some(self.session.fail(error_code::GENERIC, message)));
            }
            if !self.session.on_offered(position, batch.data) {
                padding = 0;
            }
        }
        if padding > 0 {
            let position = self.publication.append_padding(padding - 32);
            self.session.on_offered(position, padding);
        }
        if matches!(self.session.state(), ReplayState::Inactive { .. }) {
            return Err(None);
        }
        Ok(true)
    }
}
