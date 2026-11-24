//! The header fields every Aeron mark file keeps at the same offsets, after
//! its SBE message header: a cluster component's (`ClusterMarkFile`) and an
//! archive's (`ArchiveMarkFile`), both on Agrona's `MarkFile`. And whether a
//! mark file's writer may still run ([`owner`]).

#![forbid(unsafe_code)]
// The crate allows every clippy lint for its generated bindings; this module is checked.
#![warn(clippy::all)]

/// The SBE message header's length: where `MarkFileHeader` starts.
pub const HEADER_OFFSET: usize = 8;
/// Offset of the version in the file.
pub const VERSION_OFFSET: usize = HEADER_OFFSET;
/// Offset of the activity timestamp in the file.
pub const ACTIVITY_TIMESTAMP_OFFSET: usize = HEADER_OFFSET + 8;
/// Offset of the start timestamp in the file.
pub const START_TIMESTAMP_OFFSET: usize = HEADER_OFFSET + 16;
/// Offset of the writer's pid in the file.
pub const PID_OFFSET: usize = HEADER_OFFSET + 24;
/// How often a running component refreshes its activity timestamp.
pub const UPDATE_INTERVAL_MS: i64 = 1_000;
/// How old an activity timestamp must be before the component counts as gone.
pub const LIVENESS_TIMEOUT_MS: i64 = 10 * UPDATE_INTERVAL_MS;

pub mod owner {
    //! Whether a mark file's writer may still run: proven by a lock where one
    //! is kept, rather than waited out.
    //!
    //! A mark file's activity timestamp says its writer is alive while it is
    //! younger than [`LIVENESS_TIMEOUT_MS`]. That
    //! rule makes a process killed moments ago block the next start for up to
    //! that long. It also takes a process paused for longer for gone, though it
    //! may resume beside its successor.
    //!
    //! So a Rust component also holds an exclusive lock (`flock`) on a lock file
    //! beside its mark file, named by [`lock_file_name`]. It takes the lock
    //! before it writes the mark file, and lets go when it stops. In the lock
    //! file it writes its [`Record`]: its pid and start timestamp, as its mark
    //! file's header has them. The kernel releases the lock when the process
    //! ends, however it ends, and keeps it while the process is paused.
    //!
    //! [`owner`] then judges, for a component about to start over a mark file,
    //! or a tool waiting for one:
    //! - the lock held: alive, whatever the timestamp says;
    //! - the lock free, and its record naming the mark file's writer: gone, at
    //!   once;
    //! - otherwise, by the activity timestamp, as before. That covers Java's
    //!   components and earlier Rust ones, which take no lock, and a record
    //!   naming another writer.
    //!
    //! The lock proves this on one machine. A file system shared between
    //! machines may not carry it.
    //!
    //! Both mark file formats, a cluster component's and an archive's
    //! (`MarkFileHeader` in either schema), keep the fields read here at the same
    //! offsets.

    use super::{ACTIVITY_TIMESTAMP_OFFSET, LIVENESS_TIMEOUT_MS, PID_OFFSET, START_TIMESTAMP_OFFSET, VERSION_OFFSET};

    /// How many of a mark file's first bytes [`Header::read`] needs.
    pub const HEADER_READ_LENGTH: usize = PID_OFFSET + 8;

    /// The lock file of mark file `mark_file_name`: its name with `.lck` for
    /// `.dat` (`cluster-mark.lck`, `archive-mark.lck`).
    pub fn lock_file_name(mark_file_name: &str) -> String {
        let stem = mark_file_name.strip_suffix(".dat").unwrap_or(mark_file_name);
        format!("{stem}.lck")
    }

    /// What a lock file holds: its holder's pid and start timestamp.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Record {
        pub pid: i64,
        pub start_timestamp_ms: i64,
    }

    impl Record {
        /// As the lock file holds it: `pid start_timestamp_ms`, then a newline.
        pub fn encode(&self) -> String {
            format!("{} {}\n", self.pid, self.start_timestamp_ms)
        }

        /// From a lock file's text; none unless it is a record and nothing else.
        pub fn parse(text: &str) -> Option<Self> {
            let mut words = text.split_whitespace();
            let pid = words.next()?.parse().ok()?;
            let start_timestamp_ms = words.next()?.parse().ok()?;
            words.next().is_none().then_some(Self {
                pid,
                start_timestamp_ms,
            })
        }
    }

    /// What a mark file's header says of its writer.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Header {
        pub version: i32,
        pub activity_timestamp_ms: i64,
        pub start_timestamp_ms: i64,
        pub pid: i64,
    }

    impl Header {
        /// From a mark file's first [`HEADER_READ_LENGTH`] bytes or more;
        /// none from fewer.
        pub fn read(bytes: &[u8]) -> Option<Self> {
            let i64_at = |at: usize| Some(i64::from_le_bytes(bytes.get(at..at + 8)?.try_into().ok()?));
            Some(Self {
                version: i32::from_le_bytes(bytes.get(VERSION_OFFSET..VERSION_OFFSET + 4)?.try_into().ok()?),
                activity_timestamp_ms: i64_at(ACTIVITY_TIMESTAMP_OFFSET)?,
                start_timestamp_ms: i64_at(START_TIMESTAMP_OFFSET)?,
                pid: i64_at(PID_OFFSET)?,
            })
        }

        /// Its writer, as a lock file records it.
        pub fn record(&self) -> Record {
            Record {
                pid: self.pid,
                start_timestamp_ms: self.start_timestamp_ms,
            }
        }

        /// Alive by the activity timestamp: ready (a version set; a stopped Rust
        /// component zeroes it, one that failed to start writes -1), and active
        /// (an orderly stop writes -1) within the liveness timeout of `now_ms`.
        pub fn active(&self, now_ms: i64) -> bool {
            self.version > 0
                && self.activity_timestamp_ms > 0
                && now_ms - self.activity_timestamp_ms < LIVENESS_TIMEOUT_MS
        }
    }

    /// What a look at a mark file's lock file found.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Lock {
        /// A live process holds it.
        Held,
        /// No process holds it; the record it holds, if any.
        Free(Option<Record>),
        /// There is none, or it could not be read.
        Absent,
    }

    /// What [`owner`] judges of a mark file's writer.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Owner {
        /// It holds its lock: alive.
        Locked,
        /// Nothing proves it gone, and it was active at this timestamp, within
        /// the liveness timeout.
        Active { activity_timestamp_ms: i64 },
        /// Gone, or never there.
        Gone,
    }

    /// Whether the writer of a mark file whose header is `header` (none: no
    /// file, or too short) may still run, given what its lock file showed.
    pub fn owner(header: Option<&Header>, lock: &Lock, now_ms: i64) -> Owner {
        match (lock, header) {
            (Lock::Held, _) => Owner::Locked,
            (Lock::Free(Some(record)), Some(header)) if header.record() == *record => Owner::Gone,
            (_, Some(header)) if header.active(now_ms) => Owner::Active {
                activity_timestamp_ms: header.activity_timestamp_ms,
            },
            _ => Owner::Gone,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_reads_back_and_nothing_else_does() {
        let record = owner::Record {
            pid: 77,
            start_timestamp_ms: 1_700_000_000_123,
        };
        assert_eq!(owner::Record::parse(&record.encode()), Some(record));
        for text in ["", "77", "77 x", "77 1 2", "x 1"] {
            assert_eq!(owner::Record::parse(text), None, "{text:?}");
        }
        assert_eq!(owner::lock_file_name("cluster-mark.dat"), "cluster-mark.lck");
        assert_eq!(
            owner::lock_file_name("cluster-mark-service-0.dat"),
            "cluster-mark-service-0.lck"
        );
        assert_eq!(owner::lock_file_name("archive-mark.dat"), "archive-mark.lck");
    }

    #[test]
    fn a_lock_proves_its_writer_alive_or_gone_and_without_one_the_timestamp_decides() {
        use owner::{Header, Lock, Owner, Record, owner as judge};
        let now = 1_700_000_100_000;
        let writer = Record {
            pid: 9,
            start_timestamp_ms: 1_700_000_000_000,
        };
        let fresh = Header {
            version: 3 << 8,
            activity_timestamp_ms: now - 500,
            start_timestamp_ms: writer.start_timestamp_ms,
            pid: writer.pid,
        };
        let stale = Header {
            activity_timestamp_ms: now - LIVENESS_TIMEOUT_MS,
            ..fresh
        };
        let active = Owner::Active {
            activity_timestamp_ms: now - 500,
        };
        // Held: alive, even past the timeout (paused), and before it has written its mark file.
        assert_eq!(judge(Some(&stale), &Lock::Held, now), Owner::Locked);
        assert_eq!(judge(None, &Lock::Held, now), Owner::Locked);
        // Free, its record naming the mark file's writer: gone at once (killed).
        assert_eq!(judge(Some(&fresh), &Lock::Free(Some(writer)), now), Owner::Gone);
        // Free, naming another writer, or nothing, or no lock file: the timestamp decides.
        let other = Record { pid: 10, ..writer };
        for lock in [Lock::Free(Some(other)), Lock::Free(None), Lock::Absent] {
            assert_eq!(judge(Some(&fresh), &lock, now), active, "{lock:?}");
            assert_eq!(judge(Some(&stale), &lock, now), Owner::Gone, "{lock:?}");
        }
        // Stopped in order, stopped by a Rust component, failed to start, not written yet: gone.
        for header in [
            Header {
                activity_timestamp_ms: -1,
                ..fresh
            },
            Header { version: 0, ..fresh },
            Header { version: -1, ..fresh },
            Header {
                activity_timestamp_ms: 0,
                ..fresh
            },
        ] {
            assert_eq!(judge(Some(&header), &Lock::Absent, now), Owner::Gone, "{header:?}");
        }
        assert_eq!(judge(None, &Lock::Free(Some(writer)), now), Owner::Gone);
    }
}
