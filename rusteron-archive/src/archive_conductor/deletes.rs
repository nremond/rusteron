//! Truncation and purge: Java's `truncateRecording`, `purgeRecording`,
//! `deleteSegments` and `DeleteSegmentsSession`. The catalog changes at
//! once; the files go one a duty cycle, after a truncated recording's
//! replays stop; a DELETE signal says they are gone.

use std::collections::VecDeque;

use super::{Action, Conductor, EraseError, ExpectedFrame};
use crate::catalog::{NULL_POSITION, RecordingState};
use crate::control_session_adapter::{Signal, error_code};
use crate::segments::{SEGMENT_SUFFIX, segment_file_base_position, segment_file_name};

const FRAME_ALIGNMENT: i64 = 32;
const DELETE_SUFFIX: &str = ".del";

/// A truncation or purge waiting on the shell: its files listed, or its
/// segment cut.
pub(crate) struct PendingDelete {
    control_session_id: i64,
    correlation_id: i64,
    await_replays: bool,
    /// For a cut: the position, and the later files.
    erase: Option<(i64, Vec<String>)>,
}

/// Files being deleted, one a cycle.
pub(crate) struct DeleteSession {
    control_session_id: i64,
    correlation_id: i64,
    files: VecDeque<String>,
    await_replays: bool,
    in_flight: bool,
    /// The highest base position among the files: what an extension or a
    /// replay must not reach.
    pub(crate) max_delete_position: i64,
}

/// A segment file's base position, `.del` or not.
fn base_position(recording_id: i64, file_name: &str) -> Option<i64> {
    let rest = file_name.strip_prefix(&format!("{recording_id}-"))?;
    rest[..rest.find('.')?].parse().ok()
}

impl Conductor {
    /// Whether a delete under way reaches `stop_position`.
    pub(super) fn delete_outstanding(&self, recording_id: i64, stop_position: i64) -> bool {
        self.deletes
            .get(&recording_id)
            .is_some_and(|d| d.max_delete_position >= stop_position)
    }

    fn delete_allowed(&mut self, control_session_id: i64, correlation_id: i64, recording_id: i64) -> bool {
        if self.deletes.contains_key(&recording_id) || self.pending_deletes.contains_key(&recording_id) {
            let message = format!("another delete operation in progress for recording id: {recording_id}");
            self.fail(control_session_id, correlation_id, error_code::GENERIC, &message);
            return false;
        }
        true
    }

    pub(super) fn truncate_recording(&mut self, cs: i64, corr: i64, recording_id: i64, position: i64) {
        let Ok(d) = self.catalog.descriptor(recording_id) else {
            return self.fail(
                cs,
                corr,
                error_code::UNKNOWN_RECORDING,
                &super::unknown_recording(recording_id),
            );
        };
        if d.stop_position == NULL_POSITION {
            return self.fail(
                cs,
                corr,
                error_code::ACTIVE_RECORDING,
                "cannot truncate active recording",
            );
        }
        if position < d.start_position || position > d.stop_position || position & (FRAME_ALIGNMENT - 1) != 0 {
            let message = format!(
                "invalid position {position}: start={} stop={} alignment={FRAME_ALIGNMENT}",
                d.start_position, d.stop_position
            );
            return self.fail(cs, corr, error_code::INVALID_POSITION, &message);
        }
        if !self.delete_allowed(cs, corr, recording_id) {
            return;
        }
        let segment_length = d.segment_file_length;
        let segment_base = segment_file_base_position(d.start_position, position, d.term_buffer_length, segment_length);
        let segment_offset = position - segment_base;
        if let Err(e) = self.catalog.set_stop_position(recording_id, position) {
            self.error(format!("ERROR - {e}"));
        }
        if position == d.start_position {
            self.pending_deletes.insert(
                recording_id,
                PendingDelete {
                    control_session_id: cs,
                    correlation_id: corr,
                    await_replays: true,
                    erase: None,
                },
            );
            return self.actions.push_back(Action::ListSegmentFiles { recording_id });
        }
        let mut files = Vec::new();
        if segment_offset == 0 {
            files.push(segment_file_name(recording_id, segment_base));
        }
        let mut base = segment_base + i64::from(segment_length);
        while base <= d.stop_position {
            files.push(segment_file_name(recording_id, base));
            base += i64::from(segment_length);
        }
        if segment_offset > 0 && d.stop_position != position {
            let bits = d.term_buffer_length.trailing_zeros();
            self.pending_deletes.insert(
                recording_id,
                PendingDelete {
                    control_session_id: cs,
                    correlation_id: corr,
                    await_replays: true,
                    erase: Some((position, files)),
                },
            );
            return self.actions.push_back(Action::EraseSegment {
                recording_id,
                file_name: segment_file_name(recording_id, segment_base),
                segment_offset: segment_offset as u64,
                segment_length,
                expected: ExpectedFrame {
                    term_offset: (position & (i64::from(d.term_buffer_length) - 1)) as i32,
                    term_id: d.initial_term_id.wrapping_add((position >> bits) as i32),
                    stream_id: d.stream_id,
                },
            });
        }
        self.stop_all_replays(recording_id, true);
        self.delete_segments(cs, corr, recording_id, files, true);
    }

    pub(super) fn purge_recording(&mut self, cs: i64, corr: i64, recording_id: i64) {
        let Ok(d) = self.catalog.descriptor(recording_id) else {
            return self.fail(
                cs,
                corr,
                error_code::UNKNOWN_RECORDING,
                &super::unknown_recording(recording_id),
            );
        };
        if self.has_replays(recording_id) {
            let message = format!("cannot purge recording with active replay {recording_id}");
            return self.fail(cs, corr, error_code::ACTIVE_RECORDING, &message);
        }
        if d.stop_position == NULL_POSITION {
            let message = format!("cannot purge active recording {recording_id}");
            return self.fail(cs, corr, error_code::ACTIVE_RECORDING, &message);
        }
        if !self.delete_allowed(cs, corr, recording_id) {
            return;
        }
        self.catalog.change_state(recording_id, RecordingState::Deleted);
        self.pending_deletes.insert(
            recording_id,
            PendingDelete {
                control_session_id: cs,
                correlation_id: corr,
                await_replays: false,
                erase: None,
            },
        );
        self.actions.push_back(Action::ListSegmentFiles { recording_id });
    }

    /// A recording's files, listed: its `.rec` and `.del` ones go.
    pub(super) fn on_segment_files(&mut self, recording_id: i64, file_names: Vec<String>) {
        let Some(pending) = self.pending_deletes.remove(&recording_id) else {
            return;
        };
        let prefix = format!("{recording_id}-");
        let files = file_names
            .into_iter()
            .filter(|f| f.starts_with(&prefix) && (f.ends_with(SEGMENT_SUFFIX) || f.ends_with(DELETE_SUFFIX)))
            .collect();
        if pending.await_replays {
            self.stop_all_replays(recording_id, true);
        }
        self.delete_segments(
            pending.control_session_id,
            pending.correlation_id,
            recording_id,
            files,
            pending.await_replays,
        );
    }

    /// A truncated recording's segment cut, or not: then the catalog stays
    /// truncated, as Java's.
    pub(super) fn on_segment_erased(&mut self, recording_id: i64, result: Result<(), EraseError>) {
        let Some(pending) = self.pending_deletes.remove(&recording_id) else {
            return;
        };
        let Some((position, files)) = pending.erase else {
            return;
        };
        let (cs, corr) = (pending.control_session_id, pending.correlation_id);
        match result {
            Ok(()) => {
                self.stop_all_replays(recording_id, true);
                self.delete_segments(cs, corr, recording_id, files, true);
            }
            Err(EraseError::NotAligned) => {
                let message = format!("{position} position not aligned to a data header");
                self.fail(cs, corr, error_code::GENERIC, &message);
            }
            Err(EraseError::Io(message)) => {
                self.error(format!("ERROR - {message}"));
                self.fail(cs, corr, error_code::GENERIC, &message);
            }
        }
    }

    /// `deleteSegments`: OK with the count of files; DELETE once they are
    /// gone, at once when there are none.
    fn delete_segments(&mut self, cs: i64, corr: i64, recording_id: i64, files: Vec<String>, await_replays: bool) {
        self.ok(cs, corr, files.len() as i64);
        if files.is_empty() {
            return self.signal(cs, corr, recording_id, NULL_POSITION, NULL_POSITION, Signal::DELETE);
        }
        let max_delete_position = files
            .iter()
            .filter_map(|f| base_position(recording_id, f))
            .max()
            .unwrap_or(NULL_POSITION);
        self.deletes.insert(
            recording_id,
            DeleteSession {
                control_session_id: cs,
                correlation_id: corr,
                files: files.into(),
                await_replays,
                in_flight: false,
                max_delete_position,
            },
        );
    }

    /// One file a cycle for each delete, once the replays it waits on stopped.
    pub(super) fn delete_work(&mut self) -> usize {
        let mut work = 0;
        let ids: Vec<i64> = self.deletes.keys().copied().collect();
        for recording_id in ids {
            let replaying = self.has_replays(recording_id);
            let Some(d) = self.deletes.get_mut(&recording_id) else {
                continue;
            };
            if d.in_flight {
                continue;
            }
            if d.await_replays {
                if !replaying {
                    d.await_replays = false;
                    work += 1;
                }
                continue;
            }
            if let Some(file_name) = d.files.pop_front() {
                d.in_flight = true;
                self.actions.push_back(Action::DeleteSegmentFile {
                    recording_id,
                    file_name,
                });
                work += 1;
            }
        }
        work
    }

    /// A file deleted, or not: the next one next cycle, DELETE after the last.
    pub(super) fn on_segment_file_deleted(&mut self, recording_id: i64, result: Result<(), String>) {
        let Some(d) = self.deletes.get_mut(&recording_id) else {
            return;
        };
        d.in_flight = false;
        let (cs, corr, done) = (d.control_session_id, d.correlation_id, d.files.is_empty());
        if let Err(message) = result {
            self.warn(message.clone());
            self.fail(cs, corr, error_code::GENERIC, &message);
        }
        if done {
            self.deletes.remove(&recording_id);
            self.signal(cs, corr, recording_id, NULL_POSITION, NULL_POSITION, Signal::DELETE);
        }
    }
}
