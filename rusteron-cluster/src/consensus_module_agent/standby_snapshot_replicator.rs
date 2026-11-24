//! Standbys' snapshots copied into the local archive (Java's
//! `StandbySnapshotReplicator` and `MultipleRecordingReplication`): the
//! newest complete set a standby gave notice of, newer than the member's
//! own, its recordings replicated one at a time from the standby's archive,
//! then filed as the member's snapshots for a recovery to load. At every
//! start, before the recovery plan is made; and at runtime on the node
//! control toggle. An archive that fails gives way to the next; with none
//! left, the member goes on with its own snapshots.

use crate::recording_log::Entry;

use super::election::create_destination_uri;
use super::*;

/// The node control toggle's states (Java's `NodeControl.ToggleState`).
pub mod node_toggle {
    pub const INACTIVE: i32 = 0;
    pub const NEUTRAL: i32 = 1;
    pub const REPLICATE_STANDBY_SNAPSHOT: i32 = 2;
}

/// A standby's set being copied.
#[derive(Debug)]
struct SetCopy {
    endpoint: String,
    /// The standby's entries, as the recording log lists them from its end.
    entries: Vec<Entry>,
    /// The local recordings of the entries copied so far, in order.
    copied: Vec<i64>,
    replication: Option<Replication>,
}

#[derive(Debug)]
pub(super) struct StandbyReplication {
    /// The sets not tried yet, newest position first.
    sets: VecDeque<(String, Vec<Entry>)>,
    current: Option<SetCopy>,
    /// Each archive that failed, and why.
    errors: Vec<String>,
}

/// How long a copy may go without progress: Java's, twice the archive
/// client's message timeout, checked ten times in that.
fn progress_timeout_ns() -> i64 {
    2 * archive::Config::default().message_timeout_ns
}

impl ConsensusModule {
    /// Starts copying the newest standby sets newer than the local
    /// snapshots: false when there is none.
    pub(super) fn start_standby_replication(&mut self) -> bool {
        let log = &self.recording_log;
        let mut sets: Vec<(String, Vec<Entry>)> = log
            .latest_standby_snapshots(self.config.service_count)
            .into_iter()
            .filter_map(|(endpoint, mut entries)| {
                entries.retain(|e| {
                    log.get_latest_snapshot(e.service_id)
                        .is_none_or(|local| e.log_position > local.log_position)
                });
                (!entries.is_empty()).then_some((endpoint, entries))
            })
            .collect();
        if sets.is_empty() {
            return false;
        }
        sets.sort_by(|(a_endpoint, a), (b_endpoint, b)| {
            b[0].log_position
                .cmp(&a[0].log_position)
                .then_with(|| a_endpoint.cmp(b_endpoint))
        });
        self.standby_replication = Some(StandbyReplication {
            sets: sets.into(),
            current: None,
            errors: Vec::new(),
        });
        true
    }

    /// Java's `StandbySnapshotReplicator.poll`: the work done, and whether
    /// the replication is over, a set filed or every archive failed.
    pub(super) fn standby_replication_work(&mut self, ports: &mut impl Ports) -> (usize, bool) {
        let Some(mut replication) = self.standby_replication.take() else {
            return (0, true);
        };
        let (work, done) = self.standby_replication_step(&mut replication, ports);
        if !done {
            self.standby_replication = Some(replication);
        }
        (work, done)
    }

    fn standby_replication_step(&mut self, r: &mut StandbyReplication, ports: &mut impl Ports) -> (usize, bool) {
        let Some(copy) = r.current.as_mut() else {
            let Some((endpoint, entries)) = r.sets.pop_front() else {
                self.warn(format!(
                    "failed to replicate any standby snapshots, errors: {{{}}}",
                    r.errors.join(", ")
                ));
                return (1, true);
            };
            r.current = Some(SetCopy {
                endpoint,
                entries,
                copied: Vec::new(),
                replication: None,
            });
            return (1, false);
        };
        if copy.copied.len() == copy.entries.len() {
            let copy = r.current.take().expect("a set");
            return (1, self.file_standby_set(&copy, ports));
        }
        let Some(replication) = copy.replication.as_mut() else {
            let entry = &copy.entries[copy.copied.len()];
            let request = archive::Request::Replicate {
                src_recording_id: entry.recording_id,
                dst_recording_id: NULL_VALUE,
                stop_position: NULL_POSITION,
                src_control_stream_id: self.config.archive_control_request_stream_id,
                src_control_channel: create_destination_uri(
                    &self.config.leader_archive_control_channel,
                    &copy.endpoint,
                ),
                replication_channel: self.config.replication_channel.clone(),
                replication_session_id: ports.next_correlation_id() as i32,
            };
            match self.archive_request(ports, request, Some(Request::ReplicateStandby)) {
                Ok(correlation_id) => {
                    let now_ns = self.now_ns;
                    let timeout_ns = progress_timeout_ns();
                    copy.replication = Some(Replication {
                        correlation_id,
                        replication_id: None,
                        stop_position: NULL_POSITION,
                        counter: None,
                        recording_id: NULL_VALUE,
                        position: NULL_POSITION,
                        progress_deadline_ns: now_ns + timeout_ns,
                        progress_check_deadline_ns: now_ns + timeout_ns / 10,
                        has_replication_ended: false,
                        has_synced: false,
                        has_stopped: false,
                    });
                }
                Err(Fault::Error(e) | Fault::Warning(e) | Fault::Terminate(e)) => {
                    self.standby_archive_failed(r, &e, ports);
                }
            }
            return (1, false);
        };
        let timeout_ns = progress_timeout_ns();
        let (mut work, progressed) = replication.poll_progress(
            self.now_ns,
            timeout_ns / 10,
            timeout_ns,
            self.archive.archive_id(),
            ports,
        );
        if !progressed {
            self.standby_archive_failed(r, "log replication has not progressed", ports);
            return (work + 1, false);
        }
        if replication.has_replication_ended {
            if replication.has_synced {
                copy.copied.push(replication.recording_id);
            }
            // Ended without its sync, the recording is copied again.
            copy.replication = None;
            work += 1;
        }
        (work, false)
    }

    /// The copies filed as the member's snapshots (Java's appends, then
    /// `force` and the snapshot counter): whether they were.
    fn file_standby_set(&mut self, copy: &SetCopy, ports: &mut impl Ports) -> bool {
        for (entry, &recording_id) in copy.entries.iter().zip(&copy.copied) {
            let filed = self.recording_log.append_snapshot(
                recording_id,
                entry.leadership_term_id,
                entry.term_base_log_position,
                entry.log_position,
                entry.timestamp,
                entry.service_id,
            );
            if let Err(e) = filed {
                self.warn(format!("standby snapshot from {}: {e}", copy.endpoint));
                break;
            }
        }
        self.flush_recording_log(true);
        self.snapshot_count += 1;
        ports.set_counter(CounterId::SnapshotCount, self.snapshot_count);
        true
    }

    /// The set being copied given up: its archive's error kept, its
    /// replication stopped; the next archive's set tried.
    fn standby_archive_failed(&mut self, r: &mut StandbyReplication, message: &str, ports: &mut impl Ports) {
        let Some(copy) = r.current.take() else {
            return;
        };
        r.errors.push(format!("{}={message}", copy.endpoint));
        if let Some(replication) = copy.replication.filter(|c| !c.has_replication_ended) {
            match replication.replication_id {
                Some(replication_id) => {
                    let _ = self.archive_request(
                        ports,
                        archive::Request::StopReplication { replication_id },
                        Some(Request::TryStop),
                    );
                }
                None => {
                    self.requests
                        .insert(replication.correlation_id, Request::ReplicateAbandoned);
                }
            }
        }
    }

    /// The archive accepted a copy: its replication id, which its signals carry.
    pub(super) fn on_standby_replication_started(&mut self, correlation_id: i64, replication_id: i64) {
        if let Some(replication) = self.standby_copy()
            && replication.correlation_id == correlation_id
        {
            replication.replication_id = Some(replication_id);
        }
    }

    /// Whether `correlation_id` is the replication of the copy under way:
    /// once an archive takes a copy, it answers the copy's errors under the
    /// replication's id (Java's `ReplicationSession.error`), a source
    /// recording gone among them.
    pub(super) fn is_standby_replication(&mut self, correlation_id: i64) -> bool {
        self.standby_copy()
            .is_some_and(|r| r.replication_id == Some(correlation_id))
    }

    /// The archive refused a copy, or did not answer.
    pub(super) fn on_standby_replication_failed(&mut self, message: &str, ports: &mut impl Ports) {
        if let Some(mut r) = self.standby_replication.take() {
            self.standby_archive_failed(&mut r, message, ports);
            self.standby_replication = Some(r);
        }
    }

    /// A recording signal for a copy (Java's `RecordingReplication.onSignal`):
    /// whether it was one.
    pub(super) fn on_standby_replication_signal(
        &mut self,
        replication_id: i64,
        recording_id: i64,
        position: i64,
        signal: Signal,
        ports: &mut impl Ports,
    ) -> bool {
        let archive_id = self.archive.archive_id();
        let Some(replication) = self.standby_copy() else {
            return false;
        };
        if replication.replication_id != Some(replication_id) {
            return false;
        }
        if signal == Signal::DELETE {
            let message = format!("recording was deleted during replication: recordingId={recording_id}");
            self.on_standby_replication_failed(&message, ports);
            return true;
        }
        if signal == Signal::EXTEND {
            replication.counter = ports.find_recording_counter_by_recording(recording_id, archive_id);
        } else if signal == Signal::SYNC {
            replication.has_synced = true;
        } else if signal == Signal::REPLICATE_END {
            replication.has_replication_ended = true;
        } else if signal == Signal::STOP {
            replication.has_stopped = true;
        }
        if recording_id != NULL_VALUE {
            replication.recording_id = recording_id;
        }
        if position != NULL_POSITION {
            replication.position = position;
        }
        true
    }

    fn standby_copy(&mut self) -> Option<&mut Replication> {
        self.standby_replication
            .as_mut()
            .and_then(|r| r.current.as_mut())
            .and_then(|c| c.replication.as_mut())
    }

    /// Java's `checkNodeControlToggle`: a replication of the standbys' newest
    /// snapshots on request, outside an election and active.
    pub(super) fn check_node_control_toggle(&mut self, ports: &mut impl Ports) -> usize {
        if ports.node_control_toggle() != node_toggle::REPLICATE_STANDBY_SNAPSHOT {
            return 0;
        }
        if self.standby_replication.is_none() {
            self.start_standby_replication();
        }
        ports.set_node_control_toggle(node_toggle::NEUTRAL);
        1
    }
}

impl Replication {
    /// Java's `RecordingReplication.poll`: the destination recording's
    /// position checked every `interval_ns`; the work done, and whether it
    /// progressed within `timeout_ns`.
    pub(super) fn poll_progress(
        &mut self,
        now_ns: i64,
        interval_ns: i64,
        timeout_ns: i64,
        archive_id: i64,
        ports: &mut impl Ports,
    ) -> (usize, bool) {
        if self.has_replication_ended {
            return (0, true);
        }
        let mut work = 0;
        if now_ns >= self.progress_check_deadline_ns {
            self.progress_check_deadline_ns = now_ns + interval_ns;
            if self.counter.is_none() && self.recording_id != NULL_VALUE {
                // Found on EXTEND unless the driver had yet to register it then.
                self.counter = ports.find_recording_counter_by_recording(self.recording_id, archive_id);
            }
            if let Some(counter) = self.counter
                && ports.is_counter_active(counter.counter_id, counter.registration_id)
            {
                let position = ports.counter_value(counter.counter_id);
                if position > self.position {
                    self.position = position;
                    self.progress_deadline_ns = now_ns + timeout_ns;
                }
            }
            work += 1;
        }
        (work, now_ns < self.progress_deadline_ns)
    }
}
