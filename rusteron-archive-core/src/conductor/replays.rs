//! Replays: Java's `startReplay` and `startBoundedReplay` (their checks, in
//! Java's order, and the replay's channel), the publication being created,
//! the replay's id, its end.

use super::{Action, Conductor, Counters, ReplayLimit, SubscriptionToken};
use crate::catalog::NULL_POSITION;
use crate::channel::{self, ChannelUri, ReplayStream};
use crate::protocol::error_code;
use crate::replay::{ReplayError, ReplayParams, ReplaySession};

/// `AeronArchive.REPLAY_ALL_AND_FOLLOW`: as far as the recording goes.
const REPLAY_ALL_AND_FOLLOW: i64 = -1;
/// `AeronArchive.REPLAY_ALL_AND_STOP`: to where the recording is now.
const REPLAY_ALL_AND_STOP: i64 = -2;
const FRAME_ALIGNMENT: i64 = 32;

pub(crate) struct ReplayRequest {
    pub(crate) control_session_id: i64,
    pub(crate) correlation_id: i64,
    pub(crate) recording_id: i64,
    pub(crate) position: i64,
    pub(crate) length: i64,
    pub(crate) replay_stream_id: i32,
    pub(crate) file_io_max_length: i32,
    pub(crate) replay_token: i64,
    pub(crate) replay_channel: String,
    pub(crate) limit_counter_id: Option<i32>,
}

/// A replay whose publication is being added.
pub(crate) struct PendingReplay {
    control_session_id: i64,
    correlation_id: i64,
    channel: String,
    stream_id: i32,
    params: ReplayParams,
    limit: ReplayLimit,
    signals_eos: bool,
}

/// A replay running.
pub(crate) struct ActiveReplay {
    pub(crate) control_session_id: i64,
    pub(crate) correlation_id: i64,
    pub(crate) recording_id: i64,
    /// Its channel signals the end of stream: not `eos=false`, as a cluster's
    /// catch-up, a replay merge's and a replication's are.
    pub(crate) signals_eos: bool,
}

impl Conductor {
    pub(super) fn start_replay(&mut self, r: ReplayRequest, counters: &impl Counters) {
        let (cs, corr, id) = (r.control_session_id, r.correlation_id, r.recording_id);
        // Java parses the channel as it decodes the request: no answer when it cannot.
        let uri = match ChannelUri::parse(&r.replay_channel) {
            Ok(uri) => uri,
            Err(e) => return self.error(e),
        };
        if r.replay_token != NULL_POSITION && uri.get("control-mode") == Some("response") {
            return self.fail(
                cs,
                corr,
                error_code::GENERIC,
                "response channel replays are not supported by this archive",
            );
        }
        let mut limit = ReplayLimit::None;
        let mut limit_position = NULL_POSITION;
        if let Some(counter_id) = r.limit_counter_id {
            match counters.value(counter_id) {
                Some(value) => {
                    limit = ReplayLimit::Counter(counter_id);
                    limit_position = value;
                }
                None => {
                    let message = format!(
                        "unable to create replay limit counter id= {counter_id} because of: ERROR - Counter id is \
                         not allocated: {counter_id}"
                    );
                    return self.fail(cs, corr, error_code::GENERIC, &message);
                }
            }
        }
        let max = self.config.max_concurrent_replays;
        if self.num_active_replays == max {
            let message = format!("max concurrent replays reached {max}");
            return self.fail(cs, corr, error_code::MAX_REPLAYS, &message);
        }
        let Ok(d) = self.catalog.descriptor(id) else {
            return self.fail(cs, corr, error_code::UNKNOWN_RECORDING, &super::unknown_recording(id));
        };
        if limit == ReplayLimit::None
            && let Some(position) = self.recorded_position(id)
        {
            limit = ReplayLimit::Recording(id);
            limit_position = position;
        }
        let beyond_limit = |position: i64, limit: i64| {
            format!(
                "requested replay start position={position} must be less than the limit position={limit} for \
                 recording {id}"
            )
        };
        let mut replay_position = d.start_position;
        if r.position != NULL_POSITION {
            if r.position & (FRAME_ALIGNMENT - 1) != 0 {
                let message = format!(
                    "requested replay start position={} is not a multiple of FRAME_ALIGNMENT (32) for recording {id}",
                    r.position
                );
                return self.fail(cs, corr, error_code::GENERIC, &message);
            }
            if r.position < d.start_position {
                let message = format!(
                    "requested replay start position={} is less than recording start position={} for recording {id}",
                    r.position, d.start_position
                );
                return self.fail(cs, corr, error_code::INVALID_POSITION, &message);
            }
            if d.stop_position != NULL_POSITION && r.position >= d.stop_position {
                let message = beyond_limit(r.position, d.stop_position);
                return self.fail(cs, corr, error_code::INVALID_POSITION, &message);
            }
            replay_position = r.position;
        }
        if r.file_io_max_length > 0 && r.file_io_max_length < d.mtu_length {
            let message = format!("fileIoMaxLength={} < mtuLength={}", r.file_io_max_length, d.mtu_length);
            return self.fail(cs, corr, error_code::GENERIC, &message);
        }
        if self.config.file_io_max_length < d.mtu_length {
            let message = format!(
                "replayBufferCapacity={} < mtuLength={}",
                self.config.file_io_max_length, d.mtu_length
            );
            return self.fail(cs, corr, error_code::GENERIC, &message);
        }
        let (stop_position, max_length) = if limit_position != NULL_POSITION {
            if replay_position > limit_position {
                let message = beyond_limit(replay_position, limit_position);
                return self.fail(cs, corr, error_code::INVALID_POSITION, &message);
            }
            (limit_position, i64::MAX - replay_position)
        } else {
            (d.stop_position, d.stop_position - replay_position)
        };
        let replay_length = match r.length {
            REPLAY_ALL_AND_FOLLOW => max_length,
            REPLAY_ALL_AND_STOP => {
                let length = stop_position - replay_position;
                if length == 0 {
                    let message =
                        format!("when replaying and stopping the replay length must be non-zero, recordingId={id}");
                    return self.fail(cs, corr, error_code::EMPTY_RECORDING, &message);
                }
                length
            }
            length => length.min(max_length),
        };
        if replay_length < 0 {
            let message = format!(
                "replay length must be positive: replayLength={replay_length}, length={}, stopPosition=\
                 {stop_position}, replayPosition={replay_position} for recording {id}",
                r.length
            );
            return self.fail(cs, corr, error_code::GENERIC, &message);
        }
        if self
            .deletes
            .get(&id)
            .is_some_and(|ds| ds.max_delete_position > d.stop_position && stop_position > d.stop_position)
        {
            let message = format!("cannot start replay of recording {id} due to an outstanding delete operation");
            return self.fail(cs, corr, error_code::GENERIC, &message);
        }
        let stream = ReplayStream {
            initial_term_id: d.initial_term_id,
            term_length: d.term_buffer_length,
            mtu_length: d.mtu_length,
        };
        let channel = match channel::replay_channel(
            &r.replay_channel,
            replay_position,
            stream,
            self.config.replay_linger_timeout_ns,
        ) {
            Ok(channel) => channel,
            Err(e) => {
                self.error(e.clone());
                return self.fail(
                    cs,
                    corr,
                    error_code::GENERIC,
                    &format!("failed to process replayChannel - {e}"),
                );
            }
        };
        let buffer_length = if r.file_io_max_length > 0 && r.file_io_max_length < self.config.file_io_max_length {
            r.file_io_max_length
        } else {
            self.config.file_io_max_length
        };
        let params = ReplayParams {
            replay_session_id: NULL_POSITION,
            recording_id: id,
            start_position: d.start_position,
            replay_position,
            stop_position,
            replay_length,
            term_length: d.term_buffer_length,
            segment_length: d.segment_file_length,
            initial_term_id: d.initial_term_id,
            stream_id: d.stream_id,
            follows_counter: limit != ReplayLimit::None,
            buffer_length: buffer_length as usize,
            connect_deadline_ms: 0,
        };
        let token = self.next_token;
        self.next_token += 1;
        self.actions.push_back(Action::AddReplayPublication {
            publication: token,
            channel: channel.clone(),
            stream_id: r.replay_stream_id,
        });
        self.pending_replays.insert(
            token,
            PendingReplay {
                control_session_id: cs,
                correlation_id: corr,
                channel,
                stream_id: r.replay_stream_id,
                params,
                limit,
                // As aeron-io/aeron#2260's `ReplaySession` reads it.
                signals_eos: uri.get("eos") != Some("false"),
            },
        );
        self.num_active_replays += 1;
    }

    /// `newReplaySession`: the replay's id, its publication's session in its
    /// low half.
    pub(super) fn on_replay_publication(&mut self, token: SubscriptionToken, result: Result<i32, (bool, String)>) {
        let Some(pending) = self.pending_replays.remove(&token) else {
            return;
        };
        match result {
            Ok(session_id) => {
                let replay_session_id = (i64::from(self.next_replay_id) << 32) | i64::from(session_id as u32);
                self.next_replay_id = self.next_replay_id.wrapping_add(1);
                let params = ReplayParams {
                    replay_session_id,
                    connect_deadline_ms: self.now_ms + self.config.connect_timeout_ms,
                    ..pending.params
                };
                self.replays.insert(
                    replay_session_id,
                    ActiveReplay {
                        control_session_id: pending.control_session_id,
                        correlation_id: pending.correlation_id,
                        recording_id: params.recording_id,
                        signals_eos: pending.signals_eos,
                    },
                );
                self.actions.push_back(Action::StartReplay {
                    replay_session_id,
                    publication: token,
                    session: Box::new(ReplaySession::new(params)),
                    limit: pending.limit,
                });
            }
            Err((true, _)) => {
                self.actions.push_back(Action::AddReplayPublication {
                    publication: token,
                    channel: pending.channel.clone(),
                    stream_id: pending.stream_id,
                });
                self.pending_replays.insert(token, pending);
            }
            Err((false, message)) => {
                self.num_active_replays -= 1;
                self.error(format!("ERROR - {message}"));
                self.fail(
                    pending.control_session_id,
                    pending.correlation_id,
                    error_code::GENERIC,
                    &format!("failed to create replay publication: {message}"),
                );
            }
        }
    }

    /// `closeReplaySession`: its error, if any, to its client.
    pub(super) fn close_replay(&mut self, replay_session_id: i64, error: Option<ReplayError>) {
        let Some(replay) = self.replays.remove(&replay_session_id) else {
            return;
        };
        self.num_active_replays -= 1;
        if let Some(error) = error {
            self.error(format!("ERROR - {}", error.message));
            self.fail(
                replay.control_session_id,
                replay.correlation_id,
                error.code,
                &error.message,
            );
        }
    }

    /// Stops a recording's replays, or every replay for -1: all revoked as
    /// the recording goes (`gone`), or on a client's stop each as
    /// [`Conductor::stop_replay_action`] has it.
    pub(super) fn stop_all_replays(&mut self, recording_id: i64, gone: bool) {
        let aborts: Vec<Action> = self
            .replays
            .iter()
            .filter(|(_, r)| recording_id == NULL_POSITION || r.recording_id == recording_id)
            .map(|(&replay_session_id, r)| Action::AbortReplay {
                replay_session_id,
                revoke: gone || r.signals_eos,
            })
            .collect();
        self.actions.extend(aborts);
    }

    /// A client's stop of a replay: revoked, its images dropped at once
    /// without their readers reading the rest, as Java's archive does; closed
    /// if its channel does not signal the end of stream (`eos=false`), as
    /// aeron-io/aeron#2260 has Java's: a revoke would signal it anyway, and
    /// such a replay shares its image with a live stream, a cluster
    /// follower's catch-up with its log.
    pub(super) fn stop_replay_action(&self, replay_session_id: i64) -> Option<Action> {
        self.replays.get(&replay_session_id).map(|r| Action::AbortReplay {
            replay_session_id,
            revoke: r.signals_eos,
        })
    }

    pub(super) fn has_replays(&self, recording_id: i64) -> bool {
        self.replays.values().any(|r| r.recording_id == recording_id)
    }

    pub fn replay_count(&self) -> usize {
        self.replays.len()
    }
}
