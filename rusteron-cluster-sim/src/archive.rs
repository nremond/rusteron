//! A member's archive as far as its consensus module asks of it, at the
//! level of the log's messages: recordings of the leader's publication and
//! of a follower's log image, replays to the module or to a follower's
//! catch-up endpoint, replication from another member's archive, and the
//! control protocol's answers and signals, as the real archive sends them.

use std::collections::{BTreeMap, VecDeque};

use rusteron_archive_core::catalog::{Catalog, Descriptor};
use rusteron_archive_core::protocol::{self, Code, Request, Signal};

/// The recordings' term length, as the simulated log channel sets it.
pub const TERM_LENGTH: i32 = 64 * 1024;
/// The archive's error codes the module distinguishes.
const UNKNOWN_RECORDING: i64 = 5;
const ACTIVE_RECORDING: i64 = 10;
const GENERIC: i64 = 0;

/// A message of a recording: where it starts and ends in the stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub start: i64,
    pub end: i64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct Recording {
    pub id: i64,
    pub session_id: i32,
    pub stream_id: i32,
    pub channel: String,
    /// Known once the stream it records is there.
    pub start_position: Option<i64>,
    /// `None` while recording.
    pub stop_position: Option<i64>,
    pub messages: Vec<Message>,
    pub subscription_id: i64,
    pub counter_id: i32,
    pub registration_id: i64,
    pub control_correlation_id: i64,
    /// An extension waits for its stream to join where the recording stopped.
    pub extending_from: Option<i64>,
}

impl Recording {
    pub fn position(&self) -> i64 {
        self.messages.last().map_or(self.start_position.unwrap_or(0), |m| m.end)
    }

    pub fn is_recording(&self) -> bool {
        self.stop_position.is_none()
    }

    /// Recording and its stream there: its counter is up.
    pub fn has_counter(&self) -> bool {
        self.is_recording() && self.start_position.is_some() && self.extending_from.is_none()
    }

    /// The message starting at `position`.
    pub fn message_at(&self, position: i64) -> Option<&Message> {
        let i = self.messages.partition_point(|m| m.start < position);
        self.messages.get(i).filter(|m| m.start == position)
    }
}

/// Where a replay goes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplayTarget {
    /// This member's module, on the replay stream, as session `session_id`.
    Local { session_id: i32 },
    /// A follower's catch-up endpoint, as the log's session.
    Remote { endpoint: String, session_id: i32 },
}

#[derive(Clone, Debug)]
pub struct Replay {
    pub id: i64,
    pub recording_id: i64,
    pub position: i64,
    /// `i64::MAX`: as far as the recording goes, live.
    pub stop: i64,
    pub target: ReplayTarget,
    /// Its stream reached its subscriber: the image's join position is the replay's start.
    pub started: bool,
    /// A bounded replay's limit: the counter it never replays past (a
    /// member's commit position, for a backup).
    pub limit_counter_id: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct Replication {
    pub id: i64,
    /// The source archive's control endpoint.
    pub src_endpoint: String,
    pub src_recording_id: i64,
    pub dst_recording_id: i64,
    pub stop_position: i64,
    pub started: bool,
}

pub struct SimArchive {
    pub archive_id: i64,
    pub control_session_id: i64,
    pub recordings: BTreeMap<i64, Recording>,
    pub replays: Vec<Replay>,
    pub replications: Vec<Replication>,
    /// Replays to the member itself stopped before their end, by session:
    /// their images gone with them, wherever their readers were.
    pub aborted_replays: std::collections::BTreeSet<i32>,
    /// Replays to another member's catch-up endpoint stopped before their
    /// end, revoked: (endpoint, session, position). The follower's driver
    /// drops a revoked image at once, its readers there or not.
    pub revoked_replays: Vec<(String, i32, i64)>,
    /// A catch-up replay (`eos=false`) its client stops before its end is
    /// revoked, as Java's 1.54 archive does (`ReplaySession` aborted);
    /// otherwise closed, as rusteron's archive and aeron-io/aeron#2260 do,
    /// its image left to its readers. A replay to the member itself is
    /// revoked either way.
    pub revoke_stopped_catchups: bool,
    /// Busy, as an archive writing snapshots to a slow drive: the module's
    /// requests wait, in order, for [`Self::release_requests`].
    pub holding_requests: bool,
    held_requests: VecDeque<Vec<u8>>,
    /// Busy: its replays to other members send nothing.
    pub replays_stalled: bool,
    /// What it sends the module, to be delivered.
    pub responses: VecDeque<Vec<u8>>,
    next_recording_id: i64,
    next_subscription_id: i64,
    next_replay_id: i64,
    next_registration_id: i64,
}

fn param<'a>(channel: &'a str, name: &str) -> Option<&'a str> {
    channel
        .split(['?', '|'])
        .find_map(|p| p.strip_prefix(name).and_then(|v| v.strip_prefix('=')))
}

/// A channel's `session-id`.
pub fn session_id_of(channel: &str) -> Option<i32> {
    param(channel, "session-id").and_then(|s| s.parse().ok())
}

/// A channel's `endpoint`.
pub fn endpoint_of(channel: &str) -> Option<String> {
    param(channel, "endpoint").map(str::to_string)
}

impl SimArchive {
    pub fn new(archive_id: i64) -> Self {
        Self {
            archive_id,
            control_session_id: 900 + archive_id,
            recordings: BTreeMap::new(),
            replays: Vec::new(),
            replications: Vec::new(),
            aborted_replays: std::collections::BTreeSet::new(),
            revoked_replays: Vec::new(),
            revoke_stopped_catchups: true,
            holding_requests: false,
            held_requests: VecDeque::new(),
            replays_stalled: false,
            responses: VecDeque::new(),
            next_recording_id: 0,
            next_subscription_id: 5_000,
            next_replay_id: 1,
            next_registration_id: 20_000,
        }
    }

    /// The archive restarted with its node: what it recorded kept, nothing under way.
    pub fn restart(&mut self) {
        for recording in self.recordings.values_mut() {
            if recording.is_recording() {
                recording.stop_position = Some(recording.position());
                recording.start_position.get_or_insert(0);
            }
        }
        self.replays.clear();
        self.replications.clear();
        self.responses.clear();
        self.holding_requests = false;
        self.held_requests.clear();
        self.replays_stalled = false;
    }

    /// No longer busy: the requests held handled, in the order they came.
    pub fn release_requests(&mut self) {
        self.holding_requests = false;
        while let Some(message) = self.held_requests.pop_front() {
            self.handle_request(&message);
        }
    }

    fn ok(&mut self, correlation_id: i64, relevant_id: i64) {
        self.responses.push_back(protocol::control_response(
            self.control_session_id,
            correlation_id,
            relevant_id,
            Code::OK,
            "",
        ));
    }

    fn error(&mut self, correlation_id: i64, code: i64, message: &str) {
        self.responses.push_back(protocol::control_response(
            self.control_session_id,
            correlation_id,
            code,
            Code::ERROR,
            message,
        ));
    }

    fn signal(&mut self, correlation_id: i64, recording_id: i64, subscription_id: i64, position: i64, signal: Signal) {
        self.responses.push_back(protocol::recording_signal(
            self.control_session_id,
            correlation_id,
            recording_id,
            subscription_id,
            position,
            signal,
        ));
    }

    fn new_recording(&mut self, session_id: i32, stream_id: i32, channel: String, correlation_id: i64) -> i64 {
        let id = self.next_recording_id;
        self.next_recording_id += 1;
        let subscription_id = self.next_subscription_id;
        self.next_subscription_id += 1;
        let registration_id = self.next_registration_id;
        self.next_registration_id += 1;
        self.recordings.insert(
            id,
            Recording {
                id,
                session_id,
                stream_id,
                channel,
                start_position: None,
                stop_position: None,
                messages: Vec::new(),
                subscription_id,
                counter_id: 100 + id as i32,
                registration_id,
                control_correlation_id: correlation_id,
                extending_from: None,
            },
        );
        id
    }

    /// An empty recording, stopped: a service's snapshot stands for one.
    pub fn new_stopped_recording(&mut self, stream_id: i32) -> i64 {
        let id = self.new_recording(0, stream_id, "aeron:ipc?alias=snapshot".into(), -1);
        let recording = self.recordings.get_mut(&id).expect("just added");
        recording.start_position = Some(0);
        recording.stop_position = Some(0);
        id
    }

    fn stop(&mut self, recording_id: i64) -> bool {
        let Some(r) = self.recordings.get_mut(&recording_id) else {
            return false;
        };
        if !r.is_recording() {
            return false;
        }
        let position = r.position();
        r.start_position.get_or_insert(position);
        r.stop_position = Some(position);
        r.extending_from = None;
        let (corr, sub) = (r.control_correlation_id, r.subscription_id);
        self.signal(corr, recording_id, sub, position, Signal::STOP);
        true
    }

    /// The recording of a stream of `session_id` on `channel`, being recorded.
    fn recording_of(&mut self, session_id: i32, channel: Option<&str>) -> Option<&mut Recording> {
        self.recordings
            .values_mut()
            .find(|r| r.is_recording() && r.session_id == session_id && channel.is_none_or(|c| r.channel == c))
    }

    /// The stream of `session_id` is there at `position` (a publication's
    /// position, an image's join): a recording waiting for it starts, an
    /// extension checks it joins where it stopped.
    pub fn on_stream(&mut self, session_id: i32, channel: Option<&str>, position: i64) {
        let Some(r) = self.recording_of(session_id, channel) else {
            return;
        };
        if let Some(stop) = r.extending_from {
            if position != stop {
                let (id, corr) = (r.id, r.control_correlation_id);
                let message =
                    format!("cannot extend recording {id} image.joinPosition={position} != stopPosition={stop}");
                r.extending_from = None;
                r.stop_position = Some(stop);
                self.error(corr, GENERIC, &message);
                return;
            }
            r.extending_from = None;
        }
        r.start_position.get_or_insert(position);
    }

    /// A message of the stream of `session_id`, recorded if it is.
    pub fn on_data(&mut self, session_id: i32, channel: Option<&str>, message: &Message) {
        let Some(r) = self.recording_of(session_id, channel) else {
            return;
        };
        if r.extending_from.is_some() {
            return;
        }
        if r.start_position.is_none() {
            r.start_position = Some(message.start);
        }
        if message.start == r.position() {
            r.messages.push(message.clone());
        }
    }

    /// The stream of `session_id` went: an auto-stop recording of it stops.
    pub fn on_stream_gone(&mut self, session_id: i32, channel: Option<&str>) {
        if let Some(id) = self.recording_of(session_id, channel).map(|r| r.id) {
            self.stop(id);
        }
    }

    pub fn recording_by_counter(&self, counter_id: i32) -> Option<&Recording> {
        self.recordings
            .values()
            .filter(|r| r.counter_id == counter_id)
            .max_by_key(|r| r.registration_id)
    }

    /// A request from the module: held while the archive is busy.
    pub fn on_request(&mut self, message: &[u8]) {
        if self.holding_requests {
            self.held_requests.push_back(message.to_vec());
        } else {
            self.handle_request(message);
        }
    }

    fn handle_request(&mut self, message: &[u8]) {
        let Ok(request) = protocol::decode_request(message) else {
            return;
        };
        match request {
            Request::AuthConnect { correlation_id, .. } => {
                let cs = self.control_session_id;
                self.ok(correlation_id, cs);
            }
            Request::ArchiveId { correlation_id, .. } => {
                let id = self.archive_id;
                self.ok(correlation_id, id);
            }
            Request::KeepAlive { .. } | Request::CloseSession { .. } => {}
            Request::StartRecording {
                correlation_id,
                stream_id,
                channel,
                ..
            } => {
                let session_id = session_id_of(&channel).unwrap_or(0);
                let id = self.new_recording(session_id, stream_id, channel, correlation_id);
                let sub = self.recordings[&id].subscription_id;
                self.ok(correlation_id, sub);
                self.signal(correlation_id, id, sub, 0, Signal::START);
            }
            Request::ExtendRecording {
                correlation_id,
                recording_id,
                channel,
                ..
            } => {
                let subscription_id = self.next_subscription_id;
                self.next_subscription_id += 1;
                let registration_id = self.next_registration_id;
                self.next_registration_id += 1;
                let Some(r) = self.recordings.get_mut(&recording_id) else {
                    return self.error(
                        correlation_id,
                        UNKNOWN_RECORDING,
                        &format!("unknown recording id: {recording_id}"),
                    );
                };
                let Some(stop) = r.stop_position else {
                    return self.error(
                        correlation_id,
                        ACTIVE_RECORDING,
                        &format!("cannot extend active recording {recording_id}"),
                    );
                };
                r.session_id = session_id_of(&channel).unwrap_or(0);
                r.channel = channel;
                r.stop_position = None;
                r.extending_from = Some(stop);
                r.subscription_id = subscription_id;
                r.registration_id = registration_id;
                r.control_correlation_id = correlation_id;
                self.ok(correlation_id, subscription_id);
                self.signal(correlation_id, recording_id, subscription_id, stop, Signal::EXTEND);
            }
            Request::StopRecordingSubscription {
                correlation_id,
                subscription_id,
                ..
            } => {
                let found = self
                    .recordings
                    .values()
                    .find(|r| r.subscription_id == subscription_id && r.is_recording())
                    .map(|r| r.id);
                self.ok(correlation_id, 0);
                if let Some(id) = found {
                    self.stop(id);
                }
            }
            Request::StopRecordingByIdentity {
                correlation_id,
                recording_id,
                ..
            } => {
                let stopped = self.stop(recording_id);
                // The answer before the signal, as the real archive orders them.
                let signal = self.responses.pop_back().filter(|_| stopped);
                self.ok(correlation_id, i64::from(stopped));
                self.responses.extend(signal);
            }
            Request::ListRecording {
                correlation_id,
                recording_id,
                ..
            } => match self.recordings.get(&recording_id) {
                Some(r) => {
                    let bytes = descriptor_bytes(r);
                    self.responses.push_back(protocol::recording_descriptor(
                        self.control_session_id,
                        correlation_id,
                        &bytes,
                    ));
                }
                None => {
                    let cs = self.control_session_id;
                    self.responses.push_back(protocol::control_response(
                        cs,
                        correlation_id,
                        recording_id,
                        Code::RECORDING_UNKNOWN,
                        "",
                    ));
                }
            },
            Request::StopPosition {
                correlation_id,
                recording_id,
                ..
            } => match self.recordings.get(&recording_id) {
                Some(r) => {
                    // An extension not joined yet keeps its stop position, as the catalog does.
                    let position = r.stop_position.or(r.extending_from).unwrap_or(-1);
                    self.ok(correlation_id, position);
                }
                None => self.error(
                    correlation_id,
                    UNKNOWN_RECORDING,
                    &format!("unknown recording id: {recording_id}"),
                ),
            },
            Request::Replay {
                correlation_id,
                recording_id,
                position,
                length,
                replay_channel,
                limit_counter_id,
                ..
            } => {
                let Some(r) = self.recordings.get(&recording_id) else {
                    return self.error(
                        correlation_id,
                        UNKNOWN_RECORDING,
                        &format!("unknown recording id: {recording_id}"),
                    );
                };
                let start = r.start_position.unwrap_or(0);
                // NULL_POSITION: from the recording's start.
                let position = if position == -1 { start } else { position };
                if position < start || position > r.position() {
                    return self.error(
                        correlation_id,
                        GENERIC,
                        &format!(
                            "requested replay start position={position} is out of range for recording {recording_id}"
                        ),
                    );
                }
                let stop = if length == i64::MAX || limit_counter_id.is_some() {
                    i64::MAX
                } else if length < 0 {
                    r.stop_position.unwrap_or(r.position())
                } else {
                    position.saturating_add(length)
                };
                let mut id = self.next_replay_id;
                self.next_replay_id += 1;
                let target = match endpoint_of(&replay_channel) {
                    Some(endpoint) if replay_channel.starts_with("aeron:udp") => {
                        let session_id = session_id_of(&replay_channel).unwrap_or(id as i32);
                        // Java's replay session id: the replay's number, then its publication's session.
                        id = (id << 32) | i64::from(session_id as u32);
                        ReplayTarget::Remote { endpoint, session_id }
                    }
                    _ => ReplayTarget::Local { session_id: id as i32 },
                };
                self.replays.push(Replay {
                    id,
                    recording_id,
                    position,
                    stop,
                    target,
                    started: false,
                    limit_counter_id,
                });
                self.ok(correlation_id, id);
            }
            Request::StopReplay {
                correlation_id,
                replay_session_id,
                ..
            } => {
                let known = self.replays.iter().any(|r| r.id == replay_session_id);
                for r in self.replays.iter().filter(|r| r.id == replay_session_id) {
                    match &r.target {
                        ReplayTarget::Local { session_id } if r.position < r.stop => {
                            self.aborted_replays.insert(*session_id);
                        }
                        ReplayTarget::Remote { endpoint, session_id }
                            if r.position < r.stop && self.revoke_stopped_catchups =>
                        {
                            self.revoked_replays.push((endpoint.clone(), *session_id, r.position));
                        }
                        _ => {}
                    }
                }
                self.replays.retain(|r| r.id != replay_session_id);
                if known {
                    self.ok(correlation_id, 0);
                } else {
                    // The real archive's UNKNOWN_REPLAY.
                    self.error(
                        correlation_id,
                        7,
                        &format!("replay session not known: id={replay_session_id}"),
                    );
                }
            }
            Request::TruncateRecording {
                correlation_id,
                recording_id,
                position,
                ..
            } => match self.recordings.get_mut(&recording_id) {
                Some(r) if !r.is_recording() => {
                    let start = r.start_position.unwrap_or(0);
                    if position < start || position > r.position() {
                        return self.error(correlation_id, GENERIC, "invalid truncate position");
                    }
                    r.messages.retain(|m| m.end <= position);
                    r.stop_position = Some(position);
                    self.ok(correlation_id, 0);
                }
                Some(_) => self.error(
                    correlation_id,
                    ACTIVE_RECORDING,
                    &format!("cannot truncate active recording {recording_id}"),
                ),
                None => self.error(
                    correlation_id,
                    UNKNOWN_RECORDING,
                    &format!("unknown recording id: {recording_id}"),
                ),
            },
            Request::Replicate(r) => {
                let id = 3_000 + r.correlation_id;
                let Some(src_endpoint) = endpoint_of(&r.src_control_channel) else {
                    return self.error(r.correlation_id, GENERIC, "no source endpoint");
                };
                if r.dst_recording_id != -1 {
                    match self.recordings.get(&r.dst_recording_id) {
                        None => {
                            return self.error(
                                r.correlation_id,
                                UNKNOWN_RECORDING,
                                &format!("unknown recording id: {}", r.dst_recording_id),
                            );
                        }
                        Some(d) if d.is_recording() => {
                            return self.error(
                                r.correlation_id,
                                ACTIVE_RECORDING,
                                &format!("cannot replicate into active recording {}", r.dst_recording_id),
                            );
                        }
                        Some(_) => {}
                    }
                }
                self.replications.push(Replication {
                    id,
                    src_endpoint,
                    src_recording_id: r.src_recording_id,
                    dst_recording_id: r.dst_recording_id,
                    stop_position: r.stop_position,
                    started: false,
                });
                self.ok(r.correlation_id, id);
            }
            Request::StopReplication {
                correlation_id,
                replication_id,
                ..
            } => {
                let found = self.replications.iter().position(|r| r.id == replication_id);
                match found {
                    Some(i) => {
                        let replication = self.replications.remove(i);
                        self.ok(correlation_id, 0);
                        self.end_replication(&replication);
                    }
                    None => self.error(
                        correlation_id,
                        GENERIC,
                        &format!("unknown replication id {replication_id}"),
                    ),
                }
            }
            other => self.error(
                other_correlation(&other),
                GENERIC,
                "not served by the simulated archive",
            ),
        }
    }

    fn end_replication(&mut self, replication: &Replication) {
        let dst = replication.dst_recording_id;
        let position = self.recordings.get(&dst).map_or(-1, Recording::position);
        self.signal(replication.id, dst, -1, position, Signal::REPLICATE_END);
        if self.recordings.get(&dst).is_some_and(Recording::is_recording) {
            let r = self.recordings.get_mut(&dst).expect("present");
            r.stop_position = Some(position);
            self.signal(replication.id, dst, -1, position, Signal::STOP);
        }
    }

    /// One step of the replication at `index` from `src`'s recording: up to
    /// `budget` messages copied, its signals sent, ended at its stop position.
    pub fn replicate_from(&mut self, index: usize, src: &Recording, budget: usize) {
        let mut replication = self.replications[index].clone();
        if !replication.started {
            replication.started = true;
            if replication.dst_recording_id == -1 {
                let id = self.new_recording(src.session_id, src.stream_id, src.channel.clone(), replication.id);
                let start = src.start_position.unwrap_or(0);
                let r = self.recordings.get_mut(&id).expect("just added");
                r.start_position = Some(start);
                replication.dst_recording_id = id;
                self.signal(replication.id, id, -1, start, Signal::START);
            } else {
                let dst = replication.dst_recording_id;
                let r = self.recordings.get_mut(&dst).expect("checked at the request");
                let position = r.position();
                r.stop_position = None;
                r.registration_id = self.next_registration_id;
                self.next_registration_id += 1;
                self.signal(replication.id, dst, -1, position, Signal::EXTEND);
            }
        }
        let dst = replication.dst_recording_id;
        // Without a stop position, a stopped source is copied to its end, then synced.
        let sync = replication.stop_position < 0 && src.stop_position.is_some();
        let stop = if replication.stop_position >= 0 {
            replication.stop_position
        } else {
            src.stop_position.unwrap_or(i64::MAX)
        };
        let r = self.recordings.get_mut(&dst).expect("the destination");
        for _ in 0..budget {
            let position = r.position();
            if position >= stop {
                break;
            }
            match src.message_at(position) {
                Some(m) if m.end <= stop => r.messages.push(m.clone()),
                _ => break,
            }
        }
        let done = r.position() >= stop;
        let position = r.position();
        self.replications[index] = replication.clone();
        if done {
            self.replications.remove(index);
            if sync {
                self.signal(replication.id, dst, -1, position, Signal::SYNC);
            }
            self.end_replication(&replication);
        }
    }

    /// A request from another node's client, a backup's: answered on that
    /// client's own control session, `control_session_id`.
    pub fn on_remote_request(&mut self, control_session_id: i64, message: &[u8]) -> Vec<Vec<u8>> {
        let own_session = std::mem::replace(&mut self.control_session_id, control_session_id);
        let own_responses = std::mem::take(&mut self.responses);
        self.on_request(message);
        self.control_session_id = own_session;
        std::mem::replace(&mut self.responses, own_responses).into()
    }

    /// A replication whose source cannot be reached ends with an error, as
    /// the real archive's does once its connect times out.
    pub fn fail_replication(&mut self, index: usize, message: &str) {
        let replication = self.replications.remove(index);
        self.error(replication.id, GENERIC, message);
        if replication.started {
            self.end_replication(&replication);
        }
    }
}

fn other_correlation(request: &Request) -> i64 {
    match request {
        Request::StartPosition { correlation_id, .. } | Request::RecordingPosition { correlation_id, .. } => {
            *correlation_id
        }
        _ => -1,
    }
}

fn descriptor_bytes(r: &Recording) -> Vec<u8> {
    let descriptor = Descriptor {
        control_session_id: 0,
        correlation_id: 0,
        recording_id: -1,
        start_timestamp: 0,
        stop_timestamp: if r.stop_position.is_some() { 1 } else { -1 },
        start_position: r.start_position.unwrap_or(0),
        stop_position: r.stop_position.unwrap_or(-1),
        initial_term_id: 0,
        segment_file_length: 128 * 1024 * 1024,
        term_buffer_length: TERM_LENGTH,
        mtu_length: 1_408,
        session_id: r.session_id,
        stream_id: r.stream_id,
        stripped_channel: r.channel.clone(),
        original_channel: r.channel.clone(),
        source_identity: "aeron:ipc".into(),
    };
    // A catalog whose next id is this recording's, its descriptor's bytes then read back.
    let mut catalog = Catalog::create(1_024 * (r.id as usize + 2)).expect("a catalog");
    for _ in 0..r.id {
        catalog.add_recording(&descriptor).expect("room");
    }
    let id = catalog.add_recording(&descriptor).expect("room");
    catalog.descriptor_bytes(id).expect("the descriptor").to_vec()
}
