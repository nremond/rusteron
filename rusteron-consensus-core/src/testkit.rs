//! For tests of the consensus module: a node in memory. Its ports keep
//! what the module offers ([`FakePorts`]); a fake archive answers the
//! module's requests, records its log and snapshots and replays them
//! ([`FakeArchive`]); fake services ack as Java's container does
//! ([`FakeServices`]); [`FakeNode`] runs the duty cycle and carries each
//! action and its outcome between them.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use rusteron_archive::catalog::{Catalog, Descriptor};
use rusteron_archive::control_session_adapter::{self as archive_protocol, Code, Request, Signal};
use rusteron_cluster_codecs::cluster::{
    boolean_type::BooleanType, join_log_codec, request_service_ack_codec, service_ack_codec,
    service_termination_position_codec,
};
use rusteron_cluster_core::log::Fragment;
use rusteron_cluster_core::node_state::{self, NodeState};
use rusteron_cluster_core::position::UNFRAGMENTED;
use rusteron_cluster_core::recording_log::RecordingLog;

use crate::codec::{self, encode};
use crate::config::Config;
use crate::consensus::{self, ConsensusMessage};
use crate::io::{Action, CounterId, EgressId, Io, Offer, Ports, RecordingCounter, SubscriptionId};
use crate::log::{self, LogMessage};
use crate::module::{ConsensusModule, Poll};

/// The log publication's session id, as the fake driver gives it.
pub const LOG_SESSION_ID: i32 = 7_777;
/// The snapshot publications' session ids start here.
const SNAPSHOT_SESSION_BASE: i32 = 8_000;
/// The images' term length.
pub const TERM_LENGTH: i32 = 64 * 1024 * 1024;

/// A recording of the fake archive: its messages, each with the position after it.
#[derive(Clone, Debug)]
pub struct FakeRecording {
    pub recording_id: i64,
    pub session_id: i32,
    pub stream_id: i32,
    pub channel: String,
    pub start_position: i64,
    pub stop_position: Option<i64>,
    pub messages: Vec<(Vec<u8>, i64)>,
    pub subscription_id: i64,
    pub counter_id: i32,
    pub control_correlation_id: i64,
}

impl FakeRecording {
    fn position(&self) -> i64 {
        self.messages.last().map_or(self.start_position, |(_, p)| *p)
    }
}

/// An archive as far as a consensus module asks of it.
pub struct FakeArchive {
    pub archive_id: i64,
    pub control_session_id: i64,
    pub recordings: BTreeMap<i64, FakeRecording>,
    /// Replays under way: (replay session id, recording, from, to).
    pub replays: Vec<(i64, i64, i64, i64)>,
    /// What it sends back, to be delivered.
    pub responses: VecDeque<Vec<u8>>,
    pub requests: Vec<Request>,
    /// Recording counters read 0 while this holds, as a real archive's
    /// between allocating one and storing its recording's start position.
    pub counters_unset: bool,
    next_recording_id: i64,
    next_subscription_id: i64,
    next_replay: i64,
}

impl Default for FakeArchive {
    fn default() -> Self {
        Self {
            archive_id: 42,
            control_session_id: 900,
            recordings: BTreeMap::new(),
            replays: Vec::new(),
            responses: VecDeque::new(),
            requests: Vec::new(),
            counters_unset: false,
            next_recording_id: 0,
            next_subscription_id: 5_000,
            next_replay: 1,
        }
    }
}

fn session_id_of(channel: &str) -> Option<i32> {
    channel
        .split(['?', '|'])
        .find_map(|p| p.strip_prefix("session-id="))
        .and_then(|s| s.parse().ok())
}

impl FakeArchive {
    fn ok(&mut self, correlation_id: i64, relevant_id: i64) {
        self.responses.push_back(archive_protocol::control_response(
            self.control_session_id,
            correlation_id,
            relevant_id,
            Code::OK,
            "",
        ));
    }

    /// An error answered under `correlation_id`, its code as Java's `ArchiveException` has them: a request's,
    /// or a replication's after the archive took it, under the replication's id.
    pub fn error(&mut self, correlation_id: i64, code: i64, message: &str) {
        self.responses.push_back(archive_protocol::control_response(
            self.control_session_id,
            correlation_id,
            code,
            Code::ERROR,
            message,
        ));
    }

    /// A recording signal to the module.
    pub fn signal(
        &mut self,
        correlation_id: i64,
        recording_id: i64,
        subscription_id: i64,
        position: i64,
        signal: Signal,
    ) {
        self.responses.push_back(archive_protocol::recording_signal(
            self.control_session_id,
            correlation_id,
            recording_id,
            subscription_id,
            position,
            signal,
        ));
    }

    /// The recording of a publication's session, being recorded.
    pub fn recording_of_session(&self, session_id: i32) -> Option<&FakeRecording> {
        self.recordings
            .values()
            .find(|r| r.session_id == session_id && r.stop_position.is_none())
    }

    /// A request from the module.
    pub fn on_request(&mut self, message: &[u8]) {
        let request = archive_protocol::decode_request(message).expect("a request");
        self.requests.push(request.clone());
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
                let subscription_id = self.next_subscription_id;
                self.next_subscription_id += 1;
                let recording_id = self.next_recording_id;
                self.next_recording_id += 1;
                self.ok(correlation_id, subscription_id);
                let session_id = session_id_of(&channel).expect("a session id");
                self.recordings.insert(
                    recording_id,
                    FakeRecording {
                        recording_id,
                        session_id,
                        stream_id,
                        channel,
                        start_position: 0,
                        stop_position: None,
                        messages: Vec::new(),
                        subscription_id,
                        counter_id: 100 + recording_id as i32,
                        control_correlation_id: correlation_id,
                    },
                );
                self.signal(correlation_id, recording_id, subscription_id, 0, Signal::START);
            }
            Request::ExtendRecording {
                correlation_id,
                recording_id,
                channel,
                ..
            } => {
                let subscription_id = self.next_subscription_id;
                self.next_subscription_id += 1;
                let Some(recording) = self.recordings.get_mut(&recording_id) else {
                    return self.error(correlation_id, 5, &format!("unknown recording id: {recording_id}"));
                };
                recording.session_id = session_id_of(&channel).expect("a session id");
                recording.stop_position = None;
                recording.subscription_id = subscription_id;
                recording.control_correlation_id = correlation_id;
                let position = recording.position();
                self.ok(correlation_id, subscription_id);
                self.signal(correlation_id, recording_id, subscription_id, position, Signal::EXTEND);
            }
            Request::StopRecordingSubscription {
                correlation_id,
                subscription_id,
                ..
            } => {
                let found = self
                    .recordings
                    .values_mut()
                    .find(|r| r.subscription_id == subscription_id && r.stop_position.is_none())
                    .map(|r| {
                        let position = r.position();
                        r.stop_position = Some(position);
                        (r.recording_id, position, r.control_correlation_id)
                    });
                self.ok(correlation_id, 0);
                if let Some((id, position, corr)) = found {
                    self.signal(corr, id, subscription_id, position, Signal::STOP);
                }
            }
            Request::StopRecordingByIdentity {
                correlation_id,
                recording_id,
                ..
            } => {
                let stopped = self.recordings.get_mut(&recording_id).and_then(|r| {
                    if r.stop_position.is_none() {
                        let position = r.position();
                        r.stop_position = Some(position);
                        Some((position, r.subscription_id, r.control_correlation_id))
                    } else {
                        None
                    }
                });
                self.ok(correlation_id, i64::from(stopped.is_some()));
                if let Some((position, subscription_id, corr)) = stopped {
                    self.signal(corr, recording_id, subscription_id, position, Signal::STOP);
                }
            }
            Request::ListRecording {
                correlation_id,
                recording_id,
                ..
            } => match self.recordings.get(&recording_id) {
                Some(r) => {
                    let mut catalog = Catalog::create(4_096).expect("a catalog");
                    for _ in 0..recording_id {
                        catalog.add_recording(&descriptor(r)).expect("room");
                    }
                    let id = catalog.add_recording(&descriptor(r)).expect("room");
                    let bytes = catalog.descriptor_bytes(id).expect("the descriptor").to_vec();
                    self.responses.push_back(archive_protocol::recording_descriptor(
                        self.control_session_id,
                        correlation_id,
                        &bytes,
                    ));
                }
                None => {
                    let cs = self.control_session_id;
                    self.responses.push_back(archive_protocol::control_response(
                        cs,
                        correlation_id,
                        recording_id,
                        Code::RECORDING_UNKNOWN,
                        "",
                    ));
                }
            },
            Request::Replay {
                correlation_id,
                recording_id,
                position,
                length,
                ..
            } => {
                let Some(recording) = self.recordings.get(&recording_id) else {
                    return self.error(correlation_id, 5, &format!("unknown recording id: {recording_id}"));
                };
                let to = if length < 0 {
                    recording.stop_position.unwrap_or(recording.position())
                } else {
                    position.saturating_add(length)
                };
                let replay_session_id = self.next_replay;
                self.next_replay += 1;
                self.replays.push((replay_session_id, recording_id, position, to));
                self.ok(correlation_id, replay_session_id);
            }
            Request::StopReplay {
                correlation_id,
                replay_session_id,
                ..
            } => {
                self.replays.retain(|r| r.0 != replay_session_id);
                self.ok(correlation_id, 0);
            }
            Request::StopPosition {
                correlation_id,
                recording_id,
                ..
            } => match self.recordings.get(&recording_id) {
                Some(r) => {
                    let position = r.stop_position.unwrap_or(-1);
                    self.ok(correlation_id, position);
                }
                None => self.error(correlation_id, 5, &format!("unknown recording id: {recording_id}")),
            },
            Request::TruncateRecording {
                correlation_id,
                recording_id,
                position,
                ..
            } => match self.recordings.get_mut(&recording_id) {
                Some(r) if r.stop_position.is_some() => {
                    r.messages.retain(|(_, p)| *p <= position);
                    r.stop_position = Some(position);
                    self.ok(correlation_id, 0);
                }
                Some(_) => self.error(correlation_id, 10, "cannot truncate active recording"),
                None => self.error(correlation_id, 5, &format!("unknown recording id: {recording_id}")),
            },
            Request::StopReplication { correlation_id, .. } => self.ok(correlation_id, 0),
            Request::Replicate(r) => {
                // Answered with its id; the test plays the source archive's part.
                let replication_id = 3_000 + r.correlation_id;
                self.ok(r.correlation_id, replication_id);
            }
            other => panic!("the fake archive does not serve {other:?}"),
        }
    }

    /// A message offered on a publication the archive records.
    pub fn on_publication_message(&mut self, session_id: i32, message: &[u8], position: i64) {
        if let Some(r) = self
            .recordings
            .values_mut()
            .find(|r| r.session_id == session_id && r.stop_position.is_none())
        {
            r.messages.push((message.to_vec(), position));
        }
    }
}

fn descriptor(r: &FakeRecording) -> Descriptor {
    Descriptor {
        control_session_id: 0,
        correlation_id: 0,
        recording_id: -1,
        start_timestamp: 0,
        stop_timestamp: if r.stop_position.is_some() { 1 } else { -1 },
        start_position: r.start_position,
        stop_position: r.stop_position.unwrap_or(-1),
        initial_term_id: 0,
        segment_file_length: 128 * 1024 * 1024,
        term_buffer_length: 64 * 1024 * 1024,
        mtu_length: 1_408,
        session_id: r.session_id,
        stream_id: r.stream_id,
        stripped_channel: r.channel.clone(),
        original_channel: r.channel.clone(),
        source_identity: "aeron:ipc".into(),
    }
}

/// What the module offers, kept; counters and the toggle in memory.
#[derive(Default)]
pub struct FakePorts {
    pub archive: FakeArchive,
    pub log_session_id: Option<i32>,
    pub log_position: i64,
    pub log_refused: bool,
    pub egress_connected: HashSet<EgressId>,
    pub egress: HashMap<EgressId, Vec<Vec<u8>>>,
    pub consensus: Vec<(i32, Vec<u8>)>,
    pub to_services: Vec<Vec<u8>>,
    pub snapshot_session_id: Option<i32>,
    pub snapshot_position: i64,
    pub counters: HashMap<CounterId, i64>,
    pub toggle: i32,
    pub node_toggle: i32,
    pub next_correlation_id: i64,
    /// `node-state.dat`, written through [`Ports::write_node_state`].
    pub node_state_file: Vec<u8>,
    /// The candidate term `node-state.dat` held as each consensus message
    /// was offered, in order (`-2` with no state).
    pub candidate_term_on_disk_at_offer: Vec<i64>,
    /// The local archive's responses gone: its process died.
    pub archive_gone: bool,
}

impl FakePorts {
    pub fn new() -> Self {
        Self {
            toggle: 0,
            node_toggle: 0,
            next_correlation_id: 1_000,
            ..Self::default()
        }
    }

    pub fn counter(&self, counter: CounterId) -> i64 {
        self.counters.get(&counter).copied().unwrap_or(0)
    }

    /// What the module sent other members since last asked, decoded.
    pub fn take_consensus(&mut self) -> Vec<(i32, ConsensusMessage)> {
        std::mem::take(&mut self.consensus)
            .into_iter()
            .map(|(to, m)| (to, consensus::decode(&m).expect("a consensus message")))
            .collect()
    }

    /// Every message of the module's log, decoded.
    pub fn log_messages(&self) -> Vec<(LogMessage, i64)> {
        let Some(session_id) = self.log_session_id else {
            return Vec::new();
        };
        self.archive
            .recordings
            .values()
            .filter(|r| r.stream_id == 100 || r.session_id == session_id)
            .flat_map(|r| r.messages.iter())
            .filter_map(|(m, p)| log::decode(m).ok().map(|d| (d, *p)))
            .collect()
    }
}

impl Ports for FakePorts {
    fn log_position(&self) -> i64 {
        self.log_position
    }

    fn offer_log(&mut self, parts: &[&[u8]]) -> i64 {
        let Some(session_id) = self.log_session_id else {
            return crate::io::offer_result::NOT_CONNECTED;
        };
        if self.log_refused {
            return crate::io::offer_result::BACK_PRESSURED;
        }
        let message = parts.concat();
        self.log_position = log::position_after(self.log_position, message.len());
        let position = self.log_position;
        self.archive.on_publication_message(session_id, &message, position);
        position
    }

    fn egress_connected(&self, egress: EgressId) -> bool {
        self.egress_connected.contains(&egress)
    }

    fn egress_available_window(&self, _: EgressId) -> i64 {
        65_536
    }

    fn offer_egress(&mut self, egress: EgressId, message: &[u8]) -> Offer {
        self.egress.entry(egress).or_default().push(message.to_vec());
        Offer::Sent
    }

    fn offer_consensus(&mut self, member_id: i32, message: &[u8]) -> Offer {
        self.consensus.push((member_id, message.to_vec()));
        let on_disk = NodeState::load(&self.node_state_file).map_or(-2, |s| s.candidate_term_id);
        self.candidate_term_on_disk_at_offer.push(on_disk);
        Offer::Sent
    }

    fn write_node_state(&mut self, offset: u64, bytes: &[u8]) -> Result<(), String> {
        let end = offset as usize + bytes.len();
        if self.node_state_file.len() < end {
            return Err(format!("write past the file's end: {end}"));
        }
        self.node_state_file[offset as usize..end].copy_from_slice(bytes);
        Ok(())
    }

    fn offer_service(&mut self, message: &[u8]) -> Offer {
        self.to_services.push(message.to_vec());
        Offer::Sent
    }

    fn offer_archive(&mut self, message: &[u8]) -> Offer {
        self.archive.on_request(message);
        Offer::Sent
    }

    fn archive_publication_connected(&self) -> bool {
        true
    }

    fn archive_subscription_connected(&self) -> bool {
        !self.archive_gone
    }

    fn archive_response_channel(&self) -> Option<String> {
        Some("aeron:ipc?term-length=64k".into())
    }

    fn snapshot_position(&self) -> i64 {
        self.snapshot_position
    }

    fn offer_snapshot(&mut self, message: &[u8]) -> Offer {
        let Some(session_id) = self.snapshot_session_id else {
            return Offer::NotConnected;
        };
        self.snapshot_position = log::position_after(self.snapshot_position, message.len());
        let position = self.snapshot_position;
        self.archive.on_publication_message(session_id, message, position);
        Offer::Sent
    }

    fn find_recording_counter(&self, session_id: i32, archive_id: i64) -> Option<RecordingCounter> {
        if archive_id != self.archive.archive_id {
            return None;
        }
        self.archive.recording_of_session(session_id).map(|r| RecordingCounter {
            counter_id: r.counter_id,
            registration_id: 10_000 + i64::from(r.counter_id),
            recording_id: r.recording_id,
        })
    }

    fn find_recording_counter_by_recording(&self, recording_id: i64, archive_id: i64) -> Option<RecordingCounter> {
        if archive_id != self.archive.archive_id {
            return None;
        }
        self.archive
            .recordings
            .get(&recording_id)
            .filter(|r| r.stop_position.is_none())
            .map(|r| RecordingCounter {
                counter_id: r.counter_id,
                registration_id: 10_000 + i64::from(r.counter_id),
                recording_id: r.recording_id,
            })
    }

    fn counter_value(&self, counter_id: i32) -> i64 {
        if self.archive.counters_unset {
            return 0;
        }
        self.archive
            .recordings
            .values()
            .find(|r| r.counter_id == counter_id)
            .map_or(0, FakeRecording::position)
    }

    fn is_counter_active(&self, counter_id: i32, _: i64) -> bool {
        self.archive
            .recordings
            .values()
            .any(|r| r.counter_id == counter_id && r.stop_position.is_none())
    }

    fn set_counter(&mut self, counter: CounterId, value: i64) {
        self.counters.insert(counter, value);
    }

    fn counter_id(&self, counter: CounterId) -> i32 {
        100 + counter as i32
    }

    fn control_toggle(&self) -> i32 {
        self.toggle
    }

    fn set_control_toggle(&mut self, value: i32) {
        self.toggle = value;
    }

    fn compare_and_set_control_toggle(&mut self, expected: i32, new: i32) -> bool {
        if self.toggle == expected {
            self.toggle = new;
            return true;
        }
        false
    }

    fn node_control_toggle(&self) -> i32 {
        self.node_toggle
    }

    fn set_node_control_toggle(&mut self, value: i32) {
        self.node_toggle = value;
    }

    fn next_correlation_id(&mut self) -> i64 {
        self.next_correlation_id += 1;
        self.next_correlation_id
    }

    fn has_local_sockets(&self, _: SubscriptionId) -> bool {
        false
    }

    fn resolved_endpoint(&self, _: SubscriptionId) -> Option<String> {
        None
    }
}

/// Services acking as Java's container does: at start, each join of a log,
/// each snapshot (naming a recording), and termination.
pub struct FakeServices {
    pub count: usize,
    pub client_ids: Vec<i64>,
    next_ack_id: Vec<i64>,
    /// Positions of the snapshot actions already acked.
    snapshots_acked: HashSet<i64>,
    next_snapshot_recording: i64,
    /// The positions the module asked acks at (RequestServiceAck).
    pub ack_requests: Vec<i64>,
}

impl FakeServices {
    pub fn new(count: usize) -> Self {
        Self {
            count,
            client_ids: (0..count as i64).map(|i| 500 + i).collect(),
            next_ack_id: vec![0; count],
            snapshots_acked: HashSet::new(),
            next_snapshot_recording: 1_000,
            ack_requests: Vec::new(),
        }
    }

    fn ack(&mut self, service_id: usize, log_position: i64, relevant_id: i64) -> Vec<u8> {
        let ack_id = self.next_ack_id[service_id];
        self.next_ack_id[service_id] += 1;
        use service_ack_codec as c;
        encode!(c::ServiceAckEncoder, 0, |e| {
            e.log_position(log_position)
                .timestamp(0)
                .ack_id(ack_id)
                .relevant_id(relevant_id)
                .service_id(service_id as i32);
        })
    }

    /// The acks every service sends at start, from `log_position`.
    pub fn start(&mut self, log_position: i64) -> Vec<Vec<u8>> {
        (0..self.count)
            .map(|i| {
                let id = self.client_ids[i];
                self.ack(i, log_position, id)
            })
            .collect()
    }

    /// What the services answer to what the module sent them, and to the
    /// snapshot actions of the log they have seen up to `commit_position`.
    pub fn answer(&mut self, to_services: &[Vec<u8>], log: &[(LogMessage, i64)], commit_position: i64) -> Vec<Vec<u8>> {
        let mut answers = Vec::new();
        for message in to_services {
            let h = codec::header(message).expect("a message");
            let header = || codec::header_decoder(message);
            match h.template_id {
                join_log_codec::SBE_TEMPLATE_ID => {
                    let position = join_log_codec::JoinLogDecoder::default()
                        .header(header(), 0)
                        .log_position();
                    for i in 0..self.count {
                        let id = self.client_ids[i];
                        answers.push(self.ack(i, position, id));
                    }
                }
                request_service_ack_codec::SBE_TEMPLATE_ID => {
                    // The services keep up with the module: there already.
                    let position = request_service_ack_codec::RequestServiceAckDecoder::default()
                        .header(header(), 0)
                        .log_position();
                    self.ack_requests.push(position);
                    for i in 0..self.count {
                        answers.push(self.ack(i, position, -1));
                    }
                }
                service_termination_position_codec::SBE_TEMPLATE_ID => {
                    let d = service_termination_position_codec::ServiceTerminationPositionDecoder::default()
                        .header(header(), 0);
                    if d.ack_required() == BooleanType::TRUE {
                        let position = d.log_position();
                        for i in 0..self.count {
                            answers.push(self.ack(i, position, -1));
                        }
                    }
                }
                _ => {}
            }
        }
        for (message, position) in log {
            if *position > commit_position {
                continue;
            }
            if let LogMessage::ClusterAction {
                action: rusteron_cluster_codecs::cluster::cluster_action::ClusterAction::SNAPSHOT,
                log_position,
                ..
            } = message
                && self.snapshots_acked.insert(*log_position)
            {
                for i in 0..self.count {
                    let recording = self.next_snapshot_recording;
                    self.next_snapshot_recording += 1;
                    answers.push(self.ack(i, *log_position, recording));
                }
            }
        }
        answers
    }
}

/// A node in memory: other members are the test's, through the
/// consensus stream and the leader's log.
pub struct FakeNode {
    pub module: ConsensusModule,
    pub ports: FakePorts,
    pub services: FakeServices,
    pub cluster_time: i64,
    pub recording_log_file: Vec<u8>,
    pub errors: Vec<String>,
    pub terminated: Option<bool>,
    pub subscriptions: BTreeMap<SubscriptionId, (String, i32)>,
    /// The recovery state counter's key, while it is up.
    pub recovery_counter: Option<Vec<u8>>,
    /// Replays being delivered, by subscription: (replay session id, next message).
    replay_images: HashMap<SubscriptionId, (i64, usize)>,
    /// Replays' images deliver nothing while this holds: a replay slow to end.
    pub replays_held: bool,
    started: bool,
    /// The members the module added a consensus publication to.
    pub consensus_publications: Vec<i32>,
    /// Destinations on subscriptions, and on the log publication.
    pub destinations: Vec<(SubscriptionId, String)>,
    pub log_destinations: Vec<String>,
    /// The leader's log as it reaches a follower, not yet polled.
    pub leader_log: VecDeque<(Vec<u8>, i64)>,
}

impl FakeNode {
    pub fn new(config: Config) -> Self {
        let (state, bytes) = NodeState::create();
        let mut node_state_file = bytes;
        node_state_file.resize(node_state::FILE_LENGTH, 0);
        let services = FakeServices::new(config.service_count);
        let cluster_time = 1_700_000_000_000;
        let module = ConsensusModule::new(config, RecordingLog::new(), state, cluster_time).expect("a module");
        let mut ports = FakePorts::new();
        ports.archive = FakeArchive::default();
        ports.node_state_file = node_state_file;
        Self {
            module,
            ports,
            services,
            cluster_time,
            recording_log_file: Vec::new(),
            errors: Vec::new(),
            terminated: None,
            subscriptions: BTreeMap::new(),
            recovery_counter: None,
            replay_images: HashMap::new(),
            replays_held: false,
            started: false,
            consensus_publications: Vec::new(),
            destinations: Vec::new(),
            log_destinations: Vec::new(),
            leader_log: VecDeque::new(),
        }
    }

    /// The node restarted on its files and its archive, as after a crash.
    pub fn restart(self, config: Config) -> Self {
        let state = NodeState::load(&self.ports.node_state_file).expect("node state");
        let log = RecordingLog::load(&self.recording_log_file).expect("recording log");
        let cluster_time = self.cluster_time;
        let module = ConsensusModule::new(config, log, state, cluster_time).expect("a module");
        let mut ports = FakePorts::new();
        ports.node_state_file = self.ports.node_state_file;
        ports.archive = self.ports.archive;
        for recording in ports.archive.recordings.values_mut() {
            if recording.stop_position.is_none() {
                recording.stop_position = Some(recording.position());
            }
        }
        ports.archive.responses.clear();
        Self {
            module,
            ports,
            services: FakeServices {
                next_ack_id: vec![0; self.services.count],
                snapshots_acked: self.services.snapshots_acked,
                ..self.services
            },
            cluster_time,
            recording_log_file: self.recording_log_file,
            errors: Vec::new(),
            terminated: None,
            subscriptions: BTreeMap::new(),
            recovery_counter: None,
            replay_images: HashMap::new(),
            replays_held: false,
            started: false,
            consensus_publications: Vec::new(),
            destinations: Vec::new(),
            log_destinations: Vec::new(),
            leader_log: VecDeque::new(),
        }
    }

    /// One duty cycle `ms` milliseconds on.
    pub fn advance(&mut self, ms: i64) -> &mut Self {
        self.cluster_time += ms - 1;
        self.cycle();
        self
    }

    /// `n` duty cycles, 1 ms apart.
    pub fn run(&mut self, n: usize) -> &mut Self {
        for _ in 0..n {
            self.cycle();
        }
        self
    }

    fn cycle(&mut self) {
        self.cluster_time += 1;
        self.module.on_tick(self.cluster_time);
        self.module.do_work(&mut self.ports);
        self.carry();
    }

    /// Carries the module's actions out, and everything back, until quiet.
    pub fn carry(&mut self) {
        for _ in 0..16 {
            let mut moved = false;
            while let Some(action) = self.module.poll_action() {
                moved = true;
                self.act(action);
            }
            while let Some(response) = self.ports.archive.responses.pop_front() {
                moved = true;
                self.module.on_archive_message(&response, &mut self.ports);
            }
            if !self.started
                && self
                    .ports
                    .archive
                    .requests
                    .iter()
                    .any(|r| matches!(r, Request::ArchiveId { .. }))
            {
                // The services start once the recovery state counter is up.
                if let Some(position) = self.recovery_position() {
                    self.started = true;
                    moved = true;
                    for ack in self.services.start(position) {
                        self.module.on_service_message(&ack, &mut self.ports);
                    }
                }
            }
            let to_services = std::mem::take(&mut self.ports.to_services);
            let log = self.ports.log_messages();
            let commit = self.module.commit_position();
            for answer in self.services.answer(&to_services, &log, commit) {
                moved = true;
                self.module.on_service_message(&answer, &mut self.ports);
            }
            moved |= self.deliver_replays();
            if !moved {
                break;
            }
        }
    }

    fn recovery_position(&self) -> Option<i64> {
        self.recovery_counter
            .as_ref()
            .map(|key| i64::from_le_bytes(key[8..16].try_into().expect("8 bytes")))
    }

    fn act(&mut self, action: Action) {
        match action {
            Action::ConnectEgress { egress, .. } => {
                self.ports.egress_connected.insert(egress);
                self.module
                    .on_io(Io::EgressAdded { egress, result: Ok(()) }, &mut self.ports);
            }
            Action::DisconnectEgress { egress } => {
                self.ports.egress_connected.remove(&egress);
            }
            Action::AddLogPublication { channel, .. } => {
                assert!(channel.contains("alias=log"), "{channel}");
                // The publication starts where its channel's initial position says.
                let param = |name: &str| {
                    channel
                        .split(['?', '|'])
                        .find_map(|p| p.strip_prefix(&format!("{name}=")))
                        .and_then(|v| v.parse::<i64>().ok())
                };
                let term_length = param("term-length").unwrap_or(64 * 1024 * 1024);
                self.ports.log_position = (param("term-id").unwrap_or(0) - param("init-term-id").unwrap_or(0))
                    * term_length
                    + param("term-offset").unwrap_or(0);
                self.ports.log_session_id = Some(LOG_SESSION_ID);
                self.module.on_io(
                    Io::LogPublicationAdded {
                        session_id: LOG_SESSION_ID,
                    },
                    &mut self.ports,
                );
            }
            Action::AddLogDestination { destination } => self.log_destinations.push(destination),
            Action::AddConsensusPublication { member_id, .. } => self.consensus_publications.push(member_id),
            Action::AddDestination {
                subscription,
                destination,
            } => self.destinations.push((subscription, destination)),
            Action::RemoveDestination {
                subscription,
                destination,
            } => self.destinations.retain(|d| *d != (subscription, destination.clone())),
            Action::RemoveLogPublication => self.ports.log_session_id = None,
            Action::AddSubscription {
                subscription,
                channel,
                stream_id,
            } => {
                self.subscriptions.insert(subscription, (channel.clone(), stream_id));
                if let Some(session_id) = session_id_of(&channel)
                    && let Some(&(replay, _, from, _)) =
                        self.ports.archive.replays.iter().find(|r| r.0 as i32 == session_id)
                {
                    self.replay_images.insert(subscription, (replay, 0));
                    self.module.on_io(
                        Io::ImageAvailable {
                            subscription,
                            session_id,
                            correlation_id: 1,
                            join_position: from,
                            term_buffer_length: TERM_LENGTH,
                            source_identity: "aeron:ipc".into(),
                        },
                        &mut self.ports,
                    );
                }
            }
            Action::RemoveSubscription { subscription } => {
                self.subscriptions.remove(&subscription);
                self.replay_images.remove(&subscription);
            }
            Action::AddSnapshotPublication { .. } => {
                let session_id = SNAPSHOT_SESSION_BASE + self.ports.archive.recordings.len() as i32;
                self.ports.snapshot_session_id = Some(session_id);
                self.ports.snapshot_position = 0;
                self.module
                    .on_io(Io::SnapshotPublicationAdded { session_id }, &mut self.ports);
            }
            Action::RemoveSnapshotPublication => {
                if let Some(session_id) = self.ports.snapshot_session_id.take() {
                    let id = self
                        .ports
                        .archive
                        .recording_of_session(session_id)
                        .map(|r| r.recording_id);
                    if let Some(r) = id.and_then(|id| self.ports.archive.recordings.get_mut(&id)) {
                        r.stop_position = Some(r.position());
                    }
                }
            }
            Action::AddRecoveryStateCounter { key, .. } => self.recovery_counter = Some(key),
            Action::RemoveRecoveryStateCounter => {}
            Action::WriteRecordingLog { writes, .. } => {
                for (offset, bytes) in writes {
                    let end = offset as usize + bytes.len();
                    if self.recording_log_file.len() < end {
                        self.recording_log_file.resize(end, 0);
                    }
                    self.recording_log_file[offset as usize..end].copy_from_slice(&bytes);
                }
            }
            Action::Error { message, .. } => self.errors.push(message),
            Action::Terminate { expected } => self.terminated = Some(expected),
        }
    }

    /// A message from another member, on the consensus stream.
    pub fn consensus_in(&mut self, message: &[u8]) {
        let fragment = Fragment {
            payload: message,
            flags: UNFRAGMENTED,
            term_offset: 0,
            position: 0,
        };
        self.module.on_consensus_fragment(fragment, 1, &mut self.ports);
        self.carry();
    }

    /// The follower's log subscription, once the module added it.
    pub fn log_subscription(&self) -> Option<SubscriptionId> {
        self.subscriptions
            .iter()
            .find(|(_, (channel, _))| channel.contains("alias=log-cm"))
            .map(|(s, _)| *s)
    }

    /// The leader's log image on the follower's subscription.
    pub fn log_image(&mut self, session_id: i32, join_position: i64) {
        let subscription = self.log_subscription().expect("a log subscription");
        self.module.on_io(
            Io::ImageAvailable {
                subscription,
                session_id,
                correlation_id: 2,
                join_position,
                term_buffer_length: TERM_LENGTH,
                source_identity: "127.0.0.1:20000".into(),
            },
            &mut self.ports,
        );
        self.carry();
    }

    /// A message of the leader's log reaching this follower: recorded by
    /// its archive, and read by the module as far as it polls.
    pub fn leader_message(&mut self, session_id: i32, message: Vec<u8>, position: i64) {
        self.ports
            .archive
            .on_publication_message(session_id, &message, position);
        self.leader_log.push_back((message, position));
        self.carry();
    }

    fn deliver_leader_log(&mut self) -> bool {
        let Some(log_subscription) = self.log_subscription() else {
            return false;
        };
        let mut moved = false;
        for (subscription, poll) in self.module.polls(&self.ports) {
            let Poll::Image { limit, .. } = poll else {
                continue;
            };
            if subscription != log_subscription {
                continue;
            }
            while let Some((message, position)) = self.leader_log.front().cloned() {
                if position > limit {
                    break;
                }
                let fragment = Fragment {
                    payload: &message,
                    flags: UNFRAGMENTED,
                    term_offset: 0,
                    position,
                };
                let action = self.module.on_image_fragment(subscription, fragment, &mut self.ports);
                if action == rusteron_cluster_core::log::PollAction::Abort {
                    break;
                }
                self.leader_log.pop_front();
                moved = true;
                if action == rusteron_cluster_core::log::PollAction::Break {
                    break;
                }
            }
        }
        moved
    }

    /// Replays' messages delivered as their images would, bounded as the module asks.
    fn deliver_replays(&mut self) -> bool {
        let mut moved = self.deliver_leader_log();
        if self.replays_held {
            return moved;
        }
        for (subscription, poll) in self.module.polls(&self.ports) {
            let Poll::Image { limit, .. } = poll else {
                continue;
            };
            let Some(&(replay, next)) = self.replay_images.get(&subscription) else {
                continue;
            };
            let Some(&(_, recording_id, from, to)) = self.ports.archive.replays.iter().find(|r| r.0 == replay) else {
                continue;
            };
            let recording = &self.ports.archive.recordings[&recording_id];
            let messages: Vec<(Vec<u8>, i64)> = recording
                .messages
                .iter()
                .filter(|(_, p)| *p > from && *p <= to)
                .cloned()
                .collect();
            // After its last message the image holds what the recording does
            // up to the replay's end: a term's padding, stepped over with no
            // fragment.
            let end = to.min(recording.stop_position.unwrap_or(recording.position()));
            let mut index = next;
            let mut stopped = false;
            while let Some((message, position)) = messages.get(index) {
                if *position > limit {
                    break;
                }
                let fragment = Fragment {
                    payload: message,
                    flags: UNFRAGMENTED,
                    term_offset: 0,
                    position: *position,
                };
                let action = self.module.on_image_fragment(subscription, fragment, &mut self.ports);
                if action == rusteron_cluster_core::log::PollAction::Abort {
                    stopped = true;
                    break;
                }
                index += 1;
                moved = true;
                if action == rusteron_cluster_core::log::PollAction::Break {
                    stopped = true;
                    break;
                }
            }
            if !stopped && index == messages.len() && end <= limit {
                self.module.on_image_position(subscription, end);
            }
            if let Some(entry) = self.replay_images.get_mut(&subscription) {
                entry.1 = index;
            }
        }
        moved
    }
}
