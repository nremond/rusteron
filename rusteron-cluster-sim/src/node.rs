//! A member of the simulated cluster: its consensus module, the ports it
//! offers through, its subscriptions and their images, its archive, its
//! services and its files. The world carries what it sends to the others.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use rusteron_archive_core::conductor::Offer;
use rusteron_cluster_core::log::{Fragment, PollAction};
use rusteron_cluster_core::node_state::{self, NodeState};
use rusteron_cluster_core::position::UNFRAGMENTED;
use rusteron_cluster_core::recording_log::RecordingLog;
use rusteron_consensus_core::config::Config;
use rusteron_consensus_core::io::{
    Action, CounterId, EgressId, Io, Ports, RecordingCounter, SubscriptionId, offer_result,
};
use rusteron_consensus_core::log;
use rusteron_consensus_core::module::{
    CONSENSUS_FRAGMENT_LIMIT, ConsensusModule, INGRESS_FRAGMENT_LIMIT, LOG_FRAGMENT_LIMIT, Poll,
};
use rusteron_consensus_core::sessions::IngressImage;

use crate::archive::{Message, ReplayTarget, SimArchive, TERM_LENGTH, endpoint_of, session_id_of};
use crate::service::{ServiceSnapshot, SimService};

/// What a member sends over the network.
#[derive(Clone, Debug)]
pub enum Packet {
    Consensus {
        to_member: i32,
        bytes: Vec<u8>,
    },
    /// The log's data to an endpoint: the leader's publication, or a catch-up replay.
    Log {
        endpoint: String,
        session_id: i32,
        message: Message,
    },
    /// The log's stream is there at `position`, with nothing to send: a
    /// subscriber joins there.
    LogHeartbeat {
        endpoint: String,
        session_id: i32,
        position: i64,
    },
    /// The leader closed its log publication there.
    LogEnd {
        endpoint: String,
        session_id: i32,
        position: i64,
    },
    /// A catch-up replay to that endpoint stopped, and so revoked, at `position`.
    LogRevoke {
        endpoint: String,
        session_id: i32,
        position: i64,
    },
    Egress {
        client: usize,
        bytes: Vec<u8>,
    },
    /// A backup session's answer, to the backup's consensus endpoint.
    BackupEgress {
        bytes: Vec<u8>,
    },
}

/// The leader's log publication.
#[derive(Debug)]
pub struct LogPublication {
    pub session_id: i32,
    pub position: i64,
    pub destinations: Vec<String>,
}

/// Where a client's egress goes: a client, or the backup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EgressTarget {
    Client(usize),
    Backup,
}

#[derive(Debug)]
struct Egress {
    target: Option<EgressTarget>,
}

/// The module's ports, on the member's state.
pub struct SimPorts {
    /// Writes to `node-state.dat` the node has yet to apply.
    pub node_state_writes: Vec<(u64, Vec<u8>)>,
    pub archive: SimArchive,
    pub member_id: i32,
    pub log_publication: Option<LogPublication>,
    pub snapshot_publication: Option<(i32, i64)>,
    egress: BTreeMap<EgressId, Egress>,
    pub consensus_publications: BTreeSet<i32>,
    /// Who this member reaches now, as the world says.
    pub reachable_members: BTreeSet<i32>,
    pub reachable_clients: BTreeSet<usize>,
    /// Whether the backup is up and reachable both ways.
    pub reachable_backup: bool,
    pub counters: HashMap<CounterId, i64>,
    pub toggle: i32,
    pub node_toggle: i32,
    next_correlation_id: i64,
    pub to_services: Vec<Vec<u8>>,
    pub outbox: Vec<Packet>,
    next_session_id: i32,
}

/// A client's response channel names it: `aeron:udp?endpoint=client-N:...`.
pub fn client_of(channel: &str) -> Option<usize> {
    endpoint_of(channel)?
        .strip_prefix("client-")?
        .split(':')
        .next()?
        .parse()
        .ok()
}

impl SimPorts {
    fn new(archive: SimArchive, member_id: i32, session_seed: i32) -> Self {
        Self {
            node_state_writes: Vec::new(),
            archive,
            member_id,
            log_publication: None,
            snapshot_publication: None,
            egress: BTreeMap::new(),
            consensus_publications: BTreeSet::new(),
            reachable_members: BTreeSet::new(),
            reachable_clients: BTreeSet::new(),
            reachable_backup: false,
            counters: HashMap::new(),
            toggle: 0,
            node_toggle: 0,
            next_correlation_id: 1_000,
            to_services: Vec::new(),
            outbox: Vec::new(),
            next_session_id: session_seed,
        }
    }

    pub fn counter(&self, counter: CounterId) -> i64 {
        self.counters.get(&counter).copied().unwrap_or(0)
    }

    fn reaches(&self, target: EgressTarget) -> bool {
        match target {
            EgressTarget::Client(c) => self.reachable_clients.contains(&c),
            EgressTarget::Backup => self.reachable_backup,
        }
    }

    fn next_session_id(&mut self) -> i32 {
        self.next_session_id = self.next_session_id.wrapping_add(7_919);
        self.next_session_id
    }
}

impl Ports for SimPorts {
    fn log_position(&self) -> i64 {
        self.log_publication.as_ref().map_or(0, |p| p.position)
    }

    fn offer_log(&mut self, parts: &[&[u8]]) -> i64 {
        let Some(publication) = self.log_publication.as_mut() else {
            return offer_result::NOT_CONNECTED;
        };
        let bytes = parts.concat();
        let start = publication.position;
        let end = log::position_after(start, bytes.len());
        publication.position = end;
        let message = Message { start, end, bytes };
        let session_id = publication.session_id;
        for endpoint in &publication.destinations {
            self.outbox.push(Packet::Log {
                endpoint: endpoint.clone(),
                session_id,
                message: message.clone(),
            });
        }
        self.archive.on_data(session_id, None, &message);
        end
    }

    fn egress_connected(&self, egress: EgressId) -> bool {
        self.egress
            .get(&egress)
            .and_then(|e| e.target)
            .is_some_and(|t| self.reaches(t))
    }

    fn egress_available_window(&self, _: EgressId) -> i64 {
        65_536
    }

    fn offer_egress(&mut self, egress: EgressId, message: &[u8]) -> Offer {
        match self.egress.get(&egress).and_then(|e| e.target) {
            Some(target) if self.reaches(target) => {
                let bytes = message.to_vec();
                self.outbox.push(match target {
                    EgressTarget::Client(client) => Packet::Egress { client, bytes },
                    EgressTarget::Backup => Packet::BackupEgress { bytes },
                });
                Offer::Sent
            }
            _ => Offer::NotConnected,
        }
    }

    fn write_node_state(&mut self, offset: u64, bytes: &[u8]) -> Result<(), String> {
        // The member's file outlives its ports: the node applies the write
        // once the module's step returns, before anything it sent travels.
        self.node_state_writes.push((offset, bytes.to_vec()));
        Ok(())
    }

    fn offer_consensus(&mut self, member_id: i32, message: &[u8]) -> Offer {
        if self.consensus_publications.contains(&member_id) && self.reachable_members.contains(&member_id) {
            self.outbox.push(Packet::Consensus {
                to_member: member_id,
                bytes: message.to_vec(),
            });
            Offer::Sent
        } else {
            Offer::NotConnected
        }
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
        true
    }

    fn archive_response_channel(&self) -> Option<String> {
        Some("aeron:ipc?term-length=64k".into())
    }

    fn snapshot_position(&self) -> i64 {
        self.snapshot_publication.map_or(0, |(_, p)| p)
    }

    fn offer_snapshot(&mut self, message: &[u8]) -> Offer {
        let Some((session_id, position)) = self.snapshot_publication.as_mut() else {
            return Offer::NotConnected;
        };
        let start = *position;
        let end = log::position_after(start, message.len());
        *position = end;
        let message = Message {
            start,
            end,
            bytes: message.to_vec(),
        };
        let session_id = *session_id;
        self.archive.on_data(session_id, None, &message);
        Offer::Sent
    }

    fn find_recording_counter(&self, session_id: i32, archive_id: i64) -> Option<RecordingCounter> {
        if archive_id != self.archive.archive_id {
            return None;
        }
        self.archive
            .recordings
            .values()
            .find(|r| r.session_id == session_id && r.has_counter())
            .map(|r| RecordingCounter {
                counter_id: r.counter_id,
                registration_id: r.registration_id,
                recording_id: r.id,
            })
    }

    fn find_recording_counter_by_recording(&self, recording_id: i64, archive_id: i64) -> Option<RecordingCounter> {
        if archive_id != self.archive.archive_id {
            return None;
        }
        self.archive
            .recordings
            .get(&recording_id)
            .filter(|r| r.has_counter())
            .map(|r| RecordingCounter {
                counter_id: r.counter_id,
                registration_id: r.registration_id,
                recording_id: r.id,
            })
    }

    fn counter_value(&self, counter_id: i32) -> i64 {
        self.archive
            .recording_by_counter(counter_id)
            .map_or(0, |r| r.position())
    }

    fn is_counter_active(&self, counter_id: i32, registration_id: i64) -> bool {
        self.archive
            .recordings
            .values()
            .any(|r| r.counter_id == counter_id && r.registration_id == registration_id && r.has_counter())
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

/// What a subscription the module asked for receives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionKind {
    Consensus,
    Ingress,
    /// A replay of this member's archive, of one session.
    Replay {
        session_id: i32,
    },
    /// The leader's log, through the destinations added.
    FollowerLog {
        session_id: i32,
    },
    Other,
}

/// An image on a subscription: data from its join position, read in order.
#[derive(Debug)]
pub struct SimImage {
    pub join_position: i64,
    /// How far the module read it.
    pub position: i64,
    /// What arrived, by start position, read once contiguous.
    pub data: BTreeMap<i64, Message>,
    /// The member its data comes from.
    pub source: usize,
    pub end_of_stream: Option<i64>,
    /// How far its stream is known to go: data received, heartbeats.
    pub high_water: i64,
}

impl SimImage {
    /// Where its data stops being contiguous.
    pub fn contiguous_end(&self) -> i64 {
        let mut end = self.position;
        while let Some(m) = self.data.get(&end) {
            end = m.end;
        }
        end
    }
}

#[derive(Debug)]
pub struct SimSubscription {
    pub channel: String,
    pub stream_id: i32,
    pub kind: SubscriptionKind,
    pub destinations: Vec<String>,
    pub images: BTreeMap<i32, SimImage>,
}

/// A running member: what goes when it crashes.
pub struct Running {
    pub module: ConsensusModule,
    pub ports: SimPorts,
    pub services: Vec<SimService>,
    pub subscriptions: BTreeMap<SubscriptionId, SimSubscription>,
    pub consensus_inbox: VecDeque<(i32, Vec<u8>)>,
    /// Clients' messages, by client: delivered while the ingress is polled.
    pub ingress_inbox: VecDeque<(usize, Vec<u8>)>,
    pub recovery_key: Option<Vec<u8>>,
    pub terminated: Option<bool>,
    pub last_commit_position: i64,
    ingress_images: BTreeMap<usize, i64>,
    next_image_correlation_id: i64,
    /// Sessions whose image was revoked: its services read no further.
    revoked_sessions: Vec<i32>,
}

pub struct SimNode {
    pub index: usize,
    pub member_id: i32,
    pub config: Config,
    pub running: Option<Running>,
    /// The archive while the member is down.
    pub dormant_archive: Option<SimArchive>,
    pub recording_log_file: Vec<u8>,
    pub node_state_file: Vec<u8>,
    pub clock_offset_ms: i64,
    pub errors: Vec<String>,
    pub service_snapshots: BTreeMap<i64, ServiceSnapshot>,
    pub incarnation: u32,
    /// How many log messages its archive replays to it each millisecond.
    pub local_replay_rate: usize,
    /// How many log entries each of its services applies each millisecond.
    pub service_rate: usize,
    /// Its services stand still: they read nothing, and what the module
    /// sends them waits.
    pub services_paused: bool,
    session_seed: i32,
}

impl SimNode {
    pub fn new(index: usize, config: Config, session_seed: i32) -> Self {
        let (_, bytes) = NodeState::create();
        let mut node_state_file = bytes;
        node_state_file.resize(node_state::FILE_LENGTH, 0);
        Self {
            index,
            member_id: config.member_id,
            dormant_archive: Some(SimArchive::new(index as i64)),
            config,
            running: None,
            recording_log_file: Vec::new(),
            node_state_file,
            clock_offset_ms: 0,
            errors: Vec::new(),
            service_snapshots: BTreeMap::new(),
            incarnation: 0,
            local_replay_rate: 64,
            service_rate: 256,
            services_paused: false,
            session_seed,
        }
    }

    pub fn is_up(&self) -> bool {
        self.running.as_ref().is_some_and(|r| r.terminated.is_none())
    }

    pub fn archive(&self) -> &SimArchive {
        match &self.running {
            Some(r) => &r.ports.archive,
            None => self.dormant_archive.as_ref().expect("an archive"),
        }
    }

    pub fn archive_mut(&mut self) -> &mut SimArchive {
        match &mut self.running {
            Some(r) => &mut r.ports.archive,
            None => self.dormant_archive.as_mut().expect("an archive"),
        }
    }

    /// Starts the member on its files and archive, at `now_ms`.
    pub fn start(&mut self, now_ms: i64, random_seed: u64) {
        let mut archive = self.dormant_archive.take().expect("a dormant archive");
        archive.restart();
        let recording_log = RecordingLog::load(&self.recording_log_file).expect("the recording log");
        let node_state = NodeState::load(&self.node_state_file).expect("the node state");
        let config = Config {
            random_seed,
            ..self.config.clone()
        };
        let cluster_time = now_ms + self.clock_offset_ms;
        let module = ConsensusModule::new(config, recording_log, node_state, cluster_time).expect("a module");
        self.incarnation += 1;
        self.session_seed = self.session_seed.wrapping_mul(31).wrapping_add(self.incarnation as i32);
        let ports = SimPorts::new(archive, self.member_id, self.session_seed);
        let services = (0..self.config.service_count)
            .map(|i| SimService::new(i as i32, 500 + 100 * i64::from(self.incarnation) + i as i64))
            .collect();
        self.running = Some(Running {
            module,
            ports,
            services,
            subscriptions: BTreeMap::new(),
            consensus_inbox: VecDeque::new(),
            ingress_inbox: VecDeque::new(),
            recovery_key: None,
            terminated: None,
            last_commit_position: 0,
            ingress_images: BTreeMap::new(),
            next_image_correlation_id: 1,
            revoked_sessions: Vec::new(),
        });
    }

    /// The member's process dies: what it wrote stays, nothing else.
    pub fn crash(&mut self) {
        if let Some(running) = self.running.take() {
            self.dormant_archive = Some(running.ports.archive);
        }
    }

    pub fn cluster_time(&self, now_ms: i64) -> i64 {
        now_ms + self.clock_offset_ms
    }

    /// One duty cycle at `now_ms`: the module's work, its actions carried
    /// out, its archive's answers, the services, its polls. What it sends
    /// the others is left in its outbox.
    pub fn step(&mut self, now_ms: i64, violations: &mut Vec<String>) {
        let time = self.cluster_time(now_ms);
        let Some(running) = self.running.as_mut() else {
            return;
        };
        if running.terminated.is_some() {
            return;
        }
        running.module.on_tick(time);
        running.record_streams();
        running.deliver_archive();
        running.module.do_work(&mut running.ports);
        self.carry();
        let rate = self.local_replay_rate;
        let Some(running) = self.running.as_mut() else {
            return;
        };
        running.local_replays(rate);
        running.poll(time);
        self.carry();
        self.services(violations);
        self.carry();
        if let Some(running) = self.running.as_mut() {
            let commit = running.ports.counter(CounterId::CommitPosition);
            if commit < running.last_commit_position {
                violations.push(format!(
                    "member {} commit position went back from {} to {commit}",
                    self.member_id, running.last_commit_position
                ));
            }
            running.last_commit_position = commit;
        }
    }

    fn services(&mut self, violations: &mut Vec<String>) {
        let Some(running) = self.running.as_mut() else {
            return;
        };
        // Images gone before their end, a local replay stopped or a catch-up
        // revoked: their services read no further, paused or not.
        let mut cut: Vec<i32> = std::mem::take(&mut running.ports.archive.aborted_replays)
            .into_iter()
            .collect();
        cut.append(&mut running.revoked_sessions);
        for service in &mut running.services {
            for &session_id in &cut {
                service.cut_log(session_id);
            }
        }
        if self.services_paused {
            return;
        }
        let to_services = std::mem::take(&mut running.ports.to_services);
        let commit = running.ports.counter(CounterId::CommitPosition);
        let mut answers = Vec::new();
        for service in &mut running.services {
            answers.extend(service.step(
                self.service_rate,
                &to_services,
                running.recovery_key.as_deref(),
                &mut running.ports.archive,
                &mut self.service_snapshots,
                commit,
                self.member_id,
                violations,
            ));
        }
        for answer in answers {
            running.module.on_service_message(&answer, &mut running.ports);
        }
    }

    /// The node-state writes the module made through its ports, applied to
    /// the member's file.
    fn apply_node_state_writes(&mut self) {
        let Some(running) = self.running.as_mut() else {
            return;
        };
        for (offset, bytes) in std::mem::take(&mut running.ports.node_state_writes) {
            let end = offset as usize + bytes.len();
            self.node_state_file[offset as usize..end].copy_from_slice(&bytes);
        }
    }

    /// Carries the module's actions out, and the archive's answers back, until quiet.
    fn carry(&mut self) {
        for _ in 0..32 {
            let Some(running) = self.running.as_mut() else {
                return;
            };
            let mut moved = running.deliver_archive();
            while let Some(action) = self.running.as_mut().and_then(|r| r.module.poll_action()) {
                moved = true;
                self.act(action);
            }
            self.apply_node_state_writes();
            if !moved {
                break;
            }
        }
    }

    fn act(&mut self, action: Action) {
        let member_id = self.member_id;
        let Some(running) = self.running.as_mut() else {
            return;
        };
        match action {
            Action::ConnectEgress { egress, channel, .. } => {
                let target = client_of(&channel).map(EgressTarget::Client).or_else(|| {
                    endpoint_of(&channel)
                        .filter(|e| crate::backup::is_backup_endpoint(e))
                        .map(|_| EgressTarget::Backup)
                });
                running.ports.egress.insert(egress, Egress { target });
                let result = match target {
                    Some(t) if running.ports.reaches(t) => Ok(()),
                    _ => Err("egress not reachable".to_string()),
                };
                running
                    .module
                    .on_io(Io::EgressAdded { egress, result }, &mut running.ports);
            }
            Action::DisconnectEgress { egress } => {
                running.ports.egress.remove(&egress);
            }
            Action::AddLogPublication { channel, .. } => {
                let number = |name: &str| -> Option<i64> {
                    channel
                        .split(['?', '|'])
                        .find_map(|p| p.strip_prefix(&format!("{name}=")))
                        .and_then(|v| v.parse().ok())
                };
                let term_length = number("term-length").unwrap_or(i64::from(TERM_LENGTH));
                let position = (number("term-id").unwrap_or(0) - number("init-term-id").unwrap_or(0)) * term_length
                    + number("term-offset").unwrap_or(0);
                let session_id = running.ports.next_session_id();
                running.ports.log_publication = Some(LogPublication {
                    session_id,
                    position,
                    destinations: Vec::new(),
                });
                running.ports.archive.on_stream(session_id, None, position);
                running
                    .module
                    .on_io(Io::LogPublicationAdded { session_id }, &mut running.ports);
            }
            Action::AddLogDestination { destination } => {
                if let (Some(p), Some(endpoint)) = (running.ports.log_publication.as_mut(), endpoint_of(&destination)) {
                    p.destinations.push(endpoint);
                }
            }
            Action::RemoveLogPublication => {
                if let Some(p) = running.ports.log_publication.take() {
                    for endpoint in p.destinations {
                        running.ports.outbox.push(Packet::LogEnd {
                            endpoint,
                            session_id: p.session_id,
                            position: p.position,
                        });
                    }
                    running.ports.archive.on_stream_gone(p.session_id, None);
                }
            }
            Action::AddConsensusPublication { member_id, .. } => {
                running.ports.consensus_publications.insert(member_id);
            }
            Action::AddSubscription {
                subscription,
                channel,
                stream_id,
            } => {
                let kind = if stream_id == self.config.consensus_stream_id {
                    SubscriptionKind::Consensus
                } else if stream_id == self.config.ingress_stream_id {
                    SubscriptionKind::Ingress
                } else if stream_id == self.config.replay_stream_id {
                    SubscriptionKind::Replay {
                        session_id: session_id_of(&channel).unwrap_or(0),
                    }
                } else if channel.contains("alias=log-cm") {
                    SubscriptionKind::FollowerLog {
                        session_id: session_id_of(&channel).unwrap_or(0),
                    }
                } else {
                    SubscriptionKind::Other
                };
                running.subscriptions.insert(
                    subscription,
                    SimSubscription {
                        channel,
                        stream_id,
                        kind,
                        destinations: Vec::new(),
                        images: BTreeMap::new(),
                    },
                );
            }
            Action::RemoveSubscription { subscription } => {
                if let Some(s) = running.subscriptions.remove(&subscription)
                    && let SubscriptionKind::FollowerLog { session_id } = s.kind
                {
                    running.ports.archive.on_stream_gone(session_id, Some(&s.channel));
                }
            }
            Action::AddDestination {
                subscription,
                destination,
            } => {
                if let (Some(s), Some(endpoint)) =
                    (running.subscriptions.get_mut(&subscription), endpoint_of(&destination))
                {
                    s.destinations.push(endpoint);
                }
            }
            Action::RemoveDestination {
                subscription,
                destination,
            } => {
                if let (Some(s), Some(endpoint)) =
                    (running.subscriptions.get_mut(&subscription), endpoint_of(&destination))
                {
                    s.destinations.retain(|d| *d != endpoint);
                }
            }
            Action::AddSnapshotPublication { .. } => {
                let session_id = running.ports.next_session_id();
                running.ports.snapshot_publication = Some((session_id, 0));
                running.ports.archive.on_stream(session_id, None, 0);
                running
                    .module
                    .on_io(Io::SnapshotPublicationAdded { session_id }, &mut running.ports);
            }
            Action::RemoveSnapshotPublication => {
                if let Some((session_id, _)) = running.ports.snapshot_publication.take() {
                    running.ports.archive.on_stream_gone(session_id, None);
                }
            }
            Action::AddRecoveryStateCounter { key, .. } => running.recovery_key = Some(key),
            Action::RemoveRecoveryStateCounter => running.recovery_key = None,
            Action::WriteRecordingLog { writes, .. } => {
                for (offset, bytes) in writes {
                    let end = offset as usize + bytes.len();
                    if self.recording_log_file.len() < end {
                        self.recording_log_file.resize(end, 0);
                    }
                    self.recording_log_file[offset as usize..end].copy_from_slice(&bytes);
                }
            }
            Action::Error { message, .. } => self.errors.push(format!("member {member_id}: {message}")),
            Action::Terminate { expected } => running.terminated = Some(expected),
        }
    }

    /// Data of the log at one of this member's endpoints, from member `source`.
    pub fn on_log_data(&mut self, endpoint: &str, session_id: i32, message: Message, source: usize) {
        if let Some(running) = self.running.as_mut() {
            running.on_stream_data(endpoint, session_id, Some(message.start), Some(message), source);
        }
    }

    pub fn on_log_heartbeat(&mut self, endpoint: &str, session_id: i32, position: i64, source: usize) {
        if let Some(running) = self.running.as_mut() {
            running.on_stream_data(endpoint, session_id, Some(position), None, source);
        }
    }

    pub fn on_log_end(&mut self, endpoint: &str, session_id: i32, position: i64) {
        if let Some(running) = self.running.as_mut() {
            for s in running.subscriptions.values_mut() {
                if s.destinations.iter().any(|d| d == endpoint)
                    && let Some(image) = s.images.get_mut(&session_id)
                {
                    image.end_of_stream = Some(position);
                }
            }
        }
    }

    /// A catch-up replay to `endpoint` revoked: the image it alone feeds is
    /// dropped at once, as Aeron's driver drops a revoked image without
    /// waiting for its subscribers to read it, and its services read no
    /// further. An image the live log feeds too stays, as Aeron's does
    /// unless all its sources end.
    pub fn on_log_revoke(&mut self, endpoint: &str, session_id: i32, position: i64) {
        if let Some(running) = self.running.as_mut() {
            running.on_revoke(endpoint, session_id, position);
        }
    }

    /// Gaps in the images of the leader's log fed by `source`: (endpoint,
    /// session, from where data is missing), Aeron's NAKs.
    pub fn gaps_from(&self, source: usize) -> Vec<(String, i32, i64)> {
        let Some(r) = self.running.as_ref() else {
            return Vec::new();
        };
        let mut gaps = Vec::new();
        for s in r.subscriptions.values() {
            if !matches!(s.kind, SubscriptionKind::FollowerLog { .. }) {
                continue;
            }
            let Some(endpoint) = s.destinations.first() else {
                continue;
            };
            for (&session_id, image) in &s.images {
                let have = image.contiguous_end();
                if image.source == source && image.high_water > have {
                    gaps.push((endpoint.clone(), session_id, have));
                }
            }
        }
        gaps
    }

    /// An image of `session_id` from `source` too far behind its stream to
    /// be repaired: it times out, as Aeron's does.
    pub fn lose_image(&mut self, source: usize, session_id: i32) {
        if let Some(running) = self.running.as_mut() {
            running.close_images(
                |image| image.source == source && image.position < image.high_water,
                false,
            );
            let _ = session_id;
        }
    }

    /// The images whose data comes from member `source` go, as their
    /// connection is lost.
    pub fn lose_images_from(&mut self, source: usize) {
        if let Some(running) = self.running.as_mut() {
            running.close_images(|image| image.source == source, false);
        }
    }

    /// What the member holds, for a trace: its subscriptions and images, its
    /// recordings, its publication.
    pub fn describe(&self) -> Vec<String> {
        let mut lines = Vec::new();
        let Some(r) = self.running.as_ref() else {
            return vec![format!("member {}: down", self.member_id)];
        };
        lines.push(format!(
            "member {}: {:?} {:?} {:?} term {} commit {} counters {:?}",
            self.member_id,
            r.module.state(),
            r.module.role(),
            r.module.election_state(),
            r.module.leadership_term_id(),
            r.module.commit_position(),
            r.ports.counter(CounterId::CommitPosition)
        ));
        if let Some(p) = &r.ports.log_publication {
            lines.push(format!(
                "  log publication {} at {} to {:?}",
                p.session_id, p.position, p.destinations
            ));
        }
        for (id, s) in &r.subscriptions {
            lines.push(format!(
                "  subscription {id:?} {:?} {} destinations {:?}",
                s.kind, s.channel, s.destinations
            ));
            for (session, image) in &s.images {
                lines.push(format!(
                    "    image {session} from {} joined {} at {} data {:?}..{:?} ({}) eos {:?}",
                    image.source,
                    image.join_position,
                    image.position,
                    image.data.keys().next(),
                    image.data.values().last().map(|m| m.end),
                    image.data.len(),
                    image.end_of_stream
                ));
            }
        }
        for rec in r.ports.archive.recordings.values().filter(|r| r.stream_id == 100) {
            lines.push(format!(
                "  recording {} session {} {} start {:?} stop {:?} at {} counter {}",
                rec.id,
                rec.session_id,
                rec.channel,
                rec.start_position,
                rec.stop_position,
                rec.position(),
                rec.has_counter()
            ));
        }
        for replay in &r.ports.archive.replays {
            lines.push(format!("  replay {replay:?}"));
        }
        lines
    }

    /// Whether one of this member's subscriptions takes data at `endpoint`.
    pub fn is_subscribed_at(&self, endpoint: &str) -> bool {
        self.running.as_ref().is_some_and(|r| {
            r.subscriptions
                .values()
                .any(|s| s.destinations.iter().any(|d| d == endpoint))
        })
    }
}

impl Running {
    /// The archive's answers to the module: whether there were any.
    fn deliver_archive(&mut self) -> bool {
        let mut moved = false;
        while let Some(response) = self.ports.archive.responses.pop_front() {
            moved = true;
            self.module.on_archive_message(&response, &mut self.ports);
        }
        moved
    }

    /// The streams this member's archive may record: its publications and the
    /// images on the follower's log subscription. A recording asked for after
    /// its stream appeared starts where the stream is, as the archive's
    /// subscription joins it; then it takes what arrived and is not read yet.
    fn record_streams(&mut self) {
        if let Some(p) = &self.ports.log_publication {
            self.ports.archive.on_stream(p.session_id, None, p.position);
        }
        if let Some((session_id, position)) = self.ports.snapshot_publication {
            self.ports.archive.on_stream(session_id, None, position);
        }
        for s in self.subscriptions.values() {
            if !matches!(s.kind, SubscriptionKind::FollowerLog { .. }) {
                continue;
            }
            for (&session_id, image) in &s.images {
                self.ports
                    .archive
                    .on_stream(session_id, Some(&s.channel), image.join_position);
                while let Some(m) = self
                    .ports
                    .archive
                    .recordings
                    .values()
                    .find(|r| r.session_id == session_id && r.channel == s.channel && r.has_counter())
                    .and_then(|r| image.data.get(&r.position()).cloned())
                {
                    self.ports.archive.on_data(session_id, Some(&s.channel), &m);
                }
            }
        }
    }

    /// Data (or a heartbeat at `position`) of the stream of `session_id` at
    /// `endpoint`, to the subscriptions taking it there.
    fn on_stream_data(
        &mut self,
        endpoint: &str,
        session_id: i32,
        position: Option<i64>,
        message: Option<Message>,
        source: usize,
    ) {
        let ids: Vec<SubscriptionId> = self
            .subscriptions
            .iter()
            .filter(|(_, s)| {
                s.destinations.iter().any(|d| d == endpoint)
                    && matches!(s.kind, SubscriptionKind::FollowerLog { session_id: f } if f == session_id)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            let s = self.subscriptions.get_mut(&id).expect("present");
            let channel = s.channel.clone();
            let join = position.unwrap_or(0);
            let mut created = None;
            let image = s.images.entry(session_id).or_insert_with(|| {
                created = Some(join);
                SimImage {
                    join_position: join,
                    position: join,
                    data: BTreeMap::new(),
                    source,
                    end_of_stream: None,
                    high_water: join,
                }
            });
            if let Some(m) = &message
                && m.start >= image.position
            {
                image.data.entry(m.start).or_insert_with(|| m.clone());
                image.high_water = image.high_water.max(m.end);
            }
            if let Some(position) = position {
                image.high_water = image.high_water.max(position);
            }
            if let Some(join) = created {
                self.ports.archive.on_stream(session_id, Some(&channel), join);
                self.module.on_io(
                    Io::ImageAvailable {
                        subscription: id,
                        session_id,
                        correlation_id: id.0 as i64,
                        join_position: join,
                        term_buffer_length: TERM_LENGTH,
                        source_identity: format!("member-{source}"),
                    },
                    &mut self.ports,
                );
            }
        }
        // The archive records the image as it becomes contiguous.
        self.record_streams();
    }

    /// See [`SimNode::on_log_revoke`].
    fn on_revoke(&mut self, endpoint: &str, session_id: i32, position: i64) {
        let revoked: Vec<(SubscriptionId, String)> = self
            .subscriptions
            .iter()
            .filter(|(_, s)| {
                matches!(s.kind, SubscriptionKind::FollowerLog { .. })
                    && s.destinations.len() == 1
                    && s.destinations[0] == endpoint
                    && s.images
                        .get(&session_id)
                        .is_some_and(|image| image.join_position <= position)
            })
            .map(|(id, s)| (*id, s.channel.clone()))
            .collect();
        for (subscription, channel) in revoked {
            if let Some(s) = self.subscriptions.get_mut(&subscription) {
                s.images.remove(&session_id);
            }
            self.revoked_sessions.push(session_id);
            self.ports.archive.on_stream_gone(session_id, Some(&channel));
            self.module.on_io(
                Io::ImageUnavailable {
                    subscription,
                    session_id,
                    correlation_id: subscription.0 as i64,
                    end_of_stream: true,
                },
                &mut self.ports,
            );
        }
    }

    /// Images matching `which` closed and reported gone.
    fn close_images(&mut self, which: impl Fn(&SimImage) -> bool, end_of_stream: bool) {
        let mut gone = Vec::new();
        for (id, s) in self.subscriptions.iter_mut() {
            let sessions: Vec<i32> = s
                .images
                .iter()
                .filter(|(_, image)| which(image))
                .map(|(session, _)| *session)
                .collect();
            for session_id in sessions {
                s.images.remove(&session_id);
                gone.push((*id, session_id, s.channel.clone()));
            }
        }
        for (subscription, session_id, channel) in gone {
            self.ports.archive.on_stream_gone(session_id, Some(&channel));
            self.module.on_io(
                Io::ImageUnavailable {
                    subscription,
                    session_id,
                    correlation_id: subscription.0 as i64,
                    end_of_stream,
                },
                &mut self.ports,
            );
        }
    }

    /// The archive's replays to this member's module: their images join at
    /// the replay's start once subscribed, their data follows the recording,
    /// `rate` messages a millisecond.
    fn local_replays(&mut self, rate: usize) {
        let mut delivered = Vec::new();
        for replay in &mut self.ports.archive.replays {
            let ReplayTarget::Local { session_id } = replay.target else {
                continue;
            };
            let Some((&id, _)) = self
                .subscriptions
                .iter()
                .find(|(_, s)| s.kind == SubscriptionKind::Replay { session_id })
            else {
                continue;
            };
            delivered.push((id, session_id, replay.id));
        }
        for (subscription, session_id, replay_id) in delivered {
            let archive = &mut self.ports.archive;
            let Some(replay) = archive.replays.iter_mut().find(|r| r.id == replay_id) else {
                continue;
            };
            let Some(recording) = archive.recordings.get(&replay.recording_id) else {
                continue;
            };
            let s = self.subscriptions.get_mut(&subscription).expect("present");
            if !replay.started {
                replay.started = true;
                s.images.insert(
                    session_id,
                    SimImage {
                        join_position: replay.position,
                        position: replay.position,
                        data: BTreeMap::new(),
                        source: usize::MAX,
                        end_of_stream: None,
                        high_water: replay.position,
                    },
                );
                let join = replay.position;
                self.module.on_io(
                    Io::ImageAvailable {
                        subscription,
                        session_id,
                        correlation_id: subscription.0 as i64,
                        join_position: join,
                        term_buffer_length: TERM_LENGTH,
                        source_identity: "aeron:ipc".into(),
                    },
                    &mut self.ports,
                );
                continue;
            }
            // Its image closed at its end: gone, though the module may give the
            // replay up before it hears so, its subscription still there.
            let Some(image) = s.images.get_mut(&session_id) else {
                continue;
            };
            for _ in 0..rate {
                if replay.position >= replay.stop {
                    break;
                }
                match recording.message_at(replay.position) {
                    Some(m) if m.end <= replay.stop => {
                        image.data.insert(m.start, m.clone());
                        replay.position = m.end;
                    }
                    _ => break,
                }
            }
            if replay.stop != i64::MAX && replay.position >= replay.stop {
                image.end_of_stream = Some(replay.stop);
            }
        }
    }

    /// The module's polls: the consensus stream, the ingress, the log's image.
    fn poll(&mut self, _time: i64) {
        for (subscription, poll) in self.module.polls(&self.ports) {
            match poll {
                Poll::Consensus => {
                    for _ in 0..CONSENSUS_FRAGMENT_LIMIT {
                        let Some((from, bytes)) = self.consensus_inbox.pop_front() else {
                            break;
                        };
                        let fragment = Fragment {
                            payload: &bytes,
                            flags: UNFRAGMENTED,
                            term_offset: 0,
                            position: 0,
                        };
                        self.module.on_consensus_fragment(fragment, from, &mut self.ports);
                    }
                }
                Poll::Ingress => {
                    for _ in 0..INGRESS_FRAGMENT_LIMIT {
                        let Some((client, bytes)) = self.ingress_inbox.pop_front() else {
                            break;
                        };
                        let correlation_id = *self.ingress_images.entry(client).or_insert_with(|| {
                            self.next_image_correlation_id += 1;
                            self.next_image_correlation_id
                        });
                        let image = IngressImage {
                            correlation_id,
                            session_id: client as i32,
                            source_identity: format!("client-{client}"),
                        };
                        let fragment = Fragment {
                            payload: &bytes,
                            flags: UNFRAGMENTED,
                            term_offset: 0,
                            position: 0,
                        };
                        if self.module.on_ingress_fragment(fragment, &image, &mut self.ports) == PollAction::Abort {
                            self.ingress_inbox.push_front((client, bytes));
                            break;
                        }
                    }
                }
                Poll::Image { session_id, limit } => self.poll_image(subscription, session_id, limit),
            }
        }
    }

    fn poll_image(&mut self, subscription: SubscriptionId, session_id: i32, limit: i64) {
        let mut ended = false;
        for _ in 0..LOG_FRAGMENT_LIMIT {
            let Some(s) = self.subscriptions.get_mut(&subscription) else {
                return;
            };
            let Some(image) = s.images.get_mut(&session_id) else {
                return;
            };
            let Some(m) = image.data.get(&image.position).cloned() else {
                ended = image.end_of_stream.is_some_and(|e| image.position >= e);
                break;
            };
            if m.end > limit {
                break;
            }
            let fragment = Fragment {
                payload: &m.bytes,
                flags: UNFRAGMENTED,
                term_offset: 0,
                position: m.end,
            };
            let action = self.module.on_image_fragment(subscription, fragment, &mut self.ports);
            if action == PollAction::Abort {
                break;
            }
            if let Some(image) = self
                .subscriptions
                .get_mut(&subscription)
                .and_then(|s| s.images.get_mut(&session_id))
            {
                image.data.remove(&m.start);
                image.position = m.end;
            }
            if action == PollAction::Break {
                break;
            }
        }
        if ended {
            self.close_images_of(subscription, session_id);
        }
    }

    fn close_images_of(&mut self, subscription: SubscriptionId, session_id: i32) {
        if let Some(s) = self.subscriptions.get_mut(&subscription)
            && s.images.remove(&session_id).is_some()
        {
            let channel = s.channel.clone();
            self.ports.archive.on_stream_gone(session_id, Some(&channel));
            self.module.on_io(
                Io::ImageUnavailable {
                    subscription,
                    session_id,
                    correlation_id: subscription.0 as i64,
                    end_of_stream: true,
                },
                &mut self.ports,
            );
        }
    }

    /// A client's message to this member's ingress.
    pub fn on_ingress(&mut self, client: usize, bytes: Vec<u8>) {
        if self.subscriptions.values().any(|s| s.kind == SubscriptionKind::Ingress) {
            self.ingress_inbox.push_back((client, bytes));
        }
    }

    /// A client's connection lost: its image goes.
    pub fn lose_client(&mut self, client: usize) {
        if let Some(correlation_id) = self.ingress_images.remove(&client)
            && let Some((&subscription, _)) = self
                .subscriptions
                .iter()
                .find(|(_, s)| s.kind == SubscriptionKind::Ingress)
        {
            self.ingress_inbox.retain(|(c, _)| *c != client);
            self.module.on_io(
                Io::ImageUnavailable {
                    subscription,
                    session_id: client as i32,
                    correlation_id,
                    end_of_stream: false,
                },
                &mut self.ports,
            );
        }
    }
}
