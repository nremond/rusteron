//! The backup's state machine against fake archives and a fake member:
//! Java's `ClusterBackupAgentTest`, then each of the agent's transitions.

use std::collections::{BTreeMap, HashMap};

use rusteron_archive::control_session_adapter::{self as archive_protocol, Code, Request, Signal, error_code};
use rusteron_cluster::client::EventCode;
use rusteron_cluster::client::codec::PROTOCOL_SEMANTIC_VERSION;
use rusteron_cluster::cluster_backup_agent::{
    Action, ArchiveId, BackupCounter, ClusterBackupAgent, Config, Event, FixedCredentials, Io, Ports, ReplayStart,
    SourceType, State, replay_start_position,
};
use rusteron_cluster::consensus_adapter::{self, BackupResponse, BackupSnapshot, ConsensusMessage};
use rusteron_cluster::consensus_module_agent::ports::{Offer, RecordingCounter};
use rusteron_cluster::recording_log::{RecordingLog, SERVICE_ID, Snapshot};
use rusteron_cluster::testkit::consensus::FakeArchive;

const MEMBERS: &str = "0,h0:1,h0:2,h0:3,h0:4,h0:5|1,h1:1,h1:2,h1:3,h1:4,h1:5|2,h2:1,h2:2,h2:3,h2:4,h2:5";
const CONSENSUS_CHANNEL: &str = "aeron:udp?term-length=64k|endpoint=backup:9876";
const COMMIT_POSITION_COUNTER_ID: i32 = 77;
/// The cluster's log recording, in the member's archive.
const CLUSTER_LOG_RECORDING_ID: i64 = 9;
const START_MS: i64 = 1_000_000;

fn config() -> Config {
    Config {
        consensus_channel: CONSENSUS_CHANNEL.into(),
        cluster_consensus_endpoints: "h0:2,h1:2,h2:2".into(),
        catchup_endpoint: "backup:9877".into(),
        cool_down_interval_ns: 1_000_000_000,
        random_seed: 7,
        ..Config::default()
    }
}

/// The agent's ports: two fake archives, the publication to a member, counters.
struct TestPorts {
    local: FakeArchive,
    cluster: FakeArchive,
    local_link: bool,
    local_lost: bool,
    cluster_link: Option<(String, String)>,
    cluster_replays: Vec<Request>,
    next_replay: i64,
    consensus_publication: Option<String>,
    consensus_connected: bool,
    to_members: Vec<Vec<u8>>,
    next_correlation_id: i64,
    recording_subscription: Option<String>,
    recording_endpoint: Option<String>,
    counters: HashMap<BackupCounter, i64>,
    /// The local recordings' positions, by counter id.
    positions: HashMap<i32, i64>,
    /// Replays asked of the member's archive are left unanswered.
    hold_replays: bool,
    /// The local archive refuses recordings: an error code and message.
    refuse_recordings: Option<(i64, String)>,
    /// A recording extended, active once the replay's image arrives, as a
    /// real archive's: (recording, session, subscription, correlation).
    pending_extension: Option<(i64, i32, i64, i64)>,
}

impl TestPorts {
    fn new() -> Self {
        let archive = |archive_id, control_session_id| {
            let mut archive = FakeArchive::default();
            archive.archive_id = archive_id;
            archive.control_session_id = control_session_id;
            archive
        };
        Self {
            local: archive(42, 900),
            cluster: archive(1, 901),
            local_link: false,
            local_lost: false,
            cluster_link: None,
            cluster_replays: Vec::new(),
            next_replay: 1,
            consensus_publication: None,
            consensus_connected: true,
            to_members: Vec::new(),
            next_correlation_id: 100,
            recording_subscription: None,
            recording_endpoint: None,
            counters: HashMap::new(),
            positions: HashMap::new(),
            hold_replays: false,
            refuse_recordings: None,
            pending_extension: None,
        }
    }

    /// A request to the local archive: an extension waits for its image, a
    /// recording may be refused; the fake archive serves the rest.
    fn local_request(&mut self, message: &[u8]) {
        let request = archive_protocol::decode_request(message).expect("a request");
        let correlation_id = match &request {
            Request::StartRecording { correlation_id, .. } | Request::ExtendRecording { correlation_id, .. } => {
                *correlation_id
            }
            _ => return self.local.on_request(message),
        };
        if let Some((code, text)) = &self.refuse_recordings {
            let error = archive_protocol::control_response(900, correlation_id, *code, Code::ERROR, text);
            self.local.responses.push_back(error);
            self.local.requests.push(request);
            return;
        }
        if let Request::ExtendRecording {
            recording_id,
            ref channel,
            ..
        } = request
            && self.local.recordings.contains_key(&recording_id)
        {
            let subscription_id = 6_000 + correlation_id;
            let session_id = session_id_of(channel).expect("a session id");
            self.pending_extension = Some((recording_id, session_id, subscription_id, correlation_id));
            let ok = archive_protocol::control_response(900, correlation_id, subscription_id, Code::OK, "");
            self.local.responses.push_back(ok);
            self.local.requests.push(request);
            return;
        }
        self.local.on_request(message);
    }

    fn counter_of(&self, counter: BackupCounter) -> i64 {
        self.counters.get(&counter).copied().unwrap_or(0)
    }
}

fn session_id_of(channel: &str) -> Option<i32> {
    channel
        .split(['?', '|'])
        .find_map(|p| p.strip_prefix("session-id="))
        .and_then(|s| s.parse().ok())
}

impl Ports for TestPorts {
    fn offer_consensus(&mut self, message: &[u8]) -> Offer {
        if self.consensus_publication.is_some() && self.consensus_connected {
            self.to_members.push(message.to_vec());
            Offer::Sent
        } else {
            Offer::NotConnected
        }
    }

    fn consensus_connected(&self) -> bool {
        self.consensus_publication.is_some() && self.consensus_connected
    }

    fn offer_archive(&mut self, archive: ArchiveId, message: &[u8]) -> Offer {
        match archive {
            ArchiveId::Local if self.local_link => self.local_request(message),
            ArchiveId::Cluster if self.cluster_link.is_some() => {
                let request = archive_protocol::decode_request(message).expect("a request");
                if let Request::Replay {
                    correlation_id,
                    ref replay_channel,
                    ..
                } = request
                {
                    // As Java's archive numbers a replay: its id, then the session of its publication.
                    let session = session_id_of(replay_channel).unwrap_or(0);
                    let replay_session_id = (self.next_replay << 32) | i64::from(session as u32);
                    self.next_replay += 1;
                    self.cluster_replays.push(request);
                    if !self.hold_replays {
                        self.cluster.responses.push_back(archive_protocol::control_response(
                            self.cluster.control_session_id,
                            correlation_id,
                            replay_session_id,
                            Code::OK,
                            "",
                        ));
                        // Its image reaches the local archive, which records it.
                        if let Some((recording_id, session_id, subscription_id, corr)) = self.pending_extension.take()
                            && let Some(r) = self.local.recordings.get_mut(&recording_id)
                        {
                            r.session_id = session_id;
                            r.stop_position = None;
                            r.subscription_id = subscription_id;
                            r.control_correlation_id = corr;
                        }
                    }
                } else {
                    self.cluster.on_request(message);
                }
            }
            _ => return Offer::NotConnected,
        }
        Offer::Sent
    }

    fn archive_publication_connected(&self, archive: ArchiveId) -> bool {
        match archive {
            ArchiveId::Local => self.local_link,
            ArchiveId::Cluster => self.cluster_link.is_some(),
        }
    }

    fn archive_subscription_connected(&self, archive: ArchiveId) -> bool {
        match archive {
            ArchiveId::Local => self.local_link && !self.local_lost,
            ArchiveId::Cluster => self.cluster_link.is_some(),
        }
    }

    fn archive_response_channel(&self, archive: ArchiveId) -> Option<String> {
        match archive {
            ArchiveId::Local => Some("aeron:ipc?term-length=64k".into()),
            ArchiveId::Cluster => self.cluster_link.as_ref().map(|(_, response)| response.clone()),
        }
    }

    fn next_correlation_id(&mut self) -> i64 {
        self.next_correlation_id += 1;
        self.next_correlation_id
    }

    fn recording_subscription_endpoint(&self) -> Option<String> {
        self.recording_endpoint.clone()
    }

    fn find_recording_counter(&self, session_id: i32, archive_id: i64) -> Option<RecordingCounter> {
        if archive_id != self.local.archive_id {
            return None;
        }
        let r = self
            .local
            .recordings
            .values()
            .find(|r| r.session_id == session_id && r.stop_position.is_none())?;
        Some(RecordingCounter {
            counter_id: r.counter_id,
            registration_id: 10_000 + i64::from(r.counter_id),
            recording_id: r.recording_id,
        })
    }

    fn find_recording_counter_by_recording(&self, recording_id: i64, archive_id: i64) -> Option<RecordingCounter> {
        if archive_id != self.local.archive_id {
            return None;
        }
        let r = self.local.recordings.get(&recording_id)?;
        Some(RecordingCounter {
            counter_id: r.counter_id,
            registration_id: 10_000 + i64::from(r.counter_id),
            recording_id,
        })
    }

    fn counter_value(&self, counter_id: i32) -> i64 {
        self.positions.get(&counter_id).copied().unwrap_or(0)
    }

    fn is_counter_active(&self, _: i32, _: i64) -> bool {
        true
    }

    fn counter(&self, counter: BackupCounter) -> i64 {
        self.counter_of(counter)
    }

    fn set_counter(&mut self, counter: BackupCounter, value: i64) {
        self.counters.insert(counter, value);
    }
}

/// A backup in memory, its actions carried out as a shell would.
struct Harness {
    agent: ClusterBackupAgent,
    ports: TestPorts,
    now_ms: i64,
    actions: Vec<Action>,
    events: Vec<Event>,
    errors: Vec<(bool, String)>,
    terminated: Option<String>,
    recording_log_file: Vec<u8>,
    synced_writes: usize,
}

impl Harness {
    fn new(config: Config) -> Self {
        Self::with_log(config, Vec::new())
    }

    /// A backup restarting on the recording log it wrote.
    fn with_log(config: Config, recording_log_file: Vec<u8>) -> Self {
        let log = RecordingLog::load(&recording_log_file).expect("a recording log");
        let credentials = FixedCredentials {
            credentials: b"admin:admin".to_vec(),
            challenge_response: b"admin:CSadmin".to_vec(),
        };
        let agent = ClusterBackupAgent::new(config, log, Box::new(credentials), START_MS).expect("an agent");
        let mut h = Self {
            agent,
            ports: TestPorts::new(),
            now_ms: START_MS,
            actions: Vec::new(),
            events: Vec::new(),
            errors: Vec::new(),
            terminated: None,
            recording_log_file,
            synced_writes: 0,
        };
        h.carry();
        h
    }

    fn carry(&mut self) {
        while let Some(action) = self.agent.poll_action() {
            match &action {
                Action::AddConsensusPublication { channel, .. } => {
                    self.ports.consensus_publication = Some(channel.clone());
                }
                Action::CloseConsensusPublication => self.ports.consensus_publication = None,
                Action::AddArchive {
                    archive: ArchiveId::Local,
                    ..
                } => self.ports.local_link = true,
                Action::AddArchive {
                    archive: ArchiveId::Cluster,
                    request_channel,
                    response_channel,
                    ..
                } => {
                    self.ports.cluster.responses.clear();
                    self.ports.cluster_link = Some((request_channel.clone(), response_channel.clone()));
                }
                Action::CloseArchive {
                    archive: ArchiveId::Cluster,
                } => self.ports.cluster_link = None,
                Action::CloseArchive {
                    archive: ArchiveId::Local,
                } => self.ports.local_link = false,
                Action::AddRecordingSubscription { channel, .. } => {
                    self.ports.recording_subscription = Some(channel.clone());
                }
                Action::CloseRecordingSubscription => self.ports.recording_subscription = None,
                Action::WriteRecordingLog { writes, sync } => {
                    for (offset, bytes) in writes {
                        let end = *offset as usize + bytes.len();
                        if self.recording_log_file.len() < end {
                            self.recording_log_file.resize(end, 0);
                        }
                        self.recording_log_file[*offset as usize..end].copy_from_slice(bytes);
                    }
                    if *sync {
                        self.synced_writes += 1;
                    }
                }
                Action::Error { warn, message } => self.errors.push((*warn, message.clone())),
                Action::Terminate { reason } => self.terminated = Some(reason.clone()),
                Action::AddConsensusSubscription { .. } => {}
            }
            self.actions.push(action);
        }
        while let Some(event) = self.agent.poll_event() {
            self.events.push(event);
        }
    }

    /// The archives' answers, until none is left.
    fn deliver(&mut self) {
        loop {
            let mut moved = false;
            while let Some(m) = self.ports.local.responses.pop_front() {
                moved = true;
                self.agent.on_archive_message(ArchiveId::Local, &m, &mut self.ports);
                self.carry();
            }
            while let Some(m) = self.ports.cluster.responses.pop_front() {
                moved = true;
                self.agent.on_archive_message(ArchiveId::Cluster, &m, &mut self.ports);
                self.carry();
            }
            if !moved {
                break;
            }
        }
    }

    fn cycle(&mut self) {
        self.agent.on_tick(self.now_ms);
        self.deliver();
        self.agent.do_work(&mut self.ports);
        self.carry();
        self.deliver();
    }

    /// Duty cycles, a millisecond apart.
    fn run(&mut self, ms: i64) -> &mut Self {
        for _ in 0..ms {
            self.now_ms += 1;
            self.cycle();
        }
        self
    }

    fn state(&self) -> State {
        self.agent.state()
    }

    /// Cycles until `done`, up to `max` of them.
    fn run_until(&mut self, max: usize, what: &str, done: impl Fn(&Self) -> bool) -> &mut Self {
        for _ in 0..max {
            if done(self) {
                return self;
            }
            self.run(1);
        }
        assert!(
            done(self),
            "not {what} after {max} cycles: state {:?}, errors {:?}",
            self.state(),
            self.errors
        );
        self
    }

    /// Cycles until the backup, its archive connected, has queried a member.
    fn start(&mut self) -> &mut Self {
        for _ in 0..10 {
            if !self.ports.to_members.is_empty() {
                break;
            }
            self.run(1);
        }
        self
    }

    /// The member the publication is to.
    fn queried_member(&self) -> i32 {
        let channel = self.ports.consensus_publication.as_deref().expect("a publication");
        let endpoint = channel.split("endpoint=").nth(1).expect("an endpoint");
        endpoint[1..2].parse().expect("hN:2")
    }

    /// The queries sent to members, and other messages.
    fn take_to_members(&mut self) -> Vec<ConsensusMessage> {
        std::mem::take(&mut self.ports.to_members)
            .iter()
            .map(|m| consensus_adapter::decode(m).expect("a message"))
            .collect()
    }

    fn last_query(&mut self) -> i64 {
        let queries: Vec<i64> = self
            .take_to_members()
            .into_iter()
            .filter_map(|m| match m {
                ConsensusMessage::BackupQuery { correlation_id, .. } => Some(correlation_id),
                _ => None,
            })
            .collect();
        *queries.last().expect("a query")
    }

    fn respond(&mut self, response: &BackupResponse) {
        let bytes = consensus_adapter::backup_response(response);
        self.agent.on_consensus_message(&bytes, &mut self.ports);
        self.carry();
    }

    /// The query answered by the member queried, as a follower of `leader`.
    fn answer(&mut self, leader: i32, snapshots: Vec<BackupSnapshot>) -> BackupResponse {
        let correlation_id = self.last_query();
        let member_id = self.queried_member();
        let response = response(correlation_id, member_id, leader, snapshots);
        self.respond(&response);
        response
    }

    fn local_requests(&self) -> Vec<Request> {
        self.ports.local.requests.clone()
    }

    fn count_events(&self, which: impl Fn(&Event) -> bool) -> usize {
        self.events.iter().filter(|e| which(e)).count()
    }

    /// The live log's recording in the local archive.
    fn log_recording(&self) -> i64 {
        *self
            .ports
            .local
            .recordings
            .iter()
            .find(|(_, r)| r.stream_id == 100)
            .expect("a log recording")
            .0
    }

    /// Up to BACKING_UP from a fresh start, with the snapshots given.
    fn reach_backing_up(&mut self, snapshots: Vec<BackupSnapshot>) {
        self.start();
        let leader = (self.queried_member() + 1) % 3;
        self.answer(leader, snapshots);
        for _ in 0..200 {
            if self.state() == State::BackingUp {
                break;
            }
            self.complete_replications();
            self.run(1);
        }
        assert_eq!(self.state(), State::BackingUp, "errors {:?}", self.errors);
    }

    /// The local archive's replications end synced, each into a new recording.
    fn complete_replications(&mut self) {
        let replications: Vec<i64> = self
            .local_requests()
            .iter()
            .filter_map(|r| match r {
                Request::Replicate(r) => Some(3_000 + r.correlation_id),
                _ => None,
            })
            .collect();
        for replication_id in replications {
            if self
                .ports
                .local
                .recordings
                .values()
                .any(|r| r.control_correlation_id == replication_id)
            {
                continue;
            }
            let recording_id = 500 + replication_id;
            self.ports.local.recordings.insert(
                recording_id,
                rusteron_cluster::testkit::consensus::FakeRecording {
                    recording_id,
                    session_id: 0,
                    stream_id: 106,
                    channel: "aeron:ipc".into(),
                    start_position: 0,
                    stop_position: Some(4_096),
                    messages: Vec::new(),
                    subscription_id: -1,
                    counter_id: 700 + replication_id as i32,
                    control_correlation_id: replication_id,
                },
            );
            for signal in [Signal::EXTEND, Signal::SYNC, Signal::REPLICATE_END] {
                self.ports.local.signal(replication_id, recording_id, -1, 4_096, signal);
            }
        }
        self.deliver();
    }
}

fn response(
    correlation_id: i64,
    member_id: i32,
    leader_member_id: i32,
    snapshots: Vec<BackupSnapshot>,
) -> BackupResponse {
    BackupResponse {
        correlation_id,
        log_recording_id: CLUSTER_LOG_RECORDING_ID,
        log_leadership_term_id: 2,
        log_term_base_log_position: 1_024,
        last_leadership_term_id: 2,
        last_term_base_log_position: 1_024,
        commit_position_counter_id: COMMIT_POSITION_COUNTER_ID,
        leader_member_id,
        member_id,
        snapshots,
        cluster_members: MEMBERS.into(),
    }
}

fn snapshots(leadership_term_id: i64, log_position: i64) -> Vec<BackupSnapshot> {
    [(30, SERVICE_ID), (31, 0)]
        .into_iter()
        .map(|(recording_id, service_id)| BackupSnapshot {
            recording_id,
            leadership_term_id,
            term_base_log_position: 1_024,
            log_position,
            timestamp: 1_700_000_000_000,
            service_id,
        })
        .collect()
}

fn module_snapshot(log_position: i64) -> Snapshot {
    Snapshot {
        recording_id: 1,
        leadership_term_id: 0,
        term_base_log_position: 0,
        log_position,
        timestamp: 0,
        service_id: SERVICE_ID,
    }
}

// ---------------------------------------------------------------- Java's ClusterBackupAgentTest

/// `shouldReturnReplayStartPositionIfAlreadyExisting`.
#[test]
fn the_replay_starts_where_the_local_log_stopped() {
    assert_eq!(
        replay_start_position(Some(892_374), &[], ReplayStart::Beginning),
        892_374
    );
    assert_eq!(
        replay_start_position(Some(892_374), &[module_snapshot(1_000)], ReplayStart::LatestSnapshot),
        892_374
    );
}

/// `shouldReturnNullPositionIfLastTermIsNullAndSnapshotsIsEmpty`.
#[test]
fn the_replay_starts_at_the_log_start_without_a_term_or_snapshots() {
    assert_eq!(replay_start_position(None, &[], ReplayStart::Beginning), -1);
}

/// `shouldLargestPositionLessThanOrEqualToInitialReplayPosition`.
#[test]
fn the_replay_starts_at_the_latest_module_snapshot_when_asked() {
    let snapshots: Vec<Snapshot> = (1..=6).map(|i| module_snapshot(i * 1_000)).collect();
    assert_eq!(replay_start_position(None, &snapshots, ReplayStart::Beginning), -1);
    assert_eq!(
        replay_start_position(None, &snapshots, ReplayStart::LatestSnapshot),
        6_000
    );
    let services = [Snapshot {
        service_id: 0,
        log_position: 9_000,
        ..module_snapshot(0)
    }];
    assert_eq!(
        replay_start_position(None, &services, ReplayStart::LatestSnapshot),
        -1,
        "only the consensus module's snapshots count"
    );
}

/// `shouldRunTerminationHooksOnTermination`: the client closed under it, the
/// backup ends, its error first, then the termination its shell runs the
/// hooks on.
#[test]
fn the_client_closed_terminates_the_backup() {
    let mut h = Harness::new(config());
    h.start();
    h.agent.on_io(Io::ClientClosed, &mut h.ports);
    h.carry();
    let tail: Vec<&Action> = h.actions.iter().rev().take(2).collect();
    assert_eq!(
        tail,
        [
            &Action::Terminate {
                reason: "unexpected Aeron close".into()
            },
            &Action::Error {
                warn: false,
                message: "unexpected Aeron close".into()
            },
        ]
    );
    assert!(h.agent.is_terminated());
    assert_eq!(h.agent.do_work(&mut h.ports), 0, "no work once terminated");
}

// ---------------------------------------------------------------- transitions

#[test]
fn it_subscribes_then_queries_a_member_once_its_archive_connects() {
    let mut h = Harness::new(config());
    assert_eq!(
        h.actions[..2],
        [
            Action::AddConsensusSubscription {
                channel: CONSENSUS_CHANNEL.into(),
                stream_id: 108
            },
            Action::AddArchive {
                archive: ArchiveId::Local,
                request_channel: "aeron:ipc?term-length=64k".into(),
                request_stream_id: 10,
                response_channel: "aeron:ipc?term-length=64k".into(),
                response_stream_id: 20,
            }
        ]
    );
    assert!(
        h.ports.consensus_publication.is_none(),
        "nothing before the archive connects"
    );
    h.start();
    let deadline = h.ports.counter_of(BackupCounter::NextQueryDeadlineMs);
    assert!(
        deadline > START_MS && deadline < h.now_ms,
        "a millisecond before the start: {deadline}"
    );
    let channel = h
        .ports
        .consensus_publication
        .clone()
        .expect("a publication to a member");
    let member = h.queried_member();
    assert_eq!(channel, format!("aeron:udp?term-length=64k|endpoint=h{member}:2"));
    assert_eq!(
        h.take_to_members(),
        [ConsensusMessage::BackupQuery {
            correlation_id: h.ports.next_correlation_id,
            response_stream_id: 108,
            version: PROTOCOL_SEMANTIC_VERSION,
            log_position: i64::MAX,
            response_channel: CONSENSUS_CHANNEL.into(),
            encoded_credentials: b"admin:admin".to_vec(),
        }]
    );
    assert_eq!(h.state(), State::BackupQuery);
    h.run(100);
    assert!(h.take_to_members().is_empty(), "one query until it times out");
}

#[test]
fn an_unanswered_query_goes_to_the_next_member() {
    let mut h = Harness::new(config());
    h.start();
    let first = h.queried_member();
    h.take_to_members();
    h.run(5_000);
    assert_eq!(h.queried_member(), first, "within the response timeout");
    h.run(2);
    assert_ne!(h.queried_member(), first);
    assert_eq!(h.take_to_members().len(), 1, "queried again");
}

#[test]
fn backup_query_to_live_log_record_without_snapshots() {
    let mut h = Harness::new(config());
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, Vec::new());
    assert_eq!(h.state(), State::LiveLogRecord);
    assert_eq!(
        h.actions.last(),
        Some(&Action::AddArchive {
            archive: ArchiveId::Cluster,
            request_channel: format!("aeron:udp?endpoint=h{member}:5"),
            request_stream_id: 10,
            response_channel: "aeron:udp?endpoint=localhost:0".into(),
            response_stream_id: 20,
        }),
        "the source member's archive"
    );
    let Some(Event::BackupResponse {
        cluster_members,
        log_source_member,
        snapshots_to_retrieve,
    }) = h.events.iter().find(|e| matches!(e, Event::BackupResponse { .. }))
    else {
        panic!("no response event: {:?}", h.events);
    };
    assert_eq!(cluster_members.len(), 3);
    assert_eq!(
        cluster_members
            .iter()
            .filter(|m| m.is_leader)
            .map(|m| m.id)
            .collect::<Vec<_>>(),
        [(member + 1) % 3]
    );
    assert_eq!(log_source_member.id, member);
    assert_eq!(log_source_member.leadership_term_id, 2);
    assert!(snapshots_to_retrieve.is_empty());
}

#[test]
fn a_response_channel_is_used_to_a_member_that_has_one() {
    let mut h = Harness::new(config());
    h.start();
    let correlation_id = h.last_query();
    let member = h.queried_member();
    let mut r = response(correlation_id, member, (member + 1) % 3, Vec::new());
    r.cluster_members = (0..3)
        .map(|m| format!("{m},h{m}:1,h{m}:2,h{m}:3,h{m}:4,h{m}:5,h{m}:6"))
        .collect::<Vec<_>>()
        .join("|");
    h.respond(&r);
    let Some(Action::AddArchive { response_channel, .. }) = h.actions.last() else {
        panic!("no archive added: {:?}", h.actions.last());
    };
    assert_eq!(
        response_channel,
        &format!("aeron:udp?control-mode=response|control=h{member}:6")
    );
}

#[test]
fn backup_query_to_snapshot_retrieve_with_snapshots_not_held() {
    let mut h = Harness::new(config());
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, snapshots(2, 4_096));
    assert_eq!(h.state(), State::SnapshotRetrieve);
    let Some(Event::BackupResponse {
        snapshots_to_retrieve, ..
    }) = h.events.iter().find(|e| matches!(e, Event::BackupResponse { .. }))
    else {
        panic!("no response event");
    };
    assert_eq!(snapshots_to_retrieve.len(), 2);

    // A backup holding the same snapshots skips them.
    let mut log = RecordingLog::new();
    log.append_term(5, 2, 1_024, -1).unwrap();
    log.append_snapshot(40, 2, 1_024, 4_096, 1_700_000_000_000, 0).unwrap();
    log.append_snapshot(41, 2, 1_024, 4_096, 1_700_000_000_000, SERVICE_ID)
        .unwrap();
    let file = log.take_writes().into_iter().flat_map(|(_, b)| b).collect();
    let mut h = Harness::with_log(config(), file);
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, snapshots(2, 4_096));
    assert_eq!(h.state(), State::LiveLogRecord);
}

#[test]
fn snapshot_retrieve_to_live_log_record_once_each_copy_ends_synced() {
    let mut h = Harness::new(config());
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, snapshots(2, 4_096));
    h.run_until(50, "copying", |h| {
        h.local_requests().iter().any(|r| matches!(r, Request::Replicate(_)))
    });
    h.run(3);
    let replicates: Vec<Request> = h
        .local_requests()
        .into_iter()
        .filter(|r| matches!(r, Request::Replicate(_)))
        .collect();
    assert_eq!(replicates.len(), 1, "one at a time");
    let Request::Replicate(r) = &replicates[0] else {
        unreachable!()
    };
    assert_eq!(r.src_recording_id, 30);
    assert_eq!(r.dst_recording_id, -1);
    assert_eq!(r.src_control_channel, format!("aeron:udp?endpoint=h{member}:5"));
    assert_eq!(r.src_control_stream_id, 10);
    assert_eq!(
        r.live_destination, "",
        "no live merge: the backup copies a snapshot whole"
    );

    // The first copy ends unsynced: it is copied again.
    let first = 3_000 + r.correlation_id;
    h.ports.local.signal(first, 600, -1, 2_048, Signal::REPLICATE_END);
    h.run_until(50, "copying again", |h| {
        h.local_requests()
            .iter()
            .filter(|r| matches!(r, Request::Replicate(_)))
            .count()
            == 2
    });
    assert_eq!(
        h.local_requests()
            .iter()
            .filter(|r| matches!(r, Request::Replicate(r) if r.src_recording_id == 30))
            .count(),
        2
    );
    assert_eq!(h.state(), State::SnapshotRetrieve);
    for _ in 0..50 {
        if h.state() != State::SnapshotRetrieve {
            break;
        }
        h.complete_replications();
        h.run(1);
    }
    assert_eq!(h.state(), State::LiveLogRecord, "errors {:?}", h.errors);
    assert_eq!(
        h.local_requests()
            .iter()
            .filter(|r| matches!(r, Request::Replicate(r) if r.src_recording_id == 31))
            .count(),
        1
    );
}

#[test]
fn a_snapshot_copy_without_progress_resets_the_backup() {
    let mut h = Harness::new(config());
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, snapshots(2, 4_096));
    h.run(10_100);
    assert_eq!(h.state(), State::ResetBackup);
    assert!(h.errors.contains(&(true, "log replication has not progressed".into())));
    assert!(
        h.local_requests()
            .iter()
            .any(|r| matches!(r, Request::StopReplication { .. })),
        "the copy stopped"
    );
}

#[test]
fn live_log_record_to_live_log_replay_once_the_recording_starts() {
    let mut h = Harness::new(config());
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, Vec::new());
    h.run(1);
    assert_eq!(h.state(), State::LiveLogRecord, "until the archive answers");
    let Some(Request::StartRecording {
        channel,
        stream_id,
        location,
        auto_stop,
        ..
    }) = h
        .local_requests()
        .into_iter()
        .find(|r| matches!(r, Request::StartRecording { .. }))
    else {
        panic!("no recording started");
    };
    let session_id = session_id_of(&channel).expect("a session id");
    assert_eq!(
        channel,
        format!(
            "aeron:udp?alias=backup|cc=cubic|so-sndbuf=512k|so-rcvbuf=512k|rcv-wnd=512k|endpoint=backup:9877|session-id={session_id}"
        )
    );
    assert_eq!(stream_id, 100);
    assert_eq!(location, archive_protocol::Location::Remote);
    assert!(auto_stop);
    h.run(1);
    assert_eq!(h.state(), State::LiveLogReplay);
}

#[test]
fn a_wildcard_catchup_endpoint_is_bound_before_the_archive_records_there() {
    let mut h = Harness::new(Config {
        catchup_endpoint: "backup:0".into(),
        ..config()
    });
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, Vec::new());
    h.run_until(5, "subscribed", |h| h.ports.recording_subscription.is_some());
    h.run(3);
    let subscription = h.ports.recording_subscription.clone().expect("a subscription");
    assert!(subscription.contains("endpoint=backup:0"), "{subscription}");
    assert!(subscription.contains("|tags="), "{subscription}");
    assert!(
        !h.local_requests()
            .iter()
            .any(|r| matches!(r, Request::StartRecording { .. })),
        "nothing recorded before the port is known"
    );
    h.ports.recording_endpoint = Some("10.0.0.7:41000".into());
    h.run_until(50, "replaying", |h| !h.ports.cluster_replays.is_empty());
    let Some(Request::StartRecording { channel, .. }) = h
        .local_requests()
        .into_iter()
        .find(|r| matches!(r, Request::StartRecording { .. }))
    else {
        panic!("no recording started");
    };
    assert!(
        !channel.contains("endpoint="),
        "the recording takes the tagged subscription's: {channel}"
    );
    assert!(channel.contains("tags="), "{channel}");
    assert!(
        h.ports.recording_subscription.is_none(),
        "closed once the archive records"
    );
    let Request::Replay { replay_channel, .. } = &h.ports.cluster_replays[0] else {
        unreachable!()
    };
    assert!(replay_channel.contains("endpoint=backup:41000"), "{replay_channel}");
}

#[test]
fn live_log_replay_to_update_recording_log_once_its_counter_is_found() {
    let mut h = Harness::new(config());
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, Vec::new());
    h.run_until(50, "backing up", |h| h.state() == State::BackingUp);
    let [
        Request::Replay {
            recording_id,
            position,
            length,
            replay_stream_id,
            replay_channel,
            limit_counter_id,
            ..
        },
    ] = &h.ports.cluster_replays[..]
    else {
        panic!("one replay: {:?}", h.ports.cluster_replays);
    };
    assert_eq!(*recording_id, CLUSTER_LOG_RECORDING_ID);
    assert_eq!(*position, -1, "from the log's start");
    assert_eq!(*length, -1, "on as the log grows");
    assert_eq!(*replay_stream_id, 100);
    assert_eq!(
        *limit_counter_id,
        Some(COMMIT_POSITION_COUNTER_ID),
        "up to the commit position"
    );
    assert!(replay_channel.contains("endpoint=backup:9877"));
    // The fake archive's recording counts from its start: the replay's image joined.
    assert_eq!(h.agent.live_log_recording_id(), h.log_recording());
}

#[test]
fn update_recording_log_to_backing_up_with_the_term_and_deadline() {
    let mut h = Harness::new(config());
    h.reach_backing_up(Vec::new());
    let log = RecordingLog::load(&h.recording_log_file).unwrap();
    let recording_id = h.log_recording();
    let terms: Vec<(i64, i64, i64, i64)> = log
        .entries()
        .iter()
        .map(|e| {
            (
                e.recording_id,
                e.leadership_term_id,
                e.term_base_log_position,
                e.timestamp,
            )
        })
        .collect();
    assert_eq!(
        terms,
        [(recording_id, 2, 1_024, -1)],
        "the last term, on the local recording; no log entry without snapshots"
    );
    assert_eq!(h.synced_writes, 1);
    assert_eq!(h.agent.recording_log().entries().len(), 1);
    let deadline = h.ports.counter_of(BackupCounter::NextQueryDeadlineMs);
    assert!(deadline > h.now_ms + 3_590_000, "an hour on: {deadline}");
    assert_eq!(h.count_events(|e| matches!(e, Event::UpdatedRecordingLog { .. })), 1);
    assert_eq!(h.ports.counter_of(BackupCounter::SnapshotsRetrieved), 0);
}

#[test]
fn snapshots_copied_are_filed_with_their_local_recordings() {
    let mut h = Harness::new(config());
    h.reach_backing_up(snapshots(2, 4_096));
    let log = RecordingLog::load(&h.recording_log_file).unwrap();
    let recording_id = h.log_recording();
    let entries: Vec<(i32, i64, i64, i32)> = log
        .entries()
        .iter()
        .map(|e| (e.entry_type, e.recording_id, e.log_position, e.service_id))
        .collect();
    let module = log.get_latest_snapshot(SERVICE_ID).expect("the module's").recording_id;
    let service = log.get_latest_snapshot(0).expect("the service's").recording_id;
    assert!(module >= 500 && service >= 500, "local copies: {module} {service}");
    assert_eq!(
        entries,
        [
            (0, recording_id, -1, SERVICE_ID),
            (1, service, 4_096, 0),
            (1, module, 4_096, SERVICE_ID),
        ],
        "the log's term, then the snapshots: {entries:?}"
    );
    assert_eq!(h.ports.counter_of(BackupCounter::SnapshotsRetrieved), 1);
    let Some(Event::UpdatedRecordingLog { snapshots_retrieved }) =
        h.events.iter().find(|e| matches!(e, Event::UpdatedRecordingLog { .. }))
    else {
        panic!("no update");
    };
    assert_eq!(snapshots_retrieved.len(), 2);
}

#[test]
fn the_latest_snapshot_start_replays_from_the_module_snapshot() {
    let mut h = Harness::new(Config {
        initial_replay_start: ReplayStart::LatestSnapshot,
        ..config()
    });
    h.reach_backing_up(snapshots(2, 4_096));
    let Request::Replay { position, .. } = &h.ports.cluster_replays[0] else {
        unreachable!()
    };
    assert_eq!(*position, 4_096);
}

#[test]
fn backing_up_follows_the_live_log() {
    let mut h = Harness::new(config());
    h.reach_backing_up(Vec::new());
    let counter_id = h.ports.local.recordings[&h.log_recording()].counter_id;
    h.ports.positions.insert(counter_id, 2_048);
    h.run(1);
    h.ports.positions.insert(counter_id, 4_096);
    h.run(1);
    h.run(1);
    assert_eq!(h.ports.counter_of(BackupCounter::LiveLogPosition), 4_096);
    let progress: Vec<i64> = h
        .events
        .iter()
        .filter_map(|e| match e {
            Event::LiveLogProgress {
                log_position,
                recording_pos_counter_id,
                ..
            } => {
                assert_eq!(*recording_pos_counter_id, counter_id);
                Some(*log_position)
            }
            _ => None,
        })
        .collect();
    assert_eq!(progress, [2_048, 4_096]);
    h.run(20_000);
    assert_eq!(h.state(), State::BackingUp, "no stall once the counter is found");
}

#[test]
fn backing_up_to_backup_query_at_the_deadline_then_back_without_a_new_recording() {
    let mut h = Harness::new(config());
    h.reach_backing_up(Vec::new());
    let member = h.queried_member();
    let requests = h.local_requests().len();
    // ClusterTool's nextBackupQueryDeadlineMs.
    h.ports.counters.insert(BackupCounter::NextQueryDeadlineMs, h.now_ms);
    h.run(2);
    assert_eq!(h.state(), State::BackupQuery);
    assert_eq!(h.queried_member(), member, "the same member, still connected");
    let correlation_id = h.last_query();
    let mut r = response(correlation_id, member, (member + 1) % 3, Vec::new());
    r.last_leadership_term_id = 3;
    r.last_term_base_log_position = 8_192;
    h.respond(&r);
    h.run_until(10, "backing up", |h| h.state() == State::BackingUp);
    assert_eq!(h.ports.cluster_replays.len(), 1, "the replay goes on");
    assert_eq!(h.local_requests().len(), requests, "the recording goes on");
    let terms: Vec<(i64, i64)> = RecordingLog::load(&h.recording_log_file)
        .unwrap()
        .entries()
        .iter()
        .map(|e| (e.leadership_term_id, e.log_position))
        .collect();
    assert_eq!(
        terms,
        [(2, 8_192), (3, -1)],
        "the new term filed, the last one ended at its base"
    );
}

#[test]
fn a_stall_resets_the_backup_which_queries_again_after_its_cool_down() {
    let mut h = Harness::new(config());
    h.ports.consensus_connected = false;
    h.run(9_990);
    assert_eq!(h.state(), State::BackupQuery);
    h.run_until(20, "reset", |h| h.state() == State::ResetBackup);
    assert!(h.events.contains(&Event::PossibleFailure {
        message: "progress has stalled".into()
    }));
    assert!(h.errors.is_empty(), "a stall is reported, not logged");
    h.run(1);
    assert!(h.ports.consensus_publication.is_none(), "reset");
    h.run(998);
    assert_eq!(h.state(), State::ResetBackup, "cooling down");
    h.run_until(3, "querying again", |h| h.state() == State::BackupQuery);
    assert_eq!(h.count_events(|e| *e == Event::BackupQuery), 1);
}

/// Not Java's: the live log standing still while backing up, a reset.
fn watched() -> Config {
    Config {
        live_log_progress_timeout_ns: 5_000_000_000,
        ..config()
    }
}

#[test]
fn a_live_log_standing_still_resets_the_backup_when_watched() {
    let mut h = Harness::new(watched());
    h.reach_backing_up(Vec::new());
    let counter_id = h.ports.local.recordings[&h.log_recording()].counter_id;
    h.ports.positions.insert(counter_id, 2_048);
    h.run(1);
    // The recording open, its position still: Java's backup waits for good.
    h.run(4_999);
    assert_eq!(h.state(), State::BackingUp);
    h.run_until(2, "reset", |h| h.state() == State::ResetBackup);
    assert!(h.events.contains(&Event::PossibleFailure {
        message: "the live log has not progressed".into()
    }));
    h.run(1);
    assert!(
        h.local_requests()
            .iter()
            .any(|r| matches!(r, Request::StopRecordingSubscription { .. })),
        "the recording stopped"
    );
    assert!(
        h.ports
            .cluster
            .requests
            .iter()
            .any(|r| matches!(r, Request::StopReplay { .. })),
        "the replay stopped"
    );
    assert!(h.errors.is_empty(), "reported, not logged");
}

#[test]
fn a_live_log_moving_on_is_watched_without_a_reset() {
    let mut h = Harness::new(watched());
    h.reach_backing_up(Vec::new());
    let counter_id = h.ports.local.recordings[&h.log_recording()].counter_id;
    for second in 1..=30 {
        h.ports.positions.insert(counter_id, second * 1_024);
        h.run(1_000);
    }
    assert_eq!(h.state(), State::BackingUp);
}

#[test]
fn a_query_while_backing_up_leaves_the_live_logs_clock_running() {
    let mut h = Harness::new(watched());
    h.reach_backing_up(Vec::new());
    let member = h.queried_member();
    h.run(3_000);
    h.ports.counters.insert(BackupCounter::NextQueryDeadlineMs, h.now_ms);
    h.run(2);
    assert_eq!(h.state(), State::BackupQuery);
    let correlation_id = h.last_query();
    h.respond(&response(correlation_id, member, (member + 1) % 3, Vec::new()));
    h.run_until(10, "backing up", |h| h.state() == State::BackingUp);
    // 5 s after the live log began, not after the query.
    h.run_until(2_100, "reset", |h| h.state() == State::ResetBackup);
}

#[test]
fn with_no_snapshot_copies_a_response_listing_snapshots_goes_to_the_live_log() {
    let mut h = Harness::new(Config {
        retrieve_snapshots: false,
        ..config()
    });
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, snapshots(2, 4_096));
    assert_eq!(h.state(), State::LiveLogRecord);
    let Some(Event::BackupResponse {
        snapshots_to_retrieve, ..
    }) = h.events.iter().find(|e| matches!(e, Event::BackupResponse { .. }))
    else {
        panic!("no response event");
    };
    assert!(snapshots_to_retrieve.is_empty());
    h.run_until(200, "backing up", |h| h.state() == State::BackingUp);
    let snapshots_filed = RecordingLog::load(&h.recording_log_file)
        .unwrap()
        .entries()
        .iter()
        .filter(|e| e.entry_type != 0)
        .count();
    assert_eq!(snapshots_filed, 0, "terms only");
}

#[test]
fn a_leader_is_no_source_for_a_follower_backup() {
    let mut h = Harness::new(Config {
        source_type: SourceType::Follower,
        ..config()
    });
    h.start();
    let member = h.queried_member();
    h.answer(member, Vec::new());
    assert_eq!(h.state(), State::ResetBackup);
    assert!(h.ports.consensus_publication.is_none());
    h.run_until(1_010, "querying again", |h| h.state() == State::BackupQuery);
    // A closed publication is kept as Java's group keeps it: the next member once the query times out.
    h.run_until(5_010, "a publication", |h| h.ports.consensus_publication.is_some());
    assert_ne!(h.queried_member(), member, "the leader excluded");
}

#[test]
fn only_the_leader_is_a_source_for_a_leader_backup() {
    let mut h = Harness::new(Config {
        source_type: SourceType::Leader,
        ..config()
    });
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, Vec::new());
    assert_eq!(h.state(), State::ResetBackup);
    let mut h = Harness::new(Config {
        source_type: SourceType::Leader,
        ..config()
    });
    h.start();
    let member = h.queried_member();
    h.answer(member, Vec::new());
    assert_eq!(h.state(), State::LiveLogRecord);
}

#[test]
fn a_member_without_its_id_resets_the_backup_with_a_warning() {
    let mut h = Harness::new(config());
    h.start();
    let correlation_id = h.last_query();
    h.respond(&response(correlation_id, -1, 0, Vec::new()));
    assert_eq!(h.state(), State::ResetBackup);
    assert_eq!(
        h.errors,
        [(
            true,
            "onBackupResponse(): memberId is null, retrying for compatible node".into()
        )]
    );
}

#[test]
fn a_redirect_resets_the_backup_and_excludes_the_member() {
    let mut h = Harness::new(config());
    h.start();
    let correlation_id = h.last_query();
    let member = h.queried_member();
    let event = session_event(correlation_id, EventCode::REDIRECT, "");
    h.agent.on_consensus_message(&event, &mut h.ports);
    h.carry();
    assert_eq!(h.state(), State::ResetBackup);
    h.run_until(6_100, "a publication", |h| h.ports.consensus_publication.is_some());
    assert_ne!(h.queried_member(), member);
}

#[test]
fn an_authentication_rejected_is_an_error_and_resets() {
    let mut h = Harness::new(config());
    h.start();
    let correlation_id = h.last_query();
    let event = session_event(correlation_id, EventCode::AUTHENTICATION_REJECTED, "bad credentials");
    h.agent.on_consensus_message(&event, &mut h.ports);
    h.carry();
    assert_eq!(h.state(), State::ResetBackup);
    assert_eq!(h.errors, [(false, "AUTHENTICATION_REJECTED: bad credentials".into())]);
    let stale = session_event(correlation_id - 1, EventCode::ERROR, "old");
    h.agent.on_consensus_message(&stale, &mut h.ports);
    h.carry();
    assert_eq!(h.errors.len(), 1, "another query's event ignored");
}

#[test]
fn a_challenge_is_answered_and_the_response_then_awaited() {
    let mut h = Harness::new(config());
    h.start();
    h.last_query();
    let challenge = challenge(8, 1234, b"prove it");
    h.agent.on_consensus_message(&challenge, &mut h.ports);
    h.carry();
    let answers = h.take_to_members();
    let [
        ConsensusMessage::ChallengeResponse {
            correlation_id,
            cluster_session_id,
        },
    ] = &answers[..]
    else {
        panic!("one challenge response: {answers:?}");
    };
    assert_eq!(*cluster_session_id, 1234);
    let member = h.queried_member();
    h.respond(&response(*correlation_id, member, (member + 1) % 3, Vec::new()));
    assert_eq!(
        h.state(),
        State::LiveLogRecord,
        "the response to the challenge's answer taken"
    );
}

#[test]
fn the_live_log_counter_gone_resets_and_stops_the_recording_and_replay() {
    let mut h = Harness::new(config());
    h.reach_backing_up(Vec::new());
    let counter_id = h.ports.local.recordings[&h.log_recording()].counter_id;
    h.agent.on_io(
        Io::CounterUnavailable {
            counter_id: counter_id + 1,
        },
        &mut h.ports,
    );
    assert_eq!(h.state(), State::BackingUp, "another counter");
    h.agent.on_io(Io::CounterUnavailable { counter_id }, &mut h.ports);
    h.carry();
    assert_eq!(h.state(), State::ResetBackup);
    h.run(1);
    assert!(
        h.local_requests()
            .iter()
            .any(|r| matches!(r, Request::StopRecordingSubscription { .. })),
        "the recording stopped"
    );
    assert!(
        h.ports
            .cluster
            .requests
            .iter()
            .any(|r| matches!(r, Request::StopReplay { .. })),
        "the replay stopped"
    );
    assert!(
        h.ports
            .cluster
            .requests
            .iter()
            .any(|r| matches!(r, Request::CloseSession { .. }))
    );
    assert!(h.ports.cluster_link.is_none(), "the member's archive let go");
    assert!(h.errors.is_empty());
}

#[test]
fn a_restarted_backup_extends_its_recording_from_where_it_stopped() {
    let mut h = Harness::new(config());
    h.reach_backing_up(Vec::new());
    let recording_id = h.log_recording();
    let mut local = std::mem::take(&mut h.ports.local);
    let file = h.recording_log_file.clone();
    h.agent.close(&mut h.ports);
    // The local archive stopped the recording at 8 KiB.
    let r = local.recordings.get_mut(&recording_id).unwrap();
    r.stop_position = Some(8_192);
    local.responses.clear();
    local.requests.clear();

    let mut h = Harness::with_log(config(), file);
    h.ports.local = local;
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, Vec::new());
    h.run_until(50, "backing up", |h| h.state() == State::BackingUp);
    let requests = h.local_requests();
    assert!(
        requests
            .iter()
            .any(|r| matches!(r, Request::ExtendRecording { recording_id: id, .. } if *id == recording_id)),
        "{requests:?}"
    );
    assert!(
        requests
            .iter()
            .any(|r| matches!(r, Request::StopPosition { recording_id: id, .. } if *id == recording_id))
    );
    let Request::Replay { position, .. } = &h.ports.cluster_replays[0] else {
        unreachable!()
    };
    assert_eq!(*position, 8_192, "where the local recording stopped");
    assert_eq!(h.state(), State::BackingUp);
    assert_eq!(h.agent.live_log_recording_id(), recording_id);
}

#[test]
fn the_replay_starts_at_the_local_recordings_stop_position() {
    let mut log = RecordingLog::new();
    log.append_term(0, 2, 1_024, -1).unwrap();
    let file = log.take_writes().into_iter().flat_map(|(_, b)| b).collect();
    let mut h = Harness::with_log(config(), file);
    h.ports.local.recordings.insert(
        0,
        rusteron_cluster::testkit::consensus::FakeRecording {
            recording_id: 0,
            session_id: 0,
            stream_id: 100,
            channel: "aeron:udp?endpoint=backup:9877".into(),
            start_position: 0,
            stop_position: Some(12_288),
            messages: Vec::new(),
            subscription_id: 1,
            counter_id: 100,
            control_correlation_id: 0,
        },
    );
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, Vec::new());
    h.run_until(50, "backing up", |h| h.state() == State::BackingUp);
    let Request::Replay { position, .. } = &h.ports.cluster_replays[0] else {
        panic!("no replay: {:?}", h.local_requests());
    };
    assert_eq!(*position, 12_288);
    assert_eq!(h.state(), State::BackingUp);
}

#[test]
fn a_replay_refused_resets_the_backup() {
    let mut h = Harness::new(config());
    h.ports.hold_replays = true;
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, Vec::new());
    h.run_until(50, "replaying", |h| !h.ports.cluster_replays.is_empty());
    assert_eq!(h.state(), State::LiveLogReplay);
    let Request::Replay { correlation_id, .. } = h.ports.cluster_replays[0] else {
        unreachable!()
    };
    h.ports.cluster.responses.push_back(archive_protocol::control_response(
        901,
        correlation_id,
        error_code::MAX_REPLAYS,
        Code::ERROR,
        "max concurrent replays reached",
    ));
    h.run(1);
    assert_eq!(h.state(), State::ResetBackup);
    assert_eq!(
        h.errors,
        [(
            false,
            "Live log replay failed (replaySessionId=-1): errorMessage=max concurrent replays reached, errorCode=MAX_REPLAYS"
                .into()
        )]
    );
}

#[test]
fn an_error_from_the_members_archive_while_backing_up_resets_with_a_warning() {
    let mut h = Harness::new(config());
    h.reach_backing_up(Vec::new());
    h.ports.cluster.responses.push_back(archive_protocol::control_response(
        901,
        12_345,
        error_code::GENERIC,
        Code::ERROR,
        "replay aborted",
    ));
    h.run(1);
    assert_eq!(h.state(), State::ResetBackup);
    assert_eq!(h.errors, [(true, "cluster archive error: replay aborted".into())]);
}

#[test]
fn the_local_archive_lost_ends_the_backup() {
    let mut h = Harness::new(config());
    h.start();
    h.ports.local_lost = true;
    h.run(20);
    assert_eq!(h.terminated.as_deref(), Some("local archive not connected"));
}

#[test]
fn a_full_local_archive_ends_the_backup() {
    let mut h = Harness::new(config());
    h.ports.refuse_recordings = Some((error_code::STORAGE_SPACE, "insufficient usable storage".into()));
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, Vec::new());
    h.run(3);
    assert_eq!(h.terminated.as_deref(), Some("insufficient usable storage"));
}

#[test]
fn a_recording_refused_resets_the_backup() {
    let mut h = Harness::new(config());
    h.ports.refuse_recordings = Some((error_code::GENERIC, "no such channel".into()));
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, Vec::new());
    h.run(3);
    assert_eq!(h.state(), State::ResetBackup);
    assert_eq!(h.errors, [(false, "no such channel".into())]);
}

#[test]
fn a_member_that_cannot_be_reached_resets_the_backup() {
    let mut h = Harness::new(config());
    h.start();
    h.agent.on_io(
        Io::AddFailed {
            message: "unknown host: node1".into(),
        },
        &mut h.ports,
    );
    h.carry();
    assert_eq!(h.state(), State::ResetBackup);
    assert_eq!(h.errors, [(false, "unknown host: node1".into())]);
    assert!(h.events.contains(&Event::PossibleFailure {
        message: "unknown host: node1".into()
    }));
}

#[test]
fn close_stops_the_recording_and_the_replay() {
    let mut h = Harness::new(config());
    h.reach_backing_up(Vec::new());
    h.agent.close(&mut h.ports);
    h.carry();
    assert_eq!(h.state(), State::Closed);
    assert_eq!(h.ports.counter_of(BackupCounter::State), 7);
    assert!(
        h.local_requests()
            .iter()
            .any(|r| matches!(r, Request::StopRecordingSubscription { .. }))
    );
    assert!(
        h.ports
            .cluster
            .requests
            .iter()
            .any(|r| matches!(r, Request::StopReplay { .. }))
    );
    assert!(
        h.local_requests()
            .iter()
            .any(|r| matches!(r, Request::CloseSession { .. }))
    );
    assert!(
        h.ports
            .cluster
            .requests
            .iter()
            .any(|r| matches!(r, Request::CloseSession { .. }))
    );
    assert!(h.agent.is_terminated());
}

#[test]
fn the_state_counter_follows_every_transition() {
    let mut h = Harness::new(config());
    let mut seen = BTreeMap::new();
    h.start();
    let member = h.queried_member();
    h.answer((member + 1) % 3, snapshots(2, 4_096));
    for _ in 0..20 {
        h.complete_replications();
        h.run(1);
        seen.insert(h.ports.counter_of(BackupCounter::State), ());
    }
    assert_eq!(h.state(), State::BackingUp);
    assert_eq!(
        State::from_code(h.ports.counter_of(BackupCounter::State)),
        Some(State::BackingUp)
    );
    assert!(seen.contains_key(&(State::BackingUp as i64)));
}

fn session_event(correlation_id: i64, code: EventCode, detail: &str) -> Vec<u8> {
    use rusteron_cluster::codecs::cluster::{WriteBuf, message_header_codec, session_event_codec};
    let header = message_header_codec::ENCODED_LENGTH;
    let mut buf = vec![0u8; header + session_event_codec::SBE_BLOCK_LENGTH as usize + 4 + detail.len()];
    let mut e = session_event_codec::SessionEventEncoder::default()
        .wrap(WriteBuf::new(&mut buf), header)
        .header(0)
        .parent()
        .expect("encoder");
    e.cluster_session_id(-1)
        .correlation_id(correlation_id)
        .leadership_term_id(2)
        .leader_member_id(0)
        .code(code)
        .version(PROTOCOL_SEMANTIC_VERSION)
        .leader_heartbeat_timeout_ns(-1)
        .detail(detail.as_bytes());
    buf
}

fn challenge(correlation_id: i64, cluster_session_id: i64, encoded_challenge: &[u8]) -> Vec<u8> {
    use rusteron_cluster::codecs::cluster::{WriteBuf, challenge_codec, message_header_codec};
    let header = message_header_codec::ENCODED_LENGTH;
    let mut buf = vec![0u8; header + challenge_codec::SBE_BLOCK_LENGTH as usize + 4 + encoded_challenge.len()];
    let mut e = challenge_codec::ChallengeEncoder::default()
        .wrap(WriteBuf::new(&mut buf), header)
        .header(0)
        .parent()
        .expect("encoder");
    e.correlation_id(correlation_id)
        .cluster_session_id(cluster_session_id)
        .encoded_challenge(encoded_challenge);
    buf
}
