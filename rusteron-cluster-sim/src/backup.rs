//! A cluster backup beside the members: Rust's `ClusterBackupAgent` on an
//! archive of its own, querying members on the consensus stream, copying
//! their snapshots, and recording the log their archives replay to its
//! catch-up endpoint, bounded by their commit positions. Its process can
//! crash and restart on what it wrote, and its links can be cut.

use std::collections::{BTreeSet, HashMap, VecDeque};

use rusteron_cluster::cluster_backup_agent::{
    Action, ArchiveConfig, ArchiveId, BackupCounter, ClusterBackupAgent, Config, Event, FixedCredentials, Io, Ports,
    SourceType, State,
};
use rusteron_cluster::consensus_module_agent::ports::{Offer, RecordingCounter};
use rusteron_cluster::recording_log::RecordingLog;

use crate::archive::{Message, SimArchive, endpoint_of};
use crate::member_of_endpoint;

/// The backup's clock: epoch-like milliseconds, as Java's backup reads them.
pub const EPOCH_MS: i64 = 1_700_000_000_000;
/// The backup's archive id.
const ARCHIVE_ID: i64 = 100;
/// The session id the members see the backup's consensus publication as.
pub const CONSENSUS_SESSION_ID: i32 = 100;

/// The backup's endpoints: `backup:100<kind>`.
pub fn backup_endpoint(kind: u32) -> String {
    format!("backup:100{kind}")
}

pub fn is_backup_endpoint(endpoint: &str) -> bool {
    endpoint.starts_with("backup:")
}

/// What the backup sends a member.
#[derive(Clone, Debug)]
pub enum BackupPacket {
    Consensus {
        to_member: usize,
        bytes: Vec<u8>,
    },
    /// A request to the member's archive, of the control session `session`.
    ArchiveRequest {
        to_member: usize,
        session: i64,
        bytes: Vec<u8>,
    },
}

/// The backup's ports, on its process' state.
pub struct SimBackupPorts {
    pub archive: SimArchive,
    /// The member the consensus publication goes to.
    consensus_to: Option<usize>,
    /// The member whose archive the backup's cluster session is with, and the session.
    cluster_archive: Option<(usize, i64)>,
    /// Members this process reaches both ways, as the world says.
    pub reachable_members: BTreeSet<usize>,
    pub outbox: Vec<BackupPacket>,
    pub counters: HashMap<BackupCounter, i64>,
    next_correlation_id: i64,
}

impl SimBackupPorts {
    pub fn counter(&self, counter: BackupCounter) -> i64 {
        self.counters.get(&counter).copied().unwrap_or(0)
    }

    fn reaches(&self, member: Option<usize>) -> bool {
        member.is_some_and(|m| self.reachable_members.contains(&m))
    }

    /// The member and session of the cluster archive's link.
    pub fn cluster_archive(&self) -> Option<(usize, i64)> {
        self.cluster_archive
    }
}

impl Ports for SimBackupPorts {
    fn offer_consensus(&mut self, message: &[u8]) -> Offer {
        match self.consensus_to {
            Some(to_member) if self.reaches(Some(to_member)) => {
                self.outbox.push(BackupPacket::Consensus {
                    to_member,
                    bytes: message.to_vec(),
                });
                Offer::Sent
            }
            _ => Offer::NotConnected,
        }
    }

    fn consensus_connected(&self) -> bool {
        self.reaches(self.consensus_to)
    }

    fn offer_archive(&mut self, archive: ArchiveId, message: &[u8]) -> Offer {
        match archive {
            ArchiveId::Local => {
                self.archive.on_request(message);
                Offer::Sent
            }
            ArchiveId::Cluster => match self.cluster_archive {
                Some((to_member, session)) if self.reaches(Some(to_member)) => {
                    self.outbox.push(BackupPacket::ArchiveRequest {
                        to_member,
                        session,
                        bytes: message.to_vec(),
                    });
                    Offer::Sent
                }
                _ => Offer::NotConnected,
            },
        }
    }

    fn archive_publication_connected(&self, archive: ArchiveId) -> bool {
        match archive {
            ArchiveId::Local => true,
            ArchiveId::Cluster => self.reaches(self.cluster_archive.map(|(m, _)| m)),
        }
    }

    fn archive_subscription_connected(&self, archive: ArchiveId) -> bool {
        self.archive_publication_connected(archive)
    }

    fn archive_response_channel(&self, archive: ArchiveId) -> Option<String> {
        Some(match archive {
            ArchiveId::Local => "aeron:ipc?term-length=64k".into(),
            ArchiveId::Cluster => format!("aeron:udp?endpoint={}", backup_endpoint(7)),
        })
    }

    fn next_correlation_id(&mut self) -> i64 {
        self.next_correlation_id += 1;
        self.next_correlation_id
    }

    fn recording_subscription_endpoint(&self) -> Option<String> {
        None
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

    fn counter(&self, counter: BackupCounter) -> i64 {
        SimBackupPorts::counter(self, counter)
    }

    fn set_counter(&mut self, counter: BackupCounter, value: i64) {
        self.counters.insert(counter, value);
    }
}

/// The backup's process, while it runs.
pub struct RunningBackup {
    pub agent: ClusterBackupAgent,
    pub ports: SimBackupPorts,
    /// What members sent it, delivered as its subscription is polled.
    pub consensus_inbox: VecDeque<Vec<u8>>,
    /// The member archive's responses, by session.
    pub archive_inbox: VecDeque<(i64, Vec<u8>)>,
    /// Ended on its own: why.
    pub terminated: Option<String>,
    /// The live log position as last seen, which never goes back.
    pub last_live_log_position: i64,
}

/// The backup: its process, its files and its archive.
pub struct SimBackup {
    pub running: Option<RunningBackup>,
    dormant_archive: Option<SimArchive>,
    pub recording_log_file: Vec<u8>,
    pub errors: Vec<String>,
    /// What it did, for a trace: its queries, the answers, its failures.
    pub notes: Vec<String>,
    pub incarnation: u32,
    config: Config,
    next_session: i64,
}

/// The backup's configuration in the simulation: Java's, with timeouts
/// short enough for the run, and the source type the seed picked.
pub fn config(members: usize, source_type: SourceType, endpoint: impl Fn(usize, u32) -> String) -> Config {
    Config {
        consensus_channel: format!("aeron:udp?term-length=64k|endpoint={}", backup_endpoint(2)),
        cluster_consensus_endpoints: (0..members).map(|m| endpoint(m, 2)).collect::<Vec<_>>().join(","),
        catchup_endpoint: backup_endpoint(4),
        catchup_channel: "aeron:udp?term-length=64k".into(),
        backup_interval_ns: 3_000_000_000,
        response_timeout_ns: 1_000_000_000,
        progress_timeout_ns: 4_000_000_000,
        cool_down_interval_ns: 1_000_000_000,
        replication_progress_timeout_ns: 4_000_000_000,
        source_type,
        cluster_archive: ArchiveConfig {
            request_channel: "aeron:udp?term-length=64k".into(),
            request_stream_id: 10,
            response_channel: format!("aeron:udp?endpoint={}", backup_endpoint(7)),
            response_stream_id: 20,
        },
        ..Config::default()
    }
}

impl SimBackup {
    pub fn new(config: Config) -> Self {
        Self {
            running: None,
            dormant_archive: Some(SimArchive::new(ARCHIVE_ID)),
            recording_log_file: Vec::new(),
            errors: Vec::new(),
            notes: Vec::new(),
            incarnation: 0,
            config,
            next_session: 0,
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

    pub fn state(&self) -> Option<State> {
        self.running.as_ref().map(|r| r.agent.state())
    }

    /// Starts the backup on its recording log and archive.
    pub fn start(&mut self, now_ms: i64, random_seed: u64) {
        let mut archive = self.dormant_archive.take().expect("a dormant archive");
        archive.restart();
        let log = RecordingLog::load(&self.recording_log_file).expect("the backup's recording log");
        let config = Config {
            random_seed,
            ..self.config.clone()
        };
        let agent = ClusterBackupAgent::new(config, log, Box::new(FixedCredentials::default()), EPOCH_MS + now_ms)
            .expect("a backup");
        self.incarnation += 1;
        self.running = Some(RunningBackup {
            agent,
            ports: SimBackupPorts {
                archive,
                consensus_to: None,
                cluster_archive: None,
                reachable_members: BTreeSet::new(),
                outbox: Vec::new(),
                counters: HashMap::new(),
                next_correlation_id: 1_000_000 * i64::from(self.incarnation),
            },
            consensus_inbox: VecDeque::new(),
            archive_inbox: VecDeque::new(),
            terminated: None,
            last_live_log_position: 0,
        });
        self.carry();
    }

    /// The process dies: its recording log and archive stay.
    pub fn crash(&mut self) {
        if let Some(running) = self.running.take() {
            self.dormant_archive = Some(running.ports.archive);
        }
    }

    /// One duty cycle at `now_ms`; what it sends is left in its outbox, and
    /// a live log position gone back in `violations`.
    pub fn step(&mut self, now_ms: i64, violations: &mut Vec<String>) {
        let Some(r) = self.running.as_mut().filter(|r| r.terminated.is_none()) else {
            return;
        };
        r.agent.on_tick(EPOCH_MS + now_ms);
        self.deliver();
        if let Some(r) = self.running.as_mut() {
            for _ in 0..rusteron_cluster::cluster_backup_agent::CONSENSUS_FRAGMENT_LIMIT {
                let Some(bytes) = r.consensus_inbox.pop_front() else {
                    break;
                };
                r.agent.on_consensus_message(&bytes, &mut r.ports);
            }
            r.agent.do_work(&mut r.ports);
        }
        self.carry();
        self.deliver();
        if let Some(r) = self.running.as_mut() {
            let position = r.ports.counter(BackupCounter::LiveLogPosition);
            if position < r.last_live_log_position {
                violations.push(format!(
                    "backup's live log position went back from {} to {position}",
                    r.last_live_log_position
                ));
            }
            r.last_live_log_position = position;
        }
    }

    /// Both archives' responses, until quiet.
    fn deliver(&mut self) {
        for _ in 0..32 {
            let Some(r) = self.running.as_mut() else {
                return;
            };
            let mut moved = false;
            while let Some(response) = r.ports.archive.responses.pop_front() {
                moved = true;
                r.agent.on_archive_message(ArchiveId::Local, &response, &mut r.ports);
            }
            while let Some((session, response)) = r.archive_inbox.pop_front() {
                moved = true;
                if r.ports.cluster_archive.is_some_and(|(_, s)| s == session) {
                    r.agent.on_archive_message(ArchiveId::Cluster, &response, &mut r.ports);
                }
            }
            self.carry();
            if !moved {
                break;
            }
        }
    }

    fn carry(&mut self) {
        while let Some(action) = self.running.as_mut().and_then(|r| r.agent.poll_action()) {
            self.act(action);
        }
        let Some(r) = self.running.as_mut() else {
            return;
        };
        while let Some(event) = r.agent.poll_event() {
            match event {
                Event::BackupQuery => self
                    .notes
                    .push(format!("backup queries ({:?})", r.agent.config().source_type)),
                Event::PossibleFailure { message } => self.notes.push(format!("backup's possible failure: {message}")),
                Event::BackupResponse {
                    cluster_members,
                    log_source_member,
                    snapshots_to_retrieve,
                } => {
                    let leader = cluster_members.iter().find(|m| m.is_leader).map(|m| m.id);
                    self.notes.push(format!(
                        "backup's response from member {} (leader {leader:?}), {} snapshots to copy",
                        log_source_member.id,
                        snapshots_to_retrieve.len()
                    ));
                }
                Event::UpdatedRecordingLog { .. } | Event::LiveLogProgress { .. } => {}
            }
        }
    }

    fn act(&mut self, action: Action) {
        let incarnation = self.incarnation;
        let Some(r) = self.running.as_mut() else {
            return;
        };
        match action {
            Action::AddConsensusSubscription { .. }
            | Action::AddRecordingSubscription { .. }
            | Action::CloseRecordingSubscription => {}
            Action::AddConsensusPublication { channel, .. } => {
                r.ports.consensus_to = endpoint_of(&channel).and_then(|e| member_of_endpoint(&e));
                self.notes
                    .push(format!("backup's publication to member {:?}", r.ports.consensus_to));
            }
            Action::CloseConsensusPublication => r.ports.consensus_to = None,
            Action::AddArchive {
                archive: ArchiveId::Cluster,
                request_channel,
                ..
            } => {
                self.next_session += 1;
                let session = 10_000 * i64::from(incarnation) + self.next_session;
                r.ports.cluster_archive =
                    endpoint_of(&request_channel).and_then(|e| member_of_endpoint(&e).map(|m| (m, session)));
                r.archive_inbox.clear();
            }
            Action::AddArchive {
                archive: ArchiveId::Local,
                ..
            } => {}
            Action::CloseArchive {
                archive: ArchiveId::Cluster,
            } => r.ports.cluster_archive = None,
            Action::CloseArchive {
                archive: ArchiveId::Local,
            } => {}
            Action::WriteRecordingLog { writes, .. } => {
                for (offset, bytes) in writes {
                    let end = offset as usize + bytes.len();
                    if self.recording_log_file.len() < end {
                        self.recording_log_file.resize(end, 0);
                    }
                    self.recording_log_file[offset as usize..end].copy_from_slice(&bytes);
                }
            }
            Action::Error { message, .. } => self.errors.push(format!("backup: {message}")),
            Action::Terminate { reason } => r.terminated = Some(reason),
        }
    }

    /// Whether the backup's archive records at `endpoint`: the live log's
    /// recording, waiting for the member's replay.
    pub fn is_subscribed_at(&self, endpoint: &str) -> bool {
        self.running.as_ref().is_some_and(|r| {
            r.ports
                .archive
                .recordings
                .values()
                .any(|rec| rec.is_recording() && endpoint_of(&rec.channel).as_deref() == Some(endpoint))
        })
    }

    /// The replay's stream at `position`, with nothing to send: the
    /// recording joins it there.
    pub fn on_log_heartbeat(&mut self, session_id: i32, position: i64) {
        if let Some(r) = self.running.as_mut() {
            r.ports.archive.on_stream(session_id, None, position);
        }
    }

    pub fn on_log_data(&mut self, session_id: i32, message: &Message) {
        if let Some(r) = self.running.as_mut() {
            r.ports.archive.on_stream(session_id, None, message.start);
            r.ports.archive.on_data(session_id, None, message);
        }
    }

    /// The replay's image from a member gone (its process, a cut link): the
    /// recording of it stops, and its counter goes.
    pub fn lose_replay(&mut self, session_id: i32) {
        let Some(r) = self.running.as_mut() else {
            return;
        };
        let counter = r
            .ports
            .archive
            .recordings
            .values()
            .find(|rec| rec.session_id == session_id && rec.is_recording())
            .map(|rec| rec.counter_id);
        if let Some(counter_id) = counter {
            r.ports.archive.on_stream_gone(session_id, None);
            r.agent.on_io(Io::CounterUnavailable { counter_id }, &mut r.ports);
            self.carry();
        }
    }

    /// The live log as the backup recorded it: every recording of the log
    /// stream, its messages in order.
    pub fn log_messages(&self) -> Vec<Message> {
        self.archive()
            .recordings
            .values()
            .filter(|r| r.stream_id == crate::service::LOG_STREAM_ID)
            .flat_map(|r| r.messages.iter().cloned())
            .collect()
    }

    /// Where the backup's log recording ends.
    pub fn recorded_position(&self) -> i64 {
        self.archive()
            .recordings
            .values()
            .filter(|r| r.stream_id == crate::service::LOG_STREAM_ID)
            .map(|r| r.position())
            .max()
            .unwrap_or(0)
    }

    pub fn live_log_position(&self) -> i64 {
        self.running
            .as_ref()
            .map_or(0, |r| r.ports.counter(BackupCounter::LiveLogPosition))
    }
}
