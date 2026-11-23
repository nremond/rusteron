//! For tests of the conductor: a client's requests ([`client`]), a decoder
//! of what the archive sends ([`Response`]), and a fake shell
//! ([`FakeShell`]) that does the conductor's I/O in memory: publications
//! that take every message, subscriptions and counters added at once,
//! recorders whose blocks the test writes, and a source archive
//! ([`FakeSource`]) a replication talks to.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::catalog::Catalog;
use crate::conductor::{
    Action, Conductor, Config, Counters, EraseError, ImageInfo, Io, Offer, RecordingError, ReplayLimit, RequestImage,
    ResponseChannels, SubscriptionToken,
};
use crate::protocol::{self, Code, Request};
use crate::replay::{ReplayError, ReplaySession};
use crate::segments::SegmentWriter;

pub use crate::client::{self, Response};

/// Response publications that take what they are offered, unless told
/// otherwise for a session; a replication's links to its source.
#[derive(Default)]
pub struct FakeChannels {
    pub sent: Vec<(i64, Vec<u8>)>,
    pub open: HashSet<i64>,
    pub connected: HashSet<i64>,
    /// What an offer on a session's publication comes to instead of `Sent`.
    pub refuse: HashMap<i64, Offer>,
    /// Requests to replications' sources, by replication.
    pub source_sent: Vec<(i64, Vec<u8>)>,
    pub sources_connected: HashSet<i64>,
    /// Recording subscriptions' resolved endpoints.
    pub resolved: HashMap<SubscriptionToken, String>,
}

impl ResponseChannels for FakeChannels {
    fn offer(&mut self, control_session_id: i64, message: &[u8]) -> Offer {
        if let Some(&result) = self.refuse.get(&control_session_id) {
            return result;
        }
        if !self.connected.contains(&control_session_id) {
            return Offer::NotConnected;
        }
        self.sent.push((control_session_id, message.to_vec()));
        Offer::Sent
    }

    fn is_connected(&self, control_session_id: i64) -> bool {
        self.connected.contains(&control_session_id)
    }

    fn offer_source(&mut self, replication_id: i64, message: &[u8]) -> Offer {
        if !self.sources_connected.contains(&replication_id) {
            return Offer::NotConnected;
        }
        self.source_sent.push((replication_id, message.to_vec()));
        Offer::Sent
    }

    fn source_connected(&self, replication_id: i64) -> bool {
        self.sources_connected.contains(&replication_id)
    }

    fn resolved_endpoint(&self, subscription: SubscriptionToken) -> Option<String> {
        self.resolved.get(&subscription).cloned()
    }
}

/// A source archive, as far as a replication asks of it: it answers from
/// its catalog, and records the replays asked of it.
pub struct FakeSource {
    pub catalog: Catalog,
    pub control_session_id: i64,
    /// Replays asked: their request's fields.
    pub replays: Vec<Request>,
    pub stopped_replays: Vec<i64>,
    pub closed: bool,
    /// Answers the connect with this protocol version.
    pub version: i32,
}

impl FakeSource {
    pub fn new(catalog: Catalog) -> Self {
        FakeSource {
            catalog,
            control_session_id: 900,
            replays: Vec::new(),
            stopped_replays: Vec::new(),
            closed: false,
            version: protocol::PROTOCOL_SEMANTIC_VERSION,
        }
    }

    /// What the source sends back for a request.
    pub fn answer(&mut self, message: &[u8]) -> Vec<Vec<u8>> {
        let cs = self.control_session_id;
        let ok = |corr: i64, relevant: i64| protocol::control_response(cs, corr, relevant, Code::OK, "");
        match protocol::decode_request(message).expect("a request") {
            Request::AuthConnect { correlation_id, .. } => {
                vec![protocol::control_response(cs, correlation_id, cs, Code::OK, "")]
            }
            Request::ArchiveId { correlation_id, .. } => vec![ok(correlation_id, 77)],
            Request::KeepAlive { .. } => vec![],
            Request::ListRecording {
                correlation_id,
                recording_id,
                ..
            } => match self.catalog.descriptor_bytes(recording_id) {
                Ok(bytes) => vec![protocol::recording_descriptor(cs, correlation_id, bytes)],
                Err(_) => vec![protocol::control_response(
                    cs,
                    correlation_id,
                    recording_id,
                    Code::RECORDING_UNKNOWN,
                    "",
                )],
            },
            replay @ Request::Replay { correlation_id, .. } => {
                let replay_session_id = (1i64 << 32) | i64::from(self.replays.len() as u32 + 4_242);
                self.replays.push(replay);
                vec![ok(correlation_id, replay_session_id)]
            }
            Request::StopReplay {
                correlation_id,
                replay_session_id,
                ..
            } => {
                self.stopped_replays.push(replay_session_id);
                vec![ok(correlation_id, 0)]
            }
            Request::CloseSession { .. } => {
                self.closed = true;
                vec![]
            }
            other => panic!("the source does not serve {other:?}"),
        }
    }
}

/// A recording subscription the fake shell holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FakeSubscription {
    pub channel: String,
    pub stream_id: i32,
    pub registration_id: i64,
}

/// A recorder the fake shell runs: the test writes its blocks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FakeRecorder {
    pub subscription: SubscriptionToken,
    pub image_correlation_id: i64,
    pub writer: SegmentWriter,
    pub counter_key: Vec<u8>,
    pub counter_label: String,
}

/// The conductor driven as the shell drives it, its I/O done in memory.
pub struct FakeShell {
    pub conductor: Conductor,
    pub channels: FakeChannels,
    pub now_ms: i64,
    /// Every action the conductor asked for, in order.
    pub actions: Vec<Action>,
    pub subscriptions: BTreeMap<SubscriptionToken, FakeSubscription>,
    pub recorders: BTreeMap<i64, FakeRecorder>,
    pub counters: BTreeMap<i64, (Vec<u8>, String)>,
    /// Counters a bounded replay may name, by id: their values.
    pub limit_counters: HashMap<i32, i64>,
    /// Replays running, by id: their session and limit.
    pub replays: BTreeMap<i64, (ReplaySession, ReplayLimit)>,
    /// The archive directory's files.
    pub files: BTreeSet<String>,
    /// What cutting a truncated segment comes to.
    pub erase_result: Result<(), EraseError>,
    pub errors: Vec<String>,
    /// Sessions whose publication connects as soon as added.
    pub connect_on_add: bool,
    /// The archive a replication connects to.
    pub source: Option<FakeSource>,
    /// Replications' links to their source: open or closed.
    pub source_links: BTreeMap<i64, bool>,
    next_registration_id: i64,
}

impl FakeShell {
    pub fn new(config: Config) -> Self {
        Self::with_catalog(config, Catalog::create(4_096).expect("catalog"))
    }

    pub fn with_catalog(config: Config, catalog: Catalog) -> Self {
        let now_ms = 1_700_000_000_000;
        let mut conductor = Conductor::new(config, catalog);
        conductor.on_tick(now_ms);
        FakeShell {
            conductor,
            channels: FakeChannels::default(),
            now_ms,
            actions: Vec::new(),
            subscriptions: BTreeMap::new(),
            recorders: BTreeMap::new(),
            counters: BTreeMap::new(),
            limit_counters: HashMap::new(),
            replays: BTreeMap::new(),
            files: BTreeSet::new(),
            erase_result: Ok(()),
            errors: Vec::new(),
            connect_on_add: true,
            source: None,
            source_links: BTreeMap::new(),
            next_registration_id: 1_000,
        }
    }

    /// A control message from `image`, then the I/O it asked for.
    pub fn request(&mut self, image: &RequestImage, message: &[u8]) {
        let counters = LimitCounters(self.limit_counters.clone());
        self.conductor.on_control_message(image, message, &counters);
        self.run_actions();
    }

    /// A replay's client to be told OK: its segment file found.
    pub fn replay_started(&mut self, replay_session_id: i64) {
        self.io(Io::ReplayStarted { replay_session_id });
    }

    /// A replay ended, or failed with `error`.
    pub fn replay_ended(&mut self, replay_session_id: i64, error: Option<ReplayError>) {
        self.replays.remove(&replay_session_id);
        self.io(Io::ReplayEnded {
            replay_session_id,
            error,
        });
    }

    /// A duty cycle `advance_ms` later: the sessions' work, then its I/O;
    /// the source answers what it was sent.
    pub fn tick(&mut self, advance_ms: i64) -> usize {
        self.now_ms += advance_ms;
        self.conductor.on_tick(self.now_ms);
        let work = self.conductor.do_work(&mut self.channels);
        self.run_actions();
        for (replication_id, message) in std::mem::take(&mut self.channels.source_sent) {
            let Some(source) = self.source.as_mut() else { continue };
            for answer in source.answer(&message) {
                self.conductor.on_io(Io::SourceMessage {
                    replication_id,
                    message: answer,
                });
            }
            self.run_actions();
        }
        work
    }

    pub fn io(&mut self, io: Io) {
        self.conductor.on_io(io);
        self.run_actions();
    }

    /// An image on a recording subscription.
    pub fn image(&mut self, subscription: SubscriptionToken, image: ImageInfo) {
        self.io(Io::RecordingImage { subscription, image });
    }

    /// Blocks of `(length, padding)` from a recording's image, written.
    pub fn record(&mut self, recording_id: i64, blocks: &[(usize, bool)]) -> i64 {
        let recorder = self.recorders.get_mut(&recording_id).expect("recording");
        for &(length, padding) in blocks {
            recorder.writer.on_block(length, padding);
        }
        let position = recorder.writer.position();
        self.conductor.on_recorded(recording_id, position);
        position
    }

    /// A recording's image ended, or failed with `error`.
    pub fn end(&mut self, recording_id: i64, error: Option<RecordingError>) {
        let recorder = self.recorders.remove(&recording_id).expect("recording");
        self.io(Io::RecorderStopped {
            recording_id,
            position: recorder.writer.position(),
            error,
        });
    }

    /// The responses a session was sent since last asked.
    pub fn responses(&mut self, control_session_id: i64) -> Vec<Response> {
        let (mine, others) = std::mem::take(&mut self.channels.sent)
            .into_iter()
            .partition(|(id, _)| *id == control_session_id);
        self.channels.sent = others;
        mine.into_iter().map(|(_, m)| Response::decode(&m)).collect()
    }

    /// The token of the subscription on `channel`.
    pub fn subscription(&self, channel: &str) -> SubscriptionToken {
        *self
            .subscriptions
            .iter()
            .find(|(_, s)| s.channel == channel)
            .unwrap_or_else(|| panic!("no subscription on {channel}: {:?}", self.subscriptions))
            .0
    }

    pub fn run_actions(&mut self) {
        while let Some(action) = self.conductor.poll_action() {
            self.actions.push(action.clone());
            match action {
                Action::AddResponsePublication { control_session_id, .. } => {
                    self.channels.open.insert(control_session_id);
                    if self.connect_on_add {
                        self.channels.connected.insert(control_session_id);
                    }
                    self.conductor.on_io(Io::ResponsePublication {
                        control_session_id,
                        result: Ok(()),
                    });
                }
                Action::AddControlSessionCounter {
                    control_session_id,
                    key,
                    label,
                } => {
                    self.counters.insert(control_session_id, (key, label));
                    self.conductor.on_io(Io::ControlSessionCounter {
                        control_session_id,
                        result: Ok(()),
                    });
                }
                Action::CloseControlSession { control_session_id } => {
                    self.channels.open.remove(&control_session_id);
                    self.channels.connected.remove(&control_session_id);
                    self.counters.remove(&control_session_id);
                }
                Action::RejectImage { .. } => {}
                Action::AddRecordingSubscription {
                    subscription,
                    channel,
                    stream_id,
                } => {
                    let registration_id = self.next_registration_id;
                    self.next_registration_id += 1;
                    self.subscriptions.insert(
                        subscription,
                        FakeSubscription {
                            channel,
                            stream_id,
                            registration_id,
                        },
                    );
                    self.conductor.on_io(Io::RecordingSubscription {
                        subscription,
                        result: Ok(registration_id),
                    });
                }
                Action::CloseRecordingSubscription { subscription } => {
                    self.subscriptions.remove(&subscription);
                }
                Action::StartRecorder {
                    recording_id,
                    subscription,
                    image_correlation_id,
                    writer,
                    counter_key,
                    counter_label,
                    ..
                } => {
                    self.recorders.insert(
                        recording_id,
                        FakeRecorder {
                            subscription,
                            image_correlation_id,
                            writer,
                            counter_key,
                            counter_label,
                        },
                    );
                }
                Action::AbortRecorder { recording_id } => {
                    if let Some(recorder) = self.recorders.remove(&recording_id) {
                        self.conductor.on_io(Io::RecorderStopped {
                            recording_id,
                            position: recorder.writer.position(),
                            error: None,
                        });
                    }
                }
                Action::AddReplayPublication { publication, .. } => {
                    let session_id = self.next_registration_id as i32;
                    self.next_registration_id += 1;
                    self.conductor.on_io(Io::ReplayPublication {
                        publication,
                        result: Ok(session_id),
                    });
                }
                Action::StartReplay {
                    replay_session_id,
                    session,
                    limit,
                    ..
                } => {
                    self.replays.insert(replay_session_id, (*session, limit));
                }
                Action::AbortReplay { replay_session_id, .. } => {
                    if self.replays.remove(&replay_session_id).is_some() {
                        self.conductor.on_io(Io::ReplayEnded {
                            replay_session_id,
                            error: None,
                        });
                    }
                }
                Action::ListSegmentFiles { recording_id } => {
                    let prefix = format!("{recording_id}-");
                    let file_names = self.files.iter().filter(|f| f.starts_with(&prefix)).cloned().collect();
                    self.conductor.on_io(Io::SegmentFiles {
                        recording_id,
                        file_names,
                    });
                }
                Action::EraseSegment { recording_id, .. } => {
                    self.conductor.on_io(Io::SegmentErased {
                        recording_id,
                        result: self.erase_result.clone(),
                    });
                }
                Action::DeleteSegmentFile {
                    recording_id,
                    file_name,
                } => {
                    let result = if self.files.remove(&file_name) {
                        Ok(())
                    } else {
                        Err(format!("unable to delete segment file: archive/{file_name}"))
                    };
                    self.conductor.on_io(Io::SegmentFileDeleted { recording_id, result });
                }
                Action::ConnectSourceArchive { replication_id, .. } => {
                    self.source_links.insert(replication_id, true);
                    self.channels.sources_connected.insert(replication_id);
                    self.conductor.on_io(Io::SourceLinked {
                        replication_id,
                        result: Ok("aeron:udp?endpoint=localhost:20999".into()),
                    });
                }
                Action::CloseSourceArchive { replication_id } => {
                    self.source_links.insert(replication_id, false);
                    self.channels.sources_connected.remove(&replication_id);
                }
                Action::Error { message } => self.errors.push(message),
            }
        }
    }
}

struct LimitCounters(HashMap<i32, i64>);

impl Counters for LimitCounters {
    fn value(&self, counter_id: i32) -> Option<i64> {
        self.0.get(&counter_id).copied()
    }
}
