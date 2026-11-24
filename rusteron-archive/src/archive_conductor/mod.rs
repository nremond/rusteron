//! The archive's conductor as a state machine, wire-compatible with Aeron
//! Archive 1.54.2's `ArchiveConductor` in its SHARED mode (one thread):
//! control sessions, the recordings they start, stop, list, replay,
//! truncate and purge.
//!
//! The conductor reads no clock and does no I/O. The shell that drives it
//! feeds it the cached time ([`Conductor::on_tick`]), the control requests
//! its control subscriptions receive ([`Conductor::on_control_message`]),
//! and the outcome of the I/O it asked for ([`Conductor::on_io`]); it takes
//! the I/O to do from [`Conductor::poll_action`], and runs the sessions,
//! which answer through the shell's publications, in
//! [`Conductor::do_work`]. The recorder (block polls into the segment
//! files) is the shell's, its arithmetic [`crate::segments`]'s; it reports
//! each recording's progress ([`Conductor::on_recorded`]) and its end. So
//! are the replays, their arithmetic [`crate::replay_session`]'s.
//!
//! Java's behaviours kept, the odd ones included: a session answers the
//! connect until its first request, and a request before that is dropped; a
//! closed session leaves its recordings running; the count of active
//! recordings goes up once a start or an extension and down once a
//! recording; an extension is checked against its image only once
//! acknowledged.
//!
//! Where it does differ: responses are queued as Java queues them under
//! back pressure, and sent each duty cycle as many as the publication takes
//! (Java sends one queued response a cycle); a listing waits for what is
//! queued before it (Java's descriptors overtake it); a listing of a
//! negative count ends at once (Java's never does); a replay of all and stop
//! from a recording with nothing to replay is refused (Java refuses it and
//! starts it).

#![forbid(unsafe_code)]
// The crate allows these lints for its generated bindings; this module is checked.
#![warn(
    clippy::all,
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    unsafe_op_in_unsafe_fn,
    unused_unsafe,
    unused_variables
)]

mod control_session;
mod deletes;
mod replays;
mod replication_session;

use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::archive_counters;
use crate::catalog::{Catalog, Descriptor, NULL_POSITION, NULL_TIMESTAMP};
use crate::channel_uri::{self, ChannelUri, ControlStream};
use crate::control_session_adapter::{
    self, Code, Location, Malformed, PROTOCOL_MAJOR_VERSION, Request, Signal, error_code,
};
use crate::replay_session::{ReplayError, ReplaySession};
use crate::segments::{SegmentWriter, recording_segment_length};

use control_session::{ControlSession, Item, Listing, SessionState};
use deletes::{DeleteSession, PendingDelete};
use replays::{ActiveReplay, PendingReplay};
use replication_session::Replication;

/// How an archive runs. The defaults are Aeron's.
#[derive(Clone, Debug)]
pub struct Config {
    pub archive_id: i64,
    /// A recording's segment files' length, at least its term's.
    pub segment_file_length: i32,
    /// The largest block written at once.
    pub file_io_max_length: i32,
    pub max_concurrent_recordings: i32,
    /// At or below this many usable bytes, recordings are refused.
    pub low_storage_space_threshold: i64,
    /// How long a session may take to connect, or to take a response.
    pub connect_timeout_ms: i64,
    /// How often an active session is pinged.
    pub session_liveness_check_interval_ms: i64,
    /// The response channel's MTU, term length and sparseness unless the
    /// client's channel says.
    pub control: ControlStream,
    /// The first control session's id: Java draws it at random.
    pub first_control_session_id: i64,
    pub max_concurrent_replays: i32,
    /// How long a replay's publication lingers unless its channel says: the
    /// driver's publication linger.
    pub replay_linger_timeout_ns: i64,
    /// How long a replication waits on the source archive's answers.
    pub message_timeout_ms: i64,
    /// Where a replication receives the source's replay, unless it says.
    pub replication_channel: String,
    /// Where the source archive answers a replication, unless it says.
    pub archive_client_response_channel: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            archive_id: -1,
            segment_file_length: 128 * 1024 * 1024,
            file_io_max_length: 1024 * 1024,
            max_concurrent_recordings: 20,
            low_storage_space_threshold: 128 * 1024 * 1024,
            connect_timeout_ms: 5_000,
            session_liveness_check_interval_ms: 1_000,
            control: ControlStream {
                mtu_length: 1_408,
                term_length: 64 * 1024,
                sparse: true,
            },
            first_control_session_id: 0,
            max_concurrent_replays: 20,
            replay_linger_timeout_ns: 5_000_000_000,
            message_timeout_ms: 10_000,
            replication_channel: "aeron:udp?endpoint=localhost:0".into(),
            archive_client_response_channel: "aeron:udp?endpoint=localhost:0".into(),
        }
    }
}

/// The image a control request came in on: a session answers only requests
/// from the image that connected it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestImage {
    pub correlation_id: i64,
    pub session_id: i32,
    pub source_identity: String,
}

/// An image of a recording subscription, as the shell's client reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageInfo {
    pub correlation_id: i64,
    pub session_id: i32,
    pub source_identity: String,
    pub initial_term_id: i32,
    pub term_buffer_length: i32,
    pub mtu_length: i32,
    pub join_position: i64,
}

/// What an offer on a control session's publication came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Offer {
    Sent,
    /// Back pressured, or an admin action: tried again later.
    Retry,
    NotConnected,
    Closed,
    MaxPositionExceeded,
}

/// The control sessions' response publications, the shell's; and a
/// replication's links to its source archive.
pub trait ResponseChannels {
    /// Offers `message` on the session's publication, once.
    fn offer(&mut self, control_session_id: i64, message: &[u8]) -> Offer;
    fn is_connected(&self, control_session_id: i64) -> bool;

    /// Offers a request to a replication's source archive, once.
    fn offer_source(&mut self, _replication_id: i64, _message: &[u8]) -> Offer {
        Offer::NotConnected
    }

    /// Whether a replication's request publication to its source is connected.
    fn source_connected(&self, _replication_id: i64) -> bool {
        false
    }

    /// The endpoint a recording subscription bound, its port resolved.
    fn resolved_endpoint(&self, _subscription: SubscriptionToken) -> Option<String> {
        None
    }
}

/// A recording subscription, named by the conductor until the shell's client
/// gives it a registration id; so a replay's publication.
pub type SubscriptionToken = u64;

/// The counters a request may name: a bounded replay's limit.
pub trait Counters {
    /// A counter's value, if it is allocated.
    fn value(&self, counter_id: i32) -> Option<i64>;
}

/// No counters at all.
pub struct NoCounters;

impl Counters for NoCounters {
    fn value(&self, _counter_id: i32) -> Option<i64> {
        None
    }
}

/// What a replay follows once it reaches its stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayLimit {
    /// Nothing: it ends at its stop.
    None,
    /// A bounded replay's counter.
    Counter(i32),
    /// The position counter of the recording, still being written.
    Recording(i64),
}

/// What a truncation's segment must hold at its position: a frame of the
/// recording's, else the truncation fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpectedFrame {
    pub term_offset: i32,
    pub term_id: i32,
    pub stream_id: i32,
}

/// Why a segment could not be cut at a truncation's position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EraseError {
    /// No frame of the recording's starts there.
    NotAligned,
    /// The file could not be read or written: Java's message.
    Io(String),
}

/// I/O the shell does for the conductor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Add a control session's response publication, exclusive, and report
    /// it ([`Io::ResponsePublication`]).
    AddResponsePublication {
        control_session_id: i64,
        channel: String,
        stream_id: i32,
    },
    /// Add a control session's counter, and report it ([`Io::ControlSessionCounter`]).
    AddControlSessionCounter {
        control_session_id: i64,
        key: Vec<u8>,
        label: String,
    },
    /// Close a session's publication (revoked) and counter, added or being added.
    CloseControlSession {
        control_session_id: i64,
    },
    /// Reject the image a session's requests came on: it was aborted.
    RejectImage {
        image_correlation_id: i64,
        reason: String,
    },
    /// Add a recording subscription, and report it before the next control
    /// request ([`Io::RecordingSubscription`]); then report its images
    /// ([`Io::RecordingImage`]).
    AddRecordingSubscription {
        subscription: SubscriptionToken,
        channel: String,
        stream_id: i32,
    },
    CloseRecordingSubscription {
        subscription: SubscriptionToken,
    },
    /// Record an image: allocate the recording's position counter with this
    /// key and label, set it to the writer's position, open the writer's
    /// segment file (created at the segment's length, or as it is), then
    /// write the image's blocks as the writer places them, report the
    /// position after each ([`Conductor::on_recorded`]), until the image
    /// ends, an error, or [`Action::AbortRecorder`]: then report the end
    /// ([`Io::RecorderStopped`]).
    StartRecorder {
        recording_id: i64,
        subscription: SubscriptionToken,
        image_correlation_id: i64,
        writer: SegmentWriter,
        block_length_limit: usize,
        counter_key: Vec<u8>,
        counter_label: String,
    },
    /// Stop recording, writing nothing more, and report the end.
    AbortRecorder {
        recording_id: i64,
    },
    /// Add a replay's publication, exclusive, and report it
    /// ([`Io::ReplayPublication`]).
    AddReplayPublication {
        publication: SubscriptionToken,
        channel: String,
        stream_id: i32,
    },
    /// Replay on that publication, as the session says: report when the
    /// client is to be told OK ([`Io::ReplayStarted`]) and when it ends
    /// ([`Io::ReplayEnded`]).
    StartReplay {
        replay_session_id: i64,
        publication: SubscriptionToken,
        session: Box<ReplaySession>,
        limit: ReplayLimit,
    },
    /// Stop replaying and report the end: the publication revoked, its
    /// images dropped at once wherever their readers are, as its recording
    /// goes or a client stops it; closed as at the replay's end, its images
    /// left to their readers, when a client stops a replay whose channel
    /// does not signal the end of stream (`eos=false`). Java's 1.54 archive
    /// revokes those too, and a cluster follower's catch-up image then goes
    /// before its service has read it, the service left behind for good
    /// (aeron-io/aeron#2259; #2260 closes them).
    AbortReplay {
        replay_session_id: i64,
        revoke: bool,
    },
    /// List a recording's segment files, `.rec` and `.del`, and report
    /// them before the next control request ([`Io::SegmentFiles`]).
    ListSegmentFiles {
        recording_id: i64,
    },
    /// Cut a truncated recording's segment at `segment_offset`, after
    /// checking the frame there, its length kept; report it before the next
    /// control request ([`Io::SegmentErased`]).
    EraseSegment {
        recording_id: i64,
        file_name: String,
        segment_offset: u64,
        segment_length: i32,
        expected: ExpectedFrame,
    },
    /// Delete a segment file (or its `.del`), and report it ([`Io::SegmentFileDeleted`]).
    DeleteSegmentFile {
        recording_id: i64,
        file_name: String,
    },
    /// Add a replication's links to its source archive: an exclusive
    /// publication on the request channel and a subscription on the response
    /// channel, each with a session id of their own; report them with the
    /// response channel as the source is to answer on ([`Io::SourceLinked`]);
    /// then the source's messages ([`Io::SourceMessage`]).
    ConnectSourceArchive {
        replication_id: i64,
        request_channel: String,
        request_stream_id: i32,
        response_channel: String,
        response_stream_id: i32,
    },
    /// Close a replication's links, once what was offered on them is out.
    CloseSourceArchive {
        replication_id: i64,
    },
    /// For the archive's error log and errors counter.
    Error {
        message: String,
    },
}

/// What a recording ended on, when not its image's end or an abort.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordingError {
    pub code: i64,
    pub message: String,
}

/// What came of the I/O the conductor asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Io {
    /// `Err(retry)`: the driver had no room yet, the publication is asked
    /// for again; otherwise the session ends.
    ResponsePublication {
        control_session_id: i64,
        result: Result<(), (bool, String)>,
    },
    ControlSessionCounter {
        control_session_id: i64,
        result: Result<(), String>,
    },
    RecordingSubscription {
        subscription: SubscriptionToken,
        result: Result<i64, String>,
    },
    RecordingImage {
        subscription: SubscriptionToken,
        image: ImageInfo,
    },
    RecorderStopped {
        recording_id: i64,
        position: i64,
        error: Option<RecordingError>,
    },
    /// `Err(retry)`, as for a response publication; `Ok`: its session id.
    ReplayPublication {
        publication: SubscriptionToken,
        result: Result<i32, (bool, String)>,
    },
    ReplayStarted {
        replay_session_id: i64,
    },
    ReplayEnded {
        replay_session_id: i64,
        error: Option<ReplayError>,
    },
    SegmentFiles {
        recording_id: i64,
        file_names: Vec<String>,
    },
    SegmentErased {
        recording_id: i64,
        result: Result<(), EraseError>,
    },
    /// `Err`: Java's message, the file's path in it.
    SegmentFileDeleted {
        recording_id: i64,
        result: Result<(), String>,
    },
    SourceLinked {
        replication_id: i64,
        result: Result<String, String>,
    },
    SourceMessage {
        replication_id: i64,
        message: Vec<u8>,
    },
}

/// A subscription recording a stream: one recording an image.
#[derive(Clone, Debug)]
struct RecordingSubscription {
    registration_id: i64,
    control_session_id: i64,
    correlation_id: i64,
    stream_id: i32,
    original_channel: String,
    stripped_channel: String,
    auto_stop: bool,
    /// The recording an extension continues.
    extends: Option<i64>,
}

#[derive(Clone, Debug)]
struct Recording {
    subscription: SubscriptionToken,
    subscription_id: i64,
    control_session_id: i64,
    correlation_id: i64,
    auto_stop: bool,
    position: i64,
    aborted: bool,
}

/// What a session's handler shares with the conductor.
pub(crate) struct Cx<'a> {
    pub(crate) config: &'a Config,
    pub(crate) catalog: &'a Catalog,
    pub(crate) now_ms: i64,
    pub(crate) actions: &'a mut VecDeque<Action>,
}

impl Cx<'_> {
    pub(crate) fn warn(&mut self, message: String) {
        self.actions.push_back(Action::Error {
            message: format!("WARN - {message}"),
        });
    }
}

/// The archive's conductor.
pub struct Conductor {
    config: Config,
    catalog: Catalog,
    now_ms: i64,
    usable_space: i64,
    next_session_id: i64,
    next_token: SubscriptionToken,
    sessions: Vec<ControlSession>,
    subscriptions: HashMap<SubscriptionToken, RecordingSubscription>,
    /// Java's `subscriptionRefCountMap`: a subscription's own reference and
    /// its recordings'.
    ref_counts: HashMap<SubscriptionToken, i32>,
    /// Java's `recordingSubscriptionByKeyMap`: what a stop finds.
    by_key: HashMap<String, SubscriptionToken>,
    recordings: BTreeMap<i64, Recording>,
    /// Replays whose publication is being added, by its token.
    pending_replays: HashMap<SubscriptionToken, PendingReplay>,
    replays: BTreeMap<i64, ActiveReplay>,
    /// Java's `numActiveReplays`: accepted requests, publications pending included.
    num_active_replays: i32,
    next_replay_id: i32,
    /// Truncations and purges waiting on the files' listing or erasure.
    pending_deletes: HashMap<i64, PendingDelete>,
    deletes: BTreeMap<i64, DeleteSession>,
    replications: BTreeMap<i64, Replication>,
    /// Requests to replications' sources, sent at the next session work.
    source_requests: Vec<(i64, Vec<u8>)>,
    next_correlation_id: i64,
    actions: VecDeque<Action>,
}

fn semantic_version(version: i32) -> String {
    format!(
        "{}.{}.{}",
        (version >> 16) & 0xFF,
        (version >> 8) & 0xFF,
        version & 0xFF
    )
}

fn unknown_recording(recording_id: i64) -> String {
    format!("unknown recording id: {recording_id}")
}

impl Conductor {
    /// A conductor on `catalog`, its unstopped recordings' stop positions
    /// already recovered.
    pub fn new(config: Config, catalog: Catalog) -> Self {
        Self {
            next_session_id: config.first_control_session_id,
            config,
            catalog,
            now_ms: 0,
            usable_space: i64::MAX,
            next_token: 1,
            sessions: Vec::new(),
            subscriptions: HashMap::new(),
            ref_counts: HashMap::new(),
            by_key: HashMap::new(),
            recordings: BTreeMap::new(),
            pending_replays: HashMap::new(),
            replays: BTreeMap::new(),
            num_active_replays: 0,
            next_replay_id: 1,
            pending_deletes: HashMap::new(),
            deletes: BTreeMap::new(),
            replications: BTreeMap::new(),
            source_requests: Vec::new(),
            next_correlation_id: 0,
            actions: VecDeque::new(),
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// The catalog's ranges changed since last asked, to write to its file.
    pub fn take_catalog_writes(&mut self) -> Vec<(u64, Vec<u8>)> {
        self.catalog.take_writes()
    }

    /// The cached wall clock, ms since the epoch: the shell reads it once a
    /// duty cycle.
    pub fn on_tick(&mut self, epoch_ms: i64) {
        self.now_ms = epoch_ms;
    }

    /// The archive's usable disk space, before the requests that check it.
    pub fn set_usable_space(&mut self, bytes: i64) {
        self.usable_space = bytes;
    }

    pub fn poll_action(&mut self) -> Option<Action> {
        self.actions.pop_front()
    }

    pub fn control_session_count(&self) -> usize {
        self.sessions.len()
    }

    pub fn recording_session_count(&self) -> usize {
        self.recordings.len()
    }

    /// A recording's position, as its counter shows it; `None` when not recording.
    pub fn recorded_position(&self, recording_id: i64) -> Option<i64> {
        self.recordings.get(&recording_id).map(|r| r.position)
    }

    fn error(&mut self, message: String) {
        self.actions.push_back(Action::Error { message });
    }

    fn warn(&mut self, message: String) {
        self.error(format!("WARN - {message}"));
    }

    fn session_mut(&mut self, control_session_id: i64) -> Option<&mut ControlSession> {
        self.sessions.iter_mut().find(|s| s.id == control_session_id)
    }

    /// Queues a response for a session, if it still is.
    fn respond(&mut self, control_session_id: i64, correlation_id: i64, relevant_id: i64, code: Code, message: &str) {
        if let Some(session) = self.session_mut(control_session_id) {
            session
                .queue
                .push_back(Item::Response(control_session_adapter::control_response(
                    control_session_id,
                    correlation_id,
                    relevant_id,
                    code,
                    message,
                )));
        }
    }

    fn ok(&mut self, control_session_id: i64, correlation_id: i64, relevant_id: i64) {
        self.respond(control_session_id, correlation_id, relevant_id, Code::OK, "");
    }

    fn fail(&mut self, control_session_id: i64, correlation_id: i64, error_code: i64, message: &str) {
        self.respond(control_session_id, correlation_id, error_code, Code::ERROR, message);
    }

    fn signal(
        &mut self,
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        subscription_id: i64,
        position: i64,
        signal: Signal,
    ) {
        if let Some(session) = self.session_mut(control_session_id) {
            session
                .queue
                .push_back(Item::Signal(control_session_adapter::recording_signal(
                    control_session_id,
                    correlation_id,
                    recording_id,
                    subscription_id,
                    position,
                    signal,
                )));
        }
    }

    /// A message on a control subscription, from `image`; `counters` for
    /// the counters a request names.
    pub fn on_control_message(&mut self, image: &RequestImage, message: &[u8], counters: &impl Counters) {
        let template_id = message.get(2..4).map_or(0, |b| u16::from_le_bytes([b[0], b[1]]));
        let request = match control_session_adapter::decode_request(message) {
            Ok(request) => request,
            Err(Malformed::UnknownTemplate(_)) => return,
            Err(e) => return self.error(format!("ERROR - {e}")),
        };
        match request {
            Request::AuthConnect {
                correlation_id,
                response_stream_id,
                version,
                response_channel,
                client_info,
            } => self.new_control_session(
                image,
                correlation_id,
                response_stream_id,
                version,
                &response_channel,
                &client_info,
            ),
            Request::CloseSession { control_session_id } => {
                if let Some(session) = self.session_mut(control_session_id)
                    && session.image == image.correlation_id
                {
                    session.abort("session closed");
                }
            }
            request => {
                let control_session_id = request.control_session_id().expect("a session's request");
                let Some(session) = self.session_mut(control_session_id) else {
                    return self.warn(format!(
                        "control request for unknown session: controlSessionId={control_session_id} \
                         templateId={template_id}"
                    ));
                };
                if session.image != image.correlation_id {
                    let source = image.source_identity.clone();
                    return self.warn(format!(
                        "unauthorised archive action={template_id} controlSessionId={control_session_id} \
                         source={source}"
                    ));
                }
                session.attempt_to_activate();
                if session.state == SessionState::Active {
                    self.serve(request, counters);
                }
            }
        }
    }

    /// A control subscription's image went away: the session it connected ends.
    pub fn on_control_image_unavailable(&mut self, image: &RequestImage, stream_id: i32, channel: &str) {
        if let Some(session) = self
            .sessions
            .iter_mut()
            .find(|s| s.image == image.correlation_id && s.state != SessionState::Done)
        {
            let reason = format!(
                "control request publication image unavailable : controlSessionId={} image.correlationId={} \
                 sessionId={} streamId={stream_id} channel={channel}",
                session.id, image.correlation_id, image.session_id
            );
            session.abort(&reason);
        }
    }

    fn new_control_session(
        &mut self,
        image: &RequestImage,
        correlation_id: i64,
        response_stream_id: i32,
        version: i32,
        response_channel: &str,
        client_info: &str,
    ) {
        let channel = match channel_uri::response_channel(response_channel, self.config.control, image.correlation_id) {
            Ok(channel) => channel,
            Err(e) => return self.error(e),
        };
        let invalid_version = ((version >> 16) & 0xFF != PROTOCOL_MAJOR_VERSION).then(|| {
            format!(
                "invalid client version {}, archive is {}",
                semantic_version(version),
                semantic_version(control_session_adapter::PROTOCOL_SEMANTIC_VERSION)
            )
        });
        let id = self.next_session_id;
        self.next_session_id += 1;
        let (key, label) = archive_counters::control_session(
            self.config.archive_id,
            id,
            client_info,
            &image.source_identity,
            image.session_id,
        );
        self.actions.push_back(Action::AddControlSessionCounter {
            control_session_id: id,
            key,
            label,
        });
        self.actions.push_back(Action::AddResponsePublication {
            control_session_id: id,
            channel: channel.clone(),
            stream_id: response_stream_id,
        });
        self.sessions.push(ControlSession::new(
            id,
            image.correlation_id,
            correlation_id,
            channel,
            response_stream_id,
            invalid_version,
            self.now_ms,
            &self.config,
        ));
    }

    fn serve(&mut self, request: Request, counters: &impl Counters) {
        match request {
            Request::KeepAlive { .. } => {}
            Request::ArchiveId {
                control_session_id,
                correlation_id,
            } => self.ok(control_session_id, correlation_id, self.config.archive_id),
            Request::StartRecording {
                control_session_id,
                correlation_id,
                stream_id,
                location,
                auto_stop,
                channel,
            } => {
                self.start_recording(
                    control_session_id,
                    correlation_id,
                    stream_id,
                    location,
                    auto_stop,
                    channel,
                    None,
                );
            }
            Request::ExtendRecording {
                control_session_id,
                correlation_id,
                recording_id,
                stream_id,
                location,
                auto_stop,
                channel,
            } => {
                self.extend_recording(
                    control_session_id,
                    correlation_id,
                    recording_id,
                    stream_id,
                    location,
                    auto_stop,
                    channel,
                );
            }
            Request::StopRecording {
                control_session_id,
                correlation_id,
                stream_id,
                channel,
            } => self.stop_recording(control_session_id, correlation_id, stream_id, &channel),
            Request::StopRecordingSubscription {
                control_session_id,
                correlation_id,
                subscription_id,
            } => {
                if self.stop_recording_subscription(subscription_id) {
                    self.ok(control_session_id, correlation_id, 0);
                } else {
                    let message = format!("no recording subscription found for subscriptionId={subscription_id}");
                    self.fail(
                        control_session_id,
                        correlation_id,
                        error_code::UNKNOWN_SUBSCRIPTION,
                        &message,
                    );
                }
            }
            Request::StopRecordingByIdentity {
                control_session_id,
                correlation_id,
                recording_id,
            } => self.stop_recording_by_identity(control_session_id, correlation_id, recording_id),
            Request::ListRecording {
                control_session_id,
                correlation_id,
                recording_id,
            } => self.list(control_session_id, correlation_id, |catalog| {
                if catalog.has_recording(recording_id) {
                    Ok(Listing::by_id(correlation_id, recording_id))
                } else {
                    Err(recording_id)
                }
            }),
            Request::ListRecordingsForUri {
                control_session_id,
                correlation_id,
                from_recording_id,
                record_count,
                stream_id,
                channel_fragment,
            } => self.list(control_session_id, correlation_id, |_| {
                Ok(Listing::for_uri(
                    correlation_id,
                    from_recording_id,
                    record_count,
                    Some(stream_id),
                    channel_fragment,
                ))
            }),
            Request::MaxRecordedPosition {
                control_session_id,
                correlation_id,
                recording_id,
            } => {
                let position = self
                    .catalog
                    .stop_position(recording_id)
                    .ok()
                    .map(|stop| self.recorded_position(recording_id).unwrap_or(stop));
                self.position(control_session_id, correlation_id, recording_id, position);
            }
            Request::FindLastMatchingRecording {
                control_session_id,
                correlation_id,
                min_recording_id,
                session_id,
                stream_id,
                channel_fragment,
            } => {
                if min_recording_id < 0 {
                    let message = format!("minRecordingId={min_recording_id} < 0");
                    self.fail(
                        control_session_id,
                        correlation_id,
                        error_code::UNKNOWN_RECORDING,
                        &message,
                    );
                } else {
                    let id = self
                        .catalog
                        .find_last(min_recording_id, session_id, stream_id, &channel_fragment);
                    self.ok(control_session_id, correlation_id, id);
                }
            }
            Request::ListRecordings {
                control_session_id,
                correlation_id,
                from_recording_id,
                record_count,
            } => self.list(control_session_id, correlation_id, |_| {
                Ok(Listing::for_uri(
                    correlation_id,
                    from_recording_id,
                    record_count,
                    None,
                    Vec::new(),
                ))
            }),
            Request::StartPosition {
                control_session_id,
                correlation_id,
                recording_id,
            } => {
                let position = self.catalog.start_position(recording_id).ok();
                self.position(control_session_id, correlation_id, recording_id, position);
            }
            Request::StopPosition {
                control_session_id,
                correlation_id,
                recording_id,
            } => {
                let position = self.catalog.stop_position(recording_id).ok();
                self.position(control_session_id, correlation_id, recording_id, position);
            }
            Request::RecordingPosition {
                control_session_id,
                correlation_id,
                recording_id,
            } => {
                let position = self
                    .catalog
                    .has_recording(recording_id)
                    .then(|| self.recorded_position(recording_id).unwrap_or(NULL_POSITION));
                self.position(control_session_id, correlation_id, recording_id, position);
            }
            Request::Replay {
                control_session_id,
                correlation_id,
                recording_id,
                position,
                length,
                replay_stream_id,
                file_io_max_length,
                replay_token,
                replay_channel,
                limit_counter_id,
            } => self.start_replay(
                replays::ReplayRequest {
                    control_session_id,
                    correlation_id,
                    recording_id,
                    position,
                    length,
                    replay_stream_id,
                    file_io_max_length,
                    replay_token,
                    replay_channel,
                    limit_counter_id,
                },
                counters,
            ),
            Request::StopReplay {
                control_session_id,
                correlation_id,
                replay_session_id,
            } => {
                if let Some(abort) = self.stop_replay_action(replay_session_id) {
                    self.actions.push_back(abort);
                }
                self.ok(control_session_id, correlation_id, 0);
            }
            Request::StopAllReplays {
                control_session_id,
                correlation_id,
                recording_id,
            } => {
                self.stop_all_replays(recording_id, false);
                self.ok(control_session_id, correlation_id, 0);
            }
            Request::TruncateRecording {
                control_session_id,
                correlation_id,
                recording_id,
                position,
            } => self.truncate_recording(control_session_id, correlation_id, recording_id, position),
            Request::PurgeRecording {
                control_session_id,
                correlation_id,
                recording_id,
            } => self.purge_recording(control_session_id, correlation_id, recording_id),
            Request::Replicate(replicate) => self.replicate(*replicate),
            Request::StopReplication {
                control_session_id,
                correlation_id,
                replication_id,
            } => self.stop_replication(control_session_id, correlation_id, replication_id),
            Request::Unsupported {
                template_id,
                control_session_id,
                correlation_id,
            } => {
                let message = format!("request not supported by this archive: templateId={template_id}");
                self.fail(control_session_id, correlation_id, error_code::GENERIC, &message);
            }
            Request::AuthConnect { .. } | Request::CloseSession { .. } => unreachable!("served before"),
        }
    }

    fn position(&mut self, control_session_id: i64, correlation_id: i64, recording_id: i64, position: Option<i64>) {
        match position {
            Some(position) => self.ok(control_session_id, correlation_id, position),
            None => self.fail(
                control_session_id,
                correlation_id,
                error_code::UNKNOWN_RECORDING,
                &unknown_recording(recording_id),
            ),
        }
    }

    fn list(
        &mut self,
        control_session_id: i64,
        correlation_id: i64,
        listing: impl FnOnce(&Catalog) -> Result<Listing, i64>,
    ) {
        let Some(index) = self.sessions.iter().position(|s| s.id == control_session_id) else {
            return;
        };
        if self.sessions[index].listing.is_some() {
            return self.fail(
                control_session_id,
                correlation_id,
                error_code::ACTIVE_LISTING,
                "active listing already in progress",
            );
        }
        match listing(&self.catalog) {
            Ok(listing) => self.sessions[index].listing = Some(listing),
            Err(recording_id) => self.respond(
                control_session_id,
                correlation_id,
                recording_id,
                Code::RECORDING_UNKNOWN,
                "",
            ),
        }
    }

    /// Whether the archive has as many recording subscriptions, or records
    /// as many images, as it may: a new one refused. Java 1.54's check,
    /// where 1.53 counted up per subscription and down per recording.
    fn refuse_new_recording(&mut self, control_session_id: i64, correlation_id: i64) -> bool {
        let max = self.config.max_concurrent_recordings;
        if self.subscriptions.len() as i64 >= i64::from(max) || self.recordings.len() as i64 >= i64::from(max) {
            let message = format!("max concurrent recordings reached at {max}");
            self.fail(control_session_id, correlation_id, error_code::MAX_RECORDINGS, &message);
            return true;
        }
        false
    }

    /// Why a recording may not start, as Java's `isLowStorageSpace` says.
    fn low_storage_message(&self) -> Option<String> {
        let threshold = self.config.low_storage_space_threshold;
        (self.usable_space <= threshold)
            .then(|| format!("low storage threshold={threshold} <= usableSpace={}", self.usable_space))
    }

    fn low_storage(&mut self, control_session_id: i64, correlation_id: i64) -> bool {
        match self.low_storage_message() {
            Some(message) => {
                self.fail(control_session_id, correlation_id, error_code::STORAGE_SPACE, &message);
                true
            }
            None => false,
        }
    }

    /// Why an image may not be recorded now, checked again as it arrives
    /// (Java 1.54): the archive recording as many as it may, or its
    /// storage low.
    fn refuse_recording_image(&self) -> Option<(i64, String)> {
        let max = self.config.max_concurrent_recordings;
        if self.recordings.len() as i64 >= i64::from(max) {
            return Some((
                error_code::MAX_RECORDINGS,
                format!("max concurrent recordings reached at {max}"),
            ));
        }
        self.low_storage_message().map(|m| (error_code::STORAGE_SPACE, m))
    }

    /// An error for the client, and no subscription.
    fn refuse(
        &mut self,
        control_session_id: i64,
        correlation_id: i64,
        error_code: i64,
        message: &str,
    ) -> Option<SubscriptionToken> {
        self.fail(control_session_id, correlation_id, error_code, message);
        None
    }

    #[allow(clippy::too_many_arguments)]
    fn start_recording(
        &mut self,
        control_session_id: i64,
        correlation_id: i64,
        stream_id: i32,
        location: Location,
        auto_stop: bool,
        original_channel: String,
        extends: Option<i64>,
    ) -> Option<SubscriptionToken> {
        if extends.is_none()
            && (self.refuse_new_recording(control_session_id, correlation_id)
                || self.low_storage(control_session_id, correlation_id))
        {
            return None;
        }
        let prepared = ChannelUri::parse(&original_channel).and_then(|uri| {
            let key = channel_uri::recording_key(stream_id, &uri);
            if self.by_key.contains_key(&key) {
                return Ok(Err(key));
            }
            let stripped = channel_uri::stripped_channel(&uri)?;
            // An extension tests the raw channel for "udp", a start its media.
            let udp = match extends {
                Some(_) => original_channel.contains("udp"),
                None => uri.is_udp(),
            };
            let subscribe = if location == Location::Local && udp {
                format!("aeron-spy:{stripped}")
            } else {
                stripped.clone()
            };
            Ok(Ok((key, stripped, subscribe)))
        });
        let (key, stripped_channel, subscribe) = match prepared {
            Ok(Ok(prepared)) => prepared,
            Ok(Err(_)) => {
                let message = format!("recording exists for streamId={stream_id} channel={original_channel}");
                return self.refuse(
                    control_session_id,
                    correlation_id,
                    error_code::ACTIVE_SUBSCRIPTION,
                    &message,
                );
            }
            Err(e) => {
                self.error(e.clone());
                return self.refuse(control_session_id, correlation_id, error_code::GENERIC, &e);
            }
        };
        let token = self.next_token;
        self.next_token += 1;
        self.subscriptions.insert(
            token,
            RecordingSubscription {
                registration_id: NULL_POSITION,
                control_session_id,
                correlation_id,
                stream_id,
                original_channel,
                stripped_channel,
                auto_stop,
                extends,
            },
        );
        self.by_key.insert(key, token);
        self.actions.push_back(Action::AddRecordingSubscription {
            subscription: token,
            channel: subscribe,
            stream_id,
        });
        Some(token)
    }

    #[allow(clippy::too_many_arguments)]
    fn extend_recording(
        &mut self,
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        stream_id: i32,
        location: Location,
        auto_stop: bool,
        channel: String,
    ) -> Option<SubscriptionToken> {
        if self.refuse_new_recording(control_session_id, correlation_id) {
            return None;
        }
        let Ok(recorded) = self.catalog.descriptor(recording_id) else {
            return self.refuse(
                control_session_id,
                correlation_id,
                error_code::UNKNOWN_RECORDING,
                &unknown_recording(recording_id),
            );
        };
        if recorded.stream_id != stream_id {
            let message = format!(
                "cannot extend recording {recording_id} with streamId={stream_id} for existing streamId={}",
                recorded.stream_id
            );
            return self.refuse(
                control_session_id,
                correlation_id,
                error_code::UNKNOWN_RECORDING,
                &message,
            );
        }
        if self.recordings.contains_key(&recording_id) {
            let message = format!("cannot extend active recording {recording_id}");
            return self.refuse(
                control_session_id,
                correlation_id,
                error_code::ACTIVE_RECORDING,
                &message,
            );
        }
        if self.delete_outstanding(recording_id, recorded.stop_position) {
            let message = format!("cannot extend recording {recording_id} due to an outstanding delete operation");
            return self.refuse(control_session_id, correlation_id, error_code::GENERIC, &message);
        }
        if self.low_storage(control_session_id, correlation_id) {
            return None;
        }
        self.start_recording(
            control_session_id,
            correlation_id,
            stream_id,
            location,
            auto_stop,
            channel,
            Some(recording_id),
        )
    }

    fn stop_recording(&mut self, control_session_id: i64, correlation_id: i64, stream_id: i32, channel: &str) {
        let uri = match ChannelUri::parse(channel) {
            Ok(uri) => uri,
            Err(e) => {
                self.error(e.clone());
                return self.fail(control_session_id, correlation_id, error_code::GENERIC, &e);
            }
        };
        match self.by_key.remove(&channel_uri::recording_key(stream_id, &uri)) {
            Some(token) => {
                self.abort_recordings_and_release(token, "stop recording");
                self.ok(control_session_id, correlation_id, 0);
            }
            None => {
                let message = format!("no recording found for streamId={stream_id} channel={channel}");
                self.fail(
                    control_session_id,
                    correlation_id,
                    error_code::UNKNOWN_SUBSCRIPTION,
                    &message,
                );
            }
        }
    }

    /// Removes the subscription a stop finds by its registration id.
    fn remove_by_registration_id(&mut self, subscription_id: i64) -> Option<SubscriptionToken> {
        let subscriptions = &self.subscriptions;
        let key = self
            .by_key
            .iter()
            .find(|(_, t)| {
                subscriptions
                    .get(t)
                    .is_some_and(|s| s.registration_id == subscription_id)
            })
            .map(|(k, _)| k.clone())?;
        self.by_key.remove(&key)
    }

    fn stop_recording_subscription(&mut self, subscription_id: i64) -> bool {
        match self.remove_by_registration_id(subscription_id) {
            Some(token) => {
                self.abort_recordings_and_release(token, "stop recording");
                true
            }
            None => false,
        }
    }

    fn stop_recording_by_identity(&mut self, control_session_id: i64, correlation_id: i64, recording_id: i64) {
        if !self.catalog.has_recording(recording_id) {
            return self.fail(
                control_session_id,
                correlation_id,
                error_code::UNKNOWN_RECORDING,
                &unknown_recording(recording_id),
            );
        }
        let mut found = 0;
        if let Some(recording) = self.recordings.get(&recording_id) {
            let (token, subscription_id) = (recording.subscription, recording.subscription_id);
            self.abort_recording(recording_id);
            if self.remove_by_registration_id(subscription_id).is_some() {
                found = 1;
                self.release(token);
            }
        }
        self.ok(control_session_id, correlation_id, found);
    }

    fn abort_recording(&mut self, recording_id: i64) {
        if let Some(recording) = self.recordings.get_mut(&recording_id)
            && !recording.aborted
        {
            recording.aborted = true;
            self.actions.push_back(Action::AbortRecorder { recording_id });
        }
    }

    fn abort_recordings_on(&mut self, token: SubscriptionToken) {
        let ids: Vec<i64> = self
            .recordings
            .iter()
            .filter(|(_, r)| r.subscription == token)
            .map(|(&id, _)| id)
            .collect();
        for id in ids {
            self.abort_recording(id);
        }
    }

    /// Drops the subscription's own reference: closed once none is left.
    fn release(&mut self, token: SubscriptionToken) {
        let count = self.ref_counts.entry(token).or_insert(0);
        *count -= 1;
        if *count <= 0 {
            self.ref_counts.remove(&token);
            self.close_subscription(token);
        }
    }

    /// `abortRecordingSessionAndCloseSubscription`: a stop.
    fn abort_recordings_and_release(&mut self, token: SubscriptionToken, _reason: &str) {
        self.abort_recordings_on(token);
        self.release(token);
    }

    /// `closeAndRemoveRecordingSubscription`.
    fn close_and_remove_subscription(&mut self, token: SubscriptionToken) {
        self.ref_counts.remove(&token);
        self.abort_recordings_on(token);
        self.by_key.retain(|_, t| *t != token);
        self.close_subscription(token);
    }

    fn close_subscription(&mut self, token: SubscriptionToken) {
        if self.subscriptions.remove(&token).is_some() {
            self.actions
                .push_back(Action::CloseRecordingSubscription { subscription: token });
        }
    }

    /// The outcome of I/O the conductor asked for.
    pub fn on_io(&mut self, io: Io) {
        match io {
            Io::ResponsePublication {
                control_session_id,
                result,
            } => {
                let Some(session) = self.session_mut(control_session_id) else {
                    return;
                };
                if let Some(message) = session.on_publication(result) {
                    self.error(message);
                }
            }
            Io::ControlSessionCounter {
                control_session_id,
                result,
            } => {
                let Some(session) = self.session_mut(control_session_id) else {
                    return;
                };
                if let Some(message) = session.on_counter(result) {
                    self.error(message);
                }
            }
            Io::RecordingSubscription { subscription, result } => self.on_recording_subscription(subscription, result),
            Io::RecordingImage { subscription, image } => self.on_recording_image(subscription, image),
            Io::RecorderStopped {
                recording_id,
                position,
                error,
            } => self.close_recording(recording_id, position, error),
            Io::ReplayPublication { publication, result } => self.on_replay_publication(publication, result),
            Io::ReplayStarted { replay_session_id } => {
                if let Some(replay) = self.replays.get(&replay_session_id) {
                    let (control_session_id, correlation_id) = (replay.control_session_id, replay.correlation_id);
                    self.ok(control_session_id, correlation_id, replay_session_id);
                }
            }
            Io::ReplayEnded {
                replay_session_id,
                error,
            } => self.close_replay(replay_session_id, error),
            Io::SegmentFiles {
                recording_id,
                file_names,
            } => self.on_segment_files(recording_id, file_names),
            Io::SegmentErased { recording_id, result } => self.on_segment_erased(recording_id, result),
            Io::SegmentFileDeleted { recording_id, result } => self.on_segment_file_deleted(recording_id, result),
            Io::SourceLinked { replication_id, result } => self.on_source_linked(replication_id, result),
            Io::SourceMessage {
                replication_id,
                message,
            } => self.on_source_message(replication_id, &message),
        }
    }

    fn on_recording_subscription(&mut self, token: SubscriptionToken, result: Result<i64, String>) {
        let Some(subscription) = self.subscriptions.get_mut(&token) else {
            return;
        };
        let (control_session_id, correlation_id) = (subscription.control_session_id, subscription.correlation_id);
        match result {
            Ok(registration_id) => {
                subscription.registration_id = registration_id;
                self.ref_counts.insert(token, 1);
                self.ok(control_session_id, correlation_id, registration_id);
            }
            Err(e) => {
                self.subscriptions.remove(&token);
                self.by_key.retain(|_, t| *t != token);
                self.error(e.clone());
                self.fail(control_session_id, correlation_id, error_code::GENERIC, &e);
            }
        }
    }

    fn on_recording_image(&mut self, token: SubscriptionToken, image: ImageInfo) {
        let Some(subscription) = self.subscriptions.get(&token).cloned() else {
            return;
        };
        match subscription.extends {
            None => self.start_recording_session(token, &subscription, image),
            Some(recording_id) => {
                if let Err((code, message)) = self.extend_recording_session(token, &subscription, recording_id, &image)
                {
                    self.fail(
                        subscription.control_session_id,
                        subscription.correlation_id,
                        code,
                        &message,
                    );
                    self.warn(message);
                    if subscription.auto_stop {
                        self.close_and_remove_subscription(token);
                    }
                }
            }
        }
    }

    fn start_recording_session(
        &mut self,
        token: SubscriptionToken,
        subscription: &RecordingSubscription,
        image: ImageInfo,
    ) {
        if let Some((code, message)) = self.refuse_recording_image() {
            self.fail(
                subscription.control_session_id,
                subscription.correlation_id,
                code,
                &message,
            );
            return self.warn(message);
        }
        let segment_file_length = recording_segment_length(self.config.segment_file_length, image.term_buffer_length);
        let descriptor = Descriptor {
            control_session_id: 0,
            correlation_id: 0,
            recording_id: NULL_POSITION,
            start_timestamp: self.now_ms,
            stop_timestamp: NULL_TIMESTAMP,
            start_position: image.join_position,
            stop_position: NULL_POSITION,
            initial_term_id: image.initial_term_id,
            segment_file_length,
            term_buffer_length: image.term_buffer_length,
            mtu_length: image.mtu_length,
            session_id: image.session_id,
            stream_id: subscription.stream_id,
            stripped_channel: subscription.stripped_channel.clone(),
            original_channel: subscription.original_channel.clone(),
            source_identity: image.source_identity.clone(),
        };
        let recording_id = match self.catalog.add_recording(&descriptor) {
            Ok(id) => id,
            Err(e) => return self.error(format!("ERROR - {e}")),
        };
        let writer = SegmentWriter::new(
            recording_id,
            image.join_position,
            image.join_position,
            image.term_buffer_length,
            segment_file_length,
        );
        self.start_recorder(token, subscription, recording_id, writer, &image);
        self.signal(
            subscription.control_session_id,
            subscription.correlation_id,
            recording_id,
            subscription.registration_id,
            image.join_position,
            Signal::START,
        );
    }

    fn extend_recording_session(
        &mut self,
        token: SubscriptionToken,
        subscription: &RecordingSubscription,
        recording_id: i64,
        image: &ImageInfo,
    ) -> Result<(), (i64, String)> {
        let stream_id = subscription.stream_id;
        let channel = &subscription.original_channel;
        if self.recordings.contains_key(&recording_id) {
            return Err((
                error_code::ACTIVE_RECORDING,
                format!("cannot extend active recording {recording_id} streamId={stream_id} channel={channel}"),
            ));
        }
        if let Some(refused) = self.refuse_recording_image() {
            return Err(refused);
        }
        let recorded = self
            .catalog
            .descriptor(recording_id)
            .map_err(|_| (error_code::UNKNOWN_RECORDING, unknown_recording(recording_id)))?;
        if self.delete_outstanding(recording_id, recorded.stop_position) {
            return Err((
                error_code::GENERIC,
                format!(
                    "cannot extend recording {recording_id} due to an outstanding delete operation: \
                     streamId={stream_id} channel={channel}"
                ),
            ));
        }
        let mismatch = |what: &str, image: i64, recorded: i64| {
            (
                error_code::INVALID_EXTENSION,
                format!("cannot extend recording {recording_id} image.{what}={image} != rec.{what}={recorded}"),
            )
        };
        if image.join_position != recorded.stop_position {
            return Err((
                error_code::INVALID_EXTENSION,
                format!(
                    "cannot extend recording {recording_id} image.joinPosition={} != rec.stopPosition={}",
                    image.join_position, recorded.stop_position
                ),
            ));
        }
        if image.initial_term_id != recorded.initial_term_id {
            return Err(mismatch(
                "initialTermId",
                image.initial_term_id.into(),
                recorded.initial_term_id.into(),
            ));
        }
        if image.term_buffer_length != recorded.term_buffer_length {
            return Err(mismatch(
                "termBufferLength",
                image.term_buffer_length.into(),
                recorded.term_buffer_length.into(),
            ));
        }
        if image.mtu_length != recorded.mtu_length {
            return Err(mismatch(
                "mtuLength",
                image.mtu_length.into(),
                recorded.mtu_length.into(),
            ));
        }
        let writer = SegmentWriter::new(
            recording_id,
            recorded.start_position,
            image.join_position,
            recorded.term_buffer_length,
            recorded.segment_file_length,
        );
        self.start_recorder(token, subscription, recording_id, writer, image);
        if let Err(e) = self.catalog.extend_recording(
            recording_id,
            subscription.control_session_id,
            subscription.correlation_id,
            image.session_id,
        ) {
            self.error(format!("ERROR - {e}"));
        }
        self.signal(
            subscription.control_session_id,
            subscription.correlation_id,
            recording_id,
            subscription.registration_id,
            image.join_position,
            Signal::EXTEND,
        );
        Ok(())
    }

    fn start_recorder(
        &mut self,
        token: SubscriptionToken,
        subscription: &RecordingSubscription,
        recording_id: i64,
        writer: SegmentWriter,
        image: &ImageInfo,
    ) {
        let (counter_key, counter_label) = archive_counters::recording_position(
            self.config.archive_id,
            recording_id,
            image.session_id,
            subscription.stream_id,
            &subscription.stripped_channel,
            &image.source_identity,
        );
        let block_length_limit = image.term_buffer_length.min(self.config.file_io_max_length) as usize;
        self.actions.push_back(Action::StartRecorder {
            recording_id,
            subscription: token,
            image_correlation_id: image.correlation_id,
            writer,
            block_length_limit,
            counter_key,
            counter_label,
        });
        *self.ref_counts.entry(token).or_insert(0) += 1;
        self.recordings.insert(
            recording_id,
            Recording {
                subscription: token,
                subscription_id: subscription.registration_id,
                control_session_id: subscription.control_session_id,
                correlation_id: subscription.correlation_id,
                auto_stop: subscription.auto_stop,
                position: image.join_position,
                aborted: false,
            },
        );
    }

    /// A recording's position after a block: what its counter now shows.
    pub fn on_recorded(&mut self, recording_id: i64, position: i64) {
        if let Some(recording) = self.recordings.get_mut(&recording_id) {
            recording.position = position;
        }
    }

    /// `closeRecordingSession`: a recording ended at `position`.
    fn close_recording(&mut self, recording_id: i64, position: i64, error: Option<RecordingError>) {
        let Some(recording) = self.recordings.remove(&recording_id) else {
            return;
        };
        if let Err(e) = self.catalog.recording_stopped(recording_id, position, self.now_ms) {
            self.error(format!("ERROR - {e}"));
        }
        if let Some(error) = error {
            self.fail(
                recording.control_session_id,
                recording.correlation_id,
                error.code,
                &error.message,
            );
        }
        self.signal(
            recording.control_session_id,
            recording.correlation_id,
            recording_id,
            recording.subscription_id,
            position,
            Signal::STOP,
        );
        let token = recording.subscription;
        let count = self.ref_counts.entry(token).or_insert(0);
        *count -= 1;
        if *count <= 0 || recording.auto_stop {
            self.close_and_remove_subscription(token);
        }
    }

    /// Runs the control sessions, newest first as Java's session worker:
    /// connects, answers, pings, listings; closes the sessions done.
    pub fn do_work(&mut self, out: &mut impl ResponseChannels) -> usize {
        let mut work = 0;
        let mut i = self.sessions.len();
        while i > 0 {
            i -= 1;
            let mut cx = Cx {
                config: &self.config,
                catalog: &self.catalog,
                now_ms: self.now_ms,
                actions: &mut self.actions,
            };
            work += self.sessions[i].do_work(&mut cx, out);
            if self.sessions[i].state == SessionState::Done {
                let session = self.sessions.swap_remove(i);
                self.close_session(session);
                work += 1;
            }
        }
        work + self.delete_work() + self.replication_work(out)
    }

    fn close_session(&mut self, session: ControlSession) {
        self.actions.push_back(Action::CloseControlSession {
            control_session_id: session.id,
        });
        let Some(reason) = session.abort_reason else { return };
        let aborted = reason != "session closed"
            && reason != control_session::NOT_CONNECTED
            && !reason.starts_with("control request publication image unavailable");
        if aborted {
            self.actions.push_back(Action::RejectImage {
                image_correlation_id: session.image,
                reason: reason.clone(),
            });
            self.warn(format!(
                "controlSessionId={} (controlResponseStreamId={} controlResponseChannel={}) terminated: {reason}",
                session.id, session.response_stream_id, session.response_channel
            ));
        }
    }
}
