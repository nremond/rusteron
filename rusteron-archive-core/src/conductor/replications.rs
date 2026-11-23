//! Replication, the destination's side: Java's `ArchiveConductor.replicate`
//! and `ReplicationSession`, for what Aeron Cluster asks of it. The archive
//! connects to the source archive as a client, fetches the recording's
//! descriptor, creates or extends its own copy, has the source replay the
//! recording to it, and records the replay.
//!
//! ```text
//! Link → Connect → Descriptor → Extend → Replay → AwaitImage → Replicate → Done
//! signals, with the replication's id: REPLICATE (a new copy), EXTEND (its recording starts),
//!   SYNC (as far as a stopped source), REPLICATE_END (always, last but STOP), STOP (the recording)
//! ```
//!
//! Not served, refused with an error: live merge (a live destination),
//! tagged replication, response channels. Credentials are passed on to the
//! source; a challenge from it is not answered.

use super::{Action, Conductor, Offer, ResponseChannels, SubscriptionToken};
use crate::catalog::{Descriptor, NULL_POSITION};
use crate::channel::ChannelUri;
use crate::client::{self, Response};
use crate::counters::{ARCHIVE_GIT_SHA, ARCHIVE_VERSION};
use crate::protocol::{Code, Location, PROTOCOL_SEMANTIC_VERSION, Replicate, Signal, error_code};

/// The archive client's control response stream.
pub const CONTROL_RESPONSE_STREAM_ID: i32 = 20;
/// Java's client asks a source of at least this version for its archive id.
const ARCHIVE_ID_SINCE_VERSION: i32 = 0x01_0B_00;
const SEND_ATTEMPTS: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    /// The source's publication and subscription being added.
    Link,
    /// Connecting: the request publication's connection, then the answers.
    Connect(ConnectStep),
    Descriptor,
    Extend,
    Replay,
    AwaitImage,
    Replicate,
    Done,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnectStep {
    PublicationConnected,
    ConnectResponse(i64),
    ArchiveIdResponse(i64),
}

impl ConnectStep {
    /// Java's `AsyncConnect` step, for the timeout's message.
    fn name(self) -> &'static str {
        match self {
            ConnectStep::PublicationConnected => "AWAIT_PUBLICATION_CONNECTED",
            ConnectStep::ConnectResponse(_) => "AWAIT_CONNECT_RESPONSE",
            ConnectStep::ArchiveIdResponse(_) => "AWAIT_ARCHIVE_ID_RESPONSE",
        }
    }
}

pub(crate) struct Replication {
    control_session_id: i64,
    src_recording_id: i64,
    dst_recording_id: i64,
    dst_stop_position: i64,
    replication_session_id: i32,
    file_io_max_length: i32,
    replication_channel: String,
    /// What the source's authenticator is shown.
    encoded_credentials: Vec<u8>,
    state: State,
    src_control_session_id: i64,
    active_correlation_id: i64,
    action_deadline_ms: i64,
    connect_deadline_ms: i64,
    src_stop_position: i64,
    replay_stream_id: i32,
    replay_session_id: i32,
    replay_position: i64,
    is_destination_empty: bool,
    src_replay_session_id: i64,
    subscription: Option<SubscriptionToken>,
    subscription_channel: String,
    /// Why it ended, with an error for the client if it failed.
    error: Option<(i64, String)>,
}

/// Offers a request to the source up to [`SEND_ATTEMPTS`] times.
fn offer(out: &mut impl ResponseChannels, replication_id: i64, message: &[u8]) -> bool {
    (0..SEND_ATTEMPTS).any(|_| out.offer_source(replication_id, message) == Offer::Sent)
}

impl Conductor {
    /// `ArchiveConductor.replicate`: the checks, then the session; OK with its id.
    pub(super) fn replicate(&mut self, r: Replicate) {
        let (cs, corr) = (r.control_session_id, r.correlation_id);
        let channel = if r.replication_channel.is_empty() {
            self.config.replication_channel.clone()
        } else {
            r.replication_channel.clone()
        };
        let uri = match ChannelUri::parse(&channel) {
            Ok(uri) => uri,
            Err(e) => {
                self.error(e.clone());
                return self.fail(cs, corr, error_code::GENERIC, &e);
            }
        };
        let refusal = if !r.live_destination.is_empty() {
            Some("live merge is not supported by this archive")
        } else if r.channel_tag_id != NULL_POSITION || r.subscription_tag_id != NULL_POSITION {
            Some("tagged replication is not supported by this archive")
        } else if uri.get("control-mode") == Some("response")
            || r.src_response_channel.contains("control-mode=response")
        {
            Some("response channel replication is not supported by this archive")
        } else {
            None
        };
        if let Some(message) = refusal {
            return self.fail(cs, corr, error_code::GENERIC, message);
        }
        let dst = r.dst_recording_id;
        let has_recording = self.catalog.has_recording(dst);
        if dst != NULL_POSITION && !has_recording {
            let message = format!("unknown destination recording id {dst}");
            return self.fail(cs, corr, error_code::UNKNOWN_RECORDING, &message);
        }
        let mut replay_position = NULL_POSITION;
        let mut replay_stream_id = 0;
        let mut is_destination_empty = false;
        if has_recording {
            let d = self.catalog.descriptor(dst).expect("indexed");
            if d.stop_position == NULL_POSITION || self.recordings.contains_key(&dst) {
                let message = format!("cannot replicate to active recording {dst}");
                return self.fail(cs, corr, error_code::ACTIVE_RECORDING, &message);
            }
            replay_position = d.stop_position;
            replay_stream_id = d.stream_id;
            is_destination_empty = d.start_position == d.stop_position;
        }
        let response_channel = if r.src_response_channel.is_empty() {
            self.config.archive_client_response_channel.clone()
        } else {
            r.src_response_channel.clone()
        };
        let replication_id = self.next_session_id;
        self.next_session_id += 1;
        let now = self.now_ms;
        let timeout = self.config.message_timeout_ms;
        self.replications.insert(
            replication_id,
            Replication {
                control_session_id: cs,
                src_recording_id: r.src_recording_id,
                dst_recording_id: dst,
                dst_stop_position: r.stop_position,
                replication_session_id: r.replication_session_id,
                file_io_max_length: r.file_io_max_length,
                replication_channel: channel,
                encoded_credentials: r.encoded_credentials.clone(),
                state: State::Link,
                src_control_session_id: NULL_POSITION,
                active_correlation_id: NULL_POSITION,
                action_deadline_ms: now + timeout,
                connect_deadline_ms: now + timeout,
                src_stop_position: NULL_POSITION,
                replay_stream_id,
                replay_session_id: 0,
                replay_position,
                is_destination_empty,
                src_replay_session_id: NULL_POSITION,
                subscription: None,
                subscription_channel: String::new(),
                error: None,
            },
        );
        self.actions.push_back(Action::ConnectSourceArchive {
            replication_id,
            request_channel: r.src_control_channel,
            request_stream_id: r.src_control_stream_id,
            response_channel,
            response_stream_id: CONTROL_RESPONSE_STREAM_ID,
        });
        self.ok(cs, corr, replication_id);
    }

    /// `stopReplication`: from any session.
    pub(super) fn stop_replication(&mut self, cs: i64, corr: i64, replication_id: i64) {
        match self.replications.get_mut(&replication_id) {
            Some(r) if r.state != State::Done => {
                r.state = State::Done;
                self.ok(cs, corr, 0);
            }
            _ => {
                let message = format!("unknown replication id {replication_id}");
                self.fail(cs, corr, error_code::UNKNOWN_REPLICATION, &message);
            }
        }
    }

    /// The source's publication and subscription, added or not; added, the
    /// response channel as the source is to answer on (its port resolved).
    pub(super) fn on_source_linked(&mut self, replication_id: i64, result: Result<String, String>) {
        let Some(r) = self.replications.get_mut(&replication_id) else {
            return;
        };
        match result {
            Ok(channel) => {
                r.subscription_channel = channel;
                r.state = State::Connect(ConnectStep::PublicationConnected);
            }
            Err(message) => {
                r.error = Some((
                    error_code::REPLICATION_CONNECTION_FAILURE,
                    format!("Replication connection failed=ERROR - {message}"),
                ));
                r.state = State::Done;
            }
        }
    }

    /// A message from the source archive.
    pub(super) fn on_source_message(&mut self, replication_id: i64, message: &[u8]) {
        let Ok(response) = client::decode_response(message) else {
            return;
        };
        let Some(r) = self.replications.get_mut(&replication_id) else {
            return;
        };
        let now = self.now_ms;
        let timeout = self.config.message_timeout_ms;
        match (r.state, &response) {
            (
                State::Connect(ConnectStep::ConnectResponse(c)),
                Response::Control {
                    control_session_id,
                    correlation_id,
                    code,
                    version,
                    message,
                    ..
                },
            ) if *correlation_id == c => {
                if *code != Code::OK {
                    r.error = Some((
                        error_code::REPLICATION_CONNECTION_FAILURE,
                        format!("Replication connection failed=ERROR - {message}"),
                    ));
                    r.state = State::Done;
                    return;
                }
                r.src_control_session_id = *control_session_id;
                if *version >= ARCHIVE_ID_SINCE_VERSION {
                    let c = self.next_correlation_id();
                    let r = self.replications.get_mut(&replication_id).expect("present");
                    r.state = State::Connect(ConnectStep::ArchiveIdResponse(c));
                    self.source_requests
                        .push((replication_id, client::archive_id(*control_session_id, c)));
                } else {
                    self.connected_to_source(replication_id);
                }
            }
            (
                State::Connect(ConnectStep::ArchiveIdResponse(c)),
                Response::Control {
                    correlation_id, code, ..
                },
            ) if *correlation_id == c && *code == Code::OK => self.connected_to_source(replication_id),
            (
                State::Descriptor,
                Response::Descriptor {
                    control_session_id,
                    correlation_id,
                    descriptor,
                },
            ) if *control_session_id == r.src_control_session_id && *correlation_id == r.active_correlation_id => {
                let descriptor = descriptor.clone();
                self.on_recording_descriptor(replication_id, descriptor);
            }
            (
                State::Descriptor,
                Response::Control {
                    control_session_id,
                    correlation_id,
                    code,
                    message,
                    ..
                },
            ) if *control_session_id == r.src_control_session_id && *correlation_id == r.active_correlation_id => {
                if *code == Code::RECORDING_UNKNOWN {
                    r.error = Some((
                        error_code::UNKNOWN_RECORDING,
                        format!("unknown src recording id {}", r.src_recording_id),
                    ));
                } else if *code == Code::ERROR {
                    r.error = Some((
                        error_code::GENERIC,
                        format!("ERROR - response for correlationId={correlation_id}, error: {message}"),
                    ));
                }
                r.state = State::Done;
            }
            (
                _,
                Response::Control {
                    control_session_id,
                    correlation_id,
                    relevant_id,
                    code,
                    message,
                    ..
                },
            ) if *control_session_id == r.src_control_session_id => {
                if *code == Code::ERROR {
                    r.error = Some((error_code::GENERIC, format!("ERROR - {message}")));
                    r.state = State::Done;
                } else if *code == Code::OK && *correlation_id == r.active_correlation_id && r.state == State::Replay {
                    r.src_replay_session_id = *relevant_id;
                    r.active_correlation_id = NULL_POSITION;
                    r.action_deadline_ms = now + timeout;
                    r.state = State::AwaitImage;
                }
            }
            _ => {}
        }
    }

    /// Connected: the keep-alive Java's client sends, then the descriptor.
    fn connected_to_source(&mut self, replication_id: i64) {
        let now = self.now_ms;
        let timeout = self.config.message_timeout_ms;
        let r = self.replications.get_mut(&replication_id).expect("present");
        let keep_alive = client::keep_alive(r.src_control_session_id, NULL_POSITION);
        r.state = State::Descriptor;
        r.active_correlation_id = NULL_POSITION;
        r.action_deadline_ms = now + timeout;
        self.source_requests.push((replication_id, keep_alive));
    }

    fn next_correlation_id(&mut self) -> i64 {
        self.next_correlation_id += 1;
        self.next_correlation_id
    }

    /// `onRecordingDescriptor`: the copy created, replaced or kept; in sync already, or not.
    fn on_recording_descriptor(&mut self, replication_id: i64, d: Descriptor) {
        let now = self.now_ms;
        let timeout = self.config.message_timeout_ms;
        let r = self.replications.get_mut(&replication_id).expect("present");
        r.src_stop_position = d.stop_position;
        r.replay_stream_id = d.stream_id;
        r.replay_session_id = if r.replication_session_id != -1 {
            r.replication_session_id
        } else {
            d.session_id
        };
        if r.file_io_max_length != -1 && r.file_io_max_length < d.mtu_length {
            r.error = Some((
                error_code::GENERIC,
                format!(
                    "Replication fileIoMaxLength ({}) is less than than the recording mtuLength ({})",
                    r.file_io_max_length, d.mtu_length
                ),
            ));
            r.state = State::Done;
            return;
        }
        let copy = Descriptor {
            control_session_id: 0,
            correlation_id: 0,
            recording_id: NULL_POSITION,
            stop_position: d.start_position,
            stop_timestamp: d.start_timestamp,
            ..d.clone()
        };
        let (cs, dst, empty) = (r.control_session_id, r.dst_recording_id, r.is_destination_empty);
        if dst == NULL_POSITION {
            r.replay_position = d.start_position;
            match self.catalog.add_recording(&copy) {
                Ok(id) => {
                    let r = self.replications.get_mut(&replication_id).expect("present");
                    r.dst_recording_id = id;
                    self.signal(
                        cs,
                        replication_id,
                        id,
                        NULL_POSITION,
                        d.start_position,
                        Signal::REPLICATE,
                    );
                }
                Err(e) => {
                    let r = self.replications.get_mut(&replication_id).expect("present");
                    r.error = Some((error_code::GENERIC, format!("ERROR - {e}")));
                    r.state = State::Done;
                    return;
                }
            }
        } else if empty {
            r.replay_position = d.start_position;
            if let Err(e) = self.catalog.replace_recording(dst, &copy) {
                self.error(format!("ERROR - {e}"));
            }
        }
        let dst = self.replications[&replication_id].dst_recording_id;
        let in_sync =
            d.start_position == d.stop_position || self.catalog.stop_position(dst).ok() == Some(d.stop_position);
        if in_sync {
            self.signal(cs, replication_id, dst, NULL_POSITION, d.stop_position, Signal::SYNC);
        }
        let r = self.replications.get_mut(&replication_id).expect("present");
        r.state = if in_sync { State::Done } else { State::Extend };
        r.active_correlation_id = NULL_POSITION;
        r.action_deadline_ms = now + timeout;
    }

    /// Each replication's step this cycle; the ones done closed.
    pub(super) fn replication_work(&mut self, out: &mut impl ResponseChannels) -> usize {
        let mut work = 0;
        for (replication_id, message) in std::mem::take(&mut self.source_requests) {
            if !offer(out, replication_id, &message)
                && let Some(r) = self.replications.get_mut(&replication_id)
                && r.state != State::Done
            {
                r.error = Some((
                    error_code::GENERIC,
                    "ERROR - connection to the archive is no longer available".into(),
                ));
                r.state = State::Done;
            }
            work += 1;
        }
        let ids: Vec<i64> = self.replications.keys().copied().collect();
        for id in ids {
            work += self.replication_step(id, out);
            if self.replications.get(&id).is_some_and(|r| r.state == State::Done) {
                self.close_replication(id, out);
                work += 1;
            }
        }
        work
    }

    fn replication_step(&mut self, id: i64, out: &mut impl ResponseChannels) -> usize {
        let now = self.now_ms;
        let timeout = self.config.message_timeout_ms;
        let Some(r) = self.replications.get(&id) else {
            return 0;
        };
        // Its recording's subscription gone: done, synced if it got as far.
        if let Some(token) = r.subscription
            && !self.subscriptions.contains_key(&token)
        {
            let position = self.catalog.stop_position(r.dst_recording_id).unwrap_or(NULL_POSITION);
            let synced =
                r.state == State::Replicate && r.src_stop_position != NULL_POSITION && position >= r.src_stop_position;
            let (cs, dst) = (r.control_session_id, r.dst_recording_id);
            if synced {
                self.signal(cs, id, dst, NULL_POSITION, position, Signal::SYNC);
            }
            let r = self.replications.get_mut(&id).expect("present");
            r.subscription = None;
            r.state = State::Done;
            return 1;
        }
        let timed_out = now >= r.action_deadline_ms;
        let fail = |this: &mut Conductor, message: String| {
            let r = this.replications.get_mut(&id).expect("present");
            r.error = Some((error_code::GENERIC, message));
            r.state = State::Done;
            1
        };
        match r.state {
            State::Link | State::Connect(_) => {
                if now >= r.connect_deadline_ms {
                    let step = match r.state {
                        State::Connect(step) => step.name(),
                        _ => "ADD_PUBLICATION",
                    };
                    let message = format!("Replication connection failed=ERROR - Archive connect timeout: step={step}");
                    let r = self.replications.get_mut(&id).expect("present");
                    r.error = Some((error_code::REPLICATION_CONNECTION_FAILURE, message));
                    r.state = State::Done;
                    return 1;
                }
                if r.state == State::Connect(ConnectStep::PublicationConnected) && out.source_connected(id) {
                    let c = self.next_correlation_id();
                    let client_info = format!(
                        "name=archive archiveId={} replicationSessionId={id} version={ARCHIVE_VERSION} \
                         commit={ARCHIVE_GIT_SHA}",
                        self.config.archive_id
                    );
                    let r = self.replications.get_mut(&id).expect("present");
                    let connect = client::auth_connect_with(
                        c,
                        CONTROL_RESPONSE_STREAM_ID,
                        PROTOCOL_SEMANTIC_VERSION,
                        &r.subscription_channel,
                        &r.encoded_credentials,
                        &client_info,
                    );
                    if offer(out, id, &connect) {
                        r.state = State::Connect(ConnectStep::ConnectResponse(c));
                        return 1;
                    }
                }
                0
            }
            State::Descriptor => {
                if r.active_correlation_id == NULL_POSITION {
                    let c = self.next_correlation_id();
                    let r = self.replications.get_mut(&id).expect("present");
                    let request = client::list_recording(r.src_control_session_id, c, r.src_recording_id);
                    if offer(out, id, &request) {
                        r.active_correlation_id = c;
                        r.action_deadline_ms = now + timeout;
                        return 1;
                    }
                    if timed_out {
                        return fail(self, "ERROR - failed to list remote recording descriptor".into());
                    }
                    return 0;
                }
                if timed_out {
                    return fail(self, "ERROR - failed to fetch remote recording descriptor".into());
                }
                0
            }
            State::Extend => {
                let mut uri = ChannelUri::parse(&r.replication_channel).expect("parsed at the request");
                uri.put("rejoin", "false");
                uri.put("session-id", &r.replay_session_id.to_string());
                let channel = uri.to_string();
                let (cs, dst, stream_id) = (r.control_session_id, r.dst_recording_id, r.replay_stream_id);
                let token = self.extend_recording(cs, id, dst, stream_id, Location::Remote, true, channel.clone());
                let r = self.replications.get_mut(&id).expect("present");
                match token {
                    Some(token) => {
                        r.subscription = Some(token);
                        r.subscription_channel = channel;
                        r.state = State::Replay;
                        r.active_correlation_id = NULL_POSITION;
                        r.action_deadline_ms = now + timeout;
                    }
                    None => r.state = State::Done,
                }
                1
            }
            State::Replay => {
                if r.active_correlation_id != NULL_POSITION {
                    if timed_out {
                        let message = format!(
                            "ERROR - failed get acknowledgement of replay request to: {}",
                            r.replication_channel
                        );
                        return fail(self, message);
                    }
                    return 0;
                }
                let token = r.subscription.expect("extended");
                let mut uri = ChannelUri::parse(&r.replication_channel).expect("parsed at the request");
                if uri.get("endpoint").is_some() {
                    let Some(resolved) = out.resolved_endpoint(token) else {
                        if timed_out {
                            let message = format!(
                                "ERROR - failed to resolve subscription endpoint: channel={}",
                                r.subscription_channel
                            );
                            return fail(self, message);
                        }
                        return 0;
                    };
                    if let Err(e) = uri.replace_endpoint_wildcard_port(&resolved) {
                        return fail(self, e);
                    }
                }
                uri.put("session-id", &r.replay_session_id.to_string());
                let length = if r.dst_stop_position == NULL_POSITION {
                    NULL_POSITION
                } else {
                    r.dst_stop_position - r.replay_position
                };
                let c = self.next_correlation_id();
                let r = self.replications.get_mut(&id).expect("present");
                let request = client::replay(
                    r.src_control_session_id,
                    c,
                    r.src_recording_id,
                    r.replay_position,
                    length,
                    r.replay_stream_id,
                    r.file_io_max_length,
                    &uri.to_string(),
                );
                if offer(out, id, &request) {
                    r.active_correlation_id = c;
                    r.action_deadline_ms = now + timeout;
                    return 1;
                }
                if timed_out {
                    return fail(self, "ERROR - failed to send replay request".into());
                }
                0
            }
            State::AwaitImage => {
                let recording = self.recordings.get(&r.dst_recording_id);
                if recording.is_some_and(|rec| Some(rec.subscription) == r.subscription) {
                    let r = self.replications.get_mut(&id).expect("present");
                    r.state = State::Replicate;
                    return 1;
                }
                if timed_out {
                    let message = format!(
                        "ERROR - failed get replay image for sessionId={} on channel={}",
                        r.src_replay_session_id as i32, r.subscription_channel
                    );
                    return fail(self, message);
                }
                0
            }
            State::Replicate => {
                let Some(position) = self.recorded_position(r.dst_recording_id) else {
                    // Its image ended: the recording stopped.
                    let r = self.replications.get_mut(&id).expect("present");
                    r.src_replay_session_id = NULL_POSITION;
                    r.state = State::Done;
                    return 1;
                };
                let synced = r.src_stop_position != NULL_POSITION && position >= r.src_stop_position;
                if synced || (r.dst_stop_position != NULL_POSITION && position >= r.dst_stop_position) {
                    let (cs, dst, token) = (r.control_session_id, r.dst_recording_id, r.subscription);
                    if synced {
                        let subscription_id = token
                            .and_then(|t| self.subscriptions.get(&t))
                            .map_or(NULL_POSITION, |s| s.registration_id);
                        self.signal(cs, id, dst, subscription_id, position, Signal::SYNC);
                    }
                    let r = self.replications.get_mut(&id).expect("present");
                    r.src_replay_session_id = NULL_POSITION;
                    r.state = State::Done;
                    return 1;
                }
                0
            }
            State::Done => 0,
        }
    }

    /// `ReplicationSession.close`: its error first, REPLICATE_END, then the
    /// recording stopped (its STOP follows), the source's replay stopped and
    /// the connection closed.
    fn close_replication(&mut self, id: i64, out: &mut impl ResponseChannels) {
        let Some(r) = self.replications.remove(&id) else {
            return;
        };
        if let Some((code, message)) = &r.error {
            self.error(message.clone());
            self.fail(r.control_session_id, id, *code, message);
        }
        self.signal(
            r.control_session_id,
            id,
            r.dst_recording_id,
            NULL_POSITION,
            NULL_POSITION,
            Signal::REPLICATE_END,
        );
        if let Some(token) = r.subscription
            && self.subscriptions.contains_key(&token)
        {
            let registration_id = self.subscriptions[&token].registration_id;
            self.stop_recording_subscription(registration_id);
        }
        if r.src_control_session_id != NULL_POSITION {
            if r.src_replay_session_id != NULL_POSITION {
                let c = self.next_correlation_id();
                offer(
                    out,
                    id,
                    &client::stop_replay(r.src_control_session_id, c, r.src_replay_session_id),
                );
            }
            offer(out, id, &client::close_session(r.src_control_session_id));
        }
        self.actions
            .push_back(Action::CloseSourceArchive { replication_id: id });
    }

    pub fn replication_count(&self) -> usize {
        self.replications.len()
    }
}
