//! A control session, as Java's `ControlSession`: connected by a client's
//! connect request, answering on a publication of its own, until the client
//! closes it, its request image goes away, or its responses cannot be sent
//! for the connect timeout.

use std::collections::VecDeque;

use super::{Action, Config, Cx, Offer, ResponseChannels};
use crate::catalog::NULL_POSITION;
use crate::protocol::{self, Code};

/// Why a session whose client left ends: not an abort.
pub(crate) const NOT_CONNECTED: &str = "control response publication is not connected";
const CLOSED: &str = "control response publication is closed";
const MAX_POSITION: &str = "control response publication is at max position";
const RESEND_INTERVAL_MS: i64 = 200;
/// An offer is tried this many times before it waits for the next cycle.
const SEND_ATTEMPTS: usize = 3;
/// A listing looks at this many recordings a cycle at most.
const MAX_SCANS_PER_WORK_CYCLE: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SessionState {
    Init,
    Connecting,
    Connected,
    Authenticated,
    Active,
    Done,
}

impl SessionState {
    fn name(self) -> &'static str {
        match self {
            SessionState::Init => "INIT",
            SessionState::Connecting => "CONNECTING",
            SessionState::Connected => "CONNECTED",
            SessionState::Authenticated => "AUTHENTICATED",
            SessionState::Active => "ACTIVE",
            SessionState::Done => "DONE",
        }
    }
}

/// A message waiting to be sent: a response ends the session when the
/// publication is gone, a signal does not (Java offers signals without
/// checking the result).
pub(crate) enum Item {
    Response(Vec<u8>),
    Signal(Vec<u8>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Resource {
    /// To ask the shell for (again).
    Wanted,
    Pending,
    Ready,
}

enum ListingKind {
    ById {
        recording_id: i64,
    },
    /// By URI fragment and stream; every recording with no stream.
    ForUri {
        next_id: i64,
        remaining: i32,
        stream_id: Option<i32>,
        fragment: Vec<u8>,
    },
}

/// A listing in progress: a session has one at most.
pub(crate) struct Listing {
    correlation_id: i64,
    kind: ListingKind,
}

impl Listing {
    pub(crate) fn by_id(correlation_id: i64, recording_id: i64) -> Self {
        Listing {
            correlation_id,
            kind: ListingKind::ById { recording_id },
        }
    }

    pub(crate) fn for_uri(
        correlation_id: i64,
        from_recording_id: i64,
        count: i32,
        stream_id: Option<i32>,
        fragment: Vec<u8>,
    ) -> Self {
        Listing {
            correlation_id,
            kind: ListingKind::ForUri {
                next_id: from_recording_id,
                remaining: count,
                stream_id,
                fragment,
            },
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || haystack.windows(needle.len()).any(|w| w == needle)
}

/// Offers `message` up to [`SEND_ATTEMPTS`] times while back pressured.
fn offer(out: &mut impl ResponseChannels, control_session_id: i64, message: &[u8]) -> Offer {
    for _ in 0..SEND_ATTEMPTS {
        match out.offer(control_session_id, message) {
            Offer::Retry => continue,
            result => return result,
        }
    }
    Offer::Retry
}

pub(crate) struct ControlSession {
    pub(crate) id: i64,
    /// The correlation id of the image that connected it.
    pub(crate) image: i64,
    connect_correlation_id: i64,
    pub(crate) response_channel: String,
    pub(crate) response_stream_id: i32,
    invalid_version: Option<String>,
    pub(crate) state: SessionState,
    publication: Resource,
    counter: Resource,
    activity_deadline_ms: i64,
    resend_deadline_ms: i64,
    liveness_deadline_ms: i64,
    pub(crate) queue: VecDeque<Item>,
    pub(crate) listing: Option<Listing>,
    pub(crate) abort_reason: Option<String>,
}

impl ControlSession {
    /// A session whose publication and counter are being added.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: i64,
        image: i64,
        connect_correlation_id: i64,
        response_channel: String,
        response_stream_id: i32,
        invalid_version: Option<String>,
        now_ms: i64,
        config: &Config,
    ) -> Self {
        ControlSession {
            id,
            image,
            connect_correlation_id,
            response_channel,
            response_stream_id,
            invalid_version,
            state: SessionState::Init,
            publication: Resource::Pending,
            counter: Resource::Pending,
            activity_deadline_ms: now_ms + config.connect_timeout_ms,
            resend_deadline_ms: 0,
            liveness_deadline_ms: now_ms + config.session_liveness_check_interval_ms,
            queue: VecDeque::new(),
            listing: None,
            abort_reason: None,
        }
    }

    /// Ends the session; a client's close names the reason even after another.
    pub(crate) fn abort(&mut self, reason: &str) {
        if self.state != SessionState::Done || (self.abort_reason.is_some() && reason == "session closed") {
            self.abort_reason = Some(reason.to_owned());
            self.state = SessionState::Done;
            self.listing = None;
        }
    }

    /// A request from the client: the connect is acknowledged.
    pub(crate) fn attempt_to_activate(&mut self) {
        if self.state == SessionState::Authenticated && self.invalid_version.is_none() {
            self.state = SessionState::Active;
        }
    }

    /// The publication added, or not: an error to log if the session ends on it.
    pub(crate) fn on_publication(&mut self, result: Result<(), (bool, String)>) -> Option<String> {
        match result {
            Ok(()) => {
                self.publication = Resource::Ready;
                None
            }
            Err((true, _)) => {
                self.publication = Resource::Wanted;
                None
            }
            Err((false, message)) => {
                self.abort(&format!("failed with exception: {message}"));
                Some(message)
            }
        }
    }

    pub(crate) fn on_counter(&mut self, result: Result<(), String>) -> Option<String> {
        match result {
            Ok(()) => {
                self.counter = Resource::Ready;
                None
            }
            Err(message) => {
                self.abort(&format!("failed with exception: {message}"));
                Some(message)
            }
        }
    }

    /// Java's `updateActivityDeadline`: armed by a failure, if not already.
    fn update_activity_deadline(&mut self, now_ms: i64, connect_timeout_ms: i64) {
        if self.activity_deadline_ms == NULL_POSITION {
            self.activity_deadline_ms = now_ms + connect_timeout_ms;
        }
    }

    /// What `checkResult` does on a publication that is gone: the session ends.
    fn check(&mut self, result: Offer, cx: &mut Cx) {
        let (reason, message) = match result {
            Offer::NotConnected => (NOT_CONNECTED, format!("{NOT_CONNECTED}: controlSessionId={}", self.id)),
            Offer::Closed => (
                CLOSED,
                format!("ERROR - response publication is closed: controlSessionId={}", self.id),
            ),
            Offer::MaxPositionExceeded => (
                MAX_POSITION,
                format!(
                    "ERROR - response publication at max position: controlSessionId={}",
                    self.id
                ),
            ),
            Offer::Sent | Offer::Retry => return,
        };
        self.abort(reason);
        if result == Offer::NotConnected {
            cx.warn(message);
        } else {
            cx.actions.push_back(Action::Error { message });
        }
    }

    fn connect_response(&self) -> Vec<u8> {
        protocol::control_response(self.id, self.connect_correlation_id, self.id, Code::OK, "")
    }

    pub(crate) fn do_work(&mut self, cx: &mut Cx, out: &mut impl ResponseChannels) -> usize {
        let now = cx.now_ms;
        if self.activity_deadline_ms != NULL_POSITION && now > self.activity_deadline_ms {
            let reason = if self.state == SessionState::Active {
                format!(
                    "failed to send response for more than connectTimeoutMs={}",
                    cx.config.connect_timeout_ms
                )
            } else {
                format!("failed to establish initial connection: state={}", self.state.name())
            };
            self.abort(&reason);
            return 1;
        }
        match self.state {
            SessionState::Init => self.init(cx),
            SessionState::Connecting => {
                if out.is_connected(self.id) {
                    self.state = SessionState::Connected;
                    1
                } else {
                    0
                }
            }
            SessionState::Connected => self.connected(cx, out),
            SessionState::Authenticated => self.authenticated(cx, out),
            SessionState::Active => self.active(cx, out),
            SessionState::Done => 0,
        }
    }

    fn init(&mut self, cx: &mut Cx) -> usize {
        let mut work = 0;
        if self.publication == Resource::Wanted {
            cx.actions.push_back(Action::AddResponsePublication {
                control_session_id: self.id,
                channel: self.response_channel.clone(),
                stream_id: self.response_stream_id,
            });
            self.publication = Resource::Pending;
            work += 1;
        }
        if self.publication == Resource::Ready && self.counter == Resource::Ready {
            self.activity_deadline_ms = cx.now_ms + cx.config.connect_timeout_ms;
            self.state = SessionState::Connecting;
            work += 1;
        }
        work
    }

    fn connected(&mut self, cx: &mut Cx, out: &mut impl ResponseChannels) -> usize {
        if cx.now_ms <= self.resend_deadline_ms {
            return 0;
        }
        self.resend_deadline_ms = cx.now_ms + RESEND_INTERVAL_MS;
        if let Some(message) = self.invalid_version.clone() {
            let response =
                protocol::control_response(self.id, self.connect_correlation_id, self.id, Code::ERROR, &message);
            let result = offer(out, self.id, &response);
            self.check(result, cx);
        } else {
            let result = offer(out, self.id, &self.connect_response());
            if result == Offer::Sent {
                self.activity_deadline_ms = NULL_POSITION;
                self.state = SessionState::Authenticated;
            }
            self.check(result, cx);
        }
        1
    }

    /// Answers the connect again until the client's first request.
    fn authenticated(&mut self, cx: &mut Cx, out: &mut impl ResponseChannels) -> usize {
        if cx.now_ms <= self.resend_deadline_ms {
            return 0;
        }
        self.resend_deadline_ms = cx.now_ms + RESEND_INTERVAL_MS;
        let result = offer(out, self.id, &self.connect_response());
        if result == Offer::Sent {
            self.activity_deadline_ms = NULL_POSITION;
        }
        self.check(result, cx);
        1
    }

    fn active(&mut self, cx: &mut Cx, out: &mut impl ResponseChannels) -> usize {
        let mut work = 0;
        let now = cx.now_ms;
        if self.liveness_deadline_ms - now < 0 {
            self.liveness_deadline_ms = now + cx.config.session_liveness_check_interval_ms;
            if offer(out, self.id, &protocol::ping(self.id)) == Offer::Sent {
                self.activity_deadline_ms = NULL_POSITION;
            } else {
                self.update_activity_deadline(now, cx.config.connect_timeout_ms);
            }
            work += 1;
        }
        if !out.is_connected(self.id) {
            self.abort(NOT_CONNECTED);
            return work + 1;
        }
        work += self.send_queued(cx, out);
        if self.queue.is_empty() && self.state == SessionState::Active {
            work += self.list(cx, out);
            work += self.send_queued(cx, out);
        }
        work
    }

    /// Sends what is queued, in order, while the publication takes it.
    fn send_queued(&mut self, cx: &mut Cx, out: &mut impl ResponseChannels) -> usize {
        let mut work = 0;
        while let Some(item) = self.queue.front() {
            let (message, checked) = match item {
                Item::Response(m) => (m, true),
                Item::Signal(m) => (m, false),
            };
            match offer(out, self.id, message) {
                Offer::Sent => {
                    self.queue.pop_front();
                    self.activity_deadline_ms = NULL_POSITION;
                    work += 1;
                }
                result => {
                    self.update_activity_deadline(cx.now_ms, cx.config.connect_timeout_ms);
                    if checked {
                        self.check(result, cx);
                    }
                    break;
                }
            }
        }
        work
    }

    fn respond_unknown(&mut self, correlation_id: i64, recording_id: i64) {
        self.queue.push_back(Item::Response(protocol::control_response(
            self.id,
            correlation_id,
            recording_id,
            Code::RECORDING_UNKNOWN,
            "",
        )));
    }

    /// Sends a descriptor; whether it went.
    fn send_descriptor(&mut self, cx: &mut Cx, out: &mut impl ResponseChannels, correlation_id: i64, id: i64) -> bool {
        let Ok(bytes) = cx.catalog.descriptor_bytes(id) else {
            return true;
        };
        let message = protocol::recording_descriptor(self.id, correlation_id, bytes);
        match offer(out, self.id, &message) {
            Offer::Sent => {
                self.activity_deadline_ms = NULL_POSITION;
                true
            }
            result => {
                self.update_activity_deadline(cx.now_ms, cx.config.connect_timeout_ms);
                self.check(result, cx);
                false
            }
        }
    }

    /// A listing's work this cycle: descriptors, then the end.
    fn list(&mut self, cx: &mut Cx, out: &mut impl ResponseChannels) -> usize {
        let Some(mut listing) = self.listing.take() else {
            return 0;
        };
        let correlation_id = listing.correlation_id;
        let (work, done) = match &mut listing.kind {
            ListingKind::ById { recording_id } => {
                let recording_id = *recording_id;
                if cx.catalog.has_recording(recording_id) {
                    (1, self.send_descriptor(cx, out, correlation_id, recording_id))
                } else {
                    self.respond_unknown(correlation_id, recording_id);
                    (1, true)
                }
            }
            ListingKind::ForUri {
                next_id,
                remaining,
                stream_id,
                fragment,
            } => {
                if *remaining <= 0 {
                    (1, true)
                } else {
                    let ids: Vec<i64> = cx
                        .catalog
                        .recording_ids_from(*next_id)
                        .take(MAX_SCANS_PER_WORK_CYCLE)
                        .collect();
                    let mut work = 0;
                    let mut done = false;
                    let mut blocked = false;
                    for &id in &ids {
                        let matches = cx.catalog.descriptor(id).is_ok_and(|d| {
                            stream_id.is_none_or(|s| d.stream_id == s)
                                && contains(d.original_channel.as_bytes(), fragment)
                        });
                        if matches {
                            if !self.send_descriptor(cx, out, correlation_id, id) {
                                blocked = true;
                                break;
                            }
                            work += 1;
                            *remaining -= 1;
                        }
                        *next_id = id + 1;
                        if *remaining == 0 {
                            done = true;
                            break;
                        }
                    }
                    if !blocked && !done && ids.len() < MAX_SCANS_PER_WORK_CYCLE {
                        self.respond_unknown(correlation_id, *next_id);
                        done = true;
                    }
                    (work.max(1), done || self.state == SessionState::Done)
                }
            }
        };
        if !done && self.state != SessionState::Done {
            self.listing = Some(listing);
        }
        work
    }
}
