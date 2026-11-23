//! A client of an archive, without I/O: Java's `AeronArchive` as the
//! consensus module uses it, asynchronously. It connects as Java's
//! `AsyncConnect` does, then sends requests in the order given and routes
//! what the archive answers to them by correlation id, and its recording
//! signals as they come.
//!
//! The shell owns the request publication and the response subscription
//! ([`Link`]); it feeds the session the time ([`ClientSession::on_tick`]),
//! the messages its subscription receives ([`ClientSession::on_response`])
//! and the loss of the archive ([`ClientSession::on_disconnected`]), and
//! takes what came of them from [`ClientSession::poll_event`]. Correlation
//! ids are the caller's: Aeron's, unique on the driver.

use std::collections::{BTreeMap, VecDeque};

use crate::catalog::Descriptor;
use crate::client::{self, Response};
use crate::conductor::Offer;
use crate::protocol::{Code, Signal};

/// Aeron's null for ids and positions.
pub const NULL_VALUE: i64 = -1;
/// The first protocol version whose archive answers an archive id request.
const ARCHIVE_ID_SINCE_VERSION: i32 = 0x01_0B_00;

/// How a client reaches its archive, and what it says of itself.
#[derive(Clone, Debug)]
pub struct Config {
    /// The response stream the archive is asked to answer on.
    pub response_stream_id: i32,
    /// `name=` in the client info the archive keeps.
    pub client_name: String,
    /// How long the connect, and each request's answer, may take: Java's
    /// `messageTimeoutNs`, 10 s.
    pub message_timeout_ns: i64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            response_stream_id: 20,
            client_name: String::new(),
            message_timeout_ns: 10_000_000_000,
        }
    }
}

/// The request publication and response subscription, the shell's.
pub trait Link {
    /// Offers a request on the publication, once.
    fn offer(&mut self, message: &[u8]) -> Offer;
    fn publication_connected(&self) -> bool;
    fn subscription_connected(&self) -> bool;
    /// The response channel with its port resolved, once it is.
    fn resolved_response_channel(&self) -> Option<String>;
    /// A correlation id unique on the driver.
    fn next_correlation_id(&mut self) -> i64;
}

/// A request to the archive, without the session and correlation ids.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    StartRecording {
        channel: String,
        stream_id: i32,
        local: bool,
        auto_stop: bool,
    },
    ExtendRecording {
        recording_id: i64,
        channel: String,
        stream_id: i32,
        local: bool,
        auto_stop: bool,
    },
    StopRecordingSubscription {
        subscription_id: i64,
    },
    StopRecordingByIdentity {
        recording_id: i64,
    },
    ListRecording {
        recording_id: i64,
    },
    StartPosition {
        recording_id: i64,
    },
    StopPosition {
        recording_id: i64,
    },
    RecordingPosition {
        recording_id: i64,
    },
    Replay {
        recording_id: i64,
        position: i64,
        length: i64,
        channel: String,
        stream_id: i32,
    },
    BoundedReplay {
        recording_id: i64,
        position: i64,
        length: i64,
        limit_counter_id: i32,
        channel: String,
        stream_id: i32,
    },
    StopReplay {
        replay_session_id: i64,
    },
    Replicate {
        src_recording_id: i64,
        dst_recording_id: i64,
        stop_position: i64,
        src_control_stream_id: i32,
        src_control_channel: String,
        replication_channel: String,
        replication_session_id: i32,
    },
    StopReplication {
        replication_id: i64,
    },
    TruncateRecording {
        recording_id: i64,
        position: i64,
    },
}

impl Request {
    fn encode(&self, cs: i64, corr: i64) -> Vec<u8> {
        match self {
            Request::StartRecording {
                channel,
                stream_id,
                local,
                auto_stop,
            } => client::start_recording2(cs, corr, *stream_id, *local, *auto_stop, channel),
            Request::ExtendRecording {
                recording_id,
                channel,
                stream_id,
                local,
                auto_stop,
            } => client::extend_recording2(cs, corr, *recording_id, *stream_id, *local, *auto_stop, channel),
            Request::StopRecordingSubscription { subscription_id } => {
                client::stop_recording_subscription(cs, corr, *subscription_id)
            }
            Request::StopRecordingByIdentity { recording_id } => {
                client::stop_recording_by_identity(cs, corr, *recording_id)
            }
            Request::ListRecording { recording_id } => client::list_recording(cs, corr, *recording_id),
            Request::StartPosition { recording_id } => client::start_position(cs, corr, *recording_id),
            Request::StopPosition { recording_id } => client::stop_position(cs, corr, *recording_id),
            Request::RecordingPosition { recording_id } => client::recording_position(cs, corr, *recording_id),
            Request::Replay {
                recording_id,
                position,
                length,
                channel,
                stream_id,
            } => client::replay(cs, corr, *recording_id, *position, *length, *stream_id, -1, channel),
            Request::BoundedReplay {
                recording_id,
                position,
                length,
                limit_counter_id,
                channel,
                stream_id,
            } => client::bounded_replay(
                cs,
                corr,
                *recording_id,
                *position,
                *length,
                *limit_counter_id,
                *stream_id,
                channel,
            ),
            Request::StopReplay { replay_session_id } => client::stop_replay(cs, corr, *replay_session_id),
            Request::Replicate {
                src_recording_id,
                dst_recording_id,
                stop_position,
                src_control_stream_id,
                src_control_channel,
                replication_channel,
                replication_session_id,
            } => client::replicate2(
                cs,
                corr,
                *src_recording_id,
                *dst_recording_id,
                *stop_position,
                *src_control_stream_id,
                *replication_session_id,
                src_control_channel,
                replication_channel,
            ),
            Request::StopReplication { replication_id } => client::stop_replication(cs, corr, *replication_id),
            Request::TruncateRecording { recording_id, position } => {
                client::truncate_recording(cs, corr, *recording_id, *position)
            }
        }
    }
}

/// What came of the session and its requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Connected {
        control_session_id: i64,
        archive_id: i64,
    },
    /// The connect failed: refused, or not done within the message timeout.
    ConnectFailed {
        message: String,
    },
    /// A request answered OK: what it is about (a subscription, a
    /// position, a replay session...).
    Ok {
        correlation_id: i64,
        relevant_id: i64,
    },
    /// A request answered with an error, or an error the archive sent of
    /// its own (an extension refused at its image, say): Java's
    /// `ArchiveException` with its error code.
    Error {
        correlation_id: i64,
        error_code: i64,
        message: String,
    },
    /// A recording a listing found.
    Descriptor {
        correlation_id: i64,
        descriptor: Box<Descriptor>,
    },
    /// A listing, or another request, naming a recording the archive does
    /// not have.
    RecordingUnknown {
        correlation_id: i64,
        recording_id: i64,
    },
    Signal {
        correlation_id: i64,
        recording_id: i64,
        subscription_id: i64,
        position: i64,
        signal: Signal,
    },
    /// No answer within the message timeout.
    TimedOut {
        correlation_id: i64,
    },
    /// The archive's responses stopped: its image went.
    Disconnected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    AwaitPublicationConnected,
    SendConnectRequest,
    AwaitSubscriptionConnected,
    AwaitConnectResponse,
    SendArchiveIdRequest,
    AwaitArchiveIdResponse,
    Connected,
    Closed,
}

impl State {
    /// Java's `AsyncConnect.State` name, for the timeout's message.
    fn java_name(self) -> &'static str {
        match self {
            State::AwaitPublicationConnected => "AWAIT_PUBLICATION_CONNECTED",
            State::SendConnectRequest => "SEND_CONNECT_REQUEST",
            State::AwaitSubscriptionConnected => "AWAIT_SUBSCRIPTION_CONNECTED",
            State::AwaitConnectResponse => "AWAIT_CONNECT_RESPONSE",
            State::SendArchiveIdRequest => "SEND_ARCHIVE_ID_REQUEST",
            State::AwaitArchiveIdResponse => "AWAIT_ARCHIVE_ID_RESPONSE",
            State::Connected => "DONE",
            State::Closed => "CLOSED",
        }
    }
}

/// A client session with one archive.
#[derive(Debug)]
pub struct ClientSession {
    config: Config,
    state: State,
    now_ns: i64,
    connect_deadline_ns: i64,
    /// The connect's, then the archive id request's.
    connect_correlation_id: i64,
    control_session_id: i64,
    archive_id: i64,
    /// Requests not yet taken by the publication, in order.
    outbox: VecDeque<(i64, Vec<u8>)>,
    /// Requests sent and not answered yet, by correlation id: their deadline.
    pending: BTreeMap<i64, i64>,
    events: VecDeque<Event>,
}

impl ClientSession {
    /// A session to connect: the shell has added the publication and the
    /// subscription.
    pub fn new(config: Config, now_ns: i64) -> Self {
        Self {
            connect_deadline_ns: now_ns + config.message_timeout_ns,
            config,
            state: State::AwaitPublicationConnected,
            now_ns,
            connect_correlation_id: NULL_VALUE,
            control_session_id: NULL_VALUE,
            archive_id: NULL_VALUE,
            outbox: VecDeque::new(),
            pending: BTreeMap::new(),
            events: VecDeque::new(),
        }
    }

    pub fn is_connected(&self) -> bool {
        self.state == State::Connected
    }

    pub fn is_closed(&self) -> bool {
        self.state == State::Closed
    }

    pub fn control_session_id(&self) -> i64 {
        self.control_session_id
    }

    /// The archive's id once connected, [`NULL_VALUE`] before (or from an
    /// archive too old to say).
    pub fn archive_id(&self) -> i64 {
        self.archive_id
    }

    /// The time, which the deadlines are checked against.
    pub fn on_tick(&mut self, now_ns: i64) {
        self.now_ns = now_ns;
        match self.state {
            State::Connected => {
                let expired: Vec<i64> = self
                    .pending
                    .iter()
                    .filter(|(_, deadline)| now_ns - **deadline > 0)
                    .map(|(&c, _)| c)
                    .collect();
                for correlation_id in expired {
                    self.pending.remove(&correlation_id);
                    self.events.push_back(Event::TimedOut { correlation_id });
                }
            }
            State::Closed => {}
            state if now_ns - self.connect_deadline_ns > 0 => {
                self.state = State::Closed;
                self.events.push_back(Event::ConnectFailed {
                    message: format!("Archive connect timeout: step={}", state.java_name()),
                });
            }
            _ => {}
        }
    }

    /// Queues a request: sent by [`do_work`](Self::do_work), in order.
    /// Refused before the session connects.
    pub fn request(&mut self, correlation_id: i64, request: &Request) -> Result<(), &'static str> {
        if self.state != State::Connected {
            return Err("archive client not connected");
        }
        self.outbox
            .push_back((correlation_id, request.encode(self.control_session_id, correlation_id)));
        Ok(())
    }

    /// The connect's steps the link allows, then the requests the
    /// publication takes: the work done.
    pub fn do_work(&mut self, link: &mut impl Link) -> usize {
        let mut work = 0;
        loop {
            let before = self.state;
            match self.state {
                State::AwaitPublicationConnected if link.publication_connected() => {
                    self.connect_correlation_id = link.next_correlation_id();
                    self.state = State::SendConnectRequest;
                }
                State::SendConnectRequest => {
                    if let Some(channel) = link.resolved_response_channel() {
                        let client_info = format!(
                            "name={} version={} commit={}",
                            self.config.client_name,
                            crate::counters::ARCHIVE_VERSION,
                            crate::counters::ARCHIVE_GIT_SHA
                        );
                        let message = client::auth_connect(
                            self.connect_correlation_id,
                            self.config.response_stream_id,
                            client::VERSION,
                            &channel,
                            &client_info,
                        );
                        if link.offer(&message) == Offer::Sent {
                            self.state = State::AwaitSubscriptionConnected;
                        }
                    }
                }
                State::AwaitSubscriptionConnected if link.subscription_connected() => {
                    self.state = State::AwaitConnectResponse;
                }
                State::SendArchiveIdRequest => {
                    let message = client::archive_id(self.control_session_id, self.connect_correlation_id);
                    if link.offer(&message) == Offer::Sent {
                        self.state = State::AwaitArchiveIdResponse;
                    }
                }
                State::Closed => {
                    // A refused connect's close, as Java sends it.
                    while let Some((_, message)) = self.outbox.pop_front() {
                        link.offer(&message);
                    }
                }
                State::Connected => {
                    while let Some((correlation_id, message)) = self.outbox.front() {
                        if link.offer(message) != Offer::Sent {
                            break;
                        }
                        if *correlation_id != NULL_VALUE {
                            self.pending
                                .insert(*correlation_id, self.now_ns + self.config.message_timeout_ns);
                        }
                        self.outbox.pop_front();
                        work += 1;
                    }
                }
                _ => {}
            }
            if self.state == before {
                return work;
            }
            work += 1;
        }
    }

    /// A message from the response subscription: another session's is
    /// ignored, as are pings.
    pub fn on_response(&mut self, message: &[u8]) {
        let Ok(response) = client::decode_response(message) else {
            return;
        };
        match self.state {
            State::AwaitConnectResponse | State::AwaitArchiveIdResponse => self.on_connect_response(response),
            State::Connected => self.on_session_response(response),
            _ => {}
        }
    }

    fn on_connect_response(&mut self, response: Response) {
        let Response::Control {
            control_session_id,
            correlation_id,
            relevant_id,
            code,
            version,
            message,
        } = response
        else {
            return;
        };
        if correlation_id != self.connect_correlation_id {
            return;
        }
        self.control_session_id = control_session_id;
        if code != Code::OK {
            self.state = State::Closed;
            self.outbox
                .push_back((NULL_VALUE, client::close_session(control_session_id)));
            let message = if code == Code::ERROR {
                message
            } else {
                format!("unexpected response: code={code:?}")
            };
            self.events.push_back(Event::ConnectFailed { message });
            return;
        }
        if self.state == State::AwaitArchiveIdResponse || version < ARCHIVE_ID_SINCE_VERSION {
            self.archive_id = if self.state == State::AwaitArchiveIdResponse {
                relevant_id
            } else {
                NULL_VALUE
            };
            self.state = State::Connected;
            // As Java's `transitionToDone`: a keep-alive, sent before any request.
            self.outbox
                .push_front((NULL_VALUE, client::keep_alive(control_session_id, NULL_VALUE)));
            self.events.push_back(Event::Connected {
                control_session_id,
                archive_id: self.archive_id,
            });
        } else {
            self.state = State::SendArchiveIdRequest;
        }
    }

    fn on_session_response(&mut self, response: Response) {
        match response {
            Response::Control {
                control_session_id,
                correlation_id,
                relevant_id,
                code,
                message,
                ..
            } if control_session_id == self.control_session_id => {
                self.pending.remove(&correlation_id);
                let event = match code {
                    Code::OK => Event::Ok {
                        correlation_id,
                        relevant_id,
                    },
                    Code::RECORDING_UNKNOWN => Event::RecordingUnknown {
                        correlation_id,
                        recording_id: relevant_id,
                    },
                    _ => Event::Error {
                        correlation_id,
                        error_code: relevant_id,
                        message,
                    },
                };
                self.events.push_back(event);
            }
            Response::Descriptor {
                control_session_id,
                correlation_id,
                descriptor,
            } if control_session_id == self.control_session_id => {
                self.pending.remove(&correlation_id);
                self.events.push_back(Event::Descriptor {
                    correlation_id,
                    descriptor: Box::new(descriptor),
                });
            }
            Response::Signal {
                control_session_id,
                correlation_id,
                recording_id,
                subscription_id,
                position,
                signal,
            } if control_session_id == self.control_session_id => {
                self.events.push_back(Event::Signal {
                    correlation_id,
                    recording_id,
                    subscription_id,
                    position,
                    signal,
                });
            }
            _ => {}
        }
    }

    /// The response subscription lost the archive's image.
    pub fn on_disconnected(&mut self) {
        if self.state != State::Closed {
            self.state = State::Closed;
            self.events.push_back(Event::Disconnected);
        }
    }

    /// Ends the session: the close request to offer, once connected.
    pub fn close(&mut self) -> Option<Vec<u8>> {
        let connected = self.state == State::Connected;
        self.state = State::Closed;
        self.outbox.clear();
        connected.then(|| client::close_session(self.control_session_id))
    }

    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }
}
