//! A cluster client: rusteron's own (`rusteron_cluster::client`), its
//! Aeron the simulator's network. It connects as Java's `AeronCluster` does,
//! through a publication to every member's ingress, follows redirects and
//! new leaders, sends keepalives when it has nothing to send, and closes when
//! the cluster or its timeouts say. On top, an application sends messages
//! tagged with an id, which the services report as they apply them, and
//! connects again when its session closes or its messages go unanswered.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use rusteron_cluster::client::{
    Added, CLOSED, Client, ClientConfig, ClientPorts, ClientState, Connect, EgressListener, Handle, NOT_CONNECTED,
    PollAction,
};

use crate::sim::rng::Rng;
use crate::sim::service::CLIENT_MESSAGE_TAG;

/// How long messages go unanswered before the application connects again:
/// its session may have closed unseen, as an application's response timeout.
const RESPONSE_TIMEOUT_MS: i64 = 3_000;
/// How long after a failed connect the application tries again.
const RECONNECT_DELAY_MS: i64 = 100;
/// How often an idle session is kept alive: well within the cluster's session timeout.
const KEEP_ALIVE_INTERVAL_MS: i64 = 1_000;

/// Aeron, as the client sees it in the simulator: a publication to a member
/// ready a step after it is added, as a driver takes a moment, and connected
/// while the member is up; the egress as the world delivers it, each
/// member's incarnation an image of its own.
#[derive(Debug, Default)]
struct Network {
    now_ms: i64,
    next_id: Handle,
    next_correlation_id: i64,
    response_channel: String,
    /// Publications: their member, and when they are ready.
    publications: BTreeMap<Handle, (usize, i64)>,
    subscriptions: BTreeSet<Handle>,
    /// Members up, as the world says at each step.
    up: Vec<bool>,
    /// Messages to members' ingress: (member, bytes), taken by the world.
    outbox: Vec<(usize, Vec<u8>)>,
    position: i64,
    /// The egress as delivered: bytes and image.
    inbox: VecDeque<(Vec<u8>, i32)>,
    /// Images seen, and those gone with their member.
    images: BTreeSet<i32>,
    closed_images: BTreeSet<i32>,
}

/// An image per member incarnation: a member started again is a new image.
fn image(member: usize, incarnation: u32) -> i32 {
    ((incarnation as i32) << 8) | member as i32
}

fn member_of_image(image: i32) -> usize {
    (image & 0xff) as usize
}

impl ClientPorts for Network {
    fn now_ns(&self) -> i64 {
        self.now_ms * 1_000_000
    }

    fn next_correlation_id(&mut self) -> i64 {
        self.next_correlation_id += 1;
        self.next_correlation_id
    }

    fn async_add_subscription(&mut self, _channel: &str, _stream_id: i32) -> i64 {
        self.next_id += 1;
        self.subscriptions.insert(self.next_id);
        self.next_id
    }

    fn get_subscription(&mut self, registration_id: i64) -> Added<Handle> {
        Added::Ready(registration_id)
    }

    fn async_add_publication(&mut self, channel: &str, _stream_id: i32, _exclusive: bool) -> i64 {
        self.next_id += 1;
        let member = crate::sim::archive::endpoint_of(channel)
            .and_then(|e| crate::sim::member_of_endpoint(&e))
            .unwrap_or(usize::MAX);
        self.publications.insert(self.next_id, (member, self.now_ms + 1));
        self.next_id
    }

    fn get_publication(&mut self, registration_id: i64) -> Added<Handle> {
        match self.publications.get(&registration_id) {
            None => Added::Failed(format!("no publication {registration_id}")),
            Some((usize::MAX, _)) => Added::Failed(format!("no member's endpoint in publication {registration_id}")),
            Some(&(_, ready_ms)) if self.now_ms < ready_ms => Added::Pending,
            Some(_) => Added::Ready(registration_id),
        }
    }

    fn async_remove_publication(&mut self, registration_id: i64) {
        self.publications.remove(&registration_id);
    }

    fn async_remove_subscription(&mut self, registration_id: i64) {
        self.subscriptions.remove(&registration_id);
    }

    fn close_publication(&mut self, publication: Handle) {
        self.publications.remove(&publication);
    }

    fn close_subscription(&mut self, subscription: Handle) {
        self.subscriptions.remove(&subscription);
        self.inbox.clear();
    }

    fn is_connected(&self, publication: Handle) -> bool {
        self.publications
            .get(&publication)
            .is_some_and(|&(m, ready_ms)| self.now_ms >= ready_ms && self.up.get(m).copied().unwrap_or(false))
    }

    fn resolved_channel(&self, _subscription: Handle) -> Option<String> {
        Some(self.response_channel.clone())
    }

    fn offer(&mut self, publication: Handle, parts: &[&[u8]]) -> i64 {
        let Some(&(member, _)) = self.publications.get(&publication) else {
            return CLOSED;
        };
        if !self.is_connected(publication) {
            return NOT_CONNECTED;
        }
        let bytes = parts.concat();
        self.position += bytes.len() as i64;
        self.outbox.push((member, bytes));
        self.position
    }

    fn controlled_poll(
        &mut self,
        subscription: Handle,
        limit: usize,
        handler: &mut dyn FnMut(&[u8], i32) -> PollAction,
    ) -> usize {
        if !self.subscriptions.contains(&subscription) {
            return 0;
        }
        let mut read = 0;
        while read < limit {
            let Some((message, image)) = self.inbox.pop_front() else {
                break;
            };
            match handler(&message, image) {
                PollAction::Abort => {
                    self.inbox.push_front((message, image));
                    break;
                }
                PollAction::Break => {
                    read += 1;
                    break;
                }
                PollAction::Continue => read += 1,
            }
        }
        read
    }

    fn is_image_closed(&self, _subscription: Handle, session_id: i32) -> bool {
        self.closed_images.contains(&session_id)
    }

    fn idle(&mut self) {}
}

enum Session {
    Disconnected { since_ms: i64 },
    Connecting(Box<Connect>),
    Connected(Box<Client>),
}

/// What the application hears: only that the cluster answered; the
/// services' reports tell which messages were applied.
struct Heard;

impl EgressListener for Heard {
    fn on_message(&mut self, _cluster_session_id: i64, _timestamp: i64, _payload: &[u8]) {}
}

pub struct SimClient {
    pub index: usize,
    session: Session,
    network: Network,
    config: ClientConfig,
    next_message_id: u64,
    pub interval_ms: i64,
    last_send_ms: i64,
    last_keep_alive_ms: i64,
    /// Messages sent: (id, at).
    pub sent: Vec<(u64, i64)>,
    /// Messages to members' ingress: (member, bytes), taken by the world.
    pub outbox: Vec<(usize, Vec<u8>)>,
    pub sending: bool,
    /// When a message it sent was last applied, or it connected.
    last_answer_ms: i64,
    /// What became of its sessions, for the run's trace.
    pub trace: Vec<String>,
    /// The session's state last traced.
    traced: Option<ClientState>,
}

impl SimClient {
    /// A client of the members whose ingress endpoints are `ingress_endpoints`
    /// (`id=endpoint,...`).
    pub fn new(index: usize, interval_ms: i64, ingress_endpoints: String) -> Self {
        let config = ClientConfig {
            ingress_channel: "aeron:udp?term-length=64k".into(),
            ingress_endpoints: Some(ingress_endpoints),
            egress_channel: format!("aeron:udp?endpoint=client-{index}:2000"),
            ..ClientConfig::default()
        };
        let mut concluded = config.clone();
        concluded.conclude().expect("a client's config");
        Self {
            index,
            session: Session::Disconnected { since_ms: i64::MIN / 2 },
            network: Network {
                response_channel: concluded.egress_channel,
                ..Network::default()
            },
            config,
            next_message_id: (index as u64) << 40,
            interval_ms,
            last_send_ms: 0,
            last_keep_alive_ms: 0,
            sent: Vec::new(),
            outbox: Vec::new(),
            sending: true,
            last_answer_ms: 0,
            trace: Vec::new(),
            traced: None,
        }
    }

    pub fn response_channel(&self) -> String {
        self.network.response_channel.clone()
    }

    pub fn is_connected(&self) -> bool {
        matches!(&self.session, Session::Connected(c) if !c.is_closed())
    }

    /// One step at `now_ms`, the members up as `up` says: connecting,
    /// reading its egress, sending its next message or a keepalive.
    pub fn step(&mut self, now_ms: i64, up: &[bool], _rng: &mut Rng) {
        self.network.now_ms = now_ms;
        self.network.up = up.to_vec();
        let network = &mut self.network;
        match &mut self.session {
            Session::Disconnected { since_ms } => {
                if now_ms - *since_ms >= RECONNECT_DELAY_MS {
                    let connect = Connect::new(self.config.clone(), network.now_ns()).expect("a client's config");
                    self.session = Session::Connecting(Box::new(connect));
                }
            }
            Session::Connecting(connect) => {
                for _ in 0..8 {
                    match connect.poll(network) {
                        Ok(None) => {}
                        Ok(Some(client)) => {
                            self.trace.push(format!(
                                "client {}: session {} with leader {} in term {}",
                                self.index,
                                client.cluster_session_id(),
                                client.leader_member_id(),
                                client.leadership_term_id()
                            ));
                            self.traced = Some(client.state());
                            self.last_answer_ms = now_ms;
                            self.last_keep_alive_ms = now_ms;
                            self.session = Session::Connected(Box::new(client));
                            break;
                        }
                        Err(e) => {
                            self.trace.push(format!("client {}: connect failed: {e}", self.index));
                            connect.close(network);
                            self.session = Session::Disconnected { since_ms: now_ms };
                            break;
                        }
                    }
                }
            }
            Session::Connected(client) => {
                let polled = client.poll_egress(network, &mut Heard);
                let unanswered = self.sending && now_ms - self.last_answer_ms >= RESPONSE_TIMEOUT_MS;
                if self.traced != Some(client.state()) {
                    self.traced = Some(client.state());
                    self.trace.push(format!(
                        "client {}: session {} {:?}, leader {} in term {}",
                        self.index,
                        client.cluster_session_id(),
                        client.state(),
                        client.leader_member_id(),
                        client.leadership_term_id()
                    ));
                }
                if polled.is_err() || client.is_closed() || unanswered {
                    self.trace.push(format!(
                        "client {}: session {} given up: {}",
                        self.index,
                        client.cluster_session_id(),
                        match (&polled, unanswered) {
                            (Err(e), _) => e.to_string(),
                            (_, true) => "unanswered".into(),
                            _ => "closed".into(),
                        }
                    ));
                    client.close(network);
                    self.session = Session::Disconnected { since_ms: now_ms };
                } else if self.sending {
                    if now_ms - self.last_send_ms >= self.interval_ms {
                        let id = self.next_message_id;
                        let mut payload = CLIENT_MESSAGE_TAG.to_le_bytes().to_vec();
                        payload.extend_from_slice(&id.to_le_bytes());
                        if client.offer(network, &[&payload]) > 0 {
                            self.next_message_id += 1;
                            self.last_send_ms = now_ms;
                            self.sent.push((id, now_ms));
                        }
                    }
                } else if now_ms - self.last_keep_alive_ms >= KEEP_ALIVE_INTERVAL_MS {
                    self.last_keep_alive_ms = now_ms;
                    let _ = client.send_keep_alive(network);
                }
            }
        }
        self.outbox.append(&mut self.network.outbox);
    }

    /// One of its messages applied.
    pub fn on_applied(&mut self, now_ms: i64) {
        self.last_answer_ms = now_ms;
    }

    /// Member `member` gone: the images it fed close, its publications
    /// lose their subscriber (`up` says so at the next step).
    pub fn lose(&mut self, member: usize) {
        let gone: Vec<i32> = self
            .network
            .images
            .iter()
            .copied()
            .filter(|&i| member_of_image(i) == member)
            .collect();
        self.network.closed_images.extend(gone);
        self.network.inbox.retain(|(_, i)| member_of_image(*i) != member);
    }

    /// A message on its egress, from member `from` in its `incarnation`.
    pub fn on_egress(&mut self, from: usize, incarnation: u32, bytes: &[u8], _now_ms: i64) {
        let image = image(from, incarnation);
        self.network.images.insert(image);
        self.network.inbox.push_back((bytes.to_vec(), image));
    }
}
