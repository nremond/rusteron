//! A client's connect and session, step by step, over fake ports with a
//! virtual clock: Java's `AeronClusterAsyncConnectTest`, `AeronClusterTest`,
//! `EgressPollerTest` and `EgressAdapterTest` cases as scenarios, and a test
//! for each rule they leave out (the challenge, a redirect keeping the
//! leader's publication, the connect's one deadline).

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use rusteron_cluster_codecs::cluster::{
    WriteBuf, admin_response_codec::AdminResponseEncoder, challenge_codec::ChallengeEncoder, message_header_codec,
    new_leader_event_codec::NewLeaderEventEncoder, session_event_codec::SessionEventEncoder,
};
use rusteron_cluster_core::client::codec::{self, Egress};
use rusteron_cluster_core::client::{
    Added, AdminRequestType, AdminResponseCode, BACK_PRESSURED, CLOSED, Client, ClientConfig, ClientError, ClientPorts,
    ClientState, Connect, ConnectState, CredentialsSupplier, EgressListener, EventCode, Handle, MAX_POSITION_EXCEEDED,
    NOT_CONNECTED, PUBLICATION_ERROR, PollAction,
};

const MS: i64 = 1_000_000;
const ENDPOINTS: &str = "0=host0:9002,1=host1:9002,2=host2:9002";
const RESPONSE: &str = "aeron:udp?endpoint=client:40123|rejoin=false";

#[derive(Debug, Default)]
struct Fake {
    now: i64,
    next_correlation: i64,
    /// Registration id = handle, each add's channel.
    publications: BTreeMap<i64, String>,
    subscriptions: BTreeMap<i64, String>,
    /// Polls of `get_*` before an add is ready.
    add_polls: usize,
    polled: BTreeMap<i64, usize>,
    /// Adds whose channel holds one of these: refused for now, or for good.
    unavailable: BTreeSet<String>,
    failing: BTreeSet<String>,
    /// Endpoints whose publications have a subscriber.
    connected: BTreeSet<String>,
    closed_publications: Vec<Handle>,
    closed_subscriptions: Vec<Handle>,
    removed: Vec<i64>,
    /// Every offer, its parts joined.
    offers: Vec<(Handle, Vec<u8>)>,
    results: VecDeque<i64>,
    position: i64,
    egress: VecDeque<(Vec<u8>, i32)>,
    closed_images: BTreeSet<i32>,
    resolved: bool,
    idles: usize,
    /// Calls in order, for the order of closes.
    calls: Vec<String>,
}

impl Fake {
    fn new() -> Self {
        Self {
            next_correlation: 100,
            resolved: true,
            ..Self::default()
        }
    }

    fn channel(&self, publication: Handle) -> &str {
        &self.publications[&publication]
    }

    fn publication_to(&self, endpoint: &str) -> Option<Handle> {
        self.publications
            .iter()
            .rev()
            .find(|(h, c)| c.contains(endpoint) && !self.closed_publications.contains(h))
            .map(|(h, _)| *h)
    }

    fn template_sent(&self, index: usize) -> u16 {
        u16::from_le_bytes([self.offers[index].1[2], self.offers[index].1[3]])
    }

    fn egress(&mut self, message: Vec<u8>, image: i32) {
        self.egress.push_back((message, image));
    }

    fn get(&mut self, registration_id: i64, channel: Option<String>) -> Added<Handle> {
        let Some(channel) = channel else {
            return Added::Failed(format!("unknown registration {registration_id}"));
        };
        if self.failing.iter().any(|f| channel.contains(f)) {
            return Added::Failed(format!("refused {channel}"));
        }
        if self.unavailable.iter().any(|f| channel.contains(f)) {
            return Added::TemporarilyUnavailable;
        }
        let polled = self.polled.entry(registration_id).or_default();
        *polled += 1;
        if *polled <= self.add_polls {
            Added::Pending
        } else {
            Added::Ready(registration_id)
        }
    }
}

impl ClientPorts for Fake {
    fn now_ns(&self) -> i64 {
        self.now
    }

    fn next_correlation_id(&mut self) -> i64 {
        self.next_correlation += 1;
        self.next_correlation
    }

    fn async_add_subscription(&mut self, channel: &str, _stream_id: i32) -> i64 {
        let id = 1_000 + self.subscriptions.len() as i64 + self.publications.len() as i64;
        self.subscriptions.insert(id, channel.into());
        self.calls.push(format!("add subscription {channel}"));
        id
    }

    fn get_subscription(&mut self, registration_id: i64) -> Added<Handle> {
        let channel = self.subscriptions.get(&registration_id).cloned();
        self.get(registration_id, channel)
    }

    fn async_add_publication(&mut self, channel: &str, _stream_id: i32, _exclusive: bool) -> i64 {
        let id = 1_000 + self.subscriptions.len() as i64 + self.publications.len() as i64;
        self.publications.insert(id, channel.into());
        self.calls.push(format!("add publication {channel}"));
        id
    }

    fn get_publication(&mut self, registration_id: i64) -> Added<Handle> {
        let channel = self.publications.get(&registration_id).cloned();
        self.get(registration_id, channel)
    }

    fn async_remove_publication(&mut self, registration_id: i64) {
        self.removed.push(registration_id);
        self.calls.push(format!("remove publication {registration_id}"));
    }

    fn async_remove_subscription(&mut self, registration_id: i64) {
        self.removed.push(registration_id);
        self.calls.push(format!("remove subscription {registration_id}"));
    }

    fn close_publication(&mut self, publication: Handle) {
        self.closed_publications.push(publication);
        self.calls.push(format!("close publication {publication}"));
    }

    fn close_subscription(&mut self, subscription: Handle) {
        self.closed_subscriptions.push(subscription);
        self.calls.push(format!("close subscription {subscription}"));
    }

    fn is_connected(&self, publication: Handle) -> bool {
        !self.closed_publications.contains(&publication)
            && self
                .connected
                .iter()
                .any(|e| self.channel(publication).contains(e.as_str()))
    }

    fn resolved_channel(&self, _subscription: Handle) -> Option<String> {
        self.resolved.then(|| RESPONSE.to_string())
    }

    fn offer(&mut self, publication: Handle, parts: &[&[u8]]) -> i64 {
        if self.closed_publications.contains(&publication) {
            return CLOSED;
        }
        if !self.is_connected(publication) {
            return NOT_CONNECTED;
        }
        let bytes: Vec<u8> = parts.concat();
        if let Some(result) = self.results.pop_front()
            && result < 0
        {
            return result;
        }
        self.position += bytes.len() as i64;
        self.offers.push((publication, bytes));
        self.calls.push(format!("offer {publication}"));
        self.position
    }

    fn controlled_poll(
        &mut self,
        _subscription: Handle,
        limit: usize,
        handler: &mut dyn FnMut(&[u8], i32) -> PollAction,
    ) -> usize {
        let mut read = 0;
        while read < limit {
            let Some((message, image)) = self.egress.front().cloned() else {
                break;
            };
            match handler(&message, image) {
                PollAction::Abort => break,
                PollAction::Break => {
                    self.egress.pop_front();
                    read += 1;
                    break;
                }
                PollAction::Continue => {
                    self.egress.pop_front();
                    read += 1;
                }
            }
        }
        read
    }

    fn is_image_closed(&self, _subscription: Handle, session_id: i32) -> bool {
        self.closed_images.contains(&session_id)
    }

    fn idle(&mut self) {
        self.idles += 1;
    }
}

const HEADER: usize = message_header_codec::ENCODED_LENGTH;

macro_rules! encoded {
    ($encoder:ty, $len:expr, |$e:ident| $body:block) => {{
        let mut buf = vec![0u8; $len];
        {
            let mut $e = <$encoder>::default()
                .wrap(WriteBuf::new(&mut buf), HEADER)
                .header(0)
                .parent()
                .unwrap();
            $body
        }
        buf
    }};
}

fn event(session: i64, correlation: i64, term: i64, leader: i32, code: EventCode, detail: &str) -> Vec<u8> {
    encoded!(SessionEventEncoder, HEADER + 44 + 4 + detail.len(), |e| {
        e.cluster_session_id(session)
            .correlation_id(correlation)
            .leadership_term_id(term)
            .leader_member_id(leader)
            .code(code)
            .version(codec::PROTOCOL_SEMANTIC_VERSION)
            .leader_heartbeat_timeout_ns(1_000 * MS)
            .detail(detail.as_bytes());
    })
}

fn new_leader(session: i64, term: i64, leader: i32, endpoints: &str) -> Vec<u8> {
    encoded!(NewLeaderEventEncoder, HEADER + 20 + 4 + endpoints.len(), |e| {
        e.leadership_term_id(term)
            .cluster_session_id(session)
            .leader_member_id(leader)
            .ingress_endpoints(endpoints.as_bytes());
    })
}

fn challenge(correlation: i64, session: i64, bytes: &[u8]) -> Vec<u8> {
    encoded!(ChallengeEncoder, HEADER + 16 + 4 + bytes.len(), |e| {
        e.correlation_id(correlation)
            .cluster_session_id(session)
            .encoded_challenge(bytes);
    })
}

fn admin_response(session: i64, correlation: i64, payload: &[u8]) -> Vec<u8> {
    encoded!(AdminResponseEncoder, HEADER + 24 + 4 + 2 + 4 + payload.len(), |e| {
        e.cluster_session_id(session)
            .correlation_id(correlation)
            .request_type(AdminRequestType::SNAPSHOT)
            .response_code(AdminResponseCode::OK)
            .message(b"ok")
            .payload(payload);
    })
}

fn message(session: i64, term: i64, payload: &[u8]) -> Vec<u8> {
    let mut bytes = codec::session_message_header(term, session).to_vec();
    bytes.extend_from_slice(payload);
    bytes
}

fn config() -> ClientConfig {
    ClientConfig {
        ingress_channel: "aeron:udp?term-length=64k".into(),
        ingress_endpoints: Some(ENDPOINTS.into()),
        egress_channel: "aeron:udp?endpoint=client:0".into(),
        ..ClientConfig::default()
    }
}

/// Polls until the connect gives a client, an error, or nothing more happens.
fn drive<C: CredentialsSupplier>(connect: &mut Connect<C>, fake: &mut Fake) -> Result<Option<Client>, ClientError> {
    for _ in 0..50 {
        if let Some(client) = connect.poll(fake)? {
            return Ok(Some(client));
        }
    }
    Ok(None)
}

/// The connect request sent last: its correlation id, to the member's endpoint.
fn last_connect(fake: &Fake) -> (i64, String) {
    let (publication, bytes) = fake
        .offers
        .iter()
        .rev()
        .find(|(_, b)| b[2] == 3)
        .expect("a connect request");
    let correlation = i64::from_le_bytes(bytes[HEADER..HEADER + 8].try_into().unwrap());
    (correlation, fake.channel(*publication).to_string())
}

/// A client connected to leader 1 of term 3, session 7.
fn connected(fake: &mut Fake) -> Client {
    fake.connected.insert("host1".into());
    let mut connect = Connect::new(config(), fake.now).unwrap();
    assert_eq!(drive(&mut connect, fake).unwrap().map(|_| ()), None);
    let (correlation, channel) = last_connect(fake);
    assert!(channel.contains("host1"), "{channel}");
    fake.egress(event(7, correlation, 3, 1, EventCode::OK, ""), 51);
    drive(&mut connect, fake).unwrap().expect("connected")
}

#[derive(Default)]
struct Heard {
    messages: Vec<(i64, Vec<u8>)>,
    events: Vec<(i64, EventCode, String)>,
    new_leaders: Vec<(i64, i32, String)>,
    admin: Vec<(i64, Vec<u8>)>,
}

impl EgressListener for Heard {
    fn on_message(&mut self, cluster_session_id: i64, _timestamp: i64, payload: &[u8]) {
        self.messages.push((cluster_session_id, payload.to_vec()));
    }

    fn on_session_event(&mut self, correlation_id: i64, _: i64, _: i64, _: i32, code: EventCode, detail: &str) {
        self.events.push((correlation_id, code, detail.into()));
    }

    fn on_new_leader(&mut self, _: i64, leadership_term_id: i64, leader_member_id: i32, endpoints: &str) {
        self.new_leaders
            .push((leadership_term_id, leader_member_id, endpoints.into()));
    }

    fn on_admin_response(
        &mut self,
        _: i64,
        correlation_id: i64,
        _: AdminRequestType,
        _: AdminResponseCode,
        _: &str,
        payload: &[u8],
    ) {
        self.admin.push((correlation_id, payload.to_vec()));
    }
}

#[test]
fn a_connect_goes_to_the_first_member_to_connect_and_concludes_on_ok() {
    let mut fake = Fake::new();
    let client = connected(&mut fake);
    assert_eq!(client.state(), ClientState::Connected);
    assert_eq!(
        (
            client.cluster_session_id(),
            client.leadership_term_id(),
            client.leader_member_id()
        ),
        (7, 3, 1)
    );
    // Twice the leader heartbeat timeout the cluster gave, 1 s.
    assert_eq!(client.config().new_leader_timeout_ns, 2_000 * MS);
    // The other members' publications closed; the leader's kept as the ingress.
    let ingress = client.ingress_publication().unwrap();
    assert!(fake.channel(ingress).contains("host1"));
    assert!(!fake.closed_publications.contains(&ingress));
    // Member 0's, tried first; member 2 was never asked, member 1 connecting first.
    assert_eq!(fake.closed_publications.len(), 1);
    // The request: the egress channel resolved, Java's protocol version, the client's info.
    let request = &fake.offers[0].1;
    let text = String::from_utf8_lossy(request);
    assert!(text.contains(RESPONSE), "{text}");
    assert!(
        text.contains("name= version=1.54.2 commit=rusteron-cluster-core-"),
        "{text}"
    );
    assert_eq!(
        fake.channel(fake.offers[0].0),
        "aeron:udp?term-length=64k|endpoint=host1:9002"
    );
}

#[test]
fn a_connect_waits_for_the_egress_port_then_for_a_member() {
    let mut fake = Fake::new();
    fake.resolved = false;
    let mut connect = Connect::new(config(), 0).unwrap();
    drive(&mut connect, &mut fake).unwrap();
    assert_eq!(connect.state(), ConnectState::AwaitPublicationConnected);
    assert!(
        fake.publications.is_empty(),
        "no ingress before the response channel is known"
    );
    fake.resolved = true;
    drive(&mut connect, &mut fake).unwrap();
    assert_eq!(fake.publications.len(), 3, "a publication to every member");
    assert_eq!(connect.state(), ConnectState::AwaitPublicationConnected);
    fake.connected.insert("host2".into());
    drive(&mut connect, &mut fake).unwrap();
    assert_eq!(connect.state(), ConnectState::PollResponse);
    assert!(last_connect(&fake).1.contains("host2"));
}

#[test]
fn a_redirect_keeps_the_leaders_publication_if_its_endpoint_is_the_same() {
    let mut fake = Fake::new();
    // Member 0's publication ready but not connected; member 1, a follower, takes the request.
    fake.connected.insert("host1".into());
    let mut connect = Connect::new(config(), 0).unwrap();
    drive(&mut connect, &mut fake).unwrap();
    let (correlation, channel) = last_connect(&fake);
    assert!(channel.contains("host1"));
    let follower = fake.publication_to("host1").unwrap();
    let leader_publication = fake.publication_to("host0").unwrap();
    fake.egress(event(-1, correlation, 3, 0, EventCode::REDIRECT, ENDPOINTS), 51);
    drive(&mut connect, &mut fake).unwrap();
    // The follower's publication closed, the leader's kept, the request sent to it once connected.
    assert!(fake.closed_publications.contains(&follower));
    assert!(!fake.closed_publications.contains(&leader_publication));
    assert_eq!(connect.state(), ConnectState::AwaitPublicationConnected);
    fake.connected.insert("host0".into());
    drive(&mut connect, &mut fake).unwrap();
    let (again, channel) = last_connect(&fake);
    assert!(channel.contains("host0"), "{channel}");
    assert_ne!(again, correlation, "a new request, a new correlation id");
    assert_eq!(
        fake.calls
            .iter()
            .filter(|c| c.contains("add publication") && c.contains("host0"))
            .count(),
        1
    );
    fake.egress(event(9, again, 3, 0, EventCode::OK, ""), 52);
    let client = drive(&mut connect, &mut fake).unwrap().expect("connected");
    assert_eq!(
        (client.leader_member_id(), client.ingress_publication()),
        (0, Some(leader_publication))
    );
}

#[test]
fn a_redirect_to_a_moved_leader_adds_its_publication_again() {
    let mut fake = Fake::new();
    fake.connected.insert("host1".into());
    let mut connect = Connect::new(config(), 0).unwrap();
    drive(&mut connect, &mut fake).unwrap();
    let (correlation, _) = last_connect(&fake);
    let old = fake.publication_to("host0").unwrap();
    fake.egress(
        event(
            -1,
            correlation,
            3,
            0,
            EventCode::REDIRECT,
            "0=host9:9002,1=host1:9002,2=host2:9002",
        ),
        51,
    );
    drive(&mut connect, &mut fake).unwrap();
    assert!(
        fake.closed_publications.contains(&old),
        "the old leader's publication closed"
    );
    assert!(
        fake.publication_to("host9").is_some(),
        "a publication to the leader's new endpoint"
    );
}

struct Secret(Vec<u8>);

impl CredentialsSupplier for Secret {
    fn encoded_credentials(&mut self) -> Vec<u8> {
        b"user".to_vec()
    }

    fn on_challenge(&mut self, encoded_challenge: &[u8]) -> Vec<u8> {
        self.0 = encoded_challenge.to_vec();
        b"answer".to_vec()
    }
}

#[test]
fn a_challenge_is_answered_with_new_credentials_and_correlation_id() {
    let mut fake = Fake::new();
    fake.connected.insert("host1".into());
    let mut connect = Connect::with_credentials(config(), Secret(Vec::new()), 0).unwrap();
    drive(&mut connect, &mut fake).unwrap();
    let (correlation, _) = last_connect(&fake);
    assert!(String::from_utf8_lossy(&fake.offers[0].1).contains("user"));
    fake.egress(challenge(correlation, 7, b"nonce"), 51);
    drive(&mut connect, &mut fake).unwrap();
    assert_eq!(fake.template_sent(1), 8, "a challenge response");
    let response = &fake.offers[1].1;
    let response_correlation = i64::from_le_bytes(response[HEADER..HEADER + 8].try_into().unwrap());
    let session = i64::from_le_bytes(response[HEADER + 8..HEADER + 16].try_into().unwrap());
    assert_ne!(response_correlation, correlation);
    assert_eq!(session, 7);
    assert!(response.ends_with(b"answer"));
    // The server answers the response's correlation id.
    fake.egress(event(7, response_correlation, 3, 1, EventCode::OK, ""), 51);
    let client = drive(&mut connect, &mut fake).unwrap().expect("connected");
    assert_eq!(client.cluster_session_id(), 7);
}

#[test]
fn errors_and_rejections_end_the_connect_closed_is_ignored() {
    for (code, expected) in [
        (EventCode::ERROR, ClientError::Cluster("invalid client version".into())),
        (
            EventCode::AUTHENTICATION_REJECTED,
            ClientError::Authentication("invalid client version".into()),
        ),
    ] {
        let mut fake = Fake::new();
        fake.connected.insert("host1".into());
        let mut connect = Connect::new(config(), 0).unwrap();
        drive(&mut connect, &mut fake).unwrap();
        let (correlation, _) = last_connect(&fake);
        fake.egress(event(7, correlation, 3, 1, EventCode::CLOSED, "ignored"), 51);
        assert_eq!(drive(&mut connect, &mut fake).map(|c| c.is_some()), Ok(false));
        fake.egress(event(7, correlation, 3, 1, code, "invalid client version"), 51);
        assert_eq!(drive(&mut connect, &mut fake).map(|_| ()), Err(expected));
    }
}

#[test]
fn messages_for_other_requests_are_passed_over() {
    let mut fake = Fake::new();
    fake.connected.insert("host1".into());
    let mut connect = Connect::new(config(), 0).unwrap();
    drive(&mut connect, &mut fake).unwrap();
    let (correlation, _) = last_connect(&fake);
    fake.egress(message(7, 3, b"stray"), 51);
    fake.egress(event(7, correlation + 99, 3, 1, EventCode::OK, ""), 51);
    let mut foreign = codec::session_keep_alive(1, 1);
    foreign[4..6].copy_from_slice(&7001u16.to_le_bytes());
    fake.egress(foreign, 51);
    assert_eq!(drive(&mut connect, &mut fake).map(|c| c.is_some()), Ok(false));
    fake.egress(event(7, correlation, 3, 1, EventCode::OK, ""), 51);
    assert!(drive(&mut connect, &mut fake).unwrap().is_some());
}

#[test]
fn the_connect_has_one_deadline_redirects_and_challenges_included() {
    let mut fake = Fake::new();
    fake.add_polls = 1;
    fake.connected.insert("host0".into());
    fake.failing.insert("host1".into());
    let mut connect = Connect::new(config(), 0).unwrap();
    drive(&mut connect, &mut fake).unwrap();
    let (correlation, _) = last_connect(&fake);
    fake.now = 4_999 * MS;
    fake.egress(event(-1, correlation, 3, 2, EventCode::REDIRECT, ENDPOINTS), 51);
    drive(&mut connect, &mut fake).unwrap();
    fake.now = 5_000 * MS;
    assert!(
        drive(&mut connect, &mut fake).is_ok(),
        "the deadline itself is not past"
    );
    fake.now = 5_000 * MS + 1;
    match drive(&mut connect, &mut fake) {
        Err(ClientError::Timeout { message, member_errors }) => {
            assert!(message.starts_with("cluster connect timeout"), "{message}");
            assert!(member_errors.iter().any(|e| e.contains("host1")), "{member_errors:?}");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_egress_and_ingress_are_added_again_when_the_driver_has_no_room_and_fail_when_refused() {
    let mut fake = Fake::new();
    fake.unavailable.insert("endpoint=client".into());
    let mut connect = Connect::new(config(), 0).unwrap();
    drive(&mut connect, &mut fake).unwrap();
    assert_eq!(connect.state(), ConnectState::CreateEgressSubscription);
    assert!(fake.subscriptions.len() > 1, "added again each time");
    fake.unavailable.clear();
    drive(&mut connect, &mut fake).unwrap();
    assert_eq!(connect.state(), ConnectState::AwaitPublicationConnected);

    let mut fake = Fake::new();
    fake.failing.insert("endpoint=client".into());
    let mut connect = Connect::new(config(), 0).unwrap();
    assert!(matches!(
        drive(&mut connect, &mut fake),
        Err(ClientError::Registration(_))
    ));
}

#[test]
fn an_ingress_without_endpoints_is_one_publication_on_the_channel_as_given() {
    let mut fake = Fake::new();
    fake.connected.insert("aeron:ipc".into());
    let mut connect = Connect::new(
        ClientConfig {
            ingress_channel: "aeron:ipc".into(),
            ingress_endpoints: None,
            ..config()
        },
        0,
    )
    .unwrap();
    drive(&mut connect, &mut fake).unwrap();
    assert_eq!(fake.publications.values().collect::<Vec<_>>(), ["aeron:ipc"]);
    let (correlation, channel) = last_connect(&fake);
    assert_eq!(channel, "aeron:ipc");
    fake.egress(event(7, correlation, 3, 0, EventCode::OK, ""), 51);
    assert!(drive(&mut connect, &mut fake).unwrap().is_some());
}

#[test]
fn a_connect_given_up_releases_what_it_added_in_every_state() {
    // Adds pending: removed, not closed.
    let mut fake = Fake::new();
    fake.add_polls = 100;
    let mut connect = Connect::new(config(), 0).unwrap();
    drive(&mut connect, &mut fake).unwrap();
    connect.close(&mut fake);
    assert_eq!(fake.calls.last().unwrap(), "remove subscription 1000");

    // Publications to every member, none connected: all closed, the egress too.
    let mut fake = Fake::new();
    let mut connect = Connect::new(config(), 0).unwrap();
    drive(&mut connect, &mut fake).unwrap();
    connect.close(&mut fake);
    assert_eq!(fake.closed_subscriptions.len(), 1);
    assert_eq!(fake.closed_publications.len(), 3);

    // Done: nothing more to release.
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    let closes = fake.closed_publications.len();
    client.close(&mut fake);
    assert_eq!(fake.closed_publications.len(), closes + 1);
}

#[test]
fn the_config_is_checked_and_the_egress_made_not_to_rejoin() {
    let mut c = config();
    c.conclude().unwrap();
    assert_eq!(c.egress_channel, "aeron:udp?endpoint=client:0|rejoin=false");
    for bad in [
        ClientConfig {
            ingress_channel: String::new(),
            ..config()
        },
        ClientConfig {
            egress_channel: String::new(),
            ..config()
        },
        ClientConfig {
            ingress_channel: "aeron:ipc".into(),
            ..config()
        },
        ClientConfig {
            ingress_endpoints: Some("0:host0:9002".into()),
            ..config()
        },
        ClientConfig {
            client_name: "x".repeat(101),
            ..config()
        },
    ] {
        assert!(matches!(Connect::new(bad, 0), Err(ClientError::Configuration(_))));
    }
}

#[test]
fn an_offer_carries_the_session_header() {
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    let position = client.offer(&mut fake, &[b"buy", b" 10"]);
    assert!(position > 0);
    let (publication, sent) = fake.offers.last().unwrap();
    assert_eq!(Some(*publication), client.ingress_publication());
    assert_eq!(
        codec::decode_egress(sent),
        Ok(Egress::Message {
            leadership_term_id: 3,
            cluster_session_id: 7,
            timestamp: 0,
            payload: b"buy 10",
        })
    );
}

#[test]
fn a_lost_leader_closes_the_ingress_and_the_session_at_the_new_leader_timeout() {
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    fake.connected.clear();
    fake.now = 10 * MS;
    assert_eq!(client.offer(&mut fake, &[b"x"]), NOT_CONNECTED);
    assert_eq!(client.state(), ClientState::AwaitNewLeader);
    assert_eq!(client.ingress_publication(), None);
    assert_eq!(client.offer(&mut fake, &[b"x"]), CLOSED);
    // Closed at exactly the new leader timeout, 2 s, not before.
    fake.now = 10 * MS + 2_000 * MS - 1;
    client.poll_egress(&mut fake, &mut Heard::default()).unwrap();
    assert_eq!(client.state(), ClientState::AwaitNewLeader);
    fake.now += 1;
    client.poll_egress(&mut fake, &mut Heard::default()).unwrap();
    assert_eq!(client.state(), ClientState::Closed);
}

#[test]
fn a_new_leader_is_followed_and_connects_on_an_offer() {
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    let old = client.ingress_publication().unwrap();
    fake.add_polls = 1;
    fake.egress(new_leader(7, 4, 2, ENDPOINTS), 52);
    fake.egress(message(7, 4, b"after"), 52);
    let mut heard = Heard::default();
    client.poll_egress(&mut fake, &mut heard).unwrap();
    assert_eq!(heard.new_leaders, vec![(4, 2, ENDPOINTS.to_string())]);
    assert_eq!(heard.messages, vec![(7, b"after".to_vec())]);
    assert_eq!(client.state(), ClientState::AwaitNewLeaderConnection);
    assert_eq!((client.leadership_term_id(), client.leader_member_id()), (4, 2));
    assert!(fake.closed_publications.contains(&old));
    // The new leader's publication still being added: offers not connected, no state change.
    assert_eq!(client.ingress_publication(), None);
    assert_eq!(client.offer(&mut fake, &[b"x"]), NOT_CONNECTED);
    client.poll_egress(&mut fake, &mut heard).unwrap();
    let new = client.ingress_publication().expect("the new leader's publication");
    assert!(fake.channel(new).contains("host2"));
    assert_eq!(client.offer(&mut fake, &[b"x"]), NOT_CONNECTED);
    assert_eq!(client.state(), ClientState::AwaitNewLeaderConnection);
    fake.connected.insert("host2".into());
    assert!(client.offer(&mut fake, &[b"x"]) > 0);
    assert_eq!(client.state(), ClientState::Connected);
    // The new term in every header since.
    let Egress::Message { leadership_term_id, .. } = codec::decode_egress(&fake.offers.last().unwrap().1).unwrap()
    else {
        panic!()
    };
    assert_eq!(leadership_term_id, 4);
}

#[test]
fn a_new_leader_not_connected_within_the_message_timeout_closes_the_session() {
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    fake.egress(new_leader(7, 4, 2, ENDPOINTS), 52);
    client.poll_egress(&mut fake, &mut Heard::default()).unwrap();
    assert_eq!(client.state(), ClientState::AwaitNewLeaderConnection);
    fake.now = 5_000 * MS - 1;
    client.poll_egress(&mut fake, &mut Heard::default()).unwrap();
    assert_eq!(client.state(), ClientState::AwaitNewLeaderConnection);
    fake.now = 5_000 * MS;
    client.poll_egress(&mut fake, &mut Heard::default()).unwrap();
    assert_eq!(client.state(), ClientState::Closed);
}

#[test]
fn a_new_leaders_publication_never_added_times_out() {
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    fake.add_polls = usize::MAX;
    fake.egress(new_leader(7, 4, 2, ENDPOINTS), 52);
    client.poll_egress(&mut fake, &mut Heard::default()).unwrap();
    fake.now = 5_000 * MS;
    assert!(matches!(
        client.poll_egress(&mut fake, &mut Heard::default()),
        Err(ClientError::Timeout { .. })
    ));
    assert_eq!(
        client
            .poll_egress(&mut fake, &mut Heard::default())
            .map(|_| client.state()),
        Ok(ClientState::Closed)
    );
}

#[test]
fn the_egress_image_closing_closes_the_ingress() {
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    let ingress = client.ingress_publication().unwrap();
    fake.closed_images.insert(51);
    client.poll_egress(&mut fake, &mut Heard::default()).unwrap();
    assert_eq!(client.state(), ClientState::AwaitNewLeader);
    assert!(fake.closed_publications.contains(&ingress));
}

#[test]
fn max_position_exceeded_closes_the_session() {
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    fake.results.push_back(MAX_POSITION_EXCEEDED);
    assert_eq!(client.offer(&mut fake, &[b"x"]), MAX_POSITION_EXCEEDED);
    assert_eq!(client.state(), ClientState::PendingClose);
    client.poll_egress(&mut fake, &mut Heard::default()).unwrap();
    assert_eq!(client.state(), ClientState::Closed);
}

#[test]
fn a_message_refused_for_itself_leaves_the_session_connected() {
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    fake.results.push_back(PUBLICATION_ERROR);
    assert_eq!(client.offer(&mut fake, &[b"x"]), PUBLICATION_ERROR);
    assert_eq!(client.state(), ClientState::Connected);
    assert!(client.ingress_publication().is_some());
}

#[test]
fn the_cluster_closing_the_session_closes_the_client_once_heard() {
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    fake.egress(event(7, 0, 3, 1, EventCode::CLOSED, "TIMEOUT"), 51);
    let mut heard = Heard::default();
    client.poll_egress(&mut fake, &mut heard).unwrap();
    assert_eq!(heard.events, vec![(0, EventCode::CLOSED, "TIMEOUT".to_string())]);
    assert_eq!(client.state(), ClientState::Closed);
}

#[test]
fn only_this_sessions_messages_are_heard() {
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    fake.egress(message(8, 3, b"not ours"), 51);
    fake.egress(event(8, 0, 3, 1, EventCode::CLOSED, ""), 51);
    fake.egress(new_leader(8, 9, 2, ENDPOINTS), 51);
    fake.egress(admin_response(8, 5, b""), 51);
    fake.egress(message(7, 3, b"ours"), 51);
    fake.egress(admin_response(7, 6, b"payload"), 51);
    let mut heard = Heard::default();
    client.poll_egress(&mut fake, &mut heard).unwrap();
    assert_eq!(heard.messages, vec![(7, b"ours".to_vec())]);
    assert!(heard.events.is_empty() && heard.new_leaders.is_empty());
    assert_eq!(heard.admin, vec![(6, b"payload".to_vec())]);
    assert_eq!(client.state(), ClientState::Connected);
}

#[test]
fn a_poll_reads_ten_messages_at_most() {
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    for i in 0..15u8 {
        fake.egress(message(7, 3, &[i]), 51);
    }
    let mut heard = Heard::default();
    assert_eq!(client.poll_egress(&mut fake, &mut heard).unwrap(), 10);
    assert_eq!(client.poll_egress(&mut fake, &mut heard).unwrap(), 5);
    assert_eq!(heard.messages.len(), 15);
}

#[test]
fn another_schema_on_the_egress_is_an_error() {
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    let mut foreign = codec::session_keep_alive(1, 1);
    foreign[4..6].copy_from_slice(&7001u16.to_le_bytes());
    fake.egress(foreign, 51);
    assert_eq!(
        client.poll_egress(&mut fake, &mut Heard::default()),
        Err(ClientError::Cluster(
            "expected cluster egress schemaId=111 actual=7001".into()
        ))
    );
}

#[test]
fn a_close_tells_the_cluster_then_closes_egress_and_ingress() {
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    let (ingress, egress) = (client.ingress_publication().unwrap(), client.egress_subscription());
    fake.calls.clear();
    client.close(&mut fake);
    assert_eq!(
        fake.calls,
        vec![
            format!("offer {ingress}"),
            format!("close subscription {egress}"),
            format!("close publication {ingress}"),
        ]
    );
    assert_eq!(fake.template_sent(fake.offers.len() - 1), 4, "a session close request");
    client.close(&mut fake);
    assert_eq!(fake.calls.len(), 3, "closed once");
}

#[test]
fn keepalives_and_admin_requests_try_three_times() {
    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    assert_eq!(client.send_keep_alive(&mut fake), Ok(true));
    assert_eq!(fake.template_sent(fake.offers.len() - 1), 5);
    fake.results.extend([BACK_PRESSURED; 3]);
    assert_eq!(client.send_keep_alive(&mut fake), Ok(false));
    assert_eq!(fake.idles, 2);
    fake.results.push_back(MAX_POSITION_EXCEEDED);
    assert!(client.send_keep_alive(&mut fake).is_err());

    let mut fake = Fake::new();
    let mut client = connected(&mut fake);
    assert_eq!(client.send_admin_request_to_take_a_snapshot(&mut fake, 42), Ok(true));
    assert_eq!(fake.template_sent(fake.offers.len() - 1), 26);
    fake.results.push_back(CLOSED);
    assert_eq!(
        client.send_admin_request_to_take_a_snapshot(&mut fake, 43),
        Err(ClientError::Cluster("ingress publication is closed".into()))
    );
}
