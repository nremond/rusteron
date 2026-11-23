//! A session opened (Java's `AeronCluster.AsyncConnect` and `EgressPoller`).

use std::collections::BTreeMap;

use super::codec::{self, Egress, PROTOCOL_SEMANTIC_VERSION};
use super::{
    Added, CLOSED, Client, ClientConfig, ClientError, ClientPorts, CredentialsSupplier, EventCode, FRAGMENT_LIMIT,
    Handle, LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS, MemberIngress, NOT_CONNECTED, NULL_VALUE, NullCredentials, PollAction,
    client_info, close_all, members,
};

/// A connect's steps (`AsyncConnect.State`), each its `step`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectState {
    CreateEgressSubscription,
    CreateIngressPublications,
    AwaitPublicationConnected,
    SendMessage,
    PollResponse,
    ConcludeConnect,
    Done,
}

impl ConnectState {
    /// Java's `step()`: -1 to 5.
    pub fn step(self) -> i32 {
        match self {
            Self::CreateEgressSubscription => -1,
            Self::CreateIngressPublications => 0,
            Self::AwaitPublicationConnected => 1,
            Self::SendMessage => 2,
            Self::PollResponse => 3,
            Self::ConcludeConnect => 4,
            Self::Done => 5,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::CreateEgressSubscription => "CREATE_EGRESS_SUBSCRIPTION",
            Self::CreateIngressPublications => "CREATE_INGRESS_PUBLICATIONS",
            Self::AwaitPublicationConnected => "AWAIT_PUBLICATION_CONNECTED",
            Self::SendMessage => "SEND_MESSAGE",
            Self::PollResponse => "POLL_RESPONSE",
            Self::ConcludeConnect => "CONCLUDE_CONNECT",
            Self::Done => "DONE",
        }
    }
}

/// The egress read during a connect, one message a poll (Java's `EgressPoller`).
#[derive(Debug)]
struct Poller {
    complete: bool,
    template_id: i32,
    cluster_session_id: i64,
    correlation_id: i64,
    leadership_term_id: i64,
    leader_member_id: i32,
    event_code: Option<EventCode>,
    leader_heartbeat_timeout_ns: i64,
    detail: String,
    encoded_challenge: Vec<u8>,
    egress_image: Option<i32>,
}

impl Default for Poller {
    fn default() -> Self {
        Self {
            complete: false,
            template_id: NULL_VALUE as i32,
            cluster_session_id: NULL_VALUE,
            correlation_id: NULL_VALUE,
            leadership_term_id: NULL_VALUE,
            leader_member_id: NULL_VALUE as i32,
            event_code: None,
            leader_heartbeat_timeout_ns: NULL_VALUE,
            detail: String::new(),
            encoded_challenge: Vec::new(),
            egress_image: None,
        }
    }
}

impl Poller {
    fn poll(&mut self, ports: &mut impl ClientPorts, subscription: Handle) -> usize {
        if self.complete {
            let egress_image = self.egress_image;
            *self = Self {
                egress_image,
                ..Self::default()
            };
        }
        ports.controlled_poll(subscription, FRAGMENT_LIMIT, &mut |message, image| {
            self.on_fragment(message, image)
        })
    }

    fn on_fragment(&mut self, message: &[u8], image: i32) -> PollAction {
        if self.complete {
            return PollAction::Abort;
        }
        // A message cut short is skipped, as another schema's is.
        let Ok(egress) = codec::decode_egress(message) else {
            return PollAction::Continue;
        };
        match egress {
            Egress::Foreign { .. } => return PollAction::Continue,
            Egress::Message {
                leadership_term_id,
                cluster_session_id,
                ..
            } => {
                self.template_id = 1;
                self.leadership_term_id = leadership_term_id;
                self.cluster_session_id = cluster_session_id;
            }
            Egress::SessionEvent {
                cluster_session_id,
                correlation_id,
                leadership_term_id,
                leader_member_id,
                code,
                leader_heartbeat_timeout_ns,
                detail,
                ..
            } => {
                self.template_id = 2;
                self.cluster_session_id = cluster_session_id;
                self.correlation_id = correlation_id;
                self.leadership_term_id = leadership_term_id;
                self.leader_member_id = leader_member_id;
                self.event_code = Some(code);
                self.leader_heartbeat_timeout_ns = leader_heartbeat_timeout_ns;
                self.detail = detail;
                self.egress_image = Some(image);
            }
            Egress::NewLeader {
                leadership_term_id,
                cluster_session_id,
                leader_member_id,
                ingress_endpoints,
            } => {
                self.template_id = 6;
                self.cluster_session_id = cluster_session_id;
                self.leadership_term_id = leadership_term_id;
                self.leader_member_id = leader_member_id;
                self.detail = ingress_endpoints;
                self.egress_image = Some(image);
            }
            Egress::Challenge {
                correlation_id,
                cluster_session_id,
                encoded_challenge,
            } => {
                self.template_id = 7;
                self.encoded_challenge = encoded_challenge.to_vec();
                self.cluster_session_id = cluster_session_id;
                self.correlation_id = correlation_id;
            }
            Egress::AdminResponse { .. } => {
                self.template_id = 27;
                return PollAction::Continue;
            }
            Egress::Other { template_id } => {
                self.template_id = i32::from(template_id);
                return PollAction::Continue;
            }
        }
        self.complete = true;
        PollAction::Break
    }

    fn is_challenged(&self) -> bool {
        self.template_id == 7
    }
}

/// A session being opened: [`Connect::poll`] it until it gives the
/// [`Client`], an error, or the connect's deadline passes.
pub struct Connect<C: CredentialsSupplier = NullCredentials> {
    config: ClientConfig,
    credentials: C,
    deadline_ns: i64,
    state: ConnectState,
    correlation_id: i64,
    cluster_session_id: i64,
    leadership_term_id: i64,
    leader_member_id: i32,
    leader_heartbeat_timeout_ns: i64,
    egress_image: Option<i32>,
    message: Vec<u8>,
    egress_registration_id: i64,
    egress_subscription: Option<Handle>,
    poller: Poller,
    members: BTreeMap<i32, MemberIngress>,
    ingress_registration_id: i64,
    ingress_publication: Option<Handle>,
}

impl Connect<NullCredentials> {
    /// A connect with no credentials, its deadline the message timeout from now.
    pub fn new(config: ClientConfig, now_ns: i64) -> Result<Self, ClientError> {
        Self::with_credentials(config, NullCredentials, now_ns)
    }
}

impl<C: CredentialsSupplier> Connect<C> {
    /// `config` concluded here.
    pub fn with_credentials(mut config: ClientConfig, credentials: C, now_ns: i64) -> Result<Self, ClientError> {
        config.conclude()?;
        let deadline_ns = now_ns + config.message_timeout_ns;
        Self::with_deadline(config, credentials, deadline_ns)
    }

    /// `config` already concluded, the deadline given (Java's package-private constructor).
    pub fn with_deadline(config: ClientConfig, credentials: C, deadline_ns: i64) -> Result<Self, ClientError> {
        let members = members(config.ingress_endpoints.as_deref())?;
        Ok(Self {
            config,
            credentials,
            deadline_ns,
            state: ConnectState::CreateEgressSubscription,
            correlation_id: NULL_VALUE,
            cluster_session_id: NULL_VALUE,
            leadership_term_id: NULL_VALUE,
            leader_member_id: NULL_VALUE as i32,
            leader_heartbeat_timeout_ns: NULL_VALUE,
            egress_image: None,
            message: Vec::new(),
            egress_registration_id: NULL_VALUE,
            egress_subscription: None,
            poller: Poller::default(),
            members,
            ingress_registration_id: NULL_VALUE,
            ingress_publication: None,
        })
    }

    pub fn state(&self) -> ConnectState {
        self.state
    }

    pub fn step(&self) -> i32 {
        self.state.step()
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// One step of the connect: the client once connected.
    pub fn poll(&mut self, ports: &mut impl ClientPorts) -> Result<Option<Client>, ClientError> {
        self.check_deadline(ports)?;
        match self.state {
            ConnectState::CreateEgressSubscription => self.create_egress_subscription(ports)?,
            ConnectState::CreateIngressPublications => self.create_ingress_publications(ports)?,
            ConnectState::AwaitPublicationConnected => self.await_publication_connected(ports),
            ConnectState::SendMessage => self.send_message(ports)?,
            ConnectState::PollResponse => self.poll_response(ports)?,
            ConnectState::ConcludeConnect => return Ok(Some(self.conclude_connect(ports))),
            ConnectState::Done => {}
        }
        Ok(None)
    }

    /// Gives up the connect: what it added is closed, or its adds removed.
    pub fn close(&mut self, ports: &mut impl ClientPorts) {
        if self.state == ConnectState::Done {
            return;
        }
        if let Some(publication) = self.ingress_publication.take() {
            ports.close_publication(publication);
        } else if self.ingress_registration_id != NULL_VALUE {
            ports.async_remove_publication(self.ingress_registration_id);
        }
        if let Some(subscription) = self.egress_subscription.take() {
            ports.close_subscription(subscription);
        } else if self.egress_registration_id != NULL_VALUE {
            ports.async_remove_subscription(self.egress_registration_id);
        }
        close_all(&mut self.members, ports);
        self.state = ConnectState::Done;
    }

    fn check_deadline(&self, ports: &impl ClientPorts) -> Result<(), ClientError> {
        if self.deadline_ns - ports.now_ns() < 0 {
            let response_channel = self
                .egress_subscription
                .and_then(|s| ports.resolved_channel(s))
                .unwrap_or_else(|| "<unknown>".into());
            return Err(ClientError::Timeout {
                message: format!(
                    "cluster connect timeout: state={} messageTimeout={}ms ingressChannel={} ingressEndpoints={} \
                     ingressPublication={:?} responseChannel={response_channel}",
                    self.state.name(),
                    self.config.message_timeout_ns / 1_000_000,
                    self.config.ingress_channel,
                    self.config.ingress_endpoints.as_deref().unwrap_or("null"),
                    self.ingress_publication,
                ),
                member_errors: self.members.values().filter_map(|m| m.error.clone()).collect(),
            });
        }
        Ok(())
    }

    fn create_egress_subscription(&mut self, ports: &mut impl ClientPorts) -> Result<(), ClientError> {
        if self.egress_registration_id == NULL_VALUE {
            self.egress_registration_id =
                ports.async_add_subscription(&self.config.egress_channel, self.config.egress_stream_id);
        }
        match ports.get_subscription(self.egress_registration_id) {
            Added::Pending => {}
            Added::Ready(subscription) => {
                self.egress_subscription = Some(subscription);
                self.egress_registration_id = NULL_VALUE;
                self.state = ConnectState::CreateIngressPublications;
            }
            Added::TemporarilyUnavailable => self.egress_registration_id = NULL_VALUE,
            Added::Failed(e) => {
                self.egress_registration_id = NULL_VALUE;
                return Err(ClientError::Registration(e));
            }
        }
        Ok(())
    }

    fn create_ingress_publications(&mut self, ports: &mut impl ClientPorts) -> Result<(), ClientError> {
        if self.config.ingress_endpoints.is_some() {
            self.state = ConnectState::AwaitPublicationConnected;
            return Ok(());
        }
        if self.ingress_publication.is_some() {
            self.ingress_registration_id = NULL_VALUE;
            self.state = ConnectState::AwaitPublicationConnected;
            return Ok(());
        }
        if self.ingress_registration_id == NULL_VALUE {
            self.ingress_registration_id = ports.async_add_publication(
                &self.config.ingress_channel,
                self.config.ingress_stream_id,
                self.config.is_ingress_exclusive,
            );
        }
        match ports.get_publication(self.ingress_registration_id) {
            Added::Pending => {}
            Added::Ready(publication) => self.ingress_publication = Some(publication),
            Added::TemporarilyUnavailable => self.ingress_registration_id = NULL_VALUE,
            Added::Failed(e) => {
                self.ingress_registration_id = NULL_VALUE;
                return Err(ClientError::Registration(e));
            }
        }
        Ok(())
    }

    fn await_publication_connected(&mut self, ports: &mut impl ClientPorts) {
        let Some(response_channel) = self.egress_subscription.and_then(|s| ports.resolved_channel(s)) else {
            return;
        };
        match self.ingress_publication {
            None => {
                let mut connected = None;
                for member in self.members.values_mut() {
                    if member.publication.is_none() && member.error.is_none() {
                        if member.registration_id == NULL_VALUE {
                            member.async_add_publication(&self.config, ports);
                        }
                        member.async_get_publication(ports);
                    }
                    if let Some(publication) = member.publication
                        && ports.is_connected(publication)
                    {
                        connected = Some(publication);
                        break;
                    }
                }
                if let Some(publication) = connected {
                    self.ingress_publication = Some(publication);
                    self.prepare_connect_request(ports, &response_channel);
                }
            }
            Some(publication) => {
                if ports.is_connected(publication) {
                    self.prepare_connect_request(ports, &response_channel);
                }
            }
        }
    }

    fn prepare_connect_request(&mut self, ports: &mut impl ClientPorts, response_channel: &str) {
        self.correlation_id = ports.next_correlation_id();
        let credentials = self.credentials.encoded_credentials();
        self.message = codec::session_connect_request(
            self.correlation_id,
            self.config.egress_stream_id,
            PROTOCOL_SEMANTIC_VERSION,
            response_channel,
            &credentials,
            &client_info(&self.config.client_name),
        );
        self.state = ConnectState::SendMessage;
    }

    fn send_message(&mut self, ports: &mut impl ClientPorts) -> Result<(), ClientError> {
        let publication = self.ingress_publication.expect("an ingress publication");
        let position = ports.offer(publication, &[&self.message]);
        if position > 0 {
            self.state = ConnectState::PollResponse;
        } else if position == CLOSED || position == NOT_CONNECTED {
            return Err(ClientError::Cluster("unexpected loss of connection to cluster".into()));
        }
        Ok(())
    }

    fn poll_response(&mut self, ports: &mut impl ClientPorts) -> Result<(), ClientError> {
        let subscription = self.egress_subscription.expect("an egress subscription");
        if !(self.poller.poll(ports, subscription) > 0
            && self.poller.complete
            && self.poller.correlation_id == self.correlation_id)
        {
            return Ok(());
        }
        if self.poller.is_challenged() {
            self.correlation_id = NULL_VALUE;
            self.cluster_session_id = self.poller.cluster_session_id;
            let credentials = self.credentials.on_challenge(&self.poller.encoded_challenge);
            self.correlation_id = ports.next_correlation_id();
            self.message = codec::challenge_response(self.correlation_id, self.cluster_session_id, &credentials);
            self.state = ConnectState::SendMessage;
            return Ok(());
        }
        match self.poller.event_code {
            Some(EventCode::OK) => {
                self.leadership_term_id = self.poller.leadership_term_id;
                self.leader_member_id = self.poller.leader_member_id;
                self.cluster_session_id = self.poller.cluster_session_id;
                self.leader_heartbeat_timeout_ns = self.poller.leader_heartbeat_timeout_ns;
                self.egress_image = self.poller.egress_image;
                self.state = ConnectState::ConcludeConnect;
            }
            Some(EventCode::ERROR) => return Err(ClientError::Cluster(self.poller.detail.clone())),
            Some(EventCode::REDIRECT) => self.update_members(ports)?,
            Some(EventCode::AUTHENTICATION_REJECTED) => {
                return Err(ClientError::Authentication(self.poller.detail.clone()));
            }
            Some(EventCode::CLOSED) | Some(EventCode::NullVal) | None => {}
        }
        Ok(())
    }

    /// A follower's redirect: the members as it lists them, the leader's
    /// publication kept if its endpoint is the same.
    fn update_members(&mut self, ports: &mut impl ClientPorts) -> Result<(), ClientError> {
        self.leader_member_id = self.poller.leader_member_id;
        let old_leader = self.members.remove(&self.leader_member_id);
        if let Some(publication) = self.ingress_publication.take() {
            ports.close_publication(publication);
        }
        close_all(&mut self.members, ports);
        self.members = members(Some(&self.poller.detail))?;
        let Some(new_leader) = self.members.get_mut(&self.leader_member_id) else {
            return Err(ClientError::Cluster(format!(
                "redirect to member {} not among {}",
                self.leader_member_id, self.poller.detail
            )));
        };
        match old_leader {
            Some(old) if old.error.is_none() && old.endpoint == new_leader.endpoint => {
                new_leader.publication = old.publication;
                new_leader.registration_id = old.registration_id;
                self.ingress_publication = old.publication;
            }
            old => {
                if let Some(mut old) = old {
                    old.close(ports);
                }
                new_leader.async_add_publication(&self.config, ports);
            }
        }
        self.state = ConnectState::AwaitPublicationConnected;
        Ok(())
    }

    fn conclude_connect(&mut self, ports: &mut impl ClientPorts) -> Client {
        if self.config.new_leader_timeout_ns == NULL_VALUE {
            let heartbeat_timeout_ns = if self.leader_heartbeat_timeout_ns != NULL_VALUE {
                self.leader_heartbeat_timeout_ns
            } else {
                LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS
            };
            self.config.new_leader_timeout_ns = 2 * heartbeat_timeout_ns;
        }
        self.members.remove(&self.leader_member_id);
        close_all(&mut self.members, ports);
        let client = Client::new(
            self.config.clone(),
            self.ingress_publication.take().expect("an ingress publication"),
            self.egress_subscription.take().expect("an egress subscription"),
            self.egress_image,
            std::mem::take(&mut self.members),
            self.cluster_session_id,
            self.leadership_term_id,
            self.leader_member_id,
        );
        self.state = ConnectState::Done;
        client
    }
}
