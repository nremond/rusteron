//! An open session (Java's `AeronCluster` once connected): what it sends on
//! the leader's ingress, what it reads from its egress, and how it follows a
//! new leader or closes.

use std::collections::BTreeMap;

use super::codec::{self, Egress, SESSION_HEADER_LENGTH};
use super::{
    Added, AdminRequestType, AdminResponseCode, CLOSED, ClientConfig, ClientError, ClientPorts, EventCode,
    FRAGMENT_LIMIT, Handle, MAX_POSITION_EXCEEDED, MemberIngress, NOT_CONNECTED, NULL_VALUE, PollAction, SEND_ATTEMPTS,
    close_all, members,
};

/// A session's states (`AeronCluster.State`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientState {
    Connected,
    /// The leader's ingress or egress gone: waiting for a new leader's word.
    AwaitNewLeader,
    /// Told of a new leader: connected again once an offer to it succeeds.
    AwaitNewLeaderConnection,
    PendingClose,
    Closed,
}

/// What a session receives (Java's `EgressListener`).
pub trait EgressListener {
    /// A service's message to this session.
    fn on_message(&mut self, cluster_session_id: i64, timestamp: i64, payload: &[u8]);

    fn on_session_event(
        &mut self,
        _correlation_id: i64,
        _cluster_session_id: i64,
        _leadership_term_id: i64,
        _leader_member_id: i32,
        _code: EventCode,
        _detail: &str,
    ) {
    }

    fn on_new_leader(
        &mut self,
        _cluster_session_id: i64,
        _leadership_term_id: i64,
        _leader_member_id: i32,
        _ingress_endpoints: &str,
    ) {
    }

    #[allow(clippy::too_many_arguments)]
    fn on_admin_response(
        &mut self,
        _cluster_session_id: i64,
        _correlation_id: i64,
        _request_type: AdminRequestType,
        _response_code: AdminResponseCode,
        _message: &str,
        _payload: &[u8],
    ) {
    }
}

/// What a session receives, its messages taken or left for the next poll
/// (Java's `ControlledEgressListener`).
pub trait ControlledEgressListener {
    fn on_message(&mut self, cluster_session_id: i64, timestamp: i64, payload: &[u8]) -> PollAction;

    fn on_session_event(
        &mut self,
        _correlation_id: i64,
        _cluster_session_id: i64,
        _leadership_term_id: i64,
        _leader_member_id: i32,
        _code: EventCode,
        _detail: &str,
    ) {
    }

    fn on_new_leader(
        &mut self,
        _cluster_session_id: i64,
        _leadership_term_id: i64,
        _leader_member_id: i32,
        _ingress_endpoints: &str,
    ) {
    }

    #[allow(clippy::too_many_arguments)]
    fn on_admin_response(
        &mut self,
        _cluster_session_id: i64,
        _correlation_id: i64,
        _request_type: AdminRequestType,
        _response_code: AdminResponseCode,
        _message: &str,
        _payload: &[u8],
    ) {
    }
}

/// An uncontrolled listener seen as a controlled one that takes everything.
struct Taking<'a, L: EgressListener>(&'a mut L);

impl<L: EgressListener> ControlledEgressListener for Taking<'_, L> {
    fn on_message(&mut self, cluster_session_id: i64, timestamp: i64, payload: &[u8]) -> PollAction {
        self.0.on_message(cluster_session_id, timestamp, payload);
        PollAction::Continue
    }

    fn on_session_event(
        &mut self,
        correlation_id: i64,
        cluster_session_id: i64,
        leadership_term_id: i64,
        leader_member_id: i32,
        code: EventCode,
        detail: &str,
    ) {
        self.0.on_session_event(
            correlation_id,
            cluster_session_id,
            leadership_term_id,
            leader_member_id,
            code,
            detail,
        );
    }

    fn on_new_leader(
        &mut self,
        cluster_session_id: i64,
        leadership_term_id: i64,
        leader_member_id: i32,
        ingress_endpoints: &str,
    ) {
        self.0.on_new_leader(
            cluster_session_id,
            leadership_term_id,
            leader_member_id,
            ingress_endpoints,
        );
    }

    fn on_admin_response(
        &mut self,
        cluster_session_id: i64,
        correlation_id: i64,
        request_type: AdminRequestType,
        response_code: AdminResponseCode,
        message: &str,
        payload: &[u8],
    ) {
        self.0.on_admin_response(
            cluster_session_id,
            correlation_id,
            request_type,
            response_code,
            message,
            payload,
        );
    }
}

/// A new leader's publication, being added: Java blocks for it, the client
/// waits for it across polls.
#[derive(Debug)]
struct PendingPublication {
    channel: String,
    registration_id: i64,
    deadline_ns: i64,
}

/// A connected session: offer messages, poll the egress, close.
#[derive(Debug)]
pub struct Client {
    config: ClientConfig,
    state: ClientState,
    state_deadline_ns: i64,
    cluster_session_id: i64,
    leadership_term_id: i64,
    leader_member_id: i32,
    publication: Option<Handle>,
    pending: Option<PendingPublication>,
    subscription: Handle,
    egress_image: Option<i32>,
    members: BTreeMap<i32, MemberIngress>,
    header: [u8; SESSION_HEADER_LENGTH],
    /// New leaders read in a poll, their publications switched after it.
    new_leaders: Vec<(i32, String)>,
}

impl Client {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config: ClientConfig,
        publication: Handle,
        subscription: Handle,
        egress_image: Option<i32>,
        members: BTreeMap<i32, MemberIngress>,
        cluster_session_id: i64,
        leadership_term_id: i64,
        leader_member_id: i32,
    ) -> Self {
        Self {
            header: codec::session_message_header(leadership_term_id, cluster_session_id),
            config,
            state: ClientState::Connected,
            state_deadline_ns: 0,
            cluster_session_id,
            leadership_term_id,
            leader_member_id,
            publication: Some(publication),
            pending: None,
            subscription,
            egress_image,
            members,
            new_leaders: Vec::new(),
        }
    }

    pub fn state(&self) -> ClientState {
        self.state
    }

    pub fn is_closed(&self) -> bool {
        self.state == ClientState::Closed
    }

    pub fn cluster_session_id(&self) -> i64 {
        self.cluster_session_id
    }

    pub fn leadership_term_id(&self) -> i64 {
        self.leadership_term_id
    }

    pub fn leader_member_id(&self) -> i32 {
        self.leader_member_id
    }

    /// The settings, concluded, the new leader timeout set.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// The leader's ingress publication; none while it is being added, or once closed.
    pub fn ingress_publication(&self) -> Option<Handle> {
        self.publication
    }

    pub fn egress_subscription(&self) -> Handle {
        self.subscription
    }

    /// The session message header every offer puts before its payload, for
    /// a shell that claims space in the publication itself.
    pub fn session_header(&self) -> &[u8; SESSION_HEADER_LENGTH] {
        &self.header
    }

    /// Offers a message to the cluster's services: its parts after the
    /// session header, as one message. Aeron's result: a position, or why not.
    pub fn offer(&mut self, ports: &mut impl ClientPorts, payload: &[&[u8]]) -> i64 {
        let result = match self.publication {
            Some(publication) => {
                let mut parts: Vec<&[u8]> = Vec::with_capacity(payload.len() + 1);
                parts.push(&self.header);
                parts.extend_from_slice(payload);
                ports.offer(publication, &parts)
            }
            None => self.no_publication(),
        };
        self.track_ingress_result(ports, result);
        result
    }

    /// An offer's result while there is no publication: not connected while
    /// the new leader's is added, as Java's would be once its wait ended; else closed.
    fn no_publication(&self) -> i64 {
        if self.pending.is_some() { NOT_CONNECTED } else { CLOSED }
    }

    /// Follows what an offer, or a claim the shell made itself, returned
    /// (Java's `trackIngressPublicationResult`).
    pub fn track_ingress_result(&mut self, ports: &mut impl ClientPorts, result: i64) {
        match self.state {
            ClientState::Connected => {
                if result == NOT_CONNECTED || result == CLOSED {
                    self.on_disconnected(ports);
                } else if result == MAX_POSITION_EXCEEDED {
                    if let Some(publication) = self.publication.take() {
                        ports.close_publication(publication);
                    }
                    self.set_state(ClientState::PendingClose, 0);
                }
            }
            ClientState::AwaitNewLeaderConnection if result > 0 => self.set_state(ClientState::Connected, 0),
            _ => {}
        }
    }

    fn offer_control(&mut self, ports: &mut impl ClientPorts, message: &[u8]) -> i64 {
        let result = match self.publication {
            Some(publication) => ports.offer(publication, &[message]),
            None => self.no_publication(),
        };
        self.track_ingress_result(ports, result);
        result
    }

    /// Keeps the session open while it sends nothing else: tried three times.
    pub fn send_keep_alive(&mut self, ports: &mut impl ClientPorts) -> Result<bool, ClientError> {
        let message = codec::session_keep_alive(self.leadership_term_id, self.cluster_session_id);
        for attempt in 1..=SEND_ATTEMPTS {
            let position = self.offer_control(ports, &message);
            if position > 0 {
                return Ok(true);
            }
            if position == MAX_POSITION_EXCEEDED {
                return Err(ClientError::Cluster("max position exceeded".into()));
            }
            if attempt < SEND_ATTEMPTS {
                ports.idle();
            }
        }
        Ok(false)
    }

    /// Asks the cluster for a snapshot: answered with an admin response.
    pub fn send_admin_request_to_take_a_snapshot(
        &mut self,
        ports: &mut impl ClientPorts,
        correlation_id: i64,
    ) -> Result<bool, ClientError> {
        let message = codec::admin_request(
            self.leadership_term_id,
            self.cluster_session_id,
            correlation_id,
            AdminRequestType::SNAPSHOT,
        );
        for attempt in 1..=SEND_ATTEMPTS {
            let position = self.offer_control(ports, &message);
            if position > 0 {
                return Ok(true);
            }
            if position == CLOSED {
                return Err(ClientError::Cluster("ingress publication is closed".into()));
            }
            if position == MAX_POSITION_EXCEEDED {
                return Err(ClientError::Cluster("max position exceeded".into()));
            }
            if attempt < SEND_ATTEMPTS {
                ports.idle();
            }
        }
        Ok(false)
    }

    /// Reads the egress, at most [`FRAGMENT_LIMIT`] messages, then follows
    /// what changed: a new leader, the egress gone, a timeout, a close.
    pub fn poll_egress(
        &mut self,
        ports: &mut impl ClientPorts,
        listener: &mut impl EgressListener,
    ) -> Result<usize, ClientError> {
        self.controlled_poll_egress(ports, &mut Taking(listener))
    }

    /// As [`Self::poll_egress`], the listener taking or leaving each message.
    pub fn controlled_poll_egress(
        &mut self,
        ports: &mut impl ClientPorts,
        listener: &mut impl ControlledEgressListener,
    ) -> Result<usize, ClientError> {
        let mut work = self.await_new_leader_publication(ports)?;
        let now_ns = ports.now_ns();
        let mut error = None;
        let subscription = self.subscription;
        work += ports.controlled_poll(subscription, FRAGMENT_LIMIT, &mut |message, image| {
            if error.is_some() {
                return PollAction::Abort;
            }
            match self.on_fragment(message, image, now_ns, listener) {
                Ok(action) => action,
                Err(e) => {
                    error = Some(e);
                    PollAction::Break
                }
            }
        });
        for (leader_member_id, ingress_endpoints) in std::mem::take(&mut self.new_leaders) {
            self.switch_to_new_leader(ports, leader_member_id, &ingress_endpoints)?;
            work += self.await_new_leader_publication(ports)?;
        }
        if let Some(e) = error {
            return Err(e);
        }
        if let Some(image) = self.egress_image
            && matches!(
                self.state,
                ClientState::Connected | ClientState::AwaitNewLeaderConnection
            )
            && ports.is_image_closed(subscription, image)
        {
            self.on_disconnected(ports);
            work += 1;
        }
        Ok(work + self.poll_state_changes(ports))
    }

    /// Closes the session if the cluster asked, or a new leader did not come
    /// in time (Java's `pollStateChanges`).
    pub fn poll_state_changes(&mut self, ports: &mut impl ClientPorts) -> usize {
        let timed_out = matches!(
            self.state,
            ClientState::AwaitNewLeader | ClientState::AwaitNewLeaderConnection
        ) && ports.now_ns() - self.state_deadline_ns >= 0;
        if self.state == ClientState::PendingClose || timed_out {
            self.close(ports);
            return 1;
        }
        0
    }

    /// Closes the session: the cluster told if still connected, then the
    /// egress subscription and the ingress publication closed.
    pub fn close(&mut self, ports: &mut impl ClientPorts) {
        if self.state == ClientState::Closed {
            return;
        }
        if self.state == ClientState::Connected
            && let Some(publication) = self.publication
            && ports.is_connected(publication)
        {
            self.close_session(ports);
        }
        ports.close_subscription(self.subscription);
        if let Some(publication) = self.publication.take() {
            ports.close_publication(publication);
        }
        if let Some(pending) = self.pending.take()
            && pending.registration_id != NULL_VALUE
        {
            ports.async_remove_publication(pending.registration_id);
        }
        close_all(&mut self.members, ports);
        self.set_state(ClientState::Closed, 0);
    }

    fn close_session(&mut self, ports: &mut impl ClientPorts) {
        let message = codec::session_close_request(self.leadership_term_id, self.cluster_session_id);
        for attempt in 1..=SEND_ATTEMPTS {
            if self.offer_control(ports, &message) > 0 {
                break;
            }
            if attempt < SEND_ATTEMPTS {
                ports.idle();
            }
        }
    }

    fn set_state(&mut self, state: ClientState, deadline_ns: i64) {
        self.state = state;
        self.state_deadline_ns = deadline_ns;
    }

    fn on_disconnected(&mut self, ports: &mut impl ClientPorts) {
        if let Some(publication) = self.publication.take() {
            ports.close_publication(publication);
        }
        if let Some(pending) = self.pending.take()
            && pending.registration_id != NULL_VALUE
        {
            ports.async_remove_publication(pending.registration_id);
        }
        let deadline_ns = ports.now_ns() + self.config.new_leader_timeout_ns;
        self.set_state(ClientState::AwaitNewLeader, deadline_ns);
    }

    fn on_fragment(
        &mut self,
        message: &[u8],
        image: i32,
        now_ns: i64,
        listener: &mut impl ControlledEgressListener,
    ) -> Result<PollAction, ClientError> {
        let egress = codec::decode_egress(message).map_err(ClientError::Cluster)?;
        match egress {
            Egress::Foreign { schema_id } => Err(ClientError::Cluster(format!(
                "expected cluster egress schemaId={} actual={schema_id}",
                codec::SCHEMA_ID
            ))),
            Egress::Message {
                cluster_session_id,
                timestamp,
                payload,
                ..
            } if cluster_session_id == self.cluster_session_id => {
                Ok(listener.on_message(cluster_session_id, timestamp, payload))
            }
            Egress::SessionEvent {
                cluster_session_id,
                correlation_id,
                leadership_term_id,
                leader_member_id,
                code,
                detail,
                ..
            } if cluster_session_id == self.cluster_session_id => {
                if code == EventCode::CLOSED {
                    self.set_state(ClientState::PendingClose, 0);
                }
                listener.on_session_event(
                    correlation_id,
                    cluster_session_id,
                    leadership_term_id,
                    leader_member_id,
                    code,
                    &detail,
                );
                Ok(PollAction::Continue)
            }
            Egress::NewLeader {
                leadership_term_id,
                cluster_session_id,
                leader_member_id,
                ingress_endpoints,
            } if cluster_session_id == self.cluster_session_id => {
                self.egress_image = Some(image);
                self.set_state(
                    ClientState::AwaitNewLeaderConnection,
                    now_ns + self.config.message_timeout_ns,
                );
                self.leadership_term_id = leadership_term_id;
                self.leader_member_id = leader_member_id;
                self.header = codec::session_message_header(leadership_term_id, self.cluster_session_id);
                self.new_leaders.push((leader_member_id, ingress_endpoints.clone()));
                listener.on_new_leader(
                    cluster_session_id,
                    leadership_term_id,
                    leader_member_id,
                    &ingress_endpoints,
                );
                Ok(PollAction::Continue)
            }
            Egress::AdminResponse {
                cluster_session_id,
                correlation_id,
                request_type,
                response_code,
                message,
                payload,
            } if cluster_session_id == self.cluster_session_id => {
                listener.on_admin_response(
                    cluster_session_id,
                    correlation_id,
                    request_type,
                    response_code,
                    &message,
                    payload,
                );
                Ok(PollAction::Continue)
            }
            _ => Ok(PollAction::Continue),
        }
    }

    /// Java's `onNewLeader` after its listener's part: the old publication
    /// closed, the new leader's added, to its endpoint if the client was given
    /// endpoints, else on the ingress channel as it is.
    fn switch_to_new_leader(
        &mut self,
        ports: &mut impl ClientPorts,
        leader_member_id: i32,
        ingress_endpoints: &str,
    ) -> Result<(), ClientError> {
        if let Some(publication) = self.publication.take() {
            ports.close_publication(publication);
        }
        if let Some(pending) = self.pending.take()
            && pending.registration_id != NULL_VALUE
        {
            ports.async_remove_publication(pending.registration_id);
        }
        let channel = if self.config.ingress_endpoints.is_none() {
            self.config.ingress_channel.clone()
        } else {
            self.config.ingress_endpoints = Some(ingress_endpoints.to_string());
            close_all(&mut self.members, ports);
            self.members = members(Some(ingress_endpoints))?;
            let Some(leader) = self.members.get(&leader_member_id) else {
                return Err(ClientError::Cluster(format!(
                    "new leader {leader_member_id} not among {ingress_endpoints}"
                )));
            };
            self.config.member_ingress_channel(&leader.endpoint)
        };
        self.pending = Some(PendingPublication {
            channel,
            registration_id: NULL_VALUE,
            deadline_ns: ports.now_ns() + self.config.message_timeout_ns,
        });
        Ok(())
    }

    /// The new leader's publication, once the driver has it (Java's
    /// `addNewLeaderIngressPublication`, its wait spread across polls).
    fn await_new_leader_publication(&mut self, ports: &mut impl ClientPorts) -> Result<usize, ClientError> {
        let Some(pending) = self.pending.as_mut() else {
            return Ok(0);
        };
        if pending.registration_id == NULL_VALUE {
            pending.registration_id = ports.async_add_publication(
                &pending.channel,
                self.config.ingress_stream_id,
                self.config.is_ingress_exclusive,
            );
        }
        match ports.get_publication(pending.registration_id) {
            Added::Ready(publication) => {
                self.pending = None;
                self.publication = Some(publication);
                if let Some(leader) = self.members.get_mut(&self.leader_member_id) {
                    leader.publication = Some(publication);
                }
                return Ok(1);
            }
            Added::Pending => {}
            Added::TemporarilyUnavailable => pending.registration_id = NULL_VALUE,
            Added::Failed(e) => {
                self.pending = None;
                return Err(ClientError::Registration(e));
            }
        }
        if ports.now_ns() >= pending.deadline_ns {
            let pending = self.pending.take().expect("pending");
            if pending.registration_id != NULL_VALUE {
                ports.async_remove_publication(pending.registration_id);
            }
            return Err(ClientError::Timeout {
                message: format!(
                    "failed to add new leader ingress publication (leaderMemberId={}, leadershipTermId={}, \
                     channel={}, streamId={}) within {}ms",
                    self.leader_member_id,
                    self.leadership_term_id,
                    pending.channel,
                    self.config.ingress_stream_id,
                    self.config.message_timeout_ns / 1_000_000
                ),
                member_errors: Vec::new(),
            });
        }
        Ok(0)
    }
}
