//! Aeron Cluster's client, `AeronCluster` of Aeron 1.54.2, as sans-IO state
//! machines: [`Connect`] opens a session (Java's `AsyncConnect`), and
//! [`Client`] holds it, sending on the leader's ingress and reading the
//! session's egress. They reach Aeron through [`ClientPorts`], which a shell
//! implements on Aeron's C client and a simulator on its network.
//!
//! What they do is Java's, step by step: one deadline for the whole connect;
//! a publication to every member's ingress, the request sent to the first
//! that connects; a redirect followed to the leader, its publication kept if
//! its endpoint is the same; a challenge answered; a new leader's ingress
//! taken up when the cluster says so, and the session closed when the
//! cluster does not within its timeouts. One difference: where Java blocks
//! for the new leader's publication, [`Client`] waits for it across polls,
//! offers meanwhile not connected.

mod aeron_cluster;
mod async_connect;
pub mod codec;
pub mod io;

pub use crate::codecs::cluster::{
    admin_request_type::AdminRequestType, admin_response_code::AdminResponseCode, event_code::EventCode,
};
pub use aeron_cluster::{Client, ClientState, ControlledEgressListener, EgressListener};
pub use async_connect::{Connect, ConnectState};

use std::collections::BTreeMap;

pub use rusteron_archive::fragment_assembler::PollAction;

/// Aeron's null for ids and positions.
pub const NULL_VALUE: i64 = -1;
/// A publication's offer refused for want of a subscriber (`Publication.NOT_CONNECTED`).
pub const NOT_CONNECTED: i64 = -1;
/// Back-pressured (`Publication.BACK_PRESSURED`).
pub const BACK_PRESSURED: i64 = -2;
/// The driver busy with the log's rotation (`Publication.ADMIN_ACTION`).
pub const ADMIN_ACTION: i64 = -3;
/// Closed (`Publication.CLOSED`).
pub const CLOSED: i64 = -4;
/// At its maximum position (`Publication.MAX_POSITION_EXCEEDED`).
pub const MAX_POSITION_EXCEEDED: i64 = -5;
/// Refused for the message itself, too long for instance (Aeron C's
/// `AERON_PUBLICATION_ERROR`, where Java's offer throws): the session stays
/// as it was.
pub const PUBLICATION_ERROR: i64 = -6;
/// Fragments a poll of the egress reads at most (`AeronCluster.FRAGMENT_LIMIT`).
pub const FRAGMENT_LIMIT: usize = 10;
/// Tries of a keepalive, an admin request or a close (`AeronCluster.SEND_ATTEMPTS`).
pub const SEND_ATTEMPTS: usize = 3;
/// The leader heartbeat timeout a session event leaves out, its default
/// (`ConsensusModule.Configuration.LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS`).
pub const LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS: i64 = 10_000_000_000;

/// A client's settings: Java's `AeronCluster.Context`, its defaults Java's.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// Where the client sends: `aeron:udp`, its endpoint each member's from
    /// [`Self::ingress_endpoints`]; or a channel as it is, multicast or IPC.
    pub ingress_channel: String,
    pub ingress_stream_id: i32,
    /// `id=host:port,...`: the members' ingress endpoints, or none.
    pub ingress_endpoints: Option<String>,
    /// Where the cluster answers: a UDP channel, its port 0 for any.
    pub egress_channel: String,
    pub egress_stream_id: i32,
    /// The connect's deadline, and the new leader's.
    pub message_timeout_ns: i64,
    /// How long a client waits for a new leader before it closes; [`NULL_VALUE`]:
    /// twice the leader heartbeat timeout the cluster gives at connect.
    pub new_leader_timeout_ns: i64,
    /// `name=` in the client info the cluster keeps.
    pub client_name: String,
    /// An exclusive publication to the ingress, as Java's default.
    pub is_ingress_exclusive: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            ingress_channel: String::new(),
            ingress_stream_id: 101,
            ingress_endpoints: None,
            egress_channel: String::new(),
            egress_stream_id: 102,
            message_timeout_ns: 5_000_000_000,
            new_leader_timeout_ns: NULL_VALUE,
            client_name: String::new(),
            is_ingress_exclusive: true,
        }
    }
}

/// Java's limit on a client's name (`Aeron.Configuration.MAX_CLIENT_NAME_LENGTH`).
pub const MAX_CLIENT_NAME_LENGTH: usize = 100;

impl ClientConfig {
    /// Java's `Context.conclude` checks, and `rejoin=false` added to a UDP
    /// egress: a session's egress must not rejoin an image it lost.
    pub fn conclude(&mut self) -> Result<(), ClientError> {
        if self.ingress_channel.is_empty() {
            return Err(ClientError::Configuration("ingressChannel must be specified".into()));
        }
        if self.ingress_channel.starts_with("aeron:ipc") && self.ingress_endpoints.is_some() {
            return Err(ClientError::Configuration(
                "AeronCluster.Context ingressEndpoints must be null when using IPC ingress".into(),
            ));
        }
        if self.egress_channel.is_empty() {
            return Err(ClientError::Configuration("egressChannel must be specified".into()));
        }
        if self.egress_channel.starts_with("aeron:udp") && !self.egress_channel.contains("rejoin=") {
            let separator = if self.egress_channel.contains('?') { '|' } else { '?' };
            self.egress_channel = format!("{}{separator}rejoin=false", self.egress_channel);
        }
        if self.client_name.len() > MAX_CLIENT_NAME_LENGTH {
            return Err(ClientError::Configuration(format!(
                "AeronCluster.Context.clientName length must be <= {MAX_CLIENT_NAME_LENGTH}"
            )));
        }
        if let Some(endpoints) = &self.ingress_endpoints {
            parse_ingress_endpoints(endpoints)?;
        }
        Ok(())
    }

    /// The ingress channel to a member: its endpoint set when the channel
    /// is UDP (`ChannelUri.put(ENDPOINT_PARAM_NAME, endpoint)`).
    pub fn member_ingress_channel(&self, endpoint: &str) -> String {
        if !self.ingress_channel.starts_with("aeron:udp") {
            return self.ingress_channel.clone();
        }
        set_endpoint(&self.ingress_channel, endpoint)
    }
}

/// `channel` with its `endpoint` parameter set: replaced where it was, or
/// added last, as `ChannelUri.put` orders its parameters.
fn set_endpoint(channel: &str, endpoint: &str) -> String {
    let (media, params) = channel.split_once('?').unwrap_or((channel, ""));
    let set = format!("endpoint={endpoint}");
    let mut params: Vec<&str> = params.split('|').filter(|p| !p.is_empty()).collect();
    match params.iter().position(|p| p.starts_with("endpoint=")) {
        Some(i) => params[i] = &set,
        None => params.push(&set),
    }
    format!("{media}?{}", params.join("|"))
}

/// `id=host:port,...` by member id (`AeronCluster.parseIngressEndpoints`).
pub fn parse_ingress_endpoints(endpoints: &str) -> Result<BTreeMap<i32, String>, ClientError> {
    let mut by_id = BTreeMap::new();
    for endpoint in endpoints.split(',') {
        let Some((id, address)) = endpoint.split_once('=') else {
            return Err(ClientError::Configuration(format!(
                "endpoint missing '=' separator: {endpoints}"
            )));
        };
        let id = id
            .trim()
            .parse::<i32>()
            .map_err(|_| ClientError::Configuration(format!("member id not a number: {endpoints}")))?;
        by_id.insert(id, address.to_string());
    }
    Ok(by_id)
}

/// Why a client could not connect, or stopped: Java's exceptions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientError {
    /// `TimeoutException`: the connect, or a new leader's publication, took
    /// longer than the message timeout; with each member's error.
    Timeout {
        message: String,
        member_errors: Vec<String>,
    },
    /// `ClusterException`: the cluster refused, or the connection went.
    Cluster(String),
    /// `AuthenticationException`.
    Authentication(String),
    /// `ConfigurationException`.
    Configuration(String),
    /// `RegistrationException`: the driver refused a publication or
    /// subscription for good.
    Registration(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout { message, member_errors } if member_errors.is_empty() => write!(f, "{message}"),
            Self::Timeout { message, member_errors } => write!(f, "{message}; {}", member_errors.join("; ")),
            Self::Cluster(m) | Self::Authentication(m) | Self::Configuration(m) | Self::Registration(m) => {
                write!(f, "{m}")
            }
        }
    }
}

impl std::error::Error for ClientError {}

/// Where an asynchronous add stands (Java's `getPublication`/`getSubscription`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Added<T> {
    Pending,
    Ready(T),
    /// The driver had no room for it now: add it again (`RESOURCE_TEMPORARILY_UNAVAILABLE`).
    TemporarilyUnavailable,
    /// Refused for good.
    Failed(String),
}

/// A publication or subscription a client holds, as the ports name it.
pub type Handle = i64;

/// Aeron, as a client uses it: the shell's on Aeron's C client, or a
/// simulator's on its network.
pub trait ClientPorts {
    fn now_ns(&self) -> i64;
    /// A correlation id unique on the driver.
    fn next_correlation_id(&mut self) -> i64;
    /// Adds a subscription, the registration id returned at once.
    fn async_add_subscription(&mut self, channel: &str, stream_id: i32) -> i64;
    fn get_subscription(&mut self, registration_id: i64) -> Added<Handle>;
    /// Adds a publication, exclusive or not.
    fn async_add_publication(&mut self, channel: &str, stream_id: i32, exclusive: bool) -> i64;
    fn get_publication(&mut self, registration_id: i64) -> Added<Handle>;
    /// Gives up an add not yet ready.
    fn async_remove_publication(&mut self, registration_id: i64);
    fn async_remove_subscription(&mut self, registration_id: i64);
    fn close_publication(&mut self, publication: Handle);
    fn close_subscription(&mut self, subscription: Handle);
    fn is_connected(&self, publication: Handle) -> bool;
    /// The subscription's channel with its port resolved, once it is
    /// (`Subscription.tryResolveChannelEndpointPort`).
    fn resolved_channel(&self, subscription: Handle) -> Option<String>;
    /// Offers the parts as one message: its position, or Aeron's negative result.
    fn offer(&mut self, publication: Handle, parts: &[&[u8]]) -> i64;
    /// Polls whole messages, as a fragment assembler delivers them, each
    /// with the session id of its image; the handler says what to do.
    fn controlled_poll(
        &mut self,
        subscription: Handle,
        limit: usize,
        handler: &mut dyn FnMut(&[u8], i32) -> PollAction,
    ) -> usize;
    /// Whether the image of `session_id` the subscription had is gone.
    fn is_image_closed(&self, subscription: Handle, session_id: i32) -> bool;
    /// Waits a little between tries (the client's idle strategy).
    fn idle(&mut self);
}

/// What a client tells the cluster of itself (Java's `CredentialsSupplier`).
pub trait CredentialsSupplier {
    fn encoded_credentials(&mut self) -> Vec<u8>;
    fn on_challenge(&mut self, encoded_challenge: &[u8]) -> Vec<u8>;
}

/// No credentials (Java's `NullCredentialsSupplier`).
#[derive(Clone, Copy, Debug, Default)]
pub struct NullCredentials;

impl CredentialsSupplier for NullCredentials {
    fn encoded_credentials(&mut self) -> Vec<u8> {
        Vec::new()
    }

    fn on_challenge(&mut self, _encoded_challenge: &[u8]) -> Vec<u8> {
        Vec::new()
    }
}

/// A member's ingress, as the connect and the client keep it (Java's `MemberIngress`).
#[derive(Clone, Debug)]
pub(crate) struct MemberIngress {
    pub(crate) endpoint: String,
    pub(crate) registration_id: i64,
    pub(crate) publication: Option<Handle>,
    pub(crate) error: Option<String>,
}

impl MemberIngress {
    pub(crate) fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            registration_id: NULL_VALUE,
            publication: None,
            error: None,
        }
    }

    pub(crate) fn async_add_publication(&mut self, config: &ClientConfig, ports: &mut impl ClientPorts) {
        let channel = config.member_ingress_channel(&self.endpoint);
        self.registration_id =
            ports.async_add_publication(&channel, config.ingress_stream_id, config.is_ingress_exclusive);
        self.publication = None;
    }

    pub(crate) fn async_get_publication(&mut self, ports: &mut impl ClientPorts) {
        match ports.get_publication(self.registration_id) {
            Added::Pending => {}
            Added::Ready(publication) => {
                self.publication = Some(publication);
                self.registration_id = NULL_VALUE;
            }
            Added::TemporarilyUnavailable => self.registration_id = NULL_VALUE,
            Added::Failed(e) => {
                self.error = Some(e);
                self.registration_id = NULL_VALUE;
            }
        }
    }

    pub(crate) fn close(&mut self, ports: &mut impl ClientPorts) {
        if let Some(publication) = self.publication.take() {
            ports.close_publication(publication);
        } else if self.registration_id != NULL_VALUE {
            ports.async_remove_publication(self.registration_id);
        }
        self.registration_id = NULL_VALUE;
    }
}

pub(crate) fn members(endpoints: Option<&str>) -> Result<BTreeMap<i32, MemberIngress>, ClientError> {
    Ok(match endpoints {
        None => BTreeMap::new(),
        Some(endpoints) => parse_ingress_endpoints(endpoints)?
            .into_iter()
            .map(|(id, endpoint)| (id, MemberIngress::new(endpoint)))
            .collect(),
    })
}

pub(crate) fn close_all(members: &mut BTreeMap<i32, MemberIngress>, ports: &mut impl ClientPorts) {
    for member in members.values_mut() {
        member.close(ports);
    }
}

/// What the client says of itself: Java's `name=... version=... commit=...`.
pub fn client_info(client_name: &str) -> String {
    format!(
        "name={client_name} version=1.54.2 commit=rusteron-cluster-{}",
        env!("CARGO_PKG_VERSION")
    )
}
