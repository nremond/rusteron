//! Aeron Cluster's client on Aeron's C client: Java's `AeronCluster`.
//! [`AeronCluster::connect`] opens a session with the cluster, and the
//! session offers messages to the cluster's services and polls their answers.
//!
//! ```no_run
//! # use rusteron_cluster::client::io::*;
//! # struct Answers;
//! # impl EgressListener for Answers {
//! #     fn on_message(&mut self, _: i64, _: i64, _: &[u8]) {}
//! # }
//! # fn run() -> Result<(), ClientError> {
//! let mut ctx = ClientContext::new("/dev/shm/aeron-client");
//! ctx.config.ingress_channel = "aeron:udp".into();
//! ctx.config.ingress_endpoints = Some("0=node0:9002,1=node1:9002,2=node2:9002".into());
//! ctx.config.egress_channel = "aeron:udp?endpoint=client:0".into();
//! let mut cluster = AeronCluster::connect(ctx)?;
//! while cluster.offer(b"command") < 0 {
//!     cluster.poll_egress(&mut Answers)?;
//! }
//! # Ok(()) }
//! ```
//!
//! The session's logic is the sans-IO [`crate::client`];
//! this module gives it Aeron: publications and subscriptions added
//! asynchronously, messages reassembled per image, images seen to go.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::client::{
    ADMIN_ACTION, Added, BACK_PRESSURED, CLOSED, Client, ClientPorts, Connect, ConnectState, Handle,
    MAX_POSITION_EXCEEDED, NOT_CONNECTED, PUBLICATION_ERROR,
};
pub use crate::client::{
    AdminRequestType, AdminResponseCode, ClientConfig, ClientError, ClientState, ControlledEgressListener,
    CredentialsSupplier, EgressListener, EventCode, NullCredentials, PollAction,
};
use rusteron_archive::fragment_assembler::{Fragment, Reassembler};
use rusteron_archive::*;

/// The driver's refusal of an add for now: Aeron C reports its error code
/// negated (`AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE`).
const RESOURCE_TEMPORARILY_UNAVAILABLE: i32 = -10;

/// How a client waits while it has nothing to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientIdle {
    /// Never waits: lowest latency, a core of its own.
    Spin,
    /// Java's `BackoffIdleStrategy(1, 10, 1 µs, 1 ms)`, `AeronCluster`'s default.
    Backoff,
}

/// Where the client finds its media driver, and its settings.
pub struct ClientContext {
    pub config: ClientConfig,
    /// The driver's directory, for a client of its own.
    pub aeron_dir: String,
    /// The caller's Aeron client instead of one of its own.
    pub aeron: Option<Aeron>,
    /// A client of its own driven from [`AeronCluster::poll_egress`] and the
    /// connect's polls, not from a conductor thread (Java's agent invoker).
    pub use_conductor_agent_invoker: bool,
    pub idle: ClientIdle,
}

impl ClientContext {
    pub fn new(aeron_dir: impl Into<String>) -> Self {
        Self {
            config: ClientConfig::default(),
            aeron_dir: aeron_dir.into(),
            aeron: None,
            use_conductor_agent_invoker: false,
            idle: ClientIdle::Backoff,
        }
    }
}

/// Images that went, per subscription, as the client's callbacks report
/// them. Aeron's client calls them on its conductor thread unless it is
/// invoked from the polls, so the set is behind a lock, not a `RefCell`: a
/// leader lost while the client polled once panicked on a borrow (under
/// fault injection, M5).
#[derive(Clone, Default)]
struct Images(Arc<Mutex<HashSet<(i64, i32)>>>);

impl Images {
    fn set(&self) -> MutexGuard<'_, HashSet<(i64, i32)>> {
        self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn registration_id<S: ResourceStorage<bindings::aeron_subscription_t>>(subscription: &AeronSubscription<S>) -> i64 {
    subscription.get_constants().map(|c| c.registration_id()).unwrap_or(-1)
}

impl AeronAvailableImageCallback for Images {
    fn handle_aeron_on_available_image(&mut self, subscription: &AeronSubscriptionRef, image: &AeronImageRef) {
        if let Ok(c) = image.get_constants() {
            self.set().remove(&(registration_id(subscription), c.session_id()));
        }
    }
}

impl AeronUnavailableImageCallback for Images {
    fn handle_aeron_on_unavailable_image(&mut self, subscription: &AeronSubscriptionRef, image: &AeronImageRef) {
        if let Ok(c) = image.get_constants() {
            self.set().insert((registration_id(subscription), c.session_id()));
        }
    }
}

enum Publication {
    Concurrent(AeronPublication),
    Exclusive(AeronExclusivePublication),
}

enum AddingPublication {
    Concurrent(AeronAsyncAddPublication),
    Exclusive(AeronAsyncAddExclusivePublication),
}

/// Backoff's state: spins, then yields, then parks for longer each time.
#[derive(Default)]
struct Backoff {
    spins: u32,
    yields: u32,
    park_ns: u64,
}

impl Backoff {
    fn idle(&mut self) {
        if self.spins < 1 {
            self.spins += 1;
            std::hint::spin_loop();
        } else if self.yields < 10 {
            self.yields += 1;
            std::thread::yield_now();
        } else {
            self.park_ns = (self.park_ns * 2).clamp(1_000, 1_000_000);
            std::thread::sleep(Duration::from_nanos(self.park_ns));
        }
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Aeron, as the client core uses it.
struct Ports {
    aeron: Aeron,
    invoker: bool,
    epoch: Instant,
    next_id: Handle,
    adding_publications: HashMap<Handle, AddingPublication>,
    publications: HashMap<Handle, Publication>,
    adding_subscriptions: HashMap<Handle, AeronAsyncAddSubscription>,
    subscriptions: HashMap<Handle, AeronSubscription>,
    /// Adds the client refused at once, and why.
    failed: HashMap<Handle, String>,
    reassemblers: HashMap<(Handle, i32), Reassembler>,
    images: Images,
    image_handler: SharedHandler<Images>,
    idle: ClientIdle,
    backoff: Backoff,
}

impl Ports {
    fn new(ctx: &ClientContext) -> Result<Self, ClientError> {
        let aeron_error = |e: AeronCError| ClientError::Cluster(format!("aeron: {e}"));
        let (aeron, invoker) = match &ctx.aeron {
            Some(aeron) => (aeron.clone(), false),
            None => {
                let context = AeronContext::new().map_err(aeron_error)?;
                context
                    .set_dir(&ctx.aeron_dir.as_str().into_c_string())
                    .map_err(aeron_error)?;
                context
                    .set_use_conductor_agent_invoker(ctx.use_conductor_agent_invoker)
                    .map_err(aeron_error)?;
                context
                    .set_client_name(&"cluster-client".into_c_string())
                    .map_err(aeron_error)?;
                let aeron = Aeron::new(&context).map_err(aeron_error)?;
                aeron.start().map_err(aeron_error)?;
                (aeron, ctx.use_conductor_agent_invoker)
            }
        };
        let images = Images::default();
        Ok(Self {
            aeron,
            invoker,
            epoch: Instant::now(),
            next_id: 1,
            adding_publications: HashMap::new(),
            publications: HashMap::new(),
            adding_subscriptions: HashMap::new(),
            subscriptions: HashMap::new(),
            failed: HashMap::new(),
            reassemblers: HashMap::new(),
            image_handler: SharedHandler::new_shared(images.clone()),
            images,
            idle: ctx.idle,
            backoff: Backoff::default(),
        })
    }

    fn invoke(&self) -> usize {
        if self.invoker {
            self.aeron.main_do_work().map(|n| n.max(0) as usize).unwrap_or(0)
        } else {
            0
        }
    }

    fn id(&mut self) -> Handle {
        self.next_id += 1;
        self.next_id
    }

    fn wait(&mut self, work: usize) {
        if work > 0 {
            self.backoff.reset();
            return;
        }
        match self.idle {
            ClientIdle::Spin => std::hint::spin_loop(),
            ClientIdle::Backoff => self.backoff.idle(),
        }
    }

    fn added<T>(&mut self, id: Handle, result: Result<Option<T>, AeronCError>) -> Result<Option<T>, Added<Handle>> {
        match result {
            Ok(added) => Ok(added),
            Err(e) if e.code == RESOURCE_TEMPORARILY_UNAVAILABLE => {
                self.failed.remove(&id);
                Err(Added::TemporarilyUnavailable)
            }
            Err(e) => Err(Added::Failed(e.capture_errmsg().to_string())),
        }
    }
}

fn position(result: Result<i64, AeronOfferError>) -> i64 {
    match result {
        Ok(position) => position,
        Err(AeronOfferError::NotConnected) => NOT_CONNECTED,
        Err(AeronOfferError::BackPressured) => BACK_PRESSURED,
        Err(AeronOfferError::AdminAction) => ADMIN_ACTION,
        Err(AeronOfferError::MaxPositionExceeded) => MAX_POSITION_EXCEEDED,
        Err(AeronOfferError::Closed) => CLOSED,
        // TooManyParts, Error and any later kind (the enum is non-exhaustive).
        Err(_) => PUBLICATION_ERROR,
    }
}

fn fragment<'a>(buffer: &'a [u8], header: &AeronHeader) -> Fragment<'a> {
    let (flags, term_offset) = header
        .get_values()
        .map(|v| (v.frame().flags(), v.frame().term_offset()))
        .unwrap_or((0, -1));
    Fragment {
        payload: buffer,
        flags,
        term_offset,
        position: header.position(),
    }
}

fn controlled(action: PollAction) -> bindings::aeron_controlled_fragment_handler_action_t {
    use bindings::aeron_controlled_fragment_handler_action_en as A;
    match action {
        PollAction::Continue => A::AERON_ACTION_CONTINUE,
        PollAction::Break => A::AERON_ACTION_BREAK,
        PollAction::Abort => A::AERON_ACTION_ABORT,
    }
}

impl ClientPorts for Ports {
    fn now_ns(&self) -> i64 {
        self.epoch.elapsed().as_nanos() as i64
    }

    fn next_correlation_id(&mut self) -> i64 {
        self.aeron.next_correlation_id()
    }

    fn async_add_subscription(&mut self, channel: &str, stream_id: i32) -> i64 {
        let id = self.id();
        match self.aeron.async_add_subscription(
            &channel.into_c_string(),
            stream_id,
            Some(&self.image_handler),
            Some(&self.image_handler),
        ) {
            Ok(adding) => {
                self.adding_subscriptions.insert(id, adding);
            }
            Err(e) => {
                self.failed.insert(id, e.capture_errmsg().to_string());
            }
        }
        id
    }

    fn get_subscription(&mut self, registration_id: i64) -> Added<Handle> {
        if let Some(e) = self.failed.remove(&registration_id) {
            return Added::Failed(e);
        }
        if self.subscriptions.contains_key(&registration_id) {
            return Added::Ready(registration_id);
        }
        let Some(adding) = self.adding_subscriptions.get(&registration_id) else {
            return Added::Failed(format!("no subscription {registration_id}"));
        };
        let polled = adding.poll();
        match self.added(registration_id, polled) {
            Ok(None) => Added::Pending,
            Ok(Some(subscription)) => {
                self.adding_subscriptions.remove(&registration_id);
                self.subscriptions.insert(registration_id, subscription);
                Added::Ready(registration_id)
            }
            Err(not) => {
                self.adding_subscriptions.remove(&registration_id);
                not
            }
        }
    }

    fn async_add_publication(&mut self, channel: &str, stream_id: i32, exclusive: bool) -> i64 {
        let id = self.id();
        let channel = channel.into_c_string();
        let adding = if exclusive {
            self.aeron
                .async_add_exclusive_publication(&channel, stream_id)
                .map(AddingPublication::Exclusive)
        } else {
            self.aeron
                .async_add_publication(&channel, stream_id)
                .map(AddingPublication::Concurrent)
        };
        match adding {
            Ok(adding) => {
                self.adding_publications.insert(id, adding);
            }
            Err(e) => {
                self.failed.insert(id, e.capture_errmsg().to_string());
            }
        }
        id
    }

    fn get_publication(&mut self, registration_id: i64) -> Added<Handle> {
        if let Some(e) = self.failed.remove(&registration_id) {
            return Added::Failed(e);
        }
        if self.publications.contains_key(&registration_id) {
            return Added::Ready(registration_id);
        }
        let polled = match self.adding_publications.get(&registration_id) {
            None => return Added::Failed(format!("no publication {registration_id}")),
            Some(AddingPublication::Concurrent(a)) => a.poll().map(|p| p.map(Publication::Concurrent)),
            Some(AddingPublication::Exclusive(a)) => a.poll().map(|p| p.map(Publication::Exclusive)),
        };
        match self.added(registration_id, polled) {
            Ok(None) => Added::Pending,
            Ok(Some(publication)) => {
                self.adding_publications.remove(&registration_id);
                self.publications.insert(registration_id, publication);
                Added::Ready(registration_id)
            }
            Err(not) => {
                self.adding_publications.remove(&registration_id);
                not
            }
        }
    }

    /// An add polled once more before it is cancelled: Java's client asks
    /// the driver to remove only an add it has not heard fail, where C's
    /// cancel asks regardless and the driver logs an unknown publication.
    fn async_remove_publication(&mut self, registration_id: i64) {
        match self.get_publication(registration_id) {
            Added::Ready(publication) => self.close_publication(publication),
            Added::Pending => match self.adding_publications.remove(&registration_id) {
                Some(AddingPublication::Concurrent(a)) => {
                    let _ = a.cancel();
                }
                Some(AddingPublication::Exclusive(a)) => {
                    let _ = a.cancel();
                }
                None => {}
            },
            Added::Failed(_) | Added::TemporarilyUnavailable => {}
        }
    }

    fn async_remove_subscription(&mut self, registration_id: i64) {
        match self.get_subscription(registration_id) {
            Added::Ready(subscription) => self.close_subscription(subscription),
            Added::Pending => {
                if let Some(a) = self.adding_subscriptions.remove(&registration_id) {
                    let _ = a.cancel();
                }
            }
            Added::Failed(_) | Added::TemporarilyUnavailable => {}
        }
    }

    fn close_publication(&mut self, publication: Handle) {
        match self.publications.remove(&publication) {
            Some(Publication::Concurrent(p)) => {
                let _ = p.close();
            }
            Some(Publication::Exclusive(p)) => {
                let _ = p.close();
            }
            None => {}
        }
    }

    fn close_subscription(&mut self, subscription: Handle) {
        if let Some(s) = self.subscriptions.remove(&subscription) {
            let registration = registration_id(&s);
            self.images.set().retain(|(id, _)| *id != registration);
            let _ = s.close();
        }
        self.reassemblers.retain(|(id, _), _| *id != subscription);
    }

    fn is_connected(&self, publication: Handle) -> bool {
        match self.publications.get(&publication) {
            Some(Publication::Concurrent(p)) => p.is_connected(),
            Some(Publication::Exclusive(p)) => p.is_connected(),
            None => false,
        }
    }

    fn resolved_channel(&self, subscription: Handle) -> Option<String> {
        let s = self.subscriptions.get(&subscription)?;
        s.try_resolve_channel_endpoint_port_as_string(4_096)
            .ok()
            .filter(|c| !c.is_empty() && !c.contains(":0|") && !c.ends_with(":0"))
    }

    fn offer(&mut self, publication: Handle, parts: &[&[u8]]) -> i64 {
        match self.publications.get(&publication) {
            Some(Publication::Concurrent(p)) => position(p.offer_parts(parts)),
            Some(Publication::Exclusive(p)) => position(p.offer_parts(parts)),
            None => CLOSED,
        }
    }

    fn controlled_poll(
        &mut self,
        subscription: Handle,
        limit: usize,
        handler: &mut dyn FnMut(&[u8], i32) -> PollAction,
    ) -> usize {
        let work = self.invoke();
        let Some(s) = self.subscriptions.get(&subscription) else {
            return work;
        };
        let reassemblers = &mut self.reassemblers;
        work + s
            .controlled_poll_fn(
                |buffer, header| {
                    let session_id = header.get_values().map(|v| v.frame().session_id()).unwrap_or(0);
                    let reassembler = reassemblers.entry((subscription, session_id)).or_default();
                    controlled(
                        reassembler.on_fragment(fragment(buffer, header), |message, _| handler(message, session_id)),
                    )
                },
                limit,
            )
            .map(|n| n.max(0) as usize)
            .unwrap_or(0)
    }

    fn is_image_closed(&self, subscription: Handle, session_id: i32) -> bool {
        let Some(s) = self.subscriptions.get(&subscription) else {
            return true;
        };
        self.images.set().contains(&(registration_id(s), session_id))
    }

    fn idle(&mut self) {
        let work = self.invoke();
        self.wait(work);
    }
}

/// A session being opened (Java's `AeronCluster.AsyncConnect`): poll it
/// until it gives the session or fails.
pub struct AsyncConnect<C: CredentialsSupplier = NullCredentials> {
    ports: Option<Ports>,
    connect: Connect<C>,
}

impl AsyncConnect<NullCredentials> {
    pub fn new(ctx: ClientContext) -> Result<Self, ClientError> {
        Self::with_credentials(ctx, NullCredentials)
    }
}

impl<C: CredentialsSupplier> AsyncConnect<C> {
    pub fn with_credentials(ctx: ClientContext, credentials: C) -> Result<Self, ClientError> {
        let ports = Ports::new(&ctx)?;
        let connect = Connect::with_credentials(ctx.config, credentials, ports.now_ns())?;
        Ok(Self {
            ports: Some(ports),
            connect,
        })
    }

    pub fn state(&self) -> ConnectState {
        self.connect.state()
    }

    /// One step: the session once open. On an error, what the connect added is released.
    pub fn poll(&mut self) -> Result<Option<AeronCluster>, ClientError> {
        let ports = self.ports.as_mut().expect("polled after it connected");
        ports.invoke();
        match self.connect.poll(ports) {
            Ok(Some(client)) => {
                let ports = self.ports.take().expect("ports");
                Ok(Some(AeronCluster { ports, client }))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                self.connect.close(ports);
                Err(e)
            }
        }
    }

    fn idle(&mut self, step: ConnectState) {
        if let Some(ports) = self.ports.as_mut() {
            let work = usize::from(self.connect.state() != step);
            ports.wait(work);
        }
    }
}

impl<C: CredentialsSupplier> Drop for AsyncConnect<C> {
    fn drop(&mut self) {
        if let Some(ports) = self.ports.as_mut() {
            self.connect.close(ports);
        }
    }
}

/// An open session with the cluster (Java's `AeronCluster`). Dropping it closes it.
pub struct AeronCluster {
    ports: Ports,
    client: Client,
}

impl AeronCluster {
    /// Opens a session, blocking until the cluster answers or the message
    /// timeout passes.
    pub fn connect(ctx: ClientContext) -> Result<Self, ClientError> {
        Self::connect_with_credentials(ctx, NullCredentials)
    }

    pub fn connect_with_credentials<C: CredentialsSupplier>(
        ctx: ClientContext,
        credentials: C,
    ) -> Result<Self, ClientError> {
        let mut connect = AsyncConnect::with_credentials(ctx, credentials)?;
        loop {
            let step = connect.state();
            if let Some(cluster) = connect.poll()? {
                return Ok(cluster);
            }
            connect.idle(step);
        }
    }

    /// Opens a session without blocking: poll what it returns.
    pub fn async_connect(ctx: ClientContext) -> Result<AsyncConnect, ClientError> {
        AsyncConnect::new(ctx)
    }

    pub fn state(&self) -> ClientState {
        self.client.state()
    }

    pub fn is_closed(&self) -> bool {
        self.client.is_closed()
    }

    pub fn cluster_session_id(&self) -> i64 {
        self.client.cluster_session_id()
    }

    pub fn leadership_term_id(&self) -> i64 {
        self.client.leadership_term_id()
    }

    pub fn leader_member_id(&self) -> i32 {
        self.client.leader_member_id()
    }

    /// The concluded settings, the new leader timeout set.
    pub fn config(&self) -> &ClientConfig {
        self.client.config()
    }

    /// The leader's ingress publication's channel, while there is one.
    pub fn ingress_channel(&self) -> Option<String> {
        let publication = self.client.ingress_publication()?;
        let channel = match self.ports.publications.get(&publication)? {
            Publication::Concurrent(p) => p.get_constants().ok()?.channel().to_string(),
            Publication::Exclusive(p) => p.get_constants().ok()?.channel().to_string(),
        };
        Some(channel)
    }

    /// Whether the leader's ingress publication has a subscriber.
    pub fn is_ingress_connected(&self) -> bool {
        self.client
            .ingress_publication()
            .is_some_and(|p| self.ports.is_connected(p))
    }

    /// A correlation id unique on the driver, for requests the services answer.
    pub fn next_correlation_id(&self) -> i64 {
        self.ports.aeron.next_correlation_id()
    }

    /// Offers a message to the services: Aeron's result, a position or why not.
    pub fn offer(&mut self, message: &[u8]) -> i64 {
        self.client.offer(&mut self.ports, &[message])
    }

    /// Offers the parts as one message, with no copy.
    pub fn offer_parts(&mut self, parts: &[&[u8]]) -> i64 {
        self.client.offer(&mut self.ports, parts)
    }

    pub fn send_keep_alive(&mut self) -> Result<bool, ClientError> {
        self.ports.backoff.reset();
        self.client.send_keep_alive(&mut self.ports)
    }

    pub fn send_admin_request_to_take_a_snapshot(&mut self, correlation_id: i64) -> Result<bool, ClientError> {
        self.ports.backoff.reset();
        self.client
            .send_admin_request_to_take_a_snapshot(&mut self.ports, correlation_id)
    }

    /// Reads the egress, then follows what changed: a new leader, the
    /// egress gone, a timeout, the cluster closing the session.
    pub fn poll_egress(&mut self, listener: &mut impl EgressListener) -> Result<usize, ClientError> {
        self.client.poll_egress(&mut self.ports, listener)
    }

    pub fn controlled_poll_egress(
        &mut self,
        listener: &mut impl ControlledEgressListener,
    ) -> Result<usize, ClientError> {
        self.client.controlled_poll_egress(&mut self.ports, listener)
    }

    pub fn poll_state_changes(&mut self) -> usize {
        self.client.poll_state_changes(&mut self.ports)
    }

    /// Waits as the client's idle strategy says: not at all after work.
    pub fn idle(&mut self, work: usize) {
        self.ports.wait(work);
    }

    /// Closes the session, telling the cluster if still connected.
    pub fn close(&mut self) {
        self.ports.backoff.reset();
        self.client.close(&mut self.ports);
    }
}

impl Drop for AeronCluster {
    fn drop(&mut self) {
        self.close();
    }
}
