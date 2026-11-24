//! Clients' sessions, as Java's `SessionManager` and `ClusterSession` keep
//! them, with the default authenticator (every client accepted, with an
//! empty principal): a connect request on the leader becomes a pending
//! session, which opens once its egress connects, by appending its open
//! to the log; a follower redirects the client to the leader. Sessions time
//! out without activity, close at the client's or a service's request, and
//! are closed by the log on every member. A session closed but not yet
//! committed comes back if an election undoes the close.
//!
//! The clients' egress (Java's `EgressPublisher`) is here too: session
//! events, new leaders, admin responses.
//!
//! Backups and standbys reach a member on its consensus stream, as sessions
//! of id -1 of their own (Java's backup sessions): a backup's query or
//! heartbeat answered once its response channel connects, a standby's notice
//! of its snapshots queued until the member's commit position passes them.

use std::collections::VecDeque;

use crate::cluster_counters::session_counter;
use crate::codecs::cluster::{
    admin_request_type::AdminRequestType, admin_response_code::AdminResponseCode, admin_response_codec,
    close_reason::CloseReason, event_code::EventCode, new_leader_event_codec, session_event_codec,
};

use crate::codecs::message::encode;
use crate::consensus_adapter::{self, StandbySnapshotEntry};
use crate::consensus_module_agent::ports::{Action, EgressId, Offer, Ports};
use crate::log_publisher;

/// The client protocol's version the cluster speaks: Java's
/// `AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION`, 0.3.0.
pub const PROTOCOL_SEMANTIC_VERSION: i32 = 3 << 8;
const PROTOCOL_MAJOR_VERSION: i32 = 0;
pub const SESSION_LIMIT_MSG: &str = "concurrent session limit";
pub const SESSION_INVALID_VERSION_MSG: &str = "invalid client version";
/// How many times a client's egress is offered a message.
const SEND_ATTEMPTS: usize = 3;
/// Aeron's null for ids and positions.
const NULL_VALUE: i64 = -1;

/// A version as Agrona prints it: `major.minor.patch`.
pub fn semantic_version_string(version: i32) -> String {
    format!(
        "{}.{}.{}",
        (version >> 16) & 0xFF,
        (version >> 8) & 0xFF,
        version & 0xFF
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    Init,
    Connecting,
    Connected,
    Challenged,
    Authenticated,
    Rejected,
    Open,
    Closing,
    Invalid,
    Closed,
}

/// Where a session's egress publication is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EgressState {
    None,
    Adding,
    Added,
}

/// What a session is for (Java's `ClusterSession.Action`), with what its
/// request asked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionAction {
    Client,
    /// A backup's query, from the position it names if any.
    Backup {
        log_position: Option<i64>,
    },
    Heartbeat,
    /// A standby's notice of the snapshots it took.
    StandbySnapshot(Vec<StandbySnapshotEntry>),
}

/// What a backup's or standby's session gets once authenticated.
enum Answer {
    /// Its answer sent.
    Done,
    /// Its answer not sent yet.
    Wait,
    /// A standby's notice, to queue.
    Queue(Vec<StandbySnapshotEntry>),
    /// A standby's notice the member does not accept.
    Refuse,
}

impl SessionAction {
    /// Java's name for it.
    fn name(&self) -> &'static str {
        match self {
            SessionAction::Client => "CLIENT",
            SessionAction::Backup { .. } => "BACKUP",
            SessionAction::Heartbeat => "HEARTBEAT",
            SessionAction::StandbySnapshot(_) => "STANDBY_SNAPSHOT",
        }
    }
}

/// A client's session.
#[derive(Clone, Debug)]
pub struct ClusterSession {
    pub id: i64,
    pub egress: EgressId,
    pub response_stream_id: i32,
    pub response_channel: String,
    pub session_info: String,
    pub state: SessionState,
    /// The correlation id of the client's last request, echoed in events.
    pub correlation_id: i64,
    pub opened_log_position: i64,
    pub closed_log_position: i64,
    pub time_of_last_activity_ns: i64,
    pub ingress_image_correlation_id: i64,
    pub close_reason: CloseReason,
    pub encoded_principal: Vec<u8>,
    pub has_open_event_pending: bool,
    pub has_new_leader_event_pending: bool,
    pub action: SessionAction,
    egress_state: EgressState,
    event_code: EventCode,
    response_detail: String,
}

impl ClusterSession {
    fn new(id: i64, egress: EgressId, response_stream_id: i32, response_channel: String, session_info: String) -> Self {
        Self {
            id,
            egress,
            response_stream_id,
            response_channel,
            session_info,
            state: SessionState::Init,
            correlation_id: NULL_VALUE,
            opened_log_position: NULL_VALUE,
            closed_log_position: NULL_VALUE,
            time_of_last_activity_ns: 0,
            ingress_image_correlation_id: NULL_VALUE,
            close_reason: CloseReason::NullVal,
            encoded_principal: Vec::new(),
            has_open_event_pending: true,
            has_new_leader_event_pending: false,
            action: SessionAction::Client,
            egress_state: EgressState::None,
            event_code: EventCode::NullVal,
            response_detail: String::new(),
        }
    }

    pub fn is_open(&self) -> bool {
        self.state == SessionState::Open
    }

    fn closing(&mut self, close_reason: CloseReason) {
        self.close_reason = close_reason;
        self.has_open_event_pending = false;
        self.has_new_leader_event_pending = false;
        self.time_of_last_activity_ns = 0;
        self.state = SessionState::Closing;
    }

    fn reject(&mut self, code: EventCode, detail: String) {
        self.event_code = code;
        self.response_detail = detail;
        self.state = SessionState::Rejected;
    }

    /// Java's `isResponsePublicationConnected`, the publication's add
    /// already reported by the shell.
    fn is_response_connected(&self, ports: &impl Ports) -> bool {
        self.egress_state == EgressState::Added && ports.egress_connected(self.egress)
    }

    fn has_timed_out(&self, now_ns: i64, session_timeout_ns: i64) -> bool {
        self.state != SessionState::Init && self.time_of_last_activity_ns + session_timeout_ns < now_ns
    }
}

/// What the manager needs of the module for an append or an event.
pub struct Term {
    pub leadership_term_id: i64,
    pub leader_member_id: i32,
    /// Cluster time, in the cluster's unit.
    pub timestamp: i64,
    pub now_ns: i64,
}

/// The sessions, as Java's `SessionManager` keeps them.
#[derive(Debug)]
pub struct SessionManager {
    member_id: i32,
    cluster_id: i32,
    session_timeout_ns: i64,
    max_concurrent_sessions: usize,
    leader_heartbeat_timeout_ns: i64,
    /// Open and closing sessions, by id.
    sessions: Vec<ClusterSession>,
    pending_user_sessions: Vec<ClusterSession>,
    rejected_user_sessions: Vec<ClusterSession>,
    redirect_user_sessions: Vec<ClusterSession>,
    pending_backup_sessions: Vec<ClusterSession>,
    rejected_backup_sessions: Vec<ClusterSession>,
    /// Standbys' notices taken, each waiting for the commit position to pass
    /// its snapshots.
    pending_standby_snapshot_notifications: Vec<Vec<StandbySnapshotEntry>>,
    /// Notices past the commit position, each waiting for its deadline.
    delayed_standby_snapshot_notifications: VecDeque<(i64, Vec<StandbySnapshotEntry>)>,
    accept_standby_snapshots: bool,
    standby_snapshot_notification_delay_ns: i64,
    uncommitted_closed_sessions: VecDeque<ClusterSession>,
    /// (log position of an open, the next session id it commits).
    uncommitted_next_committed_session_ids: VecDeque<(i64, i64)>,
    next_session_id: i64,
    next_committed_session_id: i64,
    next_egress_id: u64,
    /// Clients timed out: the shell's counter.
    pub timed_out_count: i64,
    actions: Vec<Action>,
}

impl SessionManager {
    pub fn new(
        member_id: i32,
        cluster_id: i32,
        session_timeout_ns: i64,
        max_concurrent_sessions: usize,
        leader_heartbeat_timeout_ns: i64,
        accept_standby_snapshots: bool,
        standby_snapshot_notification_delay_ns: i64,
    ) -> Self {
        Self {
            member_id,
            cluster_id,
            session_timeout_ns,
            max_concurrent_sessions,
            leader_heartbeat_timeout_ns,
            sessions: Vec::new(),
            pending_user_sessions: Vec::new(),
            rejected_user_sessions: Vec::new(),
            redirect_user_sessions: Vec::new(),
            pending_backup_sessions: Vec::new(),
            rejected_backup_sessions: Vec::new(),
            pending_standby_snapshot_notifications: Vec::new(),
            delayed_standby_snapshot_notifications: VecDeque::new(),
            accept_standby_snapshots,
            standby_snapshot_notification_delay_ns,
            uncommitted_closed_sessions: VecDeque::new(),
            uncommitted_next_committed_session_ids: VecDeque::new(),
            next_session_id: 1,
            next_committed_session_id: 1,
            next_egress_id: 1,
            timed_out_count: 0,
            actions: Vec::new(),
        }
    }

    /// The egress publications to add and remove, for the module to pass on.
    pub fn take_actions(&mut self) -> Vec<Action> {
        std::mem::take(&mut self.actions)
    }

    pub fn sessions(&self) -> &[ClusterSession] {
        &self.sessions
    }

    pub fn find_by_session_id(&mut self, cluster_session_id: i64) -> Option<&mut ClusterSession> {
        self.sessions.iter_mut().find(|s| s.id == cluster_session_id)
    }

    /// The next session id the log has committed an open for: what a
    /// snapshot keeps.
    pub fn next_committed_session_id(&self) -> i64 {
        self.next_committed_session_id
    }

    fn async_connect(&mut self, session: &ClusterSession) {
        let (key, label) = session_counter(self.cluster_id, session.id, &session.session_info);
        self.actions.push(Action::ConnectEgress {
            egress: session.egress,
            channel: session.response_channel.clone(),
            stream_id: session.response_stream_id,
            counter: Some((key, label, session.id)),
        });
    }

    fn disconnect(&mut self, session: &mut ClusterSession) {
        if session.egress_state != EgressState::None {
            session.egress_state = EgressState::None;
            self.actions.push(Action::DisconnectEgress { egress: session.egress });
        }
    }

    fn new_session(
        &mut self,
        id: i64,
        response_stream_id: i32,
        response_channel: String,
        info: String,
    ) -> ClusterSession {
        let egress = EgressId(self.next_egress_id);
        self.next_egress_id += 1;
        let mut session = ClusterSession::new(id, egress, response_stream_id, response_channel, info);
        session.egress_state = EgressState::Adding;
        self.async_connect(&session);
        session
    }

    /// A client's connect request: pending on the leader, rejected for an
    /// incompatible version or past the session limit, redirected on a
    /// follower.
    #[allow(clippy::too_many_arguments)]
    pub fn on_session_connect(
        &mut self,
        correlation_id: i64,
        response_stream_id: i32,
        version: i32,
        response_channel: String,
        client_info: &str,
        image: &IngressImage,
        is_leader: bool,
        ingress_endpoints: &str,
        now_ns: i64,
    ) {
        let id = if is_leader {
            let id = self.next_session_id;
            self.next_session_id += 1;
            id
        } else {
            NULL_VALUE
        };
        let image_info = format!(
            "sourceIdentity={} sessionId={}",
            image.source_identity, image.session_id
        );
        let info = if client_info.is_empty() {
            image_info
        } else {
            format!("{client_info} {image_info}")
        };
        let mut session = self.new_session(id, response_stream_id, response_channel, info);
        session.time_of_last_activity_ns = now_ns;
        session.correlation_id = correlation_id;
        if !is_leader {
            session.event_code = EventCode::REDIRECT;
            session.response_detail = ingress_endpoints.to_string();
            self.redirect_user_sessions.push(session);
        } else if (version >> 16) & 0xFF != PROTOCOL_MAJOR_VERSION {
            let detail = format!(
                "{SESSION_INVALID_VERSION_MSG} {}, cluster is {}",
                semantic_version_string(version),
                semantic_version_string(PROTOCOL_SEMANTIC_VERSION)
            );
            self.reject(&mut session, EventCode::ERROR, detail);
            self.rejected_user_sessions.push(session);
        } else if self.pending_user_sessions.len() + self.sessions.len() >= self.max_concurrent_sessions {
            self.reject(&mut session, EventCode::ERROR, SESSION_LIMIT_MSG.into());
            self.rejected_user_sessions.push(session);
        } else {
            session.ingress_image_correlation_id = image.correlation_id;
            self.pending_user_sessions.push(session);
        }
    }

    /// Rejects a session, recorded as Java's `ClusterSession.reject` records it.
    fn reject(&mut self, session: &mut ClusterSession, code: EventCode, detail: String) {
        self.actions.push(Action::Error {
            warn: true,
            message: format!(
                "{code:?} {detail}, clusterMemberId={}, id={}",
                self.member_id, session.id
            ),
        });
        session.reject(code, detail);
    }

    /// A backup's query or heartbeat, or a standby's notice (Java's
    /// `onBackupAction`): a session of its own back to its response channel,
    /// pending, or rejected for a protocol it does not speak. `source` is the
    /// consensus image's, as Java names the session.
    #[allow(clippy::too_many_arguments)]
    pub fn on_backup_action(
        &mut self,
        action: SessionAction,
        correlation_id: i64,
        response_stream_id: i32,
        version: i32,
        response_channel: String,
        source: &str,
        now_ns: i64,
    ) {
        let info = format!("{} {source}", action.name());
        let mut session = self.new_session(NULL_VALUE, response_stream_id, response_channel, info);
        session.action = action;
        session.time_of_last_activity_ns = now_ns;
        session.correlation_id = correlation_id;
        if (version >> 16) & 0xFF == PROTOCOL_MAJOR_VERSION {
            self.pending_backup_sessions.push(session);
        } else {
            let detail = format!(
                "{SESSION_INVALID_VERSION_MSG} {}, cluster={}",
                semantic_version_string(version),
                semantic_version_string(PROTOCOL_SEMANTIC_VERSION)
            );
            self.reject(&mut session, EventCode::ERROR, detail);
            self.rejected_backup_sessions.push(session);
        }
    }

    /// Backups' and standbys' sessions, as Java's `processPendingSessions`
    /// takes them: closed once invalid or timed out, authenticated once their
    /// response channel connects (the default authenticator), then answered
    /// and closed. A backup gets `backup_response`'s answer to its correlation
    /// id and position, none while the log has no term; a standby's notice is
    /// queued, unless the member does not accept standby snapshots. The
    /// default authorisation allows all three.
    pub fn process_pending_backup_sessions(
        &mut self,
        now_ns: i64,
        ports: &mut impl Ports,
        mut backup_response: impl FnMut(i64, Option<i64>) -> Option<Vec<u8>>,
    ) -> usize {
        let mut work = 0;
        let mut i = self.pending_backup_sessions.len();
        while i > 0 {
            i -= 1;
            let state = self.pending_backup_sessions[i].state;
            if state == SessionState::Invalid {
                let mut session = self.pending_backup_sessions.swap_remove(i);
                self.close(&mut session);
                continue;
            }
            let session = &self.pending_backup_sessions[i];
            if now_ns > session.time_of_last_activity_ns + self.session_timeout_ns && state != SessionState::Init {
                let mut session = self.pending_backup_sessions.swap_remove(i);
                self.close(&mut session);
                self.timed_out_count += 1;
                continue;
            }
            let session = &mut self.pending_backup_sessions[i];
            if matches!(
                session.state,
                SessionState::Init | SessionState::Connecting | SessionState::Connected
            ) && session.is_response_connected(ports)
            {
                session.state = SessionState::Authenticated;
            }
            let state = session.state;
            match state {
                SessionState::Authenticated => {
                    let session = &self.pending_backup_sessions[i];
                    let answer = match &session.action {
                        SessionAction::Backup { log_position } => {
                            let sent = backup_response(session.correlation_id, *log_position)
                                .is_some_and(|response| offer_egress(ports, session.egress, &response));
                            if sent { Answer::Done } else { Answer::Wait }
                        }
                        SessionAction::Heartbeat => {
                            let response = consensus_adapter::heartbeat_response(session.correlation_id);
                            if offer_egress(ports, session.egress, &response) {
                                Answer::Done
                            } else {
                                Answer::Wait
                            }
                        }
                        SessionAction::StandbySnapshot(_) if !self.accept_standby_snapshots => Answer::Refuse,
                        SessionAction::StandbySnapshot(entries) => Answer::Queue(entries.clone()),
                        SessionAction::Client => Answer::Wait,
                    };
                    match answer {
                        Answer::Wait => {}
                        Answer::Refuse => {
                            let mut session = self.pending_backup_sessions.swap_remove(i);
                            let detail = "Not configured to accept StandbySnapshot".to_string();
                            self.reject(&mut session, EventCode::ERROR, detail);
                            self.rejected_backup_sessions.push(session);
                        }
                        done => {
                            if let Answer::Queue(entries) = done {
                                self.pending_standby_snapshot_notifications.push(entries);
                            }
                            let mut session = self.pending_backup_sessions.swap_remove(i);
                            self.close(&mut session);
                            work += 1;
                        }
                    }
                }
                SessionState::Rejected => {
                    let session = self.pending_backup_sessions.swap_remove(i);
                    self.rejected_backup_sessions.push(session);
                }
                _ => {}
            }
        }
        work
    }

    /// Standbys' notices whose snapshots the commit position has passed and
    /// whose delay is over, for the recording log (Java's
    /// `processPendingStandbySnapshotNotifications`).
    pub fn due_standby_snapshot_notifications(
        &mut self,
        commit_position: i64,
        now_ns: i64,
    ) -> Vec<Vec<StandbySnapshotEntry>> {
        let mut due = Vec::new();
        let delay_ns = self.standby_snapshot_notification_delay_ns;
        let mut waiting = Vec::new();
        for entries in std::mem::take(&mut self.pending_standby_snapshot_notifications) {
            match entries.first() {
                None => {}
                Some(first) if first.log_position > commit_position => waiting.push(entries),
                Some(_) if delay_ns > 0 => self
                    .delayed_standby_snapshot_notifications
                    .push_back((now_ns + delay_ns, entries)),
                Some(_) => due.push(entries),
            }
        }
        self.pending_standby_snapshot_notifications = waiting;
        while self
            .delayed_standby_snapshot_notifications
            .front()
            .is_some_and(|(deadline_ns, _)| now_ns >= *deadline_ns)
        {
            let (_, entries) = self.delayed_standby_snapshot_notifications.pop_front().expect("one");
            due.push(entries);
        }
        due
    }

    /// The shell added a session's egress publication, or failed to.
    pub fn on_egress_added(&mut self, egress: EgressId, result: &Result<(), String>, now_ns: i64) {
        let lists = [
            &mut self.sessions,
            &mut self.pending_user_sessions,
            &mut self.rejected_user_sessions,
            &mut self.redirect_user_sessions,
            &mut self.pending_backup_sessions,
            &mut self.rejected_backup_sessions,
        ];
        for list in lists {
            if let Some(session) = list.iter_mut().find(|s| s.egress == egress) {
                if session.egress_state != EgressState::Adding {
                    return;
                }
                match result {
                    Ok(()) => {
                        session.egress_state = EgressState::Added;
                        session.time_of_last_activity_ns = now_ns;
                        if matches!(session.state, SessionState::Init) {
                            session.state = SessionState::Connecting;
                        }
                    }
                    Err(_) => {
                        session.egress_state = EgressState::None;
                        if !matches!(session.state, SessionState::Open | SessionState::Closing) {
                            session.state = SessionState::Invalid;
                        }
                    }
                }
                return;
            }
        }
    }

    /// The client closes its session: its close appended to the log.
    pub fn on_session_close(
        &mut self,
        cluster_session_id: i64,
        term: &Term,
        ports: &mut impl Ports,
    ) -> Result<(), String> {
        let Some(index) = self
            .sessions
            .iter()
            .position(|s| s.id == cluster_session_id && s.is_open())
        else {
            return Ok(());
        };
        let mut session = self.sessions[index].clone();
        session.closing(CloseReason::CLIENT_ACTION);
        self.disconnect(&mut session);
        self.sessions[index] = session.clone();
        if append_session_close(ports, &session, term)? {
            self.sessions.remove(index);
            session.closed_log_position = ports.log_position();
            session.state = SessionState::Closed;
            self.uncommitted_closed_sessions.push_back(session);
        }
        Ok(())
    }

    /// A service closes a session: on the active leader, its close appended
    /// and the client told.
    pub fn on_service_close_session(
        &mut self,
        cluster_session_id: i64,
        is_active_leader: bool,
        term: &Term,
        ports: &mut impl Ports,
    ) -> Result<(), String> {
        let Some(index) = self.sessions.iter().position(|s| s.id == cluster_session_id) else {
            return Ok(());
        };
        self.sessions[index].closing(CloseReason::SERVICE_ACTION);
        if is_active_leader {
            let session = self.sessions[index].clone();
            if append_session_close(ports, &session, term)? {
                send_event(
                    ports,
                    &session,
                    term.leadership_term_id,
                    self.member_id,
                    EventCode::CLOSED,
                    "SERVICE_ACTION",
                    self.leader_heartbeat_timeout_ns,
                );
                let mut session = self.sessions.remove(index);
                session.closed_log_position = ports.log_position();
                self.close(&mut session);
                self.uncommitted_closed_sessions.push_back(session);
            }
        }
        Ok(())
    }

    fn close(&mut self, session: &mut ClusterSession) {
        self.disconnect(session);
        session.state = SessionState::Closed;
    }

    /// A session opened in the log: replayed, or followed.
    pub fn on_replay_session_open(
        &mut self,
        log_position: i64,
        correlation_id: i64,
        cluster_session_id: i64,
        time_ns: i64,
        response_stream_id: i32,
        response_channel: String,
    ) {
        let egress = EgressId(self.next_egress_id);
        self.next_egress_id += 1;
        let info = response_channel.clone();
        let mut session = ClusterSession::new(cluster_session_id, egress, response_stream_id, response_channel, info);
        session.opened_log_position = log_position;
        session.state = SessionState::Open;
        session.time_of_last_activity_ns = time_ns;
        session.correlation_id = correlation_id;
        self.add_session(session);
        if cluster_session_id >= self.next_session_id {
            self.next_session_id = cluster_session_id + 1;
        }
        if cluster_session_id >= self.next_committed_session_id {
            self.next_committed_session_id = cluster_session_id + 1;
        }
    }

    /// A session closed in the log.
    pub fn on_replay_session_close(&mut self, cluster_session_id: i64, close_reason: CloseReason) {
        if let Some(index) = self.sessions.iter().position(|s| s.id == cluster_session_id) {
            let mut session = self.sessions.remove(index);
            session.close_reason = close_reason;
            self.close(&mut session);
        }
    }

    /// A session from a snapshot.
    #[allow(clippy::too_many_arguments)]
    pub fn on_load_cluster_session(
        &mut self,
        cluster_session_id: i64,
        correlation_id: i64,
        opened_position: i64,
        time_of_last_activity: i64,
        close_reason: CloseReason,
        response_stream_id: i32,
        response_channel: String,
    ) {
        let egress = EgressId(self.next_egress_id);
        self.next_egress_id += 1;
        let info = response_channel.clone();
        let mut session = ClusterSession::new(cluster_session_id, egress, response_stream_id, response_channel, info);
        session.opened_log_position = opened_position;
        session.time_of_last_activity_ns = time_of_last_activity;
        session.correlation_id = correlation_id;
        session.close_reason = close_reason;
        session.state = if close_reason != CloseReason::NullVal {
            SessionState::Closing
        } else {
            SessionState::Open
        };
        self.add_session(session);
        if cluster_session_id >= self.next_session_id {
            self.next_session_id = cluster_session_id + 1;
            self.next_committed_session_id = self.next_session_id;
        }
    }

    pub fn load_next_session_id(&mut self, next_session_id: i64) {
        self.next_session_id = next_session_id;
        self.next_committed_session_id = next_session_id;
    }

    /// The leader's pending sessions: timed out, connected and accepted,
    /// then opened by appending to the log; rejected ones set aside.
    pub fn process_pending_sessions(&mut self, term: &Term, ports: &mut impl Ports) -> Result<usize, String> {
        let mut work = 0;
        let mut i = self.pending_user_sessions.len();
        while i > 0 {
            i -= 1;
            let now_ns = term.now_ns;
            let state = self.pending_user_sessions[i].state;
            if state == SessionState::Invalid {
                let mut session = self.pending_user_sessions.swap_remove(i);
                self.close(&mut session);
                continue;
            }
            let session = &self.pending_user_sessions[i];
            if now_ns > session.time_of_last_activity_ns + self.session_timeout_ns && state != SessionState::Init {
                let mut session = self.pending_user_sessions.swap_remove(i);
                self.close(&mut session);
                self.timed_out_count += 1;
                continue;
            }
            let session = &mut self.pending_user_sessions[i];
            if matches!(
                session.state,
                SessionState::Init | SessionState::Connecting | SessionState::Connected
            ) && session.is_response_connected(ports)
            {
                // The default authenticator accepts every client, with an empty principal.
                session.state = SessionState::Authenticated;
            }
            if session.state == SessionState::Authenticated {
                if ports.egress_available_window(session.egress) > 0 {
                    let message = log_publisher::session_open(
                        term.leadership_term_id,
                        session.id,
                        session.correlation_id,
                        term.timestamp,
                        session.response_stream_id,
                        &session.response_channel,
                        &session.encoded_principal,
                    );
                    if let Some(position) = log_publisher::append(ports, &[&message])? {
                        session.opened_log_position = position;
                        session.state = SessionState::Open;
                        session.time_of_last_activity_ns = now_ns;
                        let session = &mut self.pending_user_sessions[i];
                        if send_event(
                            ports,
                            session,
                            term.leadership_term_id,
                            self.member_id,
                            EventCode::OK,
                            "",
                            self.leader_heartbeat_timeout_ns,
                        ) {
                            session.has_open_event_pending = false;
                        }
                        let session = self.pending_user_sessions.swap_remove(i);
                        self.uncommitted_next_committed_session_ids
                            .push_back((ports.log_position(), session.id + 1));
                        self.add_session(session);
                        work += 1;
                    }
                }
            } else if session.state == SessionState::Rejected {
                let session = self.pending_user_sessions.swap_remove(i);
                self.rejected_user_sessions.push(session);
            }
        }
        Ok(work)
    }

    /// Open sessions timed out or closing get their close appended and the
    /// client told; pending open and new-leader events are sent.
    pub fn check_sessions(
        &mut self,
        term: &Term,
        ingress_endpoints: &str,
        ports: &mut impl Ports,
    ) -> Result<usize, String> {
        let mut work = 0;
        let mut i = self.sessions.len();
        while i > 0 {
            i -= 1;
            let session = &self.sessions[i];
            if term.now_ns > session.time_of_last_activity_ns + self.session_timeout_ns {
                match session.state {
                    SessionState::Open | SessionState::Closing => {
                        let was_open = session.state == SessionState::Open;
                        if was_open {
                            self.sessions[i].closing(CloseReason::TIMEOUT);
                        }
                        let session = self.sessions[i].clone();
                        if append_session_close(ports, &session, term)? {
                            send_event(
                                ports,
                                &session,
                                term.leadership_term_id,
                                self.member_id,
                                EventCode::CLOSED,
                                close_reason_name(session.close_reason),
                                self.leader_heartbeat_timeout_ns,
                            );
                            let mut session = self.sessions.remove(i);
                            session.closed_log_position = ports.log_position();
                            if was_open || session.close_reason == CloseReason::TIMEOUT {
                                self.timed_out_count += 1;
                            }
                            self.close(&mut session);
                            self.uncommitted_closed_sessions.push_back(session);
                            if !was_open {
                                work += 1;
                            }
                        }
                        if was_open {
                            work += 1;
                        }
                    }
                    _ => {
                        let mut session = self.sessions.remove(i);
                        self.close(&mut session);
                        work += 1;
                    }
                }
            } else if session.has_open_event_pending {
                let session = &self.sessions[i];
                if send_event(
                    ports,
                    session,
                    term.leadership_term_id,
                    self.member_id,
                    EventCode::OK,
                    "",
                    self.leader_heartbeat_timeout_ns,
                ) {
                    self.sessions[i].has_open_event_pending = false;
                    work += 1;
                }
            } else if session.has_new_leader_event_pending {
                let session = &self.sessions[i];
                if session.egress_state == EgressState::Added
                    && send_new_leader(
                        ports,
                        session,
                        term.leadership_term_id,
                        term.leader_member_id,
                        ingress_endpoints,
                    )
                {
                    self.sessions[i].has_new_leader_event_pending = false;
                    work += 1;
                }
            }
        }
        Ok(work)
    }

    /// A new term's sessions: at startup the open ones closing, as their
    /// clients reconnect; after an election their egress connected again
    /// and a new leader event pending.
    pub fn prepare_sessions_for_new_term(&mut self, is_startup: bool, now_ns: i64) {
        if is_startup {
            for session in self.sessions.iter_mut().filter(|s| s.state == SessionState::Open) {
                session.closing(CloseReason::TIMEOUT);
            }
            return;
        }
        let open: Vec<usize> = (0..self.sessions.len())
            .filter(|&i| self.sessions[i].state == SessionState::Open)
            .collect();
        for i in open {
            let mut session = self.sessions[i].clone();
            if session.egress_state == EgressState::None {
                session.egress_state = EgressState::Adding;
                self.async_connect(&session);
            }
            session.time_of_last_activity_ns = now_ns;
            session.has_new_leader_event_pending = true;
            self.sessions[i] = session;
        }
    }

    /// Redirects sent to clients of a follower, once their egress connects.
    pub fn send_redirects(&mut self, term: &Term, ports: &mut impl Ports) -> usize {
        let mut sessions = std::mem::take(&mut self.redirect_user_sessions);
        let work = self.send_for_all(&mut sessions, term, ports);
        self.redirect_user_sessions = sessions;
        work
    }

    /// Rejections sent, once the client's egress connects.
    pub fn send_rejections(&mut self, term: &Term, ports: &mut impl Ports) -> usize {
        let mut sessions = std::mem::take(&mut self.rejected_user_sessions);
        let mut work = self.send_for_all(&mut sessions, term, ports);
        self.rejected_user_sessions = sessions;
        let mut sessions = std::mem::take(&mut self.rejected_backup_sessions);
        work += self.send_for_all(&mut sessions, term, ports);
        self.rejected_backup_sessions = sessions;
        work
    }

    fn send_for_all(&mut self, sessions: &mut Vec<ClusterSession>, term: &Term, ports: &mut impl Ports) -> usize {
        let mut work = 0;
        let mut i = sessions.len();
        while i > 0 {
            i -= 1;
            let session = &sessions[i];
            let detail = session.response_detail.clone();
            let sent = session.is_response_connected(ports)
                && send_event(
                    ports,
                    session,
                    term.leadership_term_id,
                    term.leader_member_id,
                    session.event_code,
                    &detail,
                    self.leader_heartbeat_timeout_ns,
                );
            if sent
                || session.has_timed_out(term.now_ns, self.session_timeout_ns)
                || session.state == SessionState::Invalid
            {
                let mut session = sessions.swap_remove(i);
                self.close(&mut session);
                work += 1;
            }
        }
        work
    }

    /// After an election, sessions the log had opened past `log_position`
    /// are gone, and pending ones too: their clients told.
    pub fn clear_sessions_after(&mut self, log_position: i64, leadership_term_id: i64, ports: &mut impl Ports) {
        let mut i = self.sessions.len();
        while i > 0 {
            i -= 1;
            if self.sessions[i].opened_log_position > log_position {
                let mut session = self.sessions.remove(i);
                send_event(
                    ports,
                    &session,
                    leadership_term_id,
                    self.member_id,
                    EventCode::CLOSED,
                    "election",
                    self.leader_heartbeat_timeout_ns,
                );
                self.close(&mut session);
            }
        }
        for mut session in std::mem::take(&mut self.pending_user_sessions) {
            send_event(
                ports,
                &session,
                leadership_term_id,
                self.member_id,
                EventCode::CLOSED,
                "election",
                self.leader_heartbeat_timeout_ns,
            );
            self.close(&mut session);
        }
    }

    /// Every session's egress closed and its ingress image forgotten as a
    /// new term begins: connected again once a leader takes them on (Java's
    /// `disconnectSessions`).
    pub fn disconnect_sessions(&mut self) {
        for i in 0..self.sessions.len() {
            let mut session = self.sessions[i].clone();
            session.ingress_image_correlation_id = NULL_VALUE;
            self.disconnect(&mut session);
            self.sessions[i] = session;
        }
    }

    /// Every session's egress closed: the node stops.
    pub fn close_sessions(&mut self) {
        for mut session in std::mem::take(&mut self.sessions) {
            self.close(&mut session);
        }
    }

    /// The closes and opens the log committed are final.
    pub fn sweep_uncommitted_sessions(&mut self, commit_position: i64) {
        while self
            .uncommitted_closed_sessions
            .front()
            .is_some_and(|s| s.closed_log_position <= commit_position)
        {
            self.uncommitted_closed_sessions.pop_front();
        }
        while let Some(&(position, next)) = self.uncommitted_next_committed_session_ids.front() {
            if position > commit_position {
                break;
            }
            self.uncommitted_next_committed_session_ids.pop_front();
            self.next_committed_session_id = self.next_committed_session_id.max(next);
        }
    }

    /// An election undid the closes the log had not committed: those
    /// sessions are closing again.
    pub fn restore_uncommitted_sessions(&mut self, commit_position: i64) {
        while let Some(mut session) = self.uncommitted_closed_sessions.pop_front() {
            if session.closed_log_position > commit_position {
                session.closed_log_position = NULL_VALUE;
                session.state = SessionState::Closing;
                self.add_session(session);
            }
        }
        while let Some(&(position, next)) = self.uncommitted_next_committed_session_ids.front() {
            if position > commit_position {
                break;
            }
            self.uncommitted_next_committed_session_ids.pop_front();
            self.next_committed_session_id = self.next_committed_session_id.max(next);
        }
        self.uncommitted_next_committed_session_ids.clear();
    }

    pub fn update_time_of_last_activity(&mut self, now_ns: i64) {
        for session in &mut self.sessions {
            session.time_of_last_activity_ns = now_ns;
        }
    }

    /// What a snapshot keeps: open and closing sessions, by id.
    pub fn snapshot_sessions(&self) -> impl Iterator<Item = &ClusterSession> {
        self.sessions
            .iter()
            .filter(|s| matches!(s.state, SessionState::Open | SessionState::Closing))
    }

    /// The ingress image of sessions went: they time out.
    pub fn timeout_on_unavailable_image(&mut self, image_correlation_id: i64) {
        for session in &mut self.sessions {
            if session.ingress_image_correlation_id == image_correlation_id && session.is_open() {
                session.closing(CloseReason::TIMEOUT);
            }
        }
    }

    /// A client's keep-alive: its session active, linked to its image.
    pub fn on_session_keep_alive(&mut self, cluster_session_id: i64, image_correlation_id: i64, now_ns: i64) {
        if let Some(session) = self.find_by_session_id(cluster_session_id)
            && session.state == SessionState::Open
        {
            if session.ingress_image_correlation_id == NULL_VALUE {
                session.ingress_image_correlation_id = image_correlation_id;
            }
            session.time_of_last_activity_ns = now_ns;
        }
    }

    /// Answers an admin request as Java's default authorisation does: the
    /// cluster's admin requests are not authorised.
    pub fn on_admin_request(
        &mut self,
        cluster_session_id: i64,
        correlation_id: i64,
        request_type: AdminRequestType,
        ports: &mut impl Ports,
    ) {
        if let Some(session) = self.sessions.iter().find(|s| s.id == cluster_session_id && s.is_open()) {
            let message = format!("Execution of the {request_type:?} request was not authorised");
            send_admin_response(
                ports,
                session,
                correlation_id,
                request_type,
                AdminResponseCode::UNAUTHORISED_ACCESS,
                &message,
            );
        }
    }

    fn add_session(&mut self, session: ClusterSession) {
        let index = self.sessions.partition_point(|s| s.id < session.id);
        self.sessions.insert(index, session);
    }
}

/// The client's image a request came on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngressImage {
    pub correlation_id: i64,
    pub session_id: i32,
    pub source_identity: String,
}

fn close_reason_name(reason: CloseReason) -> &'static str {
    match reason {
        CloseReason::CLIENT_ACTION => "CLIENT_ACTION",
        CloseReason::SERVICE_ACTION => "SERVICE_ACTION",
        CloseReason::TIMEOUT => "TIMEOUT",
        CloseReason::NullVal => "NULL_VAL",
    }
}

fn append_session_close(ports: &mut impl Ports, session: &ClusterSession, term: &Term) -> Result<bool, String> {
    let message = log_publisher::session_close(
        term.leadership_term_id,
        session.id,
        term.timestamp,
        session.close_reason,
    );
    Ok(log_publisher::append(ports, &[&message])?.is_some())
}

fn offer_egress(ports: &mut impl Ports, egress: EgressId, message: &[u8]) -> bool {
    (0..SEND_ATTEMPTS).any(|_| ports.offer_egress(egress, message) == Offer::Sent)
}

/// A session event to the client, as Java's `EgressPublisher.sendEvent`.
pub fn send_event(
    ports: &mut impl Ports,
    session: &ClusterSession,
    leadership_term_id: i64,
    leader_member_id: i32,
    code: EventCode,
    detail: &str,
    leader_heartbeat_timeout_ns: i64,
) -> bool {
    if session.egress_state != EgressState::Added {
        return false;
    }
    let message = session_event(
        session.id,
        session.correlation_id,
        leadership_term_id,
        leader_member_id,
        code,
        detail,
        leader_heartbeat_timeout_ns,
    );
    offer_egress(ports, session.egress, &message)
}

#[allow(clippy::too_many_arguments)]
pub fn session_event(
    cluster_session_id: i64,
    correlation_id: i64,
    leadership_term_id: i64,
    leader_member_id: i32,
    code: EventCode,
    detail: &str,
    leader_heartbeat_timeout_ns: i64,
) -> Vec<u8> {
    use session_event_codec as c;
    encode!(c::SessionEventEncoder, 4 + detail.len(), |e| {
        e.cluster_session_id(cluster_session_id)
            .correlation_id(correlation_id)
            .leadership_term_id(leadership_term_id)
            .leader_member_id(leader_member_id)
            .code(code)
            .version(PROTOCOL_SEMANTIC_VERSION)
            .leader_heartbeat_timeout_ns(leader_heartbeat_timeout_ns)
            .detail(detail.as_bytes());
    })
}

fn send_new_leader(
    ports: &mut impl Ports,
    session: &ClusterSession,
    leadership_term_id: i64,
    leader_member_id: i32,
    ingress_endpoints: &str,
) -> bool {
    use new_leader_event_codec as c;
    let message = encode!(c::NewLeaderEventEncoder, 4 + ingress_endpoints.len(), |e| {
        e.leadership_term_id(leadership_term_id)
            .cluster_session_id(session.id)
            .leader_member_id(leader_member_id)
            .ingress_endpoints(ingress_endpoints.as_bytes());
    });
    offer_egress(ports, session.egress, &message)
}

fn send_admin_response(
    ports: &mut impl Ports,
    session: &ClusterSession,
    correlation_id: i64,
    request_type: AdminRequestType,
    code: AdminResponseCode,
    message: &str,
) -> bool {
    if session.egress_state != EgressState::Added {
        return false;
    }
    use admin_response_codec as c;
    let bytes = encode!(c::AdminResponseEncoder, 8 + message.len(), |e| {
        e.cluster_session_id(session.id)
            .correlation_id(correlation_id)
            .request_type(request_type)
            .response_code(code)
            .message(message.as_bytes())
            .payload(&[]);
    });
    offer_egress(ports, session.egress, &bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus_module_agent::ports::CounterId;
    use std::collections::{HashMap, HashSet};

    /// A log that takes everything, egress publications connected once added.
    #[derive(Default)]
    struct FakePorts {
        log: Vec<Vec<u8>>,
        position: i64,
        connected: HashSet<EgressId>,
        egress: HashMap<EgressId, Vec<Vec<u8>>>,
    }

    impl Ports for FakePorts {
        fn log_position(&self) -> i64 {
            self.position
        }
        fn offer_log(&mut self, parts: &[&[u8]]) -> i64 {
            let message = parts.concat();
            self.position = log_publisher::position_after(self.position, message.len());
            self.log.push(message);
            self.position
        }
        fn egress_connected(&self, egress: EgressId) -> bool {
            self.connected.contains(&egress)
        }
        fn egress_available_window(&self, _: EgressId) -> i64 {
            65_536
        }
        fn offer_egress(&mut self, egress: EgressId, message: &[u8]) -> Offer {
            self.egress.entry(egress).or_default().push(message.to_vec());
            Offer::Sent
        }
        fn offer_consensus(&mut self, _: i32, _: &[u8]) -> Offer {
            Offer::Sent
        }
        fn write_node_state(&mut self, _: u64, _: &[u8]) -> Result<(), String> {
            Ok(())
        }
        fn offer_service(&mut self, _: &[u8]) -> Offer {
            Offer::Sent
        }
        fn offer_archive(&mut self, _: &[u8]) -> Offer {
            Offer::Sent
        }
        fn archive_publication_connected(&self) -> bool {
            true
        }
        fn archive_subscription_connected(&self) -> bool {
            true
        }
        fn archive_response_channel(&self) -> Option<String> {
            None
        }
        fn snapshot_position(&self) -> i64 {
            0
        }
        fn offer_snapshot(&mut self, _: &[u8]) -> Offer {
            Offer::Sent
        }
        fn find_recording_counter(
            &self,
            _: i32,
            _: i64,
        ) -> Option<crate::consensus_module_agent::ports::RecordingCounter> {
            None
        }
        fn find_recording_counter_by_recording(
            &self,
            _: i64,
            _: i64,
        ) -> Option<crate::consensus_module_agent::ports::RecordingCounter> {
            None
        }
        fn counter_value(&self, _: i32) -> i64 {
            0
        }
        fn is_counter_active(&self, _: i32, _: i64) -> bool {
            false
        }
        fn set_counter(&mut self, _: CounterId, _: i64) {}
        fn counter_id(&self, _: CounterId) -> i32 {
            -1
        }
        fn control_toggle(&self) -> i32 {
            1
        }
        fn set_control_toggle(&mut self, _: i32) {}
        fn compare_and_set_control_toggle(&mut self, _: i32, _: i32) -> bool {
            true
        }
        fn node_control_toggle(&self) -> i32 {
            1
        }
        fn set_node_control_toggle(&mut self, _: i32) {}
        fn next_correlation_id(&mut self) -> i64 {
            1
        }
        fn has_local_sockets(&self, _: crate::consensus_module_agent::ports::SubscriptionId) -> bool {
            false
        }
        fn resolved_endpoint(&self, _: crate::consensus_module_agent::ports::SubscriptionId) -> Option<String> {
            None
        }
    }

    fn image() -> IngressImage {
        IngressImage {
            correlation_id: 5,
            session_id: 9,
            source_identity: "127.0.0.1:1".into(),
        }
    }

    fn term(now_ns: i64) -> Term {
        Term {
            leadership_term_id: 0,
            leader_member_id: 0,
            timestamp: now_ns / 1_000_000,
            now_ns,
        }
    }

    fn egress_of(actions: &[Action]) -> EgressId {
        let [Action::ConnectEgress { egress, .. }] = actions else {
            panic!("one egress: {actions:?}");
        };
        *egress
    }

    #[test]
    fn a_client_on_the_leader_is_opened_in_the_log_once_its_egress_connects() {
        let mut m = SessionManager::new(0, 0, 10_000_000_000, 10, 10_000_000_000, false, 0);
        let mut ports = FakePorts::default();
        m.on_session_connect(
            77,
            102,
            PROTOCOL_SEMANTIC_VERSION,
            "aeron:udp?endpoint=c:1".into(),
            "name=c",
            &image(),
            true,
            "",
            0,
        );
        let actions = m.take_actions();
        let Action::ConnectEgress {
            counter: Some((_, label, 1)),
            ..
        } = &actions[0]
        else {
            panic!("{actions:?}");
        };
        assert_eq!(
            label,
            "cluster-session: name=c sourceIdentity=127.0.0.1:1 sessionId=9 - clusterId=0"
        );
        let egress = egress_of(&actions);
        assert_eq!(m.process_pending_sessions(&term(1), &mut ports).unwrap(), 0);
        m.on_egress_added(egress, &Ok(()), 2);
        ports.connected.insert(egress);
        assert_eq!(m.process_pending_sessions(&term(3), &mut ports).unwrap(), 1);
        assert!(matches!(
            log_publisher::decode(&ports.log[0]).unwrap(),
            log_publisher::LogMessage::SessionOpen {
                cluster_session_id: 1,
                correlation_id: 77,
                ..
            }
        ));
        assert_eq!(m.sessions()[0].state, SessionState::Open);
        assert_eq!(ports.egress[&egress].len(), 1, "told OK");
        assert_eq!(m.next_committed_session_id(), 1);
        m.sweep_uncommitted_sessions(ports.position);
        assert_eq!(m.next_committed_session_id(), 2);
    }

    #[test]
    fn a_session_times_out_and_its_close_is_appended() {
        let mut m = SessionManager::new(0, 0, 1_000, 10, 0, false, 0);
        let mut ports = FakePorts::default();
        m.on_replay_session_open(96, 1, 4, 0, 102, "c".into());
        assert_eq!(m.check_sessions(&term(500), "", &mut ports).unwrap(), 0);
        assert_eq!(m.check_sessions(&term(2_000), "", &mut ports).unwrap(), 1);
        assert!(matches!(
            log_publisher::decode(&ports.log[0]).unwrap(),
            log_publisher::LogMessage::SessionClose {
                cluster_session_id: 4,
                close_reason: CloseReason::TIMEOUT
            }
        ));
        assert!(m.sessions().is_empty());
        assert_eq!(m.timed_out_count, 1);
        m.restore_uncommitted_sessions(0);
        assert_eq!(
            m.sessions()[0].state,
            SessionState::Closing,
            "an uncommitted close comes back"
        );
    }

    #[test]
    fn clients_are_redirected_from_a_follower_and_refused_past_the_limit() {
        let mut m = SessionManager::new(1, 0, 10_000, 1, 0, false, 0);
        let mut ports = FakePorts::default();
        m.on_session_connect(
            1,
            102,
            PROTOCOL_SEMANTIC_VERSION,
            "c".into(),
            "",
            &image(),
            false,
            "0=h0:1",
            0,
        );
        let egress = egress_of(&m.take_actions());
        m.on_egress_added(egress, &Ok(()), 0);
        ports.connected.insert(egress);
        assert_eq!(m.send_redirects(&term(1), &mut ports), 1);
        assert_eq!(ports.egress[&egress].len(), 1);

        m.on_replay_session_open(96, 1, 4, 0, 102, "c".into());
        m.on_session_connect(2, 102, PROTOCOL_SEMANTIC_VERSION, "c".into(), "", &image(), true, "", 0);
        m.on_session_connect(3, 102, 1 << 16, "c".into(), "", &image(), true, "", 0);
        let connects = m
            .take_actions()
            .into_iter()
            .filter(|a| matches!(a, Action::ConnectEgress { .. }))
            .count();
        assert_eq!(connects, 2, "even a refused client gets its answer on an egress");
        assert_eq!(m.rejected_user_sessions.len(), 2);
        assert_eq!(
            m.rejected_user_sessions[1].response_detail,
            "invalid client version 1.0.0, cluster is 0.3.0"
        );
        assert_eq!(m.rejected_user_sessions[0].response_detail, SESSION_LIMIT_MSG);
    }
}
