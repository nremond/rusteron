//! Java's `Election`: the members canvass each other's logs; one stands as
//! candidate after a random delay and wins a quorum's votes; the leader
//! publishes the new term, replays its log and joins it; a follower
//! replicates what it lacks from the leader's archive, replays its own log,
//! catches up on the leader's and joins its live log.
//!
//! Java blocks in a few places (the log recording stopped and listed as a
//! new term begins, the services' acks as a log is joined); here each is a
//! step the duty cycle checks.
//!
//! Where Java's election is laxer, Raft's rules hold, with nothing changed
//! on the wire (`rusteron-cluster-sim` and the Jepsen suite show Java's ways
//! losing committed entries):
//! - a member that voted in a term follows no leader of an older one, and
//!   canvasses with the term it voted in;
//! - a member leading a term steps down on hearing of a newer one, in its
//!   election or after, whatever its module's state;
//! - a leader refused does not keep a member from standing;
//! - a leader counts a member's position towards its quorum only as far as
//!   that member's log is known to be its own (`matched_position`);
//! - a leader names the latest term both logs hold (`matching_term`), and a
//!   member cuts back the terms the leader never had;
//! - a member files a term as soon as its log is about to receive the term's
//!   entries, so its log's last term always describes them (Java files it
//!   once joined, and an election starting over in between leaves entries
//!   no term holds);
//! - a member canvasses and votes with the term of its log's last entry, not
//!   of the last term event it replayed;
//! - no member truncates below its commit position.
//!
//! And where Java's election can leave a member out for good: the module
//! reads its log with the term the log holds there, not one it stood for in
//! an election that failed; the acks of a services' join given up on are
//! dropped; a member in follower replay without word from its leader starts
//! over.
//!
//! And where Java's election leaves the others waiting: a member replaying
//! its log as a follower gives the replay up once its leader is silent for
//! the heartbeat timeout, so it canvasses and votes again; the replay is held
//! until its services reach where the module read it (`replay_drained`).

use super::*;

/// Java's `TimeUnit.DAYS.toNanos(1)`: how long before its start an
/// election's last updates count as sent.
const ONE_DAY_NS: i64 = 86_400_000_000_000;
/// How long an election may stay in one state before it says what it waits on.
const STALL_NS: i64 = 10_000_000_000;
/// `ReplayMerge.LIVE_ADD_MAX_WINDOW`: how near the live log a catch-up must
/// be before the live destination is added.
const LIVE_ADD_MAX_WINDOW: i64 = 32 * 1024 * 1024;

impl Election {
    #[allow(clippy::too_many_arguments)]
    fn new(
        is_node_startup: bool,
        graceful_closed_leader_id: i32,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        append_position: i64,
        now_ns: i64,
    ) -> Self {
        let initial = now_ns - ONE_DAY_NS;
        Self {
            state: ElectionState::Init,
            is_node_startup,
            is_first_init: true,
            is_extended_canvass: is_node_startup,
            graceful_closed_leader_id,
            log_position,
            append_position,
            notified_commit_position: 0,
            catchup_join_position: NULL_POSITION,
            replication_leadership_term_id: NULL_VALUE,
            replication_stop_position: NULL_POSITION,
            replication_deadline_ns: 0,
            leader_term_base_log_position: NULL_POSITION,
            replication_term_base_log_position: 0,
            leader_recording_id: NULL_VALUE,
            log_leadership_term_id: leadership_term_id,
            initial_log_leadership_term_id: leadership_term_id,
            initial_term_base_log_position: term_base_log_position,
            initial_time_of_last_update_ns: initial,
            leadership_term_id,
            candidate_term_id: leadership_term_id,
            time_of_last_state_change_ns: 0,
            time_of_last_update_ns: initial,
            time_of_last_commit_position_update_ns: initial,
            nomination_deadline_ns: 0,
            stand_not_before_ns: 0,
            last_published_commit_position: 0,
            last_published_append_position: 0,
            log_session_id: None,
            is_leader_startup: false,
            leader: None,
            time_of_last_leader_message_ns: now_ns,
            replay_drain: None,
            time_of_last_stall_warning_ns: now_ns,
            log_replay: None,
            log_replication: None,
            join_log: None,
            prepare: None,
        }
    }

    fn has_update_interval_expired(&self, now_ns: i64, interval_ns: i64) -> bool {
        now_ns - self.time_of_last_update_ns >= interval_ns
    }
}

impl ConsensusModule {
    fn election(&mut self) -> &mut Election {
        self.election.as_mut().expect("an election")
    }

    /// The node's first election, as it starts (Java's `onStart`): its
    /// consensus publications to the other members added first.
    pub(super) fn start_election(&mut self, ports: &mut impl Ports) {
        let this = &self.members[self.this];
        for member in self.members.iter().filter(|m| m.id != this.id) {
            let channel = cluster_member::consensus_channel(
                &self.config.consensus_channel,
                member,
                this,
                self.config.enable_control_on_consensus_channel,
            );
            self.actions.push_back(Action::AddConsensusPublication {
                member_id: member.id,
                channel,
                stream_id: self.config.consensus_stream_id,
            });
        }
        let plan = self.recovery_plan.as_ref();
        let (term, base, append) = plan.map_or((NULL_VALUE, 0, 0), |p| {
            (
                p.last_leadership_term_id,
                p.last_term_base_log_position,
                p.appended_log_position,
            )
        });
        self.election = Some(Election::new(
            true,
            NULL_VALUE as i32,
            term,
            base,
            self.commit_position,
            append,
            self.now_ns,
        ));
        self.count_election(ports);
    }

    fn count_election(&mut self, ports: &mut impl Ports) {
        ports.set_counter(CounterId::ElectionState, ElectionState::Init as i64);
        self.election_count += 1;
        ports.set_counter(CounterId::ElectionCount, self.election_count);
    }

    /// An election after the node started (Java's `enterElection`): a
    /// leader that closed its log is not waited for.
    pub(super) fn enter_election(
        &mut self,
        is_log_end_of_stream: bool,
        reason: &str,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        if self.election.is_some() {
            return Err(Fault::Error("election in progress".into()));
        }
        self.set_role(Role::Follower, ports);
        let base = self
            .recording_log
            .find_term_entry(self.leadership_term_id)
            .map(|e| e.term_base_log_position)
            .or_else(|| self.recovery_plan.as_ref().map(|p| p.last_term_base_log_position))
            .unwrap_or(0);
        let append = match self.append_counter {
            Some(c) => ports.counter_value(c.counter_id),
            None => self
                .recovery_plan
                .as_ref()
                .map_or(0, |p| p.appended_log_position)
                .max(self.log_recording_stop_position),
        };
        self.warn(reason.to_string());
        let graceful = if is_log_end_of_stream {
            self.members[self.leader].id
        } else {
            NULL_VALUE as i32
        };
        self.election = Some(Election::new(
            false,
            graceful,
            self.leadership_term_id,
            base,
            self.commit_position,
            append,
            self.now_ns,
        ));
        self.count_election(ports);
        self.election_work(ports)?;
        Ok(())
    }

    /// Java's `Election.state`: the role each state implies, the leader's
    /// log publication, the intervals restarted.
    pub(super) fn set_election_state(&mut self, state: ElectionState, ports: &mut impl Ports) {
        let now_ns = self.now_ns;
        let Some(election) = self.election.as_mut() else {
            return;
        };
        if election.state == state {
            return;
        }
        if election.state == ElectionState::Canvass {
            election.is_extended_canvass = false;
        }
        election.state = state;
        election.time_of_last_state_change_ns = now_ns;
        election.time_of_last_update_ns = election.initial_time_of_last_update_ns;
        election.time_of_last_commit_position_update_ns = election.initial_time_of_last_update_ns;
        if state == ElectionState::Init {
            election.prepare = None;
        }
        ports.set_counter(CounterId::ElectionState, state as i64);
        match state {
            ElectionState::Canvass => {
                let (term, append) = (election.leadership_term_id, election.append_position);
                election.leader = None;
                cluster_member::reset(&mut self.members);
                let this = &mut self.members[self.this];
                this.leadership_term_id = term;
                this.log_position = append;
                self.set_role(Role::Follower, ports);
            }
            ElectionState::CandidateBallot => self.set_role(Role::Candidate, ports),
            ElectionState::LeaderLogReplication => {
                let append = election.append_position;
                election.log_session_id = None;
                self.set_role(Role::Leader, ports);
                self.add_log_publication(append, ports);
            }
            ElectionState::FollowerLogReplication | ElectionState::FollowerReplay => {
                self.set_role(Role::Follower, ports)
            }
            _ => {}
        }
    }

    pub(super) fn election_work(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        self.warn_of_stall();
        let state = self.election.as_ref().expect("an election").state;
        match state {
            ElectionState::Init => self.election_init(ports),
            ElectionState::Canvass => self.canvass(ports),
            ElectionState::Nominate => self.nominate(ports),
            ElectionState::CandidateBallot => self.candidate_ballot(ports),
            ElectionState::FollowerBallot => self.follower_ballot(ports),
            ElectionState::LeaderLogReplication => self.leader_log_replication(ports),
            ElectionState::LeaderReplay => self.leader_replay(ports),
            ElectionState::LeaderInit => self.leader_init(ports),
            ElectionState::LeaderReady => self.leader_ready(ports),
            ElectionState::FollowerLogReplication => self.follower_log_replication(ports),
            ElectionState::FollowerReplay => self.follower_replay(ports),
            ElectionState::FollowerCatchupInit => self.follower_catchup_init(ports),
            ElectionState::FollowerCatchupAwait => self.follower_join_log(ports, true),
            ElectionState::FollowerCatchup => self.follower_catchup(ports),
            ElectionState::FollowerLogInit => self.follower_log_init(ports),
            ElectionState::FollowerLogAwait => self.follower_join_log(ports, false),
            ElectionState::FollowerReady => self.follower_ready(ports),
            ElectionState::Closed => Ok(0),
        }
    }

    /// An election in one state for longer than [`STALL_NS`] says what it
    /// waits on, once every [`STALL_NS`]: its waits are states, not Java's
    /// blocking calls, and would otherwise pass unseen.
    fn warn_of_stall(&mut self) {
        let now_ns = self.now_ns;
        let election = self.election.as_ref().expect("an election");
        if now_ns < election.time_of_last_state_change_ns + STALL_NS
            || now_ns < election.time_of_last_stall_warning_ns + STALL_NS
        {
            return;
        }
        let replay = election.log_replay.as_ref().map(|r| {
            let step = match r.step {
                ReplayStep::Replay { .. } => "Replay",
                ReplayStep::AwaitImage { .. } => "AwaitImage",
                ReplayStep::AwaitServices { .. } => "AwaitServices",
                ReplayStep::Read => "Read",
            };
            format!("{step} {}..{}", r.start, r.stop)
        });
        let snapshot = self.take_snapshot.as_ref().map(|t| {
            let step = match t.step {
                SnapshotStep::Incomplete => "Incomplete",
                SnapshotStep::AddPublication => "AddPublication",
                SnapshotStep::AwaitPublication => "AwaitPublication",
                SnapshotStep::StartRecording => "StartRecording",
                SnapshotStep::AwaitCounter { .. } => "AwaitCounter",
                SnapshotStep::Offer { .. } => "Offer",
                SnapshotStep::AwaitRecorded { .. } => "AwaitRecorded",
            };
            format!("{step} at {}", t.log_position)
        });
        let message = format!(
            "election stalled - state={:?} for {} ms prepare={:?} replay={replay:?} replayDrain={:?} \
             snapshot={snapshot:?} moduleState={:?} logPosition={} commitPosition={} serviceAckId={} serviceAcks={:?}",
            election.state,
            (now_ns - election.time_of_last_state_change_ns) / 1_000_000,
            election.prepare.as_ref().map(|p| &p.step),
            election.replay_drain,
            self.state,
            self.log_adapter_position(),
            self.commit_position,
            self.service_ack_id,
            self.service_acks.fronts()
        );
        self.election().time_of_last_stall_warning_ns = now_ns;
        self.warn(message);
    }

    // ---------------------------------------------------------------- init

    fn election_init(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        if self.election().prepare.is_none() {
            let election = self.election();
            let prepare = if election.is_first_init {
                election.is_first_init = false;
                if election.is_node_startup {
                    Prepare::done()
                } else {
                    let position = election.log_position;
                    self.prepare_for_new_leadership(position, ports)
                }
            } else {
                if !self.replay_drained(ports)? {
                    return Ok(0);
                }
                self.stop_log_replication(ports);
                self.stop_all_catchups(ports);
                // What the module, and its services after it, already read of
                // the log in this election, a catch-up or a replay, stays
                // read. Java starts again from where the election began, and
                // its services then apply entries twice. Rare in Java, it
                // follows each newer term heard of here.
                let position = self.election().log_position.max(self.log_adapter_position());
                self.election().log_position = position;
                let mut prepare = self.prepare_for_new_leadership(position, ports);
                self.election().log_session_id = None;
                self.election().join_log = None;
                if let Some(replay) = self.election().log_replay.take() {
                    self.stop_replay(replay, ports);
                }
                self.election().last_published_append_position = 0;
                if let Some(subscription) = self.log_subscription.take() {
                    self.actions.push_back(Action::RemoveSubscription { subscription });
                    prepare.sockets.push(subscription);
                }
                self.log_subscription_images.clear();
                prepare
            };
            self.election().prepare = Some(prepare);
        }
        if !self.prepare_work(ports)? {
            return Ok(0);
        }
        let ran = self.election().prepare.as_ref().is_some_and(|p| p.ran);
        let last_append_position = self.last_append_position;
        let candidate_term_id = self.node_state.candidate_term_id;
        let single = self.members.len() == 1;
        let this = self.this;
        let election = self.election();
        if ran && last_append_position != NULL_POSITION {
            election.append_position = last_append_position;
        }
        election.notified_commit_position = 0;
        election.candidate_term_id = candidate_term_id.max(election.leadership_term_id);
        if single {
            // A cluster of one: it nominates itself and wins at once.
            election.candidate_term_id += 1;
            let (candidate, position) = (election.candidate_term_id, election.log_position);
            election.leadership_term_id = candidate;
            election.leader = Some(this);
            let (offset, bytes) = self
                .node_state
                .update_candidate_term_id(candidate, position, self.epoch_ms());
            ports
                .write_node_state(offset, &bytes)
                .map_err(|e| Fault::Error(format!("node state: {e}")))?;
            self.leader = self.this;
            self.set_election_state(ElectionState::LeaderLogReplication, ports);
        } else {
            self.set_election_state(ElectionState::Canvass, ports);
        }
        Ok(1)
    }

    /// Whether a replay the election gives up may stop: once the services
    /// ack the position the module read it to. They read it after the
    /// module, up to the commit position it sets, and a replay stopped while
    /// they lag takes its image from them short of where the next replay
    /// begins: they refuse to join that one ("Cluster log must be
    /// contiguous"), and the election waits on their ack for good. Until
    /// then the module reads no further than the end of a message
    /// (`log_poll_limit`), and what it read is committed: a replay stops at
    /// the leader's commit position.
    fn replay_drained(&mut self, ports: &mut impl Ports) -> Outcome<bool> {
        let reading = matches!(
            self.election().log_replay,
            Some(LogReplay {
                step: ReplayStep::Read,
                ..
            })
        );
        if !reading || self.config.service_count == 0 {
            return Ok(true);
        }
        match self.election().replay_drain {
            Some(position) => {
                if !self.services_joined(position)? {
                    return Ok(false);
                }
                self.election().replay_drain = None;
                Ok(true)
            }
            None => {
                let position = self.log_adapter_position();
                self.propose_commit_position(position, ports);
                // A snapshot the replay asked for is awaited first: its acks
                // are the services' at this position too, and would be taken
                // for the drain's, or the drain's for its.
                if self.take_snapshot.is_some() || self.state == ModuleState::Snapshot {
                    return Ok(false);
                }
                // The services' log position is the end of the last message
                // they read, or their image's once their log ends: one past a
                // term's padding, or inside a fragmented message, they do not
                // reach while the replay goes on (Java's container alike). The
                // module reads on to a message's end, or to the replay's stop.
                let at_message_end = self.reader.as_ref().is_some_and(|r| r.message_position == r.position);
                let stop = self.election().log_replay.as_ref().map_or(position, |r| r.stop);
                if !at_message_end && position < stop {
                    return Ok(false);
                }
                if self.offer_service(ports, &service_proxy::request_service_ack(position)) {
                    self.election().replay_drain = Some(position);
                }
                Ok(false)
            }
        }
    }

    pub(super) fn epoch_ms(&self) -> i64 {
        self.now_ns / 1_000_000
    }

    /// Java's `prepareForNewLeadership`, up to its waits: this member a
    /// follower, its ingress, log, log publication and recording let go.
    fn prepare_for_new_leadership(&mut self, log_position: i64, ports: &mut impl Ports) -> Prepare {
        self.set_role(Role::Follower, ports);
        if let Some(ingress) = self.ingress.take() {
            self.actions
                .push_back(Action::RemoveSubscription { subscription: ingress });
        }
        self.ingress_reassemblers.clear();
        self.remove_log_destinations();
        self.last_quorum_backtrack_commit_position = NULL_POSITION;
        let mut sockets = Vec::new();
        if let Some(subscription) = self.disconnect_log() {
            sockets.push(subscription);
        }
        self.actions.push_back(Action::RemoveLogPublication);
        self.log_session_id = None;
        ports.set_control_toggle(toggle::INACTIVE);
        let counter = self.append_counter;
        self.try_stop_log_recording(ports);
        Prepare {
            log_position,
            step: if self.log_recording_id != NULL_VALUE {
                PrepareStep::StopPosition { correlation_id: None }
            } else {
                PrepareStep::SocketsClosed
            },
            counter,
            sockets,
            ran: true,
        }
    }

    /// The waits of `prepareForNewLeadership`: whether they are over.
    fn prepare_work(&mut self, ports: &mut impl Ports) -> Outcome<bool> {
        loop {
            let archive_id = self.archive.archive_id();
            let recording_id = self.log_recording_id;
            let prepare = self.election().prepare.as_mut().expect("preparing");
            match prepare.step {
                PrepareStep::StopPosition { correlation_id: None } => {
                    let correlation_id = self.archive_request(
                        ports,
                        archive::Request::StopPosition { recording_id },
                        Some(Request::PrepareStopPosition),
                    )?;
                    let prepare = self.election().prepare.as_mut().expect("preparing");
                    prepare.step = PrepareStep::StopPosition {
                        correlation_id: Some(correlation_id),
                    };
                    return Ok(false);
                }
                PrepareStep::StopPosition { .. } | PrepareStep::RecoveryPlan { .. } => return Ok(false),
                PrepareStep::CounterGone => {
                    let gone = match prepare.counter {
                        Some(c) => !ports.is_counter_active(c.counter_id, c.registration_id),
                        None => ports
                            .find_recording_counter_by_recording(recording_id, archive_id)
                            .is_none(),
                    };
                    if !gone {
                        return Ok(false);
                    }
                    prepare.step = PrepareStep::SocketsClosed;
                }
                PrepareStep::SocketsClosed => {
                    if prepare.sockets.iter().any(|s| ports.has_local_sockets(*s)) {
                        return Ok(false);
                    }
                    prepare.step = PrepareStep::Done;
                }
                PrepareStep::Done => return Ok(true),
            }
        }
    }

    /// The archive's answer to the stop position of the log's recording:
    /// asked again while it still records; then the recovery plan.
    pub(super) fn on_prepare_stop_position(
        &mut self,
        correlation_id: i64,
        stop_position: i64,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        let Some(Election {
            prepare: Some(prepare), ..
        }) = self.election.as_mut()
        else {
            return Ok(());
        };
        if prepare.step
            != (PrepareStep::StopPosition {
                correlation_id: Some(correlation_id),
            })
        {
            return Ok(());
        }
        if stop_position == NULL_POSITION {
            prepare.step = PrepareStep::StopPosition { correlation_id: None };
            return Ok(());
        }
        self.last_append_position = stop_position;
        self.time_of_last_append_position_update_ns = self.now_ns;
        match self.recording_log.plan_recording_id(self.log_recording_id) {
            Some(recording_id) => {
                let correlation_id = self.archive_request(
                    ports,
                    archive::Request::ListRecording { recording_id },
                    Some(Request::PrepareRecoveryPlan),
                )?;
                if let Some(prepare) = self.election().prepare.as_mut() {
                    prepare.step = PrepareStep::RecoveryPlan { correlation_id };
                }
                Ok(())
            }
            None => self.on_prepare_recovery_plan(None, ports),
        }
    }

    /// The recovery plan made again: the sessions, the commit position and
    /// what was not committed rolled back to where the election started.
    pub(super) fn on_prepare_recovery_plan(
        &mut self,
        extent: Option<RecordingExtent>,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        let plan = self
            .recording_log
            .create_recovery_plan(self.config.service_count as i32, self.log_recording_id, extent)
            .map_err(|e| Fault::Error(e.to_string()))?;
        self.recovery_plan = Some(plan);
        let Some(prepare) = self.election().prepare.as_mut() else {
            return Ok(());
        };
        let log_position = prepare.log_position;
        prepare.step = PrepareStep::CounterGone;
        let term = self.leadership_term_id;
        self.sessions.clear_sessions_after(log_position, term, ports);
        self.sessions.disconnect_sessions();
        self.set_commit_position(log_position, ports);
        self.restore_uncommitted_entries(log_position, ports);
        Ok(())
    }

    /// Whether the election waits on the archive's answer `correlation_id`
    /// for the recovery plan.
    pub(super) fn is_prepare_plan(&self, correlation_id: i64) -> bool {
        matches!(
            self.election,
            Some(Election {
                prepare: Some(Prepare {
                    step: PrepareStep::RecoveryPlan { correlation_id: c },
                    ..
                }),
                ..
            }) if c == correlation_id
        )
    }

    /// The log's image let go and its subscription closed, the position it
    /// reached kept (Java's `LogAdapter.disconnect`).
    pub(super) fn disconnect_log(&mut self) -> Option<SubscriptionId> {
        let reader = self.reader.take()?;
        if let Some(position) = reader.position {
            self.log_adapter_position = position;
        }
        self.actions.push_back(Action::RemoveSubscription {
            subscription: reader.subscription,
        });
        if self.log_subscription == Some(reader.subscription) {
            self.log_subscription = None;
            self.log_subscription_images.clear();
        }
        Some(reader.subscription)
    }

    /// The follower's catch-up and live destinations removed.
    fn remove_log_destinations(&mut self) {
        for destination in [self.catchup_log_destination.take(), self.live_log_destination.take()]
            .into_iter()
            .flatten()
        {
            self.remove_log_destination(destination);
        }
    }

    /// Java's `LogAdapter.asyncRemoveDestination`: only while its image is there.
    pub(super) fn remove_log_destination(&mut self, destination: String) {
        if let Some(reader) = &self.reader
            && Some(reader.subscription) == self.log_subscription
        {
            self.actions.push_back(Action::RemoveDestination {
                subscription: reader.subscription,
                destination,
            });
        }
    }

    // ---------------------------------------------------------------- ballots

    fn canvass(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let now_ns = self.now_ns;
        let mut work = 0;
        let election = self.election.as_ref().expect("an election");
        let deadline_ns = if election.is_extended_canvass {
            election.time_of_last_state_change_ns + self.config.startup_canvass_timeout_ns
        } else {
            self.time_of_last_leader_update_ns + self.config.leader_heartbeat_timeout_ns
        };
        if election.has_update_interval_expired(now_ns, self.config.election_status_interval_ns) {
            self.election().time_of_last_update_ns = now_ns;
            self.publish_canvass_position(ports)?;
            work += 1;
        }
        let appointed = self.config.appointed_leader_id;
        if appointed != NULL_VALUE as i32 && appointed != self.config.member_id {
            return Ok(work);
        }
        let graceful = self.election().graceful_closed_leader_id;
        let this = &self.members[self.this];
        if cluster_member::is_unanimous_candidate(&self.members, this, graceful)
            || (now_ns >= deadline_ns && cluster_member::is_quorum_candidate(&self.members, this))
        {
            let delay_ns = (self.next_random() * (self.config.election_timeout_ns >> 1) as f64) as i64;
            let election = self.election();
            election.nomination_deadline_ns = (now_ns + delay_ns).max(election.stand_not_before_ns);
            self.set_election_state(ElectionState::Nominate, ports);
            work += 1;
        }
        Ok(work)
    }

    fn nominate(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let now_ns = self.now_ns;
        let election = self.election.as_ref().expect("an election");
        if now_ns >= election.nomination_deadline_ns {
            let (proposed, position) = (election.candidate_term_id + 1, election.log_position);
            let candidate = self.propose_max_candidate_term_id(proposed, position, ports)?;
            self.election().candidate_term_id = candidate;
            cluster_member::become_candidate(&mut self.members, candidate, self.config.member_id);
            self.set_election_state(ElectionState::CandidateBallot, ports);
            return Ok(1);
        }
        if election.has_update_interval_expired(now_ns, self.config.election_status_interval_ns) {
            self.election().time_of_last_update_ns = now_ns;
            self.publish_canvass_position(ports)?;
            return Ok(1);
        }
        Ok(0)
    }

    fn candidate_ballot(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        self.refresh_log_term();
        let now_ns = self.now_ns;
        let election = self.election.as_ref().expect("an election");
        let (candidate, graceful) = (election.candidate_term_id, election.graceful_closed_leader_id);
        if cluster_member::is_unanimous_leader(&self.members, candidate, graceful) {
            self.win_ballot(candidate, ports);
            return Ok(1);
        }
        if now_ns >= election.time_of_last_state_change_ns + self.config.election_timeout_ns {
            if cluster_member::is_quorum_leader(&self.members, candidate) {
                self.win_ballot(candidate, ports);
            } else {
                self.set_election_state(ElectionState::Canvass, ports);
            }
            return Ok(1);
        }
        let (log_term, append) = (election.log_leadership_term_id, election.append_position);
        let message = consensus_adapter::request_vote(log_term, append, candidate, self.config.member_id);
        let mut work = 0;
        for i in 0..self.members.len() {
            if !self.members[i].is_ballot_sent {
                work += 1;
                let id = self.members[i].id;
                self.members[i].is_ballot_sent = consensus_adapter::send(ports, id, &message)?;
            }
        }
        Ok(work)
    }

    fn win_ballot(&mut self, candidate_term_id: i64, ports: &mut impl Ports) {
        let this = self.this;
        let election = self.election();
        election.leader = Some(this);
        election.leadership_term_id = candidate_term_id;
        self.leader = this;
        self.set_election_state(ElectionState::LeaderLogReplication, ports);
    }

    fn follower_ballot(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let election = self.election.as_ref().expect("an election");
        if self.now_ns >= election.time_of_last_state_change_ns + self.config.election_timeout_ns {
            self.set_election_state(ElectionState::Canvass, ports);
            return Ok(1);
        }
        Ok(0)
    }

    /// Java's `NodeStateFile.proposeMaxCandidateTermId`: the higher term
    /// persisted before it is acted on, written and synced before this
    /// returns. Queued as an action, it was written after the vote it
    /// allowed had gone out, and a crash in between left the member free
    /// to vote again in that term.
    fn propose_max_candidate_term_id(
        &mut self,
        candidate_term_id: i64,
        log_position: i64,
        ports: &mut impl Ports,
    ) -> Outcome<i64> {
        let epoch_ms = self.epoch_ms();
        let (candidate_term_id, write) =
            self.node_state
                .propose_max_candidate_term_id(candidate_term_id, log_position, epoch_ms);
        if let Some((offset, bytes)) = write {
            ports
                .write_node_state(offset, &bytes)
                .map_err(|e| Fault::Error(format!("node state: {e}")))?;
        }
        Ok(candidate_term_id)
    }

    fn place_vote(
        &mut self,
        candidate_term_id: i64,
        candidate_id: i32,
        vote: bool,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        if cluster_member::find_member(&self.members, candidate_id).is_none() {
            return Ok(());
        }
        self.refresh_log_term();
        let election = self.election.as_ref().expect("an election");
        let message = consensus_adapter::vote(
            candidate_term_id,
            election.log_leadership_term_id,
            election.append_position,
            candidate_id,
            self.config.member_id,
            vote,
        );
        consensus_adapter::send(ports, candidate_id, &message)?;
        Ok(())
    }

    /// The member's log, and the term it voted in where Java sends its
    /// current term: Raft's term on every message, so a leader of an older
    /// term learns that this member has moved past it.
    fn publish_canvass_position(&mut self, ports: &mut impl Ports) -> Outcome<()> {
        self.refresh_log_term();
        let election = self.election.as_ref().expect("an election");
        let message = consensus_adapter::canvass_position(
            election.log_leadership_term_id,
            election.append_position,
            election.candidate_term_id.max(election.leadership_term_id),
            self.config.member_id,
        );
        for i in 0..self.members.len() {
            if i != self.this {
                let id = self.members[i].id;
                // Java's canvassPosition sends and forgets.
                consensus_adapter::send(ports, id, &message)?;
            }
        }
        Ok(())
    }

    // ---------------------------------------------------------------- the leader

    fn leader_log_replication(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let election = self.election.as_ref().expect("an election");
        if election.log_session_id.is_none() {
            return Ok(0);
        }
        let append = election.append_position;
        let this = &mut self.members[self.this];
        this.log_position = append;
        this.time_of_last_append_position_ns = self.now_ns;
        let quorum = self.quorum_position_bounded_by_leader_log(append);
        let mut work = self.publish_new_leadership_term_on_interval(quorum, ports)?;
        work += self.publish_commit_position_on_interval(quorum, ports)?;
        if quorum >= append {
            self.set_election_state(ElectionState::LeaderReplay, ports);
            work += 1;
        }
        Ok(work)
    }

    fn leader_replay(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let mut work = 0;
        if self.election().log_replay.is_none() {
            let election = self.election();
            let (start, stop) = (election.log_position, election.append_position);
            if start < stop {
                self.new_log_replay(start, stop, ports)?;
            } else {
                self.set_election_state(ElectionState::LeaderInit, ports);
            }
            work += 1;
            let election = self.election();
            election.is_leader_startup = election.is_node_startup;
            let (term, append) = (election.leadership_term_id, election.append_position);
            let this = &mut self.members[self.this];
            this.leadership_term_id = term;
            this.log_position = append;
            this.time_of_last_append_position_ns = self.now_ns;
        } else {
            let (replay_work, done) = self.log_replay_work(ports)?;
            work += replay_work;
            if done.is_some() {
                let replay = self.election().log_replay.take().expect("a replay");
                self.stop_replay(replay, ports);
                let election = self.election();
                election.log_position = election.append_position;
                self.set_election_state(ElectionState::LeaderInit, ports);
            }
        }
        let append = self.election().append_position;
        let quorum = self.quorum_position_bounded_by_leader_log(append);
        work += self.publish_new_leadership_term_on_interval(quorum, ports)?;
        work += self.publish_commit_position_on_interval(quorum, ports)?;
        Ok(work)
    }

    /// Java's `newLogReplay`: the archive asked to replay the log from
    /// `start` to `stop`, to the services and the module.
    pub(super) fn new_log_replay(&mut self, start: i64, stop: i64, ports: &mut impl Ports) -> Outcome<()> {
        self.take_log_term_at(start, ports);
        let request = archive::Request::Replay {
            recording_id: self.log_recording_id,
            position: start,
            length: stop - start,
            channel: self.config.replay_channel.clone(),
            stream_id: self.config.replay_stream_id,
        };
        let correlation_id = self.archive_request(ports, request, None)?;
        self.election().log_replay = Some(LogReplay {
            start,
            stop,
            replay_session_id: NULL_VALUE,
            services_asked: false,
            step: ReplayStep::Replay { correlation_id },
        });
        Ok(())
    }

    /// Java's `LogReplay.doWork`: the services told to join the replay once
    /// its image is there, then the log read up to its stop. The position
    /// reached once done.
    fn log_replay_work(&mut self, ports: &mut impl Ports) -> Outcome<(usize, Option<i64>)> {
        let mut work = 0;
        let replay = self.election().log_replay.as_mut().expect("a replay");
        match &mut replay.step {
            ReplayStep::Replay { .. } | ReplayStep::AwaitImage { .. } => {}
            ReplayStep::AwaitServices { reader } => {
                if !replay.services_asked {
                    let (start, stop, session_id) = (replay.start, replay.stop, reader.session_id);
                    replay.services_asked = true;
                    let channel = add_session_id(&self.config.replay_channel, session_id);
                    self.join_services(
                        ports,
                        &channel,
                        self.config.replay_stream_id,
                        session_id,
                        start,
                        stop,
                        true,
                        Role::Follower,
                    )?;
                    return Ok((1, None));
                }
                let start = replay.start;
                if self.services_joined(start)? {
                    let replay = self.election().log_replay.as_mut().expect("a replay");
                    let ReplayStep::AwaitServices { reader } = std::mem::replace(
                        &mut replay.step,
                        ReplayStep::Replay {
                            correlation_id: NULL_VALUE,
                        },
                    ) else {
                        unreachable!()
                    };
                    replay.step = ReplayStep::Read;
                    self.reader = Some(reader);
                    work += 1;
                }
            }
            ReplayStep::Read => {
                let stop = replay.stop;
                if self.state == ModuleState::Active || self.state == ModuleState::Suspended {
                    let position = self.log_adapter_position();
                    if self.propose_commit_position(position, ports) {
                        work += 1;
                    } else if self.reader.as_ref().is_none_or(|r| r.closed) && position < stop {
                        return Err(Fault::Warning(format!(
                            "unexpected image close when replaying log: position={position}"
                        )));
                    }
                }
                let position = self.log_adapter_position();
                if position >= stop && self.state != ModuleState::Snapshot {
                    return Ok((work, Some(position)));
                }
            }
        }
        Ok((work, None))
    }

    fn leader_init(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let election = self.election.as_ref().expect("an election");
        let (term, log_position, is_startup) = (
            election.leadership_term_id,
            election.log_position,
            election.is_leader_startup,
        );
        let log_session_id = election.log_session_id.expect("the log publication");
        let is_ipc = self.config.log_channel.starts_with(IPC_CHANNEL);
        let channel = format!(
            "{}?tags={}|session-id={log_session_id}|alias=log",
            if is_ipc { IPC_CHANNEL } else { UDP_CHANNEL },
            self.log_publication_channel_tag
        );
        match election.join_log {
            None => {
                self.election().join_log = Some(JoinLog::StartRecording);
                self.set_leadership_term_id(term, ports);
                self.start_log_recording(channel, true, ports)?;
                self.election().join_log = Some(JoinLog::AwaitCounter);
                Ok(1)
            }
            Some(JoinLog::StartRecording) => Ok(0),
            Some(JoinLog::AwaitCounter) => {
                if !self.try_create_append_position(log_session_id, log_position, ports) {
                    return Ok(0);
                }
                let local_channel = if is_ipc {
                    channel
                } else {
                    format!("{SPY_PREFIX}{channel}")
                };
                self.join_services(
                    ports,
                    &local_channel,
                    self.config.log_stream_id,
                    log_session_id,
                    log_position,
                    i64::MAX,
                    is_startup,
                    Role::Leader,
                )?;
                self.election().join_log = Some(JoinLog::AwaitServices);
                Ok(1)
            }
            Some(JoinLog::AwaitServices) => {
                if !self.services_joined(log_position)? {
                    return Ok(0);
                }
                self.election().join_log = None;
                self.update_recording_log()?;
                self.set_election_state(ElectionState::LeaderReady, ports);
                Ok(1)
            }
        }
    }

    /// Java's `startLogRecording`: the log recorded in a new recording, or
    /// the last term's extended.
    fn start_log_recording(&mut self, channel: String, local: bool, ports: &mut impl Ports) -> Outcome<()> {
        let last = self.recording_log.find_last_term_recording_id();
        let request = if last == NULL_VALUE {
            archive::Request::StartRecording {
                channel,
                stream_id: self.config.log_stream_id,
                local,
                auto_stop: true,
            }
        } else {
            archive::Request::ExtendRecording {
                recording_id: last,
                channel,
                stream_id: self.config.log_stream_id,
                local,
                auto_stop: true,
            }
        };
        self.archive_request(ports, request, Some(Request::LogRecording))?;
        Ok(())
    }

    /// Java's `tryCreateAppendPosition`: the recording's counter of the log
    /// session, once the archive allocated it.
    /// Java's `tryCreateAppendPosition`, the counter also awaited until it
    /// reads the position its recording starts from: an archive allocates
    /// the counter, then stores that position, and a leader that read 0
    /// meanwhile would take its quorum back to 0.
    fn try_create_append_position(&mut self, log_session_id: i32, start_position: i64, ports: &mut impl Ports) -> bool {
        let Some(counter) = ports.find_recording_counter(log_session_id, self.archive.archive_id()) else {
            return false;
        };
        if counter.registration_id == 0
            || counter.recording_id == NULL_VALUE
            || ports.counter_value(counter.counter_id) < start_position
        {
            return false;
        }
        self.log_recording_id = counter.recording_id;
        self.append_counter = Some(counter);
        true
    }

    /// Java's `updateRecordingLog`: the term in the recording log, synced.
    fn update_recording_log(&mut self) -> Outcome<()> {
        let election = self.election.as_ref().expect("an election");
        let (term, log_position) = (election.leadership_term_id, election.log_position);
        self.ensure_recording_log_coherent(term, log_position, NULL_VALUE, self.now_ns)?;
        self.election().log_leadership_term_id = term;
        Ok(())
    }

    /// Java's `ensureRecordingLogCoherent`: the recording log made to agree
    /// with the log up to `leadership_term_id`.
    fn ensure_recording_log_coherent(
        &mut self,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        now_ns: i64,
    ) -> Outcome<()> {
        if self.log_recording_id == NULL_VALUE {
            return Ok(());
        }
        let election = self.election.as_ref().expect("an election");
        let (initial_term, initial_base) = (
            election.initial_log_leadership_term_id,
            election.initial_term_base_log_position,
        );
        let base = if term_base_log_position != NULL_VALUE {
            term_base_log_position
        } else {
            initial_base
        };
        let timestamp = self.config.time_unit.from_nanos(now_ns);
        self.recording_log
            .ensure_coherent(
                self.log_recording_id,
                initial_term,
                initial_base,
                leadership_term_id,
                base,
                log_position,
                now_ns,
                timestamp,
            )
            .map_err(|e| e.to_string())?;
        self.flush_recording_log(true);
        Ok(())
    }

    fn leader_ready(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let election = self.election.as_ref().expect("an election");
        let (append, term, log_position) = (
            election.append_position,
            election.leadership_term_id,
            election.log_position,
        );
        let quorum = self.quorum_position_bounded_by_leader_log(append);
        let mut work = self.update_leader_position_to(append, quorum, ports)?;
        work += self.publish_new_leadership_term_on_interval(quorum, ports)?;
        if cluster_member::has_quorum_at_position(
            &self.members,
            term,
            log_position,
            self.now_ns,
            self.config.leader_heartbeat_timeout_ns,
        ) {
            let message = log_publisher::new_leadership_term_event(
                self.leadership_term_id,
                log_publisher::position_after(ports.log_position(), log_publisher::NEW_LEADERSHIP_TERM_EVENT_LENGTH),
                self.timestamp(),
                log_position,
                self.config.member_id,
                self.log_session_id.unwrap_or(0),
                self.config.time_unit.codec(),
                self.config.app_version,
            );
            if log_publisher::append(ports, &[&message])?.is_some() {
                self.election_complete(ports)?;
                work += 1;
            }
        }
        Ok(work)
    }

    fn publish_new_leadership_term_on_interval(
        &mut self,
        quorum_position: i64,
        ports: &mut impl Ports,
    ) -> Outcome<usize> {
        let now_ns = self.now_ns;
        let election = self.election.as_ref().expect("an election");
        if election.has_update_interval_expired(now_ns, self.config.leader_heartbeat_interval_ns) {
            let log_term = election.log_leadership_term_id;
            self.election().time_of_last_update_ns = now_ns;
            let timestamp = self.config.time_unit.from_nanos(now_ns);
            for i in 0..self.members.len() {
                self.publish_new_leadership_term(i, log_term, quorum_position, timestamp, ports)?;
            }
            return Ok(1);
        }
        Ok(0)
    }

    /// The election's new term to a member, from where its log is
    /// (Java's `Election.publishNewLeadershipTerm`).
    fn publish_new_leadership_term(
        &mut self,
        member: usize,
        log_leadership_term_id: i64,
        quorum_position: i64,
        timestamp: i64,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        let election = self.election.as_ref().expect("an election");
        let Some(log_session_id) = election.log_session_id else {
            return Ok(());
        };
        if member == self.this {
            return Ok(());
        }
        let (log_leadership_term_id, next) = self.matching_term(log_leadership_term_id);
        let next = next.as_ref();
        let election = self.election.as_ref().expect("an election");
        let append = election.append_position;
        let term = NewLeadershipTerm {
            log_leadership_term_id,
            next_leadership_term_id: next.map_or(election.leadership_term_id, |e| e.leadership_term_id),
            next_term_base_log_position: next.map_or(append, |e| e.term_base_log_position),
            next_log_position: next.map_or(NULL_POSITION, |e| {
                if e.log_position != NULL_POSITION {
                    e.log_position
                } else {
                    append
                }
            }),
            leadership_term_id: election.leadership_term_id,
            term_base_log_position: append,
            log_position: append,
            commit_position: quorum_position,
            leader_recording_id: self.log_recording_id,
            timestamp,
            leader_member_id: self.config.member_id,
            log_session_id,
            app_version: self.config.app_version,
            is_startup: election.is_leader_startup,
        };
        let id = self.members[member].id;
        consensus_adapter::send(ports, id, &consensus_adapter::new_leadership_term(&term))?;
        Ok(())
    }

    fn publish_commit_position_on_interval(&mut self, quorum_position: i64, ports: &mut impl Ports) -> Outcome<usize> {
        let now_ns = self.now_ns;
        let interval = self.config.leader_heartbeat_interval_ns;
        let election = self.election.as_ref().expect("an election");
        // Not sent while the quorum position goes back in an election: the
        // new term is sent on its interval all the same.
        if election.last_published_commit_position < quorum_position
            || (election.last_published_commit_position == quorum_position
                && now_ns - election.time_of_last_commit_position_update_ns >= interval)
        {
            let term = election.leadership_term_id;
            let election = self.election();
            election.time_of_last_commit_position_update_ns = now_ns;
            election.last_published_commit_position = quorum_position;
            self.publish_commit_position(quorum_position, term, ports)?;
            return Ok(1);
        }
        Ok(0)
    }

    // ---------------------------------------------------------------- a follower

    fn follower_log_replication(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let now_ns = self.now_ns;
        let mut work = 0;
        let election = self.election.as_ref().expect("an election");
        if election.log_replication.is_none() {
            if election.append_position < election.replication_stop_position {
                let (leader, recording_id, stop) = (
                    election.leader.expect("a leader"),
                    election.leader_recording_id,
                    election.replication_stop_position,
                );
                // Its term filed before it starts, open, not once done as
                // Java does: a replication cut short would leave entries no
                // term holds.
                let (term, base) = (
                    election.replication_leadership_term_id,
                    election.replication_term_base_log_position,
                );
                self.file_open_term(term, base)?;
                let replication = self.new_log_replication(leader, recording_id, stop, ports)?;
                let deadline_ns = now_ns + self.config.leader_heartbeat_timeout_ns;
                let election = self.election();
                election.log_replication = Some(replication);
                election.replication_deadline_ns = deadline_ns;
                work += 1;
            } else {
                let (term, base, stop) = (
                    election.replication_leadership_term_id,
                    election.replication_term_base_log_position,
                    election.replication_stop_position,
                );
                self.update_recording_log_for_replication(term, base, stop)?;
                self.set_election_state(ElectionState::Canvass, ports);
            }
            return Ok(work);
        }
        work += self.replication_poll(ports)?;
        let replication = self.election().log_replication.as_ref().expect("a replication");
        let done = replication.has_replication_ended && replication.has_stopped;
        // Replication runs alongside: the position at its end is published
        // after the check for it.
        work += self.publish_follower_replication_position(ports)?;
        if done {
            let election = self.election();
            if election.notified_commit_position >= election.append_position {
                let replication = election.log_replication.as_ref().expect("a replication");
                election.append_position = replication.position;
                self.stop_log_replication(ports);
                let election = self.election();
                let (term, base, stop) = (
                    election.replication_leadership_term_id,
                    election.replication_term_base_log_position,
                    election.replication_stop_position,
                );
                self.update_recording_log_for_replication(term, base, stop)?;
                self.set_election_state(ElectionState::Canvass, ports);
                work += 1;
            } else if now_ns >= election.replication_deadline_ns {
                return Err(Fault::Warning("timeout awaiting commit position".into()));
            }
        }
        Ok(work)
    }

    /// Java's `newLogReplication`: the leader's recording replicated into
    /// the log's, up to `stop_position`.
    fn new_log_replication(
        &mut self,
        leader: usize,
        leader_recording_id: i64,
        stop_position: i64,
        ports: &mut impl Ports,
    ) -> Outcome<Replication> {
        let src_archive_channel = create_destination_uri(
            &self.config.leader_archive_control_channel,
            &self.members[leader].archive_endpoint,
        );
        let replication_session_id = ports.next_correlation_id() as i32;
        let request = archive::Request::Replicate {
            src_recording_id: leader_recording_id,
            dst_recording_id: self.log_recording_id,
            stop_position,
            src_control_stream_id: self.config.archive_control_request_stream_id,
            src_control_channel: src_archive_channel,
            replication_channel: self.config.replication_channel.clone(),
            replication_session_id,
        };
        let correlation_id = self.archive_request(ports, request, Some(Request::Replicate))?;
        let now_ns = self.now_ns;
        Ok(Replication {
            correlation_id,
            replication_id: None,
            stop_position,
            counter: None,
            recording_id: NULL_VALUE,
            position: NULL_POSITION,
            progress_deadline_ns: now_ns + self.config.leader_heartbeat_timeout_ns,
            progress_check_deadline_ns: now_ns + self.config.leader_heartbeat_interval_ns,
            has_replication_ended: false,
            has_synced: false,
            has_stopped: false,
        })
    }

    /// Java's `RecordingReplication.poll`: the destination recording's
    /// position checked on an interval; no progress in time ends it.
    fn replication_poll(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let now_ns = self.now_ns;
        let interval = self.config.leader_heartbeat_interval_ns;
        let timeout = self.config.leader_heartbeat_timeout_ns;
        let archive_id = self.archive.archive_id();
        let replication = self.election().log_replication.as_mut().expect("a replication");
        if replication.has_replication_ended {
            return Ok(0);
        }
        let (work, progressed) = replication.poll_progress(now_ns, interval, timeout, archive_id, ports);
        if !progressed {
            let fault =
                if replication.stop_position == NULL_POSITION || replication.position < replication.stop_position {
                    Fault::Warning("log replication has not progressed".into())
                } else {
                    Fault::Error("log replication failed to stop".into())
                };
            self.close_log_replication(ports);
            return Err(fault);
        }
        Ok(work)
    }

    /// Java's `RecordingReplication.close`: the archive asked to stop it, unless it ended.
    fn close_log_replication(&mut self, ports: &mut impl Ports) {
        let Some(replication) = self.election.as_mut().and_then(|e| e.log_replication.as_mut()) else {
            return;
        };
        if replication.has_replication_ended {
            return;
        }
        replication.has_replication_ended = true;
        match replication.replication_id {
            Some(replication_id) => {
                let _ = self.archive_request(
                    ports,
                    archive::Request::StopReplication { replication_id },
                    Some(Request::TryStop),
                );
            }
            None => {
                // Not answered yet: stopped once it is.
                let correlation_id = replication.correlation_id;
                self.requests.insert(correlation_id, Request::ReplicateAbandoned);
            }
        }
    }

    pub(super) fn stop_log_replication(&mut self, ports: &mut impl Ports) {
        self.close_log_replication(ports);
        if let Some(election) = self.election.as_mut() {
            election.log_replication = None;
            election.replication_deadline_ns = 0;
            election.last_published_commit_position = 0;
        }
    }

    /// A recording signal for the replication (Java's
    /// `RecordingReplication.onSignal`), its recording the log's from then on.
    pub(super) fn on_replication_signal(
        &mut self,
        correlation_id: i64,
        recording_id: i64,
        position: i64,
        signal: Signal,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        let archive_id = self.archive.archive_id();
        let Some(election) = self.election.as_mut() else {
            return Ok(());
        };
        if election.state == ElectionState::Init {
            return Ok(());
        }
        let Some(replication) = election.log_replication.as_mut() else {
            return Ok(());
        };
        if replication.replication_id != Some(correlation_id) {
            return Ok(());
        }
        if signal == Signal::EXTEND {
            replication.counter = ports.find_recording_counter_by_recording(recording_id, archive_id);
        } else if signal == Signal::SYNC {
            replication.has_synced = true;
        } else if signal == Signal::REPLICATE_END {
            replication.has_replication_ended = true;
        } else if signal == Signal::STOP {
            replication.has_stopped = true;
        } else if signal == Signal::DELETE {
            return Err(Fault::Error(format!(
                "recording was deleted during replication: recordingId={recording_id}"
            )));
        }
        if recording_id != NULL_VALUE {
            replication.recording_id = recording_id;
        }
        if position != NULL_POSITION {
            replication.position = position;
        }
        let recording_id = replication.recording_id;
        if recording_id != NULL_VALUE {
            let new_recording = self.log_recording_id == NULL_VALUE;
            self.log_recording_id = recording_id;
            if new_recording {
                // A new recording: the replication's term filed as soon as it
                // has one, as `follower_log_replication` files it otherwise.
                let election = self.election.as_ref().expect("an election");
                let (term, base) = (
                    election.replication_leadership_term_id,
                    election.replication_term_base_log_position,
                );
                self.file_open_term(term, base)?;
            }
        }
        Ok(())
    }

    /// The archive accepted the replication: its id, which its signals carry.
    pub(super) fn on_replication_started(&mut self, correlation_id: i64, replication_id: i64) {
        if let Some(replication) = self.election.as_mut().and_then(|e| e.log_replication.as_mut())
            && replication.correlation_id == correlation_id
        {
            replication.replication_id = Some(replication_id);
        }
    }

    /// The leader's term filed, open, before its log can reach this member's
    /// recording, not once joined as Java does: an election starting over in
    /// between would leave entries in the log that no term in the recording
    /// log holds, and its last term would then not describe them.
    fn file_leaders_term(&mut self) -> Outcome<()> {
        let election = self.election.as_ref().expect("an election");
        let (term, base) = (election.leadership_term_id, election.leader_term_base_log_position);
        if base != NULL_POSITION && self.log_recording_id != NULL_VALUE {
            self.ensure_recording_log_coherent(term, base, NULL_VALUE, self.now_ns)?;
            self.election().log_leadership_term_id = term;
        }
        Ok(())
    }

    /// A term the log is about to receive entries of filed, open: the last
    /// term made `leadership_term_id`, from `term_base_log_position` (or, for
    /// the last term itself, from where it began). Its end is filed once
    /// known; until then the entries the log holds after its base are its.
    fn file_open_term(&mut self, leadership_term_id: i64, term_base_log_position: i64) -> Outcome<()> {
        if self.log_recording_id == NULL_VALUE {
            return Ok(());
        }
        match self.recording_log.find_last_term() {
            Some(last) if last.leadership_term_id == leadership_term_id => {
                if last.log_position != NULL_POSITION {
                    self.commit_log_position(leadership_term_id, NULL_POSITION)?;
                }
            }
            _ => {
                let base = if term_base_log_position != NULL_VALUE {
                    term_base_log_position
                } else {
                    self.election().append_position
                };
                self.ensure_recording_log_coherent(leadership_term_id, base, NULL_VALUE, self.now_ns)?;
            }
        }
        self.election().log_leadership_term_id = leadership_term_id;
        Ok(())
    }

    fn update_recording_log_for_replication(
        &mut self,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
    ) -> Outcome<()> {
        self.ensure_recording_log_coherent(leadership_term_id, term_base_log_position, log_position, self.now_ns)?;
        self.election().log_leadership_term_id = leadership_term_id;
        Ok(())
    }

    fn publish_follower_replication_position(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let now_ns = self.now_ns;
        let interval = self.config.leader_heartbeat_interval_ns;
        let election = self.election.as_ref().expect("an election");
        let position = election.log_replication.as_ref().map_or(NULL_POSITION, |r| r.position);
        if position > election.append_position
            || (position == election.append_position && election.has_update_interval_expired(now_ns, interval))
        {
            let leader = self.members[election.leader.expect("a leader")].id;
            let message = consensus_adapter::append_position(
                election.leadership_term_id,
                position,
                self.config.member_id,
                consensus_adapter::APPEND_POSITION_FLAG_NONE,
            );
            if consensus_adapter::send(ports, leader, &message)? {
                let election = self.election();
                election.append_position = position;
                election.time_of_last_update_ns = now_ns;
                return Ok(1);
            }
        }
        Ok(0)
    }

    fn publish_follower_append_position(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let now_ns = self.now_ns;
        let interval = self.config.leader_heartbeat_interval_ns;
        let election = self.election.as_ref().expect("an election");
        if election.last_published_append_position != election.append_position
            || election.has_update_interval_expired(now_ns, interval)
        {
            let leader = self.members[election.leader.expect("a leader")].id;
            let message = consensus_adapter::append_position(
                election.leadership_term_id,
                election.append_position,
                self.config.member_id,
                consensus_adapter::APPEND_POSITION_FLAG_NONE,
            );
            if consensus_adapter::send(ports, leader, &message)? {
                let election = self.election();
                election.last_published_append_position = election.append_position;
                election.time_of_last_update_ns = now_ns;
                return Ok(1);
            }
        }
        Ok(0)
    }

    fn follower_replay(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let election = self.election.as_ref().expect("an election");
        if self.now_ns >= election.time_of_last_leader_message_ns + self.config.leader_heartbeat_timeout_ns
            && self.state != ModuleState::Snapshot
        {
            // Java replays to the end whatever its leader does, neither
            // canvassing nor voting meanwhile: a leader lost then leaves the
            // others no quorum to elect another until the replay ends, seconds
            // for a member just restarted. What it replayed stays replayed.
            let leader = election.leader.map_or(NULL_VALUE as i32, |l| self.members[l].id);
            return Err(Fault::Warning(format!(
                "no word from the leader in follower replay - leaderMemberId={leader} logPosition={} appendPosition={}",
                self.log_adapter_position().max(election.log_position),
                election.append_position
            )));
        }
        if election.log_replay.is_none() {
            let (log_position, append, notified) = (
                election.log_position,
                election.append_position,
                election.notified_commit_position,
            );
            if log_position < append {
                if notified == 0 {
                    // Java waits for its leader's commit position for good: a
                    // leader gone since leaves this member out of the cluster.
                    let election = self.election.as_ref().expect("an election");
                    if self.now_ns >= election.time_of_last_state_change_ns + self.config.leader_heartbeat_timeout_ns {
                        return Err(Fault::Warning(
                            "no commit position from the leader in follower replay".into(),
                        ));
                    }
                    return self.publish_follower_append_position(ports);
                } else if log_position >= notified {
                    // "log replay rejected": the leader's quorum is not past this log.
                    self.set_election_state(ElectionState::Canvass, ports);
                } else {
                    self.new_log_replay(log_position, append.min(notified), ports)?;
                    return Ok(1);
                }
                return Ok(0);
            }
            self.follower_replay_done(ports);
            return Ok(1);
        }
        let (work, done) = self.log_replay_work(ports)?;
        if let Some(position) = done {
            let replay = self.election().log_replay.take().expect("a replay");
            self.stop_replay(replay, ports);
            let election = self.election();
            election.log_position = position;
            election.last_published_append_position = 0;
            if position == election.append_position {
                self.follower_replay_done(ports);
            } else {
                // An incomplete replay.
                self.set_election_state(ElectionState::Canvass, ports);
            }
        }
        Ok(work)
    }

    fn follower_replay_done(&mut self, ports: &mut impl Ports) {
        let state = if self.election().catchup_join_position != NULL_POSITION {
            ElectionState::FollowerCatchupInit
        } else {
            ElectionState::FollowerLogInit
        };
        self.set_election_state(state, ports);
    }

    fn follower_catchup_init(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        if self.log_subscription.is_none() {
            self.file_leaders_term()?;
            self.add_follower_subscription(ports);
            let endpoint = self.members[self.this].catchup_endpoint.clone();
            let destination = create_destination_uri(&self.config.log_channel, &endpoint);
            self.add_log_subscription_destination(destination.clone());
            self.catchup_log_destination = Some(destination);
        }
        let endpoint = &self.members[self.this].catchup_endpoint;
        let catchup_endpoint = match endpoint.strip_suffix(":0") {
            Some(host) => self
                .log_subscription
                .and_then(|s| ports.resolved_endpoint(s))
                .and_then(|resolved| resolved.rfind(':').map(|i| format!("{host}{}", &resolved[i..]))),
            None => Some(endpoint.clone()),
        };
        let election = self.election.as_ref().expect("an election");
        let leader = self.members[election.leader.expect("a leader")].id;
        let message = catchup_endpoint.as_ref().map(|e| {
            consensus_adapter::catchup_position(
                election.leadership_term_id,
                election.log_position,
                self.config.member_id,
                e,
            )
        });
        let sent = match message {
            Some(message) => consensus_adapter::send(ports, leader, &message)?,
            None => false,
        };
        let now_ns = self.now_ns;
        if sent {
            self.election().time_of_last_update_ns = now_ns;
            self.time_of_last_append_position_update_ns = now_ns;
            self.time_of_last_append_position_send_ns = now_ns;
            self.set_election_state(ElectionState::FollowerCatchupAwait, ports);
        } else if now_ns >= self.election().time_of_last_state_change_ns + self.config.leader_heartbeat_timeout_ns {
            return Err(Fault::Error("failed to send catchup position".into()));
        }
        Ok(1)
    }

    /// Java's `addFollowerSubscription`: the follower's log subscription,
    /// its destinations added as the follower catches up and goes live.
    fn add_follower_subscription(&mut self, ports: &mut impl Ports) {
        let session_id = self.election().log_session_id.unwrap_or(0);
        let tags = format!("{},{}", ports.next_correlation_id(), ports.next_correlation_id());
        let log_uri = ChannelUri::parse(&self.config.log_channel).ok();
        let mut channel =
            format!("aeron:udp?tags={tags}|control-mode=manual|session-id={session_id}|group=true|rejoin=false");
        for key in ["so-rcvbuf", "rcv-wnd"] {
            if let Some(value) = log_uri.as_ref().and_then(|u| u.get(key)) {
                channel.push_str(&format!("|{key}={value}"));
            }
        }
        channel.push_str("|alias=log-cm");
        self.log_subscription_channel = channel.clone();
        let subscription = self.new_subscription(channel, self.config.log_stream_id);
        self.log_subscription = Some(subscription);
        self.log_subscription_images.clear();
    }

    fn add_log_subscription_destination(&mut self, destination: String) {
        if let Some(subscription) = self.log_subscription {
            self.actions.push_back(Action::AddDestination {
                subscription,
                destination,
            });
        }
    }

    fn add_live_log_destination(&mut self) {
        let destination = if self.config.is_log_mdc() {
            create_destination_uri(&self.config.log_channel, &self.members[self.this].log_endpoint)
        } else {
            self.config.log_channel.clone()
        };
        self.add_log_subscription_destination(destination.clone());
        self.live_log_destination = Some(destination);
    }

    /// FOLLOWER_CATCHUP_AWAIT and FOLLOWER_LOG_AWAIT: the leader's image on
    /// the follower's subscription joined, recorded and handed to the services.
    fn follower_join_log(&mut self, ports: &mut impl Ports, is_catchup: bool) -> Outcome<usize> {
        let now_ns = self.now_ns;
        let timed_out =
            now_ns >= self.election().time_of_last_state_change_ns + self.config.leader_heartbeat_timeout_ns;
        let election = self.election.as_ref().expect("an election");
        let (log_position, session_id, is_leader_startup) = (
            election.log_position,
            election.log_session_id.unwrap_or(0),
            election.is_leader_startup,
        );
        let state_name = if is_catchup {
            "followerCatchupAwait"
        } else {
            "followerLogAwait"
        };
        match election.join_log {
            Some(JoinLog::AwaitServices) => {
                if !self.services_joined(log_position)? {
                    return Ok(0);
                }
                self.election().join_log = None;
                if is_catchup {
                    self.set_election_state(ElectionState::FollowerCatchup, ports);
                } else {
                    self.update_recording_log()?;
                    self.set_election_state(ElectionState::FollowerReady, ports);
                }
                Ok(1)
            }
            Some(_) | None => {
                let image = self
                    .log_subscription_images
                    .iter()
                    .find(|i| i.session_id == session_id)
                    .copied();
                let Some(image) = image else {
                    if timed_out {
                        let what = if is_catchup { "catchup log" } else { "live log" };
                        return Err(Fault::Error(format!("failed to join {what}")));
                    }
                    return Ok(0);
                };
                if image.join_position != log_position {
                    let inequality = if image.join_position < log_position {
                        " less "
                    } else {
                        " greater "
                    };
                    return Err(Fault::Warning(format!(
                        "{state_name} - joinPosition={}{inequality}than logPosition={log_position}",
                        image.join_position
                    )));
                }
                if self.try_join_log_as_follower(image, is_leader_startup, ports)? {
                    self.election().join_log = Some(JoinLog::AwaitServices);
                    return Ok(1);
                }
                if timed_out {
                    let what = if is_catchup {
                        "catchup log as follower"
                    } else {
                        "live log as follower"
                    };
                    return Err(Fault::Error(format!("failed to join {what}")));
                }
                Ok(0)
            }
        }
    }

    /// Java's `tryJoinLogAsFollower`: the image recorded, the recording's
    /// counter found, the services asked to join it.
    fn try_join_log_as_follower(
        &mut self,
        image: LogImage,
        is_leader_startup: bool,
        ports: &mut impl Ports,
    ) -> Outcome<bool> {
        let subscription = self.log_subscription.expect("the follower's log subscription");
        let channel = self.log_subscription_channel.clone();
        if self.log_subscription_id == NULL_VALUE && !self.requests.values().any(|r| *r == Request::LogRecording) {
            self.start_log_recording(channel.clone(), false, ports)?;
        }
        if !self.try_create_append_position(image.session_id, image.join_position, ports) {
            return Ok(false);
        }
        // A new recording has an id only now.
        self.file_leaders_term()?;
        self.take_log_term_at(image.join_position, ports);
        if let Some(previous) = self.reader.take()
            && let Some(position) = previous.position
        {
            self.log_adapter_position = position;
        }
        self.reader = Some(ImageReader {
            subscription,
            session_id: image.session_id,
            position: Some(image.join_position),
            message_position: Some(image.join_position),
            reassembler: Reassembler::default(),
            closed: false,
            end_of_stream: false,
            term_buffer_length: image.term_buffer_length,
        });
        self.last_append_position = image.join_position;
        self.time_of_last_append_position_update_ns = self.now_ns;
        self.join_services(
            ports,
            &channel,
            self.config.log_stream_id,
            image.session_id,
            image.join_position,
            i64::MAX,
            is_leader_startup,
            Role::Follower,
        )?;
        Ok(true)
    }

    fn follower_catchup(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let election = self.election.as_ref().expect("an election");
        let (notified, catchup_join) = (election.notified_commit_position, election.catchup_join_position);
        let mut work = self.catchup_poll(ports)?;
        if self.live_log_destination.is_none() && self.is_catchup_near_live(catchup_join.max(notified)) {
            self.add_live_log_destination();
            work += 1;
        }
        let position = self.commit_position;
        if position >= catchup_join
            && position >= notified
            && self.catchup_log_destination.is_none()
            && self.state != ModuleState::Snapshot
        {
            let election = self.election();
            election.append_position = position;
            election.log_position = position;
            self.set_election_state(ElectionState::FollowerLogInit, ports);
            work += 1;
        }
        Ok(work)
    }

    /// Java's `catchupPoll`, after the shell polled the log up to
    /// [`ConsensusModule::polls`]' bound: progress reported to the leader.
    fn catchup_poll(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let mut work = 0;
        let now_ns = self.now_ns;
        if self.state == ModuleState::Active || self.state == ModuleState::Suspended {
            let Some(counter) = self.append_counter else {
                return Err(Fault::Warning(format!(
                    "unexpected recording stop during catchup: position={}",
                    self.log_adapter_position()
                )));
            };
            let append = ports.counter_value(counter.counter_id);
            if self.reader.as_ref().is_none_or(|r| r.closed) {
                return Err(Fault::Warning(format!(
                    "unexpected image close during catchup: position={}",
                    self.log_adapter_position()
                )));
            }
            let leader = self.election().leader.expect("a leader");
            let leader = self.members[leader].id;
            let term = self.leadership_term_id;
            work += self.update_follower_position_to(
                leader,
                term,
                append,
                consensus_adapter::APPEND_POSITION_FLAG_CATCHUP,
                ports,
            )?;
            let position = self.log_adapter_position();
            self.propose_commit_position(position, ports);
        }
        if now_ns > self.time_of_last_append_position_update_ns + self.config.leader_heartbeat_timeout_ns
            && self.state == ModuleState::Active
        {
            let append = self
                .append_counter
                .map_or(NULL_POSITION, |c| ports.counter_value(c.counter_id));
            let election = self.election.as_ref().expect("an election");
            return Err(Fault::Warning(format!(
                "no catchup progress: commitPosition={} limitPosition={} lastAppendPosition={} appendPosition={append} logPosition={}",
                self.commit_position,
                election.notified_commit_position,
                self.last_append_position,
                election.log_position
            )));
        }
        Ok(work)
    }

    /// Java's `isCatchupNearLive`: the log's image within a quarter term of `position`.
    fn is_catchup_near_live(&self, position: i64) -> bool {
        self.reader.as_ref().is_some_and(|r| {
            let window = (i64::from(r.term_buffer_length) >> 2).min(LIVE_ADD_MAX_WINDOW);
            r.position.unwrap_or(0) >= position - window
        })
    }

    fn follower_log_init(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        if self.log_subscription.is_none() {
            if self.election().log_session_id.is_some() {
                self.file_leaders_term()?;
                self.add_follower_subscription(ports);
                self.add_live_log_destination();
                self.set_election_state(ElectionState::FollowerLogAwait, ports);
            }
        } else {
            self.set_election_state(ElectionState::FollowerReady, ports);
        }
        Ok(1)
    }

    fn follower_ready(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let election = self.election.as_ref().expect("an election");
        let leader = self.members[election.leader.expect("a leader")].id;
        let message = consensus_adapter::append_position(
            election.leadership_term_id,
            election.log_position,
            self.config.member_id,
            consensus_adapter::APPEND_POSITION_FLAG_NONE,
        );
        if consensus_adapter::send(ports, leader, &message)? {
            self.election_complete(ports)?;
        } else if self.now_ns >= election.time_of_last_state_change_ns + self.config.leader_heartbeat_timeout_ns {
            return Err(Fault::Error("ready follower failed to notify leader".into()));
        }
        Ok(1)
    }

    /// Java's `electionComplete`: the term the node's, the leader known,
    /// clients' ingress connected.
    pub(super) fn election_complete(&mut self, ports: &mut impl Ports) -> Outcome<()> {
        let election = self.election.take().expect("an election");
        ports.set_counter(CounterId::ElectionState, ElectionState::Closed as i64);
        self.set_leadership_term_id(election.leadership_term_id, ports);
        if self.role == Role::Leader {
            self.time_of_last_log_update_ns = self.now_ns - self.config.leader_heartbeat_interval_ns;
            ports.set_control_toggle(toggle::NEUTRAL);
            self.sessions
                .prepare_sessions_for_new_term(election.is_leader_startup, self.now_ns);
        } else {
            self.time_of_last_log_update_ns = self.now_ns;
            self.time_of_last_append_position_update_ns = self.now_ns;
            self.time_of_last_append_position_send_ns = self.now_ns;
        }
        ports.set_node_control_toggle(node_toggle::NEUTRAL);
        self.refresh_recovery_plan(ports)?;
        self.notified_commit_position = self.notified_commit_position.max(election.log_position);
        self.set_commit_position(election.log_position, ports);
        if let Some(leader) = election.leader {
            self.leader = leader;
        }
        let leader_id = self.members[self.leader].id;
        cluster_member::set_is_leader(&mut self.members, leader_id);
        self.ingress_endpoints = cluster_member::ingress_endpoints(&self.members);
        self.connect_ingress();
        Ok(())
    }

    /// Java's `connectIngress`. A multicast ingress reaches every member,
    /// and only the leader takes it: a follower would answer each connect
    /// with a redirect to the leader's own endpoint, where no one listens.
    fn connect_ingress(&mut self) {
        if self.ingress.is_some() {
            return;
        }
        let mut uri = ChannelUri::parse(&self.config.ingress_channel)
            .unwrap_or_else(|_| ChannelUri::parse("aeron:udp").expect("a channel"));
        if uri.get("endpoint").is_none() {
            uri.put("endpoint", &self.members[self.this].ingress_endpoint);
        }
        if self.role != Role::Leader && uri.get("endpoint").is_some_and(is_multicast_endpoint) {
            return;
        }
        uri.put("rejoin", "false");
        uri.put("reliable", "true");
        let subscription = self.new_subscription(uri.to_string(), self.config.ingress_stream_id);
        self.ingress = Some(subscription);
    }

    // ---------------------------------------------------------------- what members say during an election

    pub(super) fn election_on_canvass_position(
        &mut self,
        log_leadership_term_id: i64,
        log_position: i64,
        leadership_term_id: i64,
        follower_member_id: i32,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        let now_ns = self.now_ns;
        let election = self.election();
        if election.state == ElectionState::Init {
            return Ok(());
        }
        if follower_member_id == election.graceful_closed_leader_id {
            election.graceful_closed_leader_id = NULL_VALUE as i32;
        }
        let Some(follower) = self.members.iter().position(|m| m.id == follower_member_id) else {
            return Ok(());
        };
        if follower == self.this {
            return Ok(());
        }
        self.update_member_log_position(follower, log_leadership_term_id, log_position);
        let election = self.election.as_ref().expect("an election");
        if self.role == Role::Leader && leadership_term_id > election.leadership_term_id {
            // Raft: the member voted past this leader's term, and will not follow it.
            return Err(Fault::Warning(format!(
                "potential new election in progress - this.leadershipTermId={} leadershipTermId={leadership_term_id} followerMemberId={follower_member_id}",
                election.leadership_term_id
            )));
        }
        if log_leadership_term_id < election.leadership_term_id {
            if self.role == Role::Leader {
                let append = election.append_position;
                let quorum = self.quorum_position_bounded_by_leader_log(append);
                let timestamp = self.config.time_unit.from_nanos(now_ns);
                self.publish_new_leadership_term(follower, log_leadership_term_id, quorum, timestamp, ports)?;
            }
        } else if log_leadership_term_id > election.leadership_term_id
            && matches!(
                election.state,
                ElectionState::LeaderLogReplication | ElectionState::LeaderReady
            )
        {
            return Err(Fault::Warning("potential new election in progress".into()));
        }
        Ok(())
    }

    pub(super) fn election_on_request_vote(
        &mut self,
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_term_id: i64,
        candidate_id: i32,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        self.refresh_log_term();
        let election = self.election.as_ref().expect("an election");
        if election.state == ElectionState::Init || candidate_id == self.config.member_id {
            return Ok(());
        }
        if candidate_term_id <= election.candidate_term_id {
            return self.place_vote(candidate_term_id, candidate_id, false, ports);
        }
        let ours = cluster_member::compare_log_positions(
            election.log_leadership_term_id,
            election.append_position,
            log_leadership_term_id,
            log_position,
        );
        if ours == std::cmp::Ordering::Greater {
            let candidate = self.propose_max_candidate_term_id(candidate_term_id, log_position, ports)?;
            self.election().candidate_term_id = candidate;
            self.place_vote(candidate_term_id, candidate_id, false, ports)?;
            self.wait_for_a_leader_of_the_term_taken();
            if let Some(member) = self.members.iter().position(|m| m.id == candidate_id)
                && self.role == Role::Leader
            {
                let append = self.election().append_position;
                let quorum = self.quorum_position_bounded_by_leader_log(append);
                let timestamp = self.config.time_unit.from_nanos(self.now_ns);
                self.publish_new_leadership_term(member, log_leadership_term_id, quorum, timestamp, ports)?;
            }
        } else if matches!(
            election.state,
            ElectionState::Canvass
                | ElectionState::Nominate
                | ElectionState::CandidateBallot
                | ElectionState::FollowerBallot
        ) {
            let candidate = self.propose_max_candidate_term_id(candidate_term_id, log_position, ports)?;
            self.election().candidate_term_id = candidate;
            self.place_vote(candidate_term_id, candidate_id, true, ports)?;
            self.set_election_state(ElectionState::FollowerBallot, ports);
        } else if self.role == Role::Leader {
            // Raft: a leader hearing of a candidate of a newer term, with a log
            // as far as its own, steps down.
            return Err(Fault::Warning(format!(
                "potential new election in progress - state={:?} this.candidateTermId={} candidateTermId={candidate_term_id} candidateMemberId={candidate_id}",
                election.state, election.candidate_term_id
            )));
        }
        Ok(())
    }

    /// A newer term refused, and taken, in Canvass or Nominate: the member
    /// canvasses at once with it, and stands no sooner than a canvass round
    /// later, so that a leader of that term, if there is one, answers first
    /// (the leader answers a canvass with its term, and Canvass and Nominate
    /// follow it). Java stands on what it canvassed, after its random delay
    /// alone. A member cut off for a few seconds, its log ahead with entries
    /// the others never had, gets their finished election late once the
    /// links heal, as Aeron's streams resend what they held: refusing their
    /// candidate, it took the term, stood on their stale positions in the
    /// term after, and the leader they had elected lost its term for
    /// nothing. With no leader, nobody answers, and the member with the
    /// longest log stands a canvass round later at most.
    fn wait_for_a_leader_of_the_term_taken(&mut self) {
        let not_before_ns = self.now_ns + self.config.election_status_interval_ns;
        let Some(election) = self.election.as_mut() else {
            return;
        };
        if matches!(election.state, ElectionState::Canvass | ElectionState::Nominate) {
            election.stand_not_before_ns = election.stand_not_before_ns.max(not_before_ns);
            election.nomination_deadline_ns = election.nomination_deadline_ns.max(not_before_ns);
            election.time_of_last_update_ns = election.initial_time_of_last_update_ns;
        }
    }

    pub(super) fn election_on_vote(
        &mut self,
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_term_id: i64,
        candidate_id: i32,
        voter_id: i32,
        vote: bool,
    ) {
        let election = self.election.as_ref().expect("an election");
        if election.state == ElectionState::CandidateBallot
            && candidate_term_id == election.candidate_term_id
            && candidate_id == self.config.member_id
            && let Some(voter) = self.members.iter().position(|m| m.id == voter_id)
        {
            self.members[voter].candidate_term_id = candidate_term_id;
            self.members[voter].vote = Some(vote);
            self.update_member_log_position(voter, log_leadership_term_id, log_position);
        }
    }

    pub(super) fn election_on_new_leadership_term(
        &mut self,
        t: &NewLeadershipTerm,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        self.refresh_log_term();
        let now_ns = self.now_ns;
        let commit_position = self.commit_position;
        let this_id = self.config.member_id;
        let timeout_ns = self.config.leader_heartbeat_timeout_ns;
        let election = self.election();
        if election.state == ElectionState::Init {
            return Ok(());
        }
        let Some(leader) = self.members.iter().position(|m| m.id == t.leader_member_id) else {
            return Ok(());
        };
        let election = self.election.as_mut().expect("an election");
        if t.leader_member_id == this_id && t.leadership_term_id == election.leadership_term_id {
            return Ok(());
        }
        if t.leader_member_id == election.graceful_closed_leader_id {
            election.graceful_closed_leader_id = NULL_VALUE as i32;
        }
        // Raft: a member that voted in a term follows no leader of an older one.
        let followable = t.leadership_term_id >= election.candidate_term_id.max(election.leadership_term_id);
        if followable {
            // Only a leader this member would follow keeps it from standing.
            self.time_of_last_leader_update_ns = now_ns;
        }
        let election = self.election.as_mut().expect("an election");
        if election.leader == Some(leader) && t.leadership_term_id == election.leadership_term_id {
            election.time_of_last_leader_message_ns = now_ns;
        }
        let in_ballot = matches!(
            election.state,
            ElectionState::FollowerBallot | ElectionState::CandidateBallot
        ) && t.leadership_term_id == election.candidate_term_id;
        // Nominate follows as Canvass does, Java only Canvass: a member that
        // has not yet stood in a new term follows a leader of its own, or a
        // newer one, rather than stand against it.
        let canvass = matches!(election.state, ElectionState::Canvass | ElectionState::Nominate)
            && t.leadership_term_id >= election.candidate_term_id;
        if self.role == Role::Leader
            && t.leadership_term_id > election.candidate_term_id.max(election.leadership_term_id)
        {
            // Raft: a leader hearing of a newer term steps down. Java's election
            // leaves one in LEADER_LOG_REPLICATION or LEADER_READY for good. A
            // follower keeps to Java's ways: its leader's silence ends its part.
            return Err(Fault::Warning(format!(
                "new leader detected due to new leadership term - memberId={this_id} state={:?} this.leadershipTermId={} this.candidateTermId={} newLeadershipTermId={} newLeaderMemberId={}",
                election.state,
                election.leadership_term_id,
                election.candidate_term_id,
                t.leadership_term_id,
                t.leader_member_id
            )));
        }
        if in_ballot || canvass {
            let (log_term, append) = (election.log_leadership_term_id, election.append_position);
            if self.holds_nothing_after(t.log_leadership_term_id, append) {
                // Its terms after the leader's are filled in as empty ones:
                // this member's log ends in the leader's term, and they go.
                // Looked for in the recording log, not by the log's term: that
                // is the term of the last entry (`refresh_log_term`), so a term
                // filed last and left empty would stay, and the leader's
                // entries replicated into it be filed under it.
                self.drop_terms_after(t.log_leadership_term_id);
                if t.log_leadership_term_id < log_term {
                    self.election().log_leadership_term_id = t.log_leadership_term_id;
                }
            }
            let election = self.election.as_mut().expect("an election");
            if t.log_leadership_term_id == election.log_leadership_term_id {
                if t.next_term_base_log_position != NULL_POSITION && t.next_term_base_log_position < commit_position {
                    // Raft: committed entries are never given up. A leader
                    // elected by Raft's rules holds them all, so one asking
                    // for some is refused, and the refusal recorded.
                    let message = format!(
                        "refusing to truncate below the commit position - memberId={this_id} state={:?} this.commitPosition={commit_position} this.appendPosition={} leadershipTermId={} leaderMemberId={} nextTermBaseLogPosition={}",
                        election.state,
                        election.append_position,
                        t.leadership_term_id,
                        t.leader_member_id,
                        t.next_term_base_log_position
                    );
                    self.error(message);
                    return Ok(());
                }
                let cut = t.next_term_base_log_position;
                if cut != NULL_POSITION && cut < election.append_position {
                    let (log_term, append) = (election.log_leadership_term_id, election.append_position);
                    let message = format!(
                        "Truncating Cluster Log - memberId={this_id} state={:?} this.logLeadershipTermId={log_term} this.leadershipTermId={} this.candidateTermId={} this.commitPosition={commit_position} this.logPosition={} this.appendPosition={append} oldPosition={append} newPosition={cut}",
                        election.state, election.leadership_term_id, election.candidate_term_id, election.log_position
                    );
                    self.truncate_log_entry(log_term, cut, ports)?;
                    self.election().append_position = cut;
                    return Err(Fault::Warning(message));
                }
                election.leader = Some(leader);
                election.time_of_last_leader_message_ns = now_ns;
                election.is_leader_startup = t.is_startup;
                election.leadership_term_id = t.leadership_term_id;
                election.leader_term_base_log_position = t.term_base_log_position;
                election.candidate_term_id = t.leadership_term_id.max(election.candidate_term_id);
                election.log_session_id = Some(t.log_session_id);
                election.leader_recording_id = t.leader_recording_id;
                election.catchup_join_position = if election.append_position < t.log_position {
                    t.log_position
                } else {
                    NULL_POSITION
                };
                election.notified_commit_position = election.notified_commit_position.max(t.commit_position);
                self.leader = leader;
                let election = self.election();
                if election.append_position < t.term_base_log_position {
                    if t.next_leadership_term_id != NULL_VALUE {
                        if election.append_position < t.next_term_base_log_position {
                            election.replication_leadership_term_id = t.log_leadership_term_id;
                            election.replication_stop_position = t.next_term_base_log_position;
                            // An open, uncommitted term: its base is already known.
                            election.replication_term_base_log_position = NULL_VALUE;
                            self.set_election_state(ElectionState::FollowerLogReplication, ports);
                        } else if election.append_position == t.next_term_base_log_position
                            && t.next_log_position != NULL_POSITION
                        {
                            election.replication_leadership_term_id = t.next_leadership_term_id;
                            election.replication_stop_position = t.next_log_position;
                            election.replication_term_base_log_position = t.next_term_base_log_position;
                            self.set_election_state(ElectionState::FollowerLogReplication, ports);
                        }
                    } else {
                        return Err(Fault::Error(format!(
                            "invalid newLeadershipTerm - this.appendPosition={} < termBaseLogPosition={} and nextLeadershipTermId={}, logLeadershipTermId={}, nextTermBaseLogPosition={}, nextLogPosition={}, leadershipTermId={}, termBaseLogPosition={}, logPosition={}, commitPosition={}, leaderRecordingId={}, leaderMemberId={}, logSessionId={}, isStartup={}",
                            election.append_position,
                            t.term_base_log_position,
                            t.next_leadership_term_id,
                            t.log_leadership_term_id,
                            t.next_term_base_log_position,
                            t.next_log_position,
                            t.leadership_term_id,
                            t.term_base_log_position,
                            t.log_position,
                            t.commit_position,
                            t.leader_recording_id,
                            t.leader_member_id,
                            t.log_session_id,
                            t.is_startup
                        )));
                    }
                } else {
                    self.set_election_state(ElectionState::FollowerReplay, ports);
                }
            } else if t.log_leadership_term_id < election.log_leadership_term_id {
                return self.truncate_terms_the_leader_lacks(t, ports);
            } else {
                self.set_election_state(ElectionState::Canvass, ports);
            }
        }
        let election = self.election();
        if election.state == ElectionState::FollowerLogReplication
            && t.log_leadership_term_id == election.log_leadership_term_id
            && election.leader == Some(leader)
        {
            election.replication_deadline_ns = now_ns + timeout_ns;
        }
        Ok(())
    }

    /// Whether this member's log holds nothing after `leadership_term_id`'s
    /// entries: its later terms, if any, begin where it ends.
    fn holds_nothing_after(&self, leadership_term_id: i64, append_position: i64) -> bool {
        use crate::recording_log::ENTRY_TYPE_TERM;
        self.recording_log
            .entries()
            .iter()
            .find(|e| e.is_valid && e.entry_type == ENTRY_TYPE_TERM && e.leadership_term_id > leadership_term_id)
            .is_none_or(|e| e.term_base_log_position >= append_position)
    }

    /// The module's term made the one the log holds `position` in, as
    /// reading the log from that term's first event would make it. Java reads
    /// a log from where an election left it with whatever term the module
    /// has: one it stood for in an election that failed leaves the cluster
    /// actions it reads unapplied while its services apply them, and their
    /// acks out of sequence for good.
    fn take_log_term_at(&mut self, position: i64, ports: &mut impl Ports) {
        use crate::recording_log::ENTRY_TYPE_TERM;
        let term = self
            .recording_log
            .entries()
            .iter()
            .filter(|e| e.is_valid && e.entry_type == ENTRY_TYPE_TERM && e.term_base_log_position < position)
            .map(|e| e.leadership_term_id)
            .next_back();
        if let Some(term) = term
            && term != self.leadership_term_id
        {
            self.set_leadership_term_id(term, ports);
        }
    }

    /// The election's log term made the term of the log's last entry, as the
    /// recording log files it, for a member not leading: Raft compares logs
    /// by it. Java's election takes the term of the last term event it
    /// replayed, which a replay stopping short of the log's end leaves older
    /// than the log's, and a member then votes for a log shorter in its own
    /// last term. A term filed before its entries arrive counts once they do.
    fn refresh_log_term(&mut self) {
        use crate::recording_log::ENTRY_TYPE_TERM;
        if self.role == Role::Leader {
            return;
        }
        let Some(append) = self.election.as_ref().map(|e| e.append_position) else {
            return;
        };
        let term = self
            .recording_log
            .entries()
            .iter()
            .filter(|e| e.is_valid && e.entry_type == ENTRY_TYPE_TERM && e.term_base_log_position < append)
            .map(|e| e.leadership_term_id)
            .next_back();
        if let Some(term) = term {
            self.election().log_leadership_term_id = term;
        }
    }

    /// The recording log's terms after `leadership_term_id` invalidated.
    fn drop_terms_after(&mut self, leadership_term_id: i64) {
        use crate::recording_log::ENTRY_TYPE_TERM;
        let dropped: Vec<usize> = self
            .recording_log
            .entries()
            .iter()
            .enumerate()
            .filter(|(_, e)| e.is_valid && e.entry_type == ENTRY_TYPE_TERM && e.leadership_term_id > leadership_term_id)
            .map(|(i, _)| i)
            .collect();
        if dropped.is_empty() {
            return;
        }
        for index in dropped.into_iter().rev() {
            self.recording_log.invalidate_entry(index);
        }
        self.flush_recording_log(true);
    }

    /// Raft's log matching, as a member: a leader naming an older term than
    /// this member's last has none of its later terms (`matching_term`), so
    /// none of their entries was committed. The log is cut back to where it
    /// left that older term, and the member canvasses again from there. Java's
    /// leaders name the member's own term whatever theirs hold, so this does
    /// not arise with them.
    fn truncate_terms_the_leader_lacks(&mut self, t: &NewLeadershipTerm, ports: &mut impl Ports) -> Outcome<()> {
        use crate::recording_log::ENTRY_TYPE_TERM;
        let entries = self.recording_log.entries();
        let terms = entries
            .iter()
            .enumerate()
            .filter(|(_, e)| e.is_valid && e.entry_type == ENTRY_TYPE_TERM);
        let mut keep = terms
            .clone()
            .filter(|(_, e)| e.leadership_term_id <= t.log_leadership_term_id)
            .map(|(_, e)| e.leadership_term_id)
            .next_back()
            .unwrap_or(NULL_VALUE);
        let mut dropped: Vec<usize> = terms
            .filter(|(_, e)| e.leadership_term_id > t.log_leadership_term_id)
            .map(|(i, _)| i)
            .collect();
        let Some(&first) = dropped.first() else {
            self.set_election_state(ElectionState::Canvass, ports);
            return Ok(());
        };
        if keep == NULL_VALUE {
            // None to keep: the first term stays, empty from the cut, so the
            // recording log keeps its recording.
            keep = entries[first].leadership_term_id;
            dropped.remove(0);
        }
        let (this_id, commit) = (self.config.member_id, self.commit_position);
        let cut = entries[first].term_base_log_position;
        let election = self.election.as_ref().expect("an election");
        let append = election.append_position;
        let message = format!(
            "{} - memberId={this_id} state={:?} this.logLeadershipTermId={} this.leadershipTermId={} this.candidateTermId={} this.commitPosition={commit} this.logPosition={} this.appendPosition={append} oldPosition={append} newPosition={cut} leaderLogLeadershipTermId={} leaderMemberId={}",
            if cut < commit {
                "refusing to truncate below the commit position"
            } else {
                "Truncating Cluster Log"
            },
            election.state,
            election.log_leadership_term_id,
            election.leadership_term_id,
            election.candidate_term_id,
            election.log_position,
            t.log_leadership_term_id,
            t.leader_member_id
        );
        if cut < commit {
            // A leader elected by Raft's rules holds every committed entry.
            self.error(message);
            return Ok(());
        }
        for index in dropped.into_iter().rev() {
            self.recording_log.invalidate_entry(index);
        }
        if cut < append {
            self.truncate_log_entry(keep, cut, ports)?;
        } else if keep != NULL_VALUE {
            self.commit_log_position(keep, cut)?;
        }
        self.flush_recording_log(true);
        let election = self.election();
        election.append_position = cut.min(append);
        election.log_leadership_term_id = keep;
        Err(Fault::Warning(message))
    }

    /// Java's `truncateLogEntry`: the log's recording cut back to where the
    /// leader's next term began.
    fn truncate_log_entry(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        self.archive_request(
            ports,
            archive::Request::TruncateRecording {
                recording_id: self.log_recording_id,
                position: log_position,
            },
            None,
        )?;
        if leadership_term_id != NULL_VALUE {
            self.commit_log_position(leadership_term_id, log_position)?;
        }
        self.disconnect_log();
        self.log_adapter_position = self.log_adapter_position.min(log_position);
        self.log_recording_stop_position = log_position;
        Ok(())
    }

    pub(super) fn election_on_append_position(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        flags: u8,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        let election = self.election.as_ref().expect("an election");
        if election.state == ElectionState::Init || leadership_term_id > election.leadership_term_id {
            return Ok(());
        }
        if let Some(follower) = self.members.iter().position(|m| m.id == follower_member_id) {
            let leader_term = self.election().leadership_term_id;
            let term = self.reported_term(follower, leadership_term_id, flags, leader_term);
            self.update_member_log_position(follower, term, log_position);
            self.track_catchup_completion(follower, leadership_term_id, flags, ports)?;
        }
        Ok(())
    }

    pub(super) fn election_on_commit_position(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        leader_member_id: i32,
    ) -> Outcome<()> {
        let now_ns = self.now_ns;
        let timeout = self.config.leader_heartbeat_timeout_ns;
        let this_id = self.config.member_id;
        let is_leader = self.role == Role::Leader;
        let leader_id = self
            .election
            .as_ref()
            .and_then(|e| e.leader)
            .map(|i| self.members[i].id);
        let election = self.election();
        if election.state == ElectionState::Init {
            return Ok(());
        }
        // The term is not checked: leaders before a fix sent a commit
        // position of another term than their new term's.
        if leader_id == Some(leader_member_id) {
            election.time_of_last_leader_message_ns = now_ns;
            election.notified_commit_position = election.notified_commit_position.max(log_position);
            if election.state == ElectionState::FollowerLogReplication {
                election.replication_deadline_ns = now_ns + timeout;
            }
        } else if leadership_term_id > election.leadership_term_id && is_leader {
            // Java ends only LEADER_READY so. Raft: a leader hearing of a newer
            // one steps down, in whichever of its states.
            return Err(Fault::Warning(format!(
                "new leader detected due to commit position -  memberId={this_id} this.leadershipTermId={} this.leaderMemberId={} this.logPosition={} newLeadershipTermId={leadership_term_id} newLeaderMemberId={leader_member_id} newCommitPosition={log_position})",
                election.leadership_term_id,
                leader_id.unwrap_or(NULL_VALUE as i32),
                election.log_position
            )));
        }
        Ok(())
    }

    /// A new term replayed or caught up on (Java's
    /// `Election.onReplayNewLeadershipTermEvent`): the recording log made to agree.
    pub(super) fn election_on_replay_new_leadership_term_event(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        timestamp: i64,
        term_base_log_position: i64,
    ) -> Outcome<()> {
        let election = self.election.as_ref().expect("an election");
        if matches!(
            election.state,
            ElectionState::FollowerCatchup | ElectionState::FollowerReplay
        ) {
            let now_ns = self.config.time_unit.to_nanos(timestamp);
            self.ensure_recording_log_coherent(leadership_term_id, term_base_log_position, NULL_VALUE, now_ns)?;
            let election = self.election();
            election.log_position = log_position;
            election.log_leadership_term_id = leadership_term_id;
        }
        Ok(())
    }

    /// SplitMix64 on the configured seed: the nomination delay's draw in [0, 1).
    fn next_random(&mut self) -> f64 {
        self.random_state = self.random_state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.random_state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        (z >> 11) as f64 / (1u64 << 53) as f64
    }
}

impl Prepare {
    fn done() -> Self {
        Self {
            log_position: 0,
            step: PrepareStep::Done,
            counter: None,
            sockets: Vec::new(),
            ran: false,
        }
    }
}

/// Java's `SocketAddressParser.isMulticastAddress`: an IPv4 address in
/// 224.0.0.0/4 or an IPv6 one in ff00::/8, with its port. A name is not one.
fn is_multicast_endpoint(endpoint: &str) -> bool {
    endpoint
        .parse::<std::net::SocketAddr>()
        .is_ok_and(|address| address.ip().is_multicast())
}

/// `ChannelUri.createDestinationUri`: the channel's media to `endpoint`,
/// its interface kept.
pub(super) fn create_destination_uri(channel: &str, endpoint: &str) -> String {
    let uri = ChannelUri::parse(channel).ok();
    let media = if uri.as_ref().is_some_and(|u| !u.is_udp()) {
        "ipc"
    } else {
        "udp"
    };
    let mut destination = format!("aeron:{media}?endpoint={endpoint}");
    if let Some(interface) = uri.as_ref().and_then(|u| u.get("interface")) {
        destination.push_str(&format!("|interface={interface}"));
    }
    destination
}
