//! What members say to each other outside an election, and each one's part
//! once it is over (Java's `ConsensusModuleAgent`): the leader answers a
//! canvass with its term, tracks followers' append positions, replays its
//! log to a follower catching up and collects termination acks; a follower
//! follows the leader's log up to the commit position, reports its append
//! position, and stops where the leader's termination says.

use super::*;

impl ConsensusModule {
    /// A fragment of the consensus stream (Java's `ConsensusAdapter`,
    /// reassembled per image).
    pub fn on_consensus_fragment(&mut self, fragment: Fragment<'_>, session_id: i32, ports: &mut impl Ports) {
        if self.terminated {
            return;
        }
        let mut reassembler = self.consensus_reassemblers.remove(&session_id).unwrap_or_default();
        let mut message = None;
        reassembler.on_fragment(fragment, |m, _| {
            message = Some(m.to_vec());
            PollAction::Continue
        });
        self.consensus_reassemblers.insert(session_id, reassembler);
        if let Some(message) = message {
            let result = match consensus_adapter::decode(&message) {
                Ok(m) => self.consensus_message(m, session_id, ports),
                Err(e) => Err(Fault::Error(e)),
            };
            self.handle(result, ports);
        }
        self.actions.extend(self.sessions.take_actions());
    }

    fn consensus_message(&mut self, message: ConsensusMessage, session_id: i32, ports: &mut impl Ports) -> Outcome<()> {
        match message {
            ConsensusMessage::CanvassPosition {
                log_leadership_term_id,
                log_position,
                leadership_term_id,
                follower_member_id,
                ..
            } => self.on_canvass_position(
                log_leadership_term_id,
                log_position,
                leadership_term_id,
                follower_member_id,
                ports,
            ),
            ConsensusMessage::RequestVote {
                log_leadership_term_id,
                log_position,
                candidate_term_id,
                candidate_member_id,
                ..
            } => {
                if self.election.is_some() {
                    self.election_on_request_vote(
                        log_leadership_term_id,
                        log_position,
                        candidate_term_id,
                        candidate_member_id,
                        ports,
                    )
                } else if candidate_term_id > self.leadership_term_id {
                    let reason = format!(
                        "unexpected vote request: this.leadershipTermId={} candidateTermId={candidate_term_id}",
                        self.leadership_term_id
                    );
                    self.enter_election(false, &reason, ports)
                } else {
                    Ok(())
                }
            }
            ConsensusMessage::Vote {
                candidate_term_id,
                log_leadership_term_id,
                log_position,
                candidate_member_id,
                follower_member_id,
                vote,
            } => {
                if self.election.is_some() {
                    self.election_on_vote(
                        log_leadership_term_id,
                        log_position,
                        candidate_term_id,
                        candidate_member_id,
                        follower_member_id,
                        vote,
                    );
                }
                Ok(())
            }
            ConsensusMessage::NewLeadershipTerm(term) => self.on_new_leadership_term(term, ports),
            ConsensusMessage::AppendPosition {
                leadership_term_id,
                log_position,
                follower_member_id,
                flags,
            } => {
                if self.election.is_some() {
                    self.election_on_append_position(leadership_term_id, log_position, follower_member_id, flags, ports)
                } else if leadership_term_id <= self.leadership_term_id && self.role == Role::Leader {
                    if let Some(follower) = self.members.iter().position(|m| m.id == follower_member_id) {
                        let term = self.reported_term(follower, leadership_term_id, flags, self.leadership_term_id);
                        self.update_member_log_position(follower, term, log_position);
                        self.track_catchup_completion(follower, leadership_term_id, flags, ports)?;
                    }
                    Ok(())
                } else {
                    Ok(())
                }
            }
            ConsensusMessage::CommitPosition {
                leadership_term_id,
                log_position,
                leader_member_id,
            } => self.on_commit_position(leadership_term_id, log_position, leader_member_id, ports),
            ConsensusMessage::CatchupPosition {
                leadership_term_id,
                log_position,
                follower_member_id,
                catchup_endpoint,
            } => self.on_catchup_position(
                leadership_term_id,
                log_position,
                follower_member_id,
                &catchup_endpoint,
                ports,
            ),
            ConsensusMessage::StopCatchup {
                leadership_term_id,
                follower_member_id,
            } => {
                if leadership_term_id == self.leadership_term_id
                    && follower_member_id == self.config.member_id
                    && let Some(destination) = self.catchup_log_destination.take()
                {
                    self.remove_log_destination(destination);
                }
                Ok(())
            }
            ConsensusMessage::TerminationPosition {
                leadership_term_id,
                log_position,
            } => {
                if leadership_term_id == self.leadership_term_id && self.role == Role::Follower {
                    self.termination_position = log_position;
                    self.termination_leadership_term_id = leadership_term_id;
                    self.time_of_last_log_update_ns = self.now_ns;
                }
                Ok(())
            }
            ConsensusMessage::TerminationAck {
                leadership_term_id,
                log_position,
                member_id,
            } => {
                if leadership_term_id == self.leadership_term_id
                    && log_position >= self.termination_position
                    && self.role == Role::Leader
                    && let Some(member) = self.members.iter_mut().find(|m| m.id == member_id)
                {
                    member.has_terminated = true;
                    let now_ns = self.now_ns;
                    if self.termination.as_ref().is_some_and(|t| self.can_terminate(t, now_ns)) {
                        self.commit_log_position(leadership_term_id, self.termination_position)?;
                        self.close_and_terminate(ports);
                    }
                }
                Ok(())
            }
            ConsensusMessage::BackupQuery {
                correlation_id,
                response_stream_id,
                version,
                log_position,
                response_channel,
                encoded_credentials: _,
            } => {
                let log_position = (log_position != NULL_VALUE).then_some(log_position);
                let action = SessionAction::Backup { log_position };
                self.on_backup_action(
                    action,
                    correlation_id,
                    response_stream_id,
                    version,
                    response_channel,
                    session_id,
                );
                Ok(())
            }
            ConsensusMessage::HeartbeatRequest {
                correlation_id,
                response_stream_id,
                response_channel,
                encoded_credentials: _,
            } => {
                let (action, version) = (SessionAction::Heartbeat, session_manager::PROTOCOL_SEMANTIC_VERSION);
                self.on_backup_action(
                    action,
                    correlation_id,
                    response_stream_id,
                    version,
                    response_channel,
                    session_id,
                );
                Ok(())
            }
            ConsensusMessage::StandbySnapshot {
                correlation_id,
                version,
                response_stream_id,
                snapshots,
                response_channel,
                encoded_credentials: _,
            } => {
                let action = SessionAction::StandbySnapshot(snapshots);
                self.on_backup_action(
                    action,
                    correlation_id,
                    response_stream_id,
                    version,
                    response_channel,
                    session_id,
                );
                Ok(())
            }
            // The default authenticator challenges no backup.
            ConsensusMessage::ChallengeResponse { .. } | ConsensusMessage::Ignored => Ok(()),
        }
    }

    /// A member canvassing, with the term it voted in (`leadership_term_id`):
    /// the leader answers with its term, or steps down if that vote is newer.
    fn on_canvass_position(
        &mut self,
        log_leadership_term_id: i64,
        log_position: i64,
        leadership_term_id: i64,
        follower_member_id: i32,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        if self.election.is_some() {
            return self.election_on_canvass_position(
                log_leadership_term_id,
                log_position,
                leadership_term_id,
                follower_member_id,
                ports,
            );
        }
        if self.role != Role::Leader {
            return Ok(());
        }
        let Some(follower) = self.members.iter().position(|m| m.id == follower_member_id) else {
            return Ok(());
        };
        if leadership_term_id > self.leadership_term_id {
            // Raft: the member voted past this leader's term, and follows it no more.
            let reason = format!(
                "unexpected canvass position: this.leadershipTermId={} leadershipTermId={leadership_term_id}",
                self.leadership_term_id
            );
            return self.enter_election(false, &reason, ports);
        }
        if log_leadership_term_id > self.leadership_term_id {
            return Ok(());
        }
        // Counted towards the quorum only as far as its log is this leader's
        // (`matched_position`).
        self.update_member_log_position(follower, log_leadership_term_id, log_position);
        self.stop_existing_catchup_replay(follower, ports);
        let base = self
            .recording_log
            .get_term_entry(self.leadership_term_id)
            .map_err(|e| e.to_string())?
            .term_base_log_position;
        let (log_leadership_term_id, next) = self.matching_term(log_leadership_term_id);
        let (next_term, next_base, next_position) = if log_leadership_term_id < self.leadership_term_id {
            (
                next.as_ref().map_or(self.leadership_term_id, |e| e.leadership_term_id),
                next.as_ref().map_or(base, |e| e.term_base_log_position),
                next.as_ref().map_or(NULL_POSITION, |e| e.log_position),
            )
        } else {
            (NULL_VALUE, NULL_POSITION, NULL_POSITION)
        };
        let Some(log_session_id) = self.log_session_id else {
            return Err(Fault::Error("logSessionId was null, should always have a value".into()));
        };
        let term = NewLeadershipTerm {
            log_leadership_term_id,
            next_leadership_term_id: next_term,
            next_term_base_log_position: next_base,
            next_log_position: next_position,
            leadership_term_id: self.leadership_term_id,
            term_base_log_position: base,
            log_position: ports.log_position(),
            commit_position: self.commit_position,
            leader_recording_id: self.log_recording_id,
            timestamp: self.cluster_time,
            leader_member_id: self.config.member_id,
            log_session_id,
            app_version: self.config.app_version,
            is_startup: false,
        };
        consensus_adapter::send(
            ports,
            follower_member_id,
            &consensus_adapter::new_leadership_term(&term),
        )?;
        Ok(())
    }

    fn on_new_leadership_term(&mut self, t: NewLeadershipTerm, ports: &mut impl Ports) -> Outcome<()> {
        if (t.app_version >> 16) != (self.config.app_version >> 16) {
            return Err(Fault::Terminate(format!(
                "incompatible version: {} log={}",
                crate::session_manager::semantic_version_string(self.config.app_version),
                crate::session_manager::semantic_version_string(t.app_version)
            )));
        }
        let now_ns = self.now_ns;
        if self.election.is_some() {
            return self.election_on_new_leadership_term(&t, ports);
        }
        if t.leadership_term_id >= self.leadership_term_id {
            self.time_of_last_leader_update_ns = now_ns;
        }
        if self.role == Role::Follower
            && t.leadership_term_id == self.leadership_term_id
            && t.leader_member_id == self.members[self.leader].id
        {
            self.notified_commit_position = self.notified_commit_position.max(t.commit_position);
            self.time_of_last_log_update_ns = now_ns;
        } else if t.leadership_term_id > self.leadership_term_id {
            let reason = format!(
                "unexpected new leadership term event: this.leadershipTermId={} newLeadershipTermId={}",
                self.leadership_term_id, t.leadership_term_id
            );
            self.enter_election(false, &reason, ports)?;
        }
        Ok(())
    }

    fn on_commit_position(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        leader_member_id: i32,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        let now_ns = self.now_ns;
        // Only a leader this member would follow keeps it from standing: not
        // one of an older term than its vote.
        let followable = self.election.as_ref().map_or(self.leadership_term_id, |e| {
            e.candidate_term_id.max(self.leadership_term_id)
        });
        if leadership_term_id >= followable {
            self.time_of_last_leader_update_ns = now_ns;
        }
        if self.election.is_some() {
            return self.election_on_commit_position(leadership_term_id, log_position, leader_member_id);
        }
        if leadership_term_id == self.leadership_term_id {
            if self.members[self.leader].id == leader_member_id && self.role == Role::Follower {
                self.notified_commit_position = self.notified_commit_position.max(log_position);
                self.time_of_last_log_update_ns = now_ns;
            }
        } else if leadership_term_id > self.leadership_term_id {
            let append = self
                .append_counter
                .map_or(NULL_POSITION, |c| ports.counter_value(c.counter_id));
            let reason = format!(
                "unexpected commit position from new leader -  memberId={} this.leadershipTermId={} this.leaderMemberId={} this.commitPosition={} this.appendPosition={append} newLeadershipTermId={leadership_term_id} newLeaderMemberId={leader_member_id} newCommitPosition={log_position})",
                self.config.member_id, self.leadership_term_id, self.members[self.leader].id, self.commit_position
            );
            self.enter_election(false, &reason, ports)?;
        }
        Ok(())
    }

    /// A follower catching up asks for the log from `log_position`: the
    /// leader's archive replays it to the follower's catch-up endpoint.
    fn on_catchup_position(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        catchup_endpoint: &str,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        if leadership_term_id > self.leadership_term_id || self.role != Role::Leader {
            return Ok(());
        }
        let Some(follower) = self.members.iter().position(|m| m.id == follower_member_id) else {
            return Ok(());
        };
        let member = &self.members[follower];
        // A replay asked for and not answered yet counts as one under way.
        if member.catchup_replay_session_id != NULL_VALUE || member.catchup_replay_correlation_id != NULL_VALUE {
            return Ok(());
        }
        let mut uri = ChannelUri::parse(&self.config.follower_catchup_channel)
            .unwrap_or_else(|_| ChannelUri::parse("aeron:udp").expect("a channel"));
        uri.put("endpoint", catchup_endpoint);
        uri.put("session-id", &self.log_session_id.unwrap_or(0).to_string());
        uri.put("linger", "0");
        uri.put("eos", "false");
        uri.put("alias", &format!("catchup-followerId-{follower_member_id}"));
        let request = archive::Request::Replay {
            recording_id: self.log_recording_id,
            position: log_position,
            length: i64::MAX,
            channel: uri.to_string(),
            stream_id: self.config.log_stream_id,
        };
        let correlation_id = self.archive_request(
            ports,
            request,
            Some(Request::CatchupReplay {
                member_id: follower_member_id,
            }),
        )?;
        self.members[follower].catchup_replay_correlation_id = correlation_id;
        Ok(())
    }

    /// The archive started a follower's catch-up replay: its session, or
    /// stopped at once when the catch-up was given up meanwhile.
    pub(super) fn on_catchup_replay_started(
        &mut self,
        member_id: i32,
        correlation_id: i64,
        replay_session_id: i64,
        ports: &mut impl Ports,
    ) {
        match self.members.iter_mut().find(|m| m.id == member_id) {
            Some(member)
                if member.catchup_replay_correlation_id == correlation_id
                    && member.catchup_replay_session_id == NULL_VALUE =>
            {
                member.catchup_replay_session_id = replay_session_id;
            }
            _ => {
                let _ = self.archive_request(
                    ports,
                    archive::Request::StopReplay { replay_session_id },
                    Some(Request::TryStop),
                );
            }
        }
    }

    /// The archive refused a follower's catch-up replay: the follower asks again.
    pub(super) fn on_catchup_replay_failed(&mut self, member_id: i32, correlation_id: i64, message: &str) {
        if let Some(member) = self.members.iter_mut().find(|m| m.id == member_id)
            && member.catchup_replay_correlation_id == correlation_id
        {
            member.catchup_replay_session_id = NULL_VALUE;
            member.catchup_replay_correlation_id = NULL_VALUE;
        }
        self.warn(format!("catchup replay failed - {message}"));
    }

    /// The term a follower's append position is of. One catching up from
    /// this leader's replay holds the leader's log, but keeps its old term
    /// until it reads the new term's first event, which it reads only once
    /// the leader commits past it: its position is taken as of the leader's
    /// term, or neither would move.
    pub(super) fn reported_term(&self, follower: usize, leadership_term_id: i64, flags: u8, leader_term: i64) -> i64 {
        if flags & consensus_adapter::APPEND_POSITION_FLAG_CATCHUP != 0
            && self.members[follower].catchup_replay_session_id != NULL_VALUE
        {
            leader_term
        } else {
            leadership_term_id
        }
    }

    pub(super) fn update_member_log_position(&mut self, member: usize, leadership_term_id: i64, log_position: i64) {
        let now_ns = self.now_ns;
        let member = &mut self.members[member];
        member.leadership_term_id = leadership_term_id;
        member.log_position = log_position;
        member.time_of_last_append_position_ns = now_ns;
    }

    /// Java's `trackCatchupCompletion`: a follower caught up with the
    /// leader's log has its replay stopped and is told so.
    pub(super) fn track_catchup_completion(
        &mut self,
        follower: usize,
        leadership_term_id: i64,
        flags: u8,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        let member = &self.members[follower];
        let catching_up = member.catchup_replay_session_id != NULL_VALUE
            || flags & consensus_adapter::APPEND_POSITION_FLAG_CATCHUP != 0;
        if !catching_up || member.log_position < ports.log_position() {
            return Ok(());
        }
        if member.catchup_replay_correlation_id != NULL_VALUE {
            let replay_session_id = member.catchup_replay_session_id;
            if replay_session_id != NULL_VALUE {
                self.archive_request(
                    ports,
                    archive::Request::StopReplay { replay_session_id },
                    Some(Request::TryStop),
                )?;
            }
            self.members[follower].catchup_replay_correlation_id = NULL_VALUE;
        }
        let id = self.members[follower].id;
        if consensus_adapter::send(ports, id, &consensus_adapter::stop_catchup(leadership_term_id, id))? {
            self.members[follower].catchup_replay_session_id = NULL_VALUE;
        }
        Ok(())
    }

    /// Java's `stopExistingCatchupReplay`: a follower canvassing again
    /// starts its catch-up over.
    fn stop_existing_catchup_replay(&mut self, follower: usize, ports: &mut impl Ports) {
        let replay_session_id = self.members[follower].catchup_replay_session_id;
        if replay_session_id != NULL_VALUE
            && self
                .archive_request(
                    ports,
                    archive::Request::StopReplay { replay_session_id },
                    Some(Request::TryStop),
                )
                .is_ok()
        {
            let member = &mut self.members[follower];
            member.catchup_replay_session_id = NULL_VALUE;
            member.catchup_replay_correlation_id = NULL_VALUE;
        }
    }

    /// Java's `stopAllCatchups`, as an election starts again.
    pub(super) fn stop_all_catchups(&mut self, ports: &mut impl Ports) {
        for i in 0..self.members.len() {
            let member = &self.members[i];
            let (session, correlation) = (member.catchup_replay_session_id, member.catchup_replay_correlation_id);
            if session == NULL_VALUE && correlation == NULL_VALUE {
                continue;
            }
            if session != NULL_VALUE && correlation != NULL_VALUE {
                let _ = self.archive_request(
                    ports,
                    archive::Request::StopReplay {
                        replay_session_id: session,
                    },
                    Some(Request::TryStop),
                );
            }
            let member = &mut self.members[i];
            member.catchup_replay_session_id = NULL_VALUE;
            member.catchup_replay_correlation_id = NULL_VALUE;
        }
        if let Some(election) = self.election.as_mut() {
            election.catchup_join_position = NULL_POSITION;
        }
    }

    /// The commit position to every other member (Java's `publishCommitPosition`).
    pub(super) fn publish_commit_position(
        &mut self,
        commit_position: i64,
        leadership_term_id: i64,
        ports: &mut impl Ports,
    ) -> Outcome<()> {
        let message = consensus_adapter::commit_position(leadership_term_id, commit_position, self.config.member_id);
        for i in 0..self.members.len() {
            if i != self.this {
                let id = self.members[i].id;
                consensus_adapter::send(ports, id, &message)?;
            }
        }
        Ok(())
    }

    /// A follower's work once the election is over (Java's
    /// `consensusWork`): the log followed up to the commit position the
    /// leader notified, the termination position acted on, the append
    /// position reported.
    pub(super) fn follower_work(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let mut work = 0;
        if self.state == ModuleState::Active || self.state == ModuleState::Suspended {
            let position = self.log_adapter_position();
            if self.termination_position != NULL_POSITION && position >= self.termination_position {
                self.set_state(ModuleState::Terminating, ports);
                if self.config.service_count > 0 {
                    self.send_termination_position(self.termination_position, true, ports);
                } else {
                    self.do_termination(position, ports);
                }
            } else {
                if self.reader.as_ref().is_none_or(|r| r.closed) {
                    let eos = self.reader.as_ref().is_some_and(|r| r.end_of_stream);
                    self.enter_election(eos, &format!("log disconnected from leader: eos={eos}"), ports)?;
                    return Ok(1);
                }
                if self.propose_commit_position(position, ports) {
                    work += 1;
                }
            }
        }
        work += self.update_follower_position(ports)?;
        Ok(work)
    }

    fn update_follower_position(&mut self, ports: &mut impl Ports) -> Outcome<usize> {
        let recorded = self
            .append_counter
            .map_or(self.log_recording_stop_position, |c| ports.counter_value(c.counter_id));
        let leader = self.members[self.leader].id;
        let term = self.leadership_term_id;
        self.update_follower_position_to(
            leader,
            term,
            recorded,
            consensus_adapter::APPEND_POSITION_FLAG_NONE,
            ports,
        )
    }

    /// The append position to the leader when it moved, or on the heartbeat
    /// interval (Java's `updateFollowerPosition`).
    pub(super) fn update_follower_position_to(
        &mut self,
        leader_id: i32,
        leadership_term_id: i64,
        append_position: i64,
        flags: u8,
        ports: &mut impl Ports,
    ) -> Outcome<usize> {
        let now_ns = self.now_ns;
        let position = append_position.max(self.last_append_position);
        if position > self.last_append_position
            || now_ns >= self.time_of_last_append_position_send_ns + self.config.leader_heartbeat_interval_ns
        {
            let message =
                consensus_adapter::append_position(leadership_term_id, position, self.config.member_id, flags);
            if consensus_adapter::send(ports, leader_id, &message)? {
                if position > self.last_append_position {
                    self.last_append_position = position;
                    self.time_of_last_append_position_update_ns = now_ns;
                }
                self.time_of_last_append_position_send_ns = now_ns;
                return Ok(1);
            }
        }
        Ok(0)
    }

    /// The termination position to every other member, their acks awaited
    /// (Java's `ClusterTermination.terminationPosition`).
    pub(super) fn send_member_termination_positions(&mut self, position: i64, ports: &mut impl Ports) -> Outcome<()> {
        let message = consensus_adapter::termination_position(self.leadership_term_id, position);
        for i in 0..self.members.len() {
            self.members[i].has_terminated = false;
            if i != self.this {
                let id = self.members[i].id;
                if !consensus_adapter::send(ports, id, &message)? {
                    self.warn(format!("failed to send termination position to member={id}"));
                }
            }
        }
        Ok(())
    }

    /// A backup's or standby's request (Java's `onBackupQuery`,
    /// `onHeartbeatRequest`, `onStandbySnapshot`): taken only outside an
    /// election, active, suspended or taking a snapshot, whatever the role.
    /// Without an egress channel, the response channel is used as sent.
    fn on_backup_action(
        &mut self,
        action: SessionAction,
        correlation_id: i64,
        response_stream_id: i32,
        version: i32,
        response_channel: String,
        session_id: i32,
    ) {
        if self.election.is_none()
            && matches!(
                self.state,
                ModuleState::Active | ModuleState::Suspended | ModuleState::Snapshot
            )
        {
            let source = self
                .consensus_sources
                .get(&session_id)
                .cloned()
                .unwrap_or_else(|| format!("sourceIdentity= sessionId={session_id}"));
            let now_ns = self.now_ns;
            self.sessions.on_backup_action(
                action,
                correlation_id,
                response_stream_id,
                version,
                response_channel,
                &source,
                now_ns,
            );
        }
    }

    /// Backups' and standbys' sessions answered (Java's
    /// `processPendingBackupSessions`): a backup told this member's log
    /// recording and last term, its commit position's counter, the leader,
    /// the cluster, and the snapshots to start from at or before the position
    /// it asked for.
    pub(super) fn process_backup_sessions(&mut self, ports: &mut impl Ports) -> usize {
        let last_term = self
            .recording_log
            .find_last_term()
            .map(|e| (e.recording_id, e.leadership_term_id, e.term_base_log_position));
        let commit_position_counter_id = ports.counter_id(CounterId::CommitPosition);
        let leader_member_id = self.members[self.leader].id;
        let member_id = self.config.member_id;
        let cluster_members = cluster_member::encode_as_string(&self.members);
        let service_count = self.config.service_count;
        let recording_log = &self.recording_log;
        let response = |correlation_id, log_position: Option<i64>| {
            let (log_recording_id, leadership_term_id, term_base_log_position) = last_term?;
            let snapshots = recording_log
                .find_snapshot_at_or_before_or_lowest(log_position.unwrap_or(i64::MAX), service_count)
                .into_iter()
                .map(|s| BackupSnapshot {
                    recording_id: s.recording_id,
                    leadership_term_id: s.leadership_term_id,
                    term_base_log_position: s.term_base_log_position,
                    log_position: s.log_position,
                    timestamp: s.timestamp,
                    service_id: s.service_id,
                })
                .collect();
            Some(consensus_adapter::backup_response(&BackupResponse {
                correlation_id,
                log_recording_id,
                log_leadership_term_id: leadership_term_id,
                log_term_base_log_position: term_base_log_position,
                last_leadership_term_id: leadership_term_id,
                last_term_base_log_position: term_base_log_position,
                commit_position_counter_id,
                leader_member_id,
                member_id,
                snapshots,
                cluster_members: cluster_members.clone(),
            }))
        };
        let now_ns = self.now_ns;
        let work = self.sessions.process_pending_backup_sessions(now_ns, ports, response);
        self.actions.extend(self.sessions.take_actions());
        work
    }

    /// Standbys' notices whose snapshots are committed, filed in the
    /// recording log (Java's `processPendingStandbySnapshotNotifications`),
    /// every slow tick, whatever the role. A notice the log refuses is
    /// dropped with its error; Java retries it every tick, appending its
    /// entries again.
    pub(super) fn process_standby_snapshot_notifications(&mut self, ports: &mut impl Ports) -> usize {
        let due = self
            .sessions
            .due_standby_snapshot_notifications(self.commit_position, self.now_ns);
        let work = due.len();
        for entries in due {
            let filed = entries.iter().try_for_each(|e| {
                self.recording_log.append_standby_snapshot(
                    e.recording_id,
                    e.leadership_term_id,
                    e.term_base_log_position,
                    e.log_position,
                    e.timestamp,
                    e.service_id,
                    &e.archive_endpoint,
                )
            });
            match filed {
                Ok(()) => {
                    self.standby_snapshots_received += 1;
                    ports.set_counter(CounterId::StandbySnapshotCount, self.standby_snapshots_received);
                }
                Err(e) => self.error(format!("standby snapshot notification: {e}")),
            }
        }
        if work > 0 {
            self.flush_recording_log(false);
        }
        work
    }
}
