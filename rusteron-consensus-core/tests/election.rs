//! Elections among three members, the module under test one of them and
//! the test the other two, on the consensus stream and the leader's log:
//! the scenarios of Java's `ElectionTest` and `ConsensusModuleAgentTest`
//! a plain cluster runs into.

use rusteron_archive_core::protocol::{Request, Signal};
use rusteron_cluster_core::recording_log::{ENTRY_TYPE_TERM, RecordingLog};
use rusteron_consensus_core::config::Config;
use rusteron_consensus_core::consensus::{self, APPEND_POSITION_FLAG_CATCHUP, ConsensusMessage, NewLeadershipTerm};
use rusteron_consensus_core::io::CounterId;
use rusteron_consensus_core::log::{self, LogMessage};
use rusteron_consensus_core::module::{ElectionState, ModuleState, Role, toggle};
use rusteron_consensus_core::testkit::{FakeNode, LOG_SESSION_ID};

const MEMBERS: &str = "0,localhost:20110,localhost:20111,localhost:20112,localhost:20113,localhost:20114|\
                       1,localhost:20210,localhost:20211,localhost:20212,localhost:20213,localhost:20214|\
                       2,localhost:20310,localhost:20311,localhost:20312,localhost:20313,localhost:20314";
/// The leader's log session, when the leader is the test's.
const LEADER_SESSION: i32 = 77;

fn config(member_id: i32) -> Config {
    Config {
        member_id,
        cluster_members: MEMBERS.into(),
        ingress_channel: "aeron:udp?term-length=64k".into(),
        log_channel: "aeron:udp?term-length=64k".into(),
        replication_channel: "aeron:udp?endpoint=localhost:0".into(),
        startup_canvass_timeout_ns: 5_000_000_000,
        ..Config::default()
    }
}

/// A member started, canvassing.
fn canvassing(member_id: i32) -> FakeNode {
    canvassing_with(config(member_id))
}

fn canvassing_with(config: Config) -> FakeNode {
    let mut node = FakeNode::new(config);
    node.run(20);
    assert_eq!(node.module.state(), ModuleState::Active, "errors: {:?}", node.errors);
    assert_eq!(node.module.election_state(), Some(ElectionState::Canvass));
    node
}

fn sent(node: &mut FakeNode) -> Vec<(i32, ConsensusMessage)> {
    node.ports.take_consensus()
}

fn state(node: &FakeNode) -> Option<ElectionState> {
    node.module.election_state()
}

/// Member 0 elected leader of term 0 by the votes of 1 and 2, and both
/// following it from position 0.
fn leader() -> FakeNode {
    leader_with(config(0))
}

fn leader_with(config: Config) -> FakeNode {
    let mut node = canvassing_with(config);
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 1));
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 2));
    node.run(1);
    assert_eq!(state(&node), Some(ElectionState::Nominate));
    node.advance(500);
    assert_eq!(state(&node), Some(ElectionState::CandidateBallot));
    node.consensus_in(&consensus::vote(0, -1, 0, 0, 1, true));
    node.consensus_in(&consensus::vote(0, -1, 0, 0, 2, true));
    node.run(10);
    assert_eq!(
        state(&node),
        Some(ElectionState::LeaderReady),
        "errors: {:?}",
        node.errors
    );
    node.consensus_in(&consensus::append_position(0, 0, 1, 0));
    node.consensus_in(&consensus::append_position(0, 0, 2, 0));
    node.run(2);
    assert_eq!(state(&node), None, "errors: {:?}", node.errors);
    node
}

#[test]
fn a_member_canvasses_the_others_with_its_log() {
    let mut node = canvassing(0);
    assert_eq!(node.consensus_publications, [1, 2]);
    let canvass = ConsensusMessage::CanvassPosition {
        log_leadership_term_id: -1,
        log_position: 0,
        leadership_term_id: -1,
        follower_member_id: 0,
        protocol_version: consensus::PROTOCOL_SEMANTIC_VERSION,
    };
    let messages = sent(&mut node);
    assert!(messages.contains(&(1, canvass.clone())), "{messages:?}");
    assert!(messages.contains(&(2, canvass)), "{messages:?}");
    assert_eq!(node.module.role(), Role::Follower);
    assert_eq!(
        node.ports.counter(CounterId::ElectionState),
        ElectionState::Canvass as i64
    );
}

/// Java's election polls the archive too: a member whose archive died ends
/// mid-election, as a leader does in its term.
#[test]
fn a_member_whose_archive_is_gone_ends_mid_election() {
    let mut node = canvassing(0);
    node.ports.archive_gone = true;
    node.run(20);
    assert_eq!(node.terminated, Some(false), "errors: {:?}", node.errors);
    assert_eq!(node.module.state(), ModuleState::Closed);
    assert_eq!(node.errors, ["local archive is not connected"]);
}

#[test]
fn a_candidate_with_every_vote_leads_and_publishes_its_term() {
    let mut node = canvassing(0);
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 1));
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 2));
    node.run(1);
    assert_eq!(
        state(&node),
        Some(ElectionState::Nominate),
        "unanimous: no canvass timeout"
    );
    sent(&mut node);
    node.advance(500);
    assert_eq!(state(&node), Some(ElectionState::CandidateBallot));
    assert_eq!(node.module.role(), Role::Candidate);
    node.run(1);
    let request = ConsensusMessage::RequestVote {
        log_leadership_term_id: -1,
        log_position: 0,
        candidate_term_id: 0,
        candidate_member_id: 0,
        protocol_version: consensus::PROTOCOL_SEMANTIC_VERSION,
    };
    let messages = sent(&mut node);
    assert!(
        messages.contains(&(1, request.clone())) && messages.contains(&(2, request)),
        "{messages:?}"
    );

    node.consensus_in(&consensus::vote(0, -1, 0, 0, 1, true));
    node.consensus_in(&consensus::vote(0, -1, 0, 0, 2, true));
    node.run(1);
    assert_eq!(node.module.role(), Role::Leader);
    assert_eq!(
        node.log_destinations,
        [
            "aeron:udp?endpoint=localhost:20212",
            "aeron:udp?endpoint=localhost:20312"
        ]
    );
    node.run(10);
    let terms: Vec<_> = sent(&mut node)
        .into_iter()
        .filter_map(|(to, m)| match m {
            ConsensusMessage::NewLeadershipTerm(t) => Some((to, t)),
            _ => None,
        })
        .collect();
    let (_, term) = terms.iter().find(|(to, _)| *to == 1).expect("a new term to member 1");
    assert_eq!(
        (
            term.leadership_term_id,
            term.log_position,
            term.leader_member_id,
            term.log_session_id
        ),
        (0, 0, 0, LOG_SESSION_ID)
    );
    assert_eq!(state(&node), Some(ElectionState::LeaderReady));

    // The term is the cluster's once a quorum follows it.
    node.consensus_in(&consensus::append_position(0, 0, 1, 0));
    node.run(2);
    assert_eq!(state(&node), None, "errors: {:?}", node.errors);
    assert!(matches!(
        node.ports.log_messages().first(),
        Some((
            LogMessage::NewLeadershipTerm {
                leadership_term_id: 0,
                ..
            },
            96
        ))
    ));
    assert_eq!(node.ports.toggle, toggle::NEUTRAL);
}

#[test]
fn a_canvass_without_a_unanimous_view_times_out_with_a_majority() {
    let mut node = canvassing(0);
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 1));
    node.run(10);
    assert_eq!(state(&node), Some(ElectionState::Canvass), "member 2 unheard of");
    node.advance(5_000);
    assert_eq!(state(&node), Some(ElectionState::Nominate));
}

#[test]
fn a_member_votes_for_a_candidate_whose_log_is_no_shorter() {
    let mut node = canvassing(1);
    sent(&mut node);
    node.ports.candidate_term_on_disk_at_offer.clear();
    node.consensus_in(&consensus::request_vote(-1, 0, 0, 0));
    assert_eq!(
        node.ports.candidate_term_on_disk_at_offer,
        [0],
        "the vote's term on disk before the vote went out"
    );
    assert_eq!(
        sent(&mut node),
        [(
            0,
            ConsensusMessage::Vote {
                candidate_term_id: 0,
                log_leadership_term_id: -1,
                log_position: 0,
                candidate_member_id: 0,
                follower_member_id: 1,
                vote: true,
            }
        )]
    );
    assert_eq!(state(&node), Some(ElectionState::FollowerBallot));
    let node_state =
        rusteron_cluster_core::node_state::NodeState::load(&node.ports.node_state_file).expect("node state");
    assert_eq!(
        node_state.candidate_term_id, 0,
        "the vote persisted before it is acted on"
    );

    // A candidate of a term already voted in is refused.
    node.consensus_in(&consensus::request_vote(-1, 0, 0, 2));
    assert!(matches!(
        sent(&mut node).as_slice(),
        [(2, ConsensusMessage::Vote { vote: false, .. })]
    ));

    // No leader in the election timeout: canvass again.
    node.advance(1_000);
    assert_eq!(state(&node), Some(ElectionState::Canvass));
}

#[test]
fn a_member_with_a_longer_log_refuses_its_vote() {
    let mut node = canvassing(1);
    sent(&mut node);
    // The candidate's log ends in an earlier term.
    node.consensus_in(&consensus::request_vote(-2, 0, 0, 0));
    assert!(matches!(
        sent(&mut node).as_slice(),
        [(0, ConsensusMessage::Vote { vote: false, .. })]
    ));
    assert_eq!(state(&node), Some(ElectionState::Canvass));
}

/// A member about to stand refuses a candidate of a newer term, its log
/// further, and takes the term: it canvasses with that term at once and
/// stands no sooner than a canvass round later, so a leader of that term
/// answers first, and it follows. A member cut off with a longer log gets
/// the others' finished election late once the links heal (Aeron's streams
/// resend what they held), and stood on their stale positions in the term
/// after the one they had elected a leader in.
#[test]
fn a_member_refusing_a_newer_term_follows_its_leader_rather_than_stand() {
    let mut node = nominating_then_refusing_term_0();
    node.consensus_in(&consensus::new_leadership_term(&new_term(-1, 0, 0, 0)));
    node.run(3);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerLogAwait),
        "errors: {:?}",
        node.errors
    );
}

/// With no leader to answer, it stands a canvass round later, past the
/// term it took.
#[test]
fn a_member_refusing_a_newer_term_stands_a_canvass_round_later() {
    let mut node = nominating_then_refusing_term_0();
    node.advance(50);
    assert_eq!(
        state(&node),
        Some(ElectionState::Nominate),
        "not within a canvass round"
    );
    node.advance(1_000);
    assert_eq!(
        state(&node),
        Some(ElectionState::CandidateBallot),
        "errors: {:?}",
        node.errors
    );
    node.run(1);
    assert!(
        sent(&mut node).iter().any(|(_, m)| matches!(
            m,
            ConsensusMessage::RequestVote {
                candidate_term_id: 1,
                ..
            }
        )),
        "stands in term 1"
    );
}

/// Member 1 in Nominate on the others' positions, then refusing member 2,
/// a candidate of term 0 whose log ends in an earlier term.
fn nominating_then_refusing_term_0() -> FakeNode {
    let mut node = canvassing(1);
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 0));
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 2));
    node.run(1);
    assert_eq!(state(&node), Some(ElectionState::Nominate));
    sent(&mut node);
    node.consensus_in(&consensus::request_vote(-2, 0, 0, 2));
    node.run(2);
    let messages = sent(&mut node);
    assert!(
        messages
            .iter()
            .any(|(to, m)| *to == 2 && matches!(m, ConsensusMessage::Vote { vote: false, .. })),
        "refused: {messages:?}"
    );
    assert!(
        messages.iter().any(|(_, m)| matches!(
            m,
            ConsensusMessage::CanvassPosition {
                leadership_term_id: 0,
                ..
            }
        )),
        "canvasses with the term taken: {messages:?}"
    );
    assert_eq!(state(&node), Some(ElectionState::Nominate));
    node
}

/// A member about to stand follows a leader of its candidate term or a
/// newer one, as a canvassing member does, rather than stand against it.
#[test]
fn a_member_nominating_follows_a_leader_of_its_term() {
    let mut node = canvassing(1);
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 0));
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 2));
    node.run(1);
    assert_eq!(state(&node), Some(ElectionState::Nominate));
    node.consensus_in(&consensus::new_leadership_term(&new_term(-1, 0, 0, 0)));
    node.run(3);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerLogAwait),
        "errors: {:?}",
        node.errors
    );
}

#[test]
fn a_candidate_without_a_majority_canvasses_again() {
    let mut node = canvassing(0);
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 1));
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 2));
    node.run(1).advance(500);
    assert_eq!(state(&node), Some(ElectionState::CandidateBallot));
    node.consensus_in(&consensus::vote(0, -1, 0, 0, 1, false));
    node.advance(1_000);
    assert_eq!(state(&node), Some(ElectionState::Canvass));
    assert_eq!(node.module.role(), Role::Follower);
}

#[test]
fn only_the_appointed_leader_stands() {
    let mut node = canvassing_with(Config {
        appointed_leader_id: 1,
        ..config(0)
    });
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 1));
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 2));
    node.advance(6_000);
    assert_eq!(state(&node), Some(ElectionState::Canvass));
}

#[test]
fn a_leader_commits_what_a_quorum_appended_and_tells_the_followers() {
    let mut node = leader();
    sent(&mut node);
    let message = log::timer_event(0, 1, 0);
    log::append(&mut node.ports, &[&message]).expect("appended");
    let position = node.ports.log_position;
    node.run(2);
    assert_eq!(node.module.commit_position(), 0, "the followers are not there yet");
    node.consensus_in(&consensus::append_position(0, position, 2, 0));
    node.run(1);
    assert_eq!(node.module.commit_position(), position);
    let commits: Vec<_> = sent(&mut node)
        .into_iter()
        .filter_map(|(to, m)| match m {
            ConsensusMessage::CommitPosition { log_position, .. } => Some((to, log_position)),
            _ => None,
        })
        .collect();
    assert!(
        commits.contains(&(1, position)) && commits.contains(&(2, position)),
        "{commits:?}"
    );
}

#[test]
fn a_leader_without_an_active_quorum_steps_down() {
    let mut node = leader();
    node.advance(10_001);
    assert_eq!(node.module.role(), Role::Follower);
    assert!(
        node.errors.iter().any(|e| e == "inactive follower quorum"),
        "{:?}",
        node.errors
    );
}

#[test]
fn a_leader_replays_its_log_to_a_follower_catching_up_until_it_is_there() {
    let mut node = leader();
    sent(&mut node);
    let position = node.ports.log_position;
    node.consensus_in(&consensus::catchup_position(0, 0, 1, "localhost:20213"));
    node.run(1);
    let replay = node
        .ports
        .archive
        .requests
        .iter()
        .rev()
        .find_map(|r| match r {
            Request::Replay {
                replay_channel,
                position,
                length,
                replay_stream_id,
                ..
            } => Some((replay_channel.clone(), *position, *length, *replay_stream_id)),
            _ => None,
        })
        .expect("a replay");
    assert_eq!((replay.1, replay.2, replay.3), (0, i64::MAX, 100));
    for part in [
        "endpoint=localhost:20213",
        &format!("session-id={LOG_SESSION_ID}"),
        "linger=0",
        "eos=false",
        "alias=catchup-followerId-1",
    ] {
        assert!(replay.0.contains(part), "{}", replay.0);
    }
    // Asked again while it runs: one replay.
    node.consensus_in(&consensus::catchup_position(0, 0, 1, "localhost:20213"));
    node.run(1);
    let replays = |node: &FakeNode| {
        node.ports
            .archive
            .requests
            .iter()
            .filter(|r| matches!(r, Request::Replay { .. }))
            .count()
    };
    assert_eq!(replays(&node), 1);

    node.consensus_in(&consensus::append_position(
        0,
        position,
        1,
        APPEND_POSITION_FLAG_CATCHUP,
    ));
    node.run(1);
    assert!(
        node.ports
            .archive
            .requests
            .iter()
            .any(|r| matches!(r, Request::StopReplay { .. })),
        "the replay stopped"
    );
    assert!(sent(&mut node).contains(&(
        1,
        ConsensusMessage::StopCatchup {
            leadership_term_id: 0,
            follower_member_id: 1
        }
    )));
}

#[test]
fn a_leader_terminates_once_every_follower_acked_the_shutdown() {
    let mut node = leader();
    node.ports.toggle = toggle::SHUTDOWN;
    node.run(11);
    let position = node.ports.log_position;
    let terminations: Vec<_> = sent(&mut node)
        .into_iter()
        .filter(|(_, m)| matches!(m, ConsensusMessage::TerminationPosition { .. }))
        .collect();
    assert_eq!(
        terminations,
        [1, 2].map(|to| (
            to,
            ConsensusMessage::TerminationPosition {
                leadership_term_id: 0,
                log_position: position
            }
        ))
    );
    // The followers append the action: the snapshot is taken, the services stop.
    node.consensus_in(&consensus::append_position(0, position, 1, 0));
    node.consensus_in(&consensus::append_position(0, position, 2, 0));
    node.run(20);
    assert_eq!(
        node.module.state(),
        ModuleState::Terminating,
        "errors: {:?}",
        node.errors
    );
    assert_eq!(node.terminated, None, "the followers' acks awaited");
    node.consensus_in(&consensus::termination_ack(0, position, 1));
    assert_eq!(node.terminated, None);
    node.consensus_in(&consensus::termination_ack(0, position, 2));
    assert_eq!(node.terminated, Some(true), "errors: {:?}", node.errors);
}

/// Member 1 following leader 0's term 0 from position 0.
fn follower() -> FakeNode {
    follower_with(config(1))
}

fn follower_with(config: Config) -> FakeNode {
    let mut node = canvassing_with(config);
    node.consensus_in(&consensus::new_leadership_term(&new_term(-1, 0, 0, 0)));
    node.run(3);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerLogAwait),
        "errors: {:?}",
        node.errors
    );
    node.log_image(LEADER_SESSION, 0);
    node.run(5);
    assert_eq!(state(&node), None, "errors: {:?}", node.errors);
    node
}

/// A leader's term as member 0 sends it, the log at `position`.
fn new_term(
    log_leadership_term_id: i64,
    leadership_term_id: i64,
    position: i64,
    commit_position: i64,
) -> NewLeadershipTerm {
    NewLeadershipTerm {
        log_leadership_term_id,
        next_leadership_term_id: leadership_term_id,
        next_term_base_log_position: position,
        next_log_position: -1,
        leadership_term_id,
        term_base_log_position: position,
        log_position: position,
        commit_position,
        leader_recording_id: 0,
        timestamp: 0,
        leader_member_id: 0,
        log_session_id: LEADER_SESSION,
        app_version: 1,
        is_startup: true,
    }
}

/// Java's `connectIngress`: every member takes its own unicast ingress,
/// only the leader a multicast one.
#[test]
fn only_the_leader_subscribes_to_a_multicast_ingress() {
    let multicast = |member_id| Config {
        ingress_channel: "aeron:udp?endpoint=239.192.11.87:20123|interface=127.0.0.1|term-length=64k".into(),
        ..config(member_id)
    };
    let ingress = |node: &FakeNode, endpoint: &str| {
        node.subscriptions
            .values()
            .filter(|(channel, _)| channel.contains(endpoint))
            .count()
    };
    assert_eq!(ingress(&follower_with(multicast(1)), "239.192.11.87"), 0);
    assert_eq!(ingress(&leader_with(multicast(0)), "239.192.11.87"), 1);
    assert_eq!(ingress(&follower(), "localhost:20210"), 1);
    assert_eq!(ingress(&leader(), "localhost:20110"), 1);
}

#[test]
fn a_follower_joins_the_leaders_live_log() {
    let mut node = canvassing(1);
    node.consensus_in(&consensus::new_leadership_term(&new_term(-1, 0, 0, 0)));
    node.run(3);
    assert_eq!(state(&node), Some(ElectionState::FollowerLogAwait));
    let subscription = node.log_subscription().expect("the log subscription");
    let (channel, _) = &node.subscriptions[&subscription];
    for part in [
        "control-mode=manual",
        "session-id=77",
        "group=true",
        "rejoin=false",
        "alias=log-cm",
    ] {
        assert!(channel.contains(part), "{channel}");
    }
    assert_eq!(
        node.destinations,
        [(subscription, "aeron:udp?endpoint=localhost:20212".to_string())]
    );
    sent(&mut node);
    node.log_image(LEADER_SESSION, 0);
    node.run(5);
    assert_eq!(state(&node), None, "errors: {:?}", node.errors);
    assert_eq!(node.module.role(), Role::Follower);
    assert!(sent(&mut node).contains(&(
        0,
        ConsensusMessage::AppendPosition {
            leadership_term_id: 0,
            log_position: 0,
            follower_member_id: 1,
            flags: 0
        }
    )));
    let terms: Vec<_> = RecordingLog::load(&node.recording_log_file)
        .expect("the recording log")
        .entries()
        .iter()
        .filter(|e| e.entry_type == ENTRY_TYPE_TERM)
        .map(|e| (e.leadership_term_id, e.term_base_log_position))
        .collect();
    assert_eq!(terms, [(0, 0)]);
}

/// Java's `ClusterBackup` asks a follower by default.
#[test]
fn a_follower_answers_a_backup_naming_the_leader() {
    let mut node = follower();
    assert_eq!(node.module.state(), ModuleState::Active);
    let query = consensus::backup_query(
        5,
        108,
        rusteron_consensus_core::sessions::PROTOCOL_SEMANTIC_VERSION,
        -1,
        "aeron:udp?endpoint=localhost:20199",
        &[],
    );
    node.consensus_in(&query);
    // Answered from the slow tick, every 10 ms when idle.
    node.run(15);
    let replies: Vec<Vec<u8>> = node.ports.egress.drain().flat_map(|(_, m)| m).collect();
    assert_eq!(replies.len(), 1, "errors: {:?}", node.errors);
    let response = consensus::decode_backup_response(&replies[0]).expect("a backup response");
    assert_eq!((response.leader_member_id, response.member_id), (0, 1));
    assert_eq!(
        (response.log_leadership_term_id, response.log_term_base_log_position),
        (0, 0)
    );
    assert!(response.snapshots.is_empty(), "no snapshot yet");
    // Java's `encodeAsString`: the three members, `|` between them.
    assert_eq!(
        response.cluster_members.split('|').count(),
        3,
        "{}",
        response.cluster_members
    );
}

#[test]
fn a_follower_follows_the_log_up_to_the_commit_position_it_is_told() {
    let mut node = follower();
    let event = log::new_leadership_term_event(0, 96, 0, 0, 0, LEADER_SESSION, config(1).time_unit.codec(), 1);
    node.leader_message(LEADER_SESSION, event, 96);
    let timer = log::timer_event(0, 5, 0);
    node.leader_message(LEADER_SESSION, timer, 96 + 64);
    node.run(2);
    assert_eq!(node.module.commit_position(), 0, "nothing committed yet");
    node.consensus_in(&consensus::commit_position(0, 96, 0));
    node.run(2);
    assert_eq!(node.module.commit_position(), 96);
    node.consensus_in(&consensus::commit_position(0, 96 + 64, 0));
    node.run(2);
    assert_eq!(node.module.commit_position(), 96 + 64);
    let appends: Vec<_> = sent(&mut node)
        .into_iter()
        .filter_map(|(to, m)| match m {
            ConsensusMessage::AppendPosition { log_position, .. } => Some((to, log_position)),
            _ => None,
        })
        .collect();
    assert!(
        appends.contains(&(0, 96 + 64)),
        "the recorded position reported: {appends:?}"
    );
    // A commit position never goes back.
    node.consensus_in(&consensus::commit_position(0, 96, 0));
    node.run(2);
    assert_eq!(node.module.commit_position(), 96 + 64);
}

/// A member killed right after its archive recorded a term's padding frame
/// replays its log to the term's end once started again. A poll steps over
/// the padding with no fragment, so the module's position is its image's
/// (Java's `LogAdapter.position`): from its fragments alone it stopped short
/// of the stop, in follower replay for good, its commit position holding the
/// services before the padding.
#[test]
fn a_follower_replays_its_log_to_a_stop_after_a_terms_padding() {
    let mut node = follower();
    let event = log::new_leadership_term_event(0, 96, 0, 0, 0, LEADER_SESSION, config(1).time_unit.codec(), 1);
    node.leader_message(LEADER_SESSION, event, 96);
    node.leader_message(LEADER_SESSION, log::timer_event(0, 5, 0), 96 + 64);
    node.consensus_in(&consensus::commit_position(0, 96 + 64, 0));
    node.run(2);
    assert_eq!(node.module.commit_position(), 96 + 64);

    let stop = 96 + 64 + 32;
    let mut node = node.restart(config(1));
    node.ports
        .archive
        .recordings
        .values_mut()
        .find(|r| r.session_id == LEADER_SESSION)
        .expect("the log's recording")
        .stop_position = Some(stop);
    node.run(20);
    assert_eq!(state(&node), Some(ElectionState::Canvass), "errors: {:?}", node.errors);
    let mut term = new_term(0, 0, 0, stop);
    term.next_leadership_term_id = -1;
    term.next_term_base_log_position = -1;
    term.log_position = stop;
    term.is_startup = false;
    node.consensus_in(&consensus::new_leadership_term(&term));
    node.run(10);
    assert_eq!(node.module.commit_position(), stop, "errors: {:?}", node.errors);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerLogAwait),
        "errors: {:?}",
        node.errors
    );
}

/// A member restarted with its log to replay gives the replay up once its
/// leader falls silent, and canvasses and votes again. Java replays to the
/// end, neither canvassing nor voting meanwhile, and the other members have
/// no quorum to elect another leader until it is done: seconds after a
/// restart (a fault injector pausing the leader).
#[test]
fn a_follower_replaying_its_log_starts_over_once_its_leader_is_silent() {
    let mut node = follower();
    let event = log::new_leadership_term_event(0, 96, 0, 0, 0, LEADER_SESSION, config(1).time_unit.codec(), 1);
    node.leader_message(LEADER_SESSION, event, 96);
    node.leader_message(LEADER_SESSION, log::timer_event(0, 5, 0), 96 + 64);
    node.consensus_in(&consensus::commit_position(0, 96 + 64, 0));
    node.run(2);
    let end = 96 + 64;

    let mut node = node.restart(config(1));
    node.replays_held = true;
    node.run(20);
    assert_eq!(state(&node), Some(ElectionState::Canvass), "errors: {:?}", node.errors);
    let mut term = new_term(0, 0, 0, end);
    term.next_leadership_term_id = -1;
    term.next_term_base_log_position = -1;
    term.log_position = end;
    term.is_startup = false;
    node.consensus_in(&consensus::new_leadership_term(&term));
    node.run(10);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerReplay),
        "errors: {:?}",
        node.errors
    );
    node.advance(5_000);
    node.consensus_in(&consensus::commit_position(0, end, 0));
    node.advance(9_000);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerReplay),
        "the leader's commit position counts as word: {:?}",
        node.errors
    );
    sent(&mut node);
    node.advance(2_000);
    node.run(20);
    assert!(
        node.errors
            .iter()
            .any(|e| e.starts_with("no word from the leader in follower replay")),
        "{:?}",
        node.errors
    );
    assert_eq!(state(&node), Some(ElectionState::Canvass), "errors: {:?}", node.errors);
    let canvass = ConsensusMessage::CanvassPosition {
        log_leadership_term_id: 0,
        log_position: end,
        leadership_term_id: 0,
        follower_member_id: 1,
        protocol_version: consensus::PROTOCOL_SEMANTIC_VERSION,
    };
    assert!(sent(&mut node).contains(&(2, canvass)));
    node.consensus_in(&consensus::request_vote(0, end, 1, 2));
    node.run(1);
    let vote = ConsensusMessage::Vote {
        candidate_term_id: 1,
        log_leadership_term_id: 0,
        log_position: end,
        candidate_member_id: 2,
        follower_member_id: 1,
        vote: true,
    };
    assert!(sent(&mut node).contains(&(2, vote)), "errors: {:?}", node.errors);
    assert_eq!(state(&node), Some(ElectionState::FollowerBallot));
}

/// A replay given up past a term's padding frame is held until the module
/// reads to a message's end, and the services are asked to ack there: their
/// log position follows the messages they read, and one past a padding frame
/// they would not report reaching while the replay goes on.
#[test]
fn a_replay_given_up_past_a_padding_frame_is_held_at_a_message_end() {
    let mut node = follower();
    let event = log::new_leadership_term_event(0, 96, 0, 0, 0, LEADER_SESSION, config(1).time_unit.codec(), 1);
    node.leader_message(LEADER_SESSION, event, 96);
    node.leader_message(LEADER_SESSION, log::timer_event(0, 5, 0), 96 + 64);
    node.consensus_in(&consensus::commit_position(0, 96 + 64, 0));
    node.run(2);
    let end = 96 + 64;

    let mut node = node.restart(config(1));
    node.replays_held = true;
    node.run(20);
    let mut term = new_term(0, 0, 0, end);
    term.next_leadership_term_id = -1;
    term.next_term_base_log_position = -1;
    term.log_position = end;
    term.is_startup = false;
    node.consensus_in(&consensus::new_leadership_term(&term));
    node.run(10);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerReplay),
        "errors: {:?}",
        node.errors
    );
    // The image steps over a padding frame: the module's position moves, no message read.
    let replay = *node
        .subscriptions
        .iter()
        .find(|(_, (_, stream))| *stream == config(1).replay_stream_id)
        .expect("the replay's subscription")
        .0;
    node.module.on_image_position(replay, 32);
    node.advance(11_000);
    node.run(20);
    assert!(
        node.errors
            .iter()
            .any(|e| e.starts_with("no word from the leader in follower replay")),
        "{:?}",
        node.errors
    );
    assert_eq!(state(&node), Some(ElectionState::Init), "held: {:?}", node.errors);
    assert!(
        node.services.ack_requests.is_empty(),
        "no ack asked past the padding: {:?}",
        node.services.ack_requests
    );
    node.replays_held = false;
    node.run(20);
    assert_eq!(node.services.ack_requests, [end], "asked at the message's end");
    assert_eq!(node.module.commit_position(), end);
    assert_eq!(state(&node), Some(ElectionState::Canvass), "errors: {:?}", node.errors);
}

#[test]
fn a_follower_without_word_from_the_leader_starts_an_election() {
    let mut node = follower();
    node.advance(5_000);
    node.consensus_in(&consensus::commit_position(0, 0, 0));
    node.advance(9_000);
    assert_eq!(state(&node), None, "the commit position counts as word");
    node.advance(2_000);
    assert!(state(&node).is_some(), "errors: {:?}", node.errors);
    assert!(
        node.errors.iter().any(|e| e == "leader heartbeat timeout"),
        "{:?}",
        node.errors
    );
}

#[test]
fn a_follower_stops_where_the_leader_terminates() {
    let mut node = follower();
    let event = log::new_leadership_term_event(0, 96, 0, 0, 0, LEADER_SESSION, config(1).time_unit.codec(), 1);
    node.leader_message(LEADER_SESSION, event, 96);
    node.consensus_in(&consensus::termination_position(0, 96));
    node.consensus_in(&consensus::commit_position(0, 96, 0));
    node.run(5);
    assert_eq!(node.terminated, Some(true), "errors: {:?}", node.errors);
    assert!(sent(&mut node).contains(&(
        0,
        ConsensusMessage::TerminationAck {
            leadership_term_id: 0,
            log_position: 96,
            member_id: 1
        }
    )));
}

#[test]
fn a_follower_behind_the_leaders_term_catches_up_on_its_log() {
    let mut node = canvassing(1);
    // The leader's log is at 4000 in term 0, this member's at 0.
    let mut term = new_term(-1, 0, 0, 4000);
    term.log_position = 4000;
    node.consensus_in(&consensus::new_leadership_term(&term));
    node.run(3);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerCatchupAwait),
        "errors: {:?}",
        node.errors
    );
    let subscription = node.log_subscription().expect("the log subscription");
    assert_eq!(
        node.destinations,
        [(subscription, "aeron:udp?endpoint=localhost:20213".to_string())]
    );
    assert!(sent(&mut node).contains(&(
        0,
        ConsensusMessage::CatchupPosition {
            leadership_term_id: 0,
            log_position: 0,
            follower_member_id: 1,
            catchup_endpoint: "localhost:20213".into()
        }
    )));
    node.log_image(LEADER_SESSION, 0);
    node.run(5);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerCatchup),
        "errors: {:?}",
        node.errors
    );
    // Near the live log: the live destination joins the catch-up's.
    assert_eq!(node.destinations.len(), 2, "{:?}", node.destinations);
    let event = log::new_leadership_term_event(0, 96, 0, 0, 0, LEADER_SESSION, config(1).time_unit.codec(), 1);
    node.leader_message(LEADER_SESSION, event, 96);
    let mut position = 96;
    while position < 4000 {
        position += 64;
        node.leader_message(LEADER_SESSION, log::timer_event(0, position, 0), position);
    }
    node.run(3);
    assert_eq!(node.module.commit_position(), 4000);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerCatchup),
        "until the leader stops the catch-up"
    );
    node.consensus_in(&consensus::stop_catchup(0, 1));
    node.run(3);
    assert_eq!(state(&node), None, "errors: {:?}", node.errors);
    assert_eq!(
        node.destinations,
        [(subscription, "aeron:udp?endpoint=localhost:20212".to_string())]
    );
}

#[test]
fn a_follower_missing_a_term_replicates_it_from_the_leaders_archive() {
    let mut node = canvassing(1);
    // Term 0 ended at 1024 on the leader; term 1 starts there.
    let term = NewLeadershipTerm {
        log_leadership_term_id: -1,
        next_leadership_term_id: 0,
        next_term_base_log_position: 0,
        next_log_position: 1024,
        leadership_term_id: 1,
        term_base_log_position: 1024,
        log_position: 1024,
        commit_position: 0,
        leader_recording_id: 0,
        timestamp: 0,
        leader_member_id: 0,
        log_session_id: LEADER_SESSION,
        app_version: 1,
        is_startup: false,
    };
    node.consensus_in(&consensus::new_leadership_term(&term));
    node.run(2);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerLogReplication),
        "errors: {:?}",
        node.errors
    );
    let replicate = node
        .ports
        .archive
        .requests
        .iter()
        .find_map(|r| match r {
            Request::Replicate(r) => Some(r.clone()),
            _ => None,
        })
        .expect("a replication");
    assert_eq!(
        (
            replicate.src_recording_id,
            replicate.dst_recording_id,
            replicate.stop_position,
            replicate.src_control_channel.as_str()
        ),
        (0, -1, 1024, "aeron:udp?endpoint=localhost:20114")
    );
    // The leader's archive replicates into a new recording here.
    let replication_id = 3_000 + replicate.correlation_id;
    for (signal, position) in [
        (Signal::REPLICATE, 0),
        (Signal::START, 0),
        (Signal::SYNC, 1024),
        (Signal::REPLICATE_END, 1024),
        (Signal::STOP, 1024),
    ] {
        node.ports.archive.signal(replication_id, 9, 0, position, signal);
    }
    node.run(2);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerLogReplication),
        "until the leader commits it"
    );
    node.consensus_in(&consensus::commit_position(1, 1024, 0));
    node.run(2);
    assert_eq!(state(&node), Some(ElectionState::Canvass), "errors: {:?}", node.errors);
    let terms: Vec<_> = RecordingLog::load(&node.recording_log_file)
        .expect("the recording log")
        .entries()
        .iter()
        .filter(|e| e.entry_type == ENTRY_TYPE_TERM)
        .map(|e| {
            (
                e.recording_id,
                e.leadership_term_id,
                e.term_base_log_position,
                e.log_position,
            )
        })
        .collect();
    assert_eq!(terms, [(9, 0, 0, 1024)]);
}

#[test]
fn a_follower_ahead_of_the_new_leader_truncates_its_log() {
    let mut node = follower();
    let event = log::new_leadership_term_event(0, 96, 0, 0, 0, LEADER_SESSION, config(1).time_unit.codec(), 1);
    node.leader_message(LEADER_SESSION, event, 96);
    node.leader_message(LEADER_SESSION, log::timer_event(0, 1, 0), 160);
    node.consensus_in(&consensus::commit_position(0, 96, 0));
    node.run(2);
    assert_eq!(node.module.commit_position(), 96);
    // Leader 0 is gone; member 2 leads term 1 from 96, where 0's last commit was.
    node.advance(10_001);
    node.run(10);
    assert_eq!(state(&node), Some(ElectionState::Canvass), "errors: {:?}", node.errors);
    let term = NewLeadershipTerm {
        log_leadership_term_id: 0,
        next_leadership_term_id: 1,
        next_term_base_log_position: 96,
        next_log_position: -1,
        leadership_term_id: 1,
        term_base_log_position: 96,
        log_position: 96,
        commit_position: 96,
        leader_recording_id: 0,
        timestamp: 0,
        leader_member_id: 2,
        log_session_id: 88,
        app_version: 1,
        is_startup: false,
    };
    node.consensus_in(&consensus::new_leadership_term(&term));
    assert!(
        node.errors.iter().any(|e| e.starts_with("Truncating Cluster Log")),
        "{:?}",
        node.errors
    );
    // The archive is asked on the next duty cycle.
    node.run(10);
    assert!(
        node.ports
            .archive
            .requests
            .iter()
            .any(|r| matches!(r, Request::TruncateRecording { position: 96, .. })),
        "the log cut back to 96"
    );
    assert_eq!(state(&node), Some(ElectionState::Canvass), "errors: {:?}", node.errors);
    node.consensus_in(&consensus::new_leadership_term(&term));
    node.run(3);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerLogAwait),
        "errors: {:?}",
        node.errors
    );
}

// ---------------------------------------------------------------- Raft's rules

/// Member 1, canvassing, votes for member 2 in term 1; the ballot times out
/// and it canvasses again, its vote in term 1.
fn voted_in_term_1() -> FakeNode {
    let mut node = canvassing(1);
    node.consensus_in(&consensus::request_vote(-1, 0, 1, 2));
    node.run(1);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerBallot),
        "errors: {:?}",
        node.errors
    );
    node.advance(1_001);
    node.run(1);
    assert_eq!(state(&node), Some(ElectionState::Canvass), "errors: {:?}", node.errors);
    node
}

#[test]
fn a_member_canvasses_with_the_term_it_voted_in() {
    let mut node = voted_in_term_1();
    sent(&mut node);
    node.advance(200);
    let messages = sent(&mut node);
    assert!(
        messages.iter().any(|(_, m)| matches!(
            m,
            ConsensusMessage::CanvassPosition {
                leadership_term_id: 1,
                ..
            }
        )),
        "{messages:?}"
    );
}

#[test]
fn a_member_that_voted_follows_no_leader_of_an_older_term() {
    let mut node = voted_in_term_1();
    node.consensus_in(&consensus::new_leadership_term(&new_term(-1, 0, 0, 0)));
    node.run(3);
    assert_eq!(state(&node), Some(ElectionState::Canvass), "errors: {:?}", node.errors);
    node.consensus_in(&consensus::new_leadership_term(&new_term(-1, 1, 0, 0)));
    node.run(3);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerLogAwait),
        "a leader of its term it follows; errors: {:?}",
        node.errors
    );
}

#[test]
fn a_leader_refused_does_not_keep_a_member_from_standing() {
    let mut node = voted_in_term_1();
    node.consensus_in(&consensus::canvass_position(-1, 0, 1, 2));
    // Term 0's leader goes on publishing its term, every 100 ms, for 11 s.
    for _ in 0..110 {
        node.consensus_in(&consensus::new_leadership_term(&new_term(-1, 0, 0, 0)));
        node.consensus_in(&consensus::commit_position(0, 0, 0));
        node.advance(100);
        if state(&node) != Some(ElectionState::Canvass) {
            break;
        }
    }
    assert!(
        matches!(
            state(&node),
            Some(ElectionState::Nominate | ElectionState::CandidateBallot)
        ),
        "{:?}, errors: {:?}",
        state(&node),
        node.errors
    );
}

#[test]
fn a_leader_steps_down_when_a_member_canvasses_with_a_newer_vote() {
    let mut node = leader();
    node.consensus_in(&consensus::canvass_position(0, 0, 1, 1));
    node.run(1);
    assert_eq!(node.module.role(), Role::Follower);
    assert!(
        node.errors.iter().any(|e| e.starts_with("unexpected canvass position")),
        "{:?}",
        node.errors
    );
}

#[test]
fn a_leader_completing_its_election_steps_down_on_a_newer_term() {
    let mut node = canvassing(0);
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 1));
    node.consensus_in(&consensus::canvass_position(-1, 0, -1, 2));
    node.run(1);
    node.advance(500);
    node.consensus_in(&consensus::vote(0, -1, 0, 0, 1, true));
    node.consensus_in(&consensus::vote(0, -1, 0, 0, 2, true));
    node.run(10);
    assert_eq!(
        state(&node),
        Some(ElectionState::LeaderReady),
        "errors: {:?}",
        node.errors
    );
    let newer = NewLeadershipTerm {
        leader_member_id: 2,
        log_session_id: 88,
        ..new_term(-1, 1, 0, 0)
    };
    node.consensus_in(&consensus::new_leadership_term(&newer));
    node.run(1);
    assert_eq!(node.module.role(), Role::Follower);
    assert!(
        node.errors
            .iter()
            .any(|e| e.starts_with("new leader detected due to new leadership term")),
        "{:?}",
        node.errors
    );
}

#[test]
fn a_leader_counts_a_position_only_as_far_as_the_log_is_its_own() {
    let mut node = leader();
    sent(&mut node);
    let message = log::timer_event(0, 1, 0);
    log::append(&mut node.ports, &[&message]).expect("appended");
    let position = node.ports.log_position;
    node.run(2);
    // Of a term this leader's log does not have: nothing of it is known to be the leader's.
    node.consensus_in(&consensus::append_position(-1, position, 2, 0));
    node.run(1);
    assert_eq!(node.module.commit_position(), 0, "Java would count it");
    // A log ending in the leader's own term is the leader's, as far as it goes.
    node.consensus_in(&consensus::canvass_position(0, position, 0, 2));
    node.run(1);
    assert_eq!(node.module.commit_position(), position);
}

#[test]
fn a_member_refuses_to_truncate_below_its_commit_position() {
    let mut node = follower();
    let event = log::new_leadership_term_event(0, 96, 0, 0, 0, LEADER_SESSION, config(1).time_unit.codec(), 1);
    node.leader_message(LEADER_SESSION, event, 96);
    node.leader_message(LEADER_SESSION, log::timer_event(0, 1, 0), 160);
    node.consensus_in(&consensus::commit_position(0, 160, 0));
    node.run(2);
    assert_eq!(node.module.commit_position(), 160);
    node.advance(10_001);
    node.run(10);
    assert_eq!(state(&node), Some(ElectionState::Canvass), "errors: {:?}", node.errors);
    // Member 2 leads term 1 from 96: it lacks what was committed up to 160.
    let term = NewLeadershipTerm {
        leader_member_id: 2,
        log_session_id: 88,
        is_startup: false,
        ..new_term(0, 1, 96, 96)
    };
    node.consensus_in(&consensus::new_leadership_term(&term));
    node.run(10);
    assert!(
        node.errors
            .iter()
            .any(|e| e.starts_with("refusing to truncate below the commit position")),
        "{:?}",
        node.errors
    );
    assert!(
        !node
            .ports
            .archive
            .requests
            .iter()
            .any(|r| matches!(r, Request::TruncateRecording { .. })),
        "the log kept"
    );
    assert_eq!(state(&node), Some(ElectionState::Canvass), "errors: {:?}", node.errors);
}

/// The recording log's terms: (recording, term, base, end).
fn terms(node: &FakeNode) -> Vec<(i64, i64, i64, i64)> {
    RecordingLog::load(&node.recording_log_file)
        .expect("the recording log")
        .entries()
        .iter()
        .filter(|e| e.is_valid && e.entry_type == ENTRY_TYPE_TERM)
        .map(|e| {
            (
                e.recording_id,
                e.leadership_term_id,
                e.term_base_log_position,
                e.log_position,
            )
        })
        .collect()
}

#[test]
fn a_follower_files_the_leaders_term_as_it_starts_recording_its_log() {
    let mut node = canvassing(1);
    node.consensus_in(&consensus::new_leadership_term(&new_term(-1, 0, 0, 0)));
    node.run(3);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerLogAwait),
        "errors: {:?}",
        node.errors
    );
    assert_eq!(terms(&node), [], "nothing recorded yet");
    node.log_image(LEADER_SESSION, 0);
    for _ in 0..10 {
        node.run(1);
        if !terms(&node).is_empty() {
            break;
        }
    }
    // Filed as its log is recorded, before its services join, where Java
    // files it: an election starting over in between would leave the log's
    // entries in no term.
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerLogAwait),
        "errors: {:?}",
        node.errors
    );
    let filed = terms(&node);
    assert_eq!(filed.len(), 1, "{filed:?}");
    assert_eq!((filed[0].1, filed[0].2, filed[0].3), (0, 0, -1));
}

#[test]
fn a_follower_files_a_replicated_term_before_the_replication_ends() {
    let mut node = canvassing(1);
    let term = NewLeadershipTerm {
        log_leadership_term_id: -1,
        next_leadership_term_id: 0,
        next_term_base_log_position: 0,
        next_log_position: 1024,
        leadership_term_id: 1,
        term_base_log_position: 1024,
        log_position: 1024,
        commit_position: 0,
        leader_recording_id: 0,
        timestamp: 0,
        leader_member_id: 0,
        log_session_id: LEADER_SESSION,
        app_version: 1,
        is_startup: false,
    };
    node.consensus_in(&consensus::new_leadership_term(&term));
    node.run(2);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerLogReplication),
        "errors: {:?}",
        node.errors
    );
    let replicate = node
        .ports
        .archive
        .requests
        .iter()
        .find_map(|r| match r {
            Request::Replicate(r) => Some(r.clone()),
            _ => None,
        })
        .expect("a replication");
    let replication_id = 3_000 + replicate.correlation_id;
    // The new recording's first signals; the replication goes on.
    for (signal, position) in [(Signal::REPLICATE, 0), (Signal::START, 0)] {
        node.ports.archive.signal(replication_id, 9, 0, position, signal);
    }
    node.run(2);
    assert_eq!(terms(&node), [(9, 0, 0, -1)], "filed open; errors: {:?}", node.errors);
}

#[test]
fn a_member_drops_an_empty_term_the_leader_never_had() {
    let mut node = follower();
    let event = log::new_leadership_term_event(0, 96, 0, 0, 0, LEADER_SESSION, config(1).time_unit.codec(), 1);
    node.leader_message(LEADER_SESSION, event, 96);
    node.leader_message(LEADER_SESSION, log::timer_event(0, 1, 0), 160);
    node.consensus_in(&consensus::commit_position(0, 160, 0));
    node.run(2);
    // Leader 0 is cut off; member 2 leads term 1 from 160, and this member
    // files term 1 as it joins, before any of the term's entries.
    node.advance(10_001);
    node.run(10);
    assert_eq!(state(&node), Some(ElectionState::Canvass), "errors: {:?}", node.errors);
    let term_1 = NewLeadershipTerm {
        leader_member_id: 2,
        log_session_id: 88,
        is_startup: false,
        ..new_term(0, 1, 160, 160)
    };
    node.consensus_in(&consensus::new_leadership_term(&term_1));
    node.run(3);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerLogAwait),
        "errors: {:?}",
        node.errors
    );
    node.log_image(88, 160);
    for _ in 0..10 {
        node.run(1);
        if terms(&node).len() == 2 {
            break;
        }
    }
    let filed: Vec<_> = terms(&node).iter().map(|t| (t.1, t.2)).collect();
    assert_eq!(filed, [(0, 0), (1, 160)], "errors: {:?}", node.errors);
    node.run(5);
    assert_eq!(state(&node), None, "errors: {:?}", node.errors);
    // Member 2 falls silent with term 1 empty. Member 0 is back and leads
    // term 2 from 224: it never had term 1, and its term 0 goes on to 224.
    node.advance(10_001);
    node.run(10);
    assert_eq!(state(&node), Some(ElectionState::Canvass), "errors: {:?}", node.errors);
    let term_2 = NewLeadershipTerm {
        log_leadership_term_id: 0,
        next_leadership_term_id: 2,
        next_term_base_log_position: 224,
        next_log_position: -1,
        leadership_term_id: 2,
        term_base_log_position: 224,
        log_position: 224,
        commit_position: 160,
        leader_recording_id: 0,
        timestamp: 0,
        leader_member_id: 0,
        log_session_id: LEADER_SESSION + 1,
        app_version: 1,
        is_startup: false,
    };
    node.consensus_in(&consensus::new_leadership_term(&term_2));
    node.run(2);
    assert_eq!(
        state(&node),
        Some(ElectionState::FollowerLogReplication),
        "errors: {:?}",
        node.errors
    );
    // Term 0's entries from 160 to 224 are replicated: term 1 goes, or they
    // would be filed under it, and a leader of a later term holding that
    // recording log would send back to 160 a member that committed them.
    let filed: Vec<_> = terms(&node).iter().map(|t| t.1).collect();
    assert_eq!(filed, [0], "errors: {:?}", node.errors);
}
