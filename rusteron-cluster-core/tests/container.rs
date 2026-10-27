//! The service container against the testkit's log and consensus module:
//! its whole life, a restart from its snapshot, and the log however it is
//! framed, bounded and retried.

use std::cell::RefCell;
use std::rc::Rc;

use proptest::prelude::*;
use rusteron_cluster_core::counters::RecoveryState;
use rusteron_cluster_core::log::PollAction;
use rusteron_cluster_core::position::aligned_frame_length;
use rusteron_cluster_core::service::*;
use rusteron_cluster_core::testkit::{FakeCm, Frame, LogWriter, ToCm, cm, log};

const TERM_LENGTH: i32 = 256 * 1024;
const MTU: i32 = 1_408;
const CLIENT_ID: i64 = 77;
const RESPONSE_CHANNEL: &str = "aeron:udp?endpoint=localhost:20121";

/// A service that records what happens to it, keeps a running hash of its
/// messages, echoes them, and schedules timers when asked.
#[derive(Default)]
struct Recorder {
    events: Vec<String>,
    hash: u64,
    /// Messages applied: their payload and the position after them.
    applied: Vec<(Vec<u8>, i64)>,
    /// How many deliveries to refuse before applying, per message.
    retries: Vec<u32>,
    retried: u32,
    loaded: Option<Rc<RefCell<Vec<u8>>>>,
}

impl ClusteredService for Recorder {
    fn snapshot_sink(&mut self, _: &SnapshotInfo) -> Box<dyn SnapshotSink> {
        let (sink, bytes) = vec_sink();
        self.loaded = Some(bytes);
        sink
    }

    fn on_start(&mut self, cx: &ReadCx<'_>, snapshot: Option<&SnapshotInfo>) -> Result<(), String> {
        if let Some(info) = snapshot {
            let bytes = self.loaded.take().expect("a sink").borrow().clone();
            self.hash = u64::from_le_bytes(bytes.as_slice().try_into().map_err(|_| "snapshot is not 8 bytes")?);
            self.events.push(format!(
                "start from {} with {} sessions",
                info.log_position,
                cx.sessions().count()
            ));
        } else {
            self.events.push("start".into());
        }
        Ok(())
    }

    fn on_session_open(&mut self, _: &mut Cx<'_>, session: &ClientSession, _: i64) {
        self.events.push(format!("open {}", session.id));
    }

    fn on_session_close(&mut self, _: &mut Cx<'_>, session: &ClientSession, _: i64, reason: CloseReason) {
        self.events.push(format!("close {} {reason:?}", session.id));
    }

    fn on_session_message(&mut self, cx: &mut Cx<'_>, m: SessionMessage<'_>) -> Apply {
        let index = self.applied.len();
        if self.retried < self.retries.get(index).copied().unwrap_or(0) {
            self.retried += 1;
            cx.schedule_timer(-1, -1); // discarded with the retry
            return Apply::Retry;
        }
        self.retried = 0;
        if let Some(n) = m.payload.strip_prefix(b"timers:") {
            let n: i64 = std::str::from_utf8(n).unwrap().parse().unwrap();
            for id in 0..n {
                cx.schedule_timer(id, m.timestamp + id);
            }
        }
        self.hash = self
            .hash
            .wrapping_mul(31)
            .wrapping_add(m.payload.iter().map(|&b| b as u64).sum::<u64>());
        self.applied.push((m.payload.to_vec(), m.log_position));
        if let Some(id) = m.session_id {
            cx.egress(id, m.payload);
        }
        Apply::Done
    }

    fn on_timer_event(&mut self, _: &mut Cx<'_>, correlation_id: i64, _: i64) -> Apply {
        self.events.push(format!("timer {correlation_id}"));
        Apply::Done
    }

    fn on_take_snapshot(&mut self, _: &ReadCx<'_>, point: SnapshotPoint) -> Box<dyn SnapshotSource> {
        self.events.push(format!("snapshot at {}", point.log_position));
        Box::new(BytesSource::new(self.hash.to_le_bytes().to_vec()))
    }

    fn on_role_change(&mut self, _: &ReadCx<'_>, role: Role) {
        self.events.push(format!("role {role:?}"));
    }

    fn on_terminate(&mut self, _: &ReadCx<'_>) {
        self.events.push("terminate".into());
    }
}

struct Harness {
    c: Container<Recorder>,
    cm: FakeCm,
    log: LogWriter,
    frames: Vec<Frame>,
    delivered: usize,
    egress: Vec<(i64, Vec<u8>)>,
    now: i64,
}

impl Harness {
    fn new(service: Recorder) -> Self {
        Self {
            c: Container::new(Config::default(), service),
            cm: FakeCm::default(),
            log: LogWriter::new(TERM_LENGTH, MTU),
            frames: Vec::new(),
            delivered: 0,
            egress: Vec::new(),
            now: 0,
        }
    }

    fn tick(&mut self) {
        self.now += 2_000_000;
        self.c.on_tick(Now {
            nanos: self.now,
            epoch_ms: 0,
        });
    }

    fn flush(&mut self) {
        let cm = &mut self.cm;
        self.c.drain_cm(|bytes| {
            cm.receive(bytes).unwrap();
            Offer::Sent
        });
        let egress = &mut self.egress;
        self.c.drain_egress(|session, bytes| {
            egress.push((session, bytes.to_vec()));
            Offer::Sent
        });
    }

    fn actions(&mut self) -> Vec<Action> {
        std::iter::from_fn(|| self.c.poll_action()).collect()
    }

    fn cm_sends(&mut self, message: &[u8]) {
        self.c.on_cm_message(message);
        self.c.after_cm_poll();
    }

    fn append(&mut self, message: &[u8]) -> i64 {
        self.frames.extend(self.log.append(message));
        self.log.position()
    }

    /// Aeron's bounded controlled poll: frames starting before `limit`, at
    /// most `fragment_limit`, stopping after a Break and before an Abort.
    fn poll(&mut self, limit: i64, fragment_limit: usize) -> usize {
        let Some(limit) = self.c.log_poll_limit(limit) else {
            return 0;
        };
        let mut count = 0;
        while count < fragment_limit && self.delivered < self.frames.len() {
            let frame = &self.frames[self.delivered];
            let start = frame.position - aligned_frame_length(frame.payload.len() as i32) as i64;
            if start >= limit {
                break;
            }
            match self.c.on_log_fragment(frame.fragment()) {
                PollAction::Abort => break,
                PollAction::Break => {
                    self.delivered += 1;
                    count += 1;
                    break;
                }
                PollAction::Continue => {
                    self.delivered += 1;
                    count += 1;
                }
            }
        }
        count
    }

    fn start_fresh(&mut self) {
        let recovery = RecoveryState {
            leadership_term_id: -1,
            log_position: 0,
            timestamp: 0,
            cluster_id: 0,
            snapshot_recording_ids: vec![],
        };
        self.c.on_counters(&recovery.to_key(), CLIENT_ID);
        self.tick();
    }

    fn join_as_leader(&mut self, log_position: i64) {
        self.cm_sends(&cm::join_log(
            log_position,
            i64::MAX,
            1,
            42,
            100,
            false,
            Role::Leader,
            "aeron:udp?endpoint=localhost:20113|alias=log",
        ));
        assert_eq!(
            self.actions(),
            vec![Action::JoinLog {
                channel: "aeron:udp?endpoint=localhost:20113|alias=log-sc-0".into(),
                stream_id: 100,
                session_id: 42,
            }]
        );
        self.c.on_io(Io::LogImage {
            join_position: log_position,
        });
    }
}

fn ack(log_position: i64, ack_id: i64, relevant_id: i64, timestamp: i64) -> ToCm {
    ToCm::Ack {
        log_position,
        timestamp,
        ack_id,
        relevant_id,
        service_id: 0,
    }
}

/// Runs a service from its start to its termination, through a snapshot, and
/// returns what a restart needs: the snapshot's messages, and where it was taken.
fn run_to_snapshot(h: &mut Harness) -> (Vec<Vec<u8>>, i64, i64) {
    h.start_fresh();
    h.flush();
    assert_eq!(
        h.cm.received,
        vec![ack(0, 0, CLIENT_ID, 0)],
        "started, with the client's id"
    );

    h.join_as_leader(0);
    h.flush();
    assert_eq!(h.cm.received[1], ack(0, 1, -1, 0), "the log joined");
    assert!(h.actions().contains(&Action::MemberId(1)));

    let term_at = h.log.position_after(log::NEW_LEADERSHIP_TERM_LENGTH);
    h.append(&log::new_leadership_term(0, term_at, 10, 1, 42));
    h.append(&log::session_open(0, 5, 11, 102, RESPONSE_CHANNEL));
    let hi_at = h.append(&log::session_message(0, 5, 12, b"hi"));
    h.append(&log::timer_event(0, 9, 13));
    let commit = h.log.position();
    assert_eq!(h.poll(commit, 50), 4);
    assert!(h.actions().contains(&Action::ConnectEgress {
        session_id: 5,
        channel: RESPONSE_CHANNEL.into(),
        stream_id: 102,
    }));
    h.flush();
    assert_eq!(h.egress.len(), 1);
    let (session, frame) = &h.egress[0];
    assert_eq!((*session, &frame[32..]), (5, &b"hi"[..]));
    assert_eq!(h.c.service().applied, vec![(b"hi".to_vec(), hi_at)]);
    assert_eq!(h.c.service().events, ["start", "role Leader", "open 5", "timer 9"]);

    let snapshot_at = h.log.position_after(log::SNAPSHOT_REQUEST_LENGTH);
    h.append(&log::snapshot_request(0, snapshot_at, 14));
    h.append(&log::session_message(0, 5, 15, b"after the snapshot"));
    let commit = h.log.position();
    assert_eq!(h.poll(commit, 50), 1, "the log stops at the snapshot");
    assert_eq!(h.c.log_poll_limit(commit), None, "and waits for it");
    assert_eq!(
        h.actions(),
        vec![Action::RecordSnapshot {
            channel: "aeron:ipc?alias=snapshot".into(),
            stream_id: 106
        }]
    );
    assert!(h.c.snapshot_frame().is_none(), "nothing to offer before the recording");
    h.c.on_io(Io::SnapshotPublication {
        recording_id: 3,
        max_payload_length: 1_376,
    });
    let mut snapshot = Vec::new();
    while let Some(frame) = h.c.snapshot_frame() {
        snapshot.push(frame.to_vec());
        h.c.snapshot_frame_sent();
    }
    assert_eq!(snapshot.len(), 4, "begin, the session, end, the service's 8 bytes");
    assert_eq!(snapshot[3], h.c.service().hash.to_le_bytes());
    assert_eq!(h.actions(), vec![Action::AwaitSnapshotRecorded]);
    h.c.on_io(Io::SnapshotRecorded);
    h.flush();
    assert_eq!(h.cm.received[2], ack(snapshot_at, 2, 3, 14), "acked with its recording");
    assert_eq!(h.actions(), vec![Action::CloseSnapshotRecording]);
    let hash_at_snapshot = h.c.service().hash;

    assert_eq!(h.poll(commit, 50), 1, "the log moves on");
    assert_eq!(h.c.service().applied.len(), 2);
    (snapshot, snapshot_at, hash_at_snapshot as i64)
}

#[test]
fn a_service_lives_from_start_to_termination() {
    let mut h = Harness::new(Recorder::default());
    run_to_snapshot(&mut h);
    let end = h.log.position();
    h.cm_sends(&cm::service_termination_position(end, true));
    assert_eq!(h.actions(), vec![Action::Stop { expected: true }]);
    assert!(h.c.is_stopped());
    h.flush();
    assert_eq!(h.cm.received.last(), Some(&ack(end, 3, -1, 15)));
    assert_eq!(h.c.service().events.last().map(String::as_str), Some("terminate"));
    assert!(h.c.is_flushed());
}

#[test]
fn a_service_restarts_from_its_snapshot() {
    let mut first = Harness::new(Recorder::default());
    let (snapshot, snapshot_at, hash) = run_to_snapshot(&mut first);

    let mut h = Harness::new(Recorder::default());
    let recovery = RecoveryState {
        leadership_term_id: 0,
        log_position: snapshot_at,
        timestamp: 14,
        cluster_id: 0,
        snapshot_recording_ids: vec![3],
    };
    h.c.on_counters(&recovery.to_key(), CLIENT_ID);
    assert_eq!(
        h.actions(),
        vec![Action::ReplaySnapshot {
            recording_id: 3,
            channel: "aeron:ipc".into(),
            stream_id: 103
        }]
    );
    let mut replay = LogWriter::new(TERM_LENGTH, MTU);
    for message in &snapshot {
        for frame in replay.append(message) {
            assert_eq!(h.c.on_snapshot_fragment(frame.fragment()), PollAction::Continue);
        }
    }
    h.c.on_snapshot_replay_end();
    assert_eq!(h.actions(), vec![Action::CloseSnapshotReplay]);
    h.tick();
    h.flush();
    assert_eq!(h.cm.received, vec![ack(snapshot_at, 0, CLIENT_ID, 14)]);
    assert_eq!(h.c.service().hash as i64, hash);
    assert_eq!(
        h.c.service().events,
        [format!("start from {snapshot_at} with 1 sessions")]
    );
    assert_eq!(h.c.log_position(), snapshot_at);
}

#[test]
fn a_snapshot_cut_short_fails_the_start() {
    let mut h = Harness::new(Recorder::default());
    let recovery = RecoveryState {
        leadership_term_id: 0,
        log_position: 4_096,
        timestamp: 14,
        cluster_id: 0,
        snapshot_recording_ids: vec![3],
    };
    h.c.on_counters(&recovery.to_key(), CLIENT_ID);
    h.actions();
    h.c.on_snapshot_replay_end();
    let actions = h.actions();
    assert!(actions.contains(&Action::Stop { expected: false }));
    h.flush();
    assert_eq!(
        h.cm.received,
        vec![ack(4_096, 0, -1, 14)],
        "the start is acked as failed"
    );
}

#[test]
fn a_join_at_another_position_is_refused_and_tried_again() {
    let mut h = Harness::new(Recorder::default());
    h.start_fresh();
    h.cm_sends(&cm::join_log(
        0,
        i64::MAX,
        1,
        42,
        100,
        false,
        Role::Follower,
        "aeron:ipc",
    ));
    h.actions();
    h.c.on_io(Io::LogImage { join_position: 64 });
    let actions = h.actions();
    assert!(matches!(actions[0], Action::Error { .. }));
    assert_eq!(actions[1], Action::CloseLog);
    h.c.after_cm_poll();
    assert!(
        matches!(h.actions()[0], Action::JoinLog { .. }),
        "the join is tried again"
    );
    h.flush();
    assert_eq!(h.cm.acks().len(), 1, "nothing acked for the refused join");
}

#[test]
fn nothing_for_the_consensus_module_is_lost_to_back_pressure() {
    let mut h = Harness::new(Recorder::default());
    h.start_fresh();
    h.join_as_leader(0);
    h.append(&log::session_message(0, -1, 1, b"timers:1000"));
    h.poll(h.log.position(), 50);
    let cm = &mut h.cm;
    let mut refusals = 0;
    while !h.c.is_flushed() {
        h.c.drain_cm(|bytes| {
            refusals += 1;
            if refusals % 7 == 0 {
                return Offer::Retry;
            }
            cm.receive(bytes).unwrap();
            Offer::Sent
        });
    }
    let timers: Vec<i64> = cm
        .received
        .iter()
        .filter_map(|m| match m {
            ToCm::ScheduleTimer { correlation_id, .. } => Some(*correlation_id),
            _ => None,
        })
        .collect();
    assert_eq!(timers, (0..1000).collect::<Vec<_>>());
}

#[test]
fn a_full_queue_to_the_consensus_module_holds_the_log() {
    let config = Config {
        cm_high_water: 10,
        ..Config::default()
    };
    let mut h = Harness::new(Recorder::default());
    h.c = Container::new(config, Recorder::default());
    h.start_fresh();
    h.join_as_leader(0);
    h.append(&log::session_message(0, -1, 1, b"timers:20"));
    h.append(&log::session_message(0, -1, 2, b"next"));
    let commit = h.log.position();
    assert_eq!(h.poll(commit, 50), 1, "the first message fills the queue");
    assert_eq!(h.c.log_poll_limit(commit), None);
    h.flush();
    assert_eq!(h.poll(commit, 50), 1, "drained, the log moves on");
}

#[test]
fn a_followers_egress_goes_nowhere() {
    let mut h = Harness::new(Recorder::default());
    h.start_fresh();
    h.cm_sends(&cm::join_log(
        0,
        i64::MAX,
        1,
        42,
        100,
        false,
        Role::Follower,
        "aeron:ipc",
    ));
    h.actions();
    h.c.on_io(Io::LogImage { join_position: 0 });
    h.append(&log::session_open(0, 5, 1, 102, RESPONSE_CHANNEL));
    h.append(&log::session_message(0, 5, 2, b"hi"));
    h.poll(h.log.position(), 50);
    h.flush();
    assert!(h.egress.is_empty());
    assert!(!h.actions().iter().any(|a| matches!(a, Action::ConnectEgress { .. })));
    assert_eq!(h.c.service().applied.len(), 1);
}

#[test]
fn the_log_ends_at_the_next_joins_position_and_the_member_follows() {
    let mut h = Harness::new(Recorder::default());
    h.start_fresh();
    h.join_as_leader(0);
    h.append(&log::session_message(0, -1, 1, b"a"));
    let end = h.log.position();
    h.cm_sends(&cm::join_log(
        end,
        i64::MAX,
        1,
        43,
        100,
        false,
        Role::Follower,
        "aeron:ipc",
    ));
    h.actions();
    h.poll(end, 50);
    h.c.on_log_idle(ImageStatus {
        position: end,
        end_of_stream: false,
        closed: false,
    });
    assert!(h.actions().contains(&Action::CloseLog));
    assert_eq!(h.c.role(), Role::Follower);
    h.c.after_cm_poll();
    assert!(matches!(h.actions()[0], Action::JoinLog { session_id: 43, .. }));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// Whatever the framing, however far each poll may go and however often
    /// the service asks again, each message is applied once, in order, at the
    /// position after its last frame, and the acks hold to Java's rule.
    #[test]
    fn the_log_is_applied_once_in_order(
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..6_000), 1..30),
        mtu in prop::sample::select(vec![1_408, 4_096, 8_192]),
        retries in prop::collection::vec(0u32..3, 30),
        steps in prop::collection::vec(1usize..8, 30),
        fragment_limit in 1usize..10,
    ) {
        let mut h = Harness::new(Recorder { retries, ..Recorder::default() });
        h.log = LogWriter::new(TERM_LENGTH, mtu);
        h.start_fresh();
        h.join_as_leader(0);
        let mut ends = Vec::new();
        for (i, payload) in payloads.iter().enumerate() {
            ends.push(h.append(&log::session_message(0, -1, i as i64, payload)));
        }
        // Commit a few messages at a time.
        let mut committed = 0;
        let mut step = steps.iter().cycle();
        while h.c.service().applied.len() < payloads.len() {
            committed = (committed + step.next().unwrap()).min(ends.len());
            let commit = ends[committed - 1];
            for _ in 0..1_000 {
                h.poll(commit, fragment_limit);
                h.flush();
            }
        }
        let applied = &h.c.service().applied;
        prop_assert_eq!(applied.len(), payloads.len());
        for (i, (payload, position)) in applied.iter().enumerate() {
            prop_assert_eq!(payload, &payloads[i]);
            prop_assert_eq!(*position, ends[i]);
        }
        prop_assert!(!h.cm.received.iter().any(|m| matches!(m, ToCm::ScheduleTimer { correlation_id: -1, .. })),
            "what a refused delivery sent is discarded");
    }
}
