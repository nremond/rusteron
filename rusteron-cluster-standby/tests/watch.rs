//! The standby's watch on its live log ([`rusteron_cluster_standby::watch`]): a
//! still log stopped after 5 s, unless it ends with the members' snapshot
//! action, held then 30 s at most; and with other timeouts.

use rusteron_cluster_codecs::cluster::cluster_action::ClusterAction;
use rusteron_cluster_standby::state::{FLAGS_DEFAULT, FLAGS_STANDBY_SNAPSHOT};
use rusteron_cluster_standby::watch::{LiveLogWatch, SNAPSHOT_HOLD, TIMEOUT, Verdict, is_members_snapshot};
use rusteron_consensus_core::log;
use std::time::Duration;

const S: i64 = 1_000_000_000;
const RECORDING: i64 = 7;
const TIMEOUT_NS: i64 = TIMEOUT.as_nanos() as i64;
const SNAPSHOT_HOLD_NS: i64 = SNAPSHOT_HOLD.as_nanos() as i64;

fn action(action: ClusterAction, flags: i32) -> Vec<u8> {
    log::cluster_action(1, 0, 4_096, 9, action, flags)
}

fn timer() -> Vec<u8> {
    log::timer_event(1, 3, 9)
}

#[test]
fn a_still_log_is_stopped_after_five_seconds_and_again_five_later() {
    let mut watch = LiveLogWatch::default();
    assert_eq!(watch.check(RECORDING, 1_000, 0), Verdict::Wait);
    assert_eq!(watch.check(RECORDING, 1_000, TIMEOUT_NS - 1), Verdict::Wait);
    assert_eq!(watch.check(RECORDING, 1_000, TIMEOUT_NS), Verdict::Stop);
    assert_eq!(watch.check(RECORDING, 1_000, TIMEOUT_NS + S), Verdict::Wait);
    assert_eq!(watch.check(RECORDING, 1_000, 2 * TIMEOUT_NS), Verdict::Stop);
}

#[test]
fn a_moving_log_or_one_begun_again_is_not_stopped() {
    let mut watch = LiveLogWatch::default();
    for i in 0..100 {
        assert_eq!(watch.check(RECORDING, 1_000 + 32 * i, i * S), Verdict::Wait);
    }
    assert_eq!(watch.check(RECORDING, 4_168, 103 * S), Verdict::Wait);
    // A new recording counter, at the same position: its 5 s from its start.
    assert_eq!(watch.check(RECORDING + 1, 4_168, 105 * S), Verdict::Wait);
    assert_eq!(watch.check(RECORDING + 1, 4_168, 109 * S), Verdict::Wait);
    assert_eq!(watch.check(RECORDING + 1, 4_168, 110 * S), Verdict::Stop);
}

#[test]
fn a_log_ending_with_the_members_snapshot_is_held_thirty_seconds_at_most() {
    let mut watch = LiveLogWatch::default();
    watch.on_tail(&timer(), true, 960);
    watch.on_tail(&action(ClusterAction::SNAPSHOT, FLAGS_DEFAULT), true, 1_024);
    assert_eq!(watch.check(RECORDING, 1_024, 0), Verdict::Wait);
    assert_eq!(watch.check(RECORDING, 1_024, TIMEOUT_NS), Verdict::HoldBegins);
    assert_eq!(watch.check(RECORDING, 1_024, SNAPSHOT_HOLD_NS - 1), Verdict::Hold);
    assert_eq!(watch.check(RECORDING, 1_024, SNAPSHOT_HOLD_NS), Verdict::Stop);
    // Held its longest: not again until the log moves.
    assert_eq!(
        watch.check(RECORDING, 1_024, SNAPSHOT_HOLD_NS + TIMEOUT_NS),
        Verdict::Stop
    );
    // The snapshot done, the log moves on; the next stillness is a source's.
    watch.on_tail(&timer(), true, 1_088);
    assert_eq!(watch.check(RECORDING, 1_088, 40 * S), Verdict::Wait);
    assert_eq!(watch.check(RECORDING, 1_088, 40 * S + TIMEOUT_NS), Verdict::Stop);
}

#[test]
fn only_the_members_snapshot_ending_the_log_holds_it() {
    let mut watch = LiveLogWatch::default();
    // A standby's snapshot, which the members skip, does not still them.
    watch.on_tail(&action(ClusterAction::SNAPSHOT, FLAGS_STANDBY_SNAPSHOT), true, 1_024);
    assert_eq!(watch.check(RECORDING, 1_024, 0), Verdict::Wait);
    assert_eq!(watch.check(RECORDING, 1_024, TIMEOUT_NS), Verdict::Stop);
    // The members' action, then more log: the log ends elsewhere.
    watch.on_tail(&action(ClusterAction::SNAPSHOT, FLAGS_DEFAULT), true, 1_088);
    watch.on_tail(&timer(), true, 1_152);
    assert_eq!(watch.check(RECORDING, 1_152, 20 * S), Verdict::Wait);
    assert_eq!(watch.check(RECORDING, 1_152, 20 * S + TIMEOUT_NS), Verdict::Stop);
    // The action ending the log, but the recording further on than the tail has read.
    watch.on_tail(&action(ClusterAction::SNAPSHOT, FLAGS_DEFAULT), true, 1_216);
    assert_eq!(watch.check(RECORDING, 1_280, 30 * S), Verdict::Wait);
    assert_eq!(watch.check(RECORDING, 1_280, 30 * S + TIMEOUT_NS), Verdict::Stop);
    // A fragment that begins no message is no action; a tail read again knows nothing.
    watch.on_tail(&action(ClusterAction::SNAPSHOT, FLAGS_DEFAULT), false, 1_344);
    assert_eq!(watch.check(RECORDING, 1_344, 40 * S), Verdict::Wait);
    assert_eq!(watch.check(RECORDING, 1_344, 40 * S + TIMEOUT_NS), Verdict::Stop);
    watch.on_tail(&action(ClusterAction::SNAPSHOT, FLAGS_DEFAULT), true, 1_408);
    watch.tail_restarted();
    assert_eq!(watch.check(RECORDING, 1_408, 50 * S), Verdict::Wait);
    assert_eq!(watch.check(RECORDING, 1_408, 50 * S + TIMEOUT_NS), Verdict::Stop);
}

#[test]
fn a_message_is_the_members_snapshot_action_only_when_it_is_one() {
    assert!(is_members_snapshot(&action(ClusterAction::SNAPSHOT, FLAGS_DEFAULT)));
    assert!(!is_members_snapshot(&action(ClusterAction::SUSPEND, FLAGS_DEFAULT)));
    assert!(!is_members_snapshot(&timer()));
    assert!(!is_members_snapshot(&[0; 4]));
}

#[test]
fn other_timeouts_and_holds_are_kept_through_resets() {
    // A cluster whose log stands still longer between entries: 30 s, and a snapshot hold of 2 minutes.
    let mut watch = LiveLogWatch::new(Duration::from_secs(30), Duration::from_secs(120));
    assert_eq!(watch.check(RECORDING, 1_024, 0), Verdict::Wait);
    assert_eq!(watch.check(RECORDING, 1_024, TIMEOUT_NS), Verdict::Wait);
    assert_eq!(watch.check(RECORDING, 1_024, 30 * S), Verdict::Stop);
    // The log moved: the watch starts again, its timeout still 30 s.
    watch.on_tail(&action(ClusterAction::SNAPSHOT, FLAGS_DEFAULT), true, 1_088);
    assert_eq!(watch.check(RECORDING, 1_088, 40 * S), Verdict::Wait);
    assert_eq!(watch.check(RECORDING, 1_088, 70 * S), Verdict::HoldBegins);
    assert_eq!(watch.check(RECORDING, 1_088, 150 * S), Verdict::Hold);
    assert_eq!(watch.check(RECORDING, 1_088, 160 * S), Verdict::Stop);
}
