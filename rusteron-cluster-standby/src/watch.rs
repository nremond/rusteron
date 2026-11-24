//! The standby's watch on its backup's live log recording.
//!
//! The backup resets once that recording ends, and a dead source's replay
//! keeps it open until the driver times the image out. A recording still
//! for the watch's timeout ([`TIMEOUT`] by default) has the module stop it,
//! for the backup to reset and ask a member again. The timeout must be
//! longer than the cluster's log ever stands still while its source is
//! alive: a cluster whose timers fire every 50 ms moves its log at least
//! that often, and an idle one with no timer does not move at all, so a
//! standby of such a cluster resets once each timeout to no harm but noise.
//!
//! A cluster taking a snapshot stands still too, its log ending with the
//! members' snapshot action: a reset then only costs time. So while the log
//! ends with that action the watch holds off, the snapshot hold at most
//! ([`SNAPSHOT_HOLD`] by default) from when the log stopped: longer than
//! the members' snapshots take. A source that dies meanwhile is still let
//! go of: its image times out, 3 s on a standby's driver (`node_driver
//! --standby`), and the recording ends.
//!
//! What the log ends with is read on a replay of the recording of its own
//! ([`LiveLogWatch::on_tail`]), ahead of the service, whose replay the
//! module reads only as fast as the service: a standby behind would
//! otherwise know of the snapshot too late.

use std::time::Duration;

use rusteron_cluster_codecs::cluster::cluster_action::ClusterAction;
use rusteron_consensus_core::log::{self, LogMessage};

use crate::state::FLAGS_DEFAULT;

/// How long the live log may stand still, by default.
pub const TIMEOUT: Duration = Duration::from_secs(5);
/// How long a log ending with the members' snapshot action may stand
/// still, by default.
pub const SNAPSHOT_HOLD: Duration = Duration::from_secs(30);

/// What the module does about its live log recording.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Nothing: it moves, or has not stood still long enough.
    Wait,
    /// Nothing, as the members take their snapshot: the first time at this position.
    HoldBegins,
    /// Nothing, as the members take their snapshot.
    Hold,
    /// Stop the recording, for the backup to reset; asked again a timeout later if it is still.
    Stop,
}

/// The watch: [`check`](Self::check) the recording each duty cycle, and
/// feed it the log's tail.
#[derive(Debug)]
pub struct LiveLogWatch {
    timeout_ns: i64,
    hold_ns: i64,
    /// The recording watched (its counter's registration), where it stands, since when, and the timer.
    recording: Option<i64>,
    position: i64,
    still_since_ns: i64,
    timer_ns: i64,
    /// Where the last entry of the log read ends, if it is the members' snapshot action.
    snapshot_end: Option<i64>,
    held: bool,
    hold_over: bool,
}

impl Default for LiveLogWatch {
    fn default() -> Self {
        Self::new(TIMEOUT, SNAPSHOT_HOLD)
    }
}

impl LiveLogWatch {
    /// A watch that stops a recording still for `timeout`, and holds off
    /// `snapshot_hold` at most while the log ends with the members' snapshot action.
    pub fn new(timeout: Duration, snapshot_hold: Duration) -> Self {
        let ns = |d: Duration| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX);
        Self {
            timeout_ns: ns(timeout),
            hold_ns: ns(snapshot_hold),
            recording: None,
            position: 0,
            still_since_ns: 0,
            timer_ns: 0,
            snapshot_end: None,
            held: false,
            hold_over: false,
        }
    }

    /// One fragment of the log's tail, ending at `position`; `first` if it begins a message.
    pub fn on_tail(&mut self, fragment: &[u8], first: bool, position: i64) {
        self.snapshot_end = (first && is_members_snapshot(fragment)).then_some(position);
    }

    /// The tail read again, from elsewhere: nothing is known of where the log ends.
    pub fn tail_restarted(&mut self) {
        self.snapshot_end = None;
    }

    /// The verdict at `now_ns` on `recording` (its counter's registration),
    /// recorded to `recorded`. A recording begun again is given the time from
    /// its start: the backup extends it before its replay comes.
    pub fn check(&mut self, recording: i64, recorded: i64, now_ns: i64) -> Verdict {
        if self.recording != Some(recording) || recorded != self.position {
            *self = LiveLogWatch {
                timeout_ns: self.timeout_ns,
                hold_ns: self.hold_ns,
                recording: Some(recording),
                position: recorded,
                still_since_ns: now_ns,
                timer_ns: now_ns,
                snapshot_end: self.snapshot_end,
                held: false,
                hold_over: false,
            };
            return Verdict::Wait;
        }
        if now_ns - self.timer_ns < self.timeout_ns {
            return Verdict::Wait;
        }
        if !self.hold_over && self.snapshot_end == Some(recorded) && now_ns - self.still_since_ns < self.hold_ns {
            let begins = !self.held;
            self.held = true;
            return if begins { Verdict::HoldBegins } else { Verdict::Hold };
        }
        // Held its longest, a hold is over until the log moves again.
        self.hold_over = self.held;
        self.timer_ns = now_ns;
        Verdict::Stop
    }
}

/// Whether a log message is the members' snapshot action: a snapshot's,
/// with the members' flags.
pub fn is_members_snapshot(message: &[u8]) -> bool {
    matches!(
        log::decode(message),
        Ok(LogMessage::ClusterAction {
            action: ClusterAction::SNAPSHOT,
            flags: FLAGS_DEFAULT,
            ..
        })
    )
}

/// The time of day, UTC, to the ms: for the standby's lines to be read beside other logs' times.
pub fn clock() -> String {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let day_ms = ms % 86_400_000;
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        day_ms / 3_600_000,
        day_ms / 60_000 % 60,
        day_ms / 1_000 % 60,
        day_ms % 1_000
    )
}
