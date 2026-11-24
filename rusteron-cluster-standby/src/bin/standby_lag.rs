//! How long after the cluster commits a log position the standby has it to
//! apply, until the process is stopped (SIGTERM or SIGINT), which prints the
//! result.
//!
//! ```text
//! standby_lag [--hosts H0,H1,...|--members M] [--dir DIR] [--aeron-dir DIR] [--standby N] [--every-us 100]
//! ```
//!
//! It reads the commit position counters of the members' media drivers and
//! the standby's from their CnC files, where the layout puts them
//! ([`rusteron_cluster_tools::layout`]), every `--every-us`, as a
//! monitoring tool reads a driver's counters: no client of any driver. Each
//! advance of the furthest member's, the leader's, is timed until the
//! standby's reaches it. All of them on this machine, so on one clock; a
//! delay is within a sampling period.
//!
//! Timing starts once the standby has the leader's commit position, so a
//! standby still catching up on what was committed before it started does
//! not count. Until then the probe notes how far behind the standby is, in
//! bytes of log: a standby that cannot keep up (one far away, its window too
//! small for the round trip) is level only once the load stops, if ever.
//! The standby's position is its module's (`cluster_standby`), what its
//! service may apply: its service applies it next, as a member's does its
//! own commit position.

use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rusteron_archive::{AeronCnc, IntoCString};
use rusteron_cluster::container::find_counter;
use rusteron_cluster_core::counters::COMMIT_POSITION_TYPE_ID;
use rusteron_cluster_tools::args::Args;
use rusteron_cluster_tools::layout::Layout;

/// The commit position counter in a media driver's counters, read from its CnC file.
struct CommitPosition {
    cnc: AeronCnc,
    counter_id: i32,
}

impl CommitPosition {
    fn open(aeron_dir: &Path) -> Result<Self, String> {
        let dir = aeron_dir.to_string_lossy().into_owned();
        let cnc = AeronCnc::open(&dir.as_str().into_c_string()).map_err(|e| format!("{dir}: {e}"))?;
        let found = find_counter(&cnc.counters_reader(), |type_id, _| type_id == COMMIT_POSITION_TYPE_ID);
        let Some((counter_id, _)) = found else {
            return Err(format!("no commit position counter in {dir}"));
        };
        Ok(Self { cnc, counter_id })
    }

    fn get(&self) -> i64 {
        self.cnc
            .counters_reader()
            .get_counter_value(self.counter_id)
            .unwrap_or(0)
    }
}

/// The value at `quantile` of `sorted`: the one at that fraction of the way, rounded down.
fn at(sorted: &[i64], quantile: f64) -> i64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((quantile * sorted.len() as f64).floor() as usize).min(sorted.len() - 1);
    sorted[index]
}

fn main() -> ExitCode {
    let args = Args::from_env();
    let parsed = Layout::parse(&args).and_then(|layout| {
        let standby: usize = args.number("--standby", 0)?;
        let every_us: u64 = args.number("--every-us", 100)?;
        let members = (0..layout.members())
            .map(|i| layout.aeron_dir(&layout.member_dir(i)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((
            members,
            layout.aeron_dir(&layout.standby_dir(standby))?,
            Duration::from_micros(every_us),
        ))
    });
    let (member_dirs, standby_dir, every) = match parsed {
        Ok(parsed) => parsed,
        Err(e) => {
            eprintln!(
                "standby_lag: {e}; usage: standby_lag [--hosts H0,H1,...|--members M] [--dir DIR] [--aeron-dir DIR] \
                 [--standby N] [--every-us 100]"
            );
            return ExitCode::from(2);
        }
    };
    let opened: Result<Vec<CommitPosition>, String> = member_dirs.iter().map(|d| CommitPosition::open(d)).collect();
    let (members, standby) = match (opened, CommitPosition::open(&standby_dir)) {
        (Ok(members), Ok(standby)) => (members, standby),
        (Err(e), _) | (_, Err(e)) => {
            eprintln!("standby_lag: {e}");
            return ExitCode::from(1);
        }
    };

    let running = Arc::new(AtomicBool::new(true));
    let stop = running.clone();
    if let Err(e) = ctrlc::set_handler(move || stop.store(false, Ordering::Release)) {
        eprintln!("standby_lag: no signal handler: {e}");
        return ExitCode::from(1);
    }

    // The leader's commit positions not yet the standby's, and when the leader had each, oldest first.
    let mut pending: Vec<(i64, Instant)> = Vec::new();
    let mut pending_head = 0;
    let mut delays: Vec<i64> = Vec::new();
    let mut behind: Vec<i64> = Vec::new();
    let mut last_leader = -1;
    let probe_started_at = Instant::now();
    let mut max_behind_before = 0;
    let mut started_at: Option<Instant> = None;
    let mut samples: u64 = 0;
    let mut leader = 0;
    let mut standby_at = 0;
    while running.load(Ordering::Acquire) {
        let now = Instant::now();
        leader = members.iter().map(CommitPosition::get).max().unwrap_or(0);
        standby_at = standby.get();
        match started_at {
            None => {
                if standby_at >= leader && leader > 0 {
                    started_at = Some(now);
                    last_leader = leader;
                    println!(
                        "probe      level at log position {leader} after {:.1} s, at most {max_behind_before} bytes of log \
                         behind until then; timing from here",
                        (now - probe_started_at).as_secs_f64()
                    );
                } else {
                    max_behind_before = max_behind_before.max(leader - standby_at);
                }
            }
            Some(_) => {
                samples += 1;
                if leader > last_leader {
                    pending.push((leader, now));
                    last_leader = leader;
                }
                while pending_head < pending.len() && pending[pending_head].0 <= standby_at {
                    delays.push((now - pending[pending_head].1).as_nanos() as i64);
                    pending_head += 1;
                }
                behind.push((leader - standby_at).max(0));
            }
        }
        if let Some(rest) = every.checked_sub(now.elapsed()) {
            std::thread::sleep(rest);
        }
    }

    if started_at.is_none() {
        println!(
            "probe      never level over {:.1} s: at most {max_behind_before} bytes of log behind, {} at the end",
            probe_started_at.elapsed().as_secs_f64(),
            leader - standby_at
        );
    }
    delays.sort_unstable();
    behind.sort_unstable();
    println!(
        "probe      {samples} samples over {:.1} s; the leader's commit position at {leader}, the standby's at {standby_at}; \
         {} advances not yet the standby's",
        started_at.map_or(0.0, |at| at.elapsed().as_secs_f64()),
        pending.len() - pending_head
    );
    println!(
        "standby    has a committed position after, in ms: p50 {:.2} | p99 {:.2} | p99.9 {:.2} | max {:.2} \
         ({} advances of the leader's commit position)",
        at(&delays, 0.50) as f64 / 1e6,
        at(&delays, 0.99) as f64 / 1e6,
        at(&delays, 0.999) as f64 / 1e6,
        at(&delays, 1.0) as f64 / 1e6,
        delays.len()
    );
    println!(
        "standby    behind the leader's commit position, in bytes of log: p50 {} | p99 {} | max {}",
        at(&behind, 0.50),
        at(&behind, 0.99),
        at(&behind, 1.0)
    );
    ExitCode::SUCCESS
}
