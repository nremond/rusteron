//! Runs the cluster simulation over many seeds, on every core:
//!
//! ```text
//! cluster_sim [--seeds FROM..TO] [--seed N] [--fault-ms MS] [--settle-ms MS] [--threads N] [--trace] [--backup]
//!             [--held] [--leader-lost-during-replay] [--replay-rate N] [--service-rate N]
//! ```
//!
//! Prints each failing seed with its violations, then a summary. `--seed N`
//! runs one seed with its trace. `--backup` runs a cluster backup beside
//! the members. `--held` has partitions hold what they carry until they heal,
//! as Aeron's streams do (`SimConfig::held_partitions`). `--leader-lost-during-replay` runs that scenario instead of
//! random faults (a follower restarted with a log to replay at `--replay-rate`
//! messages a millisecond, its service applying `--service-rate`: 3 and 2 by
//! default there), and sums up how long the others took to replace the leader.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use rusteron_cluster_sim::{Report, SimConfig, leader_lost_during_replay, run};

fn fail(message: String) -> ! {
    eprintln!("{message}");
    std::process::exit(2);
}

fn main() {
    let mut config = SimConfig::default();
    let (mut from, mut to) = (0u64, 100u64);
    let mut threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    let mut scenario = false;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--leader-lost-during-replay" {
            scenario = true;
            config.fault_ms = 20_000;
            config.client_interval_ms = 2;
            config.local_replay_rate = 3;
            config.service_rate = 2;
            continue;
        }
        if flag == "--trace" {
            config.trace = true;
            continue;
        }
        if flag == "--backup" {
            config.backup = true;
            continue;
        }
        if flag == "--held" {
            config.held_partitions = true;
            continue;
        }
        let value = args.next().unwrap_or_else(|| fail(format!("{flag} needs a value")));
        let number = |v: &str| {
            v.parse::<u64>()
                .unwrap_or_else(|_| fail(format!("{flag}: not a number: {v}")))
        };
        match flag.as_str() {
            "--seeds" => {
                let (a, b) = value
                    .split_once("..")
                    .unwrap_or_else(|| fail("--seeds FROM..TO".into()));
                (from, to) = (number(a), number(b));
            }
            "--seed" => {
                from = number(&value);
                to = from + 1;
                config.trace = true;
            }
            "--fault-ms" => config.fault_ms = number(&value) as i64,
            "--settle-ms" => config.settle_ms = number(&value) as i64,
            "--threads" => threads = number(&value) as usize,
            "--replay-rate" => config.local_replay_rate = number(&value) as usize,
            "--service-rate" => config.service_rate = number(&value) as usize,
            other => fail(format!("unknown option {other}")),
        }
    }
    let next = AtomicU64::new(from);
    let failed: Mutex<Vec<Report>> = Mutex::new(Vec::new());
    let totals = Mutex::new((0usize, 0i64, 0usize, 0usize, 0i64));
    let replaced: Mutex<Vec<Option<i64>>> = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..threads.max(1) {
            scope.spawn(|| {
                loop {
                    let seed = next.fetch_add(1, Ordering::Relaxed);
                    if seed >= to {
                        break;
                    }
                    // A seed that panics is reported as failed, with its message.
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        if scenario {
                            let loss = leader_lost_during_replay(seed, &config);
                            replaced.lock().expect("replaced").push(loss.replaced_after_ms);
                            loss.report
                        } else {
                            run(seed, &config)
                        }
                    }));
                    let report = outcome.unwrap_or_else(|panic| {
                        let message = panic
                            .downcast_ref::<String>()
                            .cloned()
                            .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                            .unwrap_or_default();
                        Report {
                            seed,
                            violations: vec![format!("panicked: {message}")],
                            ..Report::default()
                        }
                    });
                    {
                        let mut t = totals.lock().expect("totals");
                        t.0 += 1;
                        t.1 += report.elections;
                        t.2 += report.crashes;
                        t.3 += report.partitions;
                        t.4 = t.4.max(report.backup_catch_up_ms);
                    }
                    if config.trace {
                        for line in &report.trace {
                            println!("{line}");
                        }
                    }
                    if !report.passed() {
                        println!("seed {seed}: FAILED");
                        for v in &report.violations {
                            println!("  {v}");
                        }
                        failed.lock().expect("failed").push(report);
                    }
                }
            });
        }
    });
    let failed = failed.into_inner().expect("failed");
    let (runs, elections, crashes, partitions, catch_up_ms) = totals.into_inner().expect("totals");
    let mut seeds: Vec<u64> = failed.iter().map(|r| r.seed).collect();
    seeds.sort_unstable();
    let backup = if config.backup {
        format!("; the backup caught up within {catch_up_ms} ms of the end")
    } else {
        String::new()
    };
    println!(
        "{runs} seeds, {} failed {seeds:?}; {elections} elections, {crashes} crashes, {partitions} partitions{backup}",
        seeds.len()
    );
    if scenario {
        let replaced = replaced.into_inner().expect("replaced");
        let mut ms: Vec<i64> = replaced.iter().flatten().copied().collect();
        ms.sort_unstable();
        let at = |q: usize| ms.get((ms.len().saturating_sub(1)) * q / 100).copied().unwrap_or(-1);
        println!(
            "leader replaced after {} / {} / {} / {} ms (min / median / p99 / max), never in {} runs",
            at(0),
            at(50),
            at(99),
            at(100),
            replaced.len() - ms.len()
        );
    }
    std::process::exit(i32::from(!seeds.is_empty()));
}
