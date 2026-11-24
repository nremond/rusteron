//! A node's consensus module, in Rust, on the node's media driver:
//!
//! ```text
//! consensus_module --aeron-dir DIR --cluster-dir DIR --cluster-members MEMBERS --cluster-member-id N \
//!     --ingress-channel URI [--service-count N] [--time-unit ms|us|ns] [--spin] ...
//! ```
//!
//! The flags are Java's `ConsensusModule.Context` options by name. It prints
//! `READY` once started and `EXIT` with how it ended, and stops at SIGTERM or
//! SIGINT as Java's does at close. `CM_DEBUG=1` logs at debug level.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rusteron_archive::channel_uri::parse_size;
use rusteron_cluster::Idle;
use rusteron_cluster::consensus::{ConsensusContext, ConsensusModuleNode};
use rusteron_consensus_core::config::{Config, TimeUnit};

struct StderrLogger;

impl log::Log for StderrLogger {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }
    fn log(&self, record: &log::Record) {
        eprintln!("{} - {}", record.level(), record.args());
    }
    fn flush(&self) {}
}

fn fail(message: String) -> ! {
    eprintln!("consensus_module: {message}");
    std::process::exit(2);
}

fn duration_ns(flag: &str, value: &str) -> i64 {
    let (number, unit) = value
        .trim()
        .split_at(value.trim().find(|c: char| !c.is_ascii_digit()).unwrap_or(value.len()));
    let n: i64 = number
        .parse()
        .unwrap_or_else(|_| fail(format!("{flag}: not a duration: {value}")));
    match unit {
        "" | "ns" => n,
        "us" => n * 1_000,
        "ms" => n * 1_000_000,
        "s" => n * 1_000_000_000,
        other => fail(format!("{flag}: unknown unit {other}")),
    }
}

fn main() {
    static LOGGER: StderrLogger = StderrLogger;
    let level = if std::env::var_os("CM_DEBUG").is_some() {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };
    let _ = log::set_logger(&LOGGER).map(|()| log::set_max_level(level));

    let mut config = Config::default();
    let mut aeron_dir = String::new();
    let mut cluster_dir = String::from("cluster");
    let mut idle = Idle::Backoff;
    let mut archive_channel = None;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--spin" {
            idle = Idle::Spin;
            continue;
        }
        if flag == "--accept-standby-snapshots" {
            config.accept_standby_snapshots = true;
            continue;
        }
        let Some(value) = args.next() else {
            fail(format!("{flag} needs a value"));
        };
        let number = |v: &str| {
            v.parse::<i64>()
                .unwrap_or_else(|_| fail(format!("{flag}: not a number: {v}")))
        };
        match flag.as_str() {
            "--aeron-dir" => aeron_dir = value,
            "--cluster-dir" => cluster_dir = value,
            "--cluster-id" => config.cluster_id = number(&value) as i32,
            "--cluster-member-id" => config.member_id = number(&value) as i32,
            "--cluster-members" => config.cluster_members = value,
            "--service-count" => config.service_count = number(&value) as usize,
            "--app-version" => config.app_version = number(&value) as i32,
            "--time-unit" => {
                config.time_unit = match value.as_str() {
                    "ms" => TimeUnit::Millis,
                    "us" => TimeUnit::Micros,
                    "ns" => TimeUnit::Nanos,
                    other => fail(format!("--time-unit: ms, us or ns, not {other}")),
                }
            }
            "--file-sync-level" => config.file_sync_level = number(&value) as i32,
            "--ingress-channel" => config.ingress_channel = value,
            "--ingress-stream-id" => config.ingress_stream_id = number(&value) as i32,
            "--log-channel" => config.log_channel = value,
            "--log-stream-id" => config.log_stream_id = number(&value) as i32,
            "--consensus-channel" => config.consensus_channel = value,
            "--consensus-stream-id" => config.consensus_stream_id = number(&value) as i32,
            "--control-channel" => config.control_channel = value,
            "--follower-catchup-channel" => config.follower_catchup_channel = value,
            "--appointed-leader-id" => config.appointed_leader_id = number(&value) as i32,
            "--replication-channel" => config.replication_channel = value,
            "--leader-archive-control-channel" => config.leader_archive_control_channel = value,
            "--archive-control-channel" => archive_channel = Some(value),
            "--archive-control-stream-id" => config.archive_control_request_stream_id = number(&value) as i32,
            "--max-concurrent-sessions" => config.max_concurrent_sessions = number(&value) as usize,
            "--session-timeout" => config.session_timeout_ns = duration_ns(&flag, &value),
            "--leader-heartbeat-timeout" => config.leader_heartbeat_timeout_ns = duration_ns(&flag, &value),
            "--leader-heartbeat-interval" => config.leader_heartbeat_interval_ns = duration_ns(&flag, &value),
            "--election-timeout" => config.election_timeout_ns = duration_ns(&flag, &value),
            "--election-status-interval" => config.election_status_interval_ns = duration_ns(&flag, &value),
            "--startup-canvass-timeout" => config.startup_canvass_timeout_ns = duration_ns(&flag, &value),
            "--termination-timeout" => config.termination_timeout_ns = duration_ns(&flag, &value),
            "--standby-snapshot-notification-delay" => {
                config.standby_snapshot_notification_delay_ns = duration_ns(&flag, &value)
            }
            "--error-buffer-length" => {
                let _ = parse_size(&flag, &value).unwrap_or_else(|e| fail(e));
            }
            other => fail(format!("unknown option {other}")),
        }
    }
    config.random_seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1);
    let mut ctx = ConsensusContext::new(aeron_dir, cluster_dir, config);
    ctx.idle = idle;
    if let Some(channel) = archive_channel {
        ctx.archive_control_request_channel = channel.clone();
        ctx.archive_control_response_channel = channel;
    }

    let running = Arc::new(AtomicBool::new(true));
    let flag = running.clone();
    if let Err(e) = ctrlc::set_handler(move || flag.store(false, Ordering::Release)) {
        fail(format!("signal handler: {e}"));
    }
    let member_id = ctx.config.member_id;
    let node = ConsensusModuleNode::launch(ctx).unwrap_or_else(|e| fail(format!("failed to launch: {e}")));
    println!("READY member={member_id}");
    let expected = node.run(|| running.load(Ordering::Acquire));
    println!("EXIT {}", if expected { "Terminated" } else { "Failed" });
    std::process::exit(if expected { 0 } else { 1 });
}
