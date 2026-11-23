//! A cluster backup, in Rust, on its node's media driver and archive:
//!
//! ```text
//! cluster_backup --aeron-dir DIR --cluster-dir DIR --cluster-consensus-endpoints HOST:PORT,... \
//!     --consensus-channel URI --catchup-endpoint HOST:PORT [--source-type ANY|LEADER|FOLLOWER] ...
//! ```
//!
//! The flags are Java's `ClusterBackup.Context` options by name; durations
//! take a unit (`ns`, `us`, `ms`, `s`). Two are not Java's
//! (`docs/backup.md` §4): `--live-log-progress-timeout DURATION` and
//! `--retrieve-snapshots false`. `--credentials TEXT` and
//! `--challenge-response TEXT` are what it sends an authenticator. It
//! prints `READY` once started and `EXIT` with how it ended, and stops at
//! SIGTERM or SIGINT as Java's does at close. `--progress` prints its
//! listener's events: `RESPONSE member=N`, and `PROGRESS recordingId=R
//! position=P` as the live log grows. `BACKUP_DEBUG=1` logs at debug level.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rusteron_archive_core::channel::parse_size;
use rusteron_cluster::Idle;
use std::io::Write;

use rusteron_cluster::backup::{
    BackupContext, BackupListener, ClusterBackupNode, ClusterMember, Exit, ReplayStart, Snapshot, SourceType,
};

/// Prints the backup's events, for scripts to follow it.
struct Printer {
    on: bool,
}

impl Printer {
    fn line(&self, text: std::fmt::Arguments) {
        if self.on {
            let mut out = std::io::stdout().lock();
            let _ = writeln!(out, "{text}");
            let _ = out.flush();
        }
    }
}

impl BackupListener for Printer {
    fn on_backup_response(&mut self, _: &[ClusterMember], log_source_member: &ClusterMember, snapshots: &[Snapshot]) {
        self.line(format_args!(
            "RESPONSE member={} snapshots={}",
            log_source_member.id,
            snapshots.len()
        ));
    }

    fn on_live_log_progress(&mut self, recording_id: i64, _: i32, log_position: i64) {
        self.line(format_args!(
            "PROGRESS recordingId={recording_id} position={log_position}"
        ));
    }
}

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
    eprintln!("cluster_backup: {message}");
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
    let level = if std::env::var_os("BACKUP_DEBUG").is_some() {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };
    let _ = log::set_logger(&LOGGER).map(|()| log::set_max_level(level));

    let mut ctx = BackupContext::new("", "cluster-backup");
    let mut printer = Printer { on: false };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--spin" => {
                ctx.idle = Idle::Spin;
                continue;
            }
            "--delete-dir-on-start" => {
                ctx.delete_dir_on_start = true;
                continue;
            }
            "--progress" => {
                printer.on = true;
                continue;
            }
            _ => {}
        }
        let Some(value) = args.next() else {
            fail(format!("{flag} needs a value"));
        };
        let number = |v: &str| {
            v.parse::<i64>()
                .unwrap_or_else(|_| fail(format!("{flag}: not a number: {v}")))
        };
        let c = &mut ctx.config;
        match flag.as_str() {
            "--aeron-dir" => ctx.aeron_dir = value,
            "--cluster-dir" => ctx.cluster_dir = value.into(),
            "--mark-file-dir" => ctx.mark_file_dir = Some(value.into()),
            "--cluster-id" => c.cluster_id = number(&value) as i32,
            "--consensus-channel" => c.consensus_channel = value,
            "--consensus-stream-id" => c.consensus_stream_id = number(&value) as i32,
            "--cluster-consensus-endpoints" => c.cluster_consensus_endpoints = value,
            "--log-stream-id" => c.log_stream_id = number(&value) as i32,
            "--catchup-endpoint" => c.catchup_endpoint = value,
            "--catchup-channel" => c.catchup_channel = value,
            "--cluster-backup-interval" => c.backup_interval_ns = duration_ns(&flag, &value),
            "--cluster-backup-response-timeout" => c.response_timeout_ns = duration_ns(&flag, &value),
            "--cluster-backup-progress-timeout" => c.progress_timeout_ns = duration_ns(&flag, &value),
            "--cluster-backup-cool-down-interval" => c.cool_down_interval_ns = duration_ns(&flag, &value),
            "--replication-progress-timeout" => c.replication_progress_timeout_ns = duration_ns(&flag, &value),
            "--live-log-progress-timeout" => c.live_log_progress_timeout_ns = duration_ns(&flag, &value),
            "--retrieve-snapshots" => {
                c.retrieve_snapshots = value.parse().unwrap_or_else(|_| fail(format!("{flag}: true or false")));
            }
            "--replication-progress-interval" => c.replication_progress_interval_ns = duration_ns(&flag, &value),
            "--source-type" => c.source_type = SourceType::parse(&value).unwrap_or_else(|e| fail(e)),
            "--initial-replay-start" => {
                c.initial_replay_start = ReplayStart::parse(&value).unwrap_or_else(|e| fail(e));
            }
            "--archive-control-request-channel" => c.local_archive.request_channel = value,
            "--archive-control-request-stream-id" => c.local_archive.request_stream_id = number(&value) as i32,
            "--archive-control-response-channel" => c.local_archive.response_channel = value,
            "--archive-control-response-stream-id" => c.local_archive.response_stream_id = number(&value) as i32,
            "--cluster-archive-control-request-channel" => c.cluster_archive.request_channel = value,
            "--cluster-archive-control-request-stream-id" => {
                c.cluster_archive.request_stream_id = number(&value) as i32;
            }
            "--cluster-archive-control-response-channel" => c.cluster_archive.response_channel = value,
            "--cluster-archive-control-response-stream-id" => {
                c.cluster_archive.response_stream_id = number(&value) as i32;
            }
            "--credentials" => ctx.credentials.credentials = value.into_bytes(),
            "--challenge-response" => ctx.credentials.challenge_response = value.into_bytes(),
            "--error-buffer-length" => {
                ctx.error_buffer_length = parse_size(&flag, &value).unwrap_or_else(|e| fail(e)) as usize;
            }
            "--file-page-size" => ctx.file_page_size = parse_size(&flag, &value).unwrap_or_else(|e| fail(e)) as usize,
            other => fail(format!("unknown option {other}")),
        }
    }
    ctx.config.random_seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1);

    let running = Arc::new(AtomicBool::new(true));
    let flag = running.clone();
    if let Err(e) = ctrlc::set_handler(move || flag.store(false, Ordering::Release)) {
        fail(format!("signal handler: {e}"));
    }
    let node = ClusterBackupNode::launch(ctx, printer).unwrap_or_else(|e| fail(format!("failed to launch: {e}")));
    println!("READY");
    match node.run(|| running.load(Ordering::Acquire)) {
        Exit::Stopped => {
            println!("EXIT Stopped");
            std::process::exit(0);
        }
        Exit::Terminated(reason) => {
            println!("EXIT Terminated: {reason}");
            std::process::exit(1);
        }
    }
}
