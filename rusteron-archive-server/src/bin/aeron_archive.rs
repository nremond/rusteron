//! Aeron Archive's server, in Rust, on a running media driver:
//!
//! ```text
//! aeron_archive [--aeron-dir DIR] [--archive-dir DIR] [--mark-file-dir DIR] [--archive-id N]
//!               [--control-channel URI] [--control-stream-id N]
//!               [--local-control-channel URI] [--local-control-stream-id N]
//!               [--replication-channel URI] [--archive-client-response-channel URI]
//!               [--segment-file-length SIZE] [--file-io-max-length SIZE]
//!               [--file-sync-level N] [--catalog-file-sync-level N]
//!               [--max-concurrent-recordings N] [--max-concurrent-replays N] [--spin]
//!               [--truncate-torn-fragments] [--verify]
//! ```
//!
//! The options are Java's `aeron.archive.*` properties; sizes take `k`, `m`
//! or `g`. `--truncate-torn-fragments` cuts off a recording's last fragment
//! that a crash left half written across a page, as `ArchiveTool verify`
//! does when told to, where Java's archive refuses to start. `--verify`
//! checks every recording at start as `ArchiveTool verify` does, marking a
//! damaged one invalid (`rusteron_archive_core::verify`). Prints `READY
//! archiveId=N` once serving, and stops on SIGINT or SIGTERM: exits 0 then, 2
//! when it could not start.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rusteron_archive_core::channel::parse_size;
use rusteron_archive_server::{Archive, ArchiveContext, Idle};

struct StderrLogger;

impl log::Log for StderrLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!("{} {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

fn default_aeron_dir() -> String {
    if let Ok(dir) = std::env::var("AERON_DIR") {
        return dir;
    }
    let user = std::env::var("USER").unwrap_or_else(|_| "default".into());
    if cfg!(target_os = "linux") {
        format!("/dev/shm/aeron-{user}")
    } else {
        format!("{}/aeron-{user}", std::env::temp_dir().display())
    }
}

fn fail(message: String) -> ! {
    eprintln!("{message}");
    std::process::exit(2);
}

fn main() {
    static LOGGER: StderrLogger = StderrLogger;
    // ARCHIVE_DEBUG=1: every action of the conductor's, on stderr.
    let level = if std::env::var_os("ARCHIVE_DEBUG").is_some() {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };
    let _ = log::set_logger(&LOGGER).map(|()| log::set_max_level(level));

    let mut ctx = ArchiveContext::new(default_aeron_dir(), "archive");
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--spin" {
            ctx.idle = Idle::Spin;
            continue;
        }
        if flag == "--truncate-torn-fragments" {
            ctx.truncate_torn_fragments = true;
            continue;
        }
        if flag == "--verify" {
            ctx.verify = true;
            continue;
        }
        let Some(value) = args.next() else {
            fail(format!("{flag} needs a value"));
        };
        let number = |v: &str| {
            v.parse::<i64>()
                .unwrap_or_else(|_| fail(format!("{flag}: not a number: {v}")))
        };
        let size = |v: &str| parse_size(&flag, v).unwrap_or_else(|e| fail(e));
        match flag.as_str() {
            "--aeron-dir" => ctx.aeron_dir = value,
            "--archive-dir" => ctx.archive_dir = value.into(),
            "--mark-file-dir" => ctx.mark_file_dir = Some(value.into()),
            "--archive-id" => ctx.archive_id = Some(number(&value)),
            "--control-channel" => ctx.control_channel = Some(value),
            "--control-stream-id" => ctx.control_stream_id = number(&value) as i32,
            "--local-control-channel" => ctx.local_control_channel = value,
            "--local-control-stream-id" => ctx.local_control_stream_id = number(&value) as i32,
            "--replication-channel" => ctx.replication_channel = value,
            "--archive-client-response-channel" => ctx.archive_client_response_channel = value,
            "--file-io-max-length" => ctx.config.file_io_max_length = size(&value) as i32,
            "--segment-file-length" => ctx.config.segment_file_length = size(&value) as i32,
            "--file-sync-level" => ctx.file_sync_level = number(&value) as i32,
            "--catalog-file-sync-level" => ctx.catalog_file_sync_level = number(&value) as i32,
            "--max-concurrent-recordings" => ctx.config.max_concurrent_recordings = number(&value) as i32,
            "--max-concurrent-replays" => ctx.config.max_concurrent_replays = number(&value) as i32,
            other => fail(format!("unknown option {other}")),
        }
    }

    let running = Arc::new(AtomicBool::new(true));
    let flag = running.clone();
    if let Err(e) = ctrlc::set_handler(move || flag.store(false, Ordering::Release)) {
        fail(format!("signal handler: {e}"));
    }
    let archive = Archive::launch(ctx).unwrap_or_else(|e| fail(format!("failed to launch: {e}")));
    println!("READY archiveId={}", archive.archive_id());
    archive.run(|| running.load(Ordering::Acquire));
    println!("EXIT");
}
