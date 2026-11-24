//! A node's own process: Aeron's C media driver, for a node whose archive,
//! consensus module and service run as processes of their own beside it
//! (`scripts/cluster.sh`), or a client's driver.
//!
//! ```text
//! node_driver --node N [--hosts H0,H1,...|--members M] [--dir DIR] [--aeron-dir DIR] [--spin]
//!             [--threading SHARED|SHARED_NETWORK|DEDICATED] [anything else, ignored]
//! node_driver --standby N [--host HOST] [the same]
//! ```
//!
//! The node's files and its driver's directory follow the layout
//! ([`rusteron_cluster_tools::layout`]): member `N`'s in `node<N>`, standby
//! `N`'s in `standby<N>`, in the slot after the members'. It ignores the
//! options it has no use for, so a script can pass every process of a node
//! the same command line. In order:
//!
//! 1. It waits out the mark files a node killed here moments ago left,
//!    unless SIGTERM comes first ([`rusteron_cluster_tools::driver::mark_files`]).
//! 2. It runs the driver on this thread: its directory deleted on start and
//!    on shutdown, sparse term buffers, the threading mode, multicast NAKs
//!    timed for a group of the other members and backing off up to 2 ms
//!    (Aeron's defaults are a group of 10 and 10 ms), every agent
//!    busy-spinning with `--spin`. A standby's driver times an image out
//!    after 3 s without a frame rather than Aeron's 10 s: its backup's
//!    source dead, the backup resets sooner, while a live source keeps
//!    sending heartbeats. Aeron's environment variables set the rest and
//!    win over these: `AERON_PRINT_CONFIGURATION=true` prints it all, and
//!    `AERON_DRIVER_CPUSET_AFFINITY` and `AERON_*_CPU_AFFINITY` pin the
//!    driver's threads, as they do `aeronmd`'s.
//! 3. It prints the up line (`" up on "`), which scripts wait for before they
//!    start the node's other processes.
//! 4. It prints the driver's error log as errors come, every second.
//! 5. On SIGTERM (or SIGINT), it prints the error log once more, closes the
//!    driver, deletes its directory, and says how long that took.
//!
//! Exits 0 when stopped by a signal, 1 when the driver failed while running,
//! 2 when it could not start.

use std::ffi::CString;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use rusteron_cluster_tools::args::Args;
use rusteron_cluster_tools::driver::{ErrorLog, Options, Threading, mark_files};
use rusteron_media_driver::bindings::aeron_threading_mode_enum;
use rusteron_media_driver::{Aeron, AeronCError, AeronCnc, AeronDriver, AeronDriverContext, AeronIdleStrategyKind};

/// The multicast NAK backoff: across zones, twice a round trip.
const NAK_MULTICAST_MAX_BACKOFF_NS: u64 = 2_000_000;
/// How long a standby's driver keeps an image without a frame: a source sends heartbeats every 100 ms.
const STANDBY_IMAGE_LIVENESS_TIMEOUT_NS: u64 = 3_000_000_000;
/// How often the driver's error log is read.
const ERROR_LOG_INTERVAL: Duration = Duration::from_secs(1);

fn main() -> ExitCode {
    let options = match Options::parse(&Args::from_env()) {
        Ok(options) => options,
        Err(e) => {
            eprintln!("node_driver: {e}");
            return ExitCode::from(2);
        }
    };
    let node = options.node;
    Logger::install(node);
    let stop = Arc::new(AtomicBool::new(false));
    let stopping = stop.clone();
    if let Err(e) = ctrlc::set_handler(move || stopping.store(true, Ordering::Release)) {
        eprintln!("node {node}: no signal handler: {e}");
        return ExitCode::from(2);
    }

    // A node killed moments ago left mark files its archive and consensus module take for a live process's
    // for 10 s: wait them out rather than see them fail to start and be started again.
    let waited = mark_files::await_stale(
        &options.node_dir,
        mark_files::LIVENESS_MS,
        mark_files::PATIENCE_MS,
        &stop,
    );
    if waited >= Duration::from_millis(100) {
        println!(
            "node {node}: waited {} ms for the mark files of a node killed here to age",
            waited.as_millis()
        );
    }
    if stop.load(Ordering::Acquire) {
        println!("node {node}: stopped before its driver started");
        return ExitCode::SUCCESS;
    }

    // Declared before the driver, the context is dropped after it: its close deletes the directory.
    let context = match context(&options) {
        Ok(context) => context,
        Err(e) => {
            eprintln!("node {node}: driver: {e}");
            return ExitCode::from(2);
        }
    };
    // Each error takes Aeron's message where it happens: closing the driver after a failed start could
    // overwrite it.
    let driver = match AeronDriver::new(&context)
        .map_err(AeronCError::capture_errmsg)
        .and_then(|driver| driver.start(true).map_err(AeronCError::capture_errmsg).map(|_| driver))
    {
        Ok(driver) => driver,
        Err(e) => {
            eprintln!("node {node}: driver: {e}");
            return ExitCode::from(2);
        }
    };
    println!(
        "node {node}: driver up in {}, Aeron's C driver {}",
        options.aeron_dir.display(),
        Aeron::version_text(),
    );
    let errors = print_errors(node, &options.aeron_dir, stop.clone());
    println!("{}", options.up_line());

    let mut exit = ExitCode::SUCCESS;
    while !stop.load(Ordering::Acquire) {
        match driver.main_do_work() {
            Ok(work) => driver.main_idle_strategy(work),
            Err(e) => {
                eprintln!("node {node}: driver failed: {}", e.capture_errmsg());
                exit = ExitCode::from(1);
                break;
            }
        }
    }

    let closing = Instant::now();
    stop.store(true, Ordering::Release);
    if let Some(errors) = errors {
        errors.thread().unpark();
        let _ = errors.join();
    }
    drop(driver);
    drop(context);
    println!("node {node}: stopped in {} ms", closing.elapsed().as_millis());
    exit
}

/// The driver's context; Aeron's environment variables set the rest.
fn context(options: &Options) -> Result<AeronDriverContext, String> {
    let dir = c_path(&options.aeron_dir)?;
    let aeron = |e: AeronCError| e.capture_errmsg().to_string();
    let context = AeronDriverContext::new().map_err(aeron)?;
    context.set_dir(&dir).map_err(aeron)?;
    context.set_dir_delete_on_start(true).map_err(aeron)?;
    context.set_dir_delete_on_shutdown(true).map_err(aeron)?;
    context.set_term_buffer_sparse_file(true).map_err(aeron)?;
    context
        .set_threading_mode(match options.threading {
            Threading::Shared => aeron_threading_mode_enum::AERON_THREADING_MODE_SHARED,
            Threading::SharedNetwork => aeron_threading_mode_enum::AERON_THREADING_MODE_SHARED_NETWORK,
            Threading::Dedicated => aeron_threading_mode_enum::AERON_THREADING_MODE_DEDICATED,
        })
        .map_err(aeron)?;
    // A follower subscribes to the log as a group, so a lost packet waits for a multicast NAK: Aeron times
    // it for a group of 10 backing off up to 10 ms, these for the other members and 2 ms. Aeron's own
    // settings still win.
    if std::env::var_os("AERON_NAK_MULTICAST_GROUP_SIZE").is_none() {
        context
            .set_nak_multicast_group_size(options.layout.members().saturating_sub(1))
            .map_err(aeron)?;
    }
    if std::env::var_os("AERON_NAK_MULTICAST_MAX_BACKOFF").is_none() {
        context
            .set_nak_multicast_max_backoff_ns(NAK_MULTICAST_MAX_BACKOFF_NS)
            .map_err(aeron)?;
    }
    if options.standby.is_some() && std::env::var_os("AERON_IMAGE_LIVENESS_TIMEOUT").is_none() {
        context
            .set_image_liveness_timeout_ns(STANDBY_IMAGE_LIVENESS_TIMEOUT_NS)
            .map_err(aeron)?;
    }
    if options.spin {
        for set in [
            AeronDriverContext::set_shared_idle_strategy_kind,
            AeronDriverContext::set_sharednetwork_idle_strategy_kind,
            AeronDriverContext::set_conductor_idle_strategy_kind,
            AeronDriverContext::set_sender_idle_strategy_kind,
            AeronDriverContext::set_receiver_idle_strategy_kind,
        ] {
            set(&context, AeronIdleStrategyKind::BusySpin).map_err(aeron)?;
        }
    }
    // As aeronmd does: without these, AERON_DRIVER_CPUSET_AFFINITY and the AERON_*_CPU_AFFINITY settings
    // are read but never applied.
    context.apply_cgroup_cpuset_affinity().map_err(aeron)?;
    context.set_thread_affinity_on_start();
    Ok(context)
}

fn c_path(path: &Path) -> Result<CString, String> {
    CString::new(path.as_os_str().as_bytes()).map_err(|e| format!("{}: {e}", path.display()))
}

/// Prints the driver's error log every [`ERROR_LOG_INTERVAL`], and once more
/// when `stop` is set and the thread unparked: the C driver keeps its errors
/// nowhere else, and its directory goes with it. A thread of its own, so the
/// driver's duty cycle reads no clock.
fn print_errors(node: usize, aeron_dir: &Path, stop: Arc<AtomicBool>) -> Option<JoinHandle<()>> {
    let dir = c_path(aeron_dir).ok()?;
    let spawned = thread::Builder::new().name("error-log".into()).spawn(move || {
        let mut log = ErrorLog::default();
        let mut cnc = None;
        let mut unread = false;
        loop {
            thread::park_timeout(ERROR_LOG_INTERVAL);
            let stopping = stop.load(Ordering::Acquire);
            if cnc.is_none() {
                match AeronCnc::open(&dir) {
                    Ok(opened) => cnc = Some(opened),
                    Err(e) if !unread => {
                        unread = true;
                        eprintln!("node {node}: the driver's error log is unread: {}", e.capture_errmsg());
                    }
                    Err(_) => {}
                }
            }
            if let Some(cnc) = &cnc {
                // Printed after the read: a panic in Aeron's callback would abort the process.
                let since = log.since();
                let mut lines = Vec::new();
                cnc.error_log_read_fn(
                    |observations, first_ms, last_ms, error| {
                        lines.extend(log.line(node, observations, first_ms, last_ms, error));
                    },
                    since,
                );
                for line in lines {
                    println!("{line}");
                }
            }
            if stopping {
                return;
            }
        }
    });
    spawned
        .map_err(|e| eprintln!("node {node}: the driver's errors will not be printed: {e}"))
        .ok()
}

/// Rusteron's reports, a failed close among them, on stderr.
struct Logger {
    node: usize,
}

impl Logger {
    fn install(node: usize) {
        if log::set_logger(Box::leak(Box::new(Logger { node }))).is_ok() {
            log::set_max_level(log::LevelFilter::Info);
        }
    }
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Info
    }

    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        // A failed close reports its code alone: Aeron's message is still this thread's last error.
        // Copied out, as rusteron 0.3 has it: the native buffer is the next failure's.
        let errmsg = if record.level() <= log::Level::Warn {
            Aeron::with_errmsg(|message| message.trim_end().to_owned())
        } else {
            String::new()
        };
        let _ = writeln!(
            std::io::stderr(),
            "node {}: {} {}: {}{}",
            self.node,
            record.level(),
            record.target(),
            record.args(),
            if errmsg.is_empty() {
                String::new()
            } else {
                format!(" (Aeron's last error: {errmsg})")
            }
        );
    }

    fn flush(&self) {}
}
