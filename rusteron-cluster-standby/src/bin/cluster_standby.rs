//! A standby's own process: the cluster backup and the standby module
//! ([`rusteron_cluster_standby::module`]) on the standby's media driver. The
//! rest of the standby are processes beside it on that driver, as a
//! member's are: its archive, and its service's container, which this
//! module feeds as a member's consensus module feeds its own
//! (`scripts/cluster.sh` with `STANDBY=1`).
//!
//! ```text
//! cluster_standby [--standby N] [--hosts H0,H1,...|--members M] [--host HOST] [--dir DIR] [--aeron-dir DIR]
//!                 [--port-base B] [--port-stride S] [--cluster-consensus-endpoints H:P,...]
//!                 [--consensus-endpoint H:P] [--catchup-endpoint H:P] [--archive-endpoint H:P]
//!                 [--cluster-id ID] [--keep-snapshots K] [--record-module-states]
//!                 [--source any|follower|leader] [--catchup-window 4m]
//!                 [--live-log-timeout 5s] [--snapshot-hold 30s]
//! ```
//!
//! Standby `N` takes the slot after the members' and follows the layout
//! ([`rusteron_cluster_tools::layout`]): its files under `DIR/standbyN`, its
//! driver's in `standbyN` under `--aeron-dir` (else `DIR/standbyN/aeron`),
//! and its endpoints on `--host` from its slot's ports. Each endpoint can be
//! given instead:
//!
//! - `--cluster-consensus-endpoints`: the members' consensus endpoints,
//!   where the backup asks and the notices go;
//! - `--consensus-endpoint`: where members answer the backup (slot + 3);
//! - `--catchup-endpoint`: where members' archives replay the log to the
//!   backup (slot + 4);
//! - `--archive-endpoint`: the standby archive's control endpoint, where
//!   members copy its snapshots from (slot + 1).
//!
//! The standby archive answers on its local control channel,
//! `aeron:ipc?term-length=64k`, stream 10: Aeron's defaults.
//!
//! - The backup asks any member, the leader included (`--source`: a
//!   follower, or the leader only), for the whole log from its start. It
//!   gives up on a source after 3 s without progress and on a query after
//!   1 s, cooling down 1 s, and keeps its recording log in `cluster`. It
//!   copies on Aeron's catch-up channel, with 512 KiB of socket buffers and
//!   receive window, enough for about 26 MB/s of log 20 ms away;
//!   `--catchup-window` (bytes, k, m or g) sizes all three, with a warning
//!   when the kernel's `net.core.rmem_max` is smaller.
//! - The module serves the service's control streams, keeps its snapshots in
//!   the local archive and its list of them in `standby-snapshots`, and
//!   sends the notices to every member's consensus endpoint. It keeps its
//!   newest K sets (`--keep-snapshots`, 3 by default, 0 all) and, of the
//!   members' sets the backup copied, the newest, and deletes the rest, as it
//!   does its service's own snapshots at the members' snapshots. At its
//!   start, before the backup, it deletes what its archive lists nowhere:
//!   what a kill during a snapshot or a copy given up left.
//! - `--live-log-timeout` and `--snapshot-hold` time the watch on the
//!   backup's live log ([`rusteron_cluster_standby::watch`]): the timeout
//!   must be longer than the cluster's log stands still while it is alive.
//!
//! `--record-module-states`, a test's aid, records the module's state at
//! each members' snapshot in the local archive too, as a module snapshot,
//! listed in `module-states`: the members' own are its oracle
//! (`standby_check states`).
//!
//! It prints an up line (`" up on "`) once started. On SIGTERM (or SIGINT)
//! the module closes first, its counters gone with it, which ends the
//! service's container, then the backup. Exits 0 when stopped by a signal, 1
//! when the backup or the module failed, 2 when it could not start.

use std::collections::HashSet;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusteron_archive::*;
use rusteron_cluster::backup::{
    BackupContext, BackupListener, ClusterMember, RecordingLog, ReplayStart, Snapshot, SourceType, spawn,
};
use rusteron_cluster_core::recording_log::{self, Entry};
use rusteron_cluster_standby::module::{KEEP_SNAPSHOTS, ModuleConfig, StandbyModule};
use rusteron_cluster_standby::snapshots::superseded_snapshots;
use rusteron_cluster_standby::watch::{self, clock};
use rusteron_cluster_tools::args::{Args, parse_duration};
use rusteron_cluster_tools::layout::{ARCHIVE_CONTROL, CONSENSUS, LOG, Layout};

/// The archives' local control channel: Aeron's default.
const LOCAL_CONTROL: &str = "aeron:ipc?term-length=64k";
/// Where the local archive answers the module, apart from the backup's answers.
const MODULE_ARCHIVE_RESPONSE_STREAM_ID: i32 = 22;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the service's log may stand still behind the log recorded before the standby says so, and again.
const STALL_REPORT: Duration = Duration::from_secs(10);

/// What the standby holds of the log: its local recording, how far it is
/// recorded, the member it is copied from, the snapshots copied; -1 before
/// the first. Written on the backup's thread, read on the module's.
struct Progress {
    recording_id: AtomicI64,
    position: AtomicI64,
    source_member_id: AtomicI32,
    snapshots_retrieved: AtomicUsize,
    /// The members' sets the backup copied that a newer one replaced, for the module to delete.
    superseded: Mutex<Vec<i64>>,
}

impl Default for Progress {
    fn default() -> Self {
        Self {
            recording_id: AtomicI64::new(-1),
            position: AtomicI64::new(-1),
            source_member_id: AtomicI32::new(-1),
            snapshots_retrieved: AtomicUsize::new(0),
            superseded: Mutex::new(Vec::new()),
        }
    }
}

/// The backup's events, kept for the module and printed.
struct Listener {
    standby: usize,
    progress: Arc<Progress>,
    /// The superseded copies already handed to the module.
    purges_asked: HashSet<i64>,
    /// Whether the source led when it answered, as the backup's response tells.
    source_led: bool,
}

impl BackupListener for Listener {
    fn on_possible_failure(&mut self, message: &str) {
        println!(
            "standby {}: the backup's source may have failed: {message}, at {} UTC",
            self.standby,
            clock()
        );
    }

    fn on_backup_response(&mut self, _members: &[ClusterMember], source: &ClusterMember, to_retrieve: &[Snapshot]) {
        let before = self.progress.source_member_id.swap(source.id, Ordering::AcqRel);
        let led = std::mem::replace(&mut self.source_led, source.is_leader);
        if before != source.id || led != source.is_leader {
            println!(
                "standby {}: the log from member {} ({}), {} snapshots to copy, at {} UTC",
                self.standby,
                source.id,
                if source.is_leader { "the leader" } else { "a follower" },
                to_retrieve.len(),
                clock()
            );
        }
    }

    fn on_updated_recording_log(&mut self, recording_log: &RecordingLog, retrieved: &[Snapshot]) {
        self.progress
            .snapshots_retrieved
            .fetch_add(retrieved.len(), Ordering::AcqRel);
        let superseded: Vec<i64> = superseded_snapshots(recording_log.entries())
            .into_iter()
            .filter(|&id| self.purges_asked.insert(id))
            .collect();
        if !superseded.is_empty() {
            self.progress
                .superseded
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .extend(superseded);
        }
    }

    fn on_live_log_progress(&mut self, recording_id: i64, _counter_id: i32, log_position: i64) {
        self.progress.position.store(log_position, Ordering::Release);
        if self.progress.recording_id.swap(recording_id, Ordering::AcqRel) != recording_id {
            println!(
                "standby {}: the log is recorded in recording {recording_id}",
                self.standby
            );
        }
    }

    fn on_terminated(&mut self, reason: &str) {
        println!("standby {}: the backup ended: {reason}", self.standby);
    }
}

struct Options {
    standby: usize,
    layout: Layout,
    host: String,
    standby_dir: PathBuf,
    aeron_dir: PathBuf,
    consensus_endpoints: Vec<String>,
    consensus_endpoint: String,
    catchup_endpoint: String,
    archive_endpoint: String,
    cluster_id: i32,
    keep_snapshots: usize,
    record_module_states: bool,
    source: SourceType,
    /// The catch-up channel's socket buffers and receive window, in bytes; 0 keeps Aeron's.
    catchup_window: usize,
    live_log_timeout: Duration,
    snapshot_hold: Duration,
}

impl Options {
    fn parse(args: &Args) -> Result<Self, String> {
        let layout = Layout::parse(args)?;
        let standby: usize = args
            .number("--standby", 0)
            .map_err(|_| "--standby N: a standby's number".to_string())?;
        let host = args.value("--host").unwrap_or("localhost").to_string();
        let slot = layout.standby_slot(standby);
        let standby_dir = layout.standby_dir(standby);
        let endpoint = |key: &str, offset| {
            args.value(key)
                .map_or_else(|| layout.endpoint(&host, slot, offset), String::from)
        };
        let duration = |key: &str, default: Duration| match args.value(key) {
            Some(d) => parse_duration(d).ok_or_else(|| format!("{key} {d}: a duration (5s, 500ms)")),
            None => Ok(default),
        };
        Ok(Self {
            standby,
            consensus_endpoints: match args.value("--cluster-consensus-endpoints") {
                Some(list) => list.split(',').map(String::from).collect(),
                None => layout.consensus_endpoints(),
            },
            consensus_endpoint: endpoint("--consensus-endpoint", CONSENSUS),
            catchup_endpoint: endpoint("--catchup-endpoint", LOG),
            archive_endpoint: endpoint("--archive-endpoint", ARCHIVE_CONTROL),
            aeron_dir: layout.aeron_dir(&standby_dir)?,
            standby_dir,
            cluster_id: args.number("--cluster-id", 0).map_err(|_| "--cluster-id ID")?,
            keep_snapshots: args
                .number("--keep-snapshots", KEEP_SNAPSHOTS)
                .map_err(|_| "--keep-snapshots K: how many sets to keep, 0 all")?,
            record_module_states: args.flag("--record-module-states"),
            source: match args.value("--source") {
                Some(s) => SourceType::parse(&s.to_ascii_uppercase())
                    .map_err(|_| "--source any|follower|leader: the members the backup may copy from")?,
                None => SourceType::Any,
            },
            catchup_window: match args.value("--catchup-window") {
                Some(w) => parse_size(w).ok_or("--catchup-window BYTES: a window, in bytes, k, m or g")?,
                None => 0,
            },
            live_log_timeout: duration("--live-log-timeout", watch::TIMEOUT)?,
            snapshot_hold: duration("--snapshot-hold", watch::SNAPSHOT_HOLD)?,
            host,
            layout,
        })
    }

    fn backup(&self) -> BackupContext {
        let aeron_dir = self.aeron_dir.display().to_string();
        let mut ctx = BackupContext::new(aeron_dir, self.standby_dir.join("cluster"));
        let c = &mut ctx.config;
        c.cluster_id = self.cluster_id;
        c.cluster_consensus_endpoints = self.consensus_endpoints.join(",");
        c.consensus_channel = format!("aeron:udp?term-length=64k|endpoint={}", self.consensus_endpoint);
        c.catchup_endpoint = self.catchup_endpoint.clone();
        if self.catchup_window > 0 {
            c.catchup_channel = with_window(&c.catchup_channel, self.catchup_window);
        }
        // Any member by default: the first to answer, so a new source soon after a kill of the old one, where
        // followers only can take much longer. Whichever it is, its archive replays only up to its commit
        // position: the standby records nothing the cluster could still drop.
        c.source_type = self.source;
        // The whole log, from its first entry: the service starts empty and applies all of it.
        c.initial_replay_start = ReplayStart::Beginning;
        c.progress_timeout_ns = 3_000_000_000;
        c.cool_down_interval_ns = 1_000_000_000;
        c.response_timeout_ns = 1_000_000_000;
        // The live log's watch is the module's (rusteron_cluster_standby::watch): a still log resets the backup,
        // unless it ends with the members' snapshot action. No member snapshot is copied, which the standby never
        // loads, so a reset follows the log again at once.
        c.live_log_progress_timeout_ns = 0;
        c.retrieve_snapshots = false;
        c.local_archive.request_channel = LOCAL_CONTROL.into();
        c.local_archive.response_channel = LOCAL_CONTROL.into();
        // A template: the backup sets the source member's archive endpoint on it.
        c.cluster_archive.request_channel = "aeron:udp?term-length=64k".into();
        c.cluster_archive.response_channel = format!("aeron:udp?endpoint={}:0", self.host);
        ctx
    }

    fn module(&self) -> ModuleConfig {
        let slot = self.layout.standby_slot(self.standby);
        let mut config = ModuleConfig::new(slot as i32, self.host.clone(), self.archive_endpoint.clone());
        config.cluster_id = self.cluster_id;
        config.member_consensus_endpoints = self.consensus_endpoints.clone();
        config.snapshots_file = self.standby_dir.join("standby-snapshots");
        config.keep_snapshots = self.keep_snapshots;
        config.member_states_file = self
            .record_module_states
            .then(|| self.standby_dir.join("module-states"));
        config.live_log_timeout = self.live_log_timeout;
        config.snapshot_hold = self.snapshot_hold;
        config
    }
}

fn main() -> ExitCode {
    let options = match Options::parse(&Args::from_env()) {
        Ok(options) => options,
        Err(e) => {
            eprintln!("cluster_standby: {e}");
            return ExitCode::from(2);
        }
    };
    let id = options.standby;
    if options.catchup_window > 0 {
        warn_if_kernel_caps(id, options.catchup_window);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let stopping = stop.clone();
    if let Err(e) = ctrlc::set_handler(move || stopping.store(true, Ordering::Release)) {
        eprintln!("standby {id}: no signal handler: {e}");
        return ExitCode::from(2);
    }

    // The module before the backup: until the backup runs, nothing records a snapshot that will be listed, so
    // one its archive lists nowhere is left over, a kill's or a copy's given up, and goes.
    let mut module = match start_module(&options) {
        Ok(module) => module,
        Err(e) => {
            eprintln!("standby {id}: module: {e}");
            return ExitCode::from(2);
        }
    };
    match backup_log(&options).and_then(|entries| module.sweep(&entries)) {
        Ok((unlisted, newest)) => println!(
            "standby {id}: at its start, its archive's recordings to {newest}, {} snapshot recordings listed \
             nowhere, to delete: {unlisted:?}",
            unlisted.len()
        ),
        Err(e) => println!("standby {id}: at its start, its archive was not looked through: {e}"),
    }
    let progress = Arc::new(Progress::default());
    let listener = Listener {
        standby: id,
        progress: progress.clone(),
        purges_asked: HashSet::new(),
        source_led: false,
    };
    let backup = match spawn(options.backup(), listener) {
        Ok(backup) => backup,
        Err(e) => {
            eprintln!("standby {id}: backup: {e}");
            module.close();
            return ExitCode::from(2);
        }
    };
    match module.loaded() {
        Some(s) => {
            println!(
                "standby {id}: starts from its snapshot at {} of term {}",
                s.log_position, s.leadership_term_id
            )
        }
        None => println!("standby {id}: starts from the log's start"),
    }
    println!(
        "standby {id} of the cluster on {} up on {}, its snapshots offered from {}",
        options.consensus_endpoints.join(","),
        options.host,
        options.archive_endpoint
    );

    let mut exit = ExitCode::SUCCESS;
    let mut joins = 0;
    // The commit position, and since when it has not moved while the log recorded is ahead of it.
    let mut stalled = (module.commit_position(), Instant::now());
    let mut idle = Backoff::default();
    while !stop.load(Ordering::Acquire) {
        if backup.is_finished() {
            eprintln!("standby {id}: the backup stopped");
            exit = ExitCode::from(1);
            break;
        }
        if let Ok(mut superseded) = progress.superseded.try_lock()
            && !superseded.is_empty()
        {
            module.purge_later(superseded.drain(..));
        }
        match module.do_work(progress.recording_id.load(Ordering::Acquire)) {
            Ok(work) => idle.idle(work),
            Err(e) => {
                eprintln!("standby {id}: module failed: {e}");
                exit = ExitCode::from(1);
                break;
            }
        }
        if module.joins() != joins {
            joins = module.joins();
            println!(
                "standby {id}: its service joins the log at {} (join {joins}), the log recorded to {} from member {}",
                module.last_ack_position().max(module.commit_position()),
                progress.position.load(Ordering::Acquire),
                progress.source_member_id.load(Ordering::Acquire)
            );
        }
        let commit = module.commit_position();
        if commit != stalled.0 || module.replay_limit() <= commit {
            stalled = (commit, Instant::now());
        } else if stalled.1.elapsed() >= STALL_REPORT {
            // What the standby's logs keep to explain a service that stopped following the log.
            println!(
                "standby {id}: its service's log has stood at {commit} for {} s, the log recorded to {}: held at \
                 {:?}, the service's last ack at {}, its log from {}, join {}",
                stalled.1.elapsed().as_secs(),
                module.replay_limit(),
                module.held_at(),
                module.last_ack_position(),
                module.service_position(),
                module.joins()
            );
            stalled.1 = Instant::now();
        }
    }

    let closing = Instant::now();
    let purged = module.purged();
    module.close();
    backup.stop();
    let ended = backup.join();
    println!(
        "standby {id}: stopped in {} ms ({ended:?}), {} snapshots copied from members, {purged} snapshot recordings \
         deleted",
        closing.elapsed().as_millis(),
        progress.snapshots_retrieved.load(Ordering::Acquire)
    );
    exit
}

/// The entries of the backup's recording log, none before its first start.
fn backup_log(options: &Options) -> Result<Vec<Entry>, String> {
    let path = options.standby_dir.join("cluster").join(recording_log::FILE_NAME);
    match std::fs::read(&path) {
        Ok(bytes) => RecordingLog::load(&bytes)
            .map(|log| log.entries().to_vec())
            .map_err(|e| format!("{}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// The module's client and its archive's, on the standby's driver.
fn start_module(options: &Options) -> Result<StandbyModule, String> {
    let error = |what: &'static str| move |e: AeronCError| format!("{what}: {e}");
    let context = AeronContext::new().map_err(error("client"))?;
    context
        .set_dir(&options.aeron_dir.display().to_string().into_c_string())
        .map_err(error("client"))?;
    context
        .set_client_name(&format!("standby-module standby={}", options.standby).into_c_string())
        .map_err(error("client"))?;
    let aeron = Aeron::new(&context).map_err(error("client"))?;
    aeron.start().map_err(error("client"))?;
    StandbyModule::new(aeron, Box::new(connect_archive), options.module())
}

/// A client of the standby's own archive, on its local control channel.
fn connect_archive(aeron: &Aeron) -> Result<AeronArchive, String> {
    let error = |e: AeronCError| format!("archive: {e}");
    let context = AeronArchiveContext::new().map_err(error)?;
    context.set_aeron(aeron).map_err(error)?;
    context
        .set_control_request_channel(&LOCAL_CONTROL.into_c_string())
        .map_err(error)?;
    context
        .set_control_response_channel(&LOCAL_CONTROL.into_c_string())
        .map_err(error)?;
    context
        .set_control_response_stream_id(MODULE_ARCHIVE_RESPONSE_STREAM_ID)
        .map_err(error)?;
    AeronArchiveAsyncConnect::new_with_aeron(&context, aeron)
        .map_err(error)?
        .poll_blocking(CONNECT_TIMEOUT)
        .map_err(error)
}

/// Spins, yields, then parks up to a millisecond, as Aeron's default idle strategy.
#[derive(Default)]
struct Backoff {
    idles: u32,
}

impl Backoff {
    fn idle(&mut self, work: usize) {
        if work > 0 {
            self.idles = 0;
            return;
        }
        self.idles = self.idles.saturating_add(1);
        match self.idles {
            0..=10 => std::hint::spin_loop(),
            11..=20 => std::thread::yield_now(),
            n => std::thread::sleep(Duration::from_micros(1 << (n - 21).min(10))),
        }
    }
}

/// A length in bytes, or with Agrona's suffixes k, m and g (powers of 1,024), as Aeron's sizes read.
fn parse_size(text: &str) -> Option<usize> {
    let (digits, unit) = match text.as_bytes().last()? {
        b'k' | b'K' => (&text[..text.len() - 1], 1 << 10),
        b'm' | b'M' => (&text[..text.len() - 1], 1 << 20),
        b'g' | b'G' => (&text[..text.len() - 1], 1 << 30),
        _ => (text, 1),
    };
    digits.parse::<usize>().ok()?.checked_mul(unit)
}

/// `channel` with `window` bytes of socket buffers and receive window: so-sndbuf, so-rcvbuf and rcv-wnd replaced,
/// or added, the other parameters kept in their order.
fn with_window(channel: &str, window: usize) -> String {
    let (media, params) = channel.split_once('?').unwrap_or((channel, ""));
    let keys = ["so-sndbuf", "so-rcvbuf", "rcv-wnd"];
    let mut out: Vec<String> = params
        .split('|')
        .filter(|p| !p.is_empty() && !keys.iter().any(|k| p.split_once('=').map(|(name, _)| name) == Some(k)))
        .map(String::from)
        .collect();
    out.extend(keys.iter().map(|k| format!("{k}={window}")));
    format!("{media}?{}", out.join("|"))
}

/// A warning when Linux caps socket receive buffers below the window asked for.
fn warn_if_kernel_caps(id: usize, window: usize) {
    let max = std::fs::read_to_string("/proc/sys/net/core/rmem_max")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok());
    if let Some(max) = max.filter(|&max| max < window) {
        println!(
            "standby {id}: WARNING the catch-up window is {window} bytes but net.core.rmem_max {max}: the kernel \
             caps the socket's buffer, and a window's burst can overflow it"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(line: &str) -> Result<Options, String> {
        Options::parse(&Args::new(line.split_whitespace().map(String::from).collect()))
    }

    #[test]
    fn the_layout_gives_every_endpoint_and_each_can_be_given() {
        let o = parse("--hosts a,b,c --host s").unwrap();
        assert_eq!(o.consensus_endpoints, ["a:21003", "b:21103", "c:21203"]);
        assert_eq!(
            (o.consensus_endpoint.as_str(), o.catchup_endpoint.as_str()),
            ("s:21303", "s:21304")
        );
        assert_eq!(o.archive_endpoint, "s:21301");
        assert_eq!(o.standby_dir, PathBuf::from("data/nodes/standby0"));
        assert_eq!(
            (o.live_log_timeout, o.snapshot_hold),
            (watch::TIMEOUT, watch::SNAPSHOT_HOLD)
        );
        let module = o.module();
        assert_eq!((module.member_id, module.archive_endpoint.as_str()), (3, "s:21301"));
        let backup = o.backup();
        assert_eq!(backup.config.cluster_consensus_endpoints, "a:21003,b:21103,c:21203");
        assert_eq!(
            backup.config.consensus_channel,
            "aeron:udp?term-length=64k|endpoint=s:21303"
        );
        assert_eq!(backup.config.catchup_endpoint, "s:21304");

        let o = parse(
            "--standby 1 --members 5 --cluster-consensus-endpoints m0:9003,m1:9103 --consensus-endpoint x:1 \
             --catchup-endpoint x:2 --archive-endpoint x:3 --live-log-timeout 30s --snapshot-hold 2m",
        )
        .unwrap();
        assert_eq!(o.consensus_endpoints, ["m0:9003", "m1:9103"]);
        assert_eq!(
            (
                o.consensus_endpoint.as_str(),
                o.catchup_endpoint.as_str(),
                o.archive_endpoint.as_str()
            ),
            ("x:1", "x:2", "x:3")
        );
        assert_eq!(
            (o.live_log_timeout, o.snapshot_hold),
            (Duration::from_secs(30), Duration::from_secs(120))
        );
        assert_eq!(o.module().member_id, 6);
        assert!(parse("--live-log-timeout soon").is_err());
    }

    #[test]
    fn sizes_read_as_agrona_reads_them() {
        assert_eq!(parse_size("524288"), Some(524_288));
        assert_eq!(parse_size("512k"), Some(524_288));
        assert_eq!(parse_size("4m"), Some(4 << 20));
        assert_eq!(parse_size("1G"), Some(1 << 30));
        assert_eq!(parse_size("4x"), None);
        assert_eq!(parse_size("m"), None);
        assert_eq!(parse_size(""), None);
    }

    #[test]
    fn a_window_replaces_the_buffers_and_the_receive_window() {
        let aeron = "aeron:udp?alias=backup|cc=cubic|so-sndbuf=512k|so-rcvbuf=512k|rcv-wnd=512k";
        assert_eq!(
            with_window(aeron, 4 << 20),
            "aeron:udp?alias=backup|cc=cubic|so-sndbuf=4194304|so-rcvbuf=4194304|rcv-wnd=4194304"
        );
        assert_eq!(
            with_window("aeron:udp", 1024),
            "aeron:udp?so-sndbuf=1024|so-rcvbuf=1024|rcv-wnd=1024"
        );
    }

    #[test]
    fn the_backups_default_is_aerons() {
        // The window's default the doc comments state: rusteron's backup copies Aeron's channel.
        let ctx = BackupContext::new("aeron", "cluster");
        let aeron = "aeron:udp?alias=backup|cc=cubic|so-sndbuf=512k|so-rcvbuf=512k|rcv-wnd=512k";
        assert_eq!(ctx.config.catchup_channel, aeron);
    }
}
