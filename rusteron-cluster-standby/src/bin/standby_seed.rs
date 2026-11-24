//! Seeds a new member from a standby: the disaster-recovery runbook, once
//! the cluster is lost, starts a new one from what the standby holds. Run
//! once for each new member, with its driver and its archive up on empty
//! directories, before its consensus module and service start
//! (`scripts/cluster.sh dr-seed`):
//!
//! ```text
//! standby_seed --node N [--hosts H0,H1,...|--members M] [--host HOST] [--dir DIR] [--aeron-dir DIR]
//!              [--port-base B] [--port-stride S] [--standby N] [--standby-host HOST]
//!              [--standby-archive H:P] [--standby-dir DIR] [--cluster-dir DIR]
//!              [--extra-archive endpoint=H:P,control-stream=ID[,streams=A+B]]...
//!              [--no-snapshot] [--stop-at-set]
//! ```
//!
//! The member and the standby follow the layout
//! ([`rusteron_cluster_tools::layout`]): member `N`'s files under
//! `DIR/nodeN` and its driver's in `nodeN` under `--aeron-dir`; standby
//! `N`'s files under `DIR/standbyN` (`--standby-dir`), and its archive's
//! control endpoint on `--standby-host` at its slot's port + 1
//! (`--standby-archive`). The member's recording log goes in its cluster
//! directory, `DIR/nodeN/cluster` (`--cluster-dir`). The member's archive is
//! reached on its local control channel, Aeron's default
//! (`aeron:ipc?term-length=64k`, stream 10).
//!
//! The member's archive copies from the standby's, with the archives' own
//! replication, to the source's end:
//! - the log recording the standby's backup keeps, which its terms name;
//! - its newest set: its service's snapshot and its module's.
//!
//! Each `--extra-archive` copies the newest recording of each of its streams
//! (`streams`, else every stream it has) from another archive of the
//! standby's, at `endpoint` on control stream `control-stream`, into the
//! member's archive with the same local control stream: an application's
//! own recordings beside the cluster, an event log say, which its service
//! goes on from.
//!
//! Then the tool writes the member's recording log
//! ([`rusteron_cluster_standby::seed::member_log`]): the standby's terms, in
//! the log's copy, and the set, in its copies. Started, the member loads the
//! set and replays the log after it, as from its own. `--no-snapshot` copies
//! no set, and the member replays the whole log. `--stop-at-set`, a test's
//! aid, copies the log only to the set, leaving out what the standby holds
//! after it: a planted bug that a check after a disaster must catch.
//!
//! The copy is the standby's log byte for byte to its end, which may fall
//! inside a fragmented message. That message reached no service, the
//! standby's included, and the standby goes on extending its log from that
//! end once the new cluster runs, so the copy keeps it too.
//!
//! Exits 0 once the member's recording log is written, 1 on a failure, 2 on
//! bad options. Refuses a member that already has a recording log.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusteron_archive::*;
use rusteron_cluster_core::recording_log::{self, RecordingLog};
use rusteron_cluster_standby::seed::{self, CopiedSet};
use rusteron_cluster_standby::snapshots::Snapshot;
use rusteron_cluster_tools::args::Args;
use rusteron_cluster_tools::layout::{ARCHIVE_CONTROL, Layout};

/// Aeron's archive control stream, remote and local.
const CONTROL_STREAM_ID: i32 = 10;
/// The archives' local control channel: Aeron's default.
const LOCAL_CONTROL: &str = "aeron:ipc?term-length=64k";
/// Where the first archive answers this tool, apart from anything else on the driver: each archive takes the
/// next one, the member's and the standby's for each copy.
const RESPONSE_STREAM_BASE: i32 = 41;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a copy may stand still before it is given up.
const STALL: Duration = Duration::from_secs(30);
/// How often a copy's progress is asked for.
const PROGRESS_INTERVAL: Duration = Duration::from_millis(500);
/// How many times a copy is tried, a while apart.
const COPY_ATTEMPTS: u32 = 5;
const RETRY_AFTER: Duration = Duration::from_secs(1);
const NULL_VALUE: i64 = -1;
const SIGNAL_REPLICATE: i32 = 3;
const SIGNAL_REPLICATE_END: i32 = 7;

/// Another archive of the standby's, whose newest recordings are copied too.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ExtraArchive {
    endpoint: String,
    control_stream: i32,
    /// Only these streams' recordings; all if empty.
    streams: Vec<i32>,
}

impl ExtraArchive {
    /// `endpoint=H:P,control-stream=ID[,streams=A+B]`.
    fn parse(spec: &str) -> Result<Self, String> {
        let bad = || format!("--extra-archive {spec}: endpoint=HOST:PORT,control-stream=ID[,streams=A+B]");
        let (mut endpoint, mut control_stream, mut streams) = (None, None, Vec::new());
        for part in spec.split(',') {
            match part.split_once('=').ok_or_else(bad)? {
                ("endpoint", e) => endpoint = Some(e.to_string()),
                ("control-stream", c) => control_stream = Some(c.parse().map_err(|_| bad())?),
                ("streams", list) => {
                    streams = list
                        .split('+')
                        .map(|s| s.parse().map_err(|_| bad()))
                        .collect::<Result<_, _>>()?
                }
                _ => return Err(bad()),
            }
        }
        Ok(Self {
            endpoint: endpoint.ok_or_else(bad)?,
            control_stream: control_stream.ok_or_else(bad)?,
            streams,
        })
    }
}

struct Options {
    node: usize,
    host: String,
    aeron_dir: PathBuf,
    cluster_dir: PathBuf,
    standby: usize,
    standby_dir: PathBuf,
    standby_archive: String,
    extra_archives: Vec<ExtraArchive>,
    snapshot: bool,
    stop_at_set: bool,
}

impl Options {
    fn parse(args: &Args) -> Result<Self, String> {
        let layout = Layout::parse(args)?;
        let node: usize = args
            .required("--node")?
            .parse()
            .map_err(|_| "--node N: a member's number".to_string())?;
        if node >= layout.members() {
            return Err(format!("--node {node}: the cluster has {} members", layout.members()));
        }
        let standby: usize = args.number("--standby", 0)?;
        let standby_host = args.value("--standby-host").unwrap_or("localhost");
        let member_dir = layout.member_dir(node);
        Ok(Self {
            node,
            host: args.value("--host").unwrap_or(&layout.hosts[node]).to_string(),
            aeron_dir: layout.aeron_dir(&member_dir)?,
            cluster_dir: args
                .value("--cluster-dir")
                .map_or_else(|| member_dir.join("cluster"), PathBuf::from),
            standby,
            standby_dir: args
                .value("--standby-dir")
                .map_or_else(|| layout.standby_dir(standby), PathBuf::from),
            standby_archive: args.value("--standby-archive").map_or_else(
                || layout.endpoint(standby_host, layout.standby_slot(standby), ARCHIVE_CONTROL),
                String::from,
            ),
            extra_archives: args
                .values("--extra-archive")
                .into_iter()
                .map(ExtraArchive::parse)
                .collect::<Result<_, _>>()?,
            snapshot: !args.flag("--no-snapshot"),
            stop_at_set: args.flag("--stop-at-set"),
        })
    }

    fn member_log(&self) -> PathBuf {
        self.cluster_dir.join(recording_log::FILE_NAME)
    }
}

/// A remote archive's control channel.
fn control_channel(endpoint: &str) -> String {
    format!("aeron:udp?endpoint={endpoint}|term-length=64k")
}

/// A recording signal: its code, its recording, its position.
type Signal = (i32, i64, i64);

/// What an attempt at a copy started: its replication, and the copy's recording once there is one.
#[derive(Default)]
struct Attempt {
    replication: Option<i64>,
    copy: Option<i64>,
}

/// A client of one archive, and the recording signals it heard.
struct Archive {
    name: String,
    archive: AeronArchive,
    signals: Arc<Mutex<Vec<Signal>>>,
    _context: AeronArchiveContext,
}

impl Archive {
    fn connect(aeron: &Aeron, name: &str, request: (&str, i32), response: (&str, i32)) -> Result<Self, String> {
        let error = |e: AeronCError| format!("{name}: {e}");
        let context = AeronArchiveContext::new().map_err(error)?;
        context.set_aeron(aeron).map_err(error)?;
        context
            .set_control_request_channel(&request.0.into_c_string())
            .map_err(error)?;
        context.set_control_request_stream_id(request.1).map_err(error)?;
        context
            .set_control_response_channel(&response.0.into_c_string())
            .map_err(error)?;
        context.set_control_response_stream_id(response.1).map_err(error)?;
        let signals = Arc::new(Mutex::new(Vec::new()));
        let heard = Arc::clone(&signals);
        context
            .set_recording_signal_consumer(Some(move |signal: &AeronArchiveRecordingSignal| {
                if let Ok(mut heard) = heard.lock() {
                    heard.push((signal.recording_signal_code(), signal.recording_id(), signal.position()));
                }
            }))
            .map_err(error)?;
        let archive = AeronArchiveAsyncConnect::new_with_aeron(&context, aeron)
            .map_err(error)?
            .poll_blocking(CONNECT_TIMEOUT)
            .map_err(error)?;
        Ok(Self {
            name: name.into(),
            archive,
            signals,
            _context: context,
        })
    }

    /// Where `recording` ends: where it stopped, or, still recorded, where it is.
    fn end(&self, recording: i64) -> Result<i64, String> {
        let stop = self
            .archive
            .get_stop_position(recording)
            .map_err(|e| format!("{}: recording {recording}: {e}", self.name))?;
        if stop != NULL_VALUE {
            return Ok(stop);
        }
        self.archive
            .get_recording_position(recording)
            .map_err(|e| format!("{}: recording {recording}: {e}", self.name))
    }

    /// Where `recording` starts.
    fn start(&self, recording: i64) -> Result<i64, String> {
        let mut count = 0;
        let mut start = None;
        self.archive
            .list_recording_fn(&mut count, recording, |d| start = Some(d.start_position()))
            .map_err(|e| format!("{}: recording {recording}: {e}", self.name))?;
        start.ok_or_else(|| format!("{}: no recording {recording}", self.name))
    }

    /// The newest recording of each stream: its id.
    fn newest_by_stream(&self) -> Result<BTreeMap<i32, i64>, String> {
        let mut count = 0;
        let mut newest = BTreeMap::new();
        self.archive
            .list_recordings_fn(&mut count, 0, i32::MAX, |d| {
                let id = newest.entry(d.stream_id()).or_insert(d.recording_id());
                *id = (*id).max(d.recording_id());
            })
            .map_err(|e| format!("{}: listing its recordings: {e}", self.name))?;
        Ok(newest)
    }

    /// Copies recording `src` from `source` into this archive with its
    /// replication, to its end or to `upto`, and waits for it: the copy's id
    /// once it ends there. A copy that fails is tried again, what it left
    /// deleted: a standby serving several members at once, its driver
    /// shared, can be slow to take a replication's connection.
    fn copy(
        &self,
        what: &str,
        source: &Archive,
        src: i64,
        src_channel: (&str, i32),
        upto: Option<i64>,
    ) -> Result<i64, String> {
        for attempt in 1.. {
            let mut tried = Attempt::default();
            match self.copy_once(what, source, src, src_channel, upto, &mut tried) {
                Ok(copy) => return Ok(copy),
                Err(e) if attempt < COPY_ATTEMPTS => {
                    println!("  {e}: trying again in {} s", RETRY_AFTER.as_secs());
                    self.give_up(&tried);
                }
                Err(e) => return Err(format!("{e}, after {COPY_ATTEMPTS} attempts")),
            }
        }
        unreachable!("the attempts end in a return")
    }

    /// What an attempt that failed left: its replication stopped, the
    /// signals it still sends heard out for [`RETRY_AFTER`], none of which
    /// says which replication it is of, so none is taken for the next's; its
    /// copy, stopped, deleted.
    fn give_up(&self, tried: &Attempt) {
        if let Some(replication) = tried.replication {
            let _ = self.archive.stop_replication(replication);
        }
        let until = Instant::now() + RETRY_AFTER;
        while Instant::now() < until {
            let _ = self.archive.poll_for_recording_signals();
            std::thread::sleep(Duration::from_millis(10));
        }
        if let Ok(mut signals) = self.signals.lock() {
            signals.clear();
        }
        if let Some(copy) = tried.copy {
            let _ = self.archive.purge_recording(copy);
        }
    }

    /// One attempt at [`copy`](Self::copy), what it started kept in `tried`.
    fn copy_once(
        &self,
        what: &str,
        source: &Archive,
        src: i64,
        src_channel: (&str, i32),
        upto: Option<i64>,
        tried: &mut Attempt,
    ) -> Result<i64, String> {
        let error = |e: String| format!("copying {what} into {}: {e}", self.name);
        let start = source.start(src)?;
        let end = match upto {
            Some(upto) => upto,
            None => source.end(src)?,
        };
        self.signals.lock().map_err(|e| e.to_string())?.clear();
        // Aeron's defaults otherwise: a new recording, no live merge, the context's channel, no credentials.
        let params = AeronArchiveReplicationParams::builder()
            .stop_position(end)
            .build()
            .map_err(|e| error(e.to_string()))?;
        let began = Instant::now();
        let replication = self
            .archive
            .replicate(src, &src_channel.0.into_c_string(), src_channel.1, &params)
            .map_err(|e| error(e.to_string()))?;
        tried.replication = Some(replication);
        let (mut copy, mut ended) = (NULL_VALUE, false);
        let (mut reached, mut moved, mut asked) = (start, Instant::now(), Instant::now());
        while !ended {
            self.archive
                .poll_for_recording_signals()
                .map_err(|e| error(e.to_string()))?;
            for (code, recording, _) in self.signals.lock().map_err(|e| e.to_string())?.drain(..) {
                match code {
                    SIGNAL_REPLICATE => copy = recording,
                    SIGNAL_REPLICATE_END => ended = true,
                    _ if copy == NULL_VALUE && recording != NULL_VALUE => copy = recording,
                    _ => {}
                }
            }
            if copy != NULL_VALUE {
                tried.copy = Some(copy);
            }
            if !ended && copy != NULL_VALUE && asked.elapsed() >= PROGRESS_INTERVAL {
                asked = Instant::now();
                if let Ok(at) = self.archive.get_recording_position(copy)
                    && at > reached
                {
                    (reached, moved) = (at, Instant::now());
                }
            }
            if moved.elapsed() >= STALL {
                let _ = self.archive.stop_replication(replication);
                return Err(error(format!(
                    "no progress for {} s, at {reached} of {end}",
                    STALL.as_secs()
                )));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        if copy == NULL_VALUE {
            return Err(error("the replication ended with no recording".into()));
        }
        let stop = self.end(copy)?;
        if stop != end {
            return Err(error(format!(
                "the copy, recording {copy}, ends at {stop}, the source at {end}"
            )));
        }
        let seconds = began.elapsed().as_secs_f64();
        let mb = (end - start) as f64 / 1e6;
        println!(
            "  {what}: recording {src} of the standby's, {start} to {end}, in recording {copy}: {mb:.1} MB in {seconds:.2} s{}",
            if seconds > 0.0 {
                format!(", {:.0} MB/s", mb / seconds)
            } else {
                String::new()
            }
        );
        Ok(copy)
    }
}

/// The member's recording log, written whole and synced, its directory too.
fn write_log(path: &Path, log: &mut RecordingLog) -> Result<(), String> {
    let error = |e: std::io::Error| format!("{}: {e}", path.display());
    let directory = path
        .parent()
        .ok_or_else(|| format!("{}: no directory", path.display()))?;
    std::fs::create_dir_all(directory).map_err(error)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(error)?;
    for (offset, bytes) in log.take_writes() {
        file.seek(SeekFrom::Start(offset)).map_err(error)?;
        file.write_all(&bytes).map_err(error)?;
    }
    file.sync_all().map_err(error)?;
    File::open(directory).and_then(|d| d.sync_all()).map_err(error)
}

fn seed(options: &Options) -> Result<(), String> {
    let began = Instant::now();
    let standby_dir = &options.standby_dir;
    let standby_log_path = standby_dir.join("cluster").join(recording_log::FILE_NAME);
    let bytes = std::fs::read(&standby_log_path).map_err(|e| format!("{}: {e}", standby_log_path.display()))?;
    let standby_log = RecordingLog::load(&bytes)
        .map_err(|e| format!("{}: {e}", standby_log_path.display()))?
        .entries()
        .to_vec();
    let log_recording = seed::log_recording_id(&standby_log).ok_or_else(|| {
        format!(
            "{}: no term: the standby has recorded no log",
            standby_log_path.display()
        )
    })?;
    let set = if options.snapshot {
        Snapshot::latest(&standby_dir.join("standby-snapshots")).map_err(|e| e.to_string())?
    } else {
        None
    };

    let aeron = client(options, "log")?;
    let response = format!("aeron:udp?endpoint={}:0", options.host);
    let standby_archive = (control_channel(&options.standby_archive), CONTROL_STREAM_ID);
    let (archive_response, standby_response) = (RESPONSE_STREAM_BASE, RESPONSE_STREAM_BASE + 1);

    println!(
        "node {}: seeding from standby {} ({}), {}",
        options.node,
        options.standby,
        standby_dir.display(),
        match set {
            Some(s) => format!("its newest set at {} (term {})", s.log_position, s.leadership_term_id),
            None if options.snapshot => "which has no set: the whole log is replayed".into(),
            None => "no set (--no-snapshot): the whole log is replayed".into(),
        }
    );
    // The other archives' recordings, on a client of their own meanwhile.
    let copies = std::thread::scope(|scope| {
        let extra = scope.spawn(|| copy_extra_archives(options));
        let log = copy_log(
            options,
            &aeron,
            set,
            log_recording,
            &standby_archive,
            (&response, standby_response),
            archive_response,
        );
        (log, extra.join())
    });
    let (log_copy, copied_set, standby_end) = copies.0?;
    copies
        .1
        .map_err(|_| "copying the other archives: the thread panicked".to_string())??;

    let mut log = seed::member_log(&standby_log, log_copy, copied_set)?;
    write_log(&options.member_log(), &mut log)?;
    println!(
        "node {}: seeded in {:.1} s, its recording log in {}: the log to {standby_end}, last term {}",
        options.node,
        began.elapsed().as_secs_f64(),
        options.member_log().display(),
        log.find_last_term().map_or(NULL_VALUE, |t| t.leadership_term_id)
    );
    Ok(())
}

/// A client on the member's driver.
fn client(options: &Options, what: &str) -> Result<Aeron, String> {
    let error = |e: AeronCError| format!("client: {e}");
    let context = AeronContext::new().map_err(error)?;
    context
        .set_dir(&options.aeron_dir.display().to_string().into_c_string())
        .map_err(error)?;
    context
        .set_client_name(&format!("standby_seed node={} {what}", options.node).into_c_string())
        .map_err(error)?;
    let aeron = Aeron::new(&context).map_err(error)?;
    aeron.start().map_err(error)?;
    Ok(aeron)
}

/// The log and the set copied into the member's archive: the log's copy, the set's, and where the standby's log
/// ends.
fn copy_log(
    options: &Options,
    aeron: &Aeron,
    set: Option<Snapshot>,
    log_recording: i64,
    standby_archive: &(String, i32),
    response: (&str, i32),
    archive_response: i32,
) -> Result<(i64, Option<CopiedSet>, i64), String> {
    let member = Archive::connect(
        aeron,
        "the member's archive",
        (LOCAL_CONTROL, CONTROL_STREAM_ID),
        (LOCAL_CONTROL, archive_response),
    )?;
    let standby = Archive::connect(
        aeron,
        "the standby's archive",
        (&standby_archive.0, standby_archive.1),
        response,
    )?;
    let standby_end = standby.end(log_recording)?;
    let upto = match set {
        Some(set) if options.stop_at_set => {
            println!(
                "node {}: --stop-at-set: the log copied only to {}, the standby's after it left out (a bug planted)",
                options.node, set.log_position
            );
            Some(set.log_position)
        }
        None if options.stop_at_set => return Err("--stop-at-set: the standby has no set".into()),
        _ => None,
    };
    let log_copy = member.copy(
        "the log",
        &standby,
        log_recording,
        (&standby_archive.0, standby_archive.1),
        upto,
    )?;
    let copied_set = match set {
        Some(set) if set.log_position > standby_end => {
            return Err(format!(
                "the set at {} is past the log's end at {standby_end}",
                set.log_position
            ));
        }
        Some(set) => Some(CopiedSet {
            set,
            service_recording_id: member.copy(
                "its service's snapshot",
                &standby,
                set.service_recording_id,
                (&standby_archive.0, standby_archive.1),
                None,
            )?,
            module_recording_id: member.copy(
                "its module's snapshot",
                &standby,
                set.module_recording_id,
                (&standby_archive.0, standby_archive.1),
                None,
            )?,
        }),
        None => None,
    };
    Ok((log_copy, copied_set, standby_end))
}

/// The newest recording of each stream of every extra archive copied from the standby's into the member's, on
/// a client of its own.
fn copy_extra_archives(options: &Options) -> Result<(), String> {
    if options.extra_archives.is_empty() {
        return Ok(());
    }
    let aeron = client(options, "extra archives")?;
    let response = format!("aeron:udp?endpoint={}:0", options.host);
    for (i, extra) in options.extra_archives.iter().enumerate() {
        let responses = RESPONSE_STREAM_BASE + 2 + 2 * i as i32;
        let channel = control_channel(&extra.endpoint);
        let member = Archive::connect(
            &aeron,
            &format!("the member's archive on control stream {}", extra.control_stream),
            (LOCAL_CONTROL, extra.control_stream),
            (LOCAL_CONTROL, responses),
        )?;
        let standby = Archive::connect(
            &aeron,
            &format!("the standby's archive at {}", extra.endpoint),
            (&channel, extra.control_stream),
            (&response, responses + 1),
        )?;
        for (stream, recording) in standby.newest_by_stream()? {
            if extra.streams.is_empty() || extra.streams.contains(&stream) {
                member.copy(
                    &format!("stream {stream} of {}", extra.endpoint),
                    &standby,
                    recording,
                    (&channel, extra.control_stream),
                    None,
                )?;
            }
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    let options = match Options::parse(&Args::from_env()) {
        Ok(options) => options,
        Err(e) => {
            eprintln!("standby_seed: {e}");
            eprintln!(
                "usage: standby_seed --node N [--hosts H0,H1,...|--members M] [--host HOST] [--dir DIR] \
                 [--aeron-dir DIR] [--port-base B] [--port-stride S] [--standby N] [--standby-host HOST] \
                 [--standby-archive H:P] [--standby-dir DIR] [--cluster-dir DIR] \
                 [--extra-archive endpoint=H:P,control-stream=ID[,streams=A+B]]... [--no-snapshot] [--stop-at-set]"
            );
            return ExitCode::from(2);
        }
    };
    if options.member_log().exists() {
        eprintln!(
            "standby_seed: {} exists: node {} is no new member; seed only an empty one",
            options.member_log().display(),
            options.node
        );
        return ExitCode::from(2);
    }
    match seed(&options) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("standby_seed: node {}: {e}", options.node);
            ExitCode::from(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(line: &str) -> Result<Options, String> {
        Options::parse(&Args::new(line.split_whitespace().map(String::from).collect()))
    }

    #[test]
    fn the_layout_places_the_member_and_the_standby() {
        let o = parse("--node 1 --standby-host s").unwrap();
        assert_eq!((o.host.as_str(), o.standby_archive.as_str()), ("localhost", "s:21301"));
        assert_eq!(
            o.member_log(),
            Path::new("data/nodes/node1/cluster").join(recording_log::FILE_NAME)
        );
        assert_eq!(o.standby_dir, Path::new("data/nodes/standby0"));
        assert!(o.extra_archives.is_empty() && o.snapshot && !o.stop_at_set);
        let o = parse(
            "--node 4 --members 5 --standby 1 --standby-archive x:9 --standby-dir sb --cluster-dir c --no-snapshot \
             --extra-archive endpoint=x:7,control-stream=110 --extra-archive endpoint=y:8,control-stream=111,streams=10+11",
        )
        .unwrap();
        assert_eq!(
            (o.standby_archive.as_str(), o.standby_dir.as_path()),
            ("x:9", Path::new("sb"))
        );
        assert_eq!(o.member_log(), Path::new("c").join(recording_log::FILE_NAME));
        assert_eq!(
            o.extra_archives,
            [
                ExtraArchive {
                    endpoint: "x:7".into(),
                    control_stream: 110,
                    streams: vec![]
                },
                ExtraArchive {
                    endpoint: "y:8".into(),
                    control_stream: 111,
                    streams: vec![10, 11]
                },
            ]
        );
        assert!(!o.snapshot);
    }

    #[test]
    fn refuses_what_seeds_no_member() {
        assert!(parse("").is_err());
        assert!(parse("--node 3").is_err());
        assert!(parse("--node 0 --extra-archive endpoint=x:7").is_err());
        assert!(parse("--node 0 --extra-archive endpoint=x:7,control-stream=a").is_err());
        assert!(parse("--node 0 --extra-archive endpoint=x:7,control-stream=1,colour=red").is_err());
    }
}
