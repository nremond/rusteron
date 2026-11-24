//! A node's own process ([`node_driver`](../node_driver/index.html)): Aeron's
//! C media driver, with the node's archive, consensus module and service as
//! processes of their own beside it on its directory. The driver does a
//! node's other jobs that a harness relies on:
//!
//! - it waits out the mark files of a node killed moments ago whose writers
//!   nothing proves gone, which the archive and the consensus module would
//!   take for a live process's ([`mark_files`]);
//! - it prints an up line once the driver runs, which scripts wait for
//!   before they start the node's other processes ([`Options::up_line`]);
//! - it prints the driver's error log as errors come ([`ErrorLog`]): the C
//!   driver keeps them only in `cnc.dat`, which goes with its directory;
//! - it closes in order on SIGTERM, its directory deleted.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::args::Args;
use crate::layout::Layout;

/// The driver's threading mode, by Aeron's `ThreadingMode` names; not
/// INVOKER, which needs a host process to run the driver's duty cycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Threading {
    /// One thread for the conductor, the sender and the receiver.
    Shared,
    /// The conductor on one, the sender and the receiver on another.
    SharedNetwork,
    /// A thread each.
    Dedicated,
}

impl Threading {
    /// Aeron's name for it.
    pub fn name(self) -> &'static str {
        match self {
            Threading::Shared => "SHARED",
            Threading::SharedNetwork => "SHARED_NETWORK",
            Threading::Dedicated => "DEDICATED",
        }
    }

    /// By its exact name, as `ThreadingMode.valueOf` reads it.
    fn parse(name: &str) -> Result<Self, String> {
        match name {
            "SHARED" => Ok(Threading::Shared),
            "SHARED_NETWORK" => Ok(Threading::SharedNetwork),
            "DEDICATED" => Ok(Threading::Dedicated),
            "INVOKER" => Err("--threading INVOKER: the driver runs on threads of its own, in no host".into()),
            _ => Err(format!("--threading {name}: SHARED, SHARED_NETWORK or DEDICATED")),
        }
    }
}

/// What the node's driver runs with. It reads what it uses of a command
/// line and ignores the rest, so a script can pass every node process the
/// same options.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    /// The node's slot: `--node N` for a member, the slot after the members' for `--standby N`.
    pub node: usize,
    /// `--standby N`: the node is standby `N`'s.
    pub standby: Option<usize>,
    /// The cluster's layout ([`Layout::parse`]).
    pub layout: Layout,
    /// The node's host: its member's in `--hosts`, or `--host` for a standby (localhost by default).
    pub host: String,
    /// The node's files: `node<N>` or `standby<N>` under `--dir`.
    pub node_dir: PathBuf,
    /// The driver's directory ([`Layout::aeron_dir`]).
    pub aeron_dir: PathBuf,
    /// `--spin`: every driver agent busy-spins.
    pub spin: bool,
    /// `--threading`, SHARED by default.
    pub threading: Threading,
}

impl Options {
    /// The options from a command line.
    pub fn parse(args: &Args) -> Result<Options, String> {
        let layout = Layout::parse(args)?;
        let standby = args
            .optional::<usize>("--standby")
            .map_err(|_| "--standby N: a standby's number".to_string())?;
        let (node, node_dir, host) = match standby {
            Some(n) => (
                layout.standby_slot(n),
                layout.standby_dir(n),
                args.value("--host").unwrap_or("localhost").to_string(),
            ),
            None => {
                let node: usize = args
                    .required("--node")?
                    .parse()
                    .map_err(|_| "--node N: a node's number".to_string())?;
                if node >= layout.members() {
                    return Err(format!("--node {node}: of {} members", layout.members()));
                }
                (node, layout.member_dir(node), layout.hosts[node].clone())
            }
        };
        let aeron_dir = layout.aeron_dir(&node_dir)?;
        Ok(Options {
            node,
            standby,
            host,
            node_dir,
            aeron_dir,
            spin: args.flag("--spin"),
            threading: Threading::parse(args.value("--threading").unwrap_or("SHARED"))?,
            layout,
        })
    }

    /// The line scripts wait for (`" up on "`) before they start the node's
    /// other processes.
    pub fn up_line(&self) -> String {
        let idle = if self.spin {
            "spinning"
        } else {
            "Aeron's default idle strategies"
        };
        match self.standby {
            Some(n) => format!(
                "standby {n} of the cluster on {} up on {}, {idle}, driver {}",
                self.layout.hosts.join(","),
                self.host,
                self.threading.name(),
            ),
            None => format!(
                "node {} of {} up on {}, {idle}, driver {}",
                self.node,
                self.layout.members(),
                self.host,
                self.threading.name(),
            ),
        }
    }
}

/// Now, epoch ms: what mark files' activity timestamps count in.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// The mark files a node killed moments ago left: its archives', its
/// consensus module's and its services'. A kill leaves their activity
/// timestamps fresh, and the Rust archive and consensus module refuse to
/// start over a file touched within [`LIVENESS_MS`](mark_files::LIVENESS_MS)
/// ("active mark file detected") unless its lock proves the writer gone
/// (`rusteron_cluster_core::markfile::owner`). So only a file whose writer
/// still holds its lock, or one with no lock naming its writer (Java's, an
/// earlier Rust component's), is waited for. An orderly stop marks them
/// stopped, and nothing is waited for.
pub mod mark_files {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    use rusteron_cluster::markfile::{probe_lock, read_header};
    use rusteron_cluster_core::markfile::{
        self,
        owner::{self, Owner},
    };

    /// How long after its last activity a mark file's process counts as
    /// gone: the rule of rusteron's archive and consensus module, and Aeron's.
    pub const LIVENESS_MS: i64 = markfile::LIVENESS_TIMEOUT_MS;
    /// How long a start waits for them: files still being touched by then are
    /// a live process's, and the start is left to refuse them.
    pub const PATIENCE_MS: i64 = 2 * LIVENESS_MS;
    /// An archive's, in its directory: `ArchiveMarkFile.FILENAME`.
    const ARCHIVE_MARK_FILE: &str = "archive-mark.dat";
    /// A consensus module's (`cluster-mark.dat`) and a service container's
    /// (`cluster-mark-service-<id>.dat`) start so.
    const CLUSTER_MARK_PREFIX: &str = "cluster-mark";

    /// Waits until no mark file of the node in `node_dir` has been touched
    /// within `liveness_ms`, for `patience_ms` at most, and not at all once
    /// `stop` is set (SIGTERM). Returns how long it waited.
    pub fn await_stale(node_dir: &Path, liveness_ms: i64, patience_ms: i64, stop: &AtomicBool) -> Duration {
        let start = Instant::now();
        let patience = Duration::from_millis(u64::try_from(patience_ms).unwrap_or(0));
        loop {
            let waited = start.elapsed();
            let Some(touched) = last_touched(node_dir) else {
                return waited;
            };
            let age = super::now_ms() - touched;
            if age >= liveness_ms || waited >= patience || stop.load(Ordering::Acquire) {
                return waited;
            }
            thread::sleep(Duration::from_millis((liveness_ms - age).clamp(1, 100).unsigned_abs()));
        }
    }

    /// When the last of the node's mark files whose writers may still run was
    /// touched, epoch ms: none if no writer may.
    pub fn last_touched(node_dir: &Path) -> Option<i64> {
        let now_ms = super::now_ms();
        files(node_dir).iter().filter_map(|file| held_since(file, now_ms)).max()
    }

    /// The node's mark files, in any directory directly under its own: its
    /// archives' (`archive-mark.dat`), and its consensus module's and
    /// services' (`cluster-mark*.dat`).
    fn files(node_dir: &Path) -> Vec<PathBuf> {
        let Ok(dirs) = fs::read_dir(node_dir) else {
            return Vec::new();
        };
        let mut files = Vec::new();
        for dir in dirs
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
        {
            let Ok(entries) = fs::read_dir(&dir) else { continue };
            files.extend(entries.filter_map(Result::ok).map(|entry| entry.path()).filter(|path| {
                path.file_name().and_then(|name| name.to_str()).is_some_and(|name| {
                    name == ARCHIVE_MARK_FILE || (name.starts_with(CLUSTER_MARK_PREFIX) && name.ends_with(".dat"))
                })
            }));
        }
        files
    }

    /// When a mark file's writer was last seen alive, if it may still run
    /// (`owner`): now, while it holds the file's lock; else its activity
    /// timestamp, if nothing proves it gone and that is within the liveness
    /// timeout. None once its lock proves it gone, or the file is stopped,
    /// unwritten, or too short to read: the start is left to judge it. Both
    /// formats, the archive's and the cluster's, start with the same header.
    fn held_since(path: &Path, now_ms: i64) -> Option<i64> {
        let dir = path.parent()?;
        let name = path.file_name()?.to_str()?;
        match owner::owner(read_header(path).as_ref(), &probe_lock(dir, name), now_ms) {
            Owner::Locked => Some(now_ms),
            Owner::Active { activity_timestamp_ms } => Some(activity_timestamp_ms),
            Owner::Gone => None,
        }
    }
}

/// The C driver's error log, `cnc.dat`'s, as the node prints it: an error
/// when first seen, and again with its count when seen since. Aeron keeps
/// one entry for each distinct error, its count and last observation moving
/// as it recurs.
#[derive(Debug, Default)]
pub struct ErrorLog {
    /// The observations printed of each error, by its text.
    printed: HashMap<String, i32>,
    /// The latest observation printed: the log need be read only from there.
    since: i64,
}

impl ErrorLog {
    /// Where to read the log from: the errors last observed at or after it.
    pub fn since(&self) -> i64 {
        self.since
    }

    /// The line to print for an entry of the log, if it has news.
    pub fn line(&mut self, node: usize, observations: i32, first_ms: i64, last_ms: i64, error: &str) -> Option<String> {
        self.since = self.since.max(last_ms);
        let error = error.trim_end();
        if self.printed.get(error) == Some(&observations) {
            return None;
        }
        self.printed.insert(error.to_string(), observations);
        Some(if observations == 1 {
            format!("node {node}: driver error at {}: {error}", utc(first_ms))
        } else {
            format!(
                "node {node}: driver error, {observations} times from {} to {}: {error}",
                utc(first_ms),
                utc(last_ms)
            )
        })
    }
}

/// Epoch ms in UTC, `1970-01-01T00:00:00.000Z`.
pub fn utc(ms: i64) -> String {
    let (days, ms) = (ms.div_euclid(86_400_000), ms.rem_euclid(86_400_000));
    // Howard Hinnant's civil_from_days: eras of 400 years from 0000-03-01.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_from_march = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_from_march + 2) / 5 + 1;
    let month = if month_from_march < 10 {
        month_from_march + 3
    } else {
        month_from_march - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        ms / 3_600_000,
        ms / 60_000 % 60,
        ms / 1000 % 60,
        ms % 1000
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusteron_cluster_core::markfile::{ACTIVITY_TIMESTAMP_OFFSET, SEMANTIC_VERSION, VERSION_OFFSET};
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    fn parse(line: &str) -> Result<Options, String> {
        Options::parse(&Args::new(line.split_whitespace().map(String::from).collect()))
    }

    #[test]
    fn reads_a_scripts_command_line_and_ignores_the_rest() {
        let options = parse(
            "--node 2 --hosts localhost,localhost,localhost --dir data/nodes --aeron-dir /dev/shm/cluster \
             --sync 1 --spin --threading DEDICATED --no-event-log --verify-archives --standby-snapshots 5",
        )
        .unwrap();
        assert_eq!(
            options,
            Options {
                node: 2,
                standby: None,
                layout: Layout {
                    aeron_root: Some("/dev/shm/cluster".into()),
                    ..Layout::default()
                },
                host: "localhost".into(),
                node_dir: "data/nodes/node2".into(),
                aeron_dir: "/dev/shm/cluster/node2".into(),
                spin: true,
                threading: Threading::Dedicated,
            }
        );
        assert_eq!(
            options.up_line(),
            "node 2 of 3 up on localhost, spinning, driver DEDICATED"
        );
    }

    #[test]
    fn defaults() {
        let options = parse("--node 0").unwrap();
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(options.layout, Layout::default());
        // Without --aeron-dir, the driver's directory is in the node's own, made absolute.
        assert_eq!(options.aeron_dir, cwd.join("data/nodes/node0/aeron"));
        assert_eq!((options.spin, options.threading), (false, Threading::Shared));
        assert_eq!(
            options.up_line(),
            "node 0 of 3 up on localhost, Aeron's default idle strategies, driver SHARED"
        );
        let relative = parse("--node 1 --aeron-dir shm --hosts a,b,c --threading SHARED_NETWORK").unwrap();
        assert_eq!(relative.aeron_dir, cwd.join("shm/node1"));
        assert_eq!(
            (relative.host.as_str(), relative.threading.name()),
            ("b", "SHARED_NETWORK")
        );
    }

    #[test]
    fn a_standby_takes_the_slot_after_the_members() {
        let options = parse(
            "--standby 0 --host 10.0.0.4 --hosts 10.0.0.1,10.0.0.2,10.0.0.3 --dir data/nodes \
             --aeron-dir /dev/shm/cluster --verify-archives",
        )
        .unwrap();
        assert_eq!(
            (options.node, options.standby, options.host.as_str()),
            (3, Some(0), "10.0.0.4")
        );
        assert_eq!(options.node_dir, Path::new("data/nodes/standby0"));
        assert_eq!(options.aeron_dir, Path::new("/dev/shm/cluster/standby0"));
        assert_eq!(
            options.up_line(),
            "standby 0 of the cluster on 10.0.0.1,10.0.0.2,10.0.0.3 up on 10.0.0.4, Aeron's default idle \
             strategies, driver SHARED"
        );
        let cwd = std::env::current_dir().unwrap();
        let local = parse("--standby 1 --members 5").unwrap();
        assert_eq!((local.node, local.host.as_str()), (6, "localhost"));
        assert_eq!(local.aeron_dir, cwd.join("data/nodes/standby1/aeron"));
    }

    #[test]
    fn refuses_what_it_cannot_run() {
        for (line, why) in [
            ("--dir data", "--node is required"),
            ("--node x", "--node N: a node's number"),
            ("--node 3", "--node 3: of 3 members"),
            ("--node 1 --hosts a", "--node 1: of 1 members"),
            (
                "--node 0 --threading INVOKER",
                "--threading INVOKER: the driver runs on threads of its own, in no host",
            ),
            (
                "--node 0 --threading shared",
                "--threading shared: SHARED, SHARED_NETWORK or DEDICATED",
            ),
            ("--standby x", "--standby N: a standby's number"),
        ] {
            assert_eq!(parse(line), Err(why.to_string()), "{line}");
        }
    }

    /// A directory of its own under the system's temporary one, deleted on drop.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let dir = std::env::temp_dir().join(format!(
                "rusteron-cluster-tools-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();
            Scratch(dir)
        }

        /// A mark file at `path` under it, with this header.
        fn mark_file(&self, path: &str, version: i32, activity: i64) {
            let path = self.0.join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            let mut bytes = vec![0; 64];
            bytes[VERSION_OFFSET..][..4].copy_from_slice(&version.to_le_bytes());
            bytes[ACTIVITY_TIMESTAMP_OFFSET..][..8].copy_from_slice(&activity.to_le_bytes());
            fs::write(path, bytes).unwrap();
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn waited(node: &Scratch, liveness_ms: i64, patience_ms: i64) -> Duration {
        mark_files::await_stale(&node.0, liveness_ms, patience_ms, &AtomicBool::new(false))
    }

    #[test]
    fn stale_files_are_not_waited_for() {
        let node = Scratch::new();
        let now = now_ms();
        node.mark_file(
            "archive/archive-mark.dat",
            SEMANTIC_VERSION,
            now - mark_files::LIVENESS_MS - 1_000,
        );
        node.mark_file(
            "cluster/cluster-mark.dat",
            SEMANTIC_VERSION,
            now - 3 * mark_files::LIVENESS_MS,
        );
        assert_eq!(mark_files::last_touched(&node.0), None);
        assert!(waited(&node, mark_files::LIVENESS_MS, mark_files::PATIENCE_MS) < Duration::from_millis(100));
    }

    fn container(start_timestamp_ms: i64) -> rusteron_cluster_core::markfile::ContainerHeader {
        rusteron_cluster_core::markfile::ContainerHeader {
            start_timestamp_ms,
            pid: i64::from(std::process::id()),
            archive_stream_id: 10,
            service_stream_id: 104,
            consensus_module_stream_id: 105,
            service_id: 0,
            cluster_id: 0,
            error_buffer_length: rusteron_cluster_core::markfile::ERROR_BUFFER_MIN_LENGTH as i32,
            aeron_directory: "/tmp/aeron".into(),
            control_channel: "aeron:ipc".into(),
            service_name: "test".into(),
        }
    }

    #[test]
    fn files_whose_locks_prove_their_writers_gone_are_not_waited_for() {
        let node = Scratch::new();
        let now = now_ms();
        let mark =
            rusteron_cluster::markfile::MarkFile::create(&node.0.join("service"), &container(now), 4_096, now).unwrap();
        // A kill: the lock goes with the process, the activity stays fresh.
        drop(mark);
        assert_eq!(mark_files::last_touched(&node.0), None);
        assert!(waited(&node, mark_files::LIVENESS_MS, mark_files::PATIENCE_MS) < Duration::from_millis(100));
    }

    #[test]
    fn files_whose_writers_hold_their_locks_are_waited_for_however_stale() {
        let node = Scratch::new();
        let then = now_ms() - 3 * mark_files::LIVENESS_MS;
        let _mark =
            rusteron_cluster::markfile::MarkFile::create(&node.0.join("service"), &container(then), 4_096, then)
                .unwrap();
        let waited = waited(&node, mark_files::LIVENESS_MS, 300);
        assert!(
            waited >= Duration::from_millis(300) && waited < Duration::from_millis(1_500),
            "{waited:?}"
        );
    }

    #[test]
    fn an_active_file_is_waited_out() {
        let node = Scratch::new();
        node.mark_file("cluster/cluster-mark-service-0.dat", SEMANTIC_VERSION, now_ms());
        let waited = waited(&node, 400, 5_000);
        assert!(
            waited >= Duration::from_millis(390) && waited < Duration::from_millis(1_500),
            "{waited:?}"
        );
    }

    #[test]
    fn files_still_touched_are_waited_for_only_so_long() {
        let node = Scratch::new();
        node.mark_file("other-archive/archive-mark.dat", SEMANTIC_VERSION, now_ms());
        let waited = waited(&node, mark_files::LIVENESS_MS, 300);
        assert!(
            waited >= Duration::from_millis(300) && waited < Duration::from_millis(1_500),
            "{waited:?}"
        );
    }

    #[test]
    fn sigterm_stops_the_wait_at_once() {
        let node = Scratch::new();
        node.mark_file("cluster/cluster-mark.dat", SEMANTIC_VERSION, now_ms());
        let stop = AtomicBool::new(true);
        let waited = mark_files::await_stale(&node.0, mark_files::LIVENESS_MS, mark_files::PATIENCE_MS, &stop);
        assert!(waited < Duration::from_millis(100), "{waited:?}");
    }

    #[test]
    fn files_stopped_in_order_or_unwritten_or_unreadable_are_not_waited_for() {
        let node = Scratch::new();
        let now = now_ms();
        // Stopped in order: Java writes -1 as the activity, the Rust components zero the version.
        node.mark_file("archive/archive-mark.dat", SEMANTIC_VERSION, -1);
        node.mark_file("cluster/cluster-mark.dat", 0, now);
        // Failed to start.
        node.mark_file("cluster/cluster-mark-service-0.dat", -1, now);
        // Not written yet.
        node.mark_file("service/cluster-mark-service-0.dat", SEMANTIC_VERSION, 0);
        // Too short to hold a header, or empty.
        fs::create_dir_all(node.0.join("other")).unwrap();
        fs::write(node.0.join("other/archive-mark.dat"), [1; 12]).unwrap();
        fs::write(node.0.join("cluster/cluster-mark-service-1.dat"), []).unwrap();
        assert_eq!(mark_files::last_touched(&node.0), None);
        assert!(waited(&node, mark_files::LIVENESS_MS, mark_files::PATIENCE_MS) < Duration::from_millis(100));
    }

    #[test]
    fn the_files_are_the_archives_the_modules_and_the_services_in_any_directory_of_the_node() {
        let now = now_ms();
        for path in [
            "archive/archive-mark.dat",
            "events/archive-mark.dat",
            "cluster/cluster-mark.dat",
            "cluster/cluster-mark-service-0.dat",
            "service/cluster-mark-service-0.dat",
        ] {
            let node = Scratch::new();
            node.mark_file(path, SEMANTIC_VERSION, now);
            assert_eq!(mark_files::last_touched(&node.0), Some(now), "{path}");
        }
        let node = Scratch::new();
        for path in [
            "archive-mark.dat",
            "cluster-mark.dat",
            "archive/archive-mark.lnk",
            "cluster/cluster-mark.lnk",
            "cluster/node-state.dat",
            "deeper/still/archive-mark.dat",
        ] {
            node.mark_file(path, SEMANTIC_VERSION, now);
        }
        assert_eq!(mark_files::last_touched(&node.0), None);
    }

    #[test]
    fn an_error_is_printed_when_new_and_when_seen_again() {
        let mut log = ErrorLog::default();
        let at = 1_793_397_789_123;
        assert_eq!(
            log.line(1, 1, at, at, "(-1) unknown error code\n[aeron_x, aeron_x.c:12] boom\n")
                .as_deref(),
            Some(
                "node 1: driver error at 2026-10-30T22:03:09.123Z: (-1) unknown error code\n[aeron_x, aeron_x.c:12] boom"
            )
        );
        assert_eq!(log.since(), at);
        // Read again from there: no news.
        assert_eq!(
            log.line(1, 1, at, at, "(-1) unknown error code\n[aeron_x, aeron_x.c:12] boom\n"),
            None
        );
        assert_eq!(
            log.line(
                1,
                3,
                at,
                at + 2_000,
                "(-1) unknown error code\n[aeron_x, aeron_x.c:12] boom\n"
            )
            .as_deref(),
            Some(
                "node 1: driver error, 3 times from 2026-10-30T22:03:09.123Z to 2026-10-30T22:03:11.123Z: \
                 (-1) unknown error code\n[aeron_x, aeron_x.c:12] boom"
            )
        );
        assert_eq!(log.since(), at + 2_000);
        // Another error in the same ms as the last read is news too.
        assert!(log.line(1, 1, at + 2_000, at + 2_000, "(-2) other").is_some());
    }

    #[test]
    fn utc_is_iso_8601() {
        assert_eq!(utc(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(utc(-1), "1969-12-31T23:59:59.999Z");
        assert_eq!(utc(951_782_400_000), "2000-02-29T00:00:00.000Z");
        assert_eq!(utc(1_793_397_789_123), "2026-10-30T22:03:09.123Z");
    }
}
