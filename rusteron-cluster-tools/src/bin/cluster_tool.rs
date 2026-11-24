//! A cluster's nodes from their media drivers' counters, and its control
//! toggle, as Aeron's `ClusterTool` reads and sets them.
//!
//! ```text
//! cluster_tool status [LAYOUT] [--standbys K] [--await-leader TIMEOUT]
//! cluster_tool snapshot [LAYOUT] [--timeout 30s]
//! cluster_tool standby-snapshot [LAYOUT] [--timeout 30s]
//! ```
//!
//! `LAYOUT` is `--hosts H0,H1,...` (or `--members M`), `--dir DIR` and
//! `--aeron-dir DIR`: the nodes' drivers are where the layout puts them
//! ([`rusteron_cluster_tools::layout`]), the members', then `--standbys`
//! standbys'.
//!
//! `status` reads each driver's CnC file as a monitoring tool reads it, with
//! no client of any driver, and prints a line a node:
//!
//! ```text
//! node 0 role=LEADER module=ACTIVE election=CLOSED term=2 commit=1048576
//! node 1 down
//! standby 0 commit=1048576
//! ```
//!
//! `down` is a node whose driver is not running, or whose consensus module
//! has no counters on it yet. A standby shows only the commit position its
//! module feeds its service. With `--await-leader TIMEOUT` (`5s`, `500ms`)
//! it waits until a member is LEADER with its module ACTIVE and its
//! election CLOSED, then prints the nodes, and fails if none is by then.
//!
//! `snapshot` and `standby-snapshot` set the leader's control toggle, from
//! NEUTRAL, through a client of its driver: the cluster appends the snapshot
//! action, the members' or a standby's. Then they wait for the leader's
//! count of snapshots taken, or of standby snapshots it heard of, to move,
//! `--timeout` at most.
//!
//! Exits 0 on success, 1 on a failure or a timeout, 2 on a bad command line.

use std::path::Path;
use std::process::ExitCode;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use rusteron_archive::{Aeron, AeronCnc, AeronContext, AeronCountersReaderRef, IntoCString};
use rusteron_cluster::container::find_counter;
use rusteron_cluster::counters::load;
use rusteron_cluster_core::counters::{
    COMMIT_POSITION_TYPE_ID, CONTROL_TOGGLE_TYPE_ID, ELECTION_STATE_TYPE_ID, LEADERSHIP_TERM_ID_TYPE_ID,
    MODULE_STATE_TYPE_ID, NODE_ROLE_TYPE_ID, SNAPSHOT_COUNTER_TYPE_ID, STANDBY_SNAPSHOT_COUNTER_TYPE_ID,
};
use rusteron_cluster_tools::args::{Args, parse_duration};
use rusteron_cluster_tools::layout::Layout;

/// `Cluster.Role`, by its counter's value.
const ROLES: [&str; 3] = ["FOLLOWER", "CANDIDATE", "LEADER"];
/// `ConsensusModule.State`.
const MODULE_STATES: [&str; 7] = [
    "INIT",
    "ACTIVE",
    "SUSPENDED",
    "SNAPSHOT",
    "QUITTING",
    "TERMINATING",
    "CLOSED",
];
/// `ElectionState`.
const ELECTION_STATES: [&str; 18] = [
    "INIT",
    "CANVASS",
    "NOMINATE",
    "CANDIDATE_BALLOT",
    "FOLLOWER_BALLOT",
    "LEADER_LOG_REPLICATION",
    "LEADER_REPLAY",
    "LEADER_INIT",
    "LEADER_READY",
    "FOLLOWER_LOG_REPLICATION",
    "FOLLOWER_REPLAY",
    "FOLLOWER_CATCHUP_INIT",
    "FOLLOWER_CATCHUP_AWAIT",
    "FOLLOWER_CATCHUP",
    "FOLLOWER_LOG_INIT",
    "FOLLOWER_LOG_AWAIT",
    "FOLLOWER_READY",
    "CLOSED",
];
const LEADER: i64 = 2;
const ACTIVE: i64 = 1;
const ELECTION_CLOSED: i64 = 17;
/// `ClusterControl.ToggleState`.
const NEUTRAL: i64 = 1;
const SNAPSHOT: i64 = 4;
const STANDBY_SNAPSHOT: i64 = 7;

/// A node's counters, those it has.
#[derive(Default)]
struct Node {
    role: Option<i64>,
    module: Option<i64>,
    election: Option<i64>,
    term: Option<i64>,
    commit: Option<i64>,
}

impl Node {
    /// The counters on the driver in `aeron_dir`, or none if it is not running.
    fn read(aeron_dir: &Path) -> Option<Node> {
        if !aeron_dir.join("cnc.dat").exists() {
            return None;
        }
        let dir = aeron_dir.to_string_lossy().into_owned();
        let cnc = AeronCnc::open(&dir.as_str().into_c_string()).ok()?;
        let reader = cnc.counters_reader();
        let value = |type_id: i32| {
            find_counter(&reader, |t, _| t == type_id)
                .and_then(|(counter_id, _)| reader.get_counter_value(counter_id).ok())
        };
        Some(Node {
            role: value(NODE_ROLE_TYPE_ID),
            module: value(MODULE_STATE_TYPE_ID),
            election: value(ELECTION_STATE_TYPE_ID),
            term: value(LEADERSHIP_TERM_ID_TYPE_ID),
            commit: value(COMMIT_POSITION_TYPE_ID),
        })
    }

    fn leads(&self) -> bool {
        self.role == Some(LEADER) && self.module == Some(ACTIVE) && self.election == Some(ELECTION_CLOSED)
    }
}

fn name(names: &[&'static str], value: Option<i64>) -> String {
    match value {
        Some(v) => usize::try_from(v)
            .ok()
            .and_then(|i| names.get(i))
            .map_or_else(|| v.to_string(), |n| n.to_string()),
        None => "-".into(),
    }
}

fn number(value: Option<i64>) -> String {
    value.map_or_else(|| "-".into(), |v| v.to_string())
}

/// The members' lines and the standbys', and the member that leads, if one does.
fn status(layout: &Layout, standbys: usize) -> Result<(Vec<String>, Option<usize>), String> {
    let mut lines = Vec::new();
    let mut leader = None;
    for i in 0..layout.members() {
        match Node::read(&layout.aeron_dir(&layout.member_dir(i))?) {
            Some(node) if node.role.is_some() || node.commit.is_some() => {
                if node.leads() {
                    leader = Some(i);
                }
                lines.push(format!(
                    "node {i} role={} module={} election={} term={} commit={}",
                    name(&ROLES, node.role),
                    name(&MODULE_STATES, node.module),
                    name(&ELECTION_STATES, node.election),
                    number(node.term),
                    number(node.commit)
                ));
            }
            _ => lines.push(format!("node {i} down")),
        }
    }
    for n in 0..standbys {
        match Node::read(&layout.aeron_dir(&layout.standby_dir(n))?) {
            Some(node) if node.commit.is_some() => lines.push(format!("standby {n} commit={}", number(node.commit))),
            _ => lines.push(format!("standby {n} down")),
        }
    }
    Ok((lines, leader))
}

/// A counter's value as an atomic, for a compare-and-set.
fn atomic(reader: &AeronCountersReaderRef, counter_id: i32) -> &AtomicI64 {
    // SAFETY: a counter's value is an aligned i64 in the driver's counters file, mapped for as long as the client
    // that gave the reader is open; other processes change it only with atomic instructions.
    unsafe { AtomicI64::from_ptr(reader.addr(counter_id)) }
}

/// Sets the leader's control toggle to `toggle`, from NEUTRAL, and waits for its counter `counted` to move.
fn toggle(layout: &Layout, toggle: i64, counted: i32, timeout: Duration) -> Result<String, String> {
    let (_, leader) = status(layout, 0)?;
    let leader = leader.ok_or("no member leads")?;
    let dir = layout.aeron_dir(&layout.member_dir(leader))?.display().to_string();
    let aeron = AeronContext::new()
        .and_then(|context| {
            context.set_dir(&dir.as_str().into_c_string())?;
            context.set_client_name(&"cluster_tool".into_c_string())?;
            let aeron = Aeron::new(&context)?;
            aeron.start()?;
            Ok(aeron)
        })
        .map_err(|e| format!("a client on node {leader}'s driver: {e}"))?;
    let reader = aeron.counters_reader();
    let find = |type_id: i32, what: &str| {
        find_counter(&reader, |t, _| t == type_id)
            .map(|(id, _)| id)
            .ok_or_else(|| format!("node {leader} has no {what} counter"))
    };
    let (toggle_id, count_id) = (
        find(CONTROL_TOGGLE_TYPE_ID, "control toggle")?,
        find(counted, "snapshot")?,
    );
    let before = load(&reader, count_id);
    if let Err(state) = atomic(&reader, toggle_id).compare_exchange(NEUTRAL, toggle, Ordering::SeqCst, Ordering::SeqCst)
    {
        return Err(format!(
            "node {leader}'s control toggle is {state}, not NEUTRAL: another action is under way"
        ));
    }
    let deadline = Instant::now() + timeout;
    loop {
        let now = load(&reader, count_id);
        if now > before {
            return Ok(format!("node {leader}: {before} -> {now}"));
        }
        if Instant::now() > deadline {
            return Err(format!(
                "node {leader}: the count stayed at {before} for {} s",
                timeout.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn run(args: &Args) -> Result<ExitCode, String> {
    let layout = Layout::parse(args)?;
    let duration = |key: &str, default: Duration| match args.value(key) {
        Some(t) => parse_duration(t).ok_or_else(|| format!("{key} {t}: a duration (5s, 500ms)")),
        None => Ok(default),
    };
    match args.as_slice().first().map(String::as_str) {
        Some("status") => {
            let standbys = args.number("--standbys", 0usize)?;
            let deadline = match args.value("--await-leader") {
                Some(_) => Some(Instant::now() + duration("--await-leader", Duration::ZERO)?),
                None => None,
            };
            loop {
                let (lines, leader) = status(&layout, standbys)?;
                if deadline.is_none_or(|d| leader.is_some() || Instant::now() >= d) {
                    for line in lines {
                        println!("{line}");
                    }
                    let failed = deadline.is_some() && leader.is_none();
                    return Ok(if failed { ExitCode::from(1) } else { ExitCode::SUCCESS });
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        Some(which @ ("snapshot" | "standby-snapshot")) => {
            let (value, counted) = match which {
                "snapshot" => (SNAPSHOT, SNAPSHOT_COUNTER_TYPE_ID),
                _ => (STANDBY_SNAPSHOT, STANDBY_SNAPSHOT_COUNTER_TYPE_ID),
            };
            match toggle(&layout, value, counted, duration("--timeout", Duration::from_secs(30))?) {
                Ok(done) => {
                    println!("{which}: {done}");
                    Ok(ExitCode::SUCCESS)
                }
                Err(e) => {
                    println!("{which} FAILED: {e}");
                    Ok(ExitCode::from(1))
                }
            }
        }
        _ => Err("status, snapshot or standby-snapshot".into()),
    }
}

fn main() -> ExitCode {
    match run(&Args::from_env()) {
        Ok(code) => code,
        Err(e) => {
            eprintln!(
                "cluster_tool: {e}; usage: cluster_tool status|snapshot|standby-snapshot [--hosts H0,H1,...] \
                 [--dir DIR] [--aeron-dir DIR] [--standbys K] [--await-leader TIMEOUT] [--timeout TIMEOUT]"
            );
            ExitCode::from(2)
        }
    }
}
