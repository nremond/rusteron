//! Aeron Cluster's `ClusterTool`: inspects and controls a cluster component
//! from its directory, with Aeron 1.54.2's commands, output and exit codes.
//!
//! ```text
//! cluster_tool <cluster-dir> <command> [options]
//! ```
//!
//! | Command | |
//! |---|---|
//! | `describe` | prints out all descriptors in the mark file |
//! | `pid` | prints PID of cluster component |
//! | `recovery-plan` | `[service count]` prints recovery plan of cluster component |
//! | `recording-log` | prints recording log of cluster component |
//! | `sort-recording-log` | reorders entries in the recording log to match the order in memory |
//! | `seed-recording-log-from-snapshot` | creates a new recording log based on the latest valid snapshot |
//! | `errors` | prints Aeron and cluster component error logs |
//! | `list-members` | prints leader memberId and active members |
//! | `backup-query` | `[delay]` get, or set, time of next backup query |
//! | `invalidate-latest-snapshot` | marks the latest snapshot as a invalid so the previous is loaded |
//! | `is-leader` | returns zero if the cluster node is leader, non-zero if not |
//! | `snapshot` | triggers a snapshot on the leader |
//! | `suspend` | suspends appending to the log |
//! | `resume` | resumes appending to the log |
//! | `shutdown` | initiates an orderly stop of the cluster with a snapshot |
//! | `abort` | stops the cluster without a snapshot |
//! | `describe-latest-cm-snapshot` | prints the contents of the latest valid consensus module snapshot |
//! | `validate-recording-log` | triggers the cluster node to validate its recording log against the archive |
//!
//! Java's system properties are read as Aeron's C client reads its own,
//! from the environment: `AERON_CLUSTER_TOOL_TIMEOUT` (`aeron.cluster.tool.timeout`,
//! a duration such as `5s`, default 0), `AERON_CLUSTER_TOOL_REPLAY_CHANNEL`
//! (default `aeron:ipc`) and `AERON_CLUSTER_TOOL_REPLAY_STREAM_ID` (default 103).
//! `backup-query`'s delay is a duration too.

use std::io::Write;
use std::path::Path;

use crate::cluster_tool_operator::{ClusterToolOperator, FAILURE, Result, SUCCESS};

/// `AERON_CLUSTER_TOOL_REPLAY_CHANNEL_DEFAULT`.
pub const REPLAY_CHANNEL_DEFAULT: &str = "aeron:ipc";
/// `AERON_CLUSTER_TOOL_REPLAY_STREAM_ID_DEFAULT`.
pub const REPLAY_STREAM_ID_DEFAULT: i32 = 103;
const HELP_PREFIX: &str = "Usage: <cluster-dir> <command> [options]";

type Action = fn(&ClusterToolOperator, &Path, &mut dyn Write, &[String]) -> Result<i32>;

/// The commands, with Java's descriptions, in the order Java declares them.
pub const COMMANDS: &[(&str, &str, Action)] = &[
    (
        "describe",
        "prints out all descriptors in the mark file.",
        |o, d, out, _| o.describe_cluster_mark_file(d, out).map(|_| SUCCESS),
    ),
    ("pid", "prints PID of cluster component.", |o, d, out, _| o.pid(d, out)),
    (
        "recovery-plan",
        "[service count] prints recovery plan of cluster component.",
        |o, d, out, args| match args.get(2) {
            Some(count) => o.recovery_plan(d, count.parse().unwrap_or(0), out),
            None => {
                print_help(out)?;
                Ok(FAILURE)
            }
        },
    ),
    (
        "recording-log",
        "prints recording log of cluster component.",
        |o, d, out, _| o.recording_log(d, out),
    ),
    (
        "sort-recording-log",
        "reorders entries in the recording log to match the order in memory.",
        |o, d, _, _| o.sort_recording_log(d),
    ),
    (
        "seed-recording-log-from-snapshot",
        "creates a new recording log based on the latest valid snapshot.",
        |o, d, _, _| o.seed_recording_log_from_snapshot(d),
    ),
    (
        "errors",
        "prints Aeron and cluster component error logs.",
        |o, d, out, _| o.errors(d, out),
    ),
    (
        "list-members",
        "prints leader memberId and active members.",
        |o, d, out, _| o.list_members(d, out),
    ),
    (
        "backup-query",
        "[delay] get, or set, time of next backup query.",
        |o, d, out, args| match args.get(2) {
            None => o.print_next_backup_query(d, out),
            Some(delay) => match parse_duration_ms(delay) {
                Some(delay_ms) => o.next_backup_query(d, delay_ms, out),
                None => Err(crate::cluster_tool_operator::ToolError(format!(
                    "aeron.cluster.tool.delay: invalid duration {delay}"
                ))),
            },
        },
    ),
    (
        "invalidate-latest-snapshot",
        "marks the latest snapshot as a invalid so the previous is loaded.",
        |o, d, out, _| o.invalidate_latest_snapshot(d, out),
    ),
    (
        "is-leader",
        "returns zero if the cluster node is leader, non-zero if not.",
        |o, d, out, _| o.is_leader(d, out),
    ),
    ("snapshot", "triggers a snapshot on the leader.", |o, d, out, _| {
        o.snapshot(d, out)
    }),
    ("suspend", "suspends appending to the log.", |o, d, out, _| {
        o.suspend(d, out)
    }),
    ("resume", "resumes appending to the log.", |o, d, out, _| {
        o.resume(d, out)
    }),
    (
        "shutdown",
        "initiates an orderly stop of the cluster with a snapshot.",
        |o, d, out, _| o.shutdown(d, out),
    ),
    ("abort", "stops the cluster without a snapshot.", |o, d, out, _| {
        o.abort(d, out)
    }),
    (
        "describe-latest-cm-snapshot",
        "prints the contents of the latest valid consensus module snapshot.",
        |o, d, out, _| o.describe_latest_consensus_module_snapshot(d, out),
    ),
    (
        "validate-recording-log",
        "triggers the cluster node to validate its recording log against the archive.",
        |o, d, out, _| o.validate_recording_log(d, out),
    ),
];

/// The order Java's help lists the commands in: its command map's (an Agrona
/// `Object2ObjectHashMap`) iteration order.
const HELP_ORDER: [&str; 18] = [
    "validate-recording-log",
    "abort",
    "shutdown",
    "resume",
    "sort-recording-log",
    "recording-log",
    "pid",
    "describe",
    "seed-recording-log-from-snapshot",
    "list-members",
    "is-leader",
    "recovery-plan",
    "describe-latest-cm-snapshot",
    "snapshot",
    "invalidate-latest-snapshot",
    "suspend",
    "backup-query",
    "errors",
];

/// `ClusterToolCommand.printHelp`.
pub fn print_help(out: &mut dyn Write) -> std::io::Result<()> {
    writeln!(out, "{HELP_PREFIX}")?;
    let indent_value = COMMANDS.iter().map(|(name, _, _)| name.len()).max().unwrap_or(0) + 1;
    for name in HELP_ORDER {
        let Some((_, description, _)) = COMMANDS.iter().find(|(n, _, _)| *n == name) else {
            continue;
        };
        let indent = indent_value - name.len();
        writeln!(out, "{:>indent$}{name}: {description}", " ")?;
    }
    out.flush()
}

/// A duration as Agrona's `SystemUtil.parseDuration` reads one: a number,
/// in nanoseconds, or with `s`, `ms`, `us` or `ns`; in milliseconds here.
pub fn parse_duration_ms(value: &str) -> Option<i64> {
    let value = value.trim().to_ascii_lowercase();
    let (number, nanos_per_unit) = if let Some(n) = value.strip_suffix("ms") {
        (n, 1_000_000)
    } else if let Some(n) = value.strip_suffix("us") {
        (n, 1_000)
    } else if let Some(n) = value.strip_suffix("ns") {
        (n, 1)
    } else if let Some(n) = value.strip_suffix('s') {
        (n, 1_000_000_000)
    } else {
        (value.as_str(), 1)
    };
    let nanos = number.trim().parse::<i64>().ok()?.checked_mul(nanos_per_unit)?;
    Some(nanos / 1_000_000)
}

/// `ClusterTool.main`: the exit status, 0 on success.
pub fn main(args: &[String], out: &mut dyn Write) -> i32 {
    if args.len() < 2 {
        let _ = print_help(out);
        return -1;
    }
    let cluster_dir = Path::new(&args[0]);
    if !cluster_dir.exists() {
        eprintln!(
            "ERR: cluster directory not found: {}",
            std::path::absolute(cluster_dir)
                .unwrap_or_else(|_| cluster_dir.to_path_buf())
                .display()
        );
        let _ = print_help(out);
        return -1;
    }
    let Some((_, _, action)) = COMMANDS.iter().find(|(name, _, _)| *name == args[1]) else {
        let _ = writeln!(out, "Unknown command: {}", args[1]);
        let _ = print_help(out);
        return -1;
    };
    let timeout_ms = std::env::var("AERON_CLUSTER_TOOL_TIMEOUT")
        .ok()
        .and_then(|v| parse_duration_ms(&v))
        .unwrap_or(0);
    let channel =
        std::env::var("AERON_CLUSTER_TOOL_REPLAY_CHANNEL").unwrap_or_else(|_| REPLAY_CHANNEL_DEFAULT.to_owned());
    let stream_id = std::env::var("AERON_CLUSTER_TOOL_REPLAY_STREAM_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(REPLAY_STREAM_ID_DEFAULT);
    let operator = ClusterToolOperator::new(&channel, stream_id, timeout_ms);
    let status = match action(&operator, cluster_dir, out, args) {
        Ok(status) => status,
        Err(e) => {
            let _ = out.flush();
            eprintln!("{e}");
            1
        }
    };
    let _ = out.flush();
    status
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_read_as_agrona_reads_them() {
        assert_eq!(parse_duration_ms("5s"), Some(5_000));
        assert_eq!(parse_duration_ms("250ms"), Some(250));
        assert_eq!(parse_duration_ms("3000000000"), Some(3_000));
        assert_eq!(parse_duration_ms("x"), None);
    }

    #[test]
    fn the_help_lines_up_as_java_s() {
        let mut out = Vec::new();
        print_help(&mut out).unwrap();
        let help = String::from_utf8(out).unwrap();
        assert!(help.starts_with("Usage: <cluster-dir> <command> [options]\n"));
        assert!(help.contains("\n                         describe: prints out all descriptors in the mark file.\n"));
        assert!(help.contains("\n seed-recording-log-from-snapshot: creates a new recording log"));
        assert_eq!(help.lines().count(), 1 + COMMANDS.len(), "every command, once");
        assert!(help.lines().nth(1).unwrap().ends_with(
            "validate-recording-log: triggers the cluster node to validate its recording log against the archive."
        ));
    }
}
