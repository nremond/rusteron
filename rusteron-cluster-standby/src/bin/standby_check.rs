//! Checks of a standby against the members.
//!
//! ```text
//! standby_check states --aeron-dir DIR [--dir DIR] [--hosts H0,H1,...|--members M] [--port-base B]
//!                      [--port-stride S] [--standby N] [--standby-host HOST] [--host HOST]
//! standby_check recording-log CLUSTER_DIR
//! ```
//!
//! - `states`: the standby's module state at each members' snapshot, which it
//!   recorded in its archive (`cluster_standby --record-module-states`,
//!   listed in `DIR/standbyN/module-states`), against every member's own
//!   module snapshot there, the one `DIR/nodeI/cluster`'s recording log
//!   lists at that log position. Both are replayed from their archives,
//!   placed by the layout ([`rusteron_cluster_tools::layout`]), to `--host`
//!   through a client on the driver in `--aeron-dir`, and read back with
//!   [`ModuleState::read`]. The states must be equal, and so must their begin
//!   and end markers, field by field: log position, term, where the term
//!   began, time unit and app version.
//! - `recording-log`: the directory's recording log, an entry a line, as
//!   Aeron's `ClusterTool recording-log` prints it: `type=TERM`,
//!   `type=SNAPSHOT` or `type=STANDBY_SNAPSHOT`, then its fields.
//!
//! The states are read with the standby's own code: a fault that the reader
//! shares with the standby's writer passes here. The members' snapshots are
//! the members' own, though, written by their consensus module.
//!
//! Exits 0 when every state is the same, 1 on a difference, a member with no
//! snapshot there or no state recorded, 2 on a bad command line.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use rusteron_archive::*;
use rusteron_cluster_codecs::cluster::snapshot_marker_codec::SnapshotMarkerDecoder;
use rusteron_cluster_core::log::{Fragment, PollAction, Reassembler};
use rusteron_cluster_core::recording_log::{
    self, ENTRY_TYPE_SNAPSHOT, ENTRY_TYPE_STANDBY_SNAPSHOT, ENTRY_TYPE_TERM, Entry, RecordingLog, SERVICE_ID,
};
use rusteron_cluster_standby::state::ModuleState;
use rusteron_cluster_tools::args::Args;
use rusteron_cluster_tools::layout::{ARCHIVE_CONTROL, Layout};
use rusteron_consensus_core::codec;

/// Where replays come to: a port a slot from here.
const REPLAY_PORT: usize = 24_800;
const REPLAY_STREAM: i32 = 9_003;
/// How long a snapshot's replay may take to reach its end marker.
const PATIENCE: Duration = Duration::from_secs(10);

fn usage(why: &str) -> ExitCode {
    eprintln!(
        "standby_check: {why}; usage: standby_check states --aeron-dir DIR [--dir DIR] [--hosts H0,H1,...] \
         [--port-base B] [--port-stride S] [--standby N] [--standby-host HOST] [--host HOST] | \
         standby_check recording-log CLUSTER_DIR"
    );
    ExitCode::from(2)
}

/// Where `states` finds the nodes.
struct Nodes {
    layout: Layout,
    standby: usize,
    standby_host: String,
    /// This client's host, where replays come to.
    host: String,
}

impl Nodes {
    fn parse(args: &Args) -> Result<Self, String> {
        // `--data` as well as the layout's `--dir`, for the scripts that named it so.
        let mut layout = Layout::parse(args)?;
        if let Some(data) = args.value("--data") {
            layout.dir = PathBuf::from(data);
        }
        Ok(Self {
            layout,
            standby: args.number("--standby", 0)?,
            standby_host: args.value("--standby-host").unwrap_or("localhost").to_string(),
            host: args.value("--host").unwrap_or("localhost").to_string(),
        })
    }

    /// Slot `slot`'s archive control endpoint.
    fn archive(&self, slot: usize) -> String {
        let host = self.layout.hosts.get(slot).unwrap_or(&self.standby_host);
        self.layout.endpoint(host, slot, ARCHIVE_CONTROL)
    }
}

fn main() -> ExitCode {
    let args = Args::from_env();
    match args.as_slice().first().map(String::as_str) {
        Some("states") => {
            let Some(aeron_dir) = args.value("--aeron-dir") else {
                return usage("--aeron-dir DIR is required");
            };
            let nodes = match Nodes::parse(&args) {
                Ok(nodes) => nodes,
                Err(e) => return usage(&e),
            };
            match states(aeron_dir, &nodes) {
                Ok(true) => ExitCode::SUCCESS,
                Ok(false) => ExitCode::from(1),
                Err(e) => {
                    println!("FAILED: {e}");
                    ExitCode::from(1)
                }
            }
        }
        Some("recording-log") => {
            let Some(dir) = args.as_slice().get(1) else {
                return usage("recording-log CLUSTER_DIR");
            };
            match entries(Path::new(dir)) {
                Ok(entries) => {
                    for e in entries {
                        println!(
                            "type={} recordingId={} leadershipTermId={} termBaseLogPosition={} logPosition={} \
                             timestamp={} serviceId={} isValid={}",
                            type_name(e.entry_type),
                            e.recording_id,
                            e.leadership_term_id,
                            e.term_base_log_position,
                            e.log_position,
                            e.timestamp,
                            e.service_id,
                            e.is_valid
                        );
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("standby_check: {e}");
                    ExitCode::from(1)
                }
            }
        }
        _ => usage("states or recording-log"),
    }
}

fn type_name(entry_type: i32) -> &'static str {
    match entry_type & !recording_log::ENTRY_TYPE_INVALID_FLAG {
        ENTRY_TYPE_TERM => "TERM",
        ENTRY_TYPE_SNAPSHOT => "SNAPSHOT",
        ENTRY_TYPE_STANDBY_SNAPSHOT => "STANDBY_SNAPSHOT",
        _ => "UNKNOWN",
    }
}

fn entries(cluster_dir: &Path) -> Result<Vec<Entry>, String> {
    let path = cluster_dir.join(recording_log::FILE_NAME);
    let bytes = std::fs::read(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(RecordingLog::load(&bytes)
        .map_err(|e| format!("{}: {e}", path.display()))?
        .entries()
        .to_vec())
}

/// Every state the standby recorded against every member's: true if all are the same.
fn states(aeron_dir: &str, nodes: &Nodes) -> Result<bool, String> {
    let listed = nodes.layout.standby_dir(nodes.standby).join("module-states");
    let standby_slot = nodes.layout.standby_slot(nodes.standby);
    let lines = std::fs::read_to_string(&listed).map_err(|e| format!("{}: {e}", listed.display()))?;
    let aeron = AeronContext::new()
        .and_then(|context| {
            context.set_dir(&aeron_dir.into_c_string())?;
            let aeron = Aeron::new(&context)?;
            aeron.start()?;
            Ok(aeron)
        })
        .map_err(|e| format!("a client on {aeron_dir}: {e}"))?;
    let mut failed = false;
    let mut checked = 0;
    for line in lines.lines().filter(|l| !l.trim().is_empty()) {
        let fields: Vec<i64> = line.split_whitespace().filter_map(|f| f.parse().ok()).collect();
        let [position, standby_recording] = fields[..] else {
            return Err(format!("{}: not a position and a recording: {line}", listed.display()));
        };
        let standby = read(&aeron, nodes, standby_slot, standby_recording)?;
        let mut standby_state = None;
        for node in 0..nodes.layout.members() {
            let cluster_dir = nodes.layout.member_dir(node).join("cluster");
            let Some(entry) = entries(&cluster_dir)?.into_iter().find(|e| {
                e.entry_type == ENTRY_TYPE_SNAPSHOT
                    && e.is_valid
                    && e.service_id == SERVICE_ID
                    && e.log_position == position
            }) else {
                println!("node {node}: no module snapshot at {position}");
                failed = true;
                continue;
            };
            let member = read(&aeron, nodes, node, entry.recording_id)?;
            let state = |messages: &[Vec<u8>]| {
                ModuleState::read(
                    messages.iter().map(Vec::as_slice),
                    entry.log_position,
                    entry.leadership_term_id,
                    entry.term_base_log_position,
                )
            };
            let (member_state, theirs) = (state(&member)?, state(&standby)?);
            if member_state != theirs {
                println!("node {node} at {position} differs:\n  member  {member_state:?}\n  standby {theirs:?}");
                failed = true;
            }
            let (member_markers, standby_markers) = (markers(&member)?, markers(&standby)?);
            if member_markers != standby_markers {
                println!(
                    "node {node} at {position}: the markers differ:\n  member  {member_markers:?}\n  standby {standby_markers:?}"
                );
                failed = true;
            }
            standby_state = Some((theirs, standby_markers));
        }
        if let Some((state, markers)) = standby_state {
            checked += 1;
            println!(
                "at {position}: {} sessions, {} timers, next session id {}, term {}, {}",
                state.sessions.len(),
                state.timers.len(),
                state.next_session_id,
                state.leadership_term_id,
                markers.first().map_or("no marker", String::as_str)
            );
        }
    }
    failed |= checked == 0;
    println!(
        "{}: {checked} of the standby's module states against every member's snapshot",
        if failed { "FAILED" } else { "same" }
    );
    Ok(!failed)
}

/// The snapshot's begin and end markers, their fields as text.
fn markers(messages: &[Vec<u8>]) -> Result<Vec<String>, String> {
    let ends = [messages.first(), messages.last()];
    ends.into_iter()
        .map(|message| {
            let message = message.ok_or("a snapshot with no messages")?;
            let d = SnapshotMarkerDecoder::default().header(codec::header_decoder(message), 0);
            Ok(format!(
                "{:?} at {} in term {}, the term from {:?}, {:?}, app version {:?}",
                d.mark(),
                d.log_position(),
                d.leadership_term_id(),
                d.term_base_log_position(),
                d.time_unit(),
                d.app_version()
            ))
        })
        .collect()
}

/// What went wrong reading slot `slot`'s recording `recording_id`.
fn failed<E: std::fmt::Debug>(slot: usize, recording_id: i64) -> impl Fn(E) -> String {
    move |e| format!("slot {slot}'s recording {recording_id}: {e:?}")
}

/// Slot `slot`'s recording `recording_id`, a module snapshot, replayed from its archive to its end marker.
fn read(aeron: &Aeron, nodes: &Nodes, slot: usize, recording_id: i64) -> Result<Vec<Vec<u8>>, String> {
    let host = &nodes.host;
    let context = AeronArchiveContext::new().map_err(failed(slot, recording_id))?;
    context.set_aeron(aeron).map_err(failed(slot, recording_id))?;
    let control = format!("aeron:udp?endpoint={}", nodes.archive(slot));
    context
        .set_control_request_channel(&control.into_c_string())
        .map_err(failed(slot, recording_id))?;
    context
        .set_control_response_channel(&format!("aeron:udp?endpoint={host}:0").into_c_string())
        .map_err(failed(slot, recording_id))?;
    let archive = AeronArchiveAsyncConnect::new_with_aeron(&context, aeron)
        .map_err(failed(slot, recording_id))?
        .poll_blocking(PATIENCE)
        .map_err(failed(slot, recording_id))?;
    let channel = format!("aeron:udp?endpoint={host}:{}", REPLAY_PORT + slot);
    let params = AeronArchiveReplayParams::builder()
        .position(0)
        .build()
        .map_err(failed(slot, recording_id))?;
    let session = archive
        .start_replay(recording_id, &channel.clone().into_c_string(), REPLAY_STREAM, &params)
        .map_err(failed(slot, recording_id))? as i32;
    let replay = ChannelUri::add_session_id(&channel, session);
    let subscription = aeron
        .async_add_subscription(&replay.into_c_string(), REPLAY_STREAM, Handlers::NONE, Handlers::NONE)
        .map_err(failed(slot, recording_id))?
        .poll_blocking(PATIENCE)
        .map_err(failed(slot, recording_id))?;
    let mut messages: Vec<Vec<u8>> = Vec::new();
    let mut reassembler = Reassembler::default();
    let deadline = Instant::now() + PATIENCE;
    let ended = |messages: &[Vec<u8>]| ModuleState::read(messages.iter().map(Vec::as_slice), 0, 0, 0).is_ok();
    loop {
        let polled = subscription
            .poll_fn(
                |buffer: &[u8], header: &AeronHeader| {
                    reassembler.on_fragment(fragment(buffer, header), |message, _| {
                        messages.push(message.to_vec());
                        PollAction::Continue
                    });
                },
                256,
            )
            .map_err(failed(slot, recording_id))?;
        if polled > 0 && ended(&messages) {
            let _ = archive.stop_replay(i64::from(session));
            return Ok(messages);
        }
        if polled <= 0 {
            if Instant::now() > deadline {
                let _ = archive.stop_replay(i64::from(session));
                return Err(format!(
                    "slot {slot}'s recording {recording_id}: no end marker within {} s, {} messages",
                    PATIENCE.as_secs(),
                    messages.len()
                ));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

fn fragment<'a>(buffer: &'a [u8], header: &AeronHeader) -> Fragment<'a> {
    let (flags, term_offset) = header
        .get_values()
        .map(|v| (v.frame().flags(), v.frame().term_offset()))
        .unwrap_or((0, -1));
    Fragment {
        payload: buffer,
        flags,
        term_offset,
        position: header.position(),
    }
}
