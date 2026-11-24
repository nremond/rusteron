//! A consensus module's files and counters against Aeron 1.54.2's own code
//! (`java/io/aeron/cluster/ConsensusFixtures.java`, run by `fixtures.sh`):
//! the recording log Java's operations make, the recovery plans Java makes of
//! real recordings, the node state file, the counters' keys and labels.

use rusteron_cluster::cluster_counters::{self, RecoveryState};
use rusteron_cluster::node_state_file::{self, NodeState};
use rusteron_cluster::recording_log::{RecordingExtent, RecordingLog};

fn fixtures() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn read(path: &str) -> Vec<u8> {
    let path = fixtures().join(path);
    std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn text(path: &str) -> String {
    String::from_utf8(read(path)).expect("ASCII")
}

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

/// The file a log's queued writes make of `file`.
fn apply(file: &mut Vec<u8>, writes: Vec<(u64, Vec<u8>)>) {
    for (offset, bytes) in writes {
        let end = offset as usize + bytes.len();
        if file.len() < end {
            file.resize(end, 0);
        }
        file[offset as usize..end].copy_from_slice(&bytes);
    }
}

/// What Java's fixture printed of a log: its next entry index, then its
/// entries in order.
fn printed(log: &RecordingLog) -> Vec<String> {
    let mut lines = vec![format!("nextEntryIndex={}", log.next_entry_index())];
    lines.extend(log.entries().iter().map(ToString::to_string));
    lines
}

fn n(field: &str) -> i64 {
    field.parse().expect("a number")
}

#[test]
fn the_recording_log_is_written_as_java_writes_it() {
    let mut log = RecordingLog::new();
    let mut file = Vec::new();
    for line in text("recording_log/ops.txt").lines() {
        let f: Vec<&str> = line.split('\t').collect();
        match f[0] {
            "appendTerm" => log.append_term(n(f[1]), n(f[2]), n(f[3]), n(f[4])).unwrap(),
            "appendSnapshot" => log
                .append_snapshot(n(f[1]), n(f[2]), n(f[3]), n(f[4]), n(f[5]), n(f[6]) as i32)
                .unwrap(),
            "appendStandbySnapshot" => log
                .append_standby_snapshot(n(f[1]), n(f[2]), n(f[3]), n(f[4]), n(f[5]), n(f[6]) as i32, f[7])
                .unwrap(),
            "invalidateLatestSnapshot" => {
                assert_eq!(log.invalidate_latest_snapshot().unwrap().to_string(), f[1])
            }
            "ensureCoherent" => log
                .ensure_coherent(n(f[1]), n(f[2]), n(f[3]), n(f[4]), n(f[5]), n(f[6]), 1, n(f[7]))
                .unwrap(),
            "commitLogPosition" => log.commit_log_position(n(f[1]), n(f[2])).unwrap(),
            other => panic!("unknown operation {other}"),
        }
        apply(&mut file, log.take_writes());
    }
    assert_eq!(file, read("recording_log/recording.log"));
    let expected: Vec<String> = text("recording_log/entries.txt").lines().map(String::from).collect();
    assert_eq!(printed(&log), expected);

    let loaded = RecordingLog::load(&file).unwrap();
    assert_eq!(printed(&loaded), expected);
    assert_eq!(loaded.find_last_term_recording_id(), 10);
    assert_eq!(loaded.get_latest_snapshot(1).unwrap().recording_id, 18);
}

#[test]
fn a_log_with_a_standby_snapshot_and_a_removed_entry_reads_as_java_reads_it() {
    let log = RecordingLog::load(&read("recording_log/removed.log")).unwrap();
    let expected: Vec<String> = text("recording_log/removed.txt").lines().map(String::from).collect();
    assert_eq!(printed(&log), expected);
}

#[test]
fn recovery_plans_are_javas() {
    let extent = text("plans/extent.txt");
    let f: Vec<&str> = extent.trim().split('\t').collect();
    let recording_id = n(f[0]);
    let extent = RecordingExtent {
        start_position: n(f[1]),
        stop_position: n(f[2]),
        initial_term_id: n(f[3]) as i32,
        term_buffer_length: n(f[4]) as i32,
        mtu_length: n(f[5]) as i32,
        session_id: n(f[6]) as i32,
    };
    let plans = text("plans/plans.txt");
    assert_eq!(plans.lines().count(), 6);
    for line in plans.lines() {
        let f: Vec<&str> = line.split('\t').collect();
        let (name, replicated, expected) = (f[0], n(f[1]), f[2]);
        let log = RecordingLog::load(&read(&format!("plans/{name}.log"))).unwrap();
        let listed = log.plan_recording_id(replicated);
        let extent = listed.map(|id| {
            assert_eq!(id, recording_id, "{name}");
            extent
        });
        if name == "empty" {
            assert_eq!(listed, None);
        }
        let plan = log.create_recovery_plan(2, replicated, extent).unwrap();
        assert_eq!(plan.to_string(), expected, "{name}");
    }
}

#[test]
fn the_node_state_file_is_javas() {
    let new = text("node_state/new.hex");
    let mut lines = new.lines();
    let expected = hex(lines.next().unwrap());
    assert_eq!(
        lines.next().unwrap().parse::<usize>().unwrap(),
        node_state_file::FILE_LENGTH
    );

    let (mut state, bytes) = NodeState::create();
    assert_eq!(bytes, expected);
    let mut file = bytes;
    file.resize(node_state_file::FILE_LENGTH, 0);
    assert_eq!(NodeState::load(&file).unwrap(), state);

    for (term, position, timestamp) in [(5, 8_192, 1_700_000_000_000), (4, 0, 0)] {
        if let (_, Some((offset, bytes))) = state.propose_max_candidate_term_id(term, position, timestamp) {
            file[offset as usize..offset as usize + bytes.len()].copy_from_slice(&bytes);
        }
    }
    assert_eq!(file[..64], hex(text("node_state/proposed.hex").trim())[..]);
    assert_eq!(NodeState::load(&file).unwrap().candidate_term_id, 5);
}

#[test]
fn the_counters_are_javas() {
    let counters_text = text("counters.txt");
    let lines: Vec<Vec<&str>> = counters_text.lines().map(|l| l.split('\t').collect()).collect();
    let line = |type_id: i32, key: &[u8], label: &str| {
        vec![
            type_id.to_string(),
            key.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            label.to_string(),
        ]
    };

    let mut expected = Vec::new();
    let (key, label) = cluster_counters::error_counter(3);
    expected.push(line(cluster_counters::ERROR_COUNT_TYPE_ID, &key, &label));
    for (type_id, name) in cluster_counters::consensus_module_counters(100_000_000, 1_000_000_000) {
        let (key, label) = cluster_counters::cluster_counter(&name, 3);
        expected.push(line(type_id, &key, &label));
    }
    let recovery = RecoveryState {
        leadership_term_id: 2,
        log_position: 4_096,
        timestamp: 1_700_000_000_000,
        cluster_id: 3,
        snapshot_recording_ids: vec![41, 42],
    };
    expected.push(line(
        cluster_counters::RECOVERY_STATE_TYPE_ID,
        &recovery.to_key(),
        &cluster_counters::recovery_state_label(2, 4_096, 3),
    ));

    let actual: Vec<Vec<String>> = lines
        .iter()
        .map(|l| l.iter().map(|s| s.to_string()).collect())
        .collect();
    assert_eq!(actual, expected);
}
