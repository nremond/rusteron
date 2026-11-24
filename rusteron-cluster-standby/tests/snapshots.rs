//! What a standby deletes of its snapshots: its own sets past the newest
//! few, listed no more first, the
//! members' sets its backup copied before the newest, and at its start what
//! nothing lists.

use std::io::Write;
use std::path::PathBuf;

use rusteron_cluster_core::recording_log::{ENTRY_TYPE_SNAPSHOT, ENTRY_TYPE_TERM, Entry};
use rusteron_cluster_standby::snapshots::{Recording, Snapshot, superseded_snapshots, unlisted};

fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn snapshot(i: i64) -> Snapshot {
    Snapshot {
        log_position: i * 1_000,
        leadership_term_id: 1,
        term_base_log_position: 0,
        timestamp: i,
        service_recording_id: 2 * i,
        module_recording_id: 2 * i + 1,
    }
}

#[test]
fn the_list_keeps_its_newest_snapshots() {
    let dir = dir("standby-keep");
    let file = dir.join("standby-snapshots");
    let listed: Vec<Snapshot> = (1..=5).map(snapshot).collect();
    for s in &listed {
        s.append(&file).unwrap();
    }
    assert_eq!(Snapshot::keep_newest(&file, 0).unwrap(), vec![], "0 keeps all");
    assert_eq!(Snapshot::all(&file).unwrap(), listed);

    assert_eq!(
        Snapshot::keep_newest(&file, 2).unwrap(),
        listed[..3],
        "the three oldest dropped"
    );
    assert_eq!(Snapshot::all(&file).unwrap(), listed[3..], "the two newest listed");
    assert_eq!(Snapshot::latest(&file).unwrap(), Some(listed[4]));
    assert_eq!(Snapshot::keep_newest(&file, 2).unwrap(), vec![], "nothing more to drop");
    assert!(
        !dir.join("standby-snapshots.next").exists(),
        "the new list moved over the old"
    );

    // Appended to after, and a line a crash cut short passed over.
    let sixth = snapshot(6);
    sixth.append(&file).unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&file)
        .unwrap()
        .write_all(b"7000 1 0")
        .unwrap();
    assert_eq!(Snapshot::all(&file).unwrap(), vec![listed[3], listed[4], sixth]);
    assert_eq!(Snapshot::keep_newest(&file, 2).unwrap(), vec![listed[3]]);
    assert_eq!(
        Snapshot::all(&file).unwrap(),
        vec![listed[4], sixth],
        "the cut line gone too"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

fn entry(recording_id: i64, log_position: i64, service_id: i32, entry_type: i32) -> Entry {
    Entry {
        recording_id,
        leadership_term_id: 0,
        term_base_log_position: 0,
        log_position,
        timestamp: 0,
        service_id,
        entry_type,
        is_valid: true,
        entry_index: 0,
        archive_endpoint: None,
        position: 0,
    }
}

#[test]
fn the_backups_copies_before_its_newest_are_superseded() {
    let term = entry(0, -1, -1, ENTRY_TYPE_TERM);
    assert_eq!(
        superseded_snapshots(std::slice::from_ref(&term)),
        Vec::<i64>::new(),
        "none copied"
    );
    let mut entries = vec![
        term,
        entry(1, 100, 0, ENTRY_TYPE_SNAPSHOT),
        entry(2, 100, -1, ENTRY_TYPE_SNAPSHOT),
    ];
    assert_eq!(superseded_snapshots(&entries), Vec::<i64>::new(), "one set: the newest");
    entries.extend([
        entry(5, 300, 0, ENTRY_TYPE_SNAPSHOT),
        entry(6, 300, -1, ENTRY_TYPE_SNAPSHOT),
        entry(3, 200, 0, ENTRY_TYPE_SNAPSHOT),
        entry(4, 200, -1, ENTRY_TYPE_SNAPSHOT),
        Entry {
            is_valid: false,
            ..entry(7, 400, 0, ENTRY_TYPE_SNAPSHOT)
        },
    ]);
    let mut superseded = superseded_snapshots(&entries);
    superseded.sort();
    assert_eq!(
        superseded,
        vec![1, 2, 3, 4],
        "both sets before the newest valid one, at 300"
    );
}

fn stopped(recording_id: i64, stream_id: i32) -> Recording {
    Recording {
        recording_id,
        stream_id,
        stop_position: 64,
    }
}

#[test]
fn the_snapshot_recordings_listed_nowhere_are_found_at_the_start() {
    let (service, module) = (106, 107);
    let sets = [snapshot(5), snapshot(6)];
    let recordings = [
        Recording {
            recording_id: 0,
            stream_id: 100,
            stop_position: 5_000,
        },
        stopped(10, service),
        stopped(11, module),
        stopped(12, service),
        stopped(13, module),
        // A set written, the standby killed before it listed it.
        stopped(14, service),
        stopped(15, module),
        Recording {
            recording_id: 16,
            stream_id: service,
            stop_position: -1,
        },
        // The backup's copies: the members' sets at 100 and 300, one it gave up, and one its log lists invalid.
        stopped(20, service),
        stopped(21, module),
        stopped(22, service),
        stopped(23, module),
        stopped(24, service),
        stopped(25, service),
        // A module state a test recorded, listed in its own file.
        stopped(26, module),
        Recording {
            recording_id: 30,
            stream_id: 200,
            stop_position: 640,
        },
    ];
    let backup_log = [
        entry(0, -1, -1, ENTRY_TYPE_TERM),
        entry(20, 100, 0, ENTRY_TYPE_SNAPSHOT),
        entry(21, 100, -1, ENTRY_TYPE_SNAPSHOT),
        entry(22, 300, 0, ENTRY_TYPE_SNAPSHOT),
        entry(23, 300, -1, ENTRY_TYPE_SNAPSHOT),
        Entry {
            is_valid: false,
            ..entry(25, 400, 0, ENTRY_TYPE_SNAPSHOT)
        },
    ];
    assert_eq!(
        unlisted(&recordings, &sets, &backup_log, &[26], [service, module]),
        vec![14, 15, 20, 21, 24, 25],
        "the set never listed, the backup's superseded copy, the one it gave up and the one listed invalid"
    );
    assert_eq!(
        unlisted(&recordings, &[], &[], &[], [service, module]),
        vec![10, 11, 12, 13, 14, 15, 20, 21, 22, 23, 24, 25, 26],
        "nothing listed: every snapshot recording stopped"
    );
    assert_eq!(
        unlisted(&[], &sets, &[], &[], [service, module]),
        Vec::<i64>::new(),
        "an empty archive"
    );
}
