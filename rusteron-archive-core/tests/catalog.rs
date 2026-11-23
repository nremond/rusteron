//! The catalog and the segments against Aeron 1.54.2's own `Catalog`
//! (`java/io/aeron/archive/CatalogFixtures.java`, run by `fixtures.sh`): the
//! operations Java applied, replayed, write Java's file byte for byte; Java's
//! file reads back as Java wrote it; and the stop position recovered from
//! each set of segment files is Java's.

use std::fs;
use std::path::PathBuf;

use rusteron_archive_core::catalog::{Catalog, CatalogError, Descriptor, RecordingState};
use rusteron_archive_core::segments::{
    BlockWrite, Recovery, SegmentWriter, compute_stop_position, parse_segment_file_name, segment_file_base_position,
    segment_file_name, truncate_torn_fragment,
};

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn ops() -> Vec<Vec<String>> {
    fs::read_to_string(fixtures().join("catalog/ops.txt"))
        .unwrap()
        .lines()
        .map(|l| l.split('\t').map(str::to_owned).collect())
        .collect()
}

/// A descriptor's fields from `op[1..]`: an add's, or a replace's after its id.
fn descriptor(op: &[String]) -> Descriptor {
    let n = |i: usize| op[i].parse::<i64>().unwrap();
    Descriptor {
        control_session_id: 0,
        correlation_id: 0,
        recording_id: -1,
        start_position: n(1),
        stop_position: n(2),
        start_timestamp: n(3),
        stop_timestamp: n(4),
        initial_term_id: n(5) as i32,
        segment_file_length: n(6) as i32,
        term_buffer_length: n(7) as i32,
        mtu_length: n(8) as i32,
        session_id: n(9) as i32,
        stream_id: n(10) as i32,
        stripped_channel: op[11].clone(),
        original_channel: op[12].clone(),
        source_identity: op[13].clone(),
    }
}

/// Java's operations on a new catalog of 4 KiB.
fn replay() -> (Catalog, Vec<Descriptor>) {
    let mut catalog = Catalog::create(4_096).unwrap();
    let mut added = Vec::new();
    for op in ops() {
        let n = |i: usize| op[i].parse::<i64>().unwrap();
        match op[0].as_str() {
            "add" => {
                let d = descriptor(&op);
                let id = catalog.add_recording(&d).unwrap();
                added.push(Descriptor { recording_id: id, ..d });
            }
            "stop" => catalog.recording_stopped(n(1), n(2), n(3)).unwrap(),
            "extend" => catalog.extend_recording(n(1), n(2), n(3), n(4) as i32).unwrap(),
            "stop_position" => catalog.set_stop_position(n(1), n(2)).unwrap(),
            "start_position" => catalog.set_start_position(n(1), n(2)).unwrap(),
            "state" => assert!(catalog.change_state(n(1), RecordingState::Deleted)),
            "replace" => {
                let d = descriptor(&op[1..]);
                catalog.replace_recording(n(1), &d).unwrap();
            }
            other => panic!("unknown operation {other}"),
        }
    }
    (catalog, added)
}

#[test]
fn the_operations_write_javas_file() {
    let java = fs::read(fixtures().join("catalog/archive.catalog")).unwrap();
    let (mut catalog, _) = replay();
    assert!(java.len() > 4_096, "Java's catalog grew");
    assert_eq!(catalog.len(), java.len(), "grown alike");

    // What the shell would write, on a file of zeros, is Java's file.
    let mut file = vec![0u8; catalog.len()];
    for (offset, bytes) in catalog.take_writes() {
        file[offset as usize..offset as usize + bytes.len()].copy_from_slice(&bytes);
    }
    let first_difference = file.iter().zip(&java).position(|(a, b)| a != b);
    assert_eq!(first_difference, None, "the files differ");
    assert!(catalog.take_writes().is_empty(), "nothing left to write");
}

#[test]
fn javas_file_reads_as_java_wrote_it() {
    let java = fs::read(fixtures().join("catalog/archive.catalog")).unwrap();
    let catalog = Catalog::open(java, 1 << 20).unwrap();
    let (replayed, added) = replay();
    assert_eq!(catalog.next_recording_id(), 13);
    assert_eq!(
        catalog.recording_ids().collect::<Vec<_>>(),
        [0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12]
    );
    for id in catalog.recording_ids() {
        assert_eq!(
            catalog.descriptor(id).unwrap(),
            replayed.descriptor(id).unwrap(),
            "recording {id}"
        );
        assert_eq!(
            catalog.descriptor_bytes(id).unwrap(),
            replayed.descriptor_bytes(id).unwrap()
        );
    }
    let d = catalog.descriptor(0).unwrap();
    assert_eq!((d.stop_position, d.stop_timestamp), (65_536, 1_700_000_001_000));
    let d = catalog.descriptor(1).unwrap();
    assert_eq!((d.control_session_id, d.correlation_id, d.session_id), (77, 88, -99));
    assert_eq!((d.stop_position, d.stop_timestamp), (-1, -1));
    assert_eq!(catalog.stop_position(2).unwrap(), 8_192, "replaced");
    assert_eq!(catalog.descriptor(2).unwrap().source_identity, "1.2.3.4:5");
    assert_eq!(catalog.descriptor(5).unwrap().stripped_channel, "aeron:ipc");
    assert_eq!(catalog.start_position(3).unwrap(), 131_072);
    assert_eq!(catalog.descriptor(4), Err(CatalogError::UnknownRecording(4)), "deleted");
    assert_eq!(
        catalog.descriptor(7).unwrap(),
        Descriptor { ..added[7].clone() },
        "moved along"
    );
    // Recordings 1 (extended), 6, 8 and 10 have no stop position.
    let unstopped: Vec<i64> = catalog.unstopped().iter().map(|d| d.recording_id).collect();
    assert_eq!(unstopped, [1, 6, 8, 10]);
}

#[test]
fn a_catalog_of_another_major_version_is_refused() {
    let mut java = fs::read(fixtures().join("catalog/archive.catalog")).unwrap();
    java[0..4].copy_from_slice(&0x04_00_00i32.to_le_bytes());
    assert_eq!(
        Catalog::open(java, 4_096).err(),
        Some(CatalogError::Version(0x04_00_00))
    );
}

#[test]
fn a_next_recording_id_behind_the_entries_is_refused() {
    let mut java = fs::read(fixtures().join("catalog/archive.catalog")).unwrap();
    java[8..16].copy_from_slice(&5i64.to_le_bytes());
    assert_eq!(
        Catalog::open(java, 4_096).err(),
        Some(CatalogError::NextRecordingId {
            expected: 13,
            actual: 5
        })
    );
}

/// A case where Java 1.54 reports a page straddle with no fragment there: no
/// frame read from a page-aligned offset past the segment's start. Its
/// `fragmentStraddlesPageBoundary(offset, 0)` is true at a page boundary and
/// `isValidFragment` (1.54.0) calls a zero-length fragment invalid, so it
/// asks to truncate; it stops where it would have, and "truncating" leaves
/// the file as it was. This archive finds no straddle: one would keep it
/// from starting without `--truncate-torn-fragments`, as it keeps Java's
/// (aeron-io/aeron#2261).
const JAVA_SPURIOUS_STRADDLE: &str = "empty-segment";

#[test]
fn the_recovered_stop_positions_are_javas() {
    let cases = fs::read_to_string(fixtures().join("recovery/cases.txt")).unwrap();
    for line in cases.lines() {
        let f: Vec<&str> = line.split('\t').collect();
        let (name, start, term) = (f[0], f[1].parse::<i64>().unwrap(), f[2].parse::<i32>().unwrap());
        let (expected, straddled) = (f[5].parse::<i64>().unwrap(), f[6] == "1");
        let bytes;
        let segment = match f[3] {
            "-" => None,
            file => {
                assert_eq!(parse_segment_file_name(file), Some((42, f[4].parse().unwrap())));
                bytes = fs::read(fixtures().join("recovery").join(name).join(file)).unwrap();
                Some((f[4].parse::<i64>().unwrap(), bytes.as_slice()))
            }
        };
        match compute_stop_position(start, term, 64 * 1024, segment) {
            Ok(stop) => {
                assert!(
                    !straddled || name == JAVA_SPURIOUS_STRADDLE,
                    "{name}: Java found the last fragment half written"
                );
                assert_eq!(stop, expected, "{name}");
            }
            Err(Recovery::PageStraddle { fragment_offset }) => {
                assert!(straddled, "{name}: Java found no straddle");
                // Java, asked not to truncate, recovers past the fragment, which the archive refuses to start on.
                let base = segment.map_or(0, |(b, _)| b);
                assert!(base + fragment_offset as i64 <= expected, "{name}");
            }
        }
    }
}

#[test]
fn a_torn_fragment_is_truncated_as_java_truncates_it() {
    let cases = fs::read_to_string(fixtures().join("recovery/cases.txt")).unwrap();
    let mut truncated = 0;
    for line in cases.lines() {
        let f: Vec<&str> = line.split('\t').collect();
        let (name, start, term, file) = (f[0], f[1].parse::<i64>().unwrap(), f[2].parse::<i32>().unwrap(), f[3]);
        if f[6] != "1" {
            continue;
        }
        let base = f[4].parse::<i64>().unwrap();
        let segment = 64 * 1024;
        let dir = fixtures().join("recovery").join(name);
        let bytes = fs::read(dir.join(file)).unwrap();
        if name == JAVA_SPURIOUS_STRADDLE {
            // Java's truncation of nothing: the file as it was, the stop where it was.
            assert_eq!(bytes, fs::read(dir.join("truncated").join(file)).unwrap(), "{name}");
            assert_eq!(f[5], f[7], "{name}");
            continue;
        }
        let Err(Recovery::PageStraddle { fragment_offset }) =
            compute_stop_position(start, term, segment, Some((base, bytes.as_slice())))
        else {
            panic!("{name}: no straddle found");
        };

        let copy = std::env::temp_dir().join(format!("rusteron-truncate-{}-{name}.rec", std::process::id()));
        fs::write(&copy, &bytes).unwrap();
        let out = fs::OpenOptions::new().write(true).open(&copy).unwrap();
        truncate_torn_fragment(&out, fragment_offset, segment as usize).unwrap();
        let after = fs::read(&copy).unwrap();
        fs::remove_file(&copy).unwrap();

        assert_eq!(after, fs::read(dir.join("truncated").join(file)).unwrap(), "{name}");
        assert_eq!(
            (base + fragment_offset as i64).max(start),
            f[7].parse::<i64>().unwrap(),
            "{name}"
        );
        truncated += 1;
    }
    // The half-written straddle, and a padding frame last, which Java takes for one too (its body never written).
    assert_eq!(truncated, 2);
}

/// A kill between the recorder creating a segment file and sizing it
/// leaves the file empty: the recording stops where the file starts, the
/// segment before it full. Java's `ArchiveTool verify` fails to read a header
/// there and marks the whole recording invalid, so a Java node is best off
/// sizing such a file before it runs the tool.
#[test]
fn a_segment_file_left_empty_stops_the_recording_at_its_start() {
    let (term, segment) = (64 * 1024, 128 * 1024);
    assert_eq!(
        compute_stop_position(0, term, segment, Some((i64::from(segment), &[]))),
        Ok(i64::from(segment))
    );
}

#[test]
fn segments_count_from_the_start_term_base() {
    let (term, segment) = (64 * 1024, 128 * 1024);
    let start = 3 * 64 * 1024 + 1_024;
    assert_eq!(segment_file_base_position(start, start, term, segment), 3 * 64 * 1024);
    assert_eq!(
        segment_file_base_position(start, start + 128 * 1024, term, segment),
        3 * 64 * 1024 + 128 * 1024
    );
    assert_eq!(segment_file_name(7, 196_608), "7-196608.rec");
    assert_eq!(parse_segment_file_name("7-196608.rec"), Some((7, 196_608)));
    assert_eq!(parse_segment_file_name("archive.catalog"), None);

    // A recording joined mid-term writes from the join's offset; padding is a header; full, the next file.
    let mut w = SegmentWriter::new(7, start, start, term, 64 * 1024);
    assert_eq!((w.file_name(), w.position()), ("7-196608.rec".to_string(), start));
    assert_eq!(
        w.on_block(4_096, false),
        BlockWrite {
            file_offset: 1_024,
            length: 4_096,
            roll_over: false
        }
    );
    let rest = 64 * 1024 - 1_024 - 4_096;
    assert_eq!(
        w.on_block(rest, true),
        BlockWrite {
            file_offset: 5_120,
            length: 32,
            roll_over: true
        }
    );
    assert_eq!(
        (w.file_name(), w.position()),
        ("7-262144.rec".to_string(), 4 * 64 * 1024)
    );
}
