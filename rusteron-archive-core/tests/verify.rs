//! `verify_recording` against Aeron 1.53.3's own `ArchiveTool verify`
//! (`java/io/aeron/archive/CatalogFixtures.java`, run by `fixtures.sh`): on
//! the recordings in `verify/before/`, each damage the tool reports in the
//! tool's words, and each whole recording's stop where the tool puts it.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use rusteron_archive_core::catalog::{Catalog, RecordingState};
use rusteron_archive_core::segments::parse_segment_file_name;
use rusteron_archive_core::verify::{Verdict, verify_recording};

const SEGMENT: i64 = 64 * 1024;

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/verify")
}

/// Java's verdicts, by recording id: name, state after, stop and stop timestamp after, error ("-" for none).
fn javas() -> BTreeMap<i64, Vec<String>> {
    fs::read_to_string(fixtures().join("cases.txt"))
        .unwrap()
        .lines()
        .map(|l| {
            let f: Vec<String> = l.split('\t').map(String::from).collect();
            (f[0].parse().unwrap(), f[1..].to_vec())
        })
        .collect()
}

#[test]
fn every_recording_is_found_as_the_tool_finds_it() {
    let before = fixtures().join("before");
    let catalog = Catalog::open(fs::read(before.join("archive.catalog")).unwrap(), 4_096).unwrap();
    let mut last_segments: BTreeMap<i64, i64> = BTreeMap::new();
    for entry in fs::read_dir(&before).unwrap() {
        if let Some((id, base)) = entry.unwrap().file_name().to_str().and_then(parse_segment_file_name) {
            let last = last_segments.entry(id).or_insert(base);
            *last = (*last).max(base);
        }
    }
    let java = javas();
    let mut checked = 0;
    for (state, d) in catalog.entries() {
        let [name, java_state, java_stop, _java_stop_timestamp, java_error] = &java[&d.recording_id][..] else {
            panic!("{}: not in Java's cases", d.recording_id);
        };
        if state == RecordingState::Deleted as i32 {
            assert_eq!(java_state, "DELETED", "{name}: the tool skips a deleted recording");
            continue;
        }
        let bytes;
        let last = match last_segments.get(&d.recording_id) {
            Some(&base) => {
                bytes = fs::read(before.join(format!("{}-{base}.rec", d.recording_id))).unwrap();
                Some((base, bytes.as_slice()))
            }
            None => None,
        };
        let verdict = verify_recording(&d, last);
        match (name.as_str(), verdict) {
            // The departure: the archive's recovery reads an unsized file as holding no frame, the tool fails to
            // read a header in it and marks the whole recording invalid.
            ("empty-last-segment", Verdict::Whole { stop_position }) => {
                assert_eq!(java_error, "failed to read fragment header");
                assert_eq!(stop_position, SEGMENT, "{name}: stopped where the empty file starts");
            }
            // Torn: the tool, told to, truncated it before its last fragment and found the rest whole.
            ("torn-active", Verdict::Torn { fragment_offset }) => {
                assert_eq!(java_error, "-");
                assert_eq!(
                    java_stop,
                    &(last.unwrap().0 + fragment_offset as i64).to_string(),
                    "{name}"
                );
            }
            ("empty-last-segment" | "torn-active", verdict) => panic!("{name}: {verdict:?}"),
            (_, Verdict::Damaged(message)) => {
                assert_eq!(&message, java_error, "{name}");
                assert_eq!(java_state, "INVALID", "{name}");
            }
            (_, Verdict::Whole { stop_position }) => {
                assert_eq!(java_error, "-", "{name}: the tool found it damaged");
                assert_eq!(&stop_position.to_string(), java_stop, "{name}");
            }
            (_, verdict) => panic!("{name}: {verdict:?}"),
        }
        checked += 1;
    }
    assert_eq!(checked, java.len() - 1, "every recording but the deleted one");
    let damaged = java.values().filter(|f| f[4] != "-").count();
    assert_eq!(
        damaged, 9,
        "the tool's eight kinds of damage, and the empty file it takes for one"
    );
}
