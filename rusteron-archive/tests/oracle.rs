//! What the archive makes of a client's channels, and the counters it
//! allocates, against Aeron 1.54.2's own code (`java/io/aeron/archive/ConductorFixtures.java`,
//! run by `fixtures.sh`).

use std::fs;
use std::path::PathBuf;

use rusteron_archive::archive_counters;
use rusteron_archive::channel_uri::{
    ChannelUri, ControlStream, ReplayStream, recording_key, replay_channel, response_channel, stripped_channel,
};

fn fixture(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// Java's outcome as written: a value, or `!` and the error's message.
fn outcome(r: Result<String, String>) -> String {
    r.unwrap_or_else(|e| format!("!{e}"))
}

#[test]
fn channels_are_stripped_keyed_answered_and_replayed_as_by_java() {
    let control = ControlStream {
        mtu_length: 1_408,
        term_length: 64 * 1024,
        sparse: true,
    };
    let channels = fixture("channels.txt");
    let mut checked = 0;
    for line in channels.lines() {
        let f: Vec<&str> = line.split('\t').collect();
        let channel = f[0];
        let parsed = ChannelUri::parse(channel);
        assert_eq!(
            outcome(parsed.clone().and_then(|u| stripped_channel(&u))),
            f[1],
            "stripped {channel}"
        );
        assert_eq!(
            outcome(parsed.clone().map(|u| recording_key(1001, &u))),
            f[2],
            "key {channel}"
        );
        assert_eq!(
            outcome(response_channel(channel, control, 4242)),
            f[3],
            "response {channel}"
        );
        assert_eq!(outcome(parsed.map(|u| u.is_udp().to_string())), f[4], "udp {channel}");
        let stream = ReplayStream {
            initial_term_id: 7,
            term_length: 64 * 1024,
            mtu_length: 1_408,
        };
        assert_eq!(
            outcome(replay_channel(channel, 131_104, stream, 5_000_000_000)),
            f[5],
            "replay {channel}"
        );
        checked += 1;
    }
    assert!(checked >= 30, "{checked} channels");
}

#[test]
fn counters_are_keyed_and_labelled_as_by_java() {
    let long_channel = format!("aeron:udp?endpoint=localhost:20121|alias={}", "x".repeat(400));
    let long_source = format!("127.0.0.1:{}", "9".repeat(120));
    let expected = [
        (101, archive_counters::error_counter(42)),
        (102, archive_counters::archive_counter("Archive Control Sessions", 42)),
        (
            103,
            archive_counters::archive_counter("archive-conductor max cycle time in ns: SHARED", -7),
        ),
        (
            100,
            archive_counters::recording_position(42, 0, 77, 1001, "aeron:ipc?alias=log", "aeron:ipc"),
        ),
        (
            100,
            archive_counters::recording_position(i64::MIN, 12_345_678_901, -5, 7, &long_channel, &long_source),
        ),
        (
            113,
            archive_counters::control_session(
                42,
                7000,
                "name=test version=1.54.2 commit=481d4271ad",
                "aeron:ipc",
                -321,
            ),
        ),
    ];
    let java = fixture("counters.txt");
    let lines: Vec<&str> = java.lines().collect();
    assert_eq!(lines.len(), expected.len());
    for (line, (type_id, (key, label))) in lines.iter().zip(expected) {
        let f: Vec<&str> = line.split('\t').collect();
        let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            (f[0].parse::<i32>().unwrap(), f[1], f[2]),
            (type_id, hex.as_str(), label.as_str())
        );
    }
}
