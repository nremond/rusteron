//! The conductor as Java's `ArchiveConductor` behaves, driven through the
//! testkit's fake shell: a client connects, starts, extends, stops and lists
//! recordings; sessions end the ways Java's do.

use rusteron_archive::archive_conductor::{
    Action, Config, EraseError, ExpectedFrame, ImageInfo, Offer, RecordingError, ReplayLimit, RequestImage,
};
use rusteron_archive::archive_counters;
use rusteron_archive::catalog::{Catalog, Descriptor};
use rusteron_archive::control_session_adapter::{Code, Request, Signal};
use rusteron_archive::replay_session::{ReplayError, ReplayParams};
use rusteron_archive::segments::SegmentWriter;
use rusteron_archive::testkit::{FakeShell, FakeSource, Response, archive_proxy};

const ARCHIVE_ID: i64 = 42;
const FIRST_SESSION: i64 = 7_000;
const RESPONSE_CHANNEL: &str = "aeron:udp?endpoint=localhost:20121";
const LOG: &str = "aeron:ipc?alias=log|term-length=64k";
const LOG_STRIPPED: &str = "aeron:ipc?alias=log";
const SEGMENT: i32 = 128 * 1024 * 1024;
const TERM: i32 = 64 * 1024;

fn config() -> Config {
    Config {
        archive_id: ARCHIVE_ID,
        first_control_session_id: FIRST_SESSION,
        ..Config::default()
    }
}

fn client_image() -> RequestImage {
    RequestImage {
        correlation_id: 55,
        session_id: -321,
        source_identity: "aeron:ipc".into(),
    }
}

fn image(correlation_id: i64, session_id: i32, join_position: i64) -> ImageInfo {
    ImageInfo {
        correlation_id,
        session_id,
        source_identity: "aeron:ipc".into(),
        initial_term_id: 5,
        term_buffer_length: TERM,
        mtu_length: 1_408,
        join_position,
    }
}

fn ok(control_session_id: i64, correlation_id: i64, relevant_id: i64) -> Response {
    Response::Control {
        control_session_id,
        correlation_id,
        relevant_id,
        code: Code::OK,
        version: 0x01_0C_00,
        message: String::new(),
    }
}

fn error(control_session_id: i64, correlation_id: i64, relevant_id: i64, message: &str) -> Response {
    Response::Control {
        control_session_id,
        correlation_id,
        relevant_id,
        code: Code::ERROR,
        version: 0x01_0C_00,
        message: message.into(),
    }
}

fn unknown(control_session_id: i64, correlation_id: i64, relevant_id: i64) -> Response {
    Response::Control {
        control_session_id,
        correlation_id,
        relevant_id,
        code: Code::RECORDING_UNKNOWN,
        version: 0x01_0C_00,
        message: String::new(),
    }
}

fn signal(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
    subscription_id: i64,
    position: i64,
    signal: Signal,
) -> Response {
    Response::Signal {
        control_session_id,
        correlation_id,
        recording_id,
        subscription_id,
        position,
        signal,
    }
}

/// Connects a client and sends its first request, as Aeron's client does:
/// the session is then active.
fn connect(shell: &mut FakeShell) -> i64 {
    let image = client_image();
    shell.request(
        &image,
        &archive_proxy::auth_connect(1, 20, archive_proxy::VERSION, RESPONSE_CHANNEL, "name=test"),
    );
    for _ in 0..3 {
        shell.tick(1);
    }
    let Some(Response::Control { control_session_id, .. }) =
        shell.channels.sent.last().map(|(_, m)| Response::decode(m))
    else {
        panic!("no connect response");
    };
    shell.request(&image, &archive_proxy::archive_id(control_session_id, 2));
    shell.tick(1);
    assert_eq!(
        shell.responses(control_session_id).last(),
        Some(&ok(control_session_id, 2, ARCHIVE_ID))
    );
    control_session_id
}

/// Starts recording `channel` on stream 1001 and its first image (session 77
/// from position `join`): the recording's id and the subscription's.
fn record(shell: &mut FakeShell, cs: i64, correlation_id: i64, channel: &str, join: i64) -> (i64, i64) {
    shell.request(
        &client_image(),
        &archive_proxy::start_recording2(cs, correlation_id, 1001, false, false, channel),
    );
    shell.tick(1);
    let Some(Response::Control { relevant_id, .. }) = shell.responses(cs).pop() else {
        panic!("no response");
    };
    let token = *shell
        .subscriptions
        .iter()
        .find(|(_, s)| s.registration_id == relevant_id)
        .unwrap()
        .0;
    let before = shell.conductor.catalog().next_recording_id();
    shell.image(token, image(9, 77, join));
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [signal(cs, correlation_id, before, relevant_id, join, Signal::START)]
    );
    (before, relevant_id)
}

#[test]
fn a_client_connects_as_to_java() {
    let mut shell = FakeShell::new(config());
    let image = client_image();
    shell.request(
        &image,
        &archive_proxy::auth_connect(
            1,
            20,
            archive_proxy::VERSION,
            RESPONSE_CHANNEL,
            "name=test version=1.54.2",
        ),
    );
    let (key, label) =
        archive_counters::control_session(ARCHIVE_ID, FIRST_SESSION, "name=test version=1.54.2", "aeron:ipc", -321);
    assert_eq!(
        label,
        "control-session: name=test version=1.54.2 sourceIdentity=aeron:ipc sessionId=-321 - archiveId=42"
    );
    assert_eq!(
        shell.actions,
        [
            Action::AddControlSessionCounter {
                control_session_id: FIRST_SESSION,
                key,
                label,
            },
            Action::AddResponsePublication {
                control_session_id: FIRST_SESSION,
                channel: "aeron:udp?endpoint=localhost:20121|mtu=1408|term-length=64k|sparse=true".into(),
                stream_id: 20,
            },
        ]
    );
    // One state a cycle: connecting, connected, answered.
    for _ in 0..3 {
        shell.tick(1);
    }
    let cs = FIRST_SESSION;
    assert_eq!(shell.responses(cs), [ok(cs, 1, cs)]);

    // Answered again every 200 ms until the client's first request.
    shell.tick(200);
    assert_eq!(shell.responses(cs), []);
    shell.tick(1);
    assert_eq!(shell.responses(cs), [ok(cs, 1, cs)]);

    shell.request(&image, &archive_proxy::archive_id(cs, 2));
    shell.request(&image, &archive_proxy::keep_alive(cs, -1));
    shell.tick(1);
    assert_eq!(shell.responses(cs), [ok(cs, 2, ARCHIVE_ID)]);
    shell.tick(300);
    assert_eq!(shell.responses(cs), [], "active: no more connect responses");

    // Pinged every second once active.
    shell.tick(1_000);
    assert_eq!(shell.responses(cs), [Response::Ping { control_session_id: cs }]);
    assert_eq!(shell.conductor.control_session_count(), 1);
    assert!(shell.errors.is_empty(), "{:?}", shell.errors);
}

#[test]
fn a_request_before_the_connect_is_answered_is_dropped() {
    let mut shell = FakeShell::new(config());
    let image = client_image();
    shell.request(
        &image,
        &archive_proxy::auth_connect(1, 20, archive_proxy::VERSION, RESPONSE_CHANNEL, ""),
    );
    shell.request(&image, &archive_proxy::archive_id(FIRST_SESSION, 2));
    for _ in 0..3 {
        shell.tick(1);
    }
    assert_eq!(shell.responses(FIRST_SESSION), [ok(FIRST_SESSION, 1, FIRST_SESSION)]);
}

#[test]
fn a_client_of_another_major_version_is_refused_until_it_times_out() {
    let mut shell = FakeShell::new(config());
    let image = client_image();
    shell.request(
        &image,
        &archive_proxy::auth_connect(1, 20, 0x02_00_01, RESPONSE_CHANNEL, ""),
    );
    for _ in 0..3 {
        shell.tick(1);
    }
    let cs = FIRST_SESSION;
    let refusal = error(cs, 1, cs, "invalid client version 2.0.1, archive is 1.12.0");
    assert_eq!(shell.responses(cs), std::slice::from_ref(&refusal));
    shell.request(&image, &archive_proxy::archive_id(cs, 2));
    shell.tick(201);
    assert_eq!(shell.responses(cs), [refusal], "refused again, the request dropped");

    shell.tick(5_000);
    assert_eq!(shell.conductor.control_session_count(), 0);
    let reason = "failed to establish initial connection: state=CONNECTED";
    assert!(shell.actions.contains(&Action::RejectImage {
        image_correlation_id: 55,
        reason: reason.into(),
    }));
    assert_eq!(
        shell.errors,
        [format!(
            "WARN - controlSessionId={cs} (controlResponseStreamId=20 controlResponseChannel=\
             aeron:udp?endpoint=localhost:20121|mtu=1408|term-length=64k|sparse=true) terminated: {reason}"
        )]
    );
}

#[test]
fn requests_for_another_session_or_from_another_image_are_dropped() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    shell.request(&client_image(), &archive_proxy::archive_id(cs + 1, 3));
    let stranger = RequestImage {
        correlation_id: 56,
        ..client_image()
    };
    shell.request(&stranger, &archive_proxy::archive_id(cs, 4));
    shell.request(&stranger, &archive_proxy::close_session(cs));
    shell.tick(1);
    assert_eq!(shell.responses(cs), []);
    assert_eq!(shell.conductor.control_session_count(), 1);
    assert_eq!(
        shell.errors,
        [
            format!(
                "WARN - control request for unknown session: controlSessionId={} templateId=68",
                cs + 1
            ),
            format!("WARN - unauthorised archive action=68 controlSessionId={cs} source=aeron:ipc"),
        ]
    );
}

#[test]
fn a_recording_starts_records_and_stops() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    shell.request(
        &client_image(),
        &archive_proxy::start_recording2(cs, 10, 1001, false, false, LOG),
    );
    shell.tick(1);
    assert_eq!(shell.responses(cs), [ok(cs, 10, 1_000)]);
    let token = shell.subscription(LOG_STRIPPED);
    assert_eq!(shell.subscriptions[&token].stream_id, 1001);

    shell.image(token, image(9, 77, 0));
    let (key, label) = archive_counters::recording_position(ARCHIVE_ID, 0, 77, 1001, LOG_STRIPPED, "aeron:ipc");
    assert_eq!(label, "rec-pos: 0 77 1001 aeron:ipc?alias=log - archiveId=42");
    let recorder = &shell.recorders[&0];
    assert_eq!(recorder.writer, SegmentWriter::new(0, 0, 0, TERM, SEGMENT));
    assert_eq!((&recorder.counter_key, &recorder.counter_label), (&key, &label));
    assert!(shell.actions.iter().any(|a| matches!(
        a,
        Action::StartRecorder {
            recording_id: 0,
            block_length_limit: 65_536,
            ..
        }
    )));
    assert_eq!(
        shell.conductor.catalog().descriptor(0).unwrap(),
        Descriptor {
            control_session_id: 0,
            correlation_id: 0,
            recording_id: 0,
            start_timestamp: shell.now_ms,
            stop_timestamp: -1,
            start_position: 0,
            stop_position: -1,
            initial_term_id: 5,
            segment_file_length: SEGMENT,
            term_buffer_length: TERM,
            mtu_length: 1_408,
            session_id: 77,
            stream_id: 1001,
            stripped_channel: LOG_STRIPPED.into(),
            original_channel: LOG.into(),
            source_identity: "aeron:ipc".into(),
        }
    );
    shell.tick(1);
    assert_eq!(shell.responses(cs), [signal(cs, 10, 0, 1_000, 0, Signal::START)]);

    assert_eq!(shell.record(0, &[(1_024, false), (96, false)]), 1_120);
    shell.request(&client_image(), &archive_proxy::recording_position(cs, 11, 0));
    shell.request(&client_image(), &archive_proxy::stop_position(cs, 12, 0));
    shell.request(&client_image(), &archive_proxy::start_position(cs, 13, 0));
    shell.request(&client_image(), &archive_proxy::recording_position(cs, 14, 5));
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            ok(cs, 11, 1_120),
            ok(cs, 12, -1),
            ok(cs, 13, 0),
            error(cs, 14, 5, "unknown recording id: 5")
        ]
    );

    let stopped_at = shell.now_ms;
    shell.request(&client_image(), &archive_proxy::stop_recording(cs, 20, 1001, LOG));
    shell.request(&client_image(), &archive_proxy::stop_position(cs, 21, 0));
    shell.request(&client_image(), &archive_proxy::recording_position(cs, 22, 0));
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            ok(cs, 20, 0),
            signal(cs, 10, 0, 1_000, 1_120, Signal::STOP),
            ok(cs, 21, 1_120),
            ok(cs, 22, -1)
        ]
    );
    let d = shell.conductor.catalog().descriptor(0).unwrap();
    assert_eq!((d.stop_position, d.stop_timestamp), (1_120, stopped_at));
    assert!(
        shell.subscriptions.is_empty(),
        "the subscription closed with its recording"
    );
    assert_eq!(shell.conductor.recording_session_count(), 0);

    shell.request(&client_image(), &archive_proxy::stop_recording(cs, 23, 1001, LOG));
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [error(
            cs,
            23,
            4,
            &format!("no recording found for streamId=1001 channel={LOG}")
        )]
    );
}

#[test]
fn a_stream_is_recorded_once_and_a_local_udp_one_through_a_spy() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    let udp = "aeron:udp?endpoint=localhost:40123|alias=a|term-length=128k";
    shell.request(
        &client_image(),
        &archive_proxy::start_recording2(cs, 10, 1, true, false, udp),
    );
    // Another alias, the same endpoint and stream: the same key.
    let again = "aeron:udp?endpoint=localhost:40123|alias=b";
    shell.request(
        &client_image(),
        &archive_proxy::start_recording2(cs, 11, 1, true, false, again),
    );
    let remote = "aeron:udp?endpoint=localhost:40124|session-id=9|tags=1,2";
    shell.request(
        &client_image(),
        &archive_proxy::start_recording(cs, 12, 1, false, remote),
    );
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            ok(cs, 10, 1_000),
            error(cs, 11, 3, &format!("recording exists for streamId=1 channel={again}")),
            ok(cs, 12, 1_001)
        ]
    );
    let channels: Vec<&str> = shell.subscriptions.values().map(|s| s.channel.as_str()).collect();
    assert_eq!(
        channels,
        [
            "aeron-spy:aeron:udp?endpoint=localhost:40123|alias=a",
            "aeron:udp?tags=1,2|endpoint=localhost:40124|session-id=9"
        ]
    );

    shell.request(
        &client_image(),
        &archive_proxy::start_recording(cs, 13, 1, false, "aeron:tcp?x=y"),
    );
    shell.request(
        &client_image(),
        &archive_proxy::start_recording(cs, 14, 1, false, "udp?x=y"),
    );
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            error(cs, 13, 0, "invalid media: tcp"),
            error(cs, 14, 0, "Aeron URIs must start with 'aeron:', found: udp?x=y")
        ]
    );
}

#[test]
fn without_auto_stop_a_subscription_records_each_image_until_stopped() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    let (first, subscription_id) = record(&mut shell, cs, 10, LOG, 0);
    let token = shell.subscription(LOG_STRIPPED);
    shell.record(first, &[(4_096, false)]);
    shell.end(first, None);
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [signal(cs, 10, first, subscription_id, 4_096, Signal::STOP)]
    );
    assert!(shell.subscriptions.contains_key(&token), "still subscribed");

    shell.image(token, image(10, 78, 8_192));
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [signal(cs, 10, 1, subscription_id, 8_192, Signal::START)]
    );
    shell.request(
        &client_image(),
        &archive_proxy::stop_recording_subscription(cs, 11, subscription_id),
    );
    shell.request(
        &client_image(),
        &archive_proxy::stop_recording_subscription(cs, 12, subscription_id),
    );
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            ok(cs, 11, 0),
            signal(cs, 10, 1, subscription_id, 8_192, Signal::STOP),
            error(
                cs,
                12,
                4,
                &format!("no recording subscription found for subscriptionId={subscription_id}")
            )
        ]
    );
    assert!(shell.subscriptions.is_empty());
}

#[test]
fn with_auto_stop_the_first_recording_to_stop_closes_its_subscription() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    shell.request(
        &client_image(),
        &archive_proxy::start_recording2(cs, 10, 1001, false, true, LOG),
    );
    shell.tick(1);
    shell.responses(cs);
    let token = shell.subscription(LOG_STRIPPED);
    shell.image(token, image(9, 77, 0));
    shell.image(token, image(10, 78, 0));
    shell.end(0, None);
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            signal(cs, 10, 0, 1_000, 0, Signal::START),
            signal(cs, 10, 1, 1_000, 0, Signal::START),
            signal(cs, 10, 0, 1_000, 0, Signal::STOP),
            signal(cs, 10, 1, 1_000, 0, Signal::STOP)
        ]
    );
    assert!(shell.subscriptions.is_empty() && shell.recorders.is_empty());
}

#[test]
fn a_recording_that_fails_reports_its_error_then_stops() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    let (id, subscription_id) = record(&mut shell, cs, 10, LOG, 0);
    let message = "ERROR - java.io.IOException - No space left on device";
    shell.end(
        id,
        Some(RecordingError {
            code: 11,
            message: message.into(),
        }),
    );
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            error(cs, 10, 11, message),
            signal(cs, 10, id, subscription_id, 0, Signal::STOP)
        ]
    );
}

#[test]
fn an_extension_continues_a_stopped_recording_from_where_it_stopped() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    let (id, _) = record(&mut shell, cs, 10, LOG, 0);
    shell.record(id, &[(4_096, false)]);
    shell.request(&client_image(), &archive_proxy::stop_recording(cs, 11, 1001, LOG));
    shell.tick(1);
    shell.responses(cs);

    // Another key: an IPC channel's alias is not part of it, its tags are.
    let other = "aeron:ipc?tags=9|alias=other";
    shell.request(
        &client_image(),
        &archive_proxy::extend_recording2(cs, 20, 9, 1001, false, false, LOG),
    );
    shell.request(
        &client_image(),
        &archive_proxy::extend_recording2(cs, 21, id, 2, false, false, LOG),
    );
    shell.request(
        &client_image(),
        &archive_proxy::extend_recording2(cs, 22, id, 1001, false, false, LOG),
    );
    shell.request(
        &client_image(),
        &archive_proxy::extend_recording2(cs, 23, id, 1001, false, false, other),
    );
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            error(cs, 20, 5, "unknown recording id: 9"),
            error(
                cs,
                21,
                5,
                "cannot extend recording 0 with streamId=2 for existing streamId=1001"
            ),
            ok(cs, 22, 1_001),
            ok(cs, 23, 1_002)
        ]
    );
    let token = shell.subscription(LOG_STRIPPED);
    shell.image(token, image(12, 88, 4_096));
    assert_eq!(
        shell.recorders[&id].writer,
        SegmentWriter::new(id, 0, 4_096, TERM, SEGMENT)
    );
    let d = shell.conductor.catalog().descriptor(id).unwrap();
    assert_eq!(
        (
            d.control_session_id,
            d.correlation_id,
            d.session_id,
            d.stop_position,
            d.stop_timestamp
        ),
        (cs, 22, 88, -1, -1)
    );
    // The second extension's image comes while the first records.
    let other_token = shell.subscription(other);
    shell.image(other_token, image(13, 89, 4_096));
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            signal(cs, 22, id, 1_001, 4_096, Signal::EXTEND),
            error(
                cs,
                23,
                2,
                &format!("cannot extend active recording 0 streamId=1001 channel={other}")
            )
        ]
    );
    assert!(shell.subscriptions.contains_key(&other_token), "kept: no auto stop");

    shell.record(id, &[(4_096, false)]);
    shell.request(&client_image(), &archive_proxy::stop_recording(cs, 24, 1001, LOG));
    shell.request(&client_image(), &archive_proxy::stop_recording(cs, 25, 1001, other));
    shell.request(
        &client_image(),
        &archive_proxy::extend_recording2(cs, 26, id, 1001, false, true, LOG),
    );
    shell.tick(1);
    shell.responses(cs);
    let token = shell.subscription(LOG_STRIPPED);
    shell.image(token, image(14, 90, 0));
    shell.tick(1);
    let message = "cannot extend recording 0 image.joinPosition=0 != rec.stopPosition=8192";
    assert_eq!(shell.responses(cs), [error(cs, 26, 9, message)]);
    assert_eq!(shell.errors.last().unwrap(), &format!("WARN - {message}"));
    assert!(shell.subscriptions.is_empty(), "auto stop: closed on the failure");
}

#[test]
fn a_recording_stops_by_its_identity() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    let (id, subscription_id) = record(&mut shell, cs, 10, LOG, 0);
    shell.request(&client_image(), &archive_proxy::stop_recording_by_identity(cs, 11, id));
    shell.request(&client_image(), &archive_proxy::stop_recording_by_identity(cs, 12, id));
    shell.request(&client_image(), &archive_proxy::stop_recording_by_identity(cs, 13, 3));
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            ok(cs, 11, 1),
            signal(cs, 10, id, subscription_id, 0, Signal::STOP),
            ok(cs, 12, 0),
            error(cs, 13, 5, "unknown recording id: 3")
        ]
    );
    assert!(shell.subscriptions.is_empty());
}

#[test]
fn listings_send_the_catalogs_descriptors() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    for (i, channel) in [
        LOG,
        "aeron:ipc?alias=other",
        "aeron:ipc?alias=log-2",
        "aeron:ipc?alias=log|tags=3",
    ]
    .into_iter()
    .enumerate()
    {
        let (id, _) = record(&mut shell, cs, 100 + i as i64, channel, 0);
        shell.request(
            &client_image(),
            &archive_proxy::stop_recording_by_identity(cs, 200 + i as i64, id),
        );
        shell.tick(1);
        shell.responses(cs);
    }
    let catalog_descriptor = |shell: &FakeShell, id: i64| Descriptor {
        control_session_id: cs,
        correlation_id: 30,
        ..shell.conductor.catalog().descriptor(id).unwrap()
    };
    let listed = |shell: &FakeShell, correlation_id: i64, id: i64| Response::Descriptor {
        control_session_id: cs,
        correlation_id,
        descriptor: Descriptor {
            correlation_id,
            ..catalog_descriptor(shell, id)
        },
    };

    shell.request(
        &client_image(),
        &archive_proxy::list_recordings_for_uri(cs, 30, 0, 10, 1001, "alias=log"),
    );
    shell.tick(1);
    let expected = [
        listed(&shell, 30, 0),
        listed(&shell, 30, 2),
        listed(&shell, 30, 3),
        unknown(cs, 30, 4),
    ];
    assert_eq!(shell.responses(cs), expected);

    shell.request(
        &client_image(),
        &archive_proxy::list_recordings_for_uri(cs, 31, 1, 1, 1001, "alias=log"),
    );
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [listed(&shell, 31, 2)],
        "the count reached: no end"
    );
    shell.request(
        &client_image(),
        &archive_proxy::list_recordings_for_uri(cs, 32, 10, 5, 1001, ""),
    );
    shell.tick(1);
    assert_eq!(shell.responses(cs), [unknown(cs, 32, 10)]);
    shell.request(
        &client_image(),
        &archive_proxy::list_recordings_for_uri(cs, 33, 0, 0, 1001, ""),
    );
    shell.tick(1);
    assert_eq!(shell.responses(cs), [], "nothing to list, nothing said");
    shell.request(
        &client_image(),
        &archive_proxy::list_recordings_for_uri(cs, 34, -5, 5, 7, ""),
    );
    shell.tick(1);
    assert_eq!(shell.responses(cs), [unknown(cs, 34, 4)]);

    shell.request(&client_image(), &archive_proxy::list_recording(cs, 35, 1));
    shell.tick(1);
    shell.request(&client_image(), &archive_proxy::list_recording(cs, 36, 99));
    shell.tick(1);
    assert_eq!(shell.responses(cs), [listed(&shell, 35, 1), unknown(cs, 36, 99)]);

    // One listing at a time.
    shell.channels.refuse.insert(cs, Offer::Retry);
    shell.request(&client_image(), &archive_proxy::list_recording(cs, 37, 1));
    shell.tick(1);
    shell.request(&client_image(), &archive_proxy::list_recording(cs, 38, 2));
    shell.channels.refuse.clear();
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            error(cs, 38, 1, "active listing already in progress"),
            listed(&shell, 37, 1)
        ]
    );

    // The descriptor's bytes are the catalog's, padding and all.
    shell.request(&client_image(), &archive_proxy::list_recording(cs, 39, 0));
    shell.tick(1);
    let (_, bytes) = shell.channels.sent.pop().unwrap();
    let catalog = shell.conductor.catalog().descriptor_bytes(0).unwrap();
    assert_eq!(bytes.len(), 8 + catalog.len());
    assert_eq!(&bytes[24..], &catalog[16..]);
    assert_eq!(bytes.len() % 32, 8, "padded as the catalog's entry");
}

#[test]
fn recordings_are_refused_past_the_limit_or_short_of_space() {
    let mut shell = FakeShell::new(Config {
        max_concurrent_recordings: 1,
        ..config()
    });
    let cs = connect(&mut shell);
    shell.request(
        &client_image(),
        &archive_proxy::start_recording2(cs, 10, 1001, false, false, LOG),
    );
    shell.request(
        &client_image(),
        &archive_proxy::start_recording2(cs, 11, 1002, false, false, LOG),
    );
    shell.request(
        &client_image(),
        &archive_proxy::extend_recording2(cs, 12, 0, 1002, false, false, LOG),
    );
    shell.request(&client_image(), &archive_proxy::stop_recording(cs, 13, 1001, LOG));
    shell.conductor.set_usable_space(100);
    shell.request(
        &client_image(),
        &archive_proxy::start_recording2(cs, 14, 1003, false, false, LOG),
    );
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            ok(cs, 10, 1_000),
            error(cs, 11, 8, "max concurrent recordings reached at 1"),
            error(cs, 12, 8, "max concurrent recordings reached at 1"),
            ok(cs, 13, 0),
            // The subscription stopped, none is left (Java 1.54; 1.53's count stayed up): the space is short.
            error(cs, 14, 11, "low storage threshold=134217728 <= usableSpace=100")
        ]
    );
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    shell.conductor.set_usable_space(100);
    shell.request(
        &client_image(),
        &archive_proxy::start_recording2(cs, 10, 1001, false, false, LOG),
    );
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [error(cs, 10, 11, "low storage threshold=134217728 <= usableSpace=100")]
    );
}

#[test]
fn an_image_past_the_limit_is_not_recorded() {
    // One subscription, so the request passes; its images then fill the recordings (Java 1.54 checks again as an
    // image arrives, where 1.53 counted the subscription once and every recording's end down).
    let mut shell = FakeShell::new(Config {
        max_concurrent_recordings: 2,
        ..config()
    });
    let cs = connect(&mut shell);
    let (first, subscription_id) = record(&mut shell, cs, 10, LOG, 0);
    let token = *shell
        .subscriptions
        .iter()
        .find(|(_, s)| s.registration_id == subscription_id)
        .unwrap()
        .0;
    shell.image(token, image(11, 78, 0));
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [signal(cs, 10, first + 1, subscription_id, 0, Signal::START)]
    );
    shell.image(token, image(12, 79, 0));
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [error(cs, 10, 8, "max concurrent recordings reached at 2")]
    );
    assert_eq!(shell.recorders.len(), 2);
    assert_eq!(
        shell.errors.last().map(String::as_str),
        Some("WARN - max concurrent recordings reached at 2")
    );
    // A recording ended: the next image is recorded.
    shell.end(first, None);
    shell.image(token, image(13, 80, 0));
    shell.tick(1);
    assert!(
        shell
            .responses(cs)
            .iter()
            .any(|r| *r == signal(cs, 10, first + 2, subscription_id, 0, Signal::START))
    );
}

#[test]
fn a_closed_session_leaves_its_recordings_running() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    let (id, _) = record(&mut shell, cs, 10, LOG, 0);
    shell.request(&client_image(), &archive_proxy::close_session(cs));
    shell.tick(1);
    assert_eq!(shell.conductor.control_session_count(), 0);
    assert!(
        shell
            .actions
            .contains(&Action::CloseControlSession { control_session_id: cs })
    );
    assert!(!shell.actions.iter().any(|a| matches!(a, Action::RejectImage { .. })));
    assert!(shell.recorders.contains_key(&id));
    shell.end(id, None);
    assert_eq!(shell.conductor.catalog().stop_position(id).unwrap(), 0);
    assert!(shell.errors.is_empty(), "{:?}", shell.errors);
}

#[test]
fn a_session_ends_with_its_client() {
    // Its request image gone.
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    shell
        .conductor
        .on_control_image_unavailable(&client_image(), 10, "aeron:ipc?term-length=64k");
    shell.tick(1);
    assert_eq!(shell.conductor.control_session_count(), 0);
    assert!(!shell.actions.iter().any(|a| matches!(a, Action::RejectImage { .. })));
    assert!(shell.errors.is_empty(), "{:?}", shell.errors);

    // Its response publication disconnected.
    let mut shell = FakeShell::new(config());
    let cs2 = connect(&mut shell);
    assert_eq!(cs, cs2);
    shell.channels.connected.clear();
    shell.tick(1);
    assert_eq!(shell.conductor.control_session_count(), 0);
    assert!(!shell.actions.iter().any(|a| matches!(a, Action::RejectImage { .. })));

    // Its responses refused for the connect timeout.
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    shell.channels.refuse.insert(cs, Offer::Retry);
    shell.request(&client_image(), &archive_proxy::archive_id(cs, 3));
    shell.tick(1);
    shell.tick(5_000);
    assert_eq!(shell.conductor.control_session_count(), 1);
    shell.tick(1);
    assert_eq!(shell.conductor.control_session_count(), 0);
    assert!(shell.actions.contains(&Action::RejectImage {
        image_correlation_id: 55,
        reason: "failed to send response for more than connectTimeoutMs=5000".into(),
    }));
}

#[test]
fn a_request_this_archive_does_not_serve_is_refused() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    // ListRecordingSubscriptions: its control session and correlation ids first, as every request's.
    let mut unsupported = archive_proxy::list_recordings(cs, 3, 0, 10);
    unsupported[2..4].copy_from_slice(&17u16.to_le_bytes());
    shell.request(&client_image(), &unsupported);
    shell.request(&client_image(), &[1, 2, 3]);
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [error(cs, 3, 0, "request not supported by this archive: templateId=17")]
    );
    assert_eq!(shell.errors, ["ERROR - message of 3 bytes is shorter than its header"]);
}

/// A recording of `blocks` blocks of 64 KiB on stream 1001, stopped: its id.
fn stopped_recording(shell: &mut FakeShell, cs: i64, correlation_id: i64, blocks: usize) -> i64 {
    let (id, _) = record(shell, cs, correlation_id, LOG, 0);
    shell.record(id, &vec![(64 * 1024, false); blocks]);
    shell.request(
        &client_image(),
        &archive_proxy::stop_recording_by_identity(cs, correlation_id + 1, id),
    );
    shell.tick(1);
    shell.responses(cs);
    id
}

fn replay_session(shell: &FakeShell) -> i64 {
    *shell.replays.keys().last().expect("a replay")
}

#[test]
fn a_replay_is_answered_once_its_segment_file_is_found() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    let id = stopped_recording(&mut shell, cs, 10, 2);
    shell.request(
        &client_image(),
        &archive_proxy::replay(cs, 30, id, -1, -1, 2001, -1, "aeron:ipc"),
    );
    assert!(shell.actions.contains(&Action::AddReplayPublication {
        publication: 2,
        channel: "aeron:ipc?mtu=1408|term-length=64k|init-term-id=5|term-id=5|term-offset=0|linger=5s".into(),
        stream_id: 2001,
    }));
    let replay = replay_session(&shell);
    assert_eq!(
        replay,
        (1 << 32) | 1_001,
        "the replay's id, its publication's session below"
    );
    let (session, limit) = &shell.replays[&replay];
    assert_eq!(*limit, ReplayLimit::None);
    assert_eq!(
        session.params(),
        &ReplayParams {
            replay_session_id: replay,
            recording_id: id,
            start_position: 0,
            replay_position: 0,
            stop_position: 128 * 1024,
            replay_length: 128 * 1024,
            term_length: TERM,
            segment_length: SEGMENT,
            initial_term_id: 5,
            stream_id: 1001,
            follows_counter: false,
            buffer_length: 1024 * 1024,
            connect_deadline_ms: shell.now_ms + 5_000,
        }
    );
    shell.tick(1);
    assert_eq!(shell.responses(cs), [], "not before its file is found");
    shell.replay_started(replay);
    shell.tick(1);
    assert_eq!(shell.responses(cs), [ok(cs, 30, replay)]);
    shell.replay_ended(replay, None);
    shell.tick(1);
    assert_eq!(shell.responses(cs), []);
    assert_eq!(shell.conductor.replay_count(), 0);
}

#[test]
fn replays_are_checked_in_javas_order() {
    let mut shell = FakeShell::new(Config {
        max_concurrent_replays: 1,
        ..config()
    });
    let cs = connect(&mut shell);
    let id = stopped_recording(&mut shell, cs, 10, 2);
    let empty = stopped_recording(&mut shell, cs, 20, 0);
    let image = client_image();
    shell.request(
        &image,
        &archive_proxy::replay(cs, 30, 99, -1, -1, 2001, -1, "aeron:ipc"),
    );
    shell.request(
        &image,
        &archive_proxy::replay(cs, 31, id, 100, -1, 2001, -1, "aeron:ipc"),
    );
    shell.request(
        &image,
        &archive_proxy::replay(cs, 32, id, 128 * 1024, -1, 2001, -1, "aeron:ipc"),
    );
    shell.request(
        &image,
        &archive_proxy::replay(cs, 33, id, 0, -1, 2001, 1_000, "aeron:ipc"),
    );
    shell.request(
        &image,
        &archive_proxy::replay(cs, 34, empty, -1, -2, 2001, -1, "aeron:ipc"),
    );
    shell.request(
        &image,
        &archive_proxy::replay(cs, 35, id, -1, -3, 2001, -1, "aeron:ipc"),
    );
    shell.request(
        &image,
        &archive_proxy::replay(cs, 36, id, -1, -1, 2001, -1, "aeron:tcp?x=y"),
    );
    shell.request(&image, &archive_proxy::replay(cs, 37, id, -1, -1, 2001, -1, "udp"));
    shell.request(
        &image,
        &archive_proxy::replay(cs, 38, id, -1, -1, 2001, -1, "aeron:ipc"),
    );
    shell.request(
        &image,
        &archive_proxy::replay(cs, 39, id, -1, -1, 2001, -1, "aeron:ipc"),
    );
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            error(cs, 30, 5, "unknown recording id: 99"),
            error(
                cs,
                31,
                0,
                "requested replay start position=100 is not a multiple of FRAME_ALIGNMENT (32) for recording 0"
            ),
            error(
                cs,
                32,
                16,
                "requested replay start position=131072 must be less than the limit position=131072 for recording 0"
            ),
            error(cs, 33, 0, "fileIoMaxLength=1000 < mtuLength=1408"),
            error(
                cs,
                34,
                15,
                "when replaying and stopping the replay length must be non-zero, recordingId=1"
            ),
            error(
                cs,
                35,
                0,
                "replay length must be positive: replayLength=-3, length=-3, stopPosition=131072, replayPosition=0 \
                 for recording 0"
            ),
            error(cs, 36, 0, "failed to process replayChannel - invalid media: tcp"),
            // 37: a channel that does not parse has no answer.
            error(cs, 39, 7, "max concurrent replays reached 1"),
        ]
    );
    assert!(
        shell
            .errors
            .contains(&"Aeron URIs must start with 'aeron:', found: udp".to_string())
    );
    assert_eq!(shell.replays.len(), 1, "38");
}

#[test]
fn a_bounded_replay_follows_its_counter_and_a_live_recordings_its_position() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    let id = stopped_recording(&mut shell, cs, 10, 2);
    shell.request(
        &client_image(),
        &archive_proxy::bounded_replay(cs, 30, id, 0, -1, 9, 2001, "aeron:ipc"),
    );
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [error(
            cs,
            30,
            0,
            "unable to create replay limit counter id= 9 because of: ERROR - Counter id is not allocated: 9"
        )]
    );
    shell.limit_counters.insert(9, 4_096);
    shell.request(
        &client_image(),
        &archive_proxy::bounded_replay(cs, 31, id, 0, -1, 9, 2001, "aeron:ipc"),
    );
    let (session, limit) = &shell.replays[&replay_session(&shell)];
    assert_eq!(*limit, ReplayLimit::Counter(9));
    let p = session.params();
    assert_eq!(
        (p.stop_position, p.replay_length, p.follows_counter),
        (4_096, i64::MAX, true)
    );

    let (live, _) = record(&mut shell, cs, 40, "aeron:ipc?alias=live", 0);
    shell.record(live, &[(1_024, false)]);
    shell.request(
        &client_image(),
        &archive_proxy::replay(cs, 41, live, -1, -1, 2001, -1, "aeron:ipc"),
    );
    let (session, limit) = &shell.replays[&replay_session(&shell)];
    assert_eq!(*limit, ReplayLimit::Recording(live));
    let p = session.params();
    assert_eq!((p.stop_position, p.follows_counter), (1_024, true));
}

#[test]
fn a_replay_that_fails_tells_its_client_why() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    let id = stopped_recording(&mut shell, cs, 10, 2);
    shell.request(
        &client_image(),
        &archive_proxy::replay(cs, 30, id, -1, -1, 2001, -1, "aeron:ipc"),
    );
    let replay = replay_session(&shell);
    shell.replay_started(replay);
    let message = format!("recording segment not found, recordingId=0, replaySessionId={replay}, segmentFile=0-0.rec");
    shell.replay_ended(
        replay,
        Some(ReplayError {
            code: 0,
            message: message.clone(),
        }),
    );
    shell.tick(1);
    assert_eq!(shell.responses(cs), [ok(cs, 30, replay), error(cs, 30, 0, &message)]);
    assert_eq!(shell.errors.last().unwrap(), &format!("ERROR - {message}"));
}

#[test]
fn replays_stop_on_request() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    let id = stopped_recording(&mut shell, cs, 10, 2);
    let replay = |shell: &mut FakeShell, correlation_id, stream_id, channel| {
        shell.request(
            &client_image(),
            &archive_proxy::replay(cs, correlation_id, id, -1, -1, stream_id, -1, channel),
        );
        replay_session(shell)
    };
    let first = replay(&mut shell, 30, 2001, "aeron:ipc");
    let second = replay(&mut shell, 31, 2002, "aeron:ipc?eos=false");
    let third = replay(&mut shell, 32, 2003, "aeron:ipc?eos=true");
    let fourth = replay(&mut shell, 33, 2004, "aeron:ipc?eos=false");
    shell.request(&client_image(), &archive_proxy::stop_replay(cs, 34, first));
    shell.request(&client_image(), &archive_proxy::stop_replay(cs, 35, 12_345));
    shell.request(&client_image(), &archive_proxy::stop_replay(cs, 36, second));
    assert_eq!(shell.replays.len(), 2);
    shell.request(&client_image(), &archive_proxy::stop_all_replays(cs, 37, -1));
    assert!(shell.replays.is_empty());
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [ok(cs, 34, 0), ok(cs, 35, 0), ok(cs, 36, 0), ok(cs, 37, 0)]
    );
    assert_eq!(shell.conductor.replay_count(), 0);
    // A client's stop revokes a replay's publication, its subscribers' images
    // dropped without their reading the rest, as Java's archive does; on a
    // channel with eos=false it closes it, the images left to their readers,
    // as aeron-io/aeron#2260 has Java's.
    let aborts: Vec<&Action> = shell
        .actions
        .iter()
        .filter(|a| matches!(a, Action::AbortReplay { .. }))
        .collect();
    let abort = |replay_session_id, revoke| Action::AbortReplay {
        replay_session_id,
        revoke,
    };
    assert_eq!(
        aborts,
        [
            &abort(first, true),
            &abort(second, false),
            &abort(third, true),
            &abort(fourth, false),
        ]
    );
}

/// Segments of two terms: a recording of four terms spans three files.
fn small_segments() -> Config {
    Config {
        segment_file_length: 2 * TERM,
        ..config()
    }
}

#[test]
fn a_truncation_cuts_its_segment_and_deletes_the_later_ones_one_a_cycle() {
    let mut shell = FakeShell::new(small_segments());
    let cs = connect(&mut shell);
    let id = stopped_recording(&mut shell, cs, 10, 4);
    for f in ["0-0.rec", "0-131072.rec", "0-262144.rec", "1-0.rec"] {
        shell.files.insert(f.into());
    }
    shell.request(
        &client_image(),
        &archive_proxy::replay(cs, 20, id, -1, -1, 2001, -1, "aeron:ipc"),
    );
    let replay = replay_session(&shell);
    let position = i64::from(TERM) + 4_096;
    shell.request(
        &client_image(),
        &archive_proxy::truncate_recording(cs, 30, id, position),
    );
    assert!(
        shell.actions.contains(&Action::AbortReplay {
            replay_session_id: replay,
            revoke: true
        }),
        "a replay of what is truncated is revoked"
    );
    assert!(shell.actions.contains(&Action::EraseSegment {
        recording_id: id,
        file_name: "0-0.rec".into(),
        segment_offset: position as u64,
        segment_length: 2 * TERM,
        expected: ExpectedFrame {
            term_offset: 4_096,
            term_id: 6,
            stream_id: 1001,
        },
    }));
    assert!(shell.replays.is_empty(), "its replays stopped");
    assert_eq!(shell.conductor.catalog().stop_position(id).unwrap(), position);
    // An extension now would reach what is being deleted.
    shell.request(
        &client_image(),
        &archive_proxy::extend_recording2(cs, 31, id, 1001, false, false, LOG),
    );
    shell.request(
        &client_image(),
        &archive_proxy::truncate_recording(cs, 32, id, position),
    );
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            ok(cs, 30, 2),
            error(
                cs,
                31,
                0,
                "cannot extend recording 0 due to an outstanding delete operation"
            ),
            error(cs, 32, 0, "another delete operation in progress for recording id: 0"),
        ]
    );
    assert_eq!(shell.files.len(), 4, "the first cycle waits for the replays");
    shell.tick(1);
    assert_eq!(shell.files.len(), 3);
    shell.tick(1);
    assert_eq!(shell.files.iter().collect::<Vec<_>>(), ["0-0.rec", "1-0.rec"]);
    shell.tick(1);
    assert_eq!(shell.responses(cs), [signal(cs, 30, id, -1, -1, Signal::DELETE)]);
}

#[test]
fn a_truncation_is_refused_or_fails_as_javas() {
    let mut shell = FakeShell::new(small_segments());
    let cs = connect(&mut shell);
    let id = stopped_recording(&mut shell, cs, 10, 4);
    let (live, _) = record(&mut shell, cs, 20, "aeron:ipc?alias=live", 0);
    shell.request(&client_image(), &archive_proxy::truncate_recording(cs, 30, live, 0));
    shell.request(&client_image(), &archive_proxy::truncate_recording(cs, 31, id, 100));
    shell.request(&client_image(), &archive_proxy::truncate_recording(cs, 32, 9, 0));
    shell.erase_result = Err(EraseError::NotAligned);
    shell.request(&client_image(), &archive_proxy::truncate_recording(cs, 33, id, 4_096));
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            error(cs, 30, 2, "cannot truncate active recording"),
            error(cs, 31, 16, "invalid position 100: start=0 stop=262144 alignment=32"),
            error(cs, 32, 5, "unknown recording id: 9"),
            error(cs, 33, 0, "4096 position not aligned to a data header"),
        ]
    );
    assert_eq!(
        shell.conductor.catalog().stop_position(id).unwrap(),
        4_096,
        "truncated in the catalog all the same"
    );

    // At its stop, in a segment: nothing to delete, said at once.
    shell.request(&client_image(), &archive_proxy::truncate_recording(cs, 34, id, 4_096));
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [ok(cs, 34, 0), signal(cs, 34, id, -1, -1, Signal::DELETE)]
    );
}

#[test]
fn a_truncation_to_the_start_and_a_purge_delete_every_file() {
    let mut shell = FakeShell::new(small_segments());
    let cs = connect(&mut shell);
    let id = stopped_recording(&mut shell, cs, 10, 4);
    for f in ["0-0.rec", "0-131072.rec.del", "0-262144.rec", "0-x.tmp"] {
        shell.files.insert(f.into());
    }
    shell.request(&client_image(), &archive_proxy::truncate_recording(cs, 30, id, 0));
    for _ in 0..5 {
        shell.tick(1);
    }
    assert_eq!(
        shell.responses(cs),
        [ok(cs, 30, 3), signal(cs, 30, id, -1, -1, Signal::DELETE)]
    );
    assert_eq!(shell.files.iter().collect::<Vec<_>>(), ["0-x.tmp"]);
    assert_eq!(shell.conductor.catalog().stop_position(id).unwrap(), 0);

    let other = stopped_recording(&mut shell, cs, 40, 1);
    let (live, _) = record(&mut shell, cs, 50, "aeron:ipc?alias=live", 0);
    shell.request(
        &client_image(),
        &archive_proxy::replay(cs, 60, other, -1, -1, 2001, -1, "aeron:ipc"),
    );
    shell.request(&client_image(), &archive_proxy::purge_recording(cs, 61, other));
    shell.request(&client_image(), &archive_proxy::purge_recording(cs, 62, live));
    shell.request(&client_image(), &archive_proxy::stop_all_replays(cs, 63, other));
    shell.files.insert(format!("{other}-0.rec"));
    shell.request(&client_image(), &archive_proxy::purge_recording(cs, 64, other));
    shell.request(&client_image(), &archive_proxy::list_recording(cs, 65, other));
    shell.tick(1);
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            error(cs, 61, 2, &format!("cannot purge recording with active replay {other}")),
            error(cs, 62, 2, &format!("cannot purge active recording {live}")),
            ok(cs, 63, 0),
            ok(cs, 64, 1),
            unknown(cs, 65, other),
            signal(cs, 64, other, -1, -1, Signal::DELETE),
        ]
    );
    assert!(!shell.conductor.catalog().has_recording(other));
    assert!(!shell.files.contains(&format!("{other}-0.rec")));
}

/// A source archive with a stopped recording (0, two terms of stream 1001)
/// and an active one (1).
fn source() -> FakeSource {
    let mut catalog = Catalog::create(4_096).unwrap();
    let recording = |stop_position, stop_timestamp| Descriptor {
        control_session_id: 0,
        correlation_id: 0,
        recording_id: -1,
        start_timestamp: 1_600_000_000_000,
        stop_timestamp,
        start_position: 0,
        stop_position,
        initial_term_id: 5,
        segment_file_length: SEGMENT,
        term_buffer_length: TERM,
        mtu_length: 1_408,
        session_id: 333,
        stream_id: 1001,
        stripped_channel: "aeron:udp?endpoint=src:40000".into(),
        original_channel: "aeron:udp?endpoint=src:40000|alias=log".into(),
        source_identity: "10.0.0.1:40000".into(),
    };
    catalog
        .add_recording(&recording(2 * i64::from(TERM), 1_600_000_000_500))
        .unwrap();
    catalog.add_recording(&recording(-1, -1)).unwrap();
    FakeSource::new(catalog)
}

const SRC_CONTROL: &str = "aeron:udp?endpoint=src:8010";
const REPLICATION: &str = "aeron:udp?endpoint=localhost:0";

fn replicate(shell: &mut FakeShell, cs: i64, correlation_id: i64, src: i64, dst: i64, stop: i64) -> i64 {
    shell.request(
        &client_image(),
        &archive_proxy::replicate2(cs, correlation_id, src, dst, stop, 10, 555, SRC_CONTROL, REPLICATION),
    );
    shell.tick(1);
    let Some(Response::Control { relevant_id, code, .. }) = shell.responses(cs).first().cloned() else {
        panic!("no answer");
    };
    assert_eq!(code, Code::OK);
    relevant_id
}

/// Ticks until the source was asked to replay, every subscription's
/// endpoint resolved meanwhile.
fn until_replayed(shell: &mut FakeShell) {
    for _ in 0..10 {
        for &token in shell.subscriptions.keys() {
            shell.channels.resolved.insert(token, "localhost:45678".into());
        }
        if !shell.source.as_ref().unwrap().replays.is_empty() {
            return;
        }
        shell.tick(1);
    }
    panic!("never replayed");
}

fn replicated_image(join_position: i64) -> ImageInfo {
    ImageInfo {
        correlation_id: 31,
        session_id: 555,
        source_identity: "10.0.0.2:5555".into(),
        ..image(31, 555, join_position)
    }
}

#[test]
fn a_replication_copies_a_stopped_recording_as_javas_does() {
    let mut shell = FakeShell::new(config());
    shell.source = Some(source());
    let cs = connect(&mut shell);
    let id = replicate(&mut shell, cs, 50, 0, -1, -1);
    assert_eq!(id, cs + 1, "a replication takes the next session id");
    assert!(shell.actions.contains(&Action::ConnectSourceArchive {
        replication_id: id,
        request_channel: SRC_CONTROL.into(),
        request_stream_id: 10,
        response_channel: "aeron:udp?endpoint=localhost:0".into(),
        response_stream_id: 20,
    }));
    until_replayed(&mut shell);
    let Request::Replay {
        recording_id,
        position,
        length,
        replay_stream_id,
        replay_channel,
        ..
    } = &shell.source.as_ref().unwrap().replays[0]
    else {
        panic!("a replay");
    };
    assert_eq!(
        (
            *recording_id,
            *position,
            *length,
            *replay_stream_id,
            replay_channel.as_str()
        ),
        (0, 0, -1, 1001, "aeron:udp?endpoint=localhost:45678|session-id=555")
    );
    let token = *shell.subscriptions.keys().last().unwrap();
    assert_eq!(
        shell.subscriptions[&token].channel,
        "aeron:udp?endpoint=localhost:0|session-id=555|rejoin=false"
    );
    shell.image(token, replicated_image(0));
    shell.tick(1);
    shell.record(0, &[(64 * 1024, false), (64 * 1024, false)]);
    for _ in 0..3 {
        shell.tick(1);
    }
    assert_eq!(
        shell.responses(cs),
        [
            signal(cs, id, 0, -1, 0, Signal::REPLICATE),
            ok(cs, id, 1_000),
            signal(cs, id, 0, 1_000, 0, Signal::EXTEND),
            signal(cs, id, 0, 1_000, 131_072, Signal::SYNC),
            signal(cs, id, 0, -1, -1, Signal::REPLICATE_END),
            signal(cs, id, 0, 1_000, 131_072, Signal::STOP),
        ]
    );
    let copy = shell.conductor.catalog().descriptor(0).unwrap();
    let original = shell.source.as_ref().unwrap().catalog.descriptor(0).unwrap();
    assert_eq!(
        copy,
        Descriptor {
            control_session_id: cs,
            correlation_id: id,
            recording_id: 0,
            stop_timestamp: copy.stop_timestamp,
            session_id: 555,
            ..original
        }
    );
    let source = shell.source.as_ref().unwrap();
    assert!(source.closed && source.stopped_replays.is_empty());
    assert!(!shell.source_links[&id], "its links closed");
    assert_eq!(shell.conductor.replication_count(), 0);
}

#[test]
fn a_replication_catches_a_copy_up_to_a_position_of_an_active_source() {
    let mut shell = FakeShell::new(config());
    shell.source = Some(source());
    let cs = connect(&mut shell);
    let dst = stopped_recording(&mut shell, cs, 10, 1);
    let id = replicate(&mut shell, cs, 60, 1, dst, 2 * i64::from(TERM));
    until_replayed(&mut shell);
    let Request::Replay { position, length, .. } = &shell.source.as_ref().unwrap().replays[0] else {
        panic!("a replay");
    };
    assert_eq!((*position, *length), (65_536, 65_536));
    let token = *shell.subscriptions.keys().last().unwrap();
    shell.image(token, replicated_image(65_536));
    shell.tick(1);
    shell.record(dst, &[(64 * 1024, false)]);
    for _ in 0..3 {
        shell.tick(1);
    }
    // No REPLICATE: the copy was there; no SYNC: the source is still recording.
    assert_eq!(
        shell.responses(cs),
        [
            ok(cs, id, 1_001),
            signal(cs, id, dst, 1_001, 65_536, Signal::EXTEND),
            signal(cs, id, dst, -1, -1, Signal::REPLICATE_END),
            signal(cs, id, dst, 1_001, 131_072, Signal::STOP),
        ]
    );
    assert_eq!(shell.conductor.catalog().stop_position(dst).unwrap(), 131_072);
    assert!(
        shell.source.as_ref().unwrap().stopped_replays.is_empty(),
        "the replay ended at the position asked"
    );
}

#[test]
fn a_copy_in_sync_is_not_replayed() {
    let mut shell = FakeShell::new(config());
    shell.source = Some(source());
    let cs = connect(&mut shell);
    let dst = stopped_recording(&mut shell, cs, 10, 2);
    let id = replicate(&mut shell, cs, 60, 0, dst, -1);
    for _ in 0..6 {
        shell.tick(1);
    }
    assert_eq!(
        shell.responses(cs),
        [
            signal(cs, id, dst, -1, 131_072, Signal::SYNC),
            signal(cs, id, dst, -1, -1, Signal::REPLICATE_END),
        ]
    );
    assert!(shell.source.as_ref().unwrap().replays.is_empty());
}

#[test]
fn replications_are_refused_fail_and_stop_as_javas() {
    let mut shell = FakeShell::new(config());
    shell.source = Some(source());
    let cs = connect(&mut shell);
    let (live, _) = record(&mut shell, cs, 10, LOG, 0);
    let image = client_image();
    shell.request(
        &image,
        &archive_proxy::replicate2(cs, 20, 0, 9, -1, 10, 555, SRC_CONTROL, REPLICATION),
    );
    shell.request(
        &image,
        &archive_proxy::replicate2(cs, 21, 0, live, -1, 10, 555, SRC_CONTROL, REPLICATION),
    );
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            error(cs, 20, 5, "unknown destination recording id 9"),
            error(cs, 21, 2, &format!("cannot replicate to active recording {live}")),
        ]
    );

    // A source recording the source does not have.
    let id = replicate(&mut shell, cs, 22, 7, -1, -1);
    for _ in 0..6 {
        shell.tick(1);
    }
    assert_eq!(
        shell.responses(cs),
        [
            error(cs, id, 5, "unknown src recording id 7"),
            signal(cs, id, -1, -1, -1, Signal::REPLICATE_END),
        ]
    );

    // A source that never answers: the connect times out.
    shell.source = None;
    let id = replicate(&mut shell, cs, 23, 0, -1, -1);
    shell.tick(1);
    shell.tick(10_000);
    shell.tick(1);
    let answers: Vec<Response> = shell
        .responses(cs)
        .into_iter()
        .filter(|r| !matches!(r, Response::Ping { .. }))
        .collect();
    assert_eq!(
        answers,
        [
            error(
                cs,
                id,
                14,
                "Replication connection failed=ERROR - Archive connect timeout: step=AWAIT_CONNECT_RESPONSE"
            ),
            signal(cs, id, -1, -1, -1, Signal::REPLICATE_END),
        ]
    );

    // Stopped by any session; then unknown.
    let id = replicate(&mut shell, cs, 24, 0, -1, -1);
    shell.request(&image, &archive_proxy::stop_replication(cs, 25, id));
    shell.tick(1);
    shell.request(&image, &archive_proxy::stop_replication(cs, 26, id));
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            ok(cs, 25, 0),
            signal(cs, id, -1, -1, -1, Signal::REPLICATE_END),
            error(cs, 26, 12, &format!("unknown replication id {id}")),
        ]
    );
}

#[test]
fn a_listing_of_every_recording_ends_as_one_by_uri() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    let first = stopped_recording(&mut shell, cs, 10, 1);
    let second = stopped_recording(&mut shell, cs, 20, 1);
    shell.request(&client_image(), &archive_proxy::list_recordings(cs, 30, 0, 10));
    shell.tick(1);
    let listed: Vec<i64> = shell
        .responses(cs)
        .iter()
        .map(|r| match r {
            Response::Descriptor { descriptor, .. } => descriptor.recording_id,
            Response::Control { code, relevant_id, .. } if *code == Code::RECORDING_UNKNOWN => -*relevant_id - 100,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(listed, [first, second, -(second + 1) - 100]);
}

#[test]
fn the_last_matching_recording_and_the_max_recorded_position() {
    let mut shell = FakeShell::new(config());
    let cs = connect(&mut shell);
    let stopped = stopped_recording(&mut shell, cs, 10, 1);
    let (live, _) = record(&mut shell, cs, 20, LOG, 0);
    shell.record(live, &[(1_024, false)]);
    let image = client_image();
    shell.request(&image, &archive_proxy::max_recorded_position(cs, 30, stopped));
    shell.request(&image, &archive_proxy::max_recorded_position(cs, 31, live));
    shell.request(&image, &archive_proxy::max_recorded_position(cs, 32, 9));
    shell.request(
        &image,
        &archive_proxy::find_last_matching_recording(cs, 33, 0, 77, 1001, "alias=log"),
    );
    shell.request(
        &image,
        &archive_proxy::find_last_matching_recording(cs, 34, live + 1, 77, 1001, ""),
    );
    shell.request(
        &image,
        &archive_proxy::find_last_matching_recording(cs, 35, 0, 78, 1001, ""),
    );
    shell.request(
        &image,
        &archive_proxy::find_last_matching_recording(cs, 36, -1, 77, 1001, ""),
    );
    shell.tick(1);
    assert_eq!(
        shell.responses(cs),
        [
            ok(cs, 30, 65_536),
            ok(cs, 31, 1_024),
            error(cs, 32, 5, "unknown recording id: 9"),
            ok(cs, 33, live),
            ok(cs, 34, -1),
            ok(cs, 35, -1),
            error(cs, 36, 5, "minRecordingId=-1 < 0"),
        ]
    );
}
