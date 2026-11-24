//! The archive client session against the archive's own conductor, in
//! memory through the testkit: the client's requests are what the
//! conductor receives, and everything the conductor sends reaches the
//! client, as on an IPC response stream several clients share.

use rusteron_archive::archive_conductor::{Config, ImageInfo, Offer, RequestImage};
use rusteron_archive::catalog::{Catalog, Descriptor};
use rusteron_archive::client::aeron_archive::{
    ClientSession, Config as ClientConfig, Event, Link, NULL_VALUE, Request,
};
use rusteron_archive::control_session_adapter::{Signal, error_code};
use rusteron_archive::testkit::{FakeShell, FakeSource};

const ARCHIVE_ID: i64 = 42;
const RESPONSE_CHANNEL: &str = "aeron:ipc?term-length=64k";
const LOG: &str = "aeron:ipc?alias=log|term-length=64k";
const TERM: i32 = 64 * 1024;
const SEGMENT: i32 = 128 * 1024 * 1024;
const MS: i64 = 1_000_000;

/// The client's publication and subscription, on the fake shell: what it
/// offers reaches the conductor, unless `deaf`.
struct Wire<'a> {
    shell: &'a mut FakeShell,
    image: &'a RequestImage,
    next_id: &'a mut i64,
    publication_connected: bool,
    deaf: bool,
}

impl Link for Wire<'_> {
    fn offer(&mut self, message: &[u8]) -> Offer {
        if !self.deaf {
            self.shell.request(self.image, message);
        }
        Offer::Sent
    }

    fn publication_connected(&self) -> bool {
        self.publication_connected
    }

    fn subscription_connected(&self) -> bool {
        true
    }

    fn resolved_response_channel(&self) -> Option<String> {
        Some(RESPONSE_CHANNEL.into())
    }

    fn next_correlation_id(&mut self) -> i64 {
        *self.next_id += 1;
        *self.next_id
    }
}

/// An archive and one client of it.
struct Harness {
    shell: FakeShell,
    client: ClientSession,
    image: RequestImage,
    next_id: i64,
    publication_connected: bool,
    deaf: bool,
    events: Vec<Event>,
}

impl Harness {
    fn new() -> Self {
        let shell = FakeShell::new(Config {
            archive_id: ARCHIVE_ID,
            first_control_session_id: 7_000,
            ..Config::default()
        });
        Self::on(shell)
    }

    fn on(shell: FakeShell) -> Self {
        let client = ClientSession::new(client_config(), shell.now_ms * MS);
        Harness {
            shell,
            client,
            image: RequestImage {
                correlation_id: 55,
                session_id: -321,
                source_identity: "aeron:ipc".into(),
            },
            next_id: 100,
            publication_connected: true,
            deaf: false,
            events: Vec::new(),
        }
    }

    /// `n` duty cycles of 1 ms, the archive's then the client's.
    fn run(&mut self, n: usize) -> &mut Self {
        for _ in 0..n {
            self.shell.tick(1);
            for (_, message) in std::mem::take(&mut self.shell.channels.sent) {
                self.client.on_response(&message);
            }
            self.client.on_tick(self.shell.now_ms * MS);
            let mut wire = Wire {
                shell: &mut self.shell,
                image: &self.image,
                next_id: &mut self.next_id,
                publication_connected: self.publication_connected,
                deaf: self.deaf,
            };
            self.client.do_work(&mut wire);
            while let Some(event) = self.client.poll_event() {
                self.events.push(event);
            }
        }
        self
    }

    fn take(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }

    fn connect(&mut self) -> i64 {
        self.run(5);
        let events = self.take();
        let [
            Event::Connected {
                control_session_id,
                archive_id,
            },
        ] = events[..]
        else {
            panic!("not connected: {events:?}");
        };
        assert_eq!(archive_id, ARCHIVE_ID);
        control_session_id
    }

    fn request(&mut self, correlation_id: i64, request: Request) -> &mut Self {
        self.client.request(correlation_id, &request).expect("connected");
        self
    }
}

fn client_config() -> ClientConfig {
    ClientConfig {
        response_stream_id: 120,
        client_name: "consensus-module".into(),
        ..ClientConfig::default()
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

#[test]
fn a_client_connects_as_javas_and_learns_the_archive_id() {
    let mut h = Harness::new();
    let cs = h.connect();
    assert_eq!(cs, 7_000);
    assert!(h.client.is_connected());
    assert_eq!(h.client.archive_id(), ARCHIVE_ID);
    // The archive answers the connect only until the first request: no more
    // connect responses reach the client.
    h.run(200);
    assert_eq!(h.take(), []);
}

#[test]
fn a_recording_is_started_listed_and_stopped_its_signals_routed() {
    let mut h = Harness::new();
    let cs = h.connect();
    h.request(
        201,
        Request::StartRecording {
            channel: LOG.into(),
            stream_id: 100,
            local: true,
            auto_stop: true,
        },
    )
    .run(2);
    let [
        Event::Ok {
            correlation_id: 201,
            relevant_id: subscription_id,
        },
    ] = h.take()[..]
    else {
        panic!("not started");
    };
    let token = h.shell.subscription("aeron:ipc?alias=log");
    let recording_id = h.shell.conductor.catalog().next_recording_id();
    h.shell.image(token, image(9, 77, 0));
    h.run(2);
    assert_eq!(
        h.take(),
        [Event::Signal {
            correlation_id: 201,
            recording_id,
            subscription_id,
            position: 0,
            signal: Signal::START,
        }]
    );
    h.shell.record(recording_id, &[(4_096, false)]);

    // One listing at a time, as Java's client lists: a second while one is
    // under way is refused.
    h.request(202, Request::RecordingPosition { recording_id })
        .request(203, Request::ListRecording { recording_id })
        .run(3)
        .request(204, Request::ListRecording { recording_id: 99 })
        .run(3);
    let events = h.take();
    assert_eq!(
        events[0],
        Event::Ok {
            correlation_id: 202,
            relevant_id: 4_096
        }
    );
    let Event::Descriptor {
        correlation_id: 203,
        ref descriptor,
    } = events[1]
    else {
        panic!("no descriptor: {events:?}");
    };
    assert_eq!(
        (descriptor.recording_id, descriptor.stop_position, descriptor.stream_id),
        (recording_id, NULL_VALUE, 100)
    );
    assert_eq!(
        events[2],
        Event::RecordingUnknown {
            correlation_id: 204,
            recording_id: 99
        }
    );

    h.request(205, Request::StopRecordingByIdentity { recording_id })
        .run(2)
        .request(206, Request::StopPosition { recording_id })
        .run(3);
    assert_eq!(
        h.take(),
        [
            Event::Ok {
                correlation_id: 205,
                relevant_id: 1
            },
            Event::Signal {
                correlation_id: 201,
                recording_id,
                subscription_id,
                position: 4_096,
                signal: Signal::STOP,
            },
            Event::Ok {
                correlation_id: 206,
                relevant_id: 4_096
            },
        ]
    );
    assert_eq!(h.client.control_session_id(), cs);
}

#[test]
fn an_error_carries_javas_code_and_message() {
    let mut h = Harness::new();
    h.connect();
    h.request(
        301,
        Request::ExtendRecording {
            recording_id: 7,
            channel: LOG.into(),
            stream_id: 100,
            local: true,
            auto_stop: true,
        },
    )
    .run(2);
    let [
        Event::Error {
            correlation_id: 301,
            error_code,
            ref message,
        },
    ] = h.take()[..]
    else {
        panic!("no error");
    };
    assert_eq!(error_code, error_code::UNKNOWN_RECORDING);
    assert_eq!(message, "unknown recording id: 7");
}

/// A source archive with a stopped recording of two terms.
fn source() -> FakeSource {
    let mut catalog = Catalog::create(4_096).unwrap();
    catalog
        .add_recording(&Descriptor {
            control_session_id: 0,
            correlation_id: 0,
            recording_id: -1,
            start_timestamp: 1_600_000_000_000,
            stop_timestamp: 1_600_000_000_500,
            start_position: 0,
            stop_position: 2 * i64::from(TERM),
            initial_term_id: 5,
            segment_file_length: SEGMENT,
            term_buffer_length: TERM,
            mtu_length: 1_408,
            session_id: 333,
            stream_id: 100,
            stripped_channel: "aeron:udp?endpoint=src:40000".into(),
            original_channel: "aeron:udp?endpoint=src:40000|alias=log".into(),
            source_identity: "10.0.0.1:40000".into(),
        })
        .unwrap();
    FakeSource::new(catalog)
}

#[test]
fn a_replication_reports_its_signals_as_the_cluster_waits_for_them() {
    let mut h = Harness::new();
    h.shell.source = Some(source());
    h.connect();
    h.request(
        401,
        Request::Replicate {
            src_recording_id: 0,
            dst_recording_id: NULL_VALUE,
            stop_position: NULL_VALUE,
            src_control_stream_id: 10,
            src_control_channel: "aeron:udp?endpoint=src:8010".into(),
            replication_channel: "aeron:udp?endpoint=localhost:0".into(),
            replication_session_id: 555,
        },
    )
    .run(2);
    let events = h.take();
    let Some(&Event::Ok {
        correlation_id: 401,
        relevant_id: replication_id,
    }) = events.first()
    else {
        panic!("not replicating: {events:?}");
    };
    for _ in 0..10 {
        for &token in h.shell.subscriptions.keys() {
            h.shell.channels.resolved.insert(token, "localhost:45678".into());
        }
        h.run(1);
    }
    let token = *h.shell.subscriptions.keys().last().unwrap();
    h.shell.image(
        token,
        ImageInfo {
            source_identity: "10.0.0.2:5555".into(),
            ..image(31, 555, 0)
        },
    );
    h.run(1);
    h.shell.record(0, &[(64 * 1024, false), (64 * 1024, false)]);
    h.run(3);
    let signals: Vec<(i64, Signal)> = events
        .into_iter()
        .chain(h.take())
        .filter_map(|e| match e {
            Event::Signal {
                correlation_id, signal, ..
            } => Some((correlation_id, signal)),
            _ => None,
        })
        .collect();
    assert_eq!(
        signals,
        [
            (replication_id, Signal::REPLICATE),
            (replication_id, Signal::EXTEND),
            (replication_id, Signal::SYNC),
            (replication_id, Signal::REPLICATE_END),
            (replication_id, Signal::STOP),
        ]
    );
}

#[test]
fn an_unanswered_request_times_out() {
    let mut h = Harness::new();
    h.connect();
    h.deaf = true;
    h.request(501, Request::StopPosition { recording_id: 0 }).run(10_000);
    assert_eq!(h.take(), []);
    h.run(2);
    assert_eq!(h.take(), [Event::TimedOut { correlation_id: 501 }]);
}

#[test]
fn a_connect_that_never_completes_fails_as_javas() {
    let mut h = Harness::new();
    h.publication_connected = false;
    h.run(10_002);
    assert_eq!(
        h.take(),
        [Event::ConnectFailed {
            message: "Archive connect timeout: step=AWAIT_PUBLICATION_CONNECTED".into()
        }]
    );
    assert!(h.client.is_closed());
}

#[test]
fn a_lost_archive_ends_the_session() {
    let mut h = Harness::new();
    h.connect();
    h.client.on_disconnected();
    h.run(1);
    assert_eq!(h.take(), [Event::Disconnected]);
    assert!(
        h.client
            .request(601, &Request::StopPosition { recording_id: 0 })
            .is_err()
    );
}

#[test]
fn clients_sharing_a_response_stream_see_only_their_own_answers() {
    let mut first = Harness::new();
    let cs1 = first.connect();
    let mut second = ClientSession::new(client_config(), first.shell.now_ms * MS);
    let image2 = RequestImage {
        correlation_id: 56,
        session_id: -322,
        source_identity: "aeron:ipc".into(),
    };
    let mut second_events = Vec::new();
    first.request(701, Request::StopPosition { recording_id: 9 });
    for _ in 0..5 {
        first.shell.tick(1);
        for (_, message) in std::mem::take(&mut first.shell.channels.sent) {
            first.client.on_response(&message);
            second.on_response(&message);
        }
        for (client, image) in [(&mut first.client, &first.image), (&mut second, &image2)] {
            client.on_tick(first.shell.now_ms * MS);
            client.do_work(&mut Wire {
                shell: &mut first.shell,
                image,
                next_id: &mut first.next_id,
                publication_connected: true,
                deaf: false,
            });
        }
        while let Some(event) = first.client.poll_event() {
            first.events.push(event);
        }
        while let Some(event) = second.poll_event() {
            second_events.push(event);
        }
    }
    let [
        Event::Connected {
            control_session_id: cs2,
            ..
        },
    ] = second_events[..]
    else {
        panic!("second not connected: {second_events:?}");
    };
    assert_ne!(cs1, cs2);
    assert_eq!(
        first.take(),
        [Event::Error {
            correlation_id: 701,
            error_code: error_code::UNKNOWN_RECORDING,
            message: "unknown recording id: 9".into()
        }]
    );
}
