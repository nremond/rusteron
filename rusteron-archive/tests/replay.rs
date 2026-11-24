//! A replay session's arithmetic as Java's `ReplaySession`: where it reads,
//! what it offers of a read, how it moves on and how it follows its limit.

use rusteron_archive::replay_session::{Batch, Limit, ReplayParams, ReplaySession, ReplayState};

const TERM: i32 = 64 * 1024;
const SEGMENT: i32 = 128 * 1024;

fn params(start: i64, position: i64, stop: i64, length: i64) -> ReplayParams {
    ReplayParams {
        replay_session_id: (1 << 32) | 77,
        recording_id: 3,
        start_position: start,
        replay_position: position,
        stop_position: stop,
        replay_length: length,
        term_length: TERM,
        segment_length: SEGMENT,
        initial_term_id: 10,
        stream_id: 1001,
        follows_counter: false,
        buffer_length: 1024 * 1024,
        connect_deadline_ms: 5_000,
    }
}

/// A frame as the recorder wrote it: its header, then `length - 32` bytes.
fn frame(bytes: &mut Vec<u8>, length: i32, frame_type: u16, term_offset: i32, term_id: i32) {
    let start = bytes.len();
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&[0, 0xC0]);
    bytes.extend_from_slice(&frame_type.to_le_bytes());
    bytes.extend_from_slice(&term_offset.to_le_bytes());
    bytes.extend_from_slice(&42i32.to_le_bytes());
    bytes.extend_from_slice(&1001i32.to_le_bytes());
    bytes.extend_from_slice(&term_id.to_le_bytes());
    bytes.extend_from_slice(&0i64.to_le_bytes());
    let aligned = (length as usize).div_ceil(32) * 32;
    bytes.resize(start + aligned, 7);
}

fn header(term_offset: i32, term_id: i32, stream_id: i32) -> Vec<u8> {
    let mut h = vec![0u8; 32];
    h[8..12].copy_from_slice(&term_offset.to_le_bytes());
    h[16..20].copy_from_slice(&stream_id.to_le_bytes());
    h[20..24].copy_from_slice(&term_id.to_le_bytes());
    h
}

#[test]
fn a_replay_reads_from_its_position_in_its_segment() {
    // A recording from mid-term; a replay from the third term, in the second segment.
    let start = 3 * i64::from(TERM) + 1_024;
    let position = start + 2 * i64::from(TERM) + 512 - 1_024;
    let mut s = ReplaySession::new(params(start, position, position + 10_000, 4_096));
    // Segments count from the start's term base: 3 terms in, 128 KiB each.
    assert_eq!(s.segment_file_name(), "3-327680.rec");
    let at = s.on_file_opened().expect("mid recording: the frame there is checked");
    assert_eq!(at, 512);
    assert_eq!(s.file_offset(), 512);
    // Term 10 + 5 terms in.
    assert!(s.check_header(&header(512, 15, 1001)).is_ok());
    let e = s.clone().check_header(&header(512, 15, 9)).unwrap_err();
    assert_eq!(e.code, 16);
    assert_eq!(
        e.message,
        format!(
            "replayPosition={position} (segmentFilePosition=327680, segmentOffset=0, termOffset=512, frameOffset=0) \
             does not point to a valid frame, recordingId=3, replaySessionId=4294967373, segmentFile=3-327680.rec"
        )
    );
    let e = s.clone().check_header(&[0; 10]).unwrap_err();
    assert_eq!(
        (
            e.code,
            e.message
                .starts_with("java.io.IOException: failed to read fragment header")
        ),
        (0, true)
    );
    assert_eq!(s.read_length(), 10_000, "up to the stop");

    // At the recording's start, or its stop, nothing is checked.
    let mut s = ReplaySession::new(params(start, start, start + 64, 64));
    assert_eq!(s.on_file_opened(), None);
}

#[test]
fn a_read_offers_its_whole_data_frames_then_a_padding_frame() {
    let mut s = ReplaySession::new(params(0, 0, 1 << 20, i64::MAX));
    s.on_file_opened();
    s.on_connected();
    let mut read = Vec::new();
    frame(&mut read, 100, 1, 0, 10);
    frame(&mut read, 64, 1, 128, 10);
    frame(&mut read, 4_000, 0, 192, 10);
    let batch = s.prepare(&mut read, -5, 2001).unwrap();
    assert_eq!(
        batch,
        Batch {
            data: 192,
            padding: 4_000
        }
    );
    // The replay's session and stream in the data frames; the rest as recorded.
    for at in [0, 128] {
        assert_eq!(i32::from_le_bytes(read[at + 12..at + 16].try_into().unwrap()), -5);
        assert_eq!(i32::from_le_bytes(read[at + 16..at + 20].try_into().unwrap()), 2001);
        assert_eq!(i32::from_le_bytes(read[at + 20..at + 24].try_into().unwrap()), 10);
    }
    assert!(s.on_offered(192, 192));
    assert!(s.on_offered(4_224, 4_000));
    assert_eq!(s.replay_position(), 192 + 4_000);
    assert_eq!(s.file_offset(), 4_192);

    // A frame cut by the read waits for the next.
    let mut read = Vec::new();
    frame(&mut read, 64, 1, 4_192, 10);
    read.extend_from_slice(&[0; 40]);
    read[64..68].copy_from_slice(&200i32.to_le_bytes());
    read[70..72].copy_from_slice(&1u16.to_le_bytes());
    assert_eq!(s.prepare(&mut read, -5, 2001).unwrap(), Batch { data: 64, padding: 0 });

    // Nothing where a frame should be: the recording ended there.
    let mut read = vec![0u8; 64];
    let e = s.prepare(&mut read, -5, 2001).unwrap_err();
    assert_eq!(
        e.message,
        "unexpected end of recording at position=4192 (segmentFilePosition=0, segmentOffset=0, termOffset=4192, \
         frameOffset=0), recordingId=3, replaySessionId=4294967373, segmentFile=3-0.rec"
    );
    assert_eq!(s.state(), ReplayState::Inactive { revoke: true });
}

#[test]
fn a_replay_ends_at_its_limit_or_with_its_subscriber() {
    let mut s = ReplaySession::new(params(0, 0, 1 << 20, 300));
    s.on_file_opened();
    s.on_connected();
    assert_eq!(s.read_length(), 64 * 1024, "up to the term's end");
    let mut read = Vec::new();
    for i in 0..4 {
        frame(&mut read, 100, 1, i * 128, 10);
    }
    // Up to the limit: a frame that starts before it goes whole.
    assert_eq!(s.prepare(&mut read, 1, 1).unwrap().data, 384);
    assert!(s.on_offered(384, 384));
    assert_eq!(s.state(), ReplayState::Inactive { revoke: false });

    let mut s = ReplaySession::new(params(0, 0, 1 << 20, i64::MAX));
    s.on_connected();
    assert!(!s.on_offered(-2, 64), "back pressured: read again");
    assert_eq!(s.state(), ReplayState::Replay);
    assert!(!s.on_offered(-1, 64));
    assert_eq!(s.state(), ReplayState::Inactive { revoke: true });
}

/// A replay its client stops ends as one at its end, its publication closed
/// and its images left to their readers: a cluster follower's catch-up image
/// revoked under its service leaves the service behind for good. One stopped
/// as its recording goes is revoked, as Java's are.
#[test]
fn a_replay_stopped_by_its_client_is_not_revoked() {
    let mut s = ReplaySession::new(params(0, 0, 1 << 20, i64::MAX));
    s.on_connected();
    s.stop();
    assert_eq!(s.state(), ReplayState::Inactive { revoke: false });

    let mut s = ReplaySession::new(params(0, 0, 1 << 20, i64::MAX));
    s.on_connected();
    s.abort();
    assert_eq!(s.state(), ReplayState::Inactive { revoke: true });
}

#[test]
fn a_replay_moves_from_term_to_term_and_segment_to_segment() {
    let mut s = ReplaySession::new(params(0, i64::from(TERM) - 64, 1 << 20, i64::MAX));
    s.on_file_opened();
    s.on_connected();
    assert_eq!(s.read_length(), 64);
    assert!(s.on_offered(1, 64));
    assert_eq!(s.next_term_if_due(), None, "the second term, same file");
    assert_eq!(s.file_offset(), u64::try_from(TERM).unwrap());
    assert!(s.on_offered(1, TERM as usize));
    assert_eq!(s.next_term_if_due(), Some("3-131072.rec".to_string()));
    assert_eq!(s.file_offset(), 0);
    let e = s.on_segment_missing();
    assert!(e.message.starts_with("recording segment not found, recordingId=3"));
}

#[test]
fn a_replay_follows_its_counter_until_it_closes() {
    let mut p = params(0, 0, 1_024, i64::MAX - 1);
    p.follows_counter = true;
    let mut s = ReplaySession::new(p);
    s.on_connected();
    assert!(!s.awaits_limit());
    assert!(s.on_offered(1, 1_024));
    assert!(s.awaits_limit());
    let open = |value| Limit {
        value,
        closed: false,
        same_registration: true,
    };
    assert!(s.on_limit(open(1_024)), "nothing new: wait");
    assert!(!s.on_limit(open(2_048)), "moved on: replay");
    assert_eq!(s.read_length(), 1_024);
    assert!(s.on_offered(1, 1_024));
    // The recording stopped at 2,048: the replay ends there.
    assert!(s.on_limit(Limit {
        value: 2_048,
        closed: true,
        same_registration: true
    }));
    assert_eq!(s.state(), ReplayState::Inactive { revoke: false });

    // A bounded replay whose counter id went to another counter stops at its stop.
    let mut p = params(0, 0, 1_024, i64::MAX - 1);
    p.follows_counter = true;
    let mut s = ReplaySession::new(p);
    s.on_connected();
    assert!(s.on_offered(1, 1_024));
    assert!(s.on_limit(Limit {
        value: 1 << 30,
        closed: true,
        same_registration: false
    }));
    assert_eq!(s.state(), ReplayState::Inactive { revoke: false });
}

#[test]
fn the_connect_timeout_bounds_the_wait_for_the_file_and_the_subscriber() {
    let mut s = ReplaySession::new(params(0, 0, 1_024, 1_024));
    assert_eq!(s.on_file_missing(5_000), None);
    let e = s.on_file_missing(5_001).unwrap();
    assert!(
        e.message
            .starts_with("recording segment file not created, recordingId=3")
    );
    let mut s = ReplaySession::new(params(0, 0, 1_024, 1_024));
    assert_eq!(s.on_not_connected(5_000, "aeron:ipc", 2001), None);
    let e = s.on_not_connected(5_001, "aeron:ipc", 2001).unwrap();
    assert!(
        e.message
            .starts_with("no connection established for replayChannel=aeron:ipc, replayStreamId=2001, recordingId=3")
    );
    // An empty recording from 0, replayed for nothing: done at once.
    let mut s = ReplaySession::new(params(0, 0, 0, 0));
    assert!(s.is_empty());
}
