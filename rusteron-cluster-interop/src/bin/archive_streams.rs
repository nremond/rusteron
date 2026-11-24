//! Records tagged messages into an archive, and lists an archive's
//! recordings with a checksum of each: the test tool for the archives an
//! application keeps beside the cluster's, which `standby_seed
//! --extra-archive` copies (`scripts/standby.sh`).
//!
//! ```text
//! archive_streams record AERON_DIR CONTROL_STREAM STREAM COUNT TAG
//! archive_streams list AERON_DIR CONTROL_STREAM
//! ```
//!
//! Both reach the archive on its local control channel,
//! `aeron:ipc?term-length=64k` on stream `CONTROL_STREAM`, through the media
//! driver in `AERON_DIR`.
//!
//! - `record`: a new recording of `aeron:ipc` stream `STREAM`, of `COUNT`
//!   messages. Message `k` is `TAG k` filled to a length that varies from 32
//!   to 2,000 bytes, so recordings of two tags differ from their first
//!   message. It waits until the archive holds them all, then stops the
//!   recording and prints `recorded recording=ID stream=S start=A stop=B`.
//! - `list`: a line a recording, oldest first:
//!   `recording=ID stream=S start=A stop=B crc=C`, where `C` is the CRC-32C of
//!   each message and its position as a replay delivers them (on
//!   `aeron:ipc`, stream 7400). A copy of a recording by an archive's
//!   replication has the same positions and messages, so the same line but
//!   for its id.
//!
//! Exits 0 on success, 1 on a failure, 2 on a bad command line.

use std::error::Error;
use std::process::ExitCode;
use std::thread::sleep;
use std::time::{Duration, Instant};

use rusteron_archive::*;

const LOCAL_CONTROL: &str = "aeron:ipc?term-length=64k";
/// The archive's responses to this client, on `aeron:ipc`.
const RESPONSE_STREAM: i32 = 7_401;
/// The replays `list` reads, on `aeron:ipc`.
const REPLAY_STREAM: i32 = 7_400;
const TIMEOUT: Duration = Duration::from_secs(30);
const NULL_VALUE: i64 = -1;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["record", aeron_dir, control, stream, count, tag] => match (control.parse(), stream.parse(), count.parse()) {
            (Ok(control), Ok(stream), Ok(count)) => record(aeron_dir, control, stream, count, tag),
            _ => return usage(),
        },
        ["list", aeron_dir, control] => match control.parse() {
            Ok(control) => list(aeron_dir, control),
            Err(_) => return usage(),
        },
        _ => return usage(),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("archive_streams: {e}");
            ExitCode::from(1)
        }
    }
}

fn usage() -> ExitCode {
    eprintln!(
        "usage: archive_streams record AERON_DIR CONTROL_STREAM STREAM COUNT TAG | \
         archive_streams list AERON_DIR CONTROL_STREAM"
    );
    ExitCode::from(2)
}

/// A client of the driver in `aeron_dir`, and of the archive on its local control stream.
fn connect(aeron_dir: &str, control: i32, what: &str) -> Result<(Aeron, AeronArchive, AeronArchiveContext)> {
    let context = AeronContext::new()?;
    context.set_dir(&aeron_dir.into_c_string())?;
    context.set_client_name(&format!("archive_streams {what}").into_c_string())?;
    let aeron = Aeron::new(&context)?;
    aeron.start()?;
    let archive_context = AeronArchiveContext::new()?;
    archive_context.set_aeron(&aeron)?;
    archive_context.set_control_request_channel(&LOCAL_CONTROL.into_c_string())?;
    archive_context.set_control_request_stream_id(control)?;
    archive_context.set_control_response_channel(&LOCAL_CONTROL.into_c_string())?;
    archive_context.set_control_response_stream_id(RESPONSE_STREAM)?;
    let archive = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?.poll_blocking(TIMEOUT)?;
    Ok((aeron, archive, archive_context))
}

/// Message `k` of `tag`: the tag and `k`, filled to a length from 32 to 2,000 bytes.
fn message(tag: &str, k: u64) -> Vec<u8> {
    let mut bytes = format!("{tag} {k} ").into_bytes();
    let length = 32 + (k as usize * 997) % 1_969;
    let fill = tag.bytes().fold(k as u8, |a, b| a.wrapping_mul(31).wrapping_add(b));
    while bytes.len() < length {
        bytes.push(fill.wrapping_add(bytes.len() as u8));
    }
    bytes
}

fn record(aeron_dir: &str, control: i32, stream: i32, count: u64, tag: &str) -> Result<()> {
    let (aeron, archive, _context) = connect(aeron_dir, control, "record")?;
    let subscription = archive.start_recording(AERON_IPC_STREAM, stream, SOURCE_LOCATION_LOCAL, false)?;
    let publication = aeron
        .async_add_exclusive_publication(AERON_IPC_STREAM, stream)?
        .poll_blocking(TIMEOUT)?;
    let session = publication.get_constants()?.session_id();
    let deadline = Instant::now() + TIMEOUT;
    let recording = loop {
        let found = archive.find_recording(|d| d.stream_id() == stream && d.session_id() == session)?;
        if let Some(found) = found {
            break found;
        }
        if Instant::now() > deadline {
            return Err(format!("no recording of stream {stream}, session {session} after {TIMEOUT:?}").into());
        }
        sleep(Duration::from_millis(10));
    };
    let mut sent = 0;
    while sent < count {
        match publication.offer(&message(tag, sent)) {
            Ok(_) => sent += 1,
            Err(e) if e.is_retryable() && Instant::now() < deadline => sleep(Duration::from_micros(100)),
            Err(e) => return Err(format!("message {sent} of {count}: {e}").into()),
        }
    }
    let end = publication.position();
    while archive.get_recording_position(recording.recording_id)? < end {
        if Instant::now() > deadline {
            return Err(format!("recording {} did not reach {end}", recording.recording_id).into());
        }
        sleep(Duration::from_millis(1));
    }
    archive.stop_recording_subscription(subscription)?;
    let stop = loop {
        let stop = archive.get_stop_position(recording.recording_id)?;
        if stop != NULL_VALUE {
            break stop;
        }
        if Instant::now() > deadline {
            return Err(format!("recording {} did not stop", recording.recording_id).into());
        }
        sleep(Duration::from_millis(1));
    };
    println!(
        "recorded recording={} stream={stream} start={} stop={stop}",
        recording.recording_id, recording.start_position
    );
    Ok(())
}

fn list(aeron_dir: &str, control: i32) -> Result<()> {
    let (aeron, archive, _context) = connect(aeron_dir, control, "list")?;
    let mut recordings = archive.collect_recordings(|_| true)?;
    recordings.sort_by_key(|r| r.recording_id);
    for r in recordings {
        let stop = match archive.get_stop_position(r.recording_id)? {
            NULL_VALUE => archive.get_recording_position(r.recording_id)?,
            stop => stop,
        };
        let crc = checksum(&aeron, &archive, r.recording_id, r.start_position, stop)?;
        println!(
            "recording={} stream={} start={} stop={stop} crc={crc:08x}",
            r.recording_id, r.stream_id, r.start_position
        );
    }
    Ok(())
}

/// The CRC-32C of a replay of `recording` from `from` to `to`: each message's position, then its bytes.
fn checksum(aeron: &Aeron, archive: &AeronArchive, recording: i64, from: i64, to: i64) -> Result<u32> {
    if from >= to {
        return Ok(0);
    }
    let params = AeronArchiveReplayParams::builder()
        .position(from)
        .length(to - from)
        .build()?;
    let session = archive.start_replay(recording, AERON_IPC_STREAM, REPLAY_STREAM, &params)? as i32;
    let channel = ChannelUri::add_session_id("aeron:ipc", session);
    let subscription = aeron
        .async_add_subscription(&channel.into_c_string(), REPLAY_STREAM, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(TIMEOUT)?;
    let mut crc = 0u32;
    let (mut position, mut last) = (from, Instant::now());
    while position < to {
        let image = subscription.image_by_session_id(session);
        let fragments = image.as_ref().map_or(0, |image| {
            image
                .poll_fn(
                    |payload: &[u8], header: &AeronHeader| {
                        crc = crc32c::crc32c_append(crc, &header.position().to_le_bytes());
                        crc = crc32c::crc32c_append(crc, payload);
                    },
                    256,
                )
                .map_or(0, |n| n.max(0))
        });
        if let Some(image) = &image {
            position = image.position();
        }
        if fragments > 0 {
            last = Instant::now();
        } else if image.as_ref().is_some_and(|image| image.is_closed()) || last.elapsed() > TIMEOUT {
            return Err(format!("the replay of recording {recording} stopped at {position} of {to}").into());
        } else {
            sleep(Duration::from_millis(1));
        }
    }
    if position != to {
        return Err(format!("the replay of recording {recording} went past {to} to {position}").into());
    }
    Ok(crc)
}
