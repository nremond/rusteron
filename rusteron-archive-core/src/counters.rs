//! The counters an archive allocates, their keys and labels as Aeron Archive
//! 1.53.3 writes them (`ArchiveCounters`, `RecordingPos`,
//! `ControlSessionCounter`): clusters find a recording's position by its
//! key, operators read the rest with `AeronStat`.

/// A recording's position: what is written of it.
pub const RECORDING_POSITION_TYPE_ID: i32 = 100;
pub const ARCHIVE_ERROR_COUNT_TYPE_ID: i32 = 101;
pub const ARCHIVE_CONTROL_SESSIONS_TYPE_ID: i32 = 102;
pub const ARCHIVE_MAX_CYCLE_TIME_TYPE_ID: i32 = 103;
pub const ARCHIVE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID: i32 = 104;
pub const ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID: i32 = 105;
pub const ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID: i32 = 106;
pub const ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID: i32 = 107;
pub const ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID: i32 = 108;
pub const ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID: i32 = 109;
pub const ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID: i32 = 110;
pub const ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID: i32 = 111;
pub const ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID: i32 = 112;
pub const ARCHIVE_CONTROL_SESSION_TYPE_ID: i32 = 113;

/// The version an archive's errors counter names.
pub const ARCHIVE_VERSION: &str = "1.54.2";
pub const ARCHIVE_GIT_SHA: &str = "481d4271ad";

const ARCHIVE_ID_LABEL_SUFFIX: &str = " - archiveId=";
/// Aeron's longest key and label.
pub const MAX_KEY_LENGTH: usize = 112;
pub const MAX_LABEL_LENGTH: usize = 380;
const SOURCE_IDENTITY_OFFSET: usize = 16;

/// What Java's `putStringWithoutLengthAscii` writes: a character past ASCII
/// as `?`.
fn ascii(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii() { c } else { '?' }).collect()
}

/// `ArchiveCounters.appendArchiveIdLabel`.
pub fn archive_id_suffix(archive_id: i64) -> String {
    format!("{ARCHIVE_ID_LABEL_SUFFIX}{archive_id}")
}

/// A counter of the archive's own: the archive's id its key, the name and
/// the archive's id its label (`ArchiveCounters.allocate`).
pub fn archive_counter(name: &str, archive_id: i64) -> (Vec<u8>, String) {
    (
        archive_id.to_le_bytes().to_vec(),
        format!("{name}{}", archive_id_suffix(archive_id)),
    )
}

/// The errors counter, with the archive's version: an archive refuses to
/// start beside another with its id.
pub fn error_counter(archive_id: i64) -> (Vec<u8>, String) {
    (
        archive_id.to_le_bytes().to_vec(),
        format!(
            "Archive Errors{} version={ARCHIVE_VERSION} commit={ARCHIVE_GIT_SHA}",
            archive_id_suffix(archive_id)
        ),
    )
}

/// The counters an archive allocates as it starts, after its errors
/// counter, in Java's order (one thread: Java's SHARED mode).
pub fn archive_counters() -> [(i32, &'static str); 11] {
    [
        (
            ARCHIVE_MAX_CYCLE_TIME_TYPE_ID,
            "archive-conductor max cycle time in ns: SHARED",
        ),
        (
            ARCHIVE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
            "archive-conductor work cycle time exceeded count: threshold=100ms SHARED",
        ),
        (ARCHIVE_CONTROL_SESSIONS_TYPE_ID, "Archive Control Sessions"),
        (ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID, "Archive Recording Sessions"),
        (ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID, "Archive Replay Sessions"),
        (
            ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID,
            "archive-recorder max write time in ns",
        ),
        (
            ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID,
            "archive-recorder total write bytes",
        ),
        (
            ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID,
            "archive-recorder total write time in ns",
        ),
        (
            ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID,
            "archive-replayer max read time in ns",
        ),
        (
            ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID,
            "archive-replayer total read bytes",
        ),
        (
            ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID,
            "archive-replayer total read time in ns",
        ),
    ]
}

/// A recording's position counter (`RecordingPos.allocate`):
///
/// ```text
/// key:   recordingId i64 · sessionId i32 · sourceIdentity's length i32 · sourceIdentity (ASCII, at most 88) · archiveId i64
/// label: rec-pos: <recordingId> <sessionId> <streamId> <strippedChannel, cut to fit> - archiveId=<archiveId>
/// ```
pub fn recording_position(
    archive_id: i64,
    recording_id: i64,
    session_id: i32,
    stream_id: i32,
    stripped_channel: &str,
    source_identity: &str,
) -> (Vec<u8>, String) {
    let source = ascii(source_identity);
    let source_length = source.len().min(MAX_KEY_LENGTH - SOURCE_IDENTITY_OFFSET - 8);
    let mut key = Vec::with_capacity(SOURCE_IDENTITY_OFFSET + source_length + 8);
    key.extend_from_slice(&recording_id.to_le_bytes());
    key.extend_from_slice(&session_id.to_le_bytes());
    key.extend_from_slice(&(source_length as i32).to_le_bytes());
    key.extend_from_slice(&source.as_bytes()[..source_length]);
    key.extend_from_slice(&archive_id.to_le_bytes());

    let mut label = format!("rec-pos: {recording_id} {session_id} {stream_id} ");
    let suffix = archive_id_suffix(archive_id);
    let room = MAX_LABEL_LENGTH.saturating_sub(label.len() + suffix.len());
    label.extend(ascii(stripped_channel).chars().take(room));
    label.push_str(&suffix);
    (key, label)
}

/// A control session's counter (`ControlSessionCounter.allocate`): the
/// archive's id and the session's its key; the client's info and the
/// request stream's source its label.
pub fn control_session(
    archive_id: i64,
    control_session_id: i64,
    client_info: &str,
    source_identity: &str,
    request_session_id: i32,
) -> (Vec<u8>, String) {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&archive_id.to_le_bytes());
    key.extend_from_slice(&control_session_id.to_le_bytes());
    let image = format!("sourceIdentity={source_identity} sessionId={request_session_id}");
    let info = if client_info.is_empty() {
        image
    } else {
        format!("{client_info} {image}")
    };
    let label = format!("control-session: {}{}", ascii(&info), archive_id_suffix(archive_id));
    (key, label)
}
