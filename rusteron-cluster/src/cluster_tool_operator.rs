//! Aeron Cluster's `ClusterToolOperator`: what each of [`ClusterTool`]'s
//! commands does, on a cluster component's directory, as Aeron 1.54.2's does
//! it and printing what it prints.
//!
//! It reads the component's mark file and recording log as Java's tool reads
//! them, the driver's error log from its CnC file, and the driver's counters
//! through a client of the driver; it asks the consensus module for its
//! members on the control channel, sets the control toggles, and has the
//! archive replay the latest snapshot for `describe-latest-cm-snapshot`.
//!
//! [`ClusterTool`]: crate::cluster_tool

#![allow(unsafe_code)]

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusteron_archive::fragment_assembler::{Fragment, PollAction, Reassembler};
use rusteron_archive::logbuffer::{HEADER_LENGTH as FRAME_HEADER_LENGTH, UNFRAGMENTED};
use rusteron_archive::{distinct_error_log, *};

use crate::cluster_counters;
use crate::codecs::cluster::{
    SBE_SCHEMA_ID as CLUSTER_SCHEMA_ID, SBE_SCHEMA_VERSION as CLUSTER_SCHEMA_VERSION,
    cluster_members_extended_response_codec, cluster_members_query_codec,
};
use crate::codecs::cluster::{close_reason::CloseReason, cluster_time_unit::ClusterTimeUnit};
use crate::consensus_module_snapshot_taker::{Loaded, Loader};
use crate::recording_log::{self, Entry, RecordingExtent, RecordingLog};

/// The tool's outcome for a command: Java's `SUCCESS`.
pub const SUCCESS: i32 = 0;
/// Java's `FAILURE`.
pub const FAILURE: i32 = -1;

/// The consensus module's mark file (`ClusterMarkFile.FILENAME`).
pub const MARK_FILE_NAME: &str = "cluster-mark.dat";
/// The link naming the directory of the consensus module's mark file.
pub const LINK_FILE_NAME: &str = "cluster-mark.lnk";
/// A service container's mark file name starts with this.
pub const SERVICE_FILENAME_PREFIX: &str = "cluster-mark-service-";
const FILE_EXTENSION: &str = ".dat";
const LINK_FILE_EXTENSION: &str = ".lnk";
const RECORDING_LOG_FILE_NAME: &str = "recording.log";

/// `ClusterMarkFile.MAJOR_VERSION`.
const MARK_FILE_MAJOR_VERSION: i32 = 0;
/// `MarkFileHeaderDecoder.SCHEMA_ID` and `TEMPLATE_ID`.
const MARK_SCHEMA_ID: u16 = 110;
const MARK_TEMPLATE_ID: u16 = 200;
const MARK_SCHEMA_VERSION: u16 = 2;
const MARK_BLOCK_LENGTH: u16 = 128;
/// `ClusterMarkFile.HEADER_LENGTH`.
const MARK_HEADER_LENGTH: usize = 8 * 1024;
/// `CncFileDescriptor.CNC_VERSION`, 0.2.0, and the length of its metadata.
const CNC_VERSION: i32 = 2 << 8;
const CNC_META_DATA_LENGTH: usize = 128;
const CNC_FILE: &str = "cnc.dat";

/// `ConsensusModule.State`, by its counter's value.
const MODULE_STATES: [&str; 7] = [
    "INIT",
    "ACTIVE",
    "SUSPENDED",
    "SNAPSHOT",
    "QUITTING",
    "TERMINATING",
    "CLOSED",
];
const MODULE_ACTIVE: i64 = 1;
const MODULE_SUSPENDED: i64 = 2;
/// `ClusterControl.ToggleState`, by value.
const CLUSTER_TOGGLE_STATES: [&str; 8] = [
    "INACTIVE",
    "NEUTRAL",
    "SUSPEND",
    "RESUME",
    "SNAPSHOT",
    "SHUTDOWN",
    "ABORT",
    "STANDBY_SNAPSHOT",
];
/// `NodeControl.ToggleState`, by value.
const NODE_TOGGLE_STATES: [&str; 4] = [
    "INACTIVE",
    "NEUTRAL",
    "REPLICATE_STANDBY_SNAPSHOT",
    "VALIDATE_RECORDING_LOG",
];
const NEUTRAL: i64 = 1;
const SUSPEND: i64 = 2;
const RESUME: i64 = 3;
/// `Cluster.Role.LEADER` and `ElectionState.CLOSED`.
const ROLE_LEADER: i64 = 2;
const ELECTION_CLOSED: i64 = 17;
/// `ConsensusModule.Configuration.SERVICE_ID`.
const CONSENSUS_MODULE_SERVICE_ID: i32 = -1;
const NULL_VALUE: i64 = -1;
/// `AeronArchive.NULL_LENGTH`.
const NULL_LENGTH: i64 = -1;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A failure Java's tool throws as an exception: printed, and the tool exits 1.
#[derive(Debug)]
pub struct ToolError(pub String);

impl std::fmt::Display for ToolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ToolError {}

impl From<io::Error> for ToolError {
    fn from(e: io::Error) -> Self {
        Self(e.to_string())
    }
}

impl From<AeronCError> for ToolError {
    fn from(e: AeronCError) -> Self {
        Self(e.to_string())
    }
}

impl From<AeronArchiveError> for ToolError {
    fn from(e: AeronArchiveError) -> Self {
        Self(e.to_string())
    }
}

impl From<recording_log::Error> for ToolError {
    fn from(e: recording_log::Error) -> Self {
        Self(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, ToolError>;

fn fail<T>(message: impl Into<String>) -> Result<T> {
    Err(ToolError(message.into()))
}

/// A cluster component's mark file header, as `MarkFileHeaderDecoder` reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkFileHeader {
    pub acting_version: u16,
    pub acting_block_length: u16,
    pub version: i32,
    pub component_type: i32,
    pub activity_timestamp: i64,
    pub start_timestamp: i64,
    pub pid: i64,
    pub candidate_term_id: i64,
    pub archive_stream_id: i32,
    pub service_stream_id: i32,
    pub consensus_module_stream_id: i32,
    pub ingress_stream_id: i32,
    pub member_id: i32,
    pub service_id: i32,
    pub header_length: i32,
    pub error_buffer_length: i32,
    pub cluster_id: i32,
    pub aeron_directory: String,
    pub control_channel: String,
    pub ingress_channel: String,
    pub service_name: String,
    pub authenticator: String,
    pub services_cluster_dir: String,
}

fn i32_at(bytes: &[u8], at: usize) -> Option<i32> {
    Some(i32::from_le_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}

fn i64_at(bytes: &[u8], at: usize) -> Option<i64> {
    Some(i64::from_le_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

fn u16_at(bytes: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(bytes.get(at..at + 2)?.try_into().ok()?))
}

/// `ClusterMarkFile.headerOffset`: past an SBE message header when the file
/// starts with the mark file's, at 0 for a file written before it had one.
pub fn header_offset(bytes: &[u8]) -> usize {
    if u16_at(bytes, 2) == Some(MARK_TEMPLATE_ID) && u16_at(bytes, 4) == Some(MARK_SCHEMA_ID) {
        8
    } else {
        0
    }
}

impl MarkFileHeader {
    /// The header of a mark file's bytes, as `wrapAndApplyHeader` reads it.
    pub fn read(bytes: &[u8]) -> Option<Self> {
        let offset = header_offset(bytes);
        let (acting_block_length, acting_version) = if offset == 8 {
            (u16_at(bytes, 0)?, u16_at(bytes, 6)?)
        } else {
            (MARK_BLOCK_LENGTH, MARK_SCHEMA_VERSION)
        };
        let at = |field: usize| offset + field;
        let mut var = offset + acting_block_length as usize;
        let mut string = || -> Option<String> {
            let length = u32::from_le_bytes(bytes.get(var..var + 4)?.try_into().ok()?) as usize;
            let text = bytes.get(var + 4..var + 4 + length)?;
            var += 4 + length;
            Some(String::from_utf8_lossy(text).into_owned())
        };
        let aeron_directory = string()?;
        let control_channel = string()?;
        let ingress_channel = string()?;
        let service_name = string()?;
        let authenticator = string()?;
        let services_cluster_dir = if acting_version >= 1 { string()? } else { String::new() };
        Some(Self {
            acting_version,
            acting_block_length,
            version: i32_at(bytes, at(0))?,
            component_type: i32_at(bytes, at(4))?,
            activity_timestamp: i64_at(bytes, at(8))?,
            start_timestamp: i64_at(bytes, at(16))?,
            pid: i64_at(bytes, at(24))?,
            candidate_term_id: i64_at(bytes, at(32))?,
            archive_stream_id: i32_at(bytes, at(40))?,
            service_stream_id: i32_at(bytes, at(44))?,
            consensus_module_stream_id: i32_at(bytes, at(48))?,
            ingress_stream_id: i32_at(bytes, at(52))?,
            member_id: i32_at(bytes, at(56))?,
            service_id: i32_at(bytes, at(60))?,
            header_length: i32_at(bytes, at(64))?,
            error_buffer_length: i32_at(bytes, at(68))?,
            cluster_id: i32_at(bytes, at(72))?,
            aeron_directory,
            control_channel,
            ingress_channel,
            service_name,
            authenticator,
            services_cluster_dir,
        })
    }

    /// `ClusterComponentType`'s name, as Java's enum prints.
    pub fn component_type_name(&self) -> &'static str {
        component_type_name(self.component_type)
    }
}

fn component_type_name(value: i32) -> &'static str {
    match value {
        0 => "UNKNOWN",
        1 => "CONSENSUS_MODULE",
        2 => "CONTAINER",
        3 => "BACKUP",
        4 => "STANDBY",
        _ => "NULL_VAL",
    }
}

/// As Java's generated `MarkFileHeaderDecoder.toString` prints the header.
impl std::fmt::Display for MarkFileHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[MarkFileHeader](sbeTemplateId={MARK_TEMPLATE_ID}|sbeSchemaId={MARK_SCHEMA_ID}|sbeSchemaVersion="
        )?;
        if self.acting_version != MARK_SCHEMA_VERSION {
            write!(f, "{}/", self.acting_version)?;
        }
        write!(f, "{MARK_SCHEMA_VERSION}|sbeBlockLength=")?;
        if self.acting_block_length != MARK_BLOCK_LENGTH {
            write!(f, "{}/", self.acting_block_length)?;
        }
        write!(
            f,
            "{MARK_BLOCK_LENGTH}):version={}|componentType={}|activityTimestamp={}|startTimestamp={}|pid={}|\
             candidateTermId={}|archiveStreamId={}|serviceStreamId={}|consensusModuleStreamId={}|ingressStreamId={}|\
             memberId={}|serviceId={}|headerLength={}|errorBufferLength={}|clusterId={}|aeronDirectory='{}'|\
             controlChannel='{}'|ingressChannel='{}'|serviceName='{}'|authenticator='{}'|servicesClusterDir='{}'",
            self.version,
            self.component_type_name(),
            self.activity_timestamp,
            self.start_timestamp,
            self.pid,
            self.candidate_term_id,
            self.archive_stream_id,
            self.service_stream_id,
            self.consensus_module_stream_id,
            self.ingress_stream_id,
            self.member_id,
            self.service_id,
            self.header_length,
            self.error_buffer_length,
            self.cluster_id,
            self.aeron_directory,
            self.control_channel,
            self.ingress_channel,
            self.service_name,
            self.authenticator,
            self.services_cluster_dir
        )
    }
}

/// An existing cluster mark file, as `ClusterMarkFile` opens one to read:
/// read once, its header decoded.
pub struct ClusterMarkFile {
    pub path: PathBuf,
    pub bytes: Vec<u8>,
    pub header: MarkFileHeader,
}

impl ClusterMarkFile {
    /// Agrona's `MarkFile` mapping an existing file, then `ClusterMarkFile`'s
    /// checks: it waits, up to `timeout_ms`, for the file, its version and its
    /// activity timestamp, and refuses another major version.
    pub fn open(dir: &Path, name: &str, timeout_ms: i64) -> Result<Self> {
        let path = dir.join(name);
        let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(0) as u64);
        let bytes = loop {
            match fs::read(&path) {
                Ok(bytes) if bytes.len() >= 16 => break bytes,
                _ if Instant::now() > deadline => return fail(format!("Mark file not created: {name}")),
                _ => std::thread::sleep(Duration::from_millis(16)),
            }
        };
        let offset = header_offset(&bytes);
        let mut bytes = bytes;
        loop {
            let version = i32_at(&bytes, offset).unwrap_or(0);
            if version != 0 {
                let major = (version >> 16) & 0xFF;
                if major != MARK_FILE_MAJOR_VERSION {
                    return fail(format!(
                        "mark file major version {major} does not match software: {MARK_FILE_MAJOR_VERSION}"
                    ));
                }
                break;
            }
            if Instant::now() > deadline {
                return fail("Mark file is created but not initialised");
            }
            std::thread::sleep(Duration::from_millis(1));
            bytes = fs::read(&path)?;
        }
        while i64_at(&bytes, offset + 8).unwrap_or(0) == 0 {
            if Instant::now() > deadline {
                return fail("No non-zero timestamp detected");
            }
            std::thread::sleep(Duration::from_millis(1));
            bytes = fs::read(&path)?;
        }
        let Some(header) = MarkFileHeader::read(&bytes) else {
            return fail(format!("mark file too short: {}", path.display()));
        };
        Ok(Self { path, bytes, header })
    }

    /// The activity timestamp, read now (`activityTimestampVolatile`).
    pub fn activity_timestamp_volatile(&self) -> i64 {
        fs::read(&self.path)
            .ok()
            .and_then(|bytes| i64_at(&bytes, header_offset(&bytes) + 8))
            .unwrap_or(self.header.activity_timestamp)
    }

    /// Its distinct error log, after the header.
    pub fn error_buffer(&self) -> &[u8] {
        let start = usize::try_from(self.header.header_length).unwrap_or(MARK_HEADER_LENGTH);
        let length = usize::try_from(self.header.error_buffer_length).unwrap_or(0);
        self.bytes
            .get(start..(start + length).min(self.bytes.len()))
            .unwrap_or_default()
    }
}

/// What a node's mark file says of how to reach it (`ClusterNodeControlProperties`).
#[derive(Clone, Debug)]
pub struct ClusterNodeControlProperties {
    pub member_id: i32,
    pub service_stream_id: i32,
    pub consensus_module_stream_id: i32,
    pub aeron_directory_name: String,
    pub control_channel: String,
}

impl From<&MarkFileHeader> for ClusterNodeControlProperties {
    fn from(h: &MarkFileHeader) -> Self {
        Self {
            member_id: h.member_id,
            service_stream_id: h.service_stream_id,
            consensus_module_stream_id: h.consensus_module_stream_id,
            aeron_directory_name: h.aeron_directory.clone(),
            control_channel: h.control_channel.clone(),
        }
    }
}

/// A member as `ClusterControlAdapter` builds it from a members response:
/// what Java's `ClusterMember.toString` prints of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterMember {
    pub id: i32,
    pub is_leader: bool,
    pub leadership_term_id: i64,
    pub log_position: i64,
    pub time_of_last_append_position_ns: i64,
    pub ingress_endpoint: String,
    pub consensus_endpoint: String,
    pub log_endpoint: String,
    pub catchup_endpoint: String,
    pub archive_endpoint: String,
}

impl std::fmt::Display for ClusterMember {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let endpoints = [
            self.ingress_endpoint.as_str(),
            &self.consensus_endpoint,
            &self.log_endpoint,
            &self.catchup_endpoint,
            &self.archive_endpoint,
        ]
        .join(",");
        write!(
            f,
            "ClusterMember{{id={}, isBallotSent=false, isLeader={}, leadershipTermId={}, logPosition={}, \
             candidateTermId=-1, catchupReplaySessionId=-1, correlationId=-1, timeOfLastAppendPositionNs={}, \
             ingressEndpoint='{}', consensusEndpoint='{}', logEndpoint='{}', catchupEndpoint='{}', \
             archiveEndpoint='{}', endpoints='{}', publication=null, vote=null}}",
            self.id,
            self.is_leader,
            self.leadership_term_id,
            self.log_position,
            self.time_of_last_append_position_ns,
            self.ingress_endpoint,
            self.consensus_endpoint,
            self.log_endpoint,
            self.catchup_endpoint,
            self.archive_endpoint,
            endpoints
        )
    }
}

/// What a members query found (`ClusterMembership`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterMembership {
    pub current_time_ns: i64,
    pub leader_member_id: i32,
    pub member_id: i32,
    pub active_members: Vec<ClusterMember>,
}

/// A `ClusterMembersQuery` asking for the extended response, as
/// `ConsensusModuleProxy.clusterMembersQuery` encodes it.
pub fn cluster_members_query(correlation_id: i64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(17);
    bytes.extend_from_slice(&cluster_members_query_codec::SBE_BLOCK_LENGTH.to_le_bytes());
    bytes.extend_from_slice(&cluster_members_query_codec::SBE_TEMPLATE_ID.to_le_bytes());
    bytes.extend_from_slice(&CLUSTER_SCHEMA_ID.to_le_bytes());
    bytes.extend_from_slice(&CLUSTER_SCHEMA_VERSION.to_le_bytes());
    bytes.extend_from_slice(&correlation_id.to_le_bytes());
    bytes.push(1); // BooleanType.TRUE: the extended response
    bytes.resize(8 + cluster_members_query_codec::SBE_BLOCK_LENGTH as usize, 0);
    bytes
}

/// A `ClusterMembersExtendedResponse`'s correlation id and membership, as
/// `ClusterControlAdapter` reads it; none for another message, or one cut short.
pub fn decode_cluster_members_extended_response(message: &[u8]) -> Option<(i64, ClusterMembership)> {
    let block_length = u16_at(message, 0)? as usize;
    if u16_at(message, 2)? != cluster_members_extended_response_codec::SBE_TEMPLATE_ID
        || u16_at(message, 4)? != CLUSTER_SCHEMA_ID
    {
        return None;
    }
    let body = 8;
    let correlation_id = i64_at(message, body)?;
    let mut membership = ClusterMembership {
        current_time_ns: i64_at(message, body + 8)?,
        leader_member_id: i32_at(message, body + 16)?,
        member_id: i32_at(message, body + 20)?,
        active_members: Vec::new(),
    };
    let mut at = body + block_length;
    let member_block = u16_at(message, at)? as usize;
    let count = u16_at(message, at + 2)?;
    at += 4;
    for _ in 0..count {
        let leadership_term_id = i64_at(message, at)?;
        let log_position = i64_at(message, at + 8)?;
        let time_of_last_append_ns = i64_at(message, at + 16)?;
        let id = i32_at(message, at + 24)?;
        at += member_block;
        let mut string = || -> Option<String> {
            let length = u32::from_le_bytes(message.get(at..at + 4)?.try_into().ok()?) as usize;
            let text = String::from_utf8_lossy(message.get(at + 4..at + 4 + length)?).into_owned();
            at += 4 + length;
            Some(text)
        };
        membership.active_members.push(ClusterMember {
            id,
            is_leader: id == membership.leader_member_id,
            leadership_term_id,
            log_position,
            time_of_last_append_position_ns: time_of_last_append_ns,
            ingress_endpoint: string()?,
            consensus_endpoint: string()?,
            log_endpoint: string()?,
            catchup_endpoint: string()?,
            archive_endpoint: string()?,
        });
    }
    Some((correlation_id, membership))
}

/// The line `list-members` prints.
pub fn format_membership(m: &ClusterMembership) -> String {
    let members: Vec<String> = m.active_members.iter().map(ClusterMember::to_string).collect();
    format!(
        "currentTimeNs={}, leaderMemberId={}, memberId={}, activeMembers=[{}]",
        m.current_time_ns,
        m.leader_member_id,
        m.member_id,
        members.join(", ")
    )
}

/// A local date and time, as Java's `Date` and `OffsetDateTime` print in the
/// JVM's default time zone, here the process's (`TZ`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalTime {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub millisecond: u32,
    pub utc_offset_seconds: i64,
}

impl LocalTime {
    /// `epoch_ms` in the local time zone.
    pub fn of(epoch_ms: i64) -> Self {
        let seconds = epoch_ms.div_euclid(1000);
        let millisecond = epoch_ms.rem_euclid(1000) as u32;
        let time: libc::time_t = seconds as libc::time_t;
        // SAFETY: localtime_r writes only the `tm` it is given.
        let mut tm: libc::tm = unsafe { std::mem::zeroed() };
        unsafe { libc::localtime_r(&time, &mut tm) };
        Self {
            year: tm.tm_year + 1900,
            month: (tm.tm_mon + 1) as u32,
            day: tm.tm_mday as u32,
            hour: tm.tm_hour as u32,
            minute: tm.tm_min as u32,
            second: tm.tm_sec as u32,
            millisecond,
            utc_offset_seconds: tm.tm_gmtoff as i64,
        }
    }

    /// `epoch_ms` at a fixed offset from UTC.
    pub fn at_offset(epoch_ms: i64, utc_offset_seconds: i64) -> Self {
        let local_ms = epoch_ms + utc_offset_seconds * 1000;
        let days = local_ms.div_euclid(86_400_000);
        let ms_of_day = local_ms.rem_euclid(86_400_000);
        let (year, month, day) = civil_from_days(days);
        Self {
            year,
            month,
            day,
            hour: (ms_of_day / 3_600_000) as u32,
            minute: (ms_of_day / 60_000 % 60) as u32,
            second: (ms_of_day / 1000 % 60) as u32,
            millisecond: (ms_of_day % 1000) as u32,
            utc_offset_seconds,
        }
    }

    /// `%tF`: `2026-11-24`.
    pub fn date(&self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }

    /// `%tH:%tM:%tS`: `18:05:07`.
    pub fn time(&self) -> String {
        format!("{:02}:{:02}:{:02}", self.hour, self.minute, self.second)
    }

    /// `uuuu-MM-dd HH:mm:ss.SSSSSSZ`, as `CommonContext.printErrorLog` prints a timestamp.
    pub fn error_log_timestamp(&self) -> String {
        let offset = self.utc_offset_seconds;
        let sign = if offset < 0 { '-' } else { '+' };
        let offset = offset.abs();
        format!(
            "{} {}.{:03}000{}{:02}{:02}",
            self.date(),
            self.time(),
            self.millisecond,
            sign,
            offset / 3600,
            offset / 60 % 60
        )
    }
}

/// Howard Hinnant's `civil_from_days`: the proleptic Gregorian date of a day
/// count from 1970-01-01.
fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    ((y + i64::from(m <= 2)) as i32, m, d)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// `printTypeAndActivityTimestamp`.
pub fn format_type_and_activity(component_type: &str, now: LocalTime, start: LocalTime, activity: LocalTime) -> String {
    format!(
        "Type: {component_type} {} (start: {} {}, activity: {} {})",
        now.time(),
        start.date(),
        start.time(),
        activity.date(),
        activity.time()
    )
}

/// `CommonContext.printErrorLog`, the times at `zone`: local, or a fixed offset in tests.
pub fn format_error_log(buffer: &[u8], zone: impl Fn(i64) -> LocalTime) -> String {
    let mut out = String::new();
    if distinct_error_log::has_errors(buffer) {
        let count = distinct_error_log::read(buffer, 0, |o| {
            out.push('\n');
            out.push_str(&format!(
                "{} observations from {} to {} for:\n{}\n",
                o.observation_count,
                zone(o.first_observation_timestamp_ms).error_log_timestamp(),
                zone(o.last_observation_timestamp_ms).error_log_timestamp(),
                o.encoded_error
            ));
        });
        out.push_str(&format!("\n{count} distinct errors observed.\n"));
    } else {
        out.push_str("\n0 distinct errors observed\n");
    }
    out
}

/// `ConsensusModuleSnapshotPrinter.formatHexDump`.
pub fn format_hex_dump(bytes: &[u8]) -> String {
    const WIDTH: usize = 16;
    let mut out = String::new();
    let mut row = 0;
    while row < bytes.len() {
        out.push_str(&format!("{row:06}:  "));
        for index in 0..WIDTH {
            match bytes.get(row + index) {
                Some(b) => out.push_str(&format!("{b:02x} ")),
                None => out.push_str("   "),
            }
        }
        let ascii = &bytes[row..(row + WIDTH).min(bytes.len())];
        out.push_str("  |  ");
        out.push_str(&String::from_utf8_lossy(ascii).replace("\r\n", " ").replace('\n', " "));
        out.push('\n');
        row += WIDTH;
    }
    out
}

/// What `ConsensusModuleSnapshotPrinter` prints for a message of the
/// module's snapshot: `offset` is where the message starts in its term, 0
/// for one reassembled from fragments, as Java's adapter is handed it.
pub fn format_snapshot_message(loaded: &Loaded, message: &[u8], offset: i32) -> String {
    match loaded {
        Loaded::Begin { app_version, time_unit } => format!(
            "Snapshot: appVersion={app_version} timeUnit={}\n",
            match *time_unit {
                ClusterTimeUnit::MILLIS => "MILLISECONDS",
                ClusterTimeUnit::MICROS => "MICROSECONDS",
                ClusterTimeUnit::NANOS => "NANOSECONDS",
                _ => "null",
            }
        ),
        Loaded::End => format!(
            "End Snapshot, offset={offset}, length={}\n{}\n",
            message.len(),
            format_hex_dump(message)
        ),
        Loaded::ConsensusModule {
            next_session_id,
            next_service_session_id,
            log_service_session_id,
            pending_message_capacity,
        } => format!(
            "Consensus Module State: nextSessionId={next_session_id} nextServiceSessionId={next_service_session_id} \
             logServiceSessionId={log_service_session_id} pendingMessageCapacity={pending_message_capacity}\n{}\n",
            format_hex_dump(message)
        ),
        Loaded::PendingMessage { cluster_session_id, .. } => format!(
            "Pending Message: length={} clusterSessionId={cluster_session_id}\n",
            message.len()
        ),
        Loaded::ClusterSession {
            cluster_session_id,
            correlation_id,
            opened_log_position,
            time_of_last_activity,
            close_reason,
            response_stream_id,
            response_channel,
        } => format!(
            "Cluster Session: clusterSessionId={cluster_session_id} correlationId={correlation_id} \
             openedLogPosition={opened_log_position} timeOfLastActivity={time_of_last_activity} \
             closeReason={} responseStreamId={response_stream_id} responseChannel={response_channel}\n",
            close_reason_name(*close_reason)
        ),
        Loaded::Timer {
            correlation_id,
            deadline,
        } => format!("Timer: correlationId={correlation_id} deadline={deadline}\n"),
        Loaded::PendingMessageTracker {
            next_service_session_id,
            log_service_session_id,
            pending_message_capacity,
            service_id,
        } => format!(
            "Pending Message Tracker: nextServiceSessionId={next_service_session_id} \
             logServiceSessionId={log_service_session_id} pendingMessageCapacity={pending_message_capacity} \
             serviceId={service_id}\n"
        ),
        Loaded::Ignored => String::new(),
    }
}

fn close_reason_name(reason: CloseReason) -> &'static str {
    match reason {
        CloseReason::CLIENT_ACTION => "CLIENT_ACTION",
        CloseReason::SERVICE_ACTION => "SERVICE_ACTION",
        CloseReason::TIMEOUT => "TIMEOUT",
        _ => "NULL_VAL",
    }
}

/// The operations, with the tool's settings (`ClusterToolOperator`).
pub struct ClusterToolOperator {
    tool_channel: String,
    tool_stream_id: i32,
    timeout_ms: i64,
}

impl ClusterToolOperator {
    pub fn new(tool_channel: &str, tool_stream_id: i32, timeout_ms: i64) -> Self {
        Self {
            tool_channel: tool_channel.to_owned(),
            tool_stream_id,
            timeout_ms,
        }
    }

    /// `markFileExists`.
    pub fn mark_file_exists(&self, cluster_dir: &Path) -> bool {
        resolve_cluster_mark_file_dir(cluster_dir).join(MARK_FILE_NAME).exists()
    }

    fn open_mark_file(&self, cluster_dir: &Path) -> Result<ClusterMarkFile> {
        ClusterMarkFile::open(
            &resolve_cluster_mark_file_dir(cluster_dir),
            MARK_FILE_NAME,
            self.timeout_ms,
        )
    }

    /// `pid`.
    pub fn pid(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        if self.mark_file_exists(cluster_dir) || self.timeout_ms > 0 {
            let mark = self.open_mark_file(cluster_dir)?;
            writeln!(out, "{}", mark.header.pid)?;
            Ok(SUCCESS)
        } else {
            Ok(FAILURE)
        }
    }

    /// `describeClusterMarkFile`.
    pub fn describe_cluster_mark_file(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        let services_dir = if self.mark_file_exists(cluster_dir) || self.timeout_ms > 0 {
            let mark = self.open_mark_file(cluster_dir)?;
            print_type_and_activity_timestamp(out, &mark)?;
            writeln!(out, "{}", mark.header)?;
            resolve_cluster_services_dir(cluster_dir, &mark.header)
        } else {
            writeln!(out, "{MARK_FILE_NAME} does not exist.")?;
            return Ok(FAILURE);
        };
        for mark in self.open_service_mark_files(&services_dir, out)? {
            print_type_and_activity_timestamp(out, &mark)?;
            writeln!(out, "{}", mark.header)?;
        }
        Ok(SUCCESS)
    }

    /// `recordingLog`.
    pub fn recording_log(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        writeln!(out, "{}", load_recording_log(cluster_dir)?)?;
        Ok(SUCCESS)
    }

    /// `recoveryPlan`: the archive is the one Aeron's default context reaches.
    pub fn recovery_plan(&self, cluster_dir: &Path, service_count: i32, out: &mut dyn Write) -> Result<i32> {
        let log = load_recording_log(cluster_dir)?;
        let context = AeronContext::new()?;
        context.set_client_name(&"cluster-tool".into_c_string())?;
        let aeron = Aeron::new(&context)?;
        aeron.start()?;
        let archive_context = AeronArchiveContext::new()?;
        archive_context.set_aeron(&aeron)?;
        let archive =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?.poll_blocking(CONNECT_TIMEOUT)?;
        let extent = match log.plan_recording_id(NULL_VALUE) {
            Some(recording_id) => list_recording(&archive, recording_id)?,
            None => None,
        };
        writeln!(out, "{}", log.create_recovery_plan(service_count, NULL_VALUE, extent)?)?;
        Ok(SUCCESS)
    }

    /// `sortRecordingLog`: rewritten in the order the log holds its entries
    /// in memory, if they are not already; FAILURE if they were, or it could
    /// not be read.
    pub fn sort_recording_log(&self, cluster_dir: &Path) -> Result<i32> {
        let Ok(log) = load_recording_log(cluster_dir) else {
            return Ok(FAILURE);
        };
        let entries = log.entries();
        if entries.iter().enumerate().all(|(i, e)| e.entry_index == i as i32) {
            return Ok(FAILURE);
        }
        update_recording_log(cluster_dir, entries)?;
        Ok(SUCCESS)
    }

    /// `seedRecordingLogFromSnapshot`: the log backed up to `recording.log.bak`,
    /// then replaced by the latest valid consensus module snapshot and the
    /// services' snapshots before it, their positions zeroed.
    pub fn seed_recording_log_from_snapshot(&self, cluster_dir: &Path) -> Result<i32> {
        let log = load_recording_log(cluster_dir)?;
        let entries = log.entries();
        let snapshot_index = entries
            .iter()
            .rposition(|e| is_valid_snapshot(e) && e.service_id == CONSENSUS_MODULE_SERVICE_ID);
        let file = cluster_dir.join(RECORDING_LOG_FILE_NAME);
        fs::copy(&file, cluster_dir.join(format!("{RECORDING_LOG_FILE_NAME}.bak")))?;
        let Some(snapshot_index) = snapshot_index else {
            update_recording_log(cluster_dir, &[])?;
            return Ok(SUCCESS);
        };
        let mut seeded = Vec::new();
        for (entry, service_id) in entries[..=snapshot_index]
            .iter()
            .rev()
            .zip(CONSENSUS_MODULE_SERVICE_ID..)
        {
            if !(is_valid_snapshot(entry) && entry.service_id == service_id) {
                break;
            }
            seeded.push(Entry {
                term_base_log_position: 0,
                log_position: 0,
                archive_endpoint: None,
                entry_index: NULL_VALUE as i32,
                ..entry.clone()
            });
        }
        seeded.reverse();
        update_recording_log(cluster_dir, &seeded)?;
        Ok(SUCCESS)
    }

    /// `invalidateLatestSnapshot`.
    pub fn invalidate_latest_snapshot(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        let file = cluster_dir.join(RECORDING_LOG_FILE_NAME);
        let mut log = load_recording_log(cluster_dir)?;
        let result = log.invalidate_latest_snapshot()?;
        write_at(&file, log.take_writes())?;
        writeln!(out, " invalidate latest snapshot: {result}")?;
        Ok(if result { SUCCESS } else { FAILURE })
    }

    /// `errors`: the component's error log, its driver's, then its services'.
    pub fn errors(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        let mut services_dir = cluster_dir.to_path_buf();
        if self.mark_file_exists(cluster_dir) || self.timeout_ms > 0 {
            let mark = self.open_mark_file(cluster_dir)?;
            print_type_and_activity_timestamp(out, &mark)?;
            print_errors(out, mark.error_buffer(), "Cluster")?;
            writeln!(out)?;
            print_driver_errors(out, &mark.header.aeron_directory)?;
            services_dir = resolve_cluster_services_dir(cluster_dir, &mark.header);
        } else {
            writeln!(out, "{MARK_FILE_NAME} does not exist.")?;
        }
        for mark in self.open_service_mark_files(&services_dir, out)? {
            print_type_and_activity_timestamp(out, &mark)?;
            print_errors(out, mark.error_buffer(), "Cluster")?;
        }
        Ok(SUCCESS)
    }

    /// `listMembers`.
    pub fn list_members(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        if !(self.mark_file_exists(cluster_dir) || self.timeout_ms > 0) {
            writeln!(out, "{MARK_FILE_NAME} does not exist.")?;
            return Ok(FAILURE);
        }
        let mark = self.open_mark_file(cluster_dir)?;
        let timeout_ms = self.timeout_ms.max(1_000);
        match query_cluster_members(&ClusterNodeControlProperties::from(&mark.header), timeout_ms)? {
            Some(membership) => {
                writeln!(out, "{}", format_membership(&membership))?;
                Ok(SUCCESS)
            }
            None => {
                writeln!(out, "timeout waiting for response from node")?;
                Ok(FAILURE)
            }
        }
    }

    /// `printNextBackupQuery`.
    pub fn print_next_backup_query(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        let Some(mark) = self.backup_mark_file(cluster_dir, out)? else {
            return Ok(FAILURE);
        };
        let next = backup_query_deadline(&mark.header.aeron_directory, None)?;
        let now = LocalTime::of(now_ms());
        let next = LocalTime::of(next);
        writeln!(
            out,
            "{} {} next: {} {}",
            next.date(),
            now.time(),
            next.date(),
            next.time()
        )?;
        Ok(SUCCESS)
    }

    /// `nextBackupQuery`: the next query `delay_ms` from now.
    pub fn next_backup_query(&self, cluster_dir: &Path, delay_ms: i64, out: &mut dyn Write) -> Result<i32> {
        let Some(mark) = self.backup_mark_file(cluster_dir, out)? else {
            return Ok(FAILURE);
        };
        backup_query_deadline(&mark.header.aeron_directory, Some(now_ms() + delay_ms))?;
        let next = backup_query_deadline(&mark.header.aeron_directory, None)?;
        let now = LocalTime::of(now_ms());
        let next = LocalTime::of(next);
        writeln!(
            out,
            "{} {} setting next: {} {}",
            next.date(),
            now.time(),
            next.date(),
            next.time()
        )?;
        Ok(SUCCESS)
    }

    fn backup_mark_file(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<Option<ClusterMarkFile>> {
        if !(self.mark_file_exists(cluster_dir) || self.timeout_ms > 0) {
            writeln!(out, "{MARK_FILE_NAME} does not exist.")?;
            return Ok(None);
        }
        let mark = self.open_mark_file(cluster_dir)?;
        if mark.header.component_type != 3 {
            writeln!(out, "not a cluster backup node")?;
            return Ok(None);
        }
        Ok(Some(mark))
    }

    /// `isLeader`: 0 for a leader whose election is closed, 1 otherwise, -1
    /// with no mark file.
    pub fn is_leader(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        if !self.mark_file_exists(cluster_dir) {
            writeln!(out, "{MARK_FILE_NAME} does not exist.")?;
            return Ok(-1);
        }
        let mark = self.open_mark_file(cluster_dir)?;
        let aeron = connect(&mark.header.aeron_directory)?;
        let counters = aeron.counters_reader();
        let (mut role, mut election) = (-1, -1);
        counters.foreach_counter_fn(|value, _id, type_id, _key, _label| {
            if type_id == cluster_counters::NODE_ROLE_TYPE_ID {
                role = value;
            } else if type_id == cluster_counters::ELECTION_STATE_TYPE_ID {
                election = value;
            }
        });
        Ok(if role == ROLE_LEADER && election == ELECTION_CLOSED {
            0
        } else {
            1
        })
    }

    /// `describeLatestConsensusModuleSnapshot`: the latest valid consensus
    /// module snapshot replayed from the local archive and printed.
    pub fn describe_latest_consensus_module_snapshot(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        let log = load_recording_log(cluster_dir)?;
        let Some(entry) = log.get_latest_snapshot(CONSENSUS_MODULE_SERVICE_ID).cloned() else {
            writeln!(out, "Snapshot not found")?;
            return Ok(FAILURE);
        };
        let properties = ClusterNodeControlProperties::from(&self.open_mark_file(cluster_dir)?.header);
        let aeron = connect(&properties.aeron_directory_name)?;
        let archive_context = AeronArchiveContext::new()?;
        archive_context.set_aeron(&aeron)?;
        archive_context.set_client_name(&"cluster-tool".into_c_string())?;
        archive_context.set_control_request_channel(&"aeron:ipc".into_c_string())?;
        archive_context.set_control_response_channel(&"aeron:ipc".into_c_string())?;
        let archive =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?.poll_blocking(CONNECT_TIMEOUT)?;
        let params = AeronArchiveReplayParams::builder()
            .position(0)
            .length(NULL_LENGTH)
            .build()?;
        let session_id = archive.start_replay(
            entry.recording_id,
            &self.tool_channel.as_str().into_c_string(),
            self.tool_stream_id,
            &params,
        )? as i32;
        let replay_channel = add_session_id(&self.tool_channel, session_id);
        let subscription = aeron.add_subscription(
            &replay_channel.as_str().into_c_string(),
            self.tool_stream_id,
            Handlers::NONE,
            Handlers::NONE,
            CONNECT_TIMEOUT,
        )?;
        let image = loop {
            if let Some(image) = subscription.image_by_session_id(session_id) {
                break image;
            }
            archive.check_for_error_response()?;
            std::thread::yield_now();
        };
        let mut loader = Loader::default();
        let mut reassembler = Reassembler::default();
        let mut text = String::new();
        let mut error: Option<String> = None;
        loop {
            let fragments = image.controlled_poll_fn(
                |buffer, header| {
                    let (flags, term_offset) = header
                        .get_values()
                        .map(|v| (v.frame().flags(), v.frame().term_offset()))
                        .unwrap_or((UNFRAGMENTED, 0));
                    let offset = if flags & UNFRAGMENTED == UNFRAGMENTED {
                        term_offset + FRAME_HEADER_LENGTH
                    } else {
                        0
                    };
                    let fragment = Fragment {
                        payload: buffer,
                        flags,
                        term_offset,
                        position: header.position(),
                    };
                    let action = reassembler.on_fragment(fragment, |message, _| match loader.on_message(message) {
                        Ok(loaded) => {
                            text.push_str(&format_snapshot_message(&loaded, message, offset));
                            if loaded == Loaded::End {
                                PollAction::Break
                            } else {
                                PollAction::Continue
                            }
                        }
                        Err(e) => {
                            error = Some(e);
                            PollAction::Break
                        }
                    });
                    use bindings::aeron_controlled_fragment_handler_action_en as A;
                    match action {
                        PollAction::Continue => A::AERON_ACTION_CONTINUE,
                        PollAction::Break => A::AERON_ACTION_BREAK,
                        PollAction::Abort => A::AERON_ACTION_ABORT,
                    }
                },
                10,
            )?;
            out.write_all(text.as_bytes())?;
            text.clear();
            if let Some(e) = error.take() {
                return fail(e);
            }
            if loader.is_done() {
                break;
            }
            if fragments == 0 {
                if image.is_closed() {
                    return fail(format!("snapshot ended unexpectedly: image sessionId={session_id}"));
                }
                archive.check_for_error_response()?;
                std::thread::yield_now();
            }
        }
        writeln!(
            out,
            "Consensus Module Snapshot End: memberId={} recordingId={} length={}",
            properties.member_id,
            entry.recording_id,
            image.position()
        )?;
        Ok(SUCCESS)
    }

    /// `snapshot`: on the leader, waited for.
    pub fn snapshot(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        self.toggle_cluster_state(out, cluster_dir, MODULE_ACTIVE, 4, true, 30_000)
    }

    /// `suspend`.
    pub fn suspend(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        self.toggle_cluster_state(out, cluster_dir, MODULE_ACTIVE, SUSPEND, false, 1_000)
    }

    /// `resume`, waited for.
    pub fn resume(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        self.toggle_cluster_state(out, cluster_dir, MODULE_SUSPENDED, RESUME, true, 1_000)
    }

    /// `shutdown`.
    pub fn shutdown(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        self.toggle_cluster_state(out, cluster_dir, MODULE_ACTIVE, 5, false, 1_000)
    }

    /// `abort`.
    pub fn abort(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        self.toggle_cluster_state(out, cluster_dir, MODULE_ACTIVE, 6, false, 1_000)
    }

    /// `validateRecordingLog`: the node control toggle, on any member, waited for.
    pub fn validate_recording_log(&self, cluster_dir: &Path, out: &mut dyn Write) -> Result<i32> {
        self.toggle_state(out, cluster_dir, false, MODULE_ACTIVE, Toggle::Node, 3, true, 30_000)
    }

    fn toggle_cluster_state(
        &self,
        out: &mut dyn Write,
        cluster_dir: &Path,
        expected_state: i64,
        target: i64,
        wait: bool,
        default_timeout_ms: i64,
    ) -> Result<i32> {
        self.toggle_state(
            out,
            cluster_dir,
            true,
            expected_state,
            Toggle::Cluster,
            target,
            wait,
            default_timeout_ms,
        )
    }

    /// `toggleState`: the members asked for the leader first, then the
    /// consensus module's state and the toggle read from the driver's
    /// counters, and the toggle set, as Java's tool does each step.
    #[allow(clippy::too_many_arguments)]
    fn toggle_state(
        &self,
        out: &mut dyn Write,
        cluster_dir: &Path,
        is_leader_required: bool,
        expected_state: i64,
        toggle: Toggle,
        target: i64,
        wait: bool,
        default_timeout_ms: i64,
    ) -> Result<i32> {
        if !self.mark_file_exists(cluster_dir) && self.timeout_ms <= 0 {
            writeln!(out, "{MARK_FILE_NAME} does not exist.")?;
            return Ok(FAILURE);
        }
        let mark = self.open_mark_file(cluster_dir)?;
        let cluster_id = mark.header.cluster_id;
        let properties = ClusterNodeControlProperties::from(&mark.header);
        let Some(membership) = query_cluster_members(&properties, self.timeout_ms.max(1_000))? else {
            writeln!(out, "Timed out querying cluster.")?;
            return Ok(FAILURE);
        };
        let prefix = format!("Member [{}]: ", membership.member_id);
        let target_name = toggle.name(target);
        if is_leader_required && membership.leader_member_id != membership.member_id {
            writeln!(
                out,
                "{prefix}Current node is not the leader (leaderMemberId = {}), unable to {target_name}",
                membership.leader_member_id
            )?;
            return Ok(FAILURE);
        }
        let cnc = Path::new(&properties.aeron_directory_name).join(CNC_FILE);
        if !cnc.exists() {
            writeln!(
                out,
                "{prefix}Unable to locate media driver. C`n`C file [{}] does not exist.",
                absolute(&cnc).display()
            )?;
            return Ok(FAILURE);
        }
        let aeron = connect(&properties.aeron_directory_name)?;
        let counters = aeron.counters_reader();
        let Some(module_state) = find_counter_value(&counters, cluster_counters::MODULE_STATE_TYPE_ID, cluster_id)
        else {
            writeln!(out, "{prefix}Unable to resolve state of consensus module.")?;
            return Ok(FAILURE);
        };
        if module_state != expected_state {
            writeln!(
                out,
                "{prefix}Unable to {target_name} as the state of the consensus module is {}, but needs to be {}",
                state_name(&MODULE_STATES, module_state),
                state_name(&MODULE_STATES, expected_state)
            )?;
            return Ok(FAILURE);
        }
        let Some(counter_id) = find_counter_id(&counters, toggle.type_id(), cluster_id) else {
            writeln!(out, "{prefix}Failed to find control toggle")?;
            return Ok(FAILURE);
        };
        // SAFETY: the toggle's value is an aligned i64 in the driver's counters
        // file, mapped while `aeron` is open, and changed by other processes
        // only atomically, as Java's `AtomicCounter` changes it.
        let value = unsafe { AtomicI64::from_ptr(counters.addr(counter_id)) };
        let applied = if target == RESUME && toggle == Toggle::Cluster && value.load(Ordering::Acquire) == SUSPEND {
            value
                .compare_exchange(SUSPEND, RESUME, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        } else {
            value
                .compare_exchange(NEUTRAL, target, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        };
        if !applied {
            writeln!(
                out,
                "{prefix}Failed to apply {target_name}, current toggle value = {}",
                state_name(&CLUSTER_TOGGLE_STATES, value.load(Ordering::Acquire))
            )?;
            return Ok(FAILURE);
        }
        if wait {
            let toggle_timeout_ms = default_timeout_ms.max(self.timeout_ms);
            let deadline = Instant::now() + Duration::from_millis(toggle_timeout_ms as u64);
            let mut current = None;
            loop {
                std::thread::yield_now();
                if Instant::now() > deadline {
                    break;
                }
                current = Some(value.load(Ordering::Acquire));
                if current == Some(NEUTRAL) {
                    break;
                }
            }
            if current != Some(NEUTRAL) {
                writeln!(
                    out,
                    "{prefix}Timed out after {toggle_timeout_ms}ms waiting for {target_name} to complete."
                )?;
                return Ok(FAILURE);
            }
        }
        writeln!(out, "{prefix}{target_name} applied successfully")?;
        Ok(SUCCESS)
    }

    /// `openServiceMarkFiles`: the services' mark files in `dir`, a link file
    /// taking the place of the mark file of the same name.
    fn open_service_mark_files(&self, dir: &Path, out: &mut dyn Write) -> Result<Vec<ClusterMarkFile>> {
        let names: Vec<String> = fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .filter(|name| {
                        name.starts_with(SERVICE_FILENAME_PREFIX)
                            && (name.ends_with(FILE_EXTENSION) || name.ends_with(LINK_FILE_EXTENSION))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut resolved: Vec<PathBuf> = Vec::new();
        let mut services = std::collections::HashSet::new();
        for name in &names {
            if let Some(stem) = name.strip_suffix(LINK_FILE_EXTENSION) {
                let target = resolve_directory_from_link_file(&dir.join(name))?;
                resolved.push(target.join(format!("{stem}{FILE_EXTENSION}")));
                services.insert(stem.to_owned());
            }
        }
        for name in &names {
            if let Some(stem) = name.strip_suffix(FILE_EXTENSION)
                && services.insert(stem.to_owned())
            {
                resolved.push(dir.join(name));
            }
        }
        // Each opened with the tool's output as Agrona's `MarkFile` logger, which notes an existing file.
        resolved
            .iter()
            .map(|path| {
                if path.exists() {
                    writeln!(out, "INFO: mark file exists: {}", path.display())?;
                }
                let parent = path.parent().unwrap_or(Path::new("."));
                let name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();
                ClusterMarkFile::open(parent, &name, self.timeout_ms)
            })
            .collect()
    }
}

/// Which control toggle, and the names of its states.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Toggle {
    /// `ClusterControl`'s.
    Cluster,
    /// `NodeControl`'s.
    Node,
}

impl Toggle {
    fn type_id(self) -> i32 {
        match self {
            Toggle::Cluster => cluster_counters::CONTROL_TOGGLE_TYPE_ID,
            Toggle::Node => cluster_counters::NODE_CONTROL_TOGGLE_TYPE_ID,
        }
    }

    fn name(self, state: i64) -> String {
        match self {
            Toggle::Cluster => state_name(&CLUSTER_TOGGLE_STATES, state),
            Toggle::Node => state_name(&NODE_TOGGLE_STATES, state),
        }
    }
}

fn state_name(names: &[&str], value: i64) -> String {
    usize::try_from(value)
        .ok()
        .and_then(|i| names.get(i))
        .map_or_else(|| value.to_string(), |n| (*n).to_owned())
}

fn is_valid_snapshot(entry: &Entry) -> bool {
    entry.is_valid && entry.entry_type == recording_log::ENTRY_TYPE_SNAPSHOT
}

fn absolute(path: &Path) -> PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
}

/// `resolveClusterMarkFileDir`: the directory a `cluster-mark.lnk` names, else `dir`.
pub fn resolve_cluster_mark_file_dir(dir: &Path) -> PathBuf {
    let link = dir.join(LINK_FILE_NAME);
    if link.exists() {
        resolve_directory_from_link_file(&link).unwrap_or_else(|_| dir.to_path_buf())
    } else {
        dir.to_path_buf()
    }
}

fn resolve_directory_from_link_file(link: &Path) -> Result<PathBuf> {
    match fs::read(link) {
        Ok(bytes) => Ok(PathBuf::from(String::from_utf8_lossy(&bytes).trim())),
        Err(e) => fail(format!("failed to read link file={}: {e}", link.display())),
    }
}

/// `resolveClusterServicesDir`: the services' directory the mark file names, else the cluster's.
fn resolve_cluster_services_dir(cluster_dir: &Path, header: &MarkFileHeader) -> PathBuf {
    if header.acting_version >= 1 && !header.services_cluster_dir.is_empty() {
        PathBuf::from(&header.services_cluster_dir)
    } else {
        cluster_dir.to_path_buf()
    }
}

fn print_type_and_activity_timestamp(out: &mut dyn Write, mark: &ClusterMarkFile) -> Result<()> {
    writeln!(
        out,
        "{}",
        format_type_and_activity(
            mark.header.component_type_name(),
            LocalTime::of(now_ms()),
            LocalTime::of(mark.header.start_timestamp),
            LocalTime::of(mark.activity_timestamp_volatile()),
        )
    )?;
    Ok(())
}

fn print_errors(out: &mut dyn Write, buffer: &[u8], name: &str) -> Result<()> {
    writeln!(out, "{name} component error log:")?;
    out.write_all(format_error_log(buffer, LocalTime::of).as_bytes())?;
    Ok(())
}

/// `printDriverErrors`: the error log in the driver's CnC file.
fn print_driver_errors(out: &mut dyn Write, aeron_directory: &str) -> Result<()> {
    writeln!(out, "Aeron driver error log (directory: {aeron_directory}):")?;
    let cnc = fs::read(Path::new(aeron_directory).join(CNC_FILE))?;
    let field = |at: usize| i32_at(&cnc, at).unwrap_or(0);
    let version = field(0);
    if (version >> 16) & 0xFF != (CNC_VERSION >> 16) & 0xFF {
        return fail(format!(
            "CnC version not compatible: app={} file={}",
            semantic_version(CNC_VERSION),
            semantic_version(version)
        ));
    }
    let offset = CNC_META_DATA_LENGTH + [4, 8, 12, 16].iter().map(|&at| field(at) as usize).sum::<usize>();
    let length = field(20) as usize;
    let buffer = cnc.get(offset..(offset + length).min(cnc.len())).unwrap_or_default();
    out.write_all(format_error_log(buffer, LocalTime::of).as_bytes())?;
    Ok(())
}

fn semantic_version(version: i32) -> String {
    format!(
        "{}.{}.{}",
        (version >> 16) & 0xFF,
        (version >> 8) & 0xFF,
        version & 0xFF
    )
}

fn load_recording_log(cluster_dir: &Path) -> Result<RecordingLog> {
    let file = cluster_dir.join(RECORDING_LOG_FILE_NAME);
    match fs::read(&file) {
        Ok(bytes) => Ok(RecordingLog::load(&bytes)?),
        Err(e) => fail(format!("failed to read recording log {}: {e}", file.display())),
    }
}

/// `updateRecordingLog`: `entries` written to a temporary file that then
/// replaces the log; the log removed when there are none.
fn update_recording_log(cluster_dir: &Path, entries: &[Entry]) -> Result<()> {
    let file = cluster_dir.join(RECORDING_LOG_FILE_NAME);
    if entries.is_empty() {
        fs::remove_file(&file)?;
        return Ok(());
    }
    let temp = cluster_dir.join(format!("{RECORDING_LOG_FILE_NAME}.tmp"));
    let _ = fs::remove_file(&temp);
    let bytes: Vec<u8> = entries.iter().flat_map(Entry::encode).collect();
    fs::write(&temp, bytes)?;
    fs::remove_file(&file)?;
    fs::rename(&temp, &file)?;
    Ok(())
}

fn write_at(file: &Path, writes: Vec<(u64, Vec<u8>)>) -> Result<()> {
    use std::os::unix::fs::FileExt;
    let f = fs::OpenOptions::new().write(true).open(file)?;
    for (position, bytes) in writes {
        f.write_all_at(&bytes, position)?;
    }
    f.sync_data()?;
    Ok(())
}

fn connect(aeron_directory: &str) -> Result<Aeron> {
    let context = AeronContext::new()?;
    context.set_dir(&aeron_directory.into_c_string())?;
    context.set_client_name(&"cluster-tool".into_c_string())?;
    let aeron = Aeron::new(&context)?;
    aeron.start()?;
    Ok(aeron)
}

/// The id of the counter of `type_id` whose key starts with `cluster_id`
/// (`ClusterCounters.find`).
fn find_counter_id(counters: &AeronCountersReaderRef, type_id: i32, cluster_id: i32) -> Option<i32> {
    let mut found = None;
    counters.foreach_counter_fn(|_value, id, t, key, _label| {
        if found.is_none() && t == type_id && key.get(..4) == Some(&cluster_id.to_le_bytes()[..]) {
            found = Some(id);
        }
    });
    found
}

fn find_counter_value(counters: &AeronCountersReaderRef, type_id: i32, cluster_id: i32) -> Option<i64> {
    find_counter_id(counters, type_id, cluster_id).and_then(|id| counters.get_counter_value(id).ok())
}

/// The backup's next query deadline (`nextBackupQueryDeadlineMs`), set to
/// `set` first when given, as `setRelease` sets it.
fn backup_query_deadline(aeron_directory: &str, set: Option<i64>) -> Result<i64> {
    let aeron = connect(aeron_directory)?;
    let counters = aeron.counters_reader();
    let mut found = Vec::new();
    counters.foreach_counter_fn(|_value, id, type_id, _key, _label| {
        if type_id == cluster_counters::BACKUP_QUERY_DEADLINE_TYPE_ID {
            found.push(id);
        }
    });
    let mut next = NULL_VALUE;
    for id in found {
        // SAFETY: as in `toggle_state`: an aligned i64 counter value, mapped while `aeron` is open.
        let value = unsafe { AtomicI64::from_ptr(counters.addr(id)) };
        if let Some(ms) = set {
            value.store(ms, Ordering::Release);
        }
        next = value.load(Ordering::Acquire);
    }
    Ok(next)
}

/// `queryClusterMembers`: a members query on the node's control channel, its
/// extended response awaited; none on a timeout.
fn query_cluster_members(
    properties: &ClusterNodeControlProperties,
    timeout_ms: i64,
) -> Result<Option<ClusterMembership>> {
    let aeron = connect(&properties.aeron_directory_name)?;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms as u64);
    let channel = properties.control_channel.as_str().into_c_string();
    let subscription = aeron.add_subscription(
        &channel,
        properties.service_stream_id,
        Handlers::NONE,
        Handlers::NONE,
        CONNECT_TIMEOUT,
    )?;
    let publication = aeron.add_publication(&channel, properties.consensus_module_stream_id, CONNECT_TIMEOUT)?;
    let correlation_id = aeron.next_correlation_id();
    while !subscription.is_connected() || !publication.is_connected() {
        if Instant::now() > deadline {
            return Ok(None);
        }
        std::thread::yield_now();
    }
    let query = cluster_members_query(correlation_id);
    while publication.offer(&query).is_err() {
        if Instant::now() > deadline {
            return Ok(None);
        }
        std::thread::yield_now();
    }
    let mut reassembler = Reassembler::default();
    let mut membership = None;
    while membership.is_none() {
        let fragments = subscription.poll_fn(
            |buffer, header| {
                let (flags, term_offset) = header
                    .get_values()
                    .map(|v| (v.frame().flags(), v.frame().term_offset()))
                    .unwrap_or((UNFRAGMENTED, 0));
                let fragment = Fragment {
                    payload: buffer,
                    flags,
                    term_offset,
                    position: header.position(),
                };
                reassembler.on_fragment(fragment, |message, _| {
                    if let Some((id, m)) = decode_cluster_members_extended_response(message)
                        && id == correlation_id
                    {
                        membership = Some(m);
                    }
                    PollAction::Continue
                });
            },
            10,
        )?;
        if fragments == 0 && membership.is_none() {
            if Instant::now() > deadline {
                return Ok(None);
            }
            std::thread::yield_now();
        }
    }
    Ok(membership)
}

/// The archive's description of a recording, as a recovery plan needs it.
fn list_recording(archive: &AeronArchive, recording_id: i64) -> Result<Option<RecordingExtent>> {
    let mut count = 0;
    let mut extent = None;
    archive.list_recording_fn(&mut count, recording_id, |d: &AeronArchiveRecordingDescriptor<_>| {
        extent = Some(RecordingExtent {
            start_position: d.start_position(),
            stop_position: d.stop_position(),
            initial_term_id: d.initial_term_id(),
            term_buffer_length: d.term_buffer_length(),
            mtu_length: d.mtu_length(),
            session_id: d.session_id(),
        });
    })?;
    Ok(extent)
}

/// `ChannelUri.addSessionId`.
fn add_session_id(channel: &str, session_id: i32) -> String {
    let separator = if channel.contains('?') { '|' } else { '?' };
    format!("{channel}{separator}session-id={session_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_print_as_java_prints_them() {
        let t = LocalTime::at_offset(1_764_000_000_123, 3_600);
        assert_eq!(t.date(), "2025-11-24");
        assert_eq!(t.time(), "17:00:00");
        assert_eq!(t.error_log_timestamp(), "2025-11-24 17:00:00.123000+0100");
        assert_eq!(
            LocalTime::at_offset(0, -5 * 3_600 - 1_800).error_log_timestamp(),
            "1969-12-31 18:30:00.000000-0530"
        );
    }

    #[test]
    fn an_empty_error_log_prints_as_java_does() {
        assert_eq!(
            format_error_log(&[0u8; 64], |ms| LocalTime::at_offset(ms, 0)),
            "\n0 distinct errors observed\n"
        );
    }

    #[test]
    fn a_hex_dump_is_java_s() {
        let dump = format_hex_dump(b"0123456789abcdefXY\n");
        assert_eq!(
            dump,
            "000000:  30 31 32 33 34 35 36 37 38 39 61 62 63 64 65 66   |  0123456789abcdef\n\
             000016:  58 59 0a                                          |  XY \n"
        );
    }

    #[test]
    fn a_member_prints_as_java_s_cluster_member() {
        let member = ClusterMember {
            id: 1,
            is_leader: true,
            leadership_term_id: 3,
            log_position: 1_024,
            time_of_last_append_position_ns: 77,
            ingress_endpoint: "h:1".into(),
            consensus_endpoint: "h:2".into(),
            log_endpoint: "h:3".into(),
            catchup_endpoint: "h:4".into(),
            archive_endpoint: "h:5".into(),
        };
        assert_eq!(
            member.to_string(),
            "ClusterMember{id=1, isBallotSent=false, isLeader=true, leadershipTermId=3, logPosition=1024, \
             candidateTermId=-1, catchupReplaySessionId=-1, correlationId=-1, timeOfLastAppendPositionNs=77, \
             ingressEndpoint='h:1', consensusEndpoint='h:2', logEndpoint='h:3', catchupEndpoint='h:4', \
             archiveEndpoint='h:5', endpoints='h:1,h:2,h:3,h:4,h:5', publication=null, vote=null}"
        );
    }

    #[test]
    fn a_members_response_decodes() {
        let members = crate::cluster_member::parse("0,a:1,a:2,a:3,a:4,a:5").unwrap();
        let response = crate::service_proxy::cluster_members_extended_response(9, 42, 0, 0, &members);
        let (id, m) = decode_cluster_members_extended_response(&response).expect("decodes");
        assert_eq!((id, m.current_time_ns, m.leader_member_id, m.member_id), (9, 42, 0, 0));
        assert_eq!(m.active_members.len(), 1);
        assert_eq!(m.active_members[0].archive_endpoint, "a:5");
        assert!(m.active_members[0].is_leader);
        let query = cluster_members_query(5);
        assert_eq!(&query[2..4], &34u16.to_le_bytes());
        assert_eq!(query.len(), 8 + cluster_members_query_codec::SBE_BLOCK_LENGTH as usize);
        assert_eq!(query[16], 1, "the extended response asked for");
    }

    #[test]
    fn a_session_id_joins_the_channel() {
        assert_eq!(add_session_id("aeron:ipc", 7), "aeron:ipc?session-id=7");
        assert_eq!(
            add_session_id("aeron:udp?endpoint=h:1", 7),
            "aeron:udp?endpoint=h:1|session-id=7"
        );
    }
}
