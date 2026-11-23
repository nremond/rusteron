//! A cluster component's mark file, as Java's `ClusterMarkFile` lays it out:
//! an 8 KiB header (an SBE message header, then `MarkFileHeader`), then a
//! distinct error log, the whole rounded up to the file page size.
//!
//! This module builds the bytes; writing them, and keeping the activity
//! timestamp fresh, is the I/O shell's job. Readers (Java's `ClusterTool`, a
//! container about to start over this one) read the version and the activity
//! timestamp with volatile loads: the version is written last when the header
//! is new, and zeroed when the component stops.

use rusteron_cluster_codecs::mark::{
    WriteBuf, cluster_component_type::ClusterComponentType, mark_file_header_codec::MarkFileHeaderEncoder,
    message_header_codec,
};

/// The mark file's own version, 0.3.0: what Java's `ClusterMarkFile.SEMANTIC_VERSION` writes.
pub const SEMANTIC_VERSION: i32 = 3 << 8;
/// What a component that failed to start writes as the version.
pub const VERSION_FAILED: i32 = -1;
/// Where the error log starts.
pub const HEADER_LENGTH: usize = 8 * 1024;
/// The smallest error log Java accepts.
pub const ERROR_BUFFER_MIN_LENGTH: usize = 1024 * 1024;
/// How often a running component refreshes its activity timestamp.
pub const UPDATE_INTERVAL_MS: i64 = 1_000;
/// How old an activity timestamp must be before the component counts as gone.
pub const LIVENESS_TIMEOUT_MS: i64 = 10 * UPDATE_INTERVAL_MS;

const HEADER_OFFSET: usize = message_header_codec::ENCODED_LENGTH;
/// Offset of the version in the file.
pub const VERSION_OFFSET: usize = HEADER_OFFSET;
/// Offset of the activity timestamp in the file.
pub const ACTIVITY_TIMESTAMP_OFFSET: usize = HEADER_OFFSET + 8;
/// Offset of the candidate term id in the file.
pub const CANDIDATE_TERM_ID_OFFSET: usize = HEADER_OFFSET + 32;
/// Offset of the member id in the file.
pub const MEMBER_ID_OFFSET: usize = HEADER_OFFSET + 56;

/// The mark file a consensus module keeps.
pub const FILE_NAME: &str = "cluster-mark.dat";
/// The link file naming the consensus module's mark file directory when it
/// is not the cluster's.
pub const LINK_FILE_NAME: &str = "cluster-mark.lnk";
/// The authenticator a consensus module without one names: Java's default.
pub const DEFAULT_AUTHENTICATOR: &str = "io.aeron.security.DefaultAuthenticatorSupplier";

/// The mark file a clustered service's container keeps: `cluster-mark-service-<id>.dat`.
pub fn service_file_name(service_id: i32) -> String {
    format!("cluster-mark-service-{service_id}.dat")
}

/// The link file naming the mark file's directory when it is not the cluster's.
pub fn service_link_file_name(service_id: i32) -> String {
    format!("cluster-mark-service-{service_id}.lnk")
}

/// The file's length: header and error log, rounded up to the page size.
pub fn file_length(error_buffer_length: usize, file_page_size: usize) -> usize {
    (HEADER_LENGTH + error_buffer_length).div_ceil(file_page_size) * file_page_size
}

/// What a service container's mark file says of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerHeader {
    pub start_timestamp_ms: i64,
    pub pid: i64,
    pub archive_stream_id: i32,
    pub service_stream_id: i32,
    pub consensus_module_stream_id: i32,
    pub service_id: i32,
    pub cluster_id: i32,
    pub error_buffer_length: i32,
    pub aeron_directory: String,
    pub control_channel: String,
    pub service_name: String,
}

/// The header too long for the 8 KiB Java leaves it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeaderTooLong(pub usize);

impl std::fmt::Display for HeaderTooLong {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mark file header of {} bytes exceeds {HEADER_LENGTH}", self.0)
    }
}

impl std::error::Error for HeaderTooLong {}

impl ContainerHeader {
    /// The header's bytes, with the version 0 and no activity yet: the
    /// writer sets the activity timestamp, then the version, last.
    pub fn encode(&self) -> Result<Vec<u8>, HeaderTooLong> {
        encode(&Fields {
            component_type: ClusterComponentType::CONTAINER,
            start_timestamp_ms: self.start_timestamp_ms,
            pid: self.pid,
            candidate_term_id: -1,
            archive_stream_id: self.archive_stream_id,
            service_stream_id: self.service_stream_id,
            consensus_module_stream_id: self.consensus_module_stream_id,
            ingress_stream_id: -1,
            member_id: -1,
            service_id: self.service_id,
            error_buffer_length: self.error_buffer_length,
            cluster_id: self.cluster_id,
            strings: [
                &self.aeron_directory,
                &self.control_channel,
                "",
                &self.service_name,
                "",
                "",
            ],
        })
    }
}

/// What a consensus module's mark file says of it, as
/// `ConsensusModule.Context.concludeMarkFile` fills it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsensusModuleHeader {
    pub start_timestamp_ms: i64,
    pub pid: i64,
    /// What the previous mark file held, or -1: the node state file takes it
    /// over once.
    pub candidate_term_id: i64,
    pub archive_stream_id: i32,
    pub service_stream_id: i32,
    pub consensus_module_stream_id: i32,
    pub ingress_stream_id: i32,
    pub member_id: i32,
    pub cluster_id: i32,
    pub error_buffer_length: i32,
    pub aeron_directory: String,
    pub control_channel: String,
    pub ingress_channel: String,
    /// The authenticator's class name, Java's default's when none.
    pub authenticator: String,
    /// Where the services' mark files are.
    pub services_cluster_dir: String,
}

impl ConsensusModuleHeader {
    /// The header's bytes, with the version 0 and no activity yet.
    pub fn encode(&self) -> Result<Vec<u8>, HeaderTooLong> {
        encode(&Fields {
            component_type: ClusterComponentType::CONSENSUS_MODULE,
            start_timestamp_ms: self.start_timestamp_ms,
            pid: self.pid,
            candidate_term_id: self.candidate_term_id,
            archive_stream_id: self.archive_stream_id,
            service_stream_id: self.service_stream_id,
            consensus_module_stream_id: self.consensus_module_stream_id,
            ingress_stream_id: self.ingress_stream_id,
            member_id: self.member_id,
            service_id: -1,
            error_buffer_length: self.error_buffer_length,
            cluster_id: self.cluster_id,
            strings: [
                &self.aeron_directory,
                &self.control_channel,
                &self.ingress_channel,
                "",
                &self.authenticator,
                &self.services_cluster_dir,
            ],
        })
    }
}

/// What a cluster backup's mark file says of it, as
/// `ClusterBackup.Context.concludeMarkFile` fills it: the service streams
/// and ingress stream Java's defaults, no member, no channels.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupHeader {
    pub start_timestamp_ms: i64,
    pub pid: i64,
    /// The local archive's control request stream.
    pub archive_stream_id: i32,
    pub cluster_id: i32,
    pub error_buffer_length: i32,
    pub aeron_directory: String,
}

impl BackupHeader {
    /// The header's bytes, with the version 0 and no activity yet.
    pub fn encode(&self) -> Result<Vec<u8>, HeaderTooLong> {
        encode(&Fields {
            component_type: ClusterComponentType::BACKUP,
            start_timestamp_ms: self.start_timestamp_ms,
            pid: self.pid,
            candidate_term_id: -1,
            archive_stream_id: self.archive_stream_id,
            service_stream_id: 104,
            consensus_module_stream_id: 105,
            ingress_stream_id: 101,
            member_id: -1,
            service_id: -1,
            error_buffer_length: self.error_buffer_length,
            cluster_id: self.cluster_id,
            strings: [&self.aeron_directory, "", "", "", "", ""],
        })
    }
}

/// Every field of a mark file's header: its strings the aeron directory,
/// control channel, ingress channel, service name, authenticator and
/// services' directory.
struct Fields<'a> {
    component_type: ClusterComponentType,
    start_timestamp_ms: i64,
    pid: i64,
    candidate_term_id: i64,
    archive_stream_id: i32,
    service_stream_id: i32,
    consensus_module_stream_id: i32,
    ingress_stream_id: i32,
    member_id: i32,
    service_id: i32,
    error_buffer_length: i32,
    cluster_id: i32,
    strings: [&'a str; 6],
}

fn encode(f: &Fields) -> Result<Vec<u8>, HeaderTooLong> {
    let length = HEADER_OFFSET + 128 + f.strings.iter().map(|s| 4 + s.len()).sum::<usize>();
    if length > HEADER_LENGTH {
        return Err(HeaderTooLong(length));
    }
    let mut bytes = vec![0; length];
    let mut e = MarkFileHeaderEncoder::default()
        .wrap(WriteBuf::new(&mut bytes), HEADER_OFFSET)
        .header(0)
        .parent()
        .expect("a header always has its parent");
    e.version(0)
        .component_type(f.component_type)
        .activity_timestamp(-1)
        .start_timestamp(f.start_timestamp_ms)
        .pid(f.pid)
        .candidate_term_id(f.candidate_term_id)
        .archive_stream_id(f.archive_stream_id)
        .service_stream_id(f.service_stream_id)
        .consensus_module_stream_id(f.consensus_module_stream_id)
        .ingress_stream_id(f.ingress_stream_id)
        .member_id(f.member_id)
        .service_id(f.service_id)
        .header_length(HEADER_LENGTH as i32)
        .error_buffer_length(f.error_buffer_length)
        .cluster_id(f.cluster_id)
        .aeron_directory(f.strings[0].as_bytes())
        .control_channel(f.strings[1].as_bytes())
        .ingress_channel(f.strings[2].as_bytes())
        .service_name(f.strings[3].as_bytes())
        .authenticator(f.strings[4].as_bytes())
        .services_cluster_dir(f.strings[5].as_bytes());
    Ok(bytes)
}

/// One error in a distinct error log (Agrona's `DistinctErrorLog`): the same
/// error observed again bumps its count and last timestamp in place.
pub mod error_log {
    /// Offset of a record's length, written last.
    pub const LENGTH_OFFSET: usize = 0;
    /// Offset of how often the error was seen.
    pub const OBSERVATION_COUNT_OFFSET: usize = 4;
    /// Offset of when it was last seen, ms since the epoch.
    pub const LAST_OBSERVATION_TIMESTAMP_OFFSET: usize = 8;
    /// Offset of when it was first seen.
    pub const FIRST_OBSERVATION_TIMESTAMP_OFFSET: usize = 16;
    /// Offset of the error's text, ASCII.
    pub const ENCODED_ERROR_OFFSET: usize = 24;
    /// Records start on multiples of this.
    pub const RECORD_ALIGNMENT: usize = 8;

    /// A new record's bytes, its length field still 0: write them, then the
    /// length (`record_length`) at [`LENGTH_OFFSET`], so a reader never sees a
    /// record half written. Non-ASCII characters become `?`.
    pub fn new_record(text: &str, timestamp_ms: i64) -> Vec<u8> {
        let mut record = vec![0; ENCODED_ERROR_OFFSET];
        record[OBSERVATION_COUNT_OFFSET..OBSERVATION_COUNT_OFFSET + 4].copy_from_slice(&1i32.to_le_bytes());
        record[LAST_OBSERVATION_TIMESTAMP_OFFSET..LAST_OBSERVATION_TIMESTAMP_OFFSET + 8]
            .copy_from_slice(&timestamp_ms.to_le_bytes());
        record[FIRST_OBSERVATION_TIMESTAMP_OFFSET..FIRST_OBSERVATION_TIMESTAMP_OFFSET + 8]
            .copy_from_slice(&timestamp_ms.to_le_bytes());
        record.extend(text.chars().map(|c| if c.is_ascii() { c as u8 } else { b'?' }));
        record
    }

    /// What goes at [`LENGTH_OFFSET`] once the record is in place.
    pub fn record_length(record: &[u8]) -> i32 {
        record.len() as i32
    }

    /// The space a record takes before the next one starts.
    pub fn aligned_length(record: &[u8]) -> usize {
        record.len().div_ceil(RECORD_ALIGNMENT) * RECORD_ALIGNMENT
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusteron_cluster_codecs::mark::{
        ReadBuf, mark_file_header_codec::MarkFileHeaderDecoder, message_header_codec::MessageHeaderDecoder,
    };

    fn header() -> ContainerHeader {
        ContainerHeader {
            start_timestamp_ms: 1_700_000_000_000,
            pid: 4_242,
            archive_stream_id: 10,
            service_stream_id: 104,
            consensus_module_stream_id: 105,
            service_id: 0,
            cluster_id: 0,
            error_buffer_length: ERROR_BUFFER_MIN_LENGTH as i32,
            aeron_directory: "/tmp/aeron".into(),
            control_channel: "aeron:ipc?term-length=128k".into(),
            service_name: "rusteron-service".into(),
        }
    }

    #[test]
    fn the_header_decodes_as_java_writes_it() {
        let bytes = header().encode().expect("fits");
        let d =
            MarkFileHeaderDecoder::default().header(MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0), 0);
        assert_eq!(d.version(), 0, "the version comes last");
        assert_eq!(d.component_type(), ClusterComponentType::CONTAINER);
        assert_eq!(d.member_id(), -1);
        assert_eq!(d.header_length(), 8_192);
        assert_eq!(d.service_stream_id(), 104);
        assert_eq!(
            i64::from_le_bytes(bytes[ACTIVITY_TIMESTAMP_OFFSET..][..8].try_into().unwrap()),
            -1
        );
        assert_eq!(
            i32::from_le_bytes(bytes[MEMBER_ID_OFFSET..][..4].try_into().unwrap()),
            -1
        );
        assert_eq!(
            i64::from_le_bytes(bytes[CANDIDATE_TERM_ID_OFFSET..][..8].try_into().unwrap()),
            -1
        );
    }

    #[test]
    fn the_consensus_modules_header_names_its_member_and_ingress() {
        let bytes = ConsensusModuleHeader {
            start_timestamp_ms: 1_700_000_000_000,
            pid: 4_343,
            candidate_term_id: 6,
            archive_stream_id: 10,
            service_stream_id: 104,
            consensus_module_stream_id: 105,
            ingress_stream_id: 101,
            member_id: 1,
            cluster_id: 3,
            error_buffer_length: ERROR_BUFFER_MIN_LENGTH as i32,
            aeron_directory: "/tmp/aeron-node1".into(),
            control_channel: "aeron:ipc?term-length=128k".into(),
            ingress_channel: "aeron:udp?term-length=64k".into(),
            authenticator: DEFAULT_AUTHENTICATOR.into(),
            services_cluster_dir: "/tmp/cluster1".into(),
        }
        .encode()
        .expect("fits");
        let mut d =
            MarkFileHeaderDecoder::default().header(MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0), 0);
        assert_eq!(d.component_type(), ClusterComponentType::CONSENSUS_MODULE);
        assert_eq!((d.member_id(), d.service_id(), d.cluster_id()), (1, -1, 3));
        assert_eq!(d.candidate_term_id(), 6);
        let dir = d.aeron_directory_decoder();
        assert_eq!(d.aeron_directory_slice(dir), b"/tmp/aeron-node1");
        let control = d.control_channel_decoder();
        assert_eq!(d.control_channel_slice(control), b"aeron:ipc?term-length=128k");
        let ingress = d.ingress_channel_decoder();
        assert_eq!(d.ingress_channel_slice(ingress), b"aeron:udp?term-length=64k");
        let name = d.service_name_decoder();
        assert_eq!(d.service_name_slice(name), b"");
        let authenticator = d.authenticator_decoder();
        assert_eq!(d.authenticator_slice(authenticator), DEFAULT_AUTHENTICATOR.as_bytes());
    }

    #[test]
    fn a_header_too_long_is_refused() {
        let mut h = header();
        h.aeron_directory = "x".repeat(HEADER_LENGTH);
        assert!(h.encode().is_err());
    }

    #[test]
    fn the_file_rounds_up_to_its_page() {
        assert_eq!(file_length(ERROR_BUFFER_MIN_LENGTH, 4_096), 8_192 + 1_048_576);
        assert_eq!(file_length(1_000_000, 4_096), 1_011_712);
        assert_eq!(service_file_name(3), "cluster-mark-service-3.dat");
        assert_eq!(SEMANTIC_VERSION, 0x00_03_00);
    }

    #[test]
    fn an_error_record_is_its_text_after_24_bytes() {
        let record = error_log::new_record("boom é", 5);
        assert_eq!(&record[24..], b"boom ?");
        assert_eq!(error_log::record_length(&record), 30);
        assert_eq!(error_log::aligned_length(&record), 32);
        assert_eq!(i32::from_le_bytes(record[4..8].try_into().unwrap()), 1);
        assert_eq!(
            i32::from_le_bytes(record[0..4].try_into().unwrap()),
            0,
            "the length is written last"
        );
    }
}
