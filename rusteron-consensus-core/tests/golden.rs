//! What backups and standbys send a member, and its answers, against bytes
//! Aeron 1.54.2's Java codecs wrote (`rusteron-cluster-codecs/java/GoldenCodecs.java`):
//! the module decodes Java's messages to the values Java set, and encodes its
//! answers to Java's bytes. A message cut short anywhere is refused, never
//! read past its end.

use rusteron_consensus_core::consensus::{
    self, BackupResponse, BackupSnapshot, ConsensusMessage, StandbySnapshotEntry, decode, decode_backup_response,
};

const RESPONSE_CHANNEL: &str = "aeron:udp?endpoint=localhost:20121";
const MEMBERS: &str = "0,localhost:20110,localhost:20111,localhost:20112,localhost:20113,localhost:20114|";
const PROTOCOL_VERSION: i32 = 1 << 16;

fn fixture(name: &str) -> Vec<u8> {
    let path = format!(
        "{}/../rusteron-cluster-codecs/tests/fixtures/{name}.hex",
        env!("CARGO_MANIFEST_DIR")
    );
    let hex = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let hex = hex.trim();
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex"))
        .collect()
}

/// Every proper prefix of the message refused.
fn refuses_cut_short(bytes: &[u8]) {
    for end in 0..bytes.len() {
        assert!(decode(&bytes[..end]).is_err(), "{end} of {} bytes decoded", bytes.len());
    }
}

/// Every proper prefix of a request refused but the one leaving out its
/// closing credentials, which Java reads as none.
fn refuses_cut_short_but_without_credentials(bytes: &[u8], credentials_length: usize) {
    let without = bytes.len() - 4 - credentials_length;
    for end in 0..bytes.len() {
        assert_eq!(
            decode(&bytes[..end]).is_ok(),
            end == without,
            "{end} of {} bytes",
            bytes.len()
        );
    }
}

fn standby_entries() -> Vec<StandbySnapshotEntry> {
    [(21, 0), (22, -1)]
        .into_iter()
        .map(|(recording_id, service_id)| StandbySnapshotEntry {
            recording_id,
            leadership_term_id: 4,
            term_base_log_position: 6_144,
            log_position: 7_168,
            timestamp: 1_700_000_006_000,
            service_id,
            archive_endpoint: "localhost:21301".into(),
        })
        .collect()
}

#[test]
fn backup_query() {
    let bytes = fixture("backup_query");
    assert_eq!(
        decode(&bytes),
        Ok(ConsensusMessage::BackupQuery {
            correlation_id: 55,
            response_stream_id: 108,
            version: PROTOCOL_VERSION,
            log_position: i64::MAX,
            response_channel: RESPONSE_CHANNEL.into(),
            encoded_credentials: vec![6, 7],
        })
    );
    refuses_cut_short_but_without_credentials(&bytes, 2);
    assert_eq!(
        consensus::backup_query(55, 108, PROTOCOL_VERSION, i64::MAX, RESPONSE_CHANNEL, &[6, 7]),
        bytes,
        "Java's bytes, credentials included"
    );
    assert_eq!(
        decode(&consensus::backup_query(
            55,
            108,
            PROTOCOL_VERSION,
            i64::MAX,
            RESPONSE_CHANNEL,
            &[]
        )),
        Ok(ConsensusMessage::BackupQuery {
            correlation_id: 55,
            response_stream_id: 108,
            version: PROTOCOL_VERSION,
            log_position: i64::MAX,
            response_channel: RESPONSE_CHANNEL.into(),
            encoded_credentials: vec![],
        })
    );
}

#[test]
fn backup_response() {
    let bytes = fixture("backup_response");
    let snapshot = |recording_id, service_id| BackupSnapshot {
        recording_id,
        leadership_term_id: 3,
        term_base_log_position: 4_096,
        log_position: 8_192,
        timestamp: 1_700_000_005_000,
        service_id,
    };
    let response = BackupResponse {
        correlation_id: 56,
        log_recording_id: 2,
        log_leadership_term_id: 3,
        log_term_base_log_position: 4_096,
        last_leadership_term_id: 3,
        last_term_base_log_position: 4_096,
        commit_position_counter_id: 71,
        leader_member_id: 1,
        member_id: 2,
        snapshots: vec![snapshot(8, 0), snapshot(9, -1)],
        cluster_members: MEMBERS.into(),
    };
    assert_eq!(consensus::backup_response(&response), bytes);
    assert_eq!(decode_backup_response(&bytes), Ok(response));
    for end in 0..bytes.len() {
        assert!(decode_backup_response(&bytes[..end]).is_err(), "{end} bytes decoded");
    }
}

#[test]
fn heartbeats() {
    let request = fixture("heartbeat_request");
    assert_eq!(
        decode(&request),
        Ok(ConsensusMessage::HeartbeatRequest {
            correlation_id: 57,
            response_stream_id: 108,
            response_channel: RESPONSE_CHANNEL.into(),
            encoded_credentials: vec![],
        })
    );
    refuses_cut_short_but_without_credentials(&request, 0);
    assert_eq!(consensus::heartbeat_request(57, 108, RESPONSE_CHANNEL), request);
    assert_eq!(consensus::heartbeat_response(57), fixture("heartbeat_response"));
}

#[test]
fn standby_snapshot() {
    let bytes = fixture("standby_snapshot");
    assert_eq!(
        decode(&bytes),
        Ok(ConsensusMessage::StandbySnapshot {
            correlation_id: 58,
            version: PROTOCOL_VERSION,
            response_stream_id: 111,
            snapshots: standby_entries(),
            response_channel: RESPONSE_CHANNEL.into(),
            encoded_credentials: vec![],
        })
    );
    refuses_cut_short_but_without_credentials(&bytes, 0);
    assert_eq!(
        consensus::standby_snapshot(58, PROTOCOL_VERSION, 111, &standby_entries(), RESPONSE_CHANNEL),
        bytes
    );
}

#[test]
fn challenge_response() {
    let bytes = fixture("challenge_response");
    assert_eq!(
        decode(&bytes),
        Ok(ConsensusMessage::ChallengeResponse {
            correlation_id: 59,
            cluster_session_id: -1,
        })
    );
    // Its credentials are not read: cut short within its header and block only.
    refuses_cut_short(&bytes[..8 + 16]);
}
