//! What a client sends and reads, against bytes Aeron 1.53.3's Java codecs
//! wrote (`rusteron-cluster/java/GoldenCodecs.java`): the client's
//! encoders write Java's bytes, its egress decoder reads Java's values, and a
//! message cut short anywhere is refused.

use rusteron_cluster::client::codec::{self, Egress, PROTOCOL_SEMANTIC_VERSION};
use rusteron_cluster::client::{AdminRequestType, AdminResponseCode, EventCode};

const RESPONSE_CHANNEL: &str = "aeron:udp?endpoint=localhost:20121";

fn fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}.hex", env!("CARGO_MANIFEST_DIR"));
    let hex = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let hex = hex.trim();
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex"))
        .collect()
}

/// Every proper prefix refused, never read past.
fn refuses_cut_short(bytes: &[u8]) {
    for end in 0..bytes.len() {
        assert!(
            codec::decode_egress(&bytes[..end]).is_err(),
            "{end} of {} bytes decoded",
            bytes.len()
        );
    }
}

#[test]
fn the_client_writes_javas_bytes() {
    assert_eq!(
        codec::session_connect_request(
            47,
            102,
            PROTOCOL_SEMANTIC_VERSION,
            RESPONSE_CHANNEL,
            &[4, 5],
            "name=bench"
        ),
        fixture("session_connect_request")
    );
    assert_eq!(codec::session_close_request(15, 16), fixture("session_close_request"));
    assert_eq!(codec::session_keep_alive(17, 18), fixture("session_keep_alive"));
    assert_eq!(
        codec::admin_request(21, 22, 48, AdminRequestType::SNAPSHOT),
        fixture("admin_request")
    );
    // Java's fixture has a timestamp; the client's header leaves it 0, which the cluster ignores.
    let mut java = fixture("session_message_header");
    java[24..32].copy_from_slice(&0i64.to_le_bytes());
    assert_eq!(codec::session_message_header(5, 9).as_slice(), java.as_slice());
    // 0.3.0: major, minor and patch in bytes 2, 1 and 0.
    assert_eq!(PROTOCOL_SEMANTIC_VERSION, 3 << 8);
}

#[test]
fn the_client_reads_javas_session_event() {
    let bytes = fixture("session_event");
    assert_eq!(
        codec::decode_egress(&bytes),
        Ok(Egress::SessionEvent {
            cluster_session_id: 13,
            correlation_id: 46,
            leadership_term_id: 14,
            leader_member_id: 1,
            code: EventCode::REDIRECT,
            version: PROTOCOL_SEMANTIC_VERSION,
            leader_heartbeat_timeout_ns: 10_000_000_000,
            detail: "0=localhost:20110,1=localhost:20210".into(),
        })
    );
    refuses_cut_short(&bytes);
}

#[test]
fn the_client_reads_javas_new_leader_challenge_and_admin_response() {
    let bytes = fixture("new_leader_event");
    assert_eq!(
        codec::decode_egress(&bytes),
        Ok(Egress::NewLeader {
            leadership_term_id: 19,
            cluster_session_id: 20,
            leader_member_id: 2,
            ingress_endpoints: "0=localhost:20110,1=localhost:20210,2=localhost:20310".into(),
        })
    );
    refuses_cut_short(&bytes);

    let bytes = fixture("challenge");
    assert_eq!(
        codec::decode_egress(&bytes),
        Ok(Egress::Challenge {
            correlation_id: 51,
            cluster_session_id: 24,
            encoded_challenge: &[7, 8, 9],
        })
    );
    refuses_cut_short(&bytes);

    let bytes = fixture("admin_response");
    assert_eq!(
        codec::decode_egress(&bytes),
        Ok(Egress::AdminResponse {
            cluster_session_id: 23,
            correlation_id: 49,
            request_type: AdminRequestType::SNAPSHOT,
            response_code: AdminResponseCode::UNAUTHORISED_ACCESS,
            message: "Execution of the SNAPSHOT request was not authorised".into(),
            payload: &[],
        })
    );
    refuses_cut_short(&bytes);
}

#[test]
fn the_client_reads_a_message_after_its_session_header() {
    let mut bytes = fixture("session_message_header");
    bytes.extend_from_slice(b"answer");
    assert_eq!(
        codec::decode_egress(&bytes),
        Ok(Egress::Message {
            leadership_term_id: 5,
            cluster_session_id: 9,
            timestamp: 111,
            payload: b"answer",
        })
    );
    refuses_cut_short(&fixture("session_message_header"));
}

#[test]
fn other_templates_and_schemas_are_named_not_read() {
    assert_eq!(
        codec::decode_egress(&fixture("session_close_request")),
        Ok(Egress::Other { template_id: 4 })
    );
    let mut foreign = fixture("session_keep_alive");
    foreign[4..6].copy_from_slice(&7001u16.to_le_bytes());
    assert_eq!(codec::decode_egress(&foreign), Ok(Egress::Foreign { schema_id: 7001 }));
}
