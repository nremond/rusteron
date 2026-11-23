//! rusteron's cluster client driven a line at a time, for Aeron's own Java
//! tests (`aeron-tests`, `RustClient`): each command on stdin is one call of
//! the client's, each answer and each egress callback a line on stdout.
//! Payloads and free text travel as hex.
//!
//! ```text
//! connect aeron_dir=D ingress_channel=C ingress_endpoints=E|- egress_channel=C message_timeout_ms=N
//!         new_leader_timeout_ms=N|-1 credentials=HEX|- challenge_response=HEX|-
//!                       connected SESSION LEADER TERM | error KIND MESSAGE_HEX
//! offer HEX             offer RESULT
//! keepalive             keepalive true|false | error KIND MESSAGE_HEX
//! admin CORRELATION     admin true|false | error KIND MESSAGE_HEX
//! poll                  callbacks, then polled WORK STATE | error KIND MESSAGE_HEX:
//!                         message SESSION TIMESTAMP HEX
//!                         event CORRELATION SESSION TERM LEADER CODE DETAIL_HEX
//!                         new-leader SESSION TERM LEADER ENDPOINTS
//!                         admin-response SESSION CORRELATION TYPE CODE MESSAGE_HEX PAYLOAD_HEX
//! state                 state STATE SESSION LEADER TERM INGRESS_CHANNEL|- INGRESS_CONNECTED
//! close                 closed
//! ```

use std::io::{BufRead, Write};

use rusteron_cluster::client::{
    AdminRequestType, AdminResponseCode, AeronCluster, ClientContext, ClientError, CredentialsSupplier, EgressListener,
    EventCode,
};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Vec<u8> {
    if text == "-" {
        return Vec::new();
    }
    (0..text.len() / 2)
        .map(|i| u8::from_str_radix(&text[2 * i..2 * i + 2], 16).unwrap_or(0))
        .collect()
}

fn error(e: &ClientError) -> String {
    let kind = match e {
        ClientError::Timeout { .. } => "timeout",
        ClientError::Cluster(_) => "cluster",
        ClientError::Authentication(_) => "authentication",
        ClientError::Configuration(_) => "configuration",
        ClientError::Registration(_) => "registration",
    };
    format!("error {kind} {}", hex(e.to_string().as_bytes()))
}

struct Credentials {
    credentials: Vec<u8>,
    challenge_response: Vec<u8>,
}

impl CredentialsSupplier for Credentials {
    fn encoded_credentials(&mut self) -> Vec<u8> {
        self.credentials.clone()
    }

    fn on_challenge(&mut self, _encoded_challenge: &[u8]) -> Vec<u8> {
        self.challenge_response.clone()
    }
}

/// The callbacks of a poll, as lines.
#[derive(Default)]
struct Lines(Vec<String>);

impl EgressListener for Lines {
    fn on_message(&mut self, cluster_session_id: i64, timestamp: i64, payload: &[u8]) {
        self.0
            .push(format!("message {cluster_session_id} {timestamp} {}", hex(payload)));
    }

    fn on_session_event(
        &mut self,
        correlation_id: i64,
        cluster_session_id: i64,
        leadership_term_id: i64,
        leader_member_id: i32,
        code: EventCode,
        detail: &str,
    ) {
        self.0.push(format!(
            "event {correlation_id} {cluster_session_id} {leadership_term_id} {leader_member_id} {code:?} {}",
            hex(detail.as_bytes())
        ));
    }

    fn on_new_leader(
        &mut self,
        cluster_session_id: i64,
        leadership_term_id: i64,
        leader_member_id: i32,
        ingress_endpoints: &str,
    ) {
        self.0.push(format!(
            "new-leader {cluster_session_id} {leadership_term_id} {leader_member_id} {}",
            if ingress_endpoints.is_empty() {
                "-"
            } else {
                ingress_endpoints
            }
        ));
    }

    fn on_admin_response(
        &mut self,
        cluster_session_id: i64,
        correlation_id: i64,
        request_type: AdminRequestType,
        response_code: AdminResponseCode,
        message: &str,
        payload: &[u8],
    ) {
        self.0.push(format!(
            "admin-response {cluster_session_id} {correlation_id} {request_type:?} {response_code:?} {} {}",
            hex(message.as_bytes()),
            hex(payload)
        ));
    }
}

fn connect(arguments: &[&str]) -> Result<AeronCluster, ClientError> {
    let value = |key: &str| {
        arguments
            .iter()
            .find_map(|a| a.strip_prefix(key).and_then(|a| a.strip_prefix('=')))
            .unwrap_or("-")
    };
    let mut ctx = ClientContext::new(value("aeron_dir"));
    ctx.config.ingress_channel = value("ingress_channel").into();
    ctx.config.ingress_endpoints = Some(value("ingress_endpoints")).filter(|e| *e != "-").map(String::from);
    ctx.config.egress_channel = value("egress_channel").into();
    if let Ok(ms) = value("message_timeout_ms").parse::<i64>() {
        ctx.config.message_timeout_ns = ms * 1_000_000;
    }
    if let Ok(ms) = value("new_leader_timeout_ms").parse::<i64>()
        && ms > 0
    {
        ctx.config.new_leader_timeout_ns = ms * 1_000_000;
    }
    let credentials = Credentials {
        credentials: unhex(value("credentials")),
        challenge_response: unhex(value("challenge_response")),
    };
    AeronCluster::connect_with_credentials(ctx, credentials)
}

fn main() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    let mut cluster: Option<AeronCluster> = None;
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let words: Vec<&str> = line.split_whitespace().collect();
        let Some((&command, arguments)) = words.split_first() else {
            continue;
        };
        let mut out = Vec::new();
        match (command, cluster.as_mut()) {
            ("connect", _) => {
                cluster = None;
                match connect(arguments) {
                    Ok(c) => {
                        out.push(format!(
                            "connected {} {} {}",
                            c.cluster_session_id(),
                            c.leader_member_id(),
                            c.leadership_term_id()
                        ));
                        cluster = Some(c);
                    }
                    Err(e) => out.push(error(&e)),
                }
            }
            ("offer", Some(c)) => {
                let payload = unhex(arguments.first().copied().unwrap_or("-"));
                out.push(format!("offer {}", c.offer(&payload)));
            }
            ("keepalive", Some(c)) => out.push(match c.send_keep_alive() {
                Ok(sent) => format!("keepalive {sent}"),
                Err(e) => error(&e),
            }),
            ("admin", Some(c)) => {
                let correlation_id = arguments.first().and_then(|a| a.parse().ok()).unwrap_or(0);
                out.push(match c.send_admin_request_to_take_a_snapshot(correlation_id) {
                    Ok(sent) => format!("admin {sent}"),
                    Err(e) => error(&e),
                });
            }
            ("poll", Some(c)) => {
                let mut lines = Lines::default();
                let polled = c.poll_egress(&mut lines);
                out.append(&mut lines.0);
                out.push(match polled {
                    Ok(work) => format!("polled {work} {:?}", c.state()),
                    Err(e) => error(&e),
                });
            }
            ("state", Some(c)) => out.push(format!(
                "state {:?} {} {} {} {} {}",
                c.state(),
                c.cluster_session_id(),
                c.leader_member_id(),
                c.leadership_term_id(),
                c.ingress_channel().unwrap_or_else(|| "-".into()),
                c.is_ingress_connected()
            )),
            ("close", c) => {
                if let Some(c) = c {
                    c.close();
                }
                out.push("closed".into());
            }
            ("quit", _) => break,
            (_, None) => out.push(format!("error cluster {}", hex(b"not connected"))),
            (other, Some(_)) => out.push(format!("error configuration {}", hex(other.as_bytes()))),
        }
        for line in out {
            let _ = writeln!(stdout, "{line}");
        }
        let _ = stdout.flush();
    }
}
