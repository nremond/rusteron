//! The Rust twin of `java/OracleService.java`: the same logic, so that the
//! two, as services of one log, write the same trace, line for line.
//!
//! - A client's 8-byte message is echoed and counted. Every 10th schedules a
//!   timer one tick later, its id the message's position × 2 + the service
//!   id; every 25th sends a service message.
//! - `close` closes the session.
//! - Every entry applied is a trace line, and folds into a hash.
//! - The snapshot is the count and the hash, 16 bytes.

use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::rc::Rc;

use rusteron_cluster::*;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

pub fn fnv(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .fold(FNV_OFFSET, |h, &b| (h ^ b as u64).wrapping_mul(FNV_PRIME))
}

pub struct TestService {
    trace: File,
    count: i64,
    hash: u64,
    loaded: Option<Rc<RefCell<Vec<u8>>>>,
}

impl TestService {
    pub fn new(trace_path: &str) -> std::io::Result<Self> {
        let trace = OpenOptions::new().create(true).append(true).open(trace_path)?;
        Ok(Self {
            trace,
            count: 0,
            hash: 0,
            loaded: None,
        })
    }

    pub fn count(&self) -> i64 {
        self.count
    }

    fn line(&mut self, line: String) {
        self.hash = (self.hash ^ fnv(line.as_bytes())).wrapping_mul(FNV_PRIME);
        let _ = writeln!(self.trace, "{line} {:x}", self.hash);
    }
}

fn close_code(reason: CloseReason) -> i32 {
    match reason {
        CloseReason::ClientAction => 0,
        CloseReason::ServiceAction => 1,
        CloseReason::Timeout => 2,
        CloseReason::Unknown => -1,
    }
}

impl ClusteredService for TestService {
    fn snapshot_sink(&mut self, _: &SnapshotInfo) -> Box<dyn SnapshotSink> {
        let (sink, bytes) = vec_sink();
        self.loaded = Some(bytes);
        sink
    }

    fn on_start(&mut self, cx: &ReadCx<'_>, snapshot: Option<&SnapshotInfo>) -> Result<(), String> {
        if snapshot.is_some() {
            let bytes = self.loaded.take().ok_or("no snapshot sink")?.borrow().clone();
            if bytes.len() != 16 {
                return Err(format!("snapshot of {} bytes, expected 16", bytes.len()));
            }
            self.count = i64::from_le_bytes(bytes[0..8].try_into().expect("8 bytes"));
            self.hash = u64::from_le_bytes(bytes[8..16].try_into().expect("8 bytes"));
        }
        self.line(format!("start {}", cx.log_position()));
        Ok(())
    }

    fn on_session_open(&mut self, cx: &mut Cx<'_>, session: &ClientSession, timestamp: i64) {
        self.line(format!("open {} {timestamp} {}", cx.log_position(), session.id));
    }

    fn on_session_close(&mut self, cx: &mut Cx<'_>, session: &ClientSession, timestamp: i64, reason: CloseReason) {
        self.line(format!(
            "close {} {timestamp} {} {}",
            cx.log_position(),
            session.id,
            close_code(reason)
        ));
    }

    fn on_session_message(&mut self, cx: &mut Cx<'_>, m: SessionMessage<'_>) -> Apply {
        let from = m.session_id.map_or_else(|| "service".to_string(), |id| id.to_string());
        self.line(format!(
            "msg {} {} {from} {:x}",
            m.log_position,
            m.timestamp,
            fnv(m.payload)
        ));
        let Some(session_id) = m.session_id else {
            return Apply::Done;
        };
        if m.payload == b"close" {
            let _ = cx.close_session(session_id);
            return Apply::Done;
        }
        if m.payload.len() == 8 {
            cx.egress(session_id, m.payload);
            self.count += 1;
            let service_id = cx.service_id() as i64;
            if self.count % 10 == 0 {
                cx.schedule_timer(m.log_position * 2 + service_id, m.timestamp + 1);
            }
            if self.count % 25 == 0 {
                let _ = cx.service_message(format!("svc{service_id}:{}", m.log_position).as_bytes());
            }
        }
        Apply::Done
    }

    fn on_timer_event(&mut self, cx: &mut Cx<'_>, correlation_id: i64, timestamp: i64) -> Apply {
        self.line(format!("timer {} {timestamp} {correlation_id}", cx.log_position()));
        Apply::Done
    }

    fn on_take_snapshot(&mut self, _: &ReadCx<'_>, _: SnapshotPoint) -> Box<dyn SnapshotSource> {
        let mut state = self.count.to_le_bytes().to_vec();
        state.extend_from_slice(&self.hash.to_le_bytes());
        Box::new(BytesSource::new(state))
    }

    fn on_new_leadership_term(&mut self, _: &mut Cx<'_>, term: &NewLeadershipTerm) {
        self.line(format!(
            "term {} {} {}",
            term.leadership_term_id, term.log_position, term.timestamp
        ));
    }

    fn on_terminate(&mut self, _: &ReadCx<'_>) {
        let _ = self.trace.flush();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn fnv_is_fnv_1a_64() {
        assert_eq!(super::fnv(b""), 0xcbf29ce484222325);
        assert_eq!(super::fnv(b"a"), 0xaf63dc4c8601ec8c);
    }
}
