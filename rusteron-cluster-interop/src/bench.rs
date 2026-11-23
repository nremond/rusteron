//! The Rust twin of `java/BenchService.java`, for measuring the container:
//! every client message is echoed, and the time from its log timestamp (the
//! consensus module's nanosecond cluster clock, stamped as it appended the
//! message) to its apply is recorded, for messages the client marked as
//! measured. At termination the histogram goes to a report file, one line:
//!
//! ```text
//! NAME COUNT P50 P90 P99 P99.9 P99.99 MAX
//! ```
//!
//! in microseconds. Both clocks read CLOCK_REALTIME on one machine: the
//! consensus module's through Agrona's `HighResolutionClock`, this one through
//! `SystemTime`.

use std::time::{SystemTime, UNIX_EPOCH};

use hdrhistogram::Histogram;
use rusteron_cluster::*;

/// A measured message's flag: byte 8 of its payload.
pub const MEASURED: u8 = 1;

pub struct BenchService {
    name: String,
    report: String,
    histogram: Histogram<u64>,
}

impl BenchService {
    pub fn new(name: impl Into<String>, report: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            report: report.into(),
            histogram: Histogram::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds"),
        }
    }
}

fn epoch_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// One report line: count and percentiles in microseconds.
pub fn report_line(name: &str, histogram: &Histogram<u64>) -> String {
    let us = |v: u64| v as f64 / 1_000.0;
    format!(
        "{name} {} {:.1} {:.1} {:.1} {:.1} {:.1} {:.1}",
        histogram.len(),
        us(histogram.value_at_quantile(0.5)),
        us(histogram.value_at_quantile(0.9)),
        us(histogram.value_at_quantile(0.99)),
        us(histogram.value_at_quantile(0.999)),
        us(histogram.value_at_quantile(0.9999)),
        us(histogram.max()),
    )
}

impl ClusteredService for BenchService {
    fn snapshot_sink(&mut self, _: &SnapshotInfo) -> Box<dyn SnapshotSink> {
        vec_sink().0
    }

    fn on_start(&mut self, _: &ReadCx<'_>, _: Option<&SnapshotInfo>) -> Result<(), String> {
        Ok(())
    }

    fn on_session_open(&mut self, _: &mut Cx<'_>, _: &ClientSession, _: i64) {}

    fn on_session_close(&mut self, _: &mut Cx<'_>, _: &ClientSession, _: i64, _: CloseReason) {}

    fn on_session_message(&mut self, cx: &mut Cx<'_>, m: SessionMessage<'_>) -> Apply {
        let now = epoch_nanos();
        if m.payload.get(8) == Some(&MEASURED) {
            self.histogram
                .saturating_record(now.saturating_sub(m.timestamp).max(0) as u64);
        }
        if let Some(session_id) = m.session_id {
            cx.egress(session_id, m.payload);
        }
        Apply::Done
    }

    fn on_timer_event(&mut self, _: &mut Cx<'_>, _: i64, _: i64) -> Apply {
        Apply::Done
    }

    fn on_take_snapshot(&mut self, _: &ReadCx<'_>, _: SnapshotPoint) -> Box<dyn SnapshotSource> {
        Box::new(BytesSource::new(Vec::new()))
    }

    fn on_terminate(&mut self, _: &ReadCx<'_>) {
        let line = report_line(&self.name, &self.histogram);
        if let Err(e) = std::fs::write(&self.report, format!("{line}\n")) {
            eprintln!("report {}: {e}", self.report);
        }
    }
}
