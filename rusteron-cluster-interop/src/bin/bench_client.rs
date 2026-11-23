//! `java/BenchClient.java` on the Rust client: offers a 32-byte message at a
//! fixed rate, each stamped with when it was due, and times its echo.
//!
//! ```text
//! bench_client AERON_DIR PORT_BASE NODES RATE WARMUP_S MEASURE_S REPORT NAME
//! ```
//!
//! Writes the report line `BenchService.reportLine` writes (count and
//! percentiles in µs) to REPORT, prints it with what was sent and received,
//! and exits 0 when every message came back.

use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use rusteron_cluster::client::{AeronCluster, ClientContext, EgressListener};
use rusteron_cluster_interop::{bench::report_line, ingress_endpoints};

struct Echoes {
    epoch: Instant,
    histogram: Histogram<u64>,
    received: u64,
}

impl Echoes {
    fn now_ns(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }
}

impl EgressListener for Echoes {
    fn on_message(&mut self, _cluster_session_id: i64, _timestamp: i64, payload: &[u8]) {
        let now = self.now_ns();
        self.received += 1;
        if payload.len() >= 9 && payload[8] == 1 {
            let due = u64::from_le_bytes(payload[..8].try_into().expect("8 bytes"));
            let latency = now.saturating_sub(due).min(self.histogram.high());
            let _ = self.histogram.record(latency.max(1));
        }
    }
}

fn usage() -> ! {
    eprintln!("usage: bench_client AERON_DIR PORT_BASE NODES RATE WARMUP_S MEASURE_S REPORT NAME");
    std::process::exit(2);
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 8 {
        usage();
    }
    let number = |i: usize| args[i].parse::<u64>().unwrap_or_else(|_| usage());
    let (port_base, nodes, rate) = (number(1) as u16, number(2) as u16, number(3));
    let (warmup, measure) = (Duration::from_secs(number(4)), Duration::from_secs(number(5)));
    let (report, name) = (&args[6], &args[7]);

    let mut ctx = ClientContext::new(args[0].clone());
    ctx.config.egress_channel = "aeron:udp?endpoint=localhost:0".into();
    ctx.config.ingress_channel = "aeron:udp?term-length=64k".into();
    ctx.config.ingress_endpoints = Some(ingress_endpoints(port_base, nodes));
    ctx.config.message_timeout_ns = 10_000_000_000;
    let mut cluster = AeronCluster::connect(ctx).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(1);
    });
    let mut echoes = Echoes {
        epoch: Instant::now(),
        histogram: Histogram::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds"),
        received: 0,
    };
    let poll = |cluster: &mut AeronCluster, echoes: &mut Echoes| {
        if let Err(e) = cluster.poll_egress(echoes) {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    let interval_ns = 1_000_000_000 / rate.max(1);
    let start = echoes.now_ns();
    let measure_from = start + warmup.as_nanos() as u64;
    let end = measure_from + measure.as_nanos() as u64;
    let mut due = start;
    let mut sent = 0u64;
    let mut message = [0u8; 32];
    while due < end {
        if echoes.now_ns() >= due {
            message[..8].copy_from_slice(&due.to_le_bytes());
            message[8] = u8::from(due >= measure_from);
            if cluster.offer(&message) > 0 {
                sent += 1;
                due += interval_ns;
            }
        }
        poll(&mut cluster, &mut echoes);
    }
    let drain = Instant::now() + Duration::from_secs(10);
    while echoes.received < sent && Instant::now() < drain {
        poll(&mut cluster, &mut echoes);
    }
    cluster.close();

    let line = report_line(name, &echoes.histogram);
    std::fs::write(report, format!("{line}\n")).unwrap_or_else(|e| {
        eprintln!("{report}: {e}");
        std::process::exit(1);
    });
    println!("{line} (sent {sent}, received {})", echoes.received);
    std::process::exit(if echoes.received == sent { 0 } else { 1 });
}
