//! `java/EchoClient.java` on the Rust client: connects to the cluster's
//! ingress endpoints, sends COUNT 8-byte sequence numbers (FIRST before them,
//! if given), and checks the services echo each once, in order.
//!
//! ```text
//! echo_client AERON_DIR PORT_BASE NODES COUNT [FIRST]
//! ```
//!
//! Prints `OK|FAILED received=N outOfOrder=N leader=N` and exits 0 when every
//! echo came back in order, 1 otherwise; a connect that times out prints
//! `cluster connect timeout: ...` first, as Java's exception says it.

use std::time::{Duration, Instant};

use rusteron_cluster::client::io::{AeronCluster, ClientContext, EgressListener};
use rusteron_cluster_interop::ingress_endpoints;

#[derive(Default)]
struct Echoes {
    received: i64,
    out_of_order: i64,
}

impl EgressListener for Echoes {
    fn on_message(&mut self, _cluster_session_id: i64, _timestamp: i64, payload: &[u8]) {
        let echoed = payload
            .get(..8)
            .map(|b| i64::from_le_bytes(b.try_into().expect("8 bytes")));
        if echoed != Some(self.received) {
            self.out_of_order += 1;
        }
        self.received += 1;
    }
}

fn usage() -> ! {
    eprintln!("usage: echo_client AERON_DIR PORT_BASE NODES COUNT [FIRST]");
    std::process::exit(2);
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 4 {
        usage();
    }
    let number = |i: usize| args[i].parse::<i64>().unwrap_or_else(|_| usage());
    let (port_base, nodes, count) = (number(1) as u16, number(2) as u16, number(3));
    let first = args.get(4);

    let mut ctx = ClientContext::new(args[0].clone());
    ctx.config.egress_channel = "aeron:udp?endpoint=localhost:0".into();
    ctx.config.ingress_channel = "aeron:udp?term-length=64k".into();
    ctx.config.ingress_endpoints = Some(ingress_endpoints(port_base, nodes));
    ctx.config.message_timeout_ns = 10_000_000_000;
    let mut cluster = match AeronCluster::connect(ctx) {
        Ok(cluster) => cluster,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    let mut echoes = Echoes::default();
    let deadline = Instant::now() + Duration::from_secs(30);
    let poll = |cluster: &mut AeronCluster, echoes: &mut Echoes| match cluster.poll_egress(echoes) {
        Ok(work) => work,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    if let Some(first) = first {
        while cluster.offer(first.as_bytes()) < 0 {
            let work = poll(&mut cluster, &mut echoes);
            cluster.idle(work);
        }
    }
    let mut sent = 0;
    while sent < count {
        if cluster.offer(&sent.to_le_bytes()) > 0 {
            sent += 1;
        }
        poll(&mut cluster, &mut echoes);
    }
    while echoes.received < count && Instant::now() < deadline {
        let work = poll(&mut cluster, &mut echoes);
        cluster.idle(work);
    }
    let ok = echoes.received == count && echoes.out_of_order == 0;
    println!(
        "{} received={} outOfOrder={} leader={}",
        if ok { "OK" } else { "FAILED" },
        echoes.received,
        echoes.out_of_order,
        cluster.leader_member_id()
    );
    cluster.close();
    std::process::exit(if ok { 0 } else { 1 });
}
