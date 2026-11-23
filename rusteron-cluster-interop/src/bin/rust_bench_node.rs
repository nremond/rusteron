//! A Rust service container running the bench service against a Java
//! consensus module, busy-spinning:
//!
//! ```text
//! rust_bench_node AERON_DIR CLUSTER_DIR SERVICE_ID REPORT_PATH [silent]
//! ```
//!
//! `silent` answers no client. Exits 0 when the cluster terminated it, 1 when
//! it stopped for anything else, 2 when it could not start.

use rusteron_cluster::{ContainerContext, Exit, Idle, ServiceContainer};
use rusteron_cluster_interop::bench::BenchService;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!("usage: rust_bench_node AERON_DIR CLUSTER_DIR SERVICE_ID REPORT_PATH [silent]");
        std::process::exit(2);
    }
    let mut ctx = ContainerContext::new(args[1].as_str(), args[2].as_str());
    ctx.config.service_id = args[3].parse().expect("a service id");
    ctx.config.responding = args.get(5).map(String::as_str) != Some("silent");
    ctx.idle = Idle::Spin;
    let service = BenchService::new("rust-apply", args[4].as_str());
    let container = match ServiceContainer::launch(ctx, service) {
        Ok(container) => container,
        Err(e) => {
            eprintln!("failed to launch: {e}");
            std::process::exit(2);
        }
    };
    println!("READY service={}", args[3]);
    let exit = container.run();
    println!("EXIT {exit:?}");
    std::process::exit(if exit == Exit::Terminated { 0 } else { 1 });
}
