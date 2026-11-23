//! A Rust service container running the test service against a Java
//! consensus module:
//!
//! ```text
//! rust_service_node AERON_DIR CLUSTER_DIR SERVICE_ID TRACE_PATH [silent]
//! ```
//!
//! `silent` answers no client. Exits 0 when the cluster terminated it, 1 when
//! it stopped for anything else, 2 when it could not start.

use rusteron_cluster::{ContainerContext, Exit, ServiceContainer};
use rusteron_cluster_interop::TestService;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!("usage: rust_service_node AERON_DIR CLUSTER_DIR SERVICE_ID TRACE_PATH [silent]");
        std::process::exit(2);
    }
    let mut ctx = ContainerContext::new(args[1].as_str(), args[2].as_str());
    ctx.config.service_id = args[3].parse().expect("a service id");
    ctx.config.responding = args.get(5).map(String::as_str) != Some("silent");
    let service = match TestService::new(&args[4]) {
        Ok(service) => service,
        Err(e) => {
            eprintln!("trace {}: {e}", args[4]);
            std::process::exit(2);
        }
    };
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
