//! Aeron Cluster's `ClusterTool`, on a cluster component's directory: see
//! [`rusteron_cluster::cluster_tool`].
//!
//! ```text
//! cluster_tool <cluster-dir> <command> [options]
//! ```

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let status = rusteron_cluster::cluster_tool::main(&args, &mut std::io::stdout().lock());
    std::process::exit(status);
}
