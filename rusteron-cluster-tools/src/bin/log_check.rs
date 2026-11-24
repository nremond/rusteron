//! Every node's cluster log compared byte for byte as its archive replays it
//! ([`rusteron_cluster_tools::logs`]): the members' logs, and standbys' too.
//!
//! ```text
//! log_check --aeron-dir DIR [--archives HOST:PORT,...] [--hosts H0,H1,...] [--port-base B] [--port-stride S]
//!           [--client-host H] [--log-stream 100] [--replay-port-base 24700]
//! ```
//!
//! Node `n` is read from the cluster archive at `archives[n]`. Without
//! `--archives`, node `n` is slot `n` of the layout
//! ([`rusteron_cluster_tools::layout`]): its archive control endpoint on
//! `hosts[n]`. With one more host than the cluster has members, the
//! standby in the slot after them is read too. Each node's newest recording
//! of the log stream is replayed over the range all of them hold, its
//! CRC-32C kept at each MiB; the client runs on the media driver in `DIR`,
//! and node `n`'s replay comes to `--client-host` on port
//! `--replay-port-base` + `n`.
//!
//! Prints `logs` lines; exits 0 when every log is the same, 1 when one differs
//! or could not be read, 2 on a bad command line.

use std::process::ExitCode;

use rusteron_archive::{Aeron, AeronContext, IntoCString};
use rusteron_cluster_tools::args::Args;
use rusteron_cluster_tools::layout::{ARCHIVE_CONTROL, Layout};
use rusteron_cluster_tools::logs::{self, CheckOptions};

fn options(args: &Args) -> Result<(String, CheckOptions), String> {
    let aeron_dir = args.required("--aeron-dir")?.to_string();
    let archives = match args.value("--archives") {
        Some(list) => list.split(',').map(String::from).collect(),
        None => {
            let layout = Layout::parse(args)?;
            layout
                .hosts
                .iter()
                .enumerate()
                .map(|(slot, host)| layout.endpoint(host, slot, ARCHIVE_CONTROL))
                .collect()
        }
    };
    Ok((
        aeron_dir,
        CheckOptions {
            archives,
            client_host: args.value("--client-host").unwrap_or("localhost").to_string(),
            log_stream: args.number("--log-stream", 100)?,
            replay_port_base: args.number("--replay-port-base", 24_700)?,
        },
    ))
}

fn main() -> ExitCode {
    let (aeron_dir, options) = match options(&Args::from_env()) {
        Ok(options) => options,
        Err(e) => {
            eprintln!(
                "log_check: {e}; usage: log_check --aeron-dir DIR [--archives HOST:PORT,...] [--hosts H,...] \
                 [--port-base B] [--port-stride S] [--client-host H] [--log-stream ID] [--replay-port-base P]"
            );
            return ExitCode::from(2);
        }
    };
    let aeron = AeronContext::new().and_then(|context| {
        context.set_dir(&aeron_dir.as_str().into_c_string())?;
        let aeron = Aeron::new(&context)?;
        aeron.start()?;
        Ok(aeron)
    });
    let aeron = match aeron {
        Ok(aeron) => aeron,
        Err(e) => {
            eprintln!("log_check: a client on {aeron_dir}: {e}");
            return ExitCode::from(1);
        }
    };
    if logs::check(&aeron, &options) {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}
