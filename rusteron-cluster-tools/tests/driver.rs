//! The node driver end to end, in a scratch directory: it waits out a mark
//! file a kill left, brings Aeron's C driver up, prints its lines, and on
//! SIGTERM stops in order, the driver's directory deleted.

use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::os::unix::fs::FileExt;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusteron_cluster_core::markfile::{ACTIVITY_TIMESTAMP_OFFSET, SEMANTIC_VERSION, VERSION_OFFSET};

/// The node's process, killed if the test fails before it stops.
struct Node(Child);

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn waits_out_mark_files_runs_the_driver_and_stops_on_sigterm() {
    let root = std::env::temp_dir().join(format!("rusteron-node-driver-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let data = root.join("data");
    let aeron_dir = root.join("shm/node1");
    // The consensus module's mark file of a node killed 9.7 s ago: 0.3 s left to wait.
    let cluster = data.join("node1/cluster");
    fs::create_dir_all(&cluster).unwrap();
    let killed = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap() - 9_700;
    let mut header = vec![0; 64];
    header[VERSION_OFFSET..][..4].copy_from_slice(&SEMANTIC_VERSION.to_le_bytes());
    header[ACTIVITY_TIMESTAMP_OFFSET..][..8].copy_from_slice(&killed.to_le_bytes());
    fs::write(cluster.join("cluster-mark.dat"), header).unwrap();

    let mut node = Node(
        Command::new(env!("CARGO_BIN_EXE_node_driver"))
            .args(["--node", "1", "--hosts", "localhost,localhost,localhost", "--dir"])
            .arg(&data)
            .arg("--aeron-dir")
            .arg(root.join("shm"))
            // Options for the node's other processes, which the driver ignores.
            .args(["--sync", "1", "--no-event-log"])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = node.0.stdout.take().unwrap();
    let (send, lines) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = send.send(line);
        }
    });
    let mut seen: Vec<String> = Vec::new();
    while !seen.last().is_some_and(|line| line.contains(" up on ")) {
        seen.push(
            lines
                .recv_timeout(Duration::from_secs(30))
                .unwrap_or_else(|e| panic!("{e}: {seen:?}")),
        );
    }
    assert!(
        seen[0].starts_with("node 1: waited ")
            && seen[0].ends_with(" ms for the mark files of a node killed here to age"),
        "{seen:?}"
    );
    let up = format!("node 1: driver up in {}, Aeron's C driver ", aeron_dir.display());
    assert!(seen.iter().any(|line| line.starts_with(&up)), "{seen:?}");
    assert_eq!(
        seen.last().unwrap(),
        "node 1 of 3 up on localhost, Aeron's default idle strategies, driver SHARED"
    );
    // The driver's CnC file is ready: its version, written last, is set.
    let mut cnc_version = [0; 4];
    File::open(aeron_dir.join("cnc.dat"))
        .unwrap()
        .read_exact_at(&mut cnc_version, 0)
        .unwrap();
    assert_ne!(i32::from_le_bytes(cnc_version), 0);

    assert!(
        Command::new("kill")
            .args(["-TERM", &node.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = node.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "still running 10 s after SIGTERM");
        thread::sleep(Duration::from_millis(20));
    };
    assert!(status.success(), "{status}");
    let rest: Vec<String> = lines.iter().collect();
    assert!(
        rest.last()
            .is_some_and(|line| line.starts_with("node 1: stopped in ") && line.ends_with(" ms")),
        "{rest:?}"
    );
    assert!(!aeron_dir.exists(), "{} left behind", aeron_dir.display());
    fs::remove_dir_all(&root).unwrap();
}
