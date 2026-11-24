//! Where a cluster's nodes are when they all follow one plan, as
//! `scripts/cluster.sh` lays them out: on one machine, or one host each.
//!
//! - **Slots.** Members take slots `0..members`, and standby `N` takes
//!   slot `members + N`.
//! - **Ports.** A node's ports start at `port_base + port_stride·slot`, with
//!   fixed offsets from there: [`ARCHIVE_CONTROL`], [`INGRESS`],
//!   [`CONSENSUS`], [`LOG`] and [`CATCHUP`].
//! - **Files.** A member's files are under `dir/node<i>` and a standby's
//!   under `dir/standby<N>`. A driver's directory is that name under the
//!   Aeron root (`--aeron-dir`), or `aeron` in the node's own directory when
//!   there is no root.
//!
//! The tools take this plan's defaults, and explicit endpoints where a
//! deployment differs.

use std::path::{Path, PathBuf};

use crate::args::Args;

/// Where slot 0's ports start.
pub const PORT_BASE: u32 = 21_000;
/// How far apart two slots' ports are.
pub const PORT_STRIDE: u32 = 100;
/// The cluster archive's control endpoint, from a slot's base.
pub const ARCHIVE_CONTROL: u32 = 1;
/// The client-facing (ingress) endpoint.
pub const INGRESS: u32 = 2;
/// The member-facing (consensus) endpoint. Members answer a standby's backup queries on the standby's.
pub const CONSENSUS: u32 = 3;
/// The log endpoint. Members' archives replay the log to a standby's.
pub const LOG: u32 = 4;
/// The catch-up (transfer) endpoint.
pub const CATCHUP: u32 = 5;
/// Members when neither `--hosts` nor `--members` says.
pub const MEMBERS: usize = 3;
/// The nodes' files when `--dir` does not say.
pub const DIR: &str = "data/nodes";

/// The plan: the members' hosts, the ports, the files.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Layout {
    /// Member `i` runs on `hosts[i]`.
    pub hosts: Vec<String>,
    pub port_base: u32,
    pub port_stride: u32,
    /// The nodes' files.
    pub dir: PathBuf,
    /// The drivers' directories' root, if any.
    pub aeron_root: Option<PathBuf>,
}

impl Default for Layout {
    fn default() -> Self {
        Self {
            hosts: vec!["localhost".into(); MEMBERS],
            port_base: PORT_BASE,
            port_stride: PORT_STRIDE,
            dir: PathBuf::from(DIR),
            aeron_root: None,
        }
    }
}

impl Layout {
    /// The layout from `--hosts H0,H1,...` (or `--members M`, each on
    /// localhost), `--port-base`, `--port-stride`, `--dir` and `--aeron-dir`.
    /// An empty `--aeron-dir` counts as none.
    pub fn parse(args: &Args) -> Result<Self, String> {
        let hosts: Vec<String> = match (args.value("--hosts"), args.value("--members")) {
            (Some(_), Some(_)) => return Err("--hosts and --members: one or the other".into()),
            (Some(hosts), None) => hosts.split(',').map(String::from).collect(),
            (None, Some(_)) => vec!["localhost".into(); args.number("--members", MEMBERS)?],
            (None, None) => vec!["localhost".into(); MEMBERS],
        };
        if hosts.is_empty() || hosts.iter().any(String::is_empty) {
            return Err("--hosts: a host for each member".into());
        }
        Ok(Self {
            hosts,
            port_base: args.number("--port-base", PORT_BASE)?,
            port_stride: args.number("--port-stride", PORT_STRIDE)?,
            dir: PathBuf::from(args.value("--dir").unwrap_or(DIR)),
            aeron_root: args.value("--aeron-dir").filter(|d| !d.is_empty()).map(PathBuf::from),
        })
    }

    /// How many members the cluster has.
    pub fn members(&self) -> usize {
        self.hosts.len()
    }

    /// Standby `standby`'s slot: after the members'.
    pub fn standby_slot(&self, standby: usize) -> usize {
        self.members() + standby
    }

    /// The port at `offset` from slot `slot`'s base.
    pub fn port(&self, slot: usize, offset: u32) -> u32 {
        let slot = u32::try_from(slot).unwrap_or(u32::MAX);
        self.port_base
            .saturating_add(self.port_stride.saturating_mul(slot))
            .saturating_add(offset)
    }

    /// `host:port` at `offset` from slot `slot`'s base.
    pub fn endpoint(&self, host: &str, slot: usize, offset: u32) -> String {
        format!("{host}:{}", self.port(slot, offset))
    }

    /// Member `member`'s endpoint at `offset`.
    pub fn member_endpoint(&self, member: usize, offset: u32) -> String {
        self.endpoint(&self.hosts[member], member, offset)
    }

    /// Every member's consensus endpoint, in member order: where a backup
    /// asks, and where a standby's notices go.
    pub fn consensus_endpoints(&self) -> Vec<String> {
        (0..self.members())
            .map(|i| self.member_endpoint(i, CONSENSUS))
            .collect()
    }

    /// The members as Aeron's `clusterMembers` names them:
    /// `id,ingress,consensus,log,catchup,archive|...`.
    pub fn cluster_members(&self) -> String {
        (0..self.members())
            .map(|i| {
                let e = |offset| self.member_endpoint(i, offset);
                format!(
                    "{i},{},{},{},{},{}|",
                    e(INGRESS),
                    e(CONSENSUS),
                    e(LOG),
                    e(CATCHUP),
                    e(ARCHIVE_CONTROL)
                )
            })
            .collect()
    }

    /// Member `member`'s files.
    pub fn member_dir(&self, member: usize) -> PathBuf {
        self.dir.join(format!("node{member}"))
    }

    /// Standby `standby`'s files.
    pub fn standby_dir(&self, standby: usize) -> PathBuf {
        self.dir.join(format!("standby{standby}"))
    }

    /// The driver's directory of the node whose files are in `node_dir`,
    /// made absolute: its name under the Aeron root, else `aeron` in it.
    pub fn aeron_dir(&self, node_dir: &Path) -> Result<PathBuf, String> {
        let dir = match (&self.aeron_root, node_dir.file_name()) {
            (Some(root), Some(name)) => root.join(name),
            _ => node_dir.join("aeron"),
        };
        std::path::absolute(&dir).map_err(|e| format!("{}: {e}", dir.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout(line: &str) -> Result<Layout, String> {
        Layout::parse(&Args::new(line.split_whitespace().map(String::from).collect()))
    }

    #[test]
    fn three_members_on_localhost_by_default() {
        let l = layout("").unwrap();
        assert_eq!(l, Layout::default());
        assert_eq!(
            l.consensus_endpoints(),
            ["localhost:21003", "localhost:21103", "localhost:21203"]
        );
        assert_eq!(l.standby_slot(0), 3);
        assert_eq!(
            l.endpoint("localhost", l.standby_slot(0), ARCHIVE_CONTROL),
            "localhost:21301"
        );
        assert_eq!(
            l.cluster_members(),
            "0,localhost:21002,localhost:21003,localhost:21004,localhost:21005,localhost:21001|\
             1,localhost:21102,localhost:21103,localhost:21104,localhost:21105,localhost:21101|\
             2,localhost:21202,localhost:21203,localhost:21204,localhost:21205,localhost:21201|"
        );
        assert_eq!(l.member_dir(2), Path::new("data/nodes/node2"));
        assert_eq!(l.standby_dir(0), Path::new("data/nodes/standby0"));
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(
            l.aeron_dir(&l.member_dir(1)).unwrap(),
            cwd.join("data/nodes/node1/aeron")
        );
    }

    #[test]
    fn hosts_ports_and_roots_as_given() {
        let l = layout("--hosts a,b,c,d,e --port-base 30000 --port-stride 10 --dir d --aeron-dir /dev/shm/x").unwrap();
        assert_eq!(l.members(), 5);
        assert_eq!(l.member_endpoint(4, CONSENSUS), "e:30043");
        assert_eq!(l.standby_slot(1), 6);
        assert_eq!(
            l.aeron_dir(&l.standby_dir(1)).unwrap(),
            Path::new("/dev/shm/x/standby1")
        );
        assert_eq!(layout("--members 5").unwrap().hosts, ["localhost"; 5]);
        // An empty root is none.
        let empty = Layout::parse(&Args::new(vec!["--aeron-dir".into(), String::new()])).unwrap();
        assert_eq!(empty.aeron_root, None);
    }

    #[test]
    fn refuses_what_names_no_cluster() {
        assert!(layout("--hosts a,b --members 2").is_err());
        assert!(layout("--hosts a,,c").is_err());
        assert!(layout("--members x").is_err());
        assert!(layout("--port-base -1").is_err());
    }
}
