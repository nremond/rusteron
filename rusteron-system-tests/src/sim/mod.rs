//! A deterministic simulator of an Aeron Cluster whose every member runs the
//! Rust consensus module (`rusteron-cluster`): the members, their
//! archives, services and a client on one thread in virtual time, the
//! network between them at the level of the log's and the consensus
//! stream's messages. A seed sets everything: latencies, faults, the
//! modules' nomination delays; the same seed replays the same run.
//!
//! Faults, for a while: members crash and restart on what they wrote,
//! members are cut off from the others (both ways, or one way between two),
//! clocks jump ahead, snapshots are asked at random. Then the network heals
//! and every member restarts, and the cluster must come back.
//!
//! Checked all along: a member's commit position never goes back; members'
//! services never apply different entries at a position, nor overlapping
//! ones; a client message is applied at one position only. Checked at the
//! end: every member up with one leader in one term, a client message sent
//! after the faults committed, and every member's service has applied every
//! client message any applied.
//!
//! With `backup`, a cluster backup (`rusteron-cluster::backup`)
//! runs beside the members on an archive of its own, its process crashing
//! and its links cut among the faults. Checked: its live log position never
//! goes back; at the end, it holds the log up to the commit position, every
//! entry of it the one members' services applied there. Members answer a
//! backup only outside elections, and Java's backup excludes the last
//! member it found unsuitable until it excludes another: once the run is
//! over, the cluster quiet, the backup has up to [`BACKUP_CATCH_UP_MS`]
//! more to catch up.
//!
//! The archive and services are models of what the module asks of them
//! (`archive`, `service`), not the archive server and service container:
//! those have their own tests. A link is up or down: up, it delivers in
//! order after a latency; down, what was on it is lost and the images it
//! carried go, as Aeron's do once their connection times out. Archives
//! revoke a catch-up replay their client stops, as Java's does, its image
//! dropped wherever its readers are, or close it, as rusteron's does
//! (`revoke_stopped_catchups`).

pub mod archive;
pub mod backup;
pub mod client;
pub mod node;
pub mod rng;
pub mod service;

use std::collections::{BTreeMap, BTreeSet};

use rusteron_cluster::cluster_backup_agent::SourceType;
use rusteron_cluster::consensus_module::context::{Config, TimeUnit};
use rusteron_cluster::consensus_module_agent::ports::CounterId;
use rusteron_cluster::consensus_module_agent::{ElectionState, ModuleState, Role, toggle};

use crate::sim::archive::ReplayTarget;
use crate::sim::backup::{BackupPacket, SimBackup, is_backup_endpoint};
use crate::sim::client::SimClient;
use crate::sim::node::{Packet, SimNode};
use crate::sim::rng::Rng;
use crate::sim::service::Applied;

/// How long a backup has, once the run is over, to hold the committed log.
pub const BACKUP_CATCH_UP_MS: i64 = 20_000;

/// How a run goes.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub members: usize,
    /// How long faults are injected for, in virtual milliseconds.
    pub fault_ms: i64,
    /// How long the cluster has after, to come back.
    pub settle_ms: i64,
    pub faults: bool,
    pub client_interval_ms: i64,
    /// Keeps a trace of what happened in the report.
    pub trace: bool,
    /// Runs a cluster backup beside the members.
    pub backup: bool,
    /// How many log messages a member's archive replays to the member itself
    /// each millisecond: how long a restarted member takes to replay its log.
    pub local_replay_rate: usize,
    /// How many log entries a member's service applies each millisecond: it
    /// lags its module's replay when below `local_replay_rate`.
    pub service_rate: usize,
    /// Partitions hold what they carry until they heal, as Aeron's streams
    /// do through a cut shorter than their timeouts (about 5 s, and every
    /// random cut here is shorter): offers succeed, and once the links are
    /// back the receivers' NAKs have it all sent again, late. Off, they lose
    /// it, and offers find the member unreachable. Off by default, so the
    /// seeds pinned here keep their schedules.
    pub held_partitions: bool,
    /// Archives revoke a catch-up replay (`eos=false`) their client stops
    /// before its end, as Java's 1.54 archive does: its image is dropped
    /// wherever a follower's readers were. Off, as rusteron's archive and
    /// aeron-io/aeron#2260 stop one: closed, its image left to its readers.
    /// On by default, the stricter of the two. A replay of a member's log to
    /// itself is revoked either way, its services' image dropped with it.
    pub revoke_stopped_catchups: bool,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            members: 3,
            fault_ms: 20_000,
            settle_ms: 15_000,
            faults: true,
            client_interval_ms: 5,
            trace: false,
            backup: false,
            local_replay_rate: 64,
            service_rate: 256,
            held_partitions: false,
            revoke_stopped_catchups: true,
        }
    }
}

/// What a run came to.
#[derive(Debug, Default)]
pub struct Report {
    pub seed: u64,
    pub violations: Vec<String>,
    pub trace: Vec<String>,
    pub elections: i64,
    pub crashes: usize,
    pub partitions: usize,
    pub snapshots: i64,
    pub commit_position: i64,
    pub client_messages_sent: usize,
    pub client_messages_applied: usize,
    /// Where the backup's log recording ended.
    pub backup_position: i64,
    /// How long the backup took to catch up once the run was over.
    pub backup_catch_up_ms: i64,
}

impl Report {
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }
}

/// Where a member's endpoints are: `n<member>:100<kind>`.
fn endpoint(member: usize, kind: u32) -> String {
    format!("n{member}:100{kind}")
}

/// The members' ingress endpoints as a client is given them: `id=endpoint,...`.
fn ingress_endpoints(members: usize) -> String {
    (0..members)
        .map(|m| format!("{m}={}", endpoint(m, 1)))
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn member_of_endpoint(endpoint: &str) -> Option<usize> {
    endpoint.strip_prefix('n')?.split(':').next()?.parse().ok()
}

fn members_string(members: usize) -> String {
    (0..members)
        .map(|m| {
            format!(
                "{m},{},{},{},{},{}",
                endpoint(m, 1),
                endpoint(m, 2),
                endpoint(m, 3),
                endpoint(m, 4),
                endpoint(m, 5)
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn member_config(member: usize, members: usize) -> Config {
    Config {
        member_id: member as i32,
        cluster_members: members_string(members),
        service_count: 1,
        time_unit: TimeUnit::Millis,
        ingress_channel: "aeron:udp?term-length=64k".into(),
        log_channel: "aeron:udp?term-length=64k".into(),
        replication_channel: format!("aeron:udp?endpoint={}", endpoint(member, 6)),
        session_timeout_ns: 5_000_000_000,
        leader_heartbeat_timeout_ns: 1_000_000_000,
        leader_heartbeat_interval_ns: 100_000_000,
        election_timeout_ns: 500_000_000,
        election_status_interval_ns: 100_000_000,
        startup_canvass_timeout_ns: 2_000_000_000,
        termination_timeout_ns: 2_000_000_000,
        ..Config::default()
    }
}

/// Who sends a packet, who receives it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Party {
    Member(usize),
    Client(usize),
    Backup,
}

#[derive(Debug)]
struct InFlight {
    from: Party,
    to: Party,
    packet: Delivery,
}

#[derive(Debug)]
enum Delivery {
    Member(Packet),
    Ingress(Vec<u8>),
    FromBackup(BackupPacket),
    /// A member's archive answering the backup's control session.
    ArchiveResponse {
        session: i64,
        bytes: Vec<u8>,
    },
}

#[derive(Debug)]
enum Event {
    Restart(usize),
    Unblock(Vec<(usize, usize)>),
    RestartBackup,
    UnblockBackup(Vec<(Party, Party)>),
}

struct World {
    config: SimConfig,
    rng: Rng,
    now_ms: i64,
    nodes: Vec<SimNode>,
    clients: Vec<SimClient>,
    /// Links cut, from one member to another.
    blocked: BTreeSet<(usize, usize)>,
    /// Links that hold what they carry until they heal, as Aeron's streams
    /// do through a cut shorter than their timeouts: offers succeed, and
    /// once the link is back the receiver's NAKs have it all sent again, in
    /// order.
    held: BTreeSet<(usize, usize)>,
    /// What held links carry, in the order it was sent.
    held_packets: Vec<InFlight>,
    latency_ms: BTreeMap<(Party, Party), i64>,
    in_flight: BTreeMap<(i64, u64), InFlight>,
    next_seq: u64,
    events: BTreeMap<(i64, u64), Event>,
    /// The log as members' services applied it: entries by end position.
    log: BTreeMap<i64, Applied>,
    /// Client messages applied: id → end position.
    client_messages: BTreeMap<u64, i64>,
    /// How far each member's service was checked, per incarnation.
    checked: BTreeMap<(usize, u32), i64>,
    report: Report,
    healed_ms: i64,
    /// Each member's role, election state and term, as last traced.
    seen: Vec<Option<(Role, Option<ElectionState>, i64)>>,
    backup: Option<SimBackup>,
    /// Links between the backup and members cut.
    backup_cut: BTreeSet<(Party, Party)>,
    /// The member each replay to the backup comes from, by its session.
    backup_replays: BTreeMap<i32, usize>,
    backup_seen: Option<rusteron_cluster::cluster_backup_agent::State>,
}

/// Runs the simulation of `seed`.
pub fn run(seed: u64, config: &SimConfig) -> Report {
    let mut world = World::new(seed, config);
    let end = config.fault_ms + config.settle_ms;
    while world.now_ms < end {
        world.now_ms += 1;
        world.tick();
    }
    if world.backup.is_some() {
        while world.now_ms < end + BACKUP_CATCH_UP_MS && !world.backup_caught_up() {
            world.now_ms += 1;
            world.tick();
        }
        world.report.backup_catch_up_ms = world.now_ms - end;
    }
    world.finish();
    world.report
}

/// What [`leader_lost_during_replay`] saw.
#[derive(Debug)]
pub struct LeaderLoss {
    pub report: Report,
    /// From the leader's loss until another member led a newer term, in
    /// LEADER_READY or with its election over; `None` if none did.
    pub replaced_after_ms: Option<i64>,
}

/// How long a member stays down before it restarts, in
/// [`leader_lost_during_replay`].
const DOWN_MS: i64 = 6_000;
/// How long the others have to replace the leader, in
/// [`leader_lost_during_replay`].
const REPLACE_MS: i64 = 30_000;

/// A fault injector's pause of a leader while a follower just restarted
/// replays its log (seen in runs with no JVM, 2026-11-02): with no random
/// fault, a
/// follower crashes at `config.fault_ms` and restarts `DOWN_MS` (6 s) later;
/// once its election replays its log, the leader is cut off from the
/// others, as a paused process is from its peers, until another member
/// leads a newer term. Then the links heal and the cluster has
/// `config.settle_ms`, its invariants checked as [`run`] checks them.
pub fn leader_lost_during_replay(seed: u64, config: &SimConfig) -> LeaderLoss {
    let config = SimConfig {
        faults: false,
        backup: false,
        ..config.clone()
    };
    let mut world = World::new(seed, &config);
    let mut replaced_after_ms = None;
    world.run_until(config.fault_ms, |_| false);
    match world.steady_leader() {
        None => world.violation("no leader before the follower's crash".into()),
        Some(leader) => {
            let follower = (0..world.nodes.len())
                .find(|&m| m != leader && world.nodes[m].is_up())
                .expect("a follower");
            world.nodes[follower].crash();
            world.member_gone(follower);
            world.trace(|| format!("member {follower} crashes"));
            let restart_ms = world.now_ms + DOWN_MS;
            world.schedule(restart_ms, Event::Restart(follower));
            let replaying = |w: &World| {
                w.nodes[follower].running.as_ref().is_some_and(|r| {
                    r.module.election_state() == Some(ElectionState::FollowerReplay)
                        && r.ports
                            .archive
                            .replays
                            .iter()
                            .any(|replay| matches!(replay.target, ReplayTarget::Local { .. }))
                })
            };
            if !world.run_until(restart_ms + 10_000, replaying) {
                world.violation(format!("member {follower} never replayed its log once restarted"));
            } else {
                let term = world.nodes[leader]
                    .running
                    .as_ref()
                    .map_or(NULL_TERM, |r| r.module.leadership_term_id());
                let links: Vec<(usize, usize)> = (0..world.nodes.len())
                    .filter(|&o| o != leader)
                    .flat_map(|o| [(leader, o), (o, leader)])
                    .collect();
                world.cut(&links);
                world.trace(|| format!("member {leader} cut off, leading term {term}"));
                let lost_ms = world.now_ms;
                if world.run_until(lost_ms + REPLACE_MS, |w| w.replaced(leader, term)) {
                    let after = world.now_ms - lost_ms;
                    replaced_after_ms = Some(after);
                    world.trace(|| format!("leader {leader} replaced after {after} ms"));
                } else {
                    world.violation(format!("leader {leader} not replaced within {REPLACE_MS} ms"));
                }
            }
        }
    }
    world.heal();
    let end = world.now_ms + config.settle_ms;
    world.run_until(end, |_| false);
    world.finish();
    LeaderLoss {
        report: world.report,
        replaced_after_ms,
    }
}

/// What [`leader_partitioned`] saw.
#[derive(Debug)]
pub struct Partition {
    pub report: Report,
    /// The member cut off, and the term it led.
    pub cut_off: Option<(usize, i64)>,
    /// The member that led a newer term while the leader was cut off, and
    /// that term.
    pub replacement: Option<(usize, i64)>,
    /// The member leading once the cluster settled, and its term.
    pub settled: Option<(usize, i64)>,
    /// From the heal until a member led with no election under way.
    pub leader_after_heal_ms: Option<i64>,
}

/// A fault injector's partition of the leader across zones: with no random
/// fault, the leader of a steady
/// cluster is cut off from every other member for `cut_ms`, both ways,
/// while the others elect another if the cut lasts long enough; then the
/// links heal and the cluster has `config.settle_ms`, its invariants
/// checked as [`run`] checks them. With `held`, the links hold what they
/// carry until they heal, as Aeron's streams do through a cut of a few
/// seconds; otherwise it is lost.
pub fn leader_partitioned(seed: u64, config: &SimConfig, cut_ms: i64, held: bool) -> Partition {
    let config = SimConfig {
        faults: false,
        backup: false,
        ..config.clone()
    };
    let mut world = World::new(seed, &config);
    let (mut cut_off, mut replacement) = (None, None);
    world.run_until(config.fault_ms, |_| false);
    match world.steady_leader() {
        None => world.violation("no leader before the partition".into()),
        Some(leader) => {
            let term = world.nodes[leader]
                .running
                .as_ref()
                .map_or(NULL_TERM, |r| r.module.leadership_term_id());
            cut_off = Some((leader, term));
            let links: Vec<(usize, usize)> = (0..world.nodes.len())
                .filter(|&o| o != leader)
                .flat_map(|o| [(leader, o), (o, leader)])
                .collect();
            if held {
                world.hold(&links);
            } else {
                world.cut(&links);
            }
            world.trace(|| format!("member {leader} cut off, leading term {term}"));
            let healed_at = world.now_ms + cut_ms;
            world.run_until(healed_at, |_| false);
            replacement = world.newer_leader(leader, term);
        }
    }
    world.heal();
    let healed_ms = world.now_ms;
    let end = healed_ms + config.settle_ms;
    let mut leader_after_heal_ms = None;
    while world.now_ms < end {
        if leader_after_heal_ms.is_none() && world.steady_leader().is_some() {
            leader_after_heal_ms = Some(world.now_ms - healed_ms);
        }
        world.now_ms += 1;
        world.tick();
    }
    let settled = world.steady_leader().map(|m| {
        let term = world.nodes[m]
            .running
            .as_ref()
            .map_or(NULL_TERM, |r| r.module.leadership_term_id());
        (m, term)
    });
    world.finish();
    Partition {
        report: world.report,
        cut_off,
        replacement,
        settled,
        leader_after_heal_ms,
    }
}

/// What [`stale_catchup_replay`] saw.
#[derive(Debug)]
pub struct StaleCatchup {
    pub report: Report,
    /// How far the follower's module had read past its service when the
    /// leader's archive stopped the stale replay; `None` if the script did
    /// not get there.
    pub service_behind: Option<i64>,
}

/// How long the follower's services stand still on the stale replay before
/// the leader's archive stops it, in [`stale_catchup_replay`].
const STALE_READ_MS: i64 = 10;
/// How long they stand still in all, in [`stale_catchup_replay`].
const SERVICE_PAUSE_MS: i64 = 200;

/// A follower's catch-up given up while the leader's archive, busy, is slow
/// to stop its replay, as fault injection had it on Java's members
/// (aeron-io/aeron#2259): with no random fault, a follower crashes at
/// `config.fault_ms` and restarts `DOWN_MS` (6 s) later. Once it catches up, the
/// leader's archive turns busy: its replays stall, for the follower to give
/// the catch-up up for want of progress, and its requests wait, the stop of
/// that replay among them. Once the follower awaits its next catch-up there,
/// the replays go on, and the old one feeds it. Its services stand still
/// while its module reads on; then the archive handles its requests, and
/// stops the old replay: revoked with `config.revoke_stopped_catchups`, as
/// Java's 1.54 archive does, its image dropped under the services; closed
/// otherwise, as rusteron's archive does. Then the cluster has
/// `config.settle_ms`, its invariants checked as [`run`] checks them: a
/// service told to join the log where it was not is one.
pub fn stale_catchup_replay(seed: u64, config: &SimConfig) -> StaleCatchup {
    let config = SimConfig {
        faults: false,
        backup: false,
        ..config.clone()
    };
    let mut world = World::new(seed, &config);
    let mut service_behind = None;
    world.run_until(config.fault_ms, |_| false);
    let script = (|| -> Result<(), String> {
        let leader = world.steady_leader().ok_or("no leader before the follower's crash")?;
        let follower = (0..world.nodes.len())
            .find(|&m| m != leader && world.nodes[m].is_up())
            .ok_or("no follower")?;
        world.nodes[follower].crash();
        world.member_gone(follower);
        world.trace(|| format!("member {follower} crashes"));
        let restart_ms = world.now_ms + DOWN_MS;
        world.schedule(restart_ms, Event::Restart(follower));
        let in_state = move |w: &World, state: ElectionState| {
            w.nodes[follower]
                .running
                .as_ref()
                .is_some_and(|r| r.module.election_state() == Some(state))
        };
        if !world.run_until(restart_ms + 10_000, |w| in_state(w, ElectionState::FollowerCatchup)) {
            return Err(format!("member {follower} never caught up once restarted"));
        }
        {
            let archive = world.nodes[leader].archive_mut();
            archive.replays_stalled = true;
            archive.holding_requests = true;
        }
        world.trace(|| format!("member {leader}'s archive busy: its replays stall, its requests wait"));
        let deadline = world.now_ms + 5_000;
        if !world.run_until(deadline, |w| !in_state(w, ElectionState::FollowerCatchup)) {
            return Err(format!("member {follower} never gave its stalled catch-up up"));
        }
        if !world.run_until(deadline, |w| in_state(w, ElectionState::FollowerCatchupAwait)) {
            return Err(format!("member {follower} never awaited another catch-up"));
        }
        world.nodes[leader].archive_mut().replays_stalled = false;
        world.trace(|| format!("member {leader}'s replays go on, its requests still wait"));
        let deadline = world.now_ms + 1_000;
        if !world.run_until(deadline, |w| in_state(w, ElectionState::FollowerCatchup)) {
            return Err(format!("member {follower} never caught up from the old replay"));
        }
        world.nodes[follower].services_paused = true;
        world.trace(|| format!("member {follower}'s services stand still"));
        let paused_ms = world.now_ms;
        world.run_until(paused_ms + STALE_READ_MS, |_| false);
        let running = world.nodes[follower].running.as_ref().ok_or("the follower down")?;
        let read = running.module.commit_position();
        let applied = running.services.iter().map(|s| s.position).min().unwrap_or(read);
        service_behind = Some(read - applied);
        world.nodes[leader].archive_mut().release_requests();
        world.trace(|| {
            format!(
                "member {leader}'s archive handles its requests: its module read to {read}, its services to {applied}"
            )
        });
        world.run_until(paused_ms + SERVICE_PAUSE_MS, |_| false);
        world.nodes[follower].services_paused = false;
        world.trace(|| format!("member {follower}'s services go on"));
        Ok(())
    })();
    if let Err(e) = script {
        world.violation(e);
    }
    world.heal();
    let end = world.now_ms + config.settle_ms;
    world.run_until(end, |_| false);
    world.finish();
    StaleCatchup {
        report: world.report,
        service_behind,
    }
}

/// A term no leader has.
const NULL_TERM: i64 = -1;

impl World {
    fn new(seed: u64, config: &SimConfig) -> Self {
        let mut rng = Rng::new(seed);
        let members = config.members;
        let nodes = (0..members)
            .map(|m| {
                let mut node = SimNode::new(m, member_config(m, members), (rng.next_u64() as i32) | 1);
                node.local_replay_rate = config.local_replay_rate;
                node.service_rate = config.service_rate;
                node.archive_mut().revoke_stopped_catchups = config.revoke_stopped_catchups;
                node
            })
            .collect();
        let mut world = World {
            rng,
            now_ms: 0,
            nodes,
            clients: vec![SimClient::new(
                0,
                config.client_interval_ms,
                ingress_endpoints(config.members),
            )],
            blocked: BTreeSet::new(),
            held: BTreeSet::new(),
            held_packets: Vec::new(),
            latency_ms: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            next_seq: 0,
            events: BTreeMap::new(),
            log: BTreeMap::new(),
            client_messages: BTreeMap::new(),
            checked: BTreeMap::new(),
            report: Report {
                seed,
                ..Report::default()
            },
            healed_ms: config.fault_ms,
            seen: vec![None; members],
            backup: None,
            backup_cut: BTreeSet::new(),
            backup_replays: BTreeMap::new(),
            backup_seen: None,
            config: config.clone(),
        };
        for m in 0..members {
            let at = world.rng.range(0, 500);
            world.schedule(at, Event::Restart(m));
        }
        if config.backup {
            let source_type = [SourceType::Any, SourceType::Leader, SourceType::Follower][world.rng.pick(3)];
            world.backup = Some(SimBackup::new(backup::config(members, source_type, endpoint)));
            let at = world.rng.range(0, 1_500);
            world.schedule(at, Event::RestartBackup);
        }
        world
    }

    /// Runs until `done` holds or `deadline_ms` comes: whether `done` held.
    fn run_until(&mut self, deadline_ms: i64, done: impl Fn(&World) -> bool) -> bool {
        while self.now_ms < deadline_ms {
            if done(self) {
                return true;
            }
            self.now_ms += 1;
            self.tick();
        }
        done(self)
    }

    /// The member leading with no election under way.
    fn steady_leader(&self) -> Option<usize> {
        (0..self.nodes.len()).find(|&m| {
            self.nodes[m].running.as_ref().is_some_and(|r| {
                r.terminated.is_none() && r.module.role() == Role::Leader && r.module.election_state().is_none()
            })
        })
    }

    /// Whether a member other than `leader` leads a term after `term`, in
    /// LEADER_READY or past it: its services joined its log as leader.
    fn replaced(&self, leader: usize, term: i64) -> bool {
        (0..self.nodes.len()).filter(|&m| m != leader).any(|m| {
            self.nodes[m].running.as_ref().is_some_and(|r| {
                r.terminated.is_none()
                    && r.module.role() == Role::Leader
                    && r.module.leadership_term_id() > term
                    && matches!(r.module.election_state(), None | Some(ElectionState::LeaderReady))
            })
        })
    }

    /// The member other than `leader` leading a term after `term`, in
    /// LEADER_READY or past it, and that term.
    fn newer_leader(&self, leader: usize, term: i64) -> Option<(usize, i64)> {
        (0..self.nodes.len()).filter(|&m| m != leader).find_map(|m| {
            let r = self.nodes[m].running.as_ref()?;
            (r.terminated.is_none()
                && r.module.role() == Role::Leader
                && r.module.leadership_term_id() > term
                && matches!(r.module.election_state(), None | Some(ElectionState::LeaderReady)))
            .then(|| (m, r.module.leadership_term_id()))
        })
    }

    fn trace(&mut self, what: impl FnOnce() -> String) {
        if self.config.trace {
            let line = format!("{:>7} {}", self.now_ms, what());
            self.report.trace.push(line);
        }
    }

    fn schedule(&mut self, at_ms: i64, event: Event) {
        self.next_seq += 1;
        self.events.insert((at_ms, self.next_seq), event);
    }

    fn latency(&mut self, from: Party, to: Party) -> i64 {
        if let Some(&l) = self.latency_ms.get(&(from, to)) {
            return l;
        }
        let l = self.rng.range(1, 4);
        self.latency_ms.insert((from, to), l);
        l
    }

    fn link_up(&self, from: usize, to: usize) -> bool {
        self.nodes[from].is_up() && self.nodes[to].is_up() && !self.blocked.contains(&(from, to))
    }

    /// The link between member `member` and the backup, toward the backup or from it.
    fn backup_link_up(&self, member: usize, to_backup: bool) -> bool {
        let link = if to_backup {
            (Party::Member(member), Party::Backup)
        } else {
            (Party::Backup, Party::Member(member))
        };
        self.nodes[member].is_up()
            && self.backup.as_ref().is_some_and(SimBackup::is_up)
            && !self.backup_cut.contains(&link)
    }

    fn send(&mut self, from: Party, to: Party, packet: Delivery) {
        let at = self.now_ms + self.latency(from, to);
        self.next_seq += 1;
        self.in_flight
            .insert((at, self.next_seq), InFlight { from, to, packet });
    }

    fn tick(&mut self) {
        self.run_events();
        if self.config.faults && self.now_ms < self.config.fault_ms {
            self.inject_faults();
        }
        if self.now_ms == self.config.fault_ms {
            self.heal();
        }
        // Who each member reaches, as its ports see it.
        for i in 0..self.nodes.len() {
            let reachable: BTreeSet<i32> = (0..self.nodes.len())
                .filter(|&j| j != i && self.link_up(i, j))
                .map(|j| j as i32)
                .collect();
            let up = self.nodes[i].is_up();
            let backup = self.backup_link_up(i, true) && self.backup_link_up(i, false);
            if let Some(running) = self.nodes[i].running.as_mut() {
                running.ports.reachable_members = reachable;
                running.ports.reachable_clients = if up {
                    (0..self.clients.len()).collect()
                } else {
                    BTreeSet::new()
                };
                running.ports.reachable_backup = backup;
            }
        }
        if self.backup.is_some() {
            let reachable: BTreeSet<usize> = (0..self.nodes.len())
                .filter(|&m| self.backup_link_up(m, true) && self.backup_link_up(m, false))
                .collect();
            if let Some(r) = self.backup.as_mut().and_then(|b| b.running.as_mut()) {
                r.ports.reachable_members = reachable;
            }
        }
        let mut violations = Vec::new();
        for i in 0..self.nodes.len() {
            let was_up = self.nodes[i].is_up();
            self.nodes[i].step(self.now_ms, &mut violations);
            if was_up && !self.nodes[i].is_up() {
                let expected = self.nodes[i].running.as_ref().and_then(|r| r.terminated);
                self.trace(|| format!("member {i} terminated: expected={expected:?}"));
                if expected == Some(false) {
                    let errors = self.nodes[i].errors.iter().rev().take(3).cloned().collect::<Vec<_>>();
                    violations.push(format!("member {i} terminated unexpectedly: {errors:?}"));
                }
                self.member_gone(i);
                // Its supervisor starts it again.
                let at = self.now_ms + 1_000;
                self.schedule(at, Event::Restart(i));
            }
        }
        self.backup_step(&mut violations);
        for v in violations {
            self.violation(v);
        }
        if self.config.trace {
            self.trace_members();
        }
        self.collect_outboxes();
        self.heartbeats();
        self.remote_replays();
        self.repair_gaps();
        self.replications();
        self.deliver();
        self.clients_step();
        self.check_services();
    }

    fn trace_members(&mut self) {
        for i in 0..self.nodes.len() {
            let now = self.nodes[i]
                .running
                .as_ref()
                .filter(|r| r.terminated.is_none())
                .map(|r| {
                    (
                        r.module.role(),
                        r.module.election_state(),
                        r.module.leadership_term_id(),
                    )
                });
            if now != self.seen[i] {
                self.seen[i] = now;
                let commit = self.nodes[i].running.as_ref().map_or(0, |r| r.module.commit_position());
                self.trace(|| match now {
                    Some((role, election, term)) => {
                        format!("member {i}: {role:?} {election:?} term {term} commit {commit}")
                    }
                    None => format!("member {i}: down"),
                });
            }
        }
    }

    fn violation(&mut self, v: String) {
        let line = format!("{:>7} {v}", self.now_ms);
        if self.report.violations.len() < 50 {
            self.report.violations.push(line.clone());
        }
        if self.config.trace {
            self.report.trace.push(format!("VIOLATION {line}"));
        }
    }

    fn run_events(&mut self) {
        while let Some((&(at, seq), _)) = self.events.iter().next() {
            if at > self.now_ms {
                break;
            }
            let event = self.events.remove(&(at, seq)).expect("present");
            match event {
                Event::Restart(m) => {
                    if self.nodes[m].running.as_ref().is_some_and(|r| r.terminated.is_none()) {
                        continue;
                    }
                    if self.nodes[m].running.is_some() {
                        self.nodes[m].crash();
                    }
                    let seed = self.rng.next_u64();
                    self.nodes[m].start(self.now_ms, seed);
                    let incarnation = self.nodes[m].incarnation;
                    self.trace(|| format!("member {m} starts (incarnation {incarnation})"));
                }
                Event::Unblock(links) => {
                    for link in &links {
                        self.blocked.remove(link);
                    }
                    self.release(&links);
                    self.trace(|| "links restored".into());
                }
                Event::RestartBackup => {
                    let seed = self.rng.next_u64();
                    let now = self.now_ms;
                    let Some(backup) = self.backup.as_mut() else {
                        continue;
                    };
                    if backup.is_up() {
                        continue;
                    }
                    backup.crash();
                    backup.start(now, seed);
                    let incarnation = backup.incarnation;
                    self.trace(|| format!("backup starts (incarnation {incarnation})"));
                }
                Event::UnblockBackup(links) => {
                    for link in links {
                        self.backup_cut.remove(&link);
                    }
                    self.trace(|| "backup's links restored".into());
                }
            }
        }
    }

    fn inject_faults(&mut self) {
        let members = self.nodes.len();
        let down = (0..members).filter(|&m| !self.nodes[m].is_up()).count();
        if self.rng.chance(1.0 / 3_000.0) && down < members / 2 {
            let m = self.rng.pick(members);
            if self.nodes[m].is_up() {
                self.nodes[m].crash();
                self.report.crashes += 1;
                self.trace(|| format!("member {m} crashes"));
                self.member_gone(m);
                let at = self.now_ms + self.rng.range(300, 3_000);
                self.schedule(at, Event::Restart(m));
            }
        }
        if self.rng.chance(1.0 / 3_000.0) {
            let m = self.rng.pick(members);
            let links: Vec<(usize, usize)> = (0..members)
                .filter(|&o| o != m)
                .flat_map(|o| [(m, o), (o, m)])
                .collect();
            self.cut_or_hold(&links);
            self.report.partitions += 1;
            self.trace(|| format!("member {m} cut off"));
            let at = self.now_ms + self.rng.range(300, 4_000);
            self.schedule(at, Event::Unblock(links));
        }
        if self.rng.chance(1.0 / 4_000.0) {
            let a = self.rng.pick(members);
            let b = (a + 1 + self.rng.pick(members - 1)) % members;
            self.cut_or_hold(&[(a, b)]);
            self.report.partitions += 1;
            self.trace(|| format!("link {a} -> {b} cut"));
            let at = self.now_ms + self.rng.range(300, 3_000);
            self.schedule(at, Event::Unblock(vec![(a, b)]));
        }
        if self.backup.is_some() {
            self.inject_backup_faults();
        }
        if self.rng.chance(1.0 / 5_000.0) {
            let m = self.rng.pick(members);
            let jump = self.rng.range(100, 1_500);
            self.nodes[m].clock_offset_ms += jump;
            self.trace(|| format!("member {m}'s clock jumps {jump} ms"));
        }
        if self.rng.chance(1.0 / 4_000.0) {
            for node in &mut self.nodes {
                if let Some(r) = node.running.as_mut()
                    && r.module.role() == Role::Leader
                    && r.ports.toggle == toggle::NEUTRAL
                {
                    r.ports.toggle = toggle::SNAPSHOT;
                }
            }
            self.trace(|| "snapshot asked".into());
        }
    }

    /// The backup's faults: its process crashes, its links to a member are
    /// cut, both ways or one.
    fn inject_backup_faults(&mut self) {
        let members = self.nodes.len();
        if self.rng.chance(1.0 / 6_000.0) && self.backup.as_ref().is_some_and(SimBackup::is_up) {
            self.backup_gone();
            self.trace(|| "backup crashes".into());
            let at = self.now_ms + self.rng.range(300, 3_000);
            self.schedule(at, Event::RestartBackup);
        }
        if self.rng.chance(1.0 / 5_000.0) {
            let m = self.rng.pick(members);
            let links = match self.rng.pick(3) {
                0 => vec![(Party::Member(m), Party::Backup), (Party::Backup, Party::Member(m))],
                1 => vec![(Party::Member(m), Party::Backup)],
                _ => vec![(Party::Backup, Party::Member(m))],
            };
            self.cut_backup(&links);
            self.trace(|| format!("backup's links to member {m} cut: {links:?}"));
            let at = self.now_ms + self.rng.range(300, 3_000);
            self.schedule(at, Event::UnblockBackup(links));
        }
    }

    /// Links between the backup and members cut: what was on them lost, the
    /// members' replays to the backup gone with their images.
    fn cut_backup(&mut self, links: &[(Party, Party)]) {
        for &(from, to) in links {
            self.backup_cut.insert((from, to));
            self.in_flight.retain(|_, f| !(f.from == from && f.to == to));
            if let (Party::Member(m), Party::Backup) = (from, to) {
                self.lose_backup_replays(m);
            }
        }
    }

    /// The replays from member `m`'s archive to the backup end, and the
    /// backup's recordings of them stop.
    fn lose_backup_replays(&mut self, m: usize) {
        let sessions: Vec<i32> = self
            .backup_replays
            .iter()
            .filter(|(_, member)| **member == m)
            .map(|(session, _)| *session)
            .collect();
        for session in &sessions {
            self.backup_replays.remove(session);
            if let Some(backup) = self.backup.as_mut() {
                backup.lose_replay(*session);
            }
        }
        self.nodes[m]
            .archive_mut()
            .replays
            .retain(|r| !matches!(&r.target, ReplayTarget::Remote { endpoint, .. } if is_backup_endpoint(endpoint)));
    }

    /// The backup's process gone: what it sent or was sent lost, the
    /// members' replays to it stopped.
    fn backup_gone(&mut self) {
        if let Some(backup) = self.backup.as_mut() {
            backup.crash();
        }
        self.in_flight
            .retain(|_, f| f.from != Party::Backup && f.to != Party::Backup);
        self.backup_replays.clear();
        for m in 0..self.nodes.len() {
            self.nodes[m].archive_mut().replays.retain(
                |r| !matches!(&r.target, ReplayTarget::Remote { endpoint, .. } if is_backup_endpoint(endpoint)),
            );
        }
    }

    /// Whether the backup holds the log to every member's commit position,
    /// backing up.
    fn backup_caught_up(&self) -> bool {
        let commit = self
            .nodes
            .iter()
            .filter_map(|n| n.running.as_ref())
            .map(|r| r.module.commit_position())
            .max()
            .unwrap_or(0);
        self.backup.as_ref().is_some_and(|b| {
            b.state() == Some(rusteron_cluster::cluster_backup_agent::State::BackingUp)
                && b.recorded_position() >= commit
                && b.live_log_position() >= commit
        })
    }

    /// The backup's duty cycle; a termination of its own is a violation, and
    /// its supervisor starts it again.
    fn backup_step(&mut self, violations: &mut Vec<String>) {
        let now = self.now_ms;
        let Some(backup) = self.backup.as_mut() else {
            return;
        };
        let was_up = backup.is_up();
        backup.step(now, violations);
        let notes = std::mem::take(&mut backup.notes);
        let terminated = backup.running.as_ref().and_then(|r| r.terminated.clone());
        let state = backup.state().filter(|_| backup.is_up());
        for note in notes {
            self.trace(|| note);
        }
        if was_up && let Some(reason) = terminated {
            violations.push(format!("backup terminated: {reason}"));
            self.backup_gone();
            let at = now + 1_000;
            self.schedule(at, Event::RestartBackup);
        }
        if self.config.trace && state != self.backup_seen {
            self.backup_seen = state;
            let position = self.backup.as_ref().map_or(0, SimBackup::live_log_position);
            self.trace(|| format!("backup: {state:?} live log at {position}"));
        }
    }

    /// Links cut: what was on them lost, the images they carried gone.
    fn cut(&mut self, links: &[(usize, usize)]) {
        for &(from, to) in links {
            self.blocked.insert((from, to));
            self.in_flight
                .retain(|_, f| !(f.from == Party::Member(from) && f.to == Party::Member(to)));
            self.nodes[to].lose_images_from(from);
        }
    }

    /// Links held: what they carry waits for them to heal.
    fn hold(&mut self, links: &[(usize, usize)]) {
        self.held.extend(links.iter().copied());
    }

    /// A partition's links cut, or held with [`SimConfig::held_partitions`].
    fn cut_or_hold(&mut self, links: &[(usize, usize)]) {
        if self.config.held_partitions {
            self.hold(links);
        } else {
            self.cut(links);
        }
    }

    /// Held links back: what they held sent again, in the order it was sent.
    fn release(&mut self, links: &[(usize, usize)]) {
        let mut kept = Vec::new();
        let mut back = Vec::new();
        for f in std::mem::take(&mut self.held_packets) {
            let link = match (f.from, f.to) {
                (Party::Member(a), Party::Member(b)) => Some((a, b)),
                _ => None,
            };
            if link.is_some_and(|l| links.contains(&l)) {
                back.push(f);
            } else {
                kept.push(f);
            }
        }
        self.held_packets = kept;
        for link in links {
            self.held.remove(link);
        }
        for f in back {
            self.send(f.from, f.to, f.packet);
        }
    }

    /// A member down: what it sent or was sent lost, the images it fed gone,
    /// its clients disconnected.
    fn member_gone(&mut self, m: usize) {
        self.in_flight
            .retain(|_, f| f.from != Party::Member(m) && f.to != Party::Member(m));
        self.held_packets
            .retain(|f| f.from != Party::Member(m) && f.to != Party::Member(m));
        for o in 0..self.nodes.len() {
            if o != m {
                self.nodes[o].lose_images_from(m);
            }
        }
        for client in &mut self.clients {
            client.lose(m);
        }
        self.lose_backup_replays(m);
    }

    /// The faults end: links restored, members restarted, the client stops
    /// later so followers catch up.
    fn heal(&mut self) {
        self.blocked.clear();
        let held: Vec<(usize, usize)> = self.held.iter().copied().collect();
        self.release(&held);
        self.backup_cut.clear();
        for m in 0..self.nodes.len() {
            if !self.nodes[m].is_up() {
                self.schedule(self.now_ms + 1, Event::Restart(m));
            }
        }
        if self.backup.as_ref().is_some_and(|b| !b.is_up()) {
            self.schedule(self.now_ms + 1, Event::RestartBackup);
        }
        self.healed_ms = self.now_ms;
        self.trace(|| "faults end: links restored, members restarted".into());
    }

    fn collect_outboxes(&mut self) {
        for i in 0..self.nodes.len() {
            let packets = match self.nodes[i].running.as_mut() {
                Some(r) => std::mem::take(&mut r.ports.outbox),
                None => continue,
            };
            for packet in packets {
                let to = match &packet {
                    Packet::Consensus { to_member, .. } => Party::Member(*to_member as usize),
                    Packet::Log { endpoint, .. }
                    | Packet::LogHeartbeat { endpoint, .. }
                    | Packet::LogEnd { endpoint, .. }
                    | Packet::LogRevoke { endpoint, .. } => match member_of_endpoint(endpoint) {
                        Some(m) => Party::Member(m),
                        None => continue,
                    },
                    Packet::Egress { client, .. } => Party::Client(*client),
                    Packet::BackupEgress { .. } => Party::Backup,
                };
                if let Party::Member(to) = to
                    && !self.link_up(i, to)
                {
                    continue;
                }
                if to == Party::Backup && !self.backup_link_up(i, true) {
                    continue;
                }
                self.send(Party::Member(i), to, Delivery::Member(packet));
            }
        }
        let packets = match self.backup.as_mut().and_then(|b| b.running.as_mut()) {
            Some(r) => std::mem::take(&mut r.ports.outbox),
            None => Vec::new(),
        };
        for packet in packets {
            let to = match &packet {
                BackupPacket::Consensus { to_member, .. } | BackupPacket::ArchiveRequest { to_member, .. } => {
                    *to_member
                }
            };
            if self.backup_link_up(to, false) {
                self.send(Party::Backup, Party::Member(to), Delivery::FromBackup(packet));
            }
        }
    }

    /// The leader's log publication heartbeats its destinations, every 10 ms.
    fn heartbeats(&mut self) {
        if self.now_ms % 10 != 0 {
            return;
        }
        for i in 0..self.nodes.len() {
            let Some(p) = self.nodes[i]
                .running
                .as_ref()
                .and_then(|r| r.ports.log_publication.as_ref())
            else {
                continue;
            };
            let (session_id, position, destinations) = (p.session_id, p.position, p.destinations.clone());
            for endpoint in destinations {
                let Some(to) = member_of_endpoint(&endpoint) else {
                    continue;
                };
                if self.link_up(i, to) && self.nodes[to].is_subscribed_at(&endpoint) {
                    self.send(
                        Party::Member(i),
                        Party::Member(to),
                        Delivery::Member(Packet::LogHeartbeat {
                            endpoint,
                            session_id,
                            position,
                        }),
                    );
                }
            }
        }
    }

    /// Replays to followers' catch-up endpoints, once they subscribe there.
    fn remote_replays(&mut self) {
        for i in 0..self.nodes.len() {
            if !self.nodes[i].is_up() {
                continue;
            }
            // Catch-up replays stopped: revoked, after what they sent.
            let revoked = std::mem::take(&mut self.nodes[i].archive_mut().revoked_replays);
            for (endpoint, session_id, position) in revoked {
                if let Some(to) = member_of_endpoint(&endpoint)
                    && self.link_up(i, to)
                {
                    let packet = Packet::LogRevoke {
                        endpoint,
                        session_id,
                        position,
                    };
                    self.send(Party::Member(i), Party::Member(to), Delivery::Member(packet));
                }
            }
            if self.nodes[i].archive().replays_stalled {
                continue;
            }
            let replays: Vec<(usize, String, i32)> = self.nodes[i]
                .archive()
                .replays
                .iter()
                .enumerate()
                .filter_map(|(k, r)| match &r.target {
                    ReplayTarget::Remote { endpoint, session_id } => Some((k, endpoint.clone(), *session_id)),
                    ReplayTarget::Local { .. } => None,
                })
                .collect();
            // A bounded replay, a backup's, goes no further than this member's commit position.
            let commit = self.nodes[i]
                .running
                .as_ref()
                .map_or(0, |r| r.ports.counter(CounterId::CommitPosition));
            for (k, endpoint, session_id) in replays {
                // A member's catch-up endpoint, or the backup's.
                let to = if is_backup_endpoint(&endpoint) {
                    if !self.backup_link_up(i, true)
                        || !self.backup.as_ref().is_some_and(|b| b.is_subscribed_at(&endpoint))
                    {
                        continue;
                    }
                    Party::Backup
                } else {
                    let Some(to) = member_of_endpoint(&endpoint) else {
                        continue;
                    };
                    if !self.link_up(i, to) || !self.nodes[to].is_subscribed_at(&endpoint) {
                        continue;
                    }
                    Party::Member(to)
                };
                let mut packets = Vec::new();
                {
                    let archive = self.nodes[i].archive_mut();
                    let replay = archive.replays[k].clone();
                    let Some(recording) = archive.recordings.get(&replay.recording_id) else {
                        continue;
                    };
                    let stop = match replay.limit_counter_id {
                        Some(_) => replay.stop.min(commit),
                        None => replay.stop,
                    };
                    let mut position = replay.position;
                    if !replay.started {
                        packets.push(Packet::LogHeartbeat {
                            endpoint: endpoint.clone(),
                            session_id,
                            position,
                        });
                    }
                    for _ in 0..64 {
                        if position >= stop {
                            break;
                        }
                        match recording.message_at(position) {
                            Some(m) if m.end <= stop => {
                                packets.push(Packet::Log {
                                    endpoint: endpoint.clone(),
                                    session_id,
                                    message: m.clone(),
                                });
                                position = m.end;
                            }
                            _ => break,
                        }
                    }
                    let r = &mut archive.replays[k];
                    r.started = true;
                    r.position = position;
                }
                if to == Party::Backup {
                    self.backup_replays.insert(session_id, i);
                }
                for packet in packets {
                    self.send(Party::Member(i), to, Delivery::Member(packet));
                }
            }
            // A finished replay to another member ends.
            let archive = self.nodes[i].archive_mut();
            archive.replays.retain(|r| {
                !(matches!(r.target, ReplayTarget::Remote { .. }) && r.stop != i64::MAX && r.position >= r.stop)
            });
        }
    }

    /// Aeron's loss repair, every 5 ms: a follower's image missing data the
    /// leader's publication still holds (two terms back) has it sent again;
    /// further back, the image times out.
    fn repair_gaps(&mut self) {
        if self.now_ms % 5 != 0 {
            return;
        }
        for to in 0..self.nodes.len() {
            for source in 0..self.nodes.len() {
                if source == to || !self.link_up(source, to) {
                    continue;
                }
                for (endpoint, session_id, from) in self.nodes[to].gaps_from(source) {
                    let Some(p) = self.nodes[source]
                        .running
                        .as_ref()
                        .and_then(|r| r.ports.log_publication.as_ref())
                        .filter(|p| p.session_id == session_id)
                    else {
                        continue;
                    };
                    if from < p.position - 2 * i64::from(archive::TERM_LENGTH) {
                        self.trace(|| format!("member {to}'s image of {session_id} at {from} too far behind: lost"));
                        self.nodes[to].lose_image(source, session_id);
                        continue;
                    }
                    let resent: Vec<archive::Message> = self.nodes[source]
                        .archive()
                        .recordings
                        .values()
                        .filter(|r| r.session_id == session_id && r.is_recording())
                        .flat_map(|r| {
                            let mut out = Vec::new();
                            let mut position = from;
                            while let Some(m) = r.message_at(position) {
                                out.push(m.clone());
                                position = m.end;
                                if out.len() == 16 {
                                    break;
                                }
                            }
                            out
                        })
                        .collect();
                    for message in resent {
                        self.send(
                            Party::Member(source),
                            Party::Member(to),
                            Delivery::Member(Packet::Log {
                                endpoint: endpoint.clone(),
                                session_id,
                                message,
                            }),
                        );
                    }
                }
            }
        }
    }

    /// Replications from the leader's archive, while both members reach each other.
    fn replications(&mut self) {
        for dst in 0..self.nodes.len() {
            if !self.nodes[dst].is_up() {
                continue;
            }
            let count = self.nodes[dst].archive().replications.len();
            for k in (0..count).rev() {
                let replication = self.nodes[dst].archive().replications[k].clone();
                let Some(src) = member_of_endpoint(&replication.src_endpoint) else {
                    continue;
                };
                if !(self.link_up(dst, src) && self.link_up(src, dst)) {
                    continue;
                }
                let recording = self.nodes[src]
                    .archive()
                    .recordings
                    .get(&replication.src_recording_id)
                    .cloned();
                let archive = self.nodes[dst].archive_mut();
                match recording {
                    Some(recording) => archive.replicate_from(k, &recording, 64),
                    None => archive.fail_replication(
                        k,
                        &format!("unknown source recording id: {}", replication.src_recording_id),
                    ),
                }
            }
        }
        // The backup's copies of members' snapshots.
        let count = match self.backup.as_ref().and_then(|b| b.running.as_ref()) {
            Some(r) => r.ports.archive.replications.len(),
            None => 0,
        };
        for k in (0..count).rev() {
            let Some(r) = self.backup.as_ref().and_then(|b| b.running.as_ref()) else {
                break;
            };
            let replication = r.ports.archive.replications[k].clone();
            let Some(src) = member_of_endpoint(&replication.src_endpoint) else {
                continue;
            };
            if !(self.backup_link_up(src, true) && self.backup_link_up(src, false)) {
                continue;
            }
            let recording = self.nodes[src]
                .archive()
                .recordings
                .get(&replication.src_recording_id)
                .cloned();
            let Some(r) = self.backup.as_mut().and_then(|b| b.running.as_mut()) else {
                break;
            };
            match recording {
                Some(recording) => r.ports.archive.replicate_from(k, &recording, 64),
                None => r.ports.archive.fail_replication(
                    k,
                    &format!("unknown source recording id: {}", replication.src_recording_id),
                ),
            }
        }
    }

    fn deliver(&mut self) {
        while let Some((&key, _)) = self.in_flight.iter().next() {
            if key.0 > self.now_ms {
                break;
            }
            let f = self.in_flight.remove(&key).expect("present");
            if let (Party::Member(from), Party::Member(to)) = (f.from, f.to)
                && self.held.contains(&(from, to))
            {
                self.held_packets.push(f);
                continue;
            }
            match (f.from, f.to, f.packet) {
                (Party::Member(from), Party::Member(to), Delivery::Member(packet)) => {
                    if !self.link_up(from, to) {
                        continue;
                    }
                    let node = &mut self.nodes[to];
                    match packet {
                        Packet::Consensus { bytes, .. } => {
                            if let Some(r) = node.running.as_mut() {
                                r.consensus_inbox.push_back((from as i32, bytes));
                            }
                        }
                        Packet::Log {
                            endpoint,
                            session_id,
                            message,
                        } => node.on_log_data(&endpoint, session_id, message, from),
                        Packet::LogHeartbeat {
                            endpoint,
                            session_id,
                            position,
                        } => node.on_log_heartbeat(&endpoint, session_id, position, from),
                        Packet::LogEnd {
                            endpoint,
                            session_id,
                            position,
                        } => node.on_log_end(&endpoint, session_id, position),
                        Packet::LogRevoke {
                            endpoint,
                            session_id,
                            position,
                        } => node.on_log_revoke(&endpoint, session_id, position),
                        Packet::Egress { .. } | Packet::BackupEgress { .. } => {}
                    }
                }
                (Party::Backup, Party::Member(to), Delivery::FromBackup(packet)) => {
                    if !self.backup_link_up(to, false) {
                        continue;
                    }
                    match packet {
                        BackupPacket::Consensus { bytes, .. } => {
                            if let Some(r) = self.nodes[to].running.as_mut() {
                                r.consensus_inbox.push_back((backup::CONSENSUS_SESSION_ID, bytes));
                            }
                        }
                        BackupPacket::ArchiveRequest { session, bytes, .. } => {
                            let responses = self.nodes[to].archive_mut().on_remote_request(session, &bytes);
                            for bytes in responses {
                                self.send(
                                    Party::Member(to),
                                    Party::Backup,
                                    Delivery::ArchiveResponse { session, bytes },
                                );
                            }
                        }
                    }
                }
                (Party::Member(from), Party::Backup, Delivery::ArchiveResponse { session, bytes }) => {
                    if self.backup_link_up(from, true)
                        && let Some(r) = self.backup.as_mut().and_then(|b| b.running.as_mut())
                    {
                        r.archive_inbox.push_back((session, bytes));
                    }
                }
                (Party::Member(from), Party::Backup, Delivery::Member(packet)) => {
                    if !self.backup_link_up(from, true) {
                        continue;
                    }
                    let Some(backup) = self.backup.as_mut() else {
                        continue;
                    };
                    match packet {
                        Packet::BackupEgress { bytes } => {
                            if let Some(r) = backup.running.as_mut() {
                                r.consensus_inbox.push_back(bytes);
                            }
                        }
                        Packet::Log {
                            session_id, message, ..
                        } => backup.on_log_data(session_id, &message),
                        Packet::LogHeartbeat {
                            session_id, position, ..
                        } => backup.on_log_heartbeat(session_id, position),
                        _ => {}
                    }
                }
                (Party::Member(from), Party::Client(c), Delivery::Member(Packet::Egress { bytes, .. })) => {
                    if self.nodes[from].is_up() {
                        let (now, incarnation) = (self.now_ms, self.nodes[from].incarnation);
                        self.clients[c].on_egress(from, incarnation, &bytes, now);
                    }
                }
                (Party::Client(c), Party::Member(to), Delivery::Ingress(bytes)) => {
                    if let Some(r) = self.nodes[to].running.as_mut()
                        && r.terminated.is_none()
                    {
                        r.on_ingress(c, bytes);
                    } else {
                        self.clients[c].lose(to);
                    }
                }
                _ => {}
            }
        }
    }

    fn clients_step(&mut self) {
        // A member takes a client's ingress while it runs and has not terminated.
        let up: Vec<bool> = self
            .nodes
            .iter()
            .map(|n| n.running.as_ref().is_some_and(|r| r.terminated.is_none()))
            .collect();
        let stop_sending = self.healed_ms + self.config.settle_ms - 3_000;
        for c in 0..self.clients.len() {
            self.clients[c].sending = self.now_ms < stop_sending;
            self.clients[c].step(self.now_ms, &up, &mut self.rng);
            for (member, bytes) in std::mem::take(&mut self.clients[c].outbox) {
                self.send(Party::Client(c), Party::Member(member), Delivery::Ingress(bytes));
            }
            for line in std::mem::take(&mut self.clients[c].trace) {
                self.trace(|| line);
            }
        }
    }

    /// The services' new entries against the log as others applied it.
    fn check_services(&mut self) {
        for i in 0..self.nodes.len() {
            let incarnation = self.nodes[i].incarnation;
            let member = self.nodes[i].member_id;
            let Some(running) = self.nodes[i].running.as_mut() else {
                continue;
            };
            let Some(service) = running.services.first_mut() else {
                continue;
            };
            let from = self.checked.get(&(i, incarnation)).copied().unwrap_or(i64::MIN);
            let fresh: Vec<(i64, Applied)> = service.applied.range(from + 1..).map(|(e, a)| (*e, *a)).collect();
            let messages = std::mem::take(&mut service.client_messages);
            let mut violations = Vec::new();
            for (end, applied) in fresh {
                self.checked.insert((i, incarnation), end);
                match self.log.get(&end) {
                    Some(known) if *known != applied => violations.push(format!(
                        "member {member} applied {applied:?} ending at {end}, others {known:?}"
                    )),
                    Some(_) => {}
                    None => {
                        if let Some((other_end, _)) = self.log.range(applied.start + 1..end).next() {
                            violations.push(format!(
                                "member {member} applied an entry from {} to {end} over another ending at {other_end}",
                                applied.start
                            ));
                        }
                        if let Some((other_end, other)) = self.log.range(end + 1..).next()
                            && other.start < end
                        {
                            violations.push(format!(
                                "member {member} applied an entry from {} to {end} inside another from {} to {other_end}",
                                applied.start, other.start
                            ));
                        }
                        self.log.insert(end, applied);
                    }
                }
            }
            for (id, end) in messages {
                match self.client_messages.get(&id) {
                    Some(&known) if known != end => violations.push(format!(
                        "member {member} applied client message {id} at {end}, others at {known}"
                    )),
                    Some(_) => {}
                    None => {
                        self.client_messages.insert(id, end);
                        let client = (id >> 40) as usize;
                        if let Some(c) = self.clients.get_mut(client) {
                            c.on_applied(self.now_ms);
                        }
                    }
                }
            }
            for v in violations {
                self.violation(v);
            }
        }
    }

    /// The end of the run: the cluster must be back, and agree.
    fn finish(&mut self) {
        let mut leaders = Vec::new();
        let mut terms = BTreeSet::new();
        for node in &self.nodes {
            let Some(r) = node.running.as_ref().filter(|r| r.terminated.is_none()) else {
                self.report
                    .violations
                    .push(format!("end: member {} is down", node.member_id));
                continue;
            };
            self.report.elections += r.ports.counter(CounterId::ElectionCount);
            self.report.snapshots += r.ports.counter(CounterId::SnapshotCount);
            self.report.commit_position = self.report.commit_position.max(r.module.commit_position());
            if r.module.election_state().is_some() || r.module.state() != ModuleState::Active {
                self.report.violations.push(format!(
                    "end: member {} in election {:?}, state {:?}",
                    node.member_id,
                    r.module.election_state(),
                    r.module.state()
                ));
            }
            if r.module.role() == Role::Leader {
                leaders.push(node.member_id);
            }
            terms.insert(r.module.leadership_term_id());
        }
        if leaders.len() != 1 || terms.len() != 1 {
            self.report
                .violations
                .push(format!("end: leaders {leaders:?} in terms {terms:?}"));
        }
        let healed = self.healed_ms;
        let sent_after: BTreeSet<u64> = self
            .clients
            .iter()
            .flat_map(|c| c.sent.iter())
            .filter(|(_, at)| *at > healed + 1_000)
            .map(|(id, _)| *id)
            .collect();
        if !sent_after.iter().any(|id| self.client_messages.contains_key(id)) {
            self.report.violations.push(format!(
                "end: none of the {} client messages sent after the faults was committed",
                sent_after.len()
            ));
        }
        for node in &self.nodes {
            let Some(service) = node.running.as_ref().and_then(|r| r.services.first()) else {
                continue;
            };
            let missing: Vec<u64> = self
                .client_messages
                .iter()
                .filter(|(_, end)| !service.applied.contains_key(end))
                .map(|(id, _)| *id)
                .take(5)
                .collect();
            if !missing.is_empty() {
                self.report.violations.push(format!(
                    "end: member {}'s service lacks client messages {missing:?} (applied to {})",
                    node.member_id, service.position
                ));
            }
        }
        if self.config.trace {
            for node in &self.nodes {
                for e in &node.errors {
                    self.report.trace.push(format!("error: {e}"));
                }
            }
            for node in &self.nodes {
                self.report.trace.extend(node.describe());
            }
        }
        self.report.client_messages_sent = self.clients.iter().map(|c| c.sent.len()).sum();
        self.report.client_messages_applied = self.client_messages.len();
        self.finish_backup();
    }

    /// The backup at the end: up, its log recorded to the commit position,
    /// and every entry of it the one members' services applied there.
    fn finish_backup(&mut self) {
        let Some(backup) = self.backup.as_ref() else {
            return;
        };
        let commit = self.report.commit_position;
        let recorded = backup.recorded_position();
        self.report.backup_position = recorded;
        let mut violations = Vec::new();
        if !backup.is_up() {
            violations.push("end: the backup is down".to_string());
        } else if recorded < commit || backup.live_log_position() < commit {
            violations.push(format!(
                "end: the backup recorded the log to {recorded} (live log position {}, state {:?}), the cluster committed {commit}",
                backup.live_log_position(),
                backup.state()
            ));
        }
        let mut differ = 0;
        for m in backup.log_messages() {
            let fine = self
                .log
                .get(&m.end)
                .is_some_and(|a| a.start == m.start && a.hash == service::hash(&m.bytes));
            if !fine {
                differ += 1;
                if differ <= 3 {
                    violations.push(format!(
                        "end: the backup's entry from {} to {} is not the committed log's: {:?}",
                        m.start,
                        m.end,
                        self.log.get(&m.end)
                    ));
                }
            }
        }
        if self.config.trace {
            for e in &backup.errors {
                self.report.trace.push(format!("error: {e}"));
            }
        }
        self.report.violations.extend(violations);
    }
}
