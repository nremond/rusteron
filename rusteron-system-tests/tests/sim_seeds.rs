//! Seeds run in the test suite: a quiet run, and runs with faults. A seed
//! that failed once and was fixed is kept here as a regression.

use rusteron_system_tests::sim::{SimConfig, leader_lost_during_replay, leader_partitioned, run, stale_catchup_replay};

fn check(seed: u64, config: &SimConfig) {
    let report = run(seed, config);
    assert!(
        report.passed(),
        "seed {seed}: {:#?}\nrerun with: cargo run -p rusteron-system-tests --release --bin cluster_sim -- --seed {seed}",
        report.violations
    );
}

#[test]
fn a_quiet_cluster_elects_a_leader_and_commits() {
    let config = SimConfig {
        faults: false,
        fault_ms: 3_000,
        settle_ms: 6_000,
        ..SimConfig::default()
    };
    let report = run(1, &config);
    assert!(report.passed(), "{:#?}", report.violations);
    assert!(report.client_messages_applied > 100, "{report:?}");
}

#[test]
fn seeds_with_faults_keep_the_invariants() {
    let config = SimConfig {
        fault_ms: 10_000,
        settle_ms: 10_000,
        ..SimConfig::default()
    };
    for seed in 0..4 {
        check(seed, &config);
    }
}

/// Seeds where Java's election rules lose committed entries or stall (see
/// the README), and seeds that failed while the module took Raft's rules
/// instead: a leader that could not count a follower catching up (972), a
/// member restarting its election mid catch-up (999) or mid replay (860),
/// a member keeping a term its new leader never had (3247, 4081),
/// recording logs filing terms under approximate ranges (391, 804, 2380),
/// a log holding entries of a term not yet filed (8692, 7947, 297720), a
/// module reading the log with another term than the log's (13933), acks
/// of a join given up on (8876, 98212), and a member waiting in follower
/// replay for a leader gone (298580).
#[test]
fn raft_rules_keep_the_invariants_in_seeds_javas_rules_fail() {
    let config = SimConfig::default();
    for seed in [
        111, 116, 178, 186, 380, 621, 641, 945, 55, 860, 972, 999, 3247, 4081, 391, 804, 2380, 8692, 7947, 297720,
        13933, 8876, 98212, 298580,
    ] {
        check(seed, &config);
    }
}

/// A seed of faults for 60 s: a member's replay stopping short of its log's
/// end left its election's log term older than its log's last entry, and it
/// voted for a log shorter in that term (306151).
#[test]
fn long_faults_keep_the_invariants() {
    let config = SimConfig {
        fault_ms: 60_000,
        ..SimConfig::default()
    };
    check(306_151, &config);
}

/// A leader lost while a follower just restarted replays its log, as a
/// fault injector pauses one: the follower gives its replay up once its leader is silent,
/// so the other member wins with its vote within a few seconds. Java's way,
/// replaying to the end without canvassing or voting, left the leader
/// unreplaced for the rest of the replay and a second more (runs with no
/// JVM, 9 to over 15 s). Its service lags the module's replay, as on real
/// members: the replay is held until the service reaches the module, or the
/// service, cut short, could not join the next.
#[test]
fn a_leader_lost_during_a_followers_replay_is_replaced_within_seconds() {
    let config = SimConfig {
        fault_ms: 20_000,
        client_interval_ms: 2,
        local_replay_rate: 3,
        service_rate: 2,
        ..SimConfig::default()
    };
    for seed in 0..10 {
        let loss = leader_lost_during_replay(seed, &config);
        assert!(loss.report.passed(), "seed {seed}: {:#?}", loss.report.violations);
        let after = loss.replaced_after_ms.expect("a leader replaced");
        eprintln!("seed {seed}: leader replaced after {after} ms");
        assert!(after <= 3_000, "seed {seed}: the leader was replaced after {after} ms");
    }
}

/// A backup beside a quiet cluster copies its snapshots and records its
/// log to the commit position.
#[test]
fn a_backup_of_a_quiet_cluster_holds_its_committed_log() {
    let config = SimConfig {
        faults: false,
        fault_ms: 3_000,
        settle_ms: 9_000,
        backup: true,
        ..SimConfig::default()
    };
    let report = run(1, &config);
    assert!(report.passed(), "{:#?}", report.violations);
    assert!(report.backup_position > 0, "{report:?}");
    assert_eq!(report.backup_position, report.commit_position);
}

/// A backup through faults, its own and the members': its log is the
/// committed log's prefix all along, and it keeps up once they end.
#[test]
fn a_backup_keeps_up_through_faults() {
    let config = SimConfig {
        fault_ms: 10_000,
        settle_ms: 10_000,
        backup: true,
        ..SimConfig::default()
    };
    for seed in 0..6 {
        check(seed, &config);
    }
}

/// A fault injector's partition of the leader across zones: the leader cut
/// off for 2 to 4.9 s while the others elect another,
/// then the links back. The new leader keeps its term, whether the links
/// lost what they carried or held it, as Aeron's streams do through such a
/// cut, and sent it all late. With held links, the cut-off member used to
/// get the others' finished election late: refusing their vote, it took
/// their term, and their stale canvass positions had it stand in the next,
/// ending the new leader's term for nothing (15 of 20 seeds at 3.6 s).
#[test]
fn a_healed_partition_keeps_the_new_leader() {
    let config = SimConfig {
        fault_ms: 10_000,
        settle_ms: 10_000,
        trace: std::env::var_os("SIM_TRACE").is_some(),
        ..SimConfig::default()
    };
    let (mut lost, mut replaced, mut runs) = (Vec::new(), 0, 0);
    for held in [true, false] {
        for cut_ms in [2_000, 3_600, 4_900] {
            for seed in 0..100 {
                let p = leader_partitioned(seed, &config, cut_ms, held);
                assert!(
                    p.report.passed(),
                    "seed {seed}, {cut_ms} ms, held {held}: {:#?}",
                    p.report.violations
                );
                runs += 1;
                if p.replacement.is_none() {
                    continue;
                }
                replaced += 1;
                if p.settled != p.replacement {
                    if config.trace {
                        p.report.trace.iter().for_each(|line| eprintln!("  {line}"));
                    }
                    lost.push((seed, cut_ms, held, p.replacement, p.settled));
                }
            }
        }
    }
    eprintln!(
        "{replaced} of {runs} cuts had the leader replaced; {} of those lost the new leader",
        lost.len()
    );
    assert!(
        replaced * 10 >= runs * 9,
        "only {replaced} of {runs} cuts had the leader replaced"
    );
    assert!(
        lost.is_empty(),
        "the new leader lost its term (seed, cut, held, replacement, settled): {lost:?}"
    );
}

/// A held cut too short for the others to replace the leader: once the
/// links are back, all of them in an election, the member with the longest
/// log must still get ahead of their terms and win. Refusing votes without
/// taking their terms once left such a cluster with no leader for more than
/// 10 s.
#[test]
fn a_short_held_partition_elects_soon_after_the_heal() {
    let config = SimConfig {
        fault_ms: 10_000,
        settle_ms: 10_000,
        ..SimConfig::default()
    };
    for seed in 0..100 {
        let p = leader_partitioned(seed, &config, 1_500, true);
        assert!(p.report.passed(), "seed {seed}: {:#?}", p.report.violations);
        let after = p.leader_after_heal_ms;
        assert!(
            after.is_some_and(|ms| ms <= 5_000),
            "seed {seed}: a leader {after:?} ms after the heal"
        );
    }
}

/// A follower's catch-up given up while the leader's archive, busy, is slow
/// to stop its replay (on Java's members under fault injection, 2026-11-22,
/// aeron-io/aeron#2259): the
/// follower's next catch-up reads on from the old replay, its services
/// behind its module, until the archive stops that replay. Rusteron's
/// archive closes a catch-up replay its client stops (`eos=false`), so the
/// follower's services read what its image holds and join the next log
/// where they are.
#[test]
fn a_stale_catchup_replay_stopped_leaves_no_service_behind() {
    let config = SimConfig {
        client_interval_ms: 1,
        revoke_stopped_catchups: false,
        ..SimConfig::default()
    };
    for seed in 0..4 {
        let s = stale_catchup_replay(seed, &config);
        assert!(
            s.service_behind.is_some_and(|behind| behind > 0),
            "seed {seed}: the services were not behind when the replay was stopped ({:?}): {:#?}",
            s.service_behind,
            s.report.violations
        );
        assert!(s.report.passed(), "seed {seed}: {:#?}", s.report.violations);
    }
}

/// The same with Java's 1.54 archive, which revokes a replay its client
/// stops: the follower's image is dropped under its services, its module
/// joins the next log where it read, and its services are told to join it
/// past where they are, as Java's service refused it under fault injection.
#[test]
fn a_stale_catchup_replay_revoked_as_javas_archive_does_leaves_the_service_behind() {
    let config = SimConfig {
        client_interval_ms: 1,
        revoke_stopped_catchups: true,
        ..SimConfig::default()
    };
    for seed in 0..4 {
        let s = stale_catchup_replay(seed, &config);
        assert!(
            s.service_behind.is_some_and(|behind| behind > 0),
            "seed {seed}: the services were not behind when the replay was revoked ({:?}): {:#?}",
            s.service_behind,
            s.report.violations
        );
        assert!(
            s.report
                .violations
                .iter()
                .any(|v| v.contains("told to join the log at")),
            "seed {seed}: no service left behind: {:#?}",
            s.report.violations
        );
    }
}
