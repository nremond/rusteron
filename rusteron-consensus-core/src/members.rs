//! The cluster's members, as Java's `ClusterMember` describes them: their
//! endpoints, parsed from `aeron.cluster.members`, and what this member
//! knows of each during elections and while it leads (their log position,
//! their vote, when it last heard from them); and the quorum arithmetic
//! Java's static helpers do over them.

use rusteron_archive_core::channel::ChannelUri;

/// Aeron's null for ids and positions.
pub const NULL_VALUE: i64 = -1;

/// A members string Java would refuse, its message Java's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

/// A member of the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterMember {
    pub id: i32,
    pub ingress_endpoint: String,
    pub consensus_endpoint: String,
    pub log_endpoint: String,
    pub catchup_endpoint: String,
    pub archive_endpoint: String,
    pub archive_response_endpoint: Option<String>,
    pub egress_response_endpoint: Option<String>,
    /// Its endpoints as given, comma-separated.
    pub endpoints: String,

    pub is_ballot_sent: bool,
    pub is_leader: bool,
    pub has_terminated: bool,
    /// The term of the log it last said it has.
    pub leadership_term_id: i64,
    pub candidate_term_id: i64,
    pub catchup_replay_session_id: i64,
    pub catchup_replay_correlation_id: i64,
    /// The position it last said it has appended, [`NULL_VALUE`] unknown.
    pub log_position: i64,
    pub time_of_last_append_position_ns: i64,
    /// Its vote for the candidate term it last voted in: `None` not yet.
    pub vote: Option<bool>,
    /// The channel this member's consensus publication to it uses.
    pub consensus_channel: Option<String>,
}

impl ClusterMember {
    fn new(id: i32, attributes: &[&str]) -> Self {
        Self {
            id,
            ingress_endpoint: attributes[0].into(),
            consensus_endpoint: attributes[1].into(),
            log_endpoint: attributes[2].into(),
            catchup_endpoint: attributes[3].into(),
            archive_endpoint: attributes[4].into(),
            archive_response_endpoint: attributes.get(5).map(|s| s.to_string()),
            egress_response_endpoint: attributes.get(6).map(|s| s.to_string()),
            endpoints: attributes.join(","),
            is_ballot_sent: false,
            is_leader: false,
            has_terminated: false,
            leadership_term_id: NULL_VALUE,
            candidate_term_id: NULL_VALUE,
            catchup_replay_session_id: NULL_VALUE,
            catchup_replay_correlation_id: NULL_VALUE,
            log_position: NULL_VALUE,
            time_of_last_append_position_ns: NULL_VALUE,
            vote: None,
            consensus_channel: None,
        }
    }

    /// What an election starts this member from knowing.
    pub fn reset(&mut self) {
        self.is_ballot_sent = false;
        self.is_leader = false;
        self.has_terminated = false;
        self.vote = None;
        self.candidate_term_id = NULL_VALUE;
        self.leadership_term_id = NULL_VALUE;
        self.log_position = NULL_VALUE;
    }

    /// Heard from within the timeout.
    pub fn is_active(&self, now_ns: i64, timeout_ns: i64) -> bool {
        self.time_of_last_append_position_ns + timeout_ns > now_ns
    }

    /// Whether this member would vote for `candidate`: its log no further.
    /// Activity is not checked, as Java does not, so nomination goes on when
    /// members fall silent.
    pub fn will_vote_for(&self, candidate: &ClusterMember) -> bool {
        self.log_position != NULL_VALUE && compare_log(self, candidate) <= 0
    }

    pub fn has_reached_position(&self, leadership_term_id: i64, position: i64, now_ns: i64, timeout_ns: i64) -> bool {
        self.is_active(now_ns, timeout_ns)
            && leadership_term_id == self.leadership_term_id
            && self.log_position >= position
    }
}

/// `aeron.cluster.members`: `id,ingress,consensus,log,catchup,archive[,archiveResponse[,egressResponse]]`
/// for each member, separated by `|`.
pub fn parse(value: &str) -> Result<Vec<ClusterMember>, Error> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    // Java's `split` drops trailing empty strings: a trailing `|` is fine.
    value
        .split('|')
        .rev()
        .skip_while(|s| s.is_empty())
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .map(|member| {
            let attributes: Vec<&str> = member.split(',').collect();
            if attributes.len() < 6 || attributes.len() > 8 {
                return Err(Error(format!("invalid member value: {member} within: {value}")));
            }
            let id = attributes[0]
                .parse()
                .map_err(|_| Error("invalid cluster member id, must be an integer value".into()))?;
            Ok(ClusterMember::new(id, &attributes[1..]))
        })
        .collect()
}

/// The members as `aeron.cluster.members` writes them: what a
/// ClusterMembersResponse carries.
pub fn encode_as_string(members: &[ClusterMember]) -> String {
    members
        .iter()
        .map(|m| format!("{},{}", m.id, m.endpoints))
        .collect::<Vec<_>>()
        .join("|")
}

/// `id=ingressEndpoint` for each member, comma-separated: what a client is
/// redirected with.
pub fn ingress_endpoints(members: &[ClusterMember]) -> String {
    members
        .iter()
        .map(|m| format!("{}={}", m.id, m.ingress_endpoint))
        .collect::<Vec<_>>()
        .join(",")
}

/// How many members make a majority.
pub fn quorum_threshold(member_count: usize) -> usize {
    (member_count >> 1) + 1
}

/// Whether a majority was heard from within the timeout.
pub fn has_active_quorum(members: &[ClusterMember], now_ns: i64, timeout_ns: i64) -> bool {
    members.iter().filter(|m| m.is_active(now_ns, timeout_ns)).count() >= quorum_threshold(members.len())
}

/// The highest position a majority of active members holds, each counting
/// for `position` of it: Java's ranking of the top quorum positions, 0 for a
/// missing one.
pub fn quorum_position(
    members: &[ClusterMember],
    now_ns: i64,
    timeout_ns: i64,
    position: impl Fn(usize, &ClusterMember) -> i64,
) -> i64 {
    let mut ranked = vec![0i64; quorum_threshold(members.len())];
    for (i, member) in members
        .iter()
        .enumerate()
        .filter(|(_, m)| m.is_active(now_ns, timeout_ns))
    {
        let mut position = position(i, member);
        for slot in ranked.iter_mut() {
            if position > *slot {
                std::mem::swap(slot, &mut position);
            }
        }
    }
    *ranked.last().expect("a quorum of at least one")
}

/// Whether a majority of active members has reached `position` in the term.
pub fn has_quorum_at_position(
    members: &[ClusterMember],
    leadership_term_id: i64,
    position: i64,
    now_ns: i64,
    timeout_ns: i64,
) -> bool {
    members
        .iter()
        .filter(|m| m.has_reached_position(leadership_term_id, position, now_ns, timeout_ns))
        .count()
        >= quorum_threshold(members.len())
}

pub fn reset(members: &mut [ClusterMember]) {
    members.iter_mut().for_each(ClusterMember::reset);
}

/// This member stands in `candidate_term_id`: its own vote cast, the others' unknown.
pub fn become_candidate(members: &mut [ClusterMember], candidate_term_id: i64, candidate_member_id: i32) {
    for member in members {
        if member.id == candidate_member_id {
            member.vote = Some(true);
            member.candidate_term_id = candidate_term_id;
            member.is_ballot_sent = true;
        } else {
            member.vote = None;
            member.candidate_term_id = NULL_VALUE;
            member.is_ballot_sent = false;
        }
    }
}

/// Every member but a gracefully closed leader voted for the candidate.
pub fn is_unanimous_leader(members: &[ClusterMember], candidate_term_id: i64, graceful_closed_leader_id: i32) -> bool {
    let mut votes = 0;
    for member in members.iter().filter(|m| m.id != graceful_closed_leader_id) {
        if member.candidate_term_id != candidate_term_id || member.vote != Some(true) {
            return false;
        }
        votes += 1;
    }
    votes >= quorum_threshold(members.len())
}

/// A majority voted for the candidate, and no one against.
pub fn is_quorum_leader(members: &[ClusterMember], candidate_term_id: i64) -> bool {
    let mut votes = 0;
    for member in members.iter().filter(|m| m.candidate_term_id == candidate_term_id) {
        match member.vote {
            Some(false) => return false,
            Some(true) => votes += 1,
            None => {}
        }
    }
    votes >= quorum_threshold(members.len())
}

/// Every member but a gracefully closed leader would vote for `candidate`.
pub fn is_unanimous_candidate(
    members: &[ClusterMember],
    candidate: &ClusterMember,
    graceful_closed_leader_id: i32,
) -> bool {
    let mut possible_votes = 0;
    for member in members.iter().filter(|m| m.id != graceful_closed_leader_id) {
        if !member.will_vote_for(candidate) {
            return false;
        }
        possible_votes += 1;
    }
    possible_votes >= quorum_threshold(members.len())
}

/// A majority would vote for `candidate`.
pub fn is_quorum_candidate(members: &[ClusterMember], candidate: &ClusterMember) -> bool {
    members.iter().filter(|m| m.will_vote_for(candidate)).count() >= quorum_threshold(members.len())
}

/// Orders logs by term, then position.
pub fn compare_log_positions(lhs_term: i64, lhs_position: i64, rhs_term: i64, rhs_position: i64) -> std::cmp::Ordering {
    lhs_term.cmp(&rhs_term).then(lhs_position.cmp(&rhs_position))
}

/// `compareLog` of two members' logs, as Java's: -1, 0 or 1.
pub fn compare_log(lhs: &ClusterMember, rhs: &ClusterMember) -> i32 {
    compare_log_positions(
        lhs.leadership_term_id,
        lhs.log_position,
        rhs.leadership_term_id,
        rhs.log_position,
    ) as i32
}

pub fn find_member(members: &[ClusterMember], member_id: i32) -> Option<&ClusterMember> {
    members.iter().find(|m| m.id == member_id)
}

pub fn set_is_leader(members: &mut [ClusterMember], leader_member_id: i32) {
    for member in members {
        member.is_leader = member.id == leader_member_id;
    }
}

/// The channel this member's consensus publication to `to` uses: the
/// template with `to`'s consensus endpoint, and a control endpoint of this
/// member's host on any port when `bind_control`.
pub fn consensus_channel(template: &str, to: &ClusterMember, this: &ClusterMember, bind_control: bool) -> String {
    let mut uri = ChannelUri::parse(template).unwrap_or_else(|_| ChannelUri::parse("aeron:udp").expect("a channel"));
    uri.put("endpoint", &to.consensus_endpoint);
    if bind_control && let Some(control) = replace_port_with_wildcard(&this.consensus_endpoint) {
        uri.put("control", &control);
    }
    uri.to_string()
}

/// `host:port` as `host:0`, Aeron's.
pub fn replace_port_with_wildcard(endpoint: &str) -> Option<String> {
    endpoint.rfind(':').map(|i| format!("{}0", &endpoint[..=i]))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MEMBERS: &str = "0,h0:1,h0:2,h0:3,h0:4,h0:5|1,h1:1,h1:2,h1:3,h1:4,h1:5|2,h2:1,h2:2,h2:3,h2:4,h2:5|";

    fn active(members: &mut [ClusterMember], positions: &[i64]) {
        for (m, &p) in members.iter_mut().zip(positions) {
            m.log_position = p;
            m.leadership_term_id = 1;
            m.time_of_last_append_position_ns = 100;
        }
    }

    #[test]
    fn members_parse_and_encode_as_java_writes_them() {
        let members = parse(MEMBERS).unwrap();
        assert_eq!(members.len(), 3);
        assert_eq!(members[1].consensus_endpoint, "h1:2");
        assert_eq!(members[2].endpoints, "h2:1,h2:2,h2:3,h2:4,h2:5");
        assert_eq!(encode_as_string(&members), MEMBERS.trim_end_matches('|'));
        assert_eq!(ingress_endpoints(&members), "0=h0:1,1=h1:1,2=h2:1");
        assert!(parse("0,a,b,c,d").is_err());
        assert!(parse("x,a,b,c,d,e").is_err());
        assert!(parse("").unwrap().is_empty());
    }

    #[test]
    fn the_quorum_position_is_the_majoritys() {
        let mut members = parse(MEMBERS).unwrap();
        active(&mut members, &[300, 100, 200]);
        assert_eq!(quorum_position(&members, 150, 1_000, |_, m| m.log_position), 200);
        members[0].time_of_last_append_position_ns = -2_000;
        assert_eq!(
            quorum_position(&members, 150, 1_000, |_, m| m.log_position),
            100,
            "an inactive member counts for nothing"
        );
        assert!(has_active_quorum(&members, 150, 1_000));
        members[1].time_of_last_append_position_ns = -2_000;
        assert!(!has_active_quorum(&members, 150, 1_000));
        assert_eq!(quorum_position(&members, 150, 1_000, |_, m| m.log_position), 0);
    }

    #[test]
    fn votes_count_as_javas() {
        let mut members = parse(MEMBERS).unwrap();
        become_candidate(&mut members, 5, 0);
        assert!(!is_quorum_leader(&members, 5));
        members[1].candidate_term_id = 5;
        members[1].vote = Some(true);
        assert!(is_quorum_leader(&members, 5));
        assert!(!is_unanimous_leader(&members, 5, NULL_VALUE as i32));
        assert!(
            is_unanimous_leader(&members, 5, 2),
            "a gracefully closed leader is not waited for"
        );
        members[2].candidate_term_id = 5;
        members[2].vote = Some(false);
        assert!(!is_quorum_leader(&members, 5), "one vote against loses the ballot");
    }

    #[test]
    fn candidates_are_judged_by_their_log() {
        let mut members = parse(MEMBERS).unwrap();
        active(&mut members, &[300, 100, 200]);
        assert!(is_unanimous_candidate(&members, &members[0].clone(), NULL_VALUE as i32));
        assert!(!is_quorum_candidate(&members, &members[1].clone()));
        assert!(is_quorum_candidate(&members, &members[2].clone()));
        members[0].log_position = NULL_VALUE;
        assert!(
            !members[0].will_vote_for(&members[2].clone()),
            "an unknown log votes for no one"
        );
    }

    #[test]
    fn the_consensus_channel_names_the_peer_and_binds_control() {
        let members = parse(MEMBERS).unwrap();
        assert_eq!(
            consensus_channel("aeron:udp?term-length=64k", &members[1], &members[0], true),
            "aeron:udp?term-length=64k|endpoint=h1:2|control=h0:0"
        );
        assert_eq!(
            consensus_channel("aeron:udp", &members[2], &members[0], false),
            "aeron:udp?endpoint=h2:2"
        );
    }
}
