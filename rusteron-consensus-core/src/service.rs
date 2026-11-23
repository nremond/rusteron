//! The consensus module's side of the services' protocol, on the service
//! control streams: what it sends them (Java's `ServiceProxy`), what they
//! send it (Java's `ConsensusModuleAdapter`), and their acknowledgements,
//! each service's in a queue, taken a set at a time (Java's `ServiceAck`).

use std::collections::VecDeque;

use rusteron_cluster_codecs::cluster::{
    boolean_type::BooleanType, cancel_timer_codec, close_session_codec, cluster_members_extended_response_codec,
    cluster_members_query_codec, cluster_members_response_codec, join_log_codec, request_service_ack_codec,
    schedule_timer_codec, service_ack_codec, service_termination_position_codec, session_message_header_codec,
};

use crate::codec::{self, SCHEMA_ID, encode};
use crate::members::ClusterMember;

/// What a service sends the consensus module.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceMessage {
    /// A message from the service to the cluster, whole, SBE header first;
    /// `service_id` is what the service put as its session id.
    Message {
        service_id: i32,
        message: Vec<u8>,
    },
    CloseSession {
        cluster_session_id: i64,
    },
    ScheduleTimer {
        correlation_id: i64,
        deadline: i64,
    },
    CancelTimer {
        correlation_id: i64,
    },
    Ack {
        log_position: i64,
        timestamp: i64,
        ack_id: i64,
        relevant_id: i64,
        service_id: i32,
    },
    ClusterMembersQuery {
        correlation_id: i64,
        extended: bool,
    },
    /// A template the module does not act on.
    Ignored,
}

/// Decodes what a service sent, its lengths checked: another schema is an
/// error, as Java throws on it.
pub fn decode(message: &[u8]) -> Result<ServiceMessage, String> {
    let h = codec::header(message)?;
    if h.schema_id != SCHEMA_ID {
        return Err(format!("expected schemaId={SCHEMA_ID}, actual={}", h.schema_id));
    }
    let header = || codec::header_decoder(message);
    Ok(match h.template_id {
        session_message_header_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 24)?;
            let d = session_message_header_codec::SessionMessageHeaderDecoder::default().header(header(), 0);
            ServiceMessage::Message {
                service_id: d.cluster_session_id() as i32,
                message: message.to_vec(),
            }
        }
        close_session_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 8)?;
            let d = close_session_codec::CloseSessionDecoder::default().header(header(), 0);
            ServiceMessage::CloseSession {
                cluster_session_id: d.cluster_session_id(),
            }
        }
        schedule_timer_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 16)?;
            let d = schedule_timer_codec::ScheduleTimerDecoder::default().header(header(), 0);
            ServiceMessage::ScheduleTimer {
                correlation_id: d.correlation_id(),
                deadline: d.deadline(),
            }
        }
        cancel_timer_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 8)?;
            let d = cancel_timer_codec::CancelTimerDecoder::default().header(header(), 0);
            ServiceMessage::CancelTimer {
                correlation_id: d.correlation_id(),
            }
        }
        service_ack_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 36)?;
            let d = service_ack_codec::ServiceAckDecoder::default().header(header(), 0);
            ServiceMessage::Ack {
                log_position: d.log_position(),
                timestamp: d.timestamp(),
                ack_id: d.ack_id(),
                relevant_id: d.relevant_id(),
                service_id: d.service_id(),
            }
        }
        cluster_members_query_codec::SBE_TEMPLATE_ID => {
            codec::needs(&h, 8)?;
            let d = cluster_members_query_codec::ClusterMembersQueryDecoder::default().header(header(), 0);
            ServiceMessage::ClusterMembersQuery {
                correlation_id: d.correlation_id(),
                extended: d.extended() == BooleanType::TRUE,
            }
        }
        _ => ServiceMessage::Ignored,
    })
}

/// Asks the services to join a log, at `log_position`, up to `max_log_position`.
#[allow(clippy::too_many_arguments)]
pub fn join_log(
    log_position: i64,
    max_log_position: i64,
    member_id: i32,
    log_session_id: i32,
    log_stream_id: i32,
    is_startup: bool,
    role: i32,
    channel: &str,
) -> Vec<u8> {
    join_log_as(
        log_position,
        max_log_position,
        member_id,
        log_session_id,
        log_stream_id,
        is_startup,
        role,
        false,
        channel,
    )
}

/// [`join_log`], the service told whether it is a standby's: it then takes
/// snapshots only on the flagged STANDBY_SNAPSHOT action.
#[allow(clippy::too_many_arguments)]
pub fn join_log_as(
    log_position: i64,
    max_log_position: i64,
    member_id: i32,
    log_session_id: i32,
    log_stream_id: i32,
    is_startup: bool,
    role: i32,
    is_standby: bool,
    channel: &str,
) -> Vec<u8> {
    use join_log_codec as c;
    encode!(c::JoinLogEncoder, 4 + channel.len(), |e| {
        e.log_position(log_position)
            .max_log_position(max_log_position)
            .member_id(member_id)
            .log_session_id(log_session_id)
            .log_stream_id(log_stream_id)
            .is_startup(boolean(is_startup))
            .role(role)
            .is_standby(boolean(is_standby))
            .log_channel(channel.as_bytes());
    })
}

/// Asks the services to ack once their log reaches `log_position`: a
/// standby's module holds its service at a members' snapshot so.
pub fn request_service_ack(log_position: i64) -> Vec<u8> {
    use request_service_ack_codec as c;
    encode!(c::RequestServiceAckEncoder, 0, |e| {
        e.log_position(log_position);
    })
}

/// Where the services stop, and whether they ack it.
pub fn termination_position(log_position: i64, ack_required: bool) -> Vec<u8> {
    use service_termination_position_codec as c;
    encode!(c::ServiceTerminationPositionEncoder, 0, |e| {
        e.log_position(log_position).ack_required(boolean(ack_required));
    })
}

pub fn cluster_members_response(correlation_id: i64, leader_member_id: i32, active_members: &str) -> Vec<u8> {
    use cluster_members_response_codec as c;
    encode!(c::ClusterMembersResponseEncoder, 8 + active_members.len(), |e| {
        e.correlation_id(correlation_id)
            .leader_member_id(leader_member_id)
            .active_members(active_members.as_bytes())
            .passive_followers(b"");
    })
}

/// A member's block in the extended response's group.
const ACTIVE_MEMBER_BLOCK_LENGTH: usize = 28;

pub fn cluster_members_extended_response(
    correlation_id: i64,
    current_time_ns: i64,
    leader_member_id: i32,
    member_id: i32,
    members: &[ClusterMember],
) -> Vec<u8> {
    use cluster_members_extended_response_codec as c;
    let var: usize = members
        .iter()
        .map(|m| {
            ACTIVE_MEMBER_BLOCK_LENGTH
                + 5 * 4
                + m.ingress_endpoint.len()
                + m.consensus_endpoint.len()
                + m.log_endpoint.len()
                + m.catchup_endpoint.len()
                + m.archive_endpoint.len()
        })
        .sum::<usize>()
        + 8;
    encode!(c::ClusterMembersExtendedResponseEncoder, var, |e| {
        e.correlation_id(correlation_id)
            .current_time_ns(current_time_ns)
            .leader_member_id(leader_member_id)
            .member_id(member_id);
        let mut active = e.active_members_encoder(members.len() as u16, c::encoder::ActiveMembersEncoder::default());
        for m in members {
            active.advance().expect("a member");
            active
                .leadership_term_id(m.leadership_term_id)
                .log_position(m.log_position)
                .time_of_last_append_ns(m.time_of_last_append_position_ns)
                .member_id(m.id)
                .ingress_endpoint(m.ingress_endpoint.as_bytes())
                .consensus_endpoint(m.consensus_endpoint.as_bytes())
                .log_endpoint(m.log_endpoint.as_bytes())
                .catchup_endpoint(m.catchup_endpoint.as_bytes())
                .archive_endpoint(m.archive_endpoint.as_bytes());
        }
        let e = active.parent().expect("the response");
        let mut passive = e.passive_members_encoder(0, c::encoder::PassiveMembersEncoder::default());
        passive.parent().expect("the response");
    })
}

fn boolean(value: bool) -> BooleanType {
    if value { BooleanType::TRUE } else { BooleanType::FALSE }
}

/// A service's ack.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServiceAck {
    pub ack_id: i64,
    pub log_position: i64,
    pub relevant_id: i64,
}

/// Each service's acks, in the order received.
#[derive(Clone, Debug)]
pub struct ServiceAcks {
    queues: Vec<VecDeque<ServiceAck>>,
}

impl ServiceAcks {
    pub fn new(service_count: usize) -> Self {
        Self {
            queues: vec![VecDeque::new(); service_count],
        }
    }

    pub fn push(&mut self, service_id: usize, ack: ServiceAck) -> Result<(), String> {
        let queue = self
            .queues
            .get_mut(service_id)
            .ok_or_else(|| format!("invalid serviceId={service_id}"))?;
        queue.push_back(ack);
        Ok(())
    }

    /// Whether every service's next ack is `ack_id` at `log_position`; an
    /// ack of another id or position is an error, as in Java.
    pub fn has_reached(&self, log_position: i64, ack_id: i64) -> Result<bool, String> {
        for (service_id, queue) in self.queues.iter().enumerate() {
            let Some(ack) = queue.front() else {
                return Ok(false);
            };
            if ack.ack_id != ack_id || ack.log_position != log_position {
                return Err(format!(
                    "ack out of sequence: expected [ackId={ack_id}, logPosition={log_position}] vs received \
                     [ackId={}, logPosition={}, relevantId={}, serviceId={service_id}]",
                    ack.ack_id, ack.log_position, ack.relevant_id
                ));
            }
        }
        Ok(true)
    }

    /// Each service's acks before `log_position` dropped; the id of their
    /// next acks, if each has one there and all the same.
    pub fn drop_before(&mut self, log_position: i64) -> Option<i64> {
        for queue in &mut self.queues {
            while queue.front().is_some_and(|ack| ack.log_position < log_position) {
                queue.pop_front();
            }
        }
        let mut ids = self
            .queues
            .iter()
            .map(|q| q.front().filter(|a| a.log_position == log_position).map(|a| a.ack_id));
        let first = ids.next()??;
        ids.all(|id| id == Some(first)).then_some(first)
    }

    /// Takes each service's next ack, all at `log_position`.
    pub fn poll(&mut self, log_position: i64) -> Result<Vec<ServiceAck>, String> {
        let mut acks = Vec::with_capacity(self.queues.len());
        for (service_id, queue) in self.queues.iter_mut().enumerate() {
            match queue.pop_front() {
                Some(ack) if ack.log_position == log_position => acks.push(ack),
                other => {
                    return Err(format!(
                        "invalid ack for serviceId={service_id} logPosition={log_position} {other:?}"
                    ));
                }
            }
        }
        Ok(acks)
    }

    pub fn clear(&mut self) {
        self.queues.iter_mut().for_each(VecDeque::clear);
    }

    /// Each service's next ack, if it has one.
    pub fn fronts(&self) -> Vec<Option<ServiceAck>> {
        self.queues.iter().map(|q| q.front().copied()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acks_are_taken_a_set_at_a_time_in_sequence() {
        let mut acks = ServiceAcks::new(2);
        let ack = |id, position| ServiceAck {
            ack_id: id,
            log_position: position,
            relevant_id: 7,
        };
        acks.push(0, ack(0, 0)).unwrap();
        assert!(!acks.has_reached(0, 0).unwrap());
        acks.push(1, ack(0, 0)).unwrap();
        assert!(acks.has_reached(0, 0).unwrap());
        assert_eq!(acks.poll(0).unwrap().len(), 2);
        acks.push(0, ack(2, 64)).unwrap();
        acks.push(1, ack(2, 64)).unwrap();
        assert!(acks.has_reached(64, 1).is_err(), "out of sequence");
        assert!(acks.push(2, ack(0, 0)).is_err());
    }

    #[test]
    fn acks_of_a_join_given_up_are_dropped_before_the_next() {
        let mut acks = ServiceAcks::new(2);
        let ack = |id, position| ServiceAck {
            ack_id: id,
            log_position: position,
            relevant_id: 7,
        };
        // A join at 64 given up, its acks come; the next join is at 128.
        for service in 0..2 {
            acks.push(service, ack(5, 64)).unwrap();
            acks.push(service, ack(6, 128)).unwrap();
        }
        assert_eq!(acks.drop_before(128), Some(6));
        assert!(acks.has_reached(128, 6).unwrap());
        assert_eq!(acks.poll(128).unwrap().len(), 2);
        acks.push(0, ack(7, 192)).unwrap();
        assert_eq!(acks.drop_before(192), None, "not every service's ack yet");
    }

    #[test]
    fn a_services_messages_decode() {
        let join = join_log(64, i64::MAX, 0, -5, 100, true, 2, "aeron:ipc?alias=log");
        assert_eq!(codec::header(&join).unwrap().template_id, 40);
        let members = crate::members::parse("0,a:1,a:2,a:3,a:4,a:5").unwrap();
        let response = cluster_members_extended_response(1, 2, 0, 0, &members);
        assert_eq!(codec::header(&response).unwrap().template_id, 43);
        let ack = {
            use service_ack_codec as c;
            encode!(c::ServiceAckEncoder, 0, |e| {
                e.log_position(64).timestamp(5).ack_id(1).relevant_id(9).service_id(1);
            })
        };
        assert_eq!(
            decode(&ack).unwrap(),
            ServiceMessage::Ack {
                log_position: 64,
                timestamp: 5,
                ack_id: 1,
                relevant_id: 9,
                service_id: 1
            }
        );
        assert!(decode(&ack[..20]).is_err());
    }
}
