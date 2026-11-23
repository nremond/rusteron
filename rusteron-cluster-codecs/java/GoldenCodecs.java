/*
 * Copyright 2026 the rusteron-cluster authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import io.aeron.cluster.codecs.*;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.codecs.mark.MarkFileHeaderEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.SemanticVersion;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Writes, with Aeron 1.53.3's own Java codecs, every message the Rust service
 * container and consensus module read or write, header included, as hex: one
 * file a message in the directory given. tests/golden.rs and
 * tests/golden_consensus.rs decode each with the Rust codecs, check the values
 * set here, encode them again and compare the bytes.
 *
 * <pre>
 * java -cp aeron-all-1.53.3.jar GoldenCodecs.java tests/fixtures
 * </pre>
 *
 * The values are arbitrary but distinct, so a field read from the wrong
 * offset shows.
 */
public final class GoldenCodecs
{
    static final String LOG_CHANNEL = "aeron:udp?endpoint=localhost:20113|alias=log|session-id=-123456";
    static final String RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:20121";
    static final String MEMBERS = "0,localhost:20110,localhost:20111,localhost:20112,localhost:20113,localhost:20114|";

    private static final ExpandableArrayBuffer BUFFER = new ExpandableArrayBuffer(1024);
    private static final MessageHeaderEncoder HEADER = new MessageHeaderEncoder();

    public static void main(final String[] args) throws IOException
    {
        final Path dir = Path.of(args[0]);
        Files.createDirectories(dir);

        // The consensus module to its services.
        final JoinLogEncoder joinLog = new JoinLogEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .logPosition(1_024)
            .maxLogPosition(4_096)
            .memberId(2)
            .logSessionId(-123_456)
            .logStreamId(100)
            .isStartup(BooleanType.TRUE)
            .role(1)
            .isStandby(BooleanType.FALSE)
            .logChannel(LOG_CHANNEL);
        write(dir, "join_log", joinLog.encodedLength());

        final ServiceTerminationPositionEncoder termination = new ServiceTerminationPositionEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .logPosition(8_192)
            .ackRequired(BooleanType.TRUE);
        write(dir, "service_termination_position", termination.encodedLength());

        final RequestServiceAckEncoder requestAck = new RequestServiceAckEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .logPosition(12_288);
        write(dir, "request_service_ack", requestAck.encodedLength());

        // A service to the consensus module.
        final ServiceAckEncoder ack = new ServiceAckEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .logPosition(2_048)
            .timestamp(1_700_000_000_123L)
            .ackId(3)
            .relevantId(77)
            .serviceId(1);
        write(dir, "service_ack", ack.encodedLength());

        final ScheduleTimerEncoder schedule = new ScheduleTimerEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .correlationId(42)
            .deadline(123_456_789);
        write(dir, "schedule_timer", schedule.encodedLength());

        final CancelTimerEncoder cancel = new CancelTimerEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .correlationId(43);
        write(dir, "cancel_timer", cancel.encodedLength());

        final CloseSessionEncoder close = new CloseSessionEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .clusterSessionId(7);
        write(dir, "close_session", close.encodedLength());

        // The log.
        final SessionMessageHeaderEncoder message = new SessionMessageHeaderEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(5)
            .clusterSessionId(9)
            .timestamp(111);
        write(dir, "session_message_header", message.encodedLength());

        final TimerEventEncoder timer = new TimerEventEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(6)
            .correlationId(44)
            .timestamp(222);
        write(dir, "timer_event", timer.encodedLength());

        final SessionOpenEventEncoder open = new SessionOpenEventEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(7)
            .correlationId(45)
            .clusterSessionId(10)
            .timestamp(333)
            .responseStreamId(102)
            .responseChannel(RESPONSE_CHANNEL)
            .putEncodedPrincipal(new byte[] {1, 2, 3}, 0, 3);
        write(dir, "session_open_event", open.encodedLength());

        final SessionCloseEventEncoder sessionClose = new SessionCloseEventEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(8)
            .clusterSessionId(11)
            .timestamp(444)
            .closeReason(CloseReason.TIMEOUT);
        write(dir, "session_close_event", sessionClose.encodedLength());

        final ClusterActionRequestEncoder action = new ClusterActionRequestEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(9)
            .logPosition(16_384)
            .timestamp(555)
            .action(ClusterAction.SNAPSHOT)
            .flags(0);
        write(dir, "cluster_action_request", action.encodedLength());

        final NewLeadershipTermEventEncoder newTerm = new NewLeadershipTermEventEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(10)
            .logPosition(20_480)
            .timestamp(666)
            .termBaseLogPosition(18_432)
            .leaderMemberId(1)
            .logSessionId(-654_321)
            .timeUnit(ClusterTimeUnit.NANOS)
            .appVersion(SemanticVersion.compose(1, 2, 3));
        write(dir, "new_leadership_term_event", newTerm.encodedLength());

        final MembershipChangeEventEncoder membership = new MembershipChangeEventEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(11)
            .logPosition(24_576)
            .timestamp(777)
            .leaderMemberId(2)
            .clusterSize(3)
            .changeType(ChangeType.QUIT)
            .memberId(0)
            .clusterMembers(MEMBERS);
        write(dir, "membership_change_event", membership.encodedLength());

        // Snapshots.
        final SnapshotMarkerEncoder marker = new SnapshotMarkerEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .typeId(2)
            .logPosition(28_672)
            .leadershipTermId(12)
            .index(0)
            .mark(SnapshotMark.BEGIN)
            .timeUnit(ClusterTimeUnit.NANOS)
            .appVersion(SemanticVersion.compose(0, 0, 1));
        write(dir, "snapshot_marker", marker.encodedLength());

        final ClientSessionEncoder session = new ClientSessionEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .clusterSessionId(12)
            .responseStreamId(103)
            .responseChannel(RESPONSE_CHANNEL)
            .putEncodedPrincipal(new byte[0], 0, 0);
        write(dir, "client_session", session.encodedLength());

        consensusModule(dir);

        // The mark file's header.
        final io.aeron.cluster.codecs.mark.MessageHeaderEncoder markHeader =
            new io.aeron.cluster.codecs.mark.MessageHeaderEncoder();
        final MarkFileHeaderEncoder mark = new MarkFileHeaderEncoder().wrapAndApplyHeader(BUFFER, 0, markHeader)
            .version(SemanticVersion.compose(0, 3, 0))
            .componentType(ClusterComponentType.CONTAINER)
            .activityTimestamp(1_700_000_000_456L)
            .startTimestamp(1_700_000_000_000L)
            .pid(4_242)
            .candidateTermId(-1)
            .archiveStreamId(10)
            .serviceStreamId(104)
            .consensusModuleStreamId(105)
            .ingressStreamId(-1)
            .memberId(2)
            .serviceId(1)
            .headerLength(8_192)
            .errorBufferLength(1_048_576)
            .clusterId(0)
            .aeronDirectory("/tmp/aeron-node")
            .controlChannel("aeron:ipc?term-length=128k")
            .ingressChannel("")
            .serviceName("rusteron-service")
            .authenticator("")
            .servicesClusterDir("/tmp/cluster");
        write(dir, "mark_file_header", markHeader.encodedLength() + mark.encodedLength());

        // The consensus module's, as ConsensusModule.concludeMarkFile fills it.
        final MarkFileHeaderEncoder cmMark = new MarkFileHeaderEncoder().wrapAndApplyHeader(BUFFER, 0, markHeader)
            .version(SemanticVersion.compose(0, 3, 0))
            .componentType(ClusterComponentType.CONSENSUS_MODULE)
            .activityTimestamp(1_700_000_001_456L)
            .startTimestamp(1_700_000_001_000L)
            .pid(4_343)
            .candidateTermId(6)
            .archiveStreamId(10)
            .serviceStreamId(104)
            .consensusModuleStreamId(105)
            .ingressStreamId(101)
            .memberId(1)
            .serviceId(-1)
            .headerLength(8_192)
            .errorBufferLength(1_048_576)
            .clusterId(3)
            .aeronDirectory("/tmp/aeron-node1")
            .controlChannel("aeron:ipc?term-length=128k")
            .ingressChannel("aeron:udp?term-length=64k")
            .serviceName("")
            .authenticator("io.aeron.security.DefaultAuthenticatorSupplier")
            .servicesClusterDir("/tmp/cluster1");
        write(dir, "mark_file_header_consensus_module", markHeader.encodedLength() + cmMark.encodedLength());
    }

    /**
     * What the consensus module exchanges with clients, with the other members, and keeps in its snapshot.
     */
    private static void consensusModule(final Path dir) throws IOException
    {
        final int protocolVersion = SemanticVersion.compose(1, 0, 0);

        // Clients.
        final SessionEventEncoder event = new SessionEventEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .clusterSessionId(13)
            .correlationId(46)
            .leadershipTermId(14)
            .leaderMemberId(1)
            .code(EventCode.REDIRECT)
            .version(SemanticVersion.compose(0, 3, 0))
            .leaderHeartbeatTimeoutNs(10_000_000_000L)
            .detail("0=localhost:20110,1=localhost:20210");
        write(dir, "session_event", event.encodedLength());

        final SessionConnectRequestEncoder connect = new SessionConnectRequestEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .correlationId(47)
            .responseStreamId(102)
            .version(SemanticVersion.compose(0, 3, 0))
            .responseChannel(RESPONSE_CHANNEL)
            .putEncodedCredentials(new byte[] {4, 5}, 0, 2)
            .clientInfo("name=bench");
        write(dir, "session_connect_request", connect.encodedLength());

        final SessionCloseRequestEncoder closeRequest = new SessionCloseRequestEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(15)
            .clusterSessionId(16);
        write(dir, "session_close_request", closeRequest.encodedLength());

        final SessionKeepAliveEncoder keepAlive = new SessionKeepAliveEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(17)
            .clusterSessionId(18);
        write(dir, "session_keep_alive", keepAlive.encodedLength());

        final NewLeaderEventEncoder newLeader = new NewLeaderEventEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(19)
            .clusterSessionId(20)
            .leaderMemberId(2)
            .ingressEndpoints("0=localhost:20110,1=localhost:20210,2=localhost:20310");
        write(dir, "new_leader_event", newLeader.encodedLength());

        final ChallengeEncoder challenge = new ChallengeEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .correlationId(51)
            .clusterSessionId(24)
            .putEncodedChallenge(new byte[] {7, 8, 9}, 0, 3);
        write(dir, "challenge", challenge.encodedLength());

        final AdminRequestEncoder adminRequest = new AdminRequestEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(21)
            .clusterSessionId(22)
            .correlationId(48)
            .requestType(AdminRequestType.SNAPSHOT)
            .putPayload(new byte[0], 0, 0);
        write(dir, "admin_request", adminRequest.encodedLength());

        final AdminResponseEncoder adminResponse = new AdminResponseEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .clusterSessionId(23)
            .correlationId(49)
            .requestType(AdminRequestType.SNAPSHOT)
            .responseCode(AdminResponseCode.UNAUTHORISED_ACCESS)
            .message("Execution of the SNAPSHOT request was not authorised")
            .putPayload(new byte[0], 0, 0);
        write(dir, "admin_response", adminResponse.encodedLength());

        // Services, and ClusterTool, asking for the members.
        final ClusterMembersQueryEncoder query = new ClusterMembersQueryEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .correlationId(50)
            .extended(BooleanType.TRUE);
        write(dir, "cluster_members_query", query.encodedLength());

        final ClusterMembersResponseEncoder members = new ClusterMembersResponseEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .correlationId(51)
            .leaderMemberId(1)
            .activeMembers(MEMBERS)
            .passiveFollowers("");
        write(dir, "cluster_members_response", members.encodedLength());

        final ClusterMembersExtendedResponseEncoder extended = new ClusterMembersExtendedResponseEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .correlationId(52)
            .currentTimeNs(1_700_000_002_000_000_000L)
            .leaderMemberId(0)
            .memberId(1);
        final ClusterMembersExtendedResponseEncoder.ActiveMembersEncoder active = extended.activeMembersCount(2);
        for (int i = 0; i < 2; i++)
        {
            active.next()
                .leadershipTermId(26 + i)
                .logPosition(36_864 + i * 1_024)
                .timeOfLastAppendNs(1_700_000_001_000_000_000L + i)
                .memberId(i)
                .ingressEndpoint("localhost:2" + i + "110")
                .consensusEndpoint("localhost:2" + i + "111")
                .logEndpoint("localhost:2" + i + "112")
                .catchupEndpoint("localhost:2" + i + "113")
                .archiveEndpoint("localhost:2" + i + "114");
        }
        extended.passiveMembersCount(0);
        write(dir, "cluster_members_extended_response", extended.encodedLength());

        // The other members.
        final CanvassPositionEncoder canvass = new CanvassPositionEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .logLeadershipTermId(24)
            .logPosition(32_768)
            .leadershipTermId(25)
            .followerMemberId(2)
            .protocolVersion(protocolVersion);
        write(dir, "canvass_position", canvass.encodedLength());

        final RequestVoteEncoder requestVote = new RequestVoteEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .logLeadershipTermId(26)
            .logPosition(36_864)
            .candidateTermId(27)
            .candidateMemberId(1)
            .protocolVersion(protocolVersion);
        write(dir, "request_vote", requestVote.encodedLength());

        final VoteEncoder vote = new VoteEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .candidateTermId(28)
            .logLeadershipTermId(29)
            .logPosition(40_960)
            .candidateMemberId(1)
            .followerMemberId(2)
            .vote(BooleanType.TRUE);
        write(dir, "vote", vote.encodedLength());

        final NewLeadershipTermEncoder newLeadershipTerm = new NewLeadershipTermEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .logLeadershipTermId(30)
            .nextLeadershipTermId(31)
            .nextTermBaseLogPosition(45_056)
            .nextLogPosition(49_152)
            .leadershipTermId(32)
            .termBaseLogPosition(53_248)
            .logPosition(57_344)
            .leaderRecordingId(33)
            .timestamp(1_700_000_003_000L)
            .leaderMemberId(0)
            .logSessionId(-777)
            .appVersion(SemanticVersion.compose(0, 0, 1))
            .isStartup(BooleanType.FALSE)
            .commitPosition(55_296);
        write(dir, "new_leadership_term", newLeadershipTerm.encodedLength());

        final AppendPositionEncoder append = new AppendPositionEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(34)
            .logPosition(61_440)
            .followerMemberId(2)
            .flags((short)1);
        write(dir, "append_position", append.encodedLength());

        final CommitPositionEncoder commit = new CommitPositionEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(35)
            .logPosition(65_536)
            .leaderMemberId(1);
        write(dir, "commit_position", commit.encodedLength());

        final CatchupPositionEncoder catchup = new CatchupPositionEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(36)
            .logPosition(69_632)
            .followerMemberId(2)
            .catchupEndpoint("localhost:20313");
        write(dir, "catchup_position", catchup.encodedLength());

        final StopCatchupEncoder stopCatchup = new StopCatchupEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(37)
            .followerMemberId(2);
        write(dir, "stop_catchup", stopCatchup.encodedLength());

        final TerminationPositionEncoder terminationPosition = new TerminationPositionEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(38)
            .logPosition(73_728);
        write(dir, "termination_position", terminationPosition.encodedLength());

        final TerminationAckEncoder terminationAck = new TerminationAckEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .leadershipTermId(39)
            .logPosition(77_824)
            .memberId(2);
        write(dir, "termination_ack", terminationAck.encodedLength());

        // Backups and standbys.
        final BackupQueryEncoder backupQuery = new BackupQueryEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .correlationId(55)
            .responseStreamId(108)
            .version(protocolVersion)
            .logPosition(Long.MAX_VALUE)
            .responseChannel(RESPONSE_CHANNEL)
            .putEncodedCredentials(new byte[] {6, 7}, 0, 2);
        write(dir, "backup_query", backupQuery.encodedLength());

        final BackupResponseEncoder backupResponse = new BackupResponseEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .correlationId(56)
            .logRecordingId(2)
            .logLeadershipTermId(3)
            .logTermBaseLogPosition(4_096)
            .lastLeadershipTermId(3)
            .lastTermBaseLogPosition(4_096)
            .commitPositionCounterId(71)
            .leaderMemberId(1)
            .memberId(2);
        final BackupResponseEncoder.SnapshotsEncoder backupSnapshots = backupResponse.snapshotsCount(2);
        backupSnapshots.next()
            .recordingId(8)
            .leadershipTermId(3)
            .termBaseLogPosition(4_096)
            .logPosition(8_192)
            .timestamp(1_700_000_005_000L)
            .serviceId(0);
        backupSnapshots.next()
            .recordingId(9)
            .leadershipTermId(3)
            .termBaseLogPosition(4_096)
            .logPosition(8_192)
            .timestamp(1_700_000_005_000L)
            .serviceId(-1);
        backupResponse.clusterMembers(MEMBERS);
        write(dir, "backup_response", backupResponse.encodedLength());

        final HeartbeatRequestEncoder heartbeatRequest = new HeartbeatRequestEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .correlationId(57)
            .responseStreamId(108)
            .responseChannel(RESPONSE_CHANNEL)
            .putEncodedCredentials(new byte[0], 0, 0);
        write(dir, "heartbeat_request", heartbeatRequest.encodedLength());

        final HeartbeatResponseEncoder heartbeatResponse = new HeartbeatResponseEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .correlationId(57);
        write(dir, "heartbeat_response", heartbeatResponse.encodedLength());

        final StandbySnapshotEncoder standbySnapshot = new StandbySnapshotEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .correlationId(58)
            .version(protocolVersion)
            .responseStreamId(111);
        final StandbySnapshotEncoder.SnapshotsEncoder standbySnapshots = standbySnapshot.snapshotsCount(2);
        standbySnapshots.next()
            .recordingId(21)
            .leadershipTermId(4)
            .termBaseLogPosition(6_144)
            .logPosition(7_168)
            .timestamp(1_700_000_006_000L)
            .serviceId(0)
            .archiveEndpoint("localhost:21301");
        standbySnapshots.next()
            .recordingId(22)
            .leadershipTermId(4)
            .termBaseLogPosition(6_144)
            .logPosition(7_168)
            .timestamp(1_700_000_006_000L)
            .serviceId(-1)
            .archiveEndpoint("localhost:21301");
        standbySnapshot.responseChannel(RESPONSE_CHANNEL).putEncodedCredentials(new byte[0], 0, 0);
        write(dir, "standby_snapshot", standbySnapshot.encodedLength());

        final ChallengeResponseEncoder challengeResponse = new ChallengeResponseEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .correlationId(59)
            .clusterSessionId(-1)
            .putEncodedCredentials(new byte[] {8}, 0, 1);
        write(dir, "challenge_response", challengeResponse.encodedLength());

        // The consensus module's snapshot.
        final ClusterSessionEncoder clusterSession = new ClusterSessionEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .clusterSessionId(40)
            .correlationId(53)
            .openedLogPosition(81_920)
            .timeOfLastActivity(-1)
            .closeReason(CloseReason.NULL_VAL)
            .responseStreamId(102)
            .responseChannel(RESPONSE_CHANNEL);
        write(dir, "cluster_session", clusterSession.encodedLength());

        final TimerEncoder timer = new TimerEncoder().wrapAndApplyHeader(BUFFER, 0, HEADER)
            .correlationId(54)
            .deadline(1_700_000_004_000L);
        write(dir, "timer", timer.encodedLength());

        final ConsensusModuleEncoder consensusModule = new ConsensusModuleEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .nextSessionId(41)
            .nextServiceSessionId(Long.MIN_VALUE + 3)
            .logServiceSessionId(Long.MIN_VALUE + 2)
            .pendingMessageCapacity(128);
        write(dir, "consensus_module", consensusModule.encodedLength());

        final PendingMessageTrackerEncoder tracker = new PendingMessageTrackerEncoder()
            .wrapAndApplyHeader(BUFFER, 0, HEADER)
            .nextServiceSessionId((1L << 56) | Long.MIN_VALUE | 5)
            .logServiceSessionId((1L << 56) | Long.MIN_VALUE | 4)
            .pendingMessageCapacity(64)
            .serviceId(1);
        write(dir, "pending_message_tracker", tracker.encodedLength());
    }

    private static void write(final Path dir, final String name, final int bodyLength) throws IOException
    {
        final int length = name.startsWith("mark_file_header") ?
            bodyLength : MessageHeaderEncoder.ENCODED_LENGTH + bodyLength;
        final StringBuilder hex = new StringBuilder(length * 2 + 1);
        for (int i = 0; i < length; i++)
        {
            hex.append(String.format("%02x", BUFFER.getByte(i)));
        }
        hex.append('\n');
        Files.writeString(dir.resolve(name + ".hex"), hex.toString(), StandardCharsets.US_ASCII);
        // An encoder skips the bytes of a block no field covers: the next message starts on zeros, as in a new file.
        BUFFER.setMemory(0, BUFFER.capacity(), (byte)0);
    }
}
