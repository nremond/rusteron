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
 * container reads or writes, header included, as hex: one file a message in
 * the directory given. tests/golden.rs decodes each with the Rust codecs,
 * checks the values set here, encodes them again and compares the bytes.
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
    }

    private static void write(final Path dir, final String name, final int bodyLength) throws IOException
    {
        final int length = name.equals("mark_file_header") ? bodyLength : MessageHeaderEncoder.ENCODED_LENGTH + bodyLength;
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
