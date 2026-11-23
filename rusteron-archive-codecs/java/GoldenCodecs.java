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

import io.aeron.archive.codecs.*;
import io.aeron.archive.codecs.mark.MarkFileHeaderEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Writes, with Aeron 1.53.3's own Java codecs, every message the Rust archive
 * server reads or writes, as hex: one file a message in the directory given.
 * Requests (a client's) are what the server decodes; responses, signals, the
 * catalog's structures and the mark file's header are what it encodes.
 * tests/golden.rs decodes each with the Rust codecs, checks the values set
 * here, encodes them again and compares the bytes.
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
    static final String CHANNEL = "aeron:udp?endpoint=localhost:3333|alias=log|tags=7,8";
    static final String REPLAY_CHANNEL = "aeron:ipc?session-id=-77|alias=replay";
    static final String RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    static final String SRC_CONTROL = "aeron:udp?endpoint=localhost:8010";
    static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:8012";
    static final String ERROR = "unknown recording id: 3";

    private static final ExpandableArrayBuffer BUFFER = new ExpandableArrayBuffer(1024);
    private static final MessageHeaderEncoder HEADER = new MessageHeaderEncoder();
    private static Path dir;

    public static void main(final String[] args) throws IOException
    {
        dir = Path.of(args[0]);
        Files.createDirectories(dir);

        // A client's requests: what the server decodes.
        write("auth_connect", b -> new AuthConnectRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .correlationId(101).responseStreamId(20).version(0x01_0C_00)
            .responseChannel(RESPONSE_CHANNEL).putEncodedCredentials(new byte[0], 0, 0).clientInfo("cm-1")
            .encodedLength());
        write("archive_id", b -> new ArchiveIdRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(102).encodedLength());
        write("keep_alive", b -> new KeepAliveRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(103).encodedLength());
        write("close_session", b -> new CloseSessionRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).encodedLength());
        write("start_recording", b -> new StartRecordingRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(104).streamId(33).sourceLocation(SourceLocation.REMOTE)
            .channel(CHANNEL).encodedLength());
        write("stop_recording", b -> new StopRecordingRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(105).streamId(33).channel(CHANNEL).encodedLength());
        write("start_recording2", b -> new StartRecordingRequest2Encoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(106).streamId(34).sourceLocation(SourceLocation.LOCAL)
            .autoStop(BooleanType.TRUE).channel(CHANNEL).encodedLength());
        write("extend_recording2", b -> new ExtendRecordingRequest2Encoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(107).recordingId(5).streamId(35).sourceLocation(SourceLocation.REMOTE)
            .autoStop(BooleanType.FALSE).channel(CHANNEL).encodedLength());
        write("stop_recording_subscription", b -> new StopRecordingSubscriptionRequestEncoder()
            .wrapAndApplyHeader(b, 0, HEADER).controlSessionId(11).correlationId(108).subscriptionId(4_242)
            .encodedLength());
        write("stop_recording_by_identity", b -> new StopRecordingByIdentityRequestEncoder()
            .wrapAndApplyHeader(b, 0, HEADER).controlSessionId(11).correlationId(109).recordingId(6).encodedLength());
        write("replay", b -> new ReplayRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(110).recordingId(7).position(4_096).length(Long.MAX_VALUE)
            .replayStreamId(66).fileIoMaxLength(1 << 20).replayToken(-9).replayChannel(REPLAY_CHANNEL)
            .encodedLength());
        write("bounded_replay", b -> new BoundedReplayRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(111).recordingId(8).position(-1).length(-1).limitCounterId(23)
            .replayStreamId(67).fileIoMaxLength(65_536).replayToken(-10).replayChannel(REPLAY_CHANNEL)
            .encodedLength());
        write("stop_replay", b -> new StopReplayRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(112).replaySessionId((3L << 32) | 0x7777).encodedLength());
        write("list_recording", b -> new ListRecordingRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(113).recordingId(9).encodedLength());
        write("list_recordings_for_uri", b -> new ListRecordingsForUriRequestEncoder()
            .wrapAndApplyHeader(b, 0, HEADER).controlSessionId(11).correlationId(114).fromRecordingId(2)
            .recordCount(Integer.MAX_VALUE).streamId(200).channel("alias=demo-events").encodedLength());
        write("start_position", b -> new StartPositionRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(115).recordingId(10).encodedLength());
        write("stop_position", b -> new StopPositionRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(116).recordingId(11).encodedLength());
        write("recording_position", b -> new RecordingPositionRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(117).recordingId(12).encodedLength());
        write("truncate_recording", b -> new TruncateRecordingRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(118).recordingId(13).position(8_192).encodedLength());
        write("purge_recording", b -> new PurgeRecordingRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(119).recordingId(14).encodedLength());
        write("replicate2", b -> new ReplicateRequest2Encoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(120).srcRecordingId(15).dstRecordingId(-1).stopPosition(1_048_576)
            .channelTagId(-1).subscriptionTagId(-1).srcControlStreamId(10).fileIoMaxLength(8 << 20)
            .replicationSessionId(-55).srcControlChannel(SRC_CONTROL).liveDestination("")
            .replicationChannel(REPLICATION_CHANNEL).putEncodedCredentials(new byte[0], 0, 0)
            .srcResponseChannel("").encodedLength());
        write("stop_replication", b -> new StopReplicationRequestEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(121).replicationId(16).encodedLength());

        // The server's responses and signals: what it encodes.
        write("control_response_ok", b -> new ControlResponseEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(101).relevantId(11).code(ControlResponseCode.OK).version(0x01_0C_00)
            .errorMessage("").encodedLength());
        write("control_response_error", b -> new ControlResponseEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(113).relevantId(5).code(ControlResponseCode.ERROR).version(0x01_0C_00)
            .errorMessage(ERROR).encodedLength());
        write("recording_signal", b -> new RecordingSignalEventEncoder().wrapAndApplyHeader(b, 0, HEADER)
            .controlSessionId(11).correlationId(106).recordingId(17).subscriptionId(4_243).position(65_536)
            .signal(RecordingSignal.REPLICATE_END).encodedLength());
        write("ping", b -> new PingEncoder().wrapAndApplyHeader(b, 0, HEADER).controlSessionId(11).encodedLength());
        write("recording_descriptor", b -> descriptor(new RecordingDescriptorEncoder().wrapAndApplyHeader(b, 0, HEADER))
            .encodedLength());

        // The catalog's structures, raw blocks with no message header, as on disk.
        write("catalog_header", b -> new CatalogHeaderEncoder().wrap(b, 0)
            .version(0x03_01_00).length(32).nextRecordingId(18).alignment(64).encodedLength());
        write("catalog_entry", b ->
        {
            final RecordingDescriptorEncoder descriptor = descriptor(
                new RecordingDescriptorEncoder().wrap(b, RecordingDescriptorHeaderEncoder.BLOCK_LENGTH));
            final int length = descriptor.encodedLength();
            new RecordingDescriptorHeaderEncoder().wrap(b, 0)
                .length(length).state(RecordingState.VALID).checksum(0x1234_5678);
            return RecordingDescriptorHeaderEncoder.BLOCK_LENGTH + length;
        });

        // The mark file's header, its message header first.
        write("mark_file_header", b -> new MarkFileHeaderEncoder()
            .wrapAndApplyHeader(b, 0, new io.aeron.archive.codecs.mark.MessageHeaderEncoder())
            .version(0x03_01_00).activityTimestamp(1_700_000_000_123L).startTimestamp(1_700_000_000_000L).pid(4_321)
            .controlStreamId(10).localControlStreamId(11).eventsStreamId(30).headerLength(8_192)
            .errorBufferLength(1 << 20).archiveId(1_001).controlChannel("aeron:udp?endpoint=localhost:8010")
            .localControlChannel("aeron:ipc?term-length=64k").eventsChannel("aeron:udp?control-mode=dynamic")
            .aeronDirectory("/dev/shm/aeron-node0").encodedLength());
    }

    /** One recording's descriptor: what the catalog holds and ListRecording sends. */
    private static RecordingDescriptorEncoder descriptor(final RecordingDescriptorEncoder e)
    {
        return e.controlSessionId(0).correlationId(0).recordingId(17).startTimestamp(1_700_000_000_000L)
            .stopTimestamp(1_700_000_001_000L).startPosition(1_024).stopPosition(1_048_576).initialTermId(-5)
            .segmentFileLength(128 << 20).termBufferLength(64 << 10).mtuLength(1_408).sessionId(-123_456)
            .streamId(100).strippedChannel("aeron:udp?endpoint=localhost:3333|alias=log")
            .originalChannel(CHANNEL).sourceIdentity("127.0.0.1:52341");
    }

    private static void write(final String name, final java.util.function.ToIntFunction<MutableDirectBuffer> encode)
        throws IOException
    {
        BUFFER.setMemory(0, BUFFER.capacity(), (byte)0);
        // A message's encoded length leaves out its header (the mark file's too, the same 8 bytes); the
        // catalog's raw blocks have none.
        int length = encode.applyAsInt(BUFFER);
        if (!name.startsWith("catalog_"))
        {
            length += MessageHeaderEncoder.ENCODED_LENGTH;
        }
        final StringBuilder hex = new StringBuilder(length * 2);
        for (int i = 0; i < length; i++)
        {
            hex.append(String.format("%02x", BUFFER.getByte(i)));
        }
        Files.writeString(dir.resolve(name + ".hex"), hex + "\n", StandardCharsets.US_ASCII);
    }
}
