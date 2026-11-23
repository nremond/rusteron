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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.ReplicationParams;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.CRC32;

/**
 * Java's archive client against an archive, Rust's or Java's alike, checking
 * every answer, the recordings' position counters and their signals; exits
 * non-zero on the first difference.
 *
 * <pre>
 * java -cp aeron-all-1.54.2.jar:. io.aeron.archive.ArchiveInterop AERON_DIR ARCHIVE_ID [MODE ...]
 * </pre>
 *
 * <ul>
 * <li>{@code run} (the default): records a stream, lists it, stops it, extends it; replays it whole and from
 * a position, bounded by a counter, and a recording while it is written; truncates and purges.</li>
 * <li>{@code record}: records a stream and prints {@code RECORDED id position} once the archive has it all,
 * then waits, publication open: the archive is killed meanwhile.</li>
 * <li>{@code recovered ID POSITION}: the recording, after the archive restarted, stopped where it had got to,
 * and replays whole.</li>
 * <li>{@code replicate SRC_CONTROL_CHANNEL}: records on the archive there, then has this one replicate it:
 * a new copy of a stopped recording (a cluster's snapshot), then that copy caught up to a position of the
 * recording extended and still recording (a cluster's log).</li>
 * </ul>
 */
public final class ArchiveInterop
{
    private static final String CHANNEL = "aeron:ipc?alias=interop|term-length=64k";
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int STREAM_ID = 1001;
    private static final List<String> SIGNALS = new ArrayList<>();
    /** Signals with their correlation and recording: {@code correlationId SIGNAL recordingId@position}. */
    private static final List<String> TAGGED_SIGNALS = new ArrayList<>();

    /**
     * What a stream carried: its messages and a checksum of them in order.
     */
    private static final class Sent
    {
        long position;
        int messages;
        final CRC32 crc = new CRC32();
    }

    public static void main(final String[] args) throws InterruptedException
    {
        final String aeronDir = args[0];
        final long expectedArchiveId = Long.parseLong(args[1]);
        final String mode = args.length > 2 ? args[2] : "run";
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
            AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .controlRequestChannel("aeron:ipc?term-length=64k")
                .controlRequestStreamId(10)
                .controlResponseChannel("aeron:ipc?term-length=64k")
                .controlResponseStreamId(20)
                .recordingSignalConsumer((controlSessionId, correlationId, recordingId, subscriptionId, position,
                    signal) ->
                {
                    SIGNALS.add(signal + "@" + position);
                    TAGGED_SIGNALS.add(correlationId + " " + signal + " " + recordingId + "@" + position);
                })))
        {
            check("archive id", expectedArchiveId, archive.archiveId());
            switch (mode)
            {
                case "run" -> run(aeron, archive);
                case "record" -> recordAndWait(aeron, archive);
                case "recovered" -> recovered(aeron, archive, Long.parseLong(args[3]), Long.parseLong(args[4]));
                case "replicate" -> replicate(aeron, archive, args[3]);
                default -> throw new IllegalArgumentException("unknown mode " + mode);
            }
        }
    }

    private static void run(final Aeron aeron, final AeronArchive archive)
    {
        final CountersReader counters = aeron.countersReader();
        final Sent first = new Sent();
        final Sent second = new Sent();
        final long recordingId;
        final long stopPosition;
        final int[] initialTermId = { 0 };
        try (ExclusivePublication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID))
        {
            final long subscriptionId = archive.startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL);
            final int counterId = awaitCounter(counters, publication.sessionId(), archive.archiveId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);
            check("counter's source", "aeron:ipc", RecordingPos.getSourceIdentity(counters, counterId));
            awaitSignal(archive, "START@0");

            publish(publication, 200, first);
            awaitPosition(counters, counterId, first.position);
            check("recording position", first.position, archive.getRecordingPosition(recordingId));
            check("stop position while recording", -1L, archive.getStopPosition(recordingId));
            check("start position", 0L, archive.getStartPosition(recordingId));

            final List<String> listed = new ArrayList<>();
            final int count = archive.listRecordingsForUri(0, 10, "alias=interop", STREAM_ID,
                (controlSessionId, correlationId, id, startTimestamp, stopTimestamp, startPosition,
                stop, termId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                strippedChannel, originalChannel, sourceIdentity) ->
                {
                    initialTermId[0] = termId;
                    listed.add(id + " " + startPosition + " " + stop + " " + termBufferLength + " " + mtuLength +
                        " " + (sessionId == publication.sessionId()) + " " + streamId + " " + strippedChannel +
                        " " + originalChannel + " " + sourceIdentity);
                });
            check("listed", 1, count);
            check("initial term id", publication.initialTermId(), initialTermId[0]);
            check("descriptor", recordingId + " 0 -1 65536 1408 true 1001 aeron:ipc?alias=interop " + CHANNEL +
                " aeron:ipc", listed.get(0));

            expectError("a second recording of the stream", ArchiveException.ACTIVE_SUBSCRIPTION,
                () -> archive.startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL));

            archive.stopRecording(subscriptionId);
            awaitSignal(archive, "STOP@" + first.position);
            stopPosition = archive.getStopPosition(recordingId);
            check("stop position", first.position, stopPosition);
        }

        // Extended from where it stopped, by a publication that starts there.
        final String extendChannel = new ChannelUriStringBuilder(CHANNEL)
            .initialPosition(stopPosition, initialTermId[0], 64 * 1024)
            .build();
        second.position = stopPosition;
        try (ExclusivePublication publication = aeron.addExclusivePublication(extendChannel, STREAM_ID))
        {
            archive.extendRecording(recordingId, CHANNEL, STREAM_ID, SourceLocation.LOCAL);
            final int counterId = awaitCounter(counters, publication.sessionId(), archive.archiveId());
            check("extended recording", recordingId, RecordingPos.getRecordingId(counters, counterId));
            awaitSignal(archive, "EXTEND@" + stopPosition);
            publish(publication, 3_000, second);
            awaitPosition(counters, counterId, second.position);
            check("extended past a segment's term", true, second.position > 2 * 64 * 1024);
            check("stopped by identity", true, archive.tryStopRecordingByIdentity(recordingId));
            awaitSignal(archive, "STOP@" + second.position);
            check("final stop position", second.position, archive.getStopPosition(recordingId));
        }

        final int[] described = { 0 };
        archive.listRecording(recordingId, (controlSessionId, correlationId, id, startTimestamp,
            stopTimestamp, startPosition, stop, termId, segmentFileLength, termBufferLength, mtuLength,
            sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> described[0]++);
        check("listed by id", 1, described[0]);
        expectError("an unknown recording's stop", ArchiveException.UNKNOWN_RECORDING,
            () -> archive.getStopPosition(recordingId + 1_000));

        replays(aeron, archive, recordingId, stopPosition, first, second);
        follow(aeron, archive);
        System.out.println("OK recording=" + recordingId + " signals=" + SIGNALS);
    }

    /**
     * The recording replayed whole, from a position, and bounded by a counter: what was published, in order.
     */
    private static void replays(
        final Aeron aeron,
        final AeronArchive archive,
        final long recordingId,
        final long stopPosition,
        final Sent first,
        final Sent second)
    {
        final int messages = first.messages + second.messages;
        final Replayed all = replay(aeron, archive.startReplay(recordingId, 0, AeronArchive.NULL_LENGTH,
            REPLAY_CHANNEL, 2001), 2001, second.position, first.messages);
        check("replayed", messages + " " + first.crc.getValue() + " " + second.crc.getValue(),
            all.messages + " " + all.first.getValue() + " " + all.rest.getValue());

        final Replayed tail = replay(aeron, archive.startReplay(recordingId, stopPosition, AeronArchive.NULL_LENGTH,
            REPLAY_CHANNEL, 2002), 2002, second.position, 0);
        check("replayed from a position", second.messages + " " + second.crc.getValue(),
            tail.messages + " " + tail.rest.getValue());

        expectError("a replay of an unknown recording", ArchiveException.UNKNOWN_RECORDING,
            () -> archive.startReplay(recordingId + 1_000, 0, AeronArchive.NULL_LENGTH, REPLAY_CHANNEL, 2003));
        expectError("a replay past the stop", ArchiveException.INVALID_POSITION,
            () -> archive.startReplay(recordingId, second.position, AeronArchive.NULL_LENGTH, REPLAY_CHANNEL, 2003));
        expectError("a replay off a frame", ArchiveException.GENERIC,
            () -> archive.startReplay(recordingId, 100, AeronArchive.NULL_LENGTH, REPLAY_CHANNEL, 2003));

        // Bounded by a counter: up to it, then on as it moves.
        try (Counter limit = aeron.addCounter(10_001, "interop replay limit"))
        {
            limit.setRelease(stopPosition);
            final long replaySessionId = archive.startBoundedReplay(
                recordingId, 0, AeronArchive.NULL_LENGTH, limit.id(), REPLAY_CHANNEL, 2004);
            try (Subscription subscription = aeron.addSubscription(
                ChannelUri.addSessionId(REPLAY_CHANNEL, (int)replaySessionId), 2004))
            {
                final Replayed bounded = new Replayed();
                final FragmentAssembler assembler = assembler(bounded);
                final Image image = awaitImage(subscription);
                pollUntil(subscription, assembler, () -> image.position() >= stopPosition, "the bound");
                final long deadline = System.currentTimeMillis() + 200;
                while (System.currentTimeMillis() < deadline)
                {
                    subscription.poll(assembler, 100);
                }
                check("held at the bound", stopPosition, image.position());
                limit.setRelease(second.position);
                pollUntil(subscription, assembler, () -> image.position() >= second.position, "the moved bound");
                check("bounded replay", messages, bounded.messages);
                archive.stopReplay(replaySessionId);
                pollUntil(subscription, assembler, image::isClosed, "the stopped replay's end");
            }
        }
    }

    /**
     * A recording replayed while it is written: the replay follows it, and ends where it stopped. Then
     * truncated and purged.
     */
    private static void follow(final Aeron aeron, final AeronArchive archive)
    {
        final String channel = "aeron:ipc?alias=live|term-length=64k";
        final Sent sent = new Sent();
        final Sent before = new Sent();
        final long recordingId;
        try (ExclusivePublication publication = aeron.addExclusivePublication(channel, 1002))
        {
            final long subscriptionId = archive.startRecording(channel, 1002, SourceLocation.LOCAL);
            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitCounter(counters, publication.sessionId(), archive.archiveId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);
            publish(publication, 100, before);
            awaitPosition(counters, counterId, before.position);

            final long replaySessionId = archive.startReplay(
                recordingId, 0, AeronArchive.NULL_LENGTH, REPLAY_CHANNEL, 2005);
            try (Subscription subscription = aeron.addSubscription(
                ChannelUri.addSessionId(REPLAY_CHANNEL, (int)replaySessionId), 2005))
            {
                final Replayed replayed = new Replayed();
                final FragmentAssembler assembler = assembler(replayed);
                final Image image = awaitImage(subscription);
                pollUntil(subscription, assembler, () -> image.position() >= before.position, "the live recording");
                sent.position = before.position;
                sent.messages = before.messages;
                publish(publication, 100, sent);
                pollUntil(subscription, assembler, () -> image.position() >= sent.position, "the recording's growth");
                archive.stopRecording(subscriptionId);
                awaitSignal(archive, "STOP@" + sent.position);
                pollUntil(subscription, assembler, image::isEndOfStream, "the end at the recording's stop");
                check("followed", sent.messages, replayed.messages);
            }
        }

        // Truncated after its first hundred messages, then purged.
        expectError("a truncation off a frame", ArchiveException.INVALID_POSITION,
            () -> archive.truncateRecording(recordingId, 100));
        check("files truncated", 0L, archive.truncateRecording(recordingId, before.position));
        awaitSignal(archive, "DELETE@-1");
        check("truncated stop", before.position, archive.getStopPosition(recordingId));
        final Replayed truncated = replay(aeron, archive.startReplay(recordingId, 0, AeronArchive.NULL_LENGTH,
            REPLAY_CHANNEL, 2006), 2006, before.position, 0);
        check("replay of the truncated", before.messages + " " + before.crc.getValue(),
            truncated.messages + " " + truncated.rest.getValue());
        SIGNALS.remove("DELETE@-1");
        check("files purged", 1L, archive.purgeRecording(recordingId));
        awaitSignal(archive, "DELETE@-1");
        final int[] listed = { 0 };
        check("purged listing", 0, archive.listRecording(recordingId, (controlSessionId, correlationId, id,
            startTimestamp, stopTimestamp, startPosition, stop, termId, segmentFileLength, termBufferLength,
            mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> listed[0]++));
    }

    private static void recordAndWait(final Aeron aeron, final AeronArchive archive) throws InterruptedException
    {
        final ExclusivePublication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID);
        archive.startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL);
        final CountersReader counters = aeron.countersReader();
        final int counterId = awaitCounter(counters, publication.sessionId(), archive.archiveId());
        final Sent sent = new Sent();
        publish(publication, 2_000, sent);
        awaitPosition(counters, counterId, sent.position);
        System.out.println("RECORDED " + RecordingPos.getRecordingId(counters, counterId) + " " + sent.position +
            " " + sent.messages);
        System.out.flush();
        Thread.sleep(60_000);
    }

    private static void recovered(
        final Aeron aeron, final AeronArchive archive, final long recordingId, final long position)
    {
        check("recovered stop position", position, archive.getStopPosition(recordingId));
        final Replayed replayed = replay(aeron, archive.startReplay(recordingId, 0, AeronArchive.NULL_LENGTH,
            REPLAY_CHANNEL, 2007), 2007, position, 0);
        check("recovered replay's end", position, replayed.position);
        System.out.println("OK recovered recording=" + recordingId + " position=" + position + " messages=" +
            replayed.messages);
    }

    private static void replicate(final Aeron aeron, final AeronArchive archive, final String srcControlChannel)
    {
        final String channel = "aeron:ipc?alias=repl|term-length=64k";
        try (AeronArchive src = AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .controlRequestChannel(srcControlChannel)
            .controlRequestStreamId(10)
            .controlResponseChannel("aeron:udp?endpoint=localhost:0")))
        {
            final CountersReader counters = aeron.countersReader();
            final Sent first = new Sent();
            final long srcRecordingId;
            final int initialTermId;
            try (ExclusivePublication publication = aeron.addExclusivePublication(channel, 1003))
            {
                final long subscriptionId = src.startRecording(channel, 1003, SourceLocation.LOCAL);
                final int counterId = awaitCounter(counters, publication.sessionId(), src.archiveId());
                srcRecordingId = RecordingPos.getRecordingId(counters, counterId);
                initialTermId = publication.initialTermId();
                publish(publication, 300, first);
                awaitPosition(counters, counterId, first.position);
                src.stopRecording(subscriptionId);
                awaitStopped(src, srcRecordingId);
            }

            // A new copy of a stopped recording, as a cluster copies a snapshot.
            final long snapshotId = archive.replicate(srcRecordingId, -1, 10, srcControlChannel, null);
            final long dstRecordingId = awaitTagged(archive, snapshotId, RecordingSignal.REPLICATE);
            awaitTagged(archive, snapshotId, RecordingSignal.STOP);
            check("snapshot copy's signals",
                List.of("REPLICATE " + dstRecordingId + "@0", "EXTEND " + dstRecordingId + "@0",
                    "SYNC " + dstRecordingId + "@" + first.position, "REPLICATE_END " + dstRecordingId + "@-1",
                    "STOP " + dstRecordingId + "@" + first.position),
                tagged(snapshotId));
            check("copy's stop", first.position, archive.getStopPosition(dstRecordingId));
            final Replayed copy = replay(aeron, archive.startReplay(dstRecordingId, 0, AeronArchive.NULL_LENGTH,
                REPLAY_CHANNEL, 2008), 2008, first.position, 0);
            check("copy's data", first.messages + " " + first.crc.getValue(),
                copy.messages + " " + copy.rest.getValue());

            // The source extended and still recording: the copy caught up to a position, as a cluster's log.
            final Sent second = new Sent();
            final String extendChannel = new ChannelUriStringBuilder(channel)
                .initialPosition(first.position, initialTermId, 64 * 1024)
                .build();
            try (ExclusivePublication publication = aeron.addExclusivePublication(extendChannel, 1003))
            {
                src.extendRecording(srcRecordingId, channel, 1003, SourceLocation.LOCAL);
                final int counterId = awaitCounter(counters, publication.sessionId(), src.archiveId());
                second.position = first.position;
                publish(publication, 300, second);
                awaitPosition(counters, counterId, second.position);

                final long logId = archive.replicate(srcRecordingId, 10, srcControlChannel, new ReplicationParams()
                    .dstRecordingId(dstRecordingId)
                    .stopPosition(second.position)
                    .replicationSessionId((int)aeron.nextCorrelationId())
                    .replicationChannel("aeron:udp?endpoint=localhost:0"));
                awaitTagged(archive, logId, RecordingSignal.STOP);
                awaitTagged(archive, logId, RecordingSignal.REPLICATE_END);
                check("log catch-up's signals",
                    List.of("EXTEND " + dstRecordingId + "@" + first.position,
                        "REPLICATE_END " + dstRecordingId + "@-1",
                        "STOP " + dstRecordingId + "@" + second.position),
                    tagged(logId));
                check("caught-up stop", second.position, archive.getStopPosition(dstRecordingId));
                expectError("an unknown replication's stop", ArchiveException.UNKNOWN_REPLICATION,
                    () -> archive.stopReplication(logId + 1_000));
            }
            final Replayed caughtUp = replay(aeron, archive.startReplay(dstRecordingId, 0, AeronArchive.NULL_LENGTH,
                REPLAY_CHANNEL, 2009), 2009, second.position, first.messages);
            check("caught-up data", first.crc.getValue() + " " + second.crc.getValue() + " " + 600,
                caughtUp.first.getValue() + " " + caughtUp.rest.getValue() + " " + caughtUp.messages);
            System.out.println("OK replicated " + srcRecordingId + " to " + dstRecordingId + " signals=" +
                TAGGED_SIGNALS);
        }
    }

    private static void awaitStopped(final AeronArchive archive, final long recordingId)
    {
        final long deadline = System.currentTimeMillis() + 10_000;
        while (-1 == archive.getStopPosition(recordingId))
        {
            if (System.currentTimeMillis() > deadline)
            {
                throw new IllegalStateException("recording " + recordingId + " never stopped");
            }
            Thread.onSpinWait();
        }
    }

    /** A replication's signals, in order: {@code SIGNAL recordingId@position}. */
    private static List<String> tagged(final long correlationId)
    {
        final List<String> signals = new ArrayList<>();
        for (final String tagged : TAGGED_SIGNALS)
        {
            final String prefix = correlationId + " ";
            if (tagged.startsWith(prefix))
            {
                signals.add(tagged.substring(prefix.length()));
            }
        }
        return signals;
    }

    /** Waits for a replication's signal: the recording it names. */
    private static long awaitTagged(final AeronArchive archive, final long correlationId, final RecordingSignal signal)
    {
        final long deadline = System.currentTimeMillis() + 10_000;
        while (true)
        {
            for (final String s : tagged(correlationId))
            {
                if (s.startsWith(signal.name() + " "))
                {
                    return Long.parseLong(s.substring(signal.name().length() + 1, s.indexOf('@')));
                }
            }
            if (System.currentTimeMillis() > deadline)
            {
                throw new IllegalStateException("no " + signal + " for " + correlationId + " in " + TAGGED_SIGNALS);
            }
            archive.pollForRecordingSignals();
            Thread.onSpinWait();
        }
    }

    /**
     * What a replay delivered: its messages, a checksum of its first {@code split} and one of the rest, and
     * the position it ended at.
     */
    private static final class Replayed
    {
        int split;
        int messages;
        long position;
        final CRC32 first = new CRC32();
        final CRC32 rest = new CRC32();
    }

    /**
     * A replay read to `end`, then to its end of stream.
     */
    private static Replayed replay(
        final Aeron aeron, final long replaySessionId, final int streamId, final long end, final int split)
    {
        final Replayed replayed = new Replayed();
        replayed.split = split;
        try (Subscription subscription = aeron.addSubscription(
            ChannelUri.addSessionId(REPLAY_CHANNEL, (int)replaySessionId), streamId))
        {
            final FragmentAssembler assembler = assembler(replayed);
            final Image image = awaitImage(subscription);
            pollUntil(subscription, assembler, () -> image.position() >= end, "the replay's end position");
            pollUntil(subscription, assembler, image::isEndOfStream, "the replay's end of stream");
            replayed.position = image.position();
        }
        return replayed;
    }

    private static FragmentAssembler assembler(final Replayed into)
    {
        return new FragmentAssembler((buffer, offset, length, header) ->
        {
            final byte[] bytes = new byte[length];
            buffer.getBytes(offset, bytes);
            (into.messages < into.split ? into.first : into.rest).update(bytes);
            into.messages++;
        });
    }

    private static void publish(final ExclusivePublication publication, final int messages, final Sent sent)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4_096]);
        for (int i = 0; i < messages; i++)
        {
            // Some messages longer than the MTU: fragmented.
            final int length = 1 + ThreadLocalRandom.current().nextInt(i % 10 == 0 ? 4_000 : 200);
            ThreadLocalRandom.current().nextBytes(buffer.byteArray());
            long position;
            while ((position = publication.offer(buffer, 0, length)) < 0)
            {
                Thread.onSpinWait();
            }
            sent.position = position;
            sent.messages++;
            sent.crc.update(buffer.byteArray(), 0, length);
        }
    }

    private interface Condition
    {
        boolean met();
    }

    private static void pollUntil(
        final Subscription subscription, final FragmentAssembler assembler, final Condition condition,
        final String what)
    {
        final long deadline = System.currentTimeMillis() + 10_000;
        while (!condition.met())
        {
            if (System.currentTimeMillis() > deadline)
            {
                throw new IllegalStateException("timed out waiting for " + what);
            }
            if (0 == subscription.poll(assembler, 100))
            {
                Thread.onSpinWait();
            }
        }
    }

    private static Image awaitImage(final Subscription subscription)
    {
        final long deadline = System.currentTimeMillis() + 10_000;
        while (0 == subscription.imageCount())
        {
            if (System.currentTimeMillis() > deadline)
            {
                throw new IllegalStateException("no image on " + subscription.channel());
            }
            Thread.onSpinWait();
        }
        return subscription.imageAtIndex(0);
    }

    private interface Call
    {
        void call();
    }

    private static void expectError(final String what, final int errorCode, final Call call)
    {
        try
        {
            call.call();
            throw new IllegalStateException(what + " was accepted");
        }
        catch (final ArchiveException ex)
        {
            check(what + "'s code (" + ex.getMessage() + ")", errorCode, ex.errorCode());
        }
    }

    private static int awaitCounter(final CountersReader counters, final int sessionId, final long archiveId)
    {
        final long deadline = System.currentTimeMillis() + 10_000;
        int counterId;
        while (CountersReader.NULL_COUNTER_ID ==
            (counterId = RecordingPos.findCounterIdBySession(counters, sessionId, archiveId)))
        {
            if (System.currentTimeMillis() > deadline)
            {
                throw new IllegalStateException("no recording position counter for session " + sessionId);
            }
            Thread.onSpinWait();
        }
        return counterId;
    }

    private static void awaitPosition(final CountersReader counters, final int counterId, final long position)
    {
        final long deadline = System.currentTimeMillis() + 10_000;
        while (counters.getCounterValue(counterId) < position)
        {
            if (System.currentTimeMillis() > deadline)
            {
                throw new IllegalStateException("recorded " + counters.getCounterValue(counterId) + " of " + position);
            }
            Thread.onSpinWait();
        }
    }

    private static void awaitSignal(final AeronArchive archive, final String signal)
    {
        final long deadline = System.currentTimeMillis() + 10_000;
        while (!SIGNALS.contains(signal))
        {
            if (System.currentTimeMillis() > deadline)
            {
                throw new IllegalStateException("no " + signal + " in " + SIGNALS);
            }
            archive.pollForRecordingSignals();
            Thread.onSpinWait();
        }
    }

    private static void check(final String what, final Object expected, final Object actual)
    {
        if (!expected.equals(actual))
        {
            throw new IllegalStateException(what + ": expected " + expected + ", was " + actual);
        }
    }
}
