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

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;

import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

/**
 * The log as archives replay it: for each archive, its newest recording of the
 * log stream, replayed over UDP from its start to {@code POSITION}, and the
 * CRC-32C of every fragment's bytes in order, as one compares a standby's log
 * with a member's.
 *
 * <pre>
 * java BackupCheck.java AERON_DIR PORT_BASE POSITION ARCHIVE_ENDPOINT...
 * </pre>
 *
 * Prints {@code endpoint recording=R start=S recorded=E position=P crc=C} for
 * each archive, E how far it has recorded; exits 1 if one cannot be replayed
 * to {@code POSITION}.
 */
public final class BackupCheck
{
    static final int LOG_STREAM = 100;
    static final int REPLAY_STREAM = 9_001;

    public static void main(final String[] args) throws InterruptedException
    {
        final String aeronDir = args[0];
        final int portBase = Integer.parseInt(args[1]);
        final long position = Long.parseLong(args[2]);
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir)))
        {
            for (int i = 3; i < args.length; i++)
            {
                final String endpoint = args[i];
                try (AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .controlRequestChannel("aeron:udp?endpoint=" + endpoint)
                    .controlResponseChannel("aeron:udp?endpoint=localhost:0")))
                {
                    final long recordingId = logRecording(archive);
                    final long start = archive.getStartPosition(recordingId);
                    final long active = archive.getRecordingPosition(recordingId);
                    final long recorded = active >= 0 ? active : archive.getStopPosition(recordingId);
                    final long crc = crc(aeron, archive, recordingId, start, position, portBase + 900 + i);
                    System.out.printf("%s recording=%d start=%d recorded=%d position=%d crc=%08x%n",
                        endpoint, recordingId, start, recorded, position, crc);
                }
            }
        }
    }

    /** The archive's newest recording of the log stream. */
    static long logRecording(final AeronArchive archive)
    {
        final long[] newest = { -1 };
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", LOG_STREAM,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                strippedChannel, originalChannel, sourceIdentity) -> newest[0] = Math.max(newest[0], recordingId));
        if (newest[0] < 0)
        {
            System.out.println("no log recording");
            System.exit(1);
        }
        return newest[0];
    }

    static long crc(
        final Aeron aeron,
        final AeronArchive archive,
        final long recordingId,
        final long start,
        final long position,
        final int port) throws InterruptedException
    {
        final String channel = "aeron:udp?endpoint=localhost:" + port;
        final CRC32C crc = new CRC32C();
        try (Subscription subscription = aeron.addSubscription(channel, REPLAY_STREAM))
        {
            final int sessionId = (int)archive.startReplay(recordingId, start, position - start, channel, REPLAY_STREAM);
            final long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            Image image;
            while ((image = subscription.imageBySessionId(sessionId)) == null)
            {
                check(System.nanoTime() < deadlineNs, "no replay of recording " + recordingId);
                Thread.sleep(1);
            }
            final byte[] bytes = new byte[64 * 1024];
            while (image.position() < position)
            {
                final int fragments = image.poll((buffer, offset, length, header) ->
                {
                    buffer.getBytes(offset, bytes, 0, length);
                    crc.update(bytes, 0, length);
                }, 64);
                if (0 == fragments)
                {
                    check(System.nanoTime() < deadlineNs && !image.isClosed(),
                        "the replay of recording " + recordingId + " reached " + image.position() + " of " + position);
                    Thread.sleep(1);
                }
            }
            check(image.position() == position, "replayed past " + position + " to " + image.position());
        }
        return crc.getValue();
    }

    static void check(final boolean condition, final String message)
    {
        if (!condition)
        {
            System.out.println("FAILED " + message);
            System.exit(1);
        }
    }
}
