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

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * The Java twin of rusteron-cluster-interop's TestService: the same logic,
 * so that the two, as services of one log, write the same trace.
 *
 * <ul>
 *     <li>A client's 8-byte message is echoed and counted. Every 10th
 *     schedules a timer one tick later, its id the message's position × 2 +
 *     the service id; every 25th sends a service message.</li>
 *     <li>"close" closes the session.</li>
 *     <li>Every entry applied is a trace line, and folds into a hash.</li>
 *     <li>The snapshot is the count and the hash, 16 bytes.</li>
 * </ul>
 */
public final class OracleService implements ClusteredService
{
    static final long FNV_OFFSET = 0xcbf29ce484222325L;
    static final long FNV_PRIME = 0x100000001b3L;

    private final FileOutputStream trace;
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
    private Cluster cluster;
    private long count;
    private long hash;

    public OracleService(final String tracePath)
    {
        try
        {
            trace = new FileOutputStream(tracePath, true);
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }

    static long fnv(final byte[] bytes, final int offset, final int length)
    {
        long h = FNV_OFFSET;
        for (int i = offset; i < offset + length; i++)
        {
            h ^= bytes[i] & 0xFF;
            h *= FNV_PRIME;
        }
        return h;
    }

    private void line(final String line)
    {
        final byte[] bytes = line.getBytes(StandardCharsets.US_ASCII);
        hash = (hash ^ fnv(bytes, 0, bytes.length)) * FNV_PRIME;
        try
        {
            trace.write((line + " " + Long.toHexString(hash) + "\n").getBytes(StandardCharsets.US_ASCII));
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }

    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;
        if (null != snapshotImage)
        {
            final UnsafeBuffer state = new UnsafeBuffer(new byte[16]);
            final int[] read = {0};
            while (read[0] < 16)
            {
                final int fragments = snapshotImage.poll((buf, offset, length, header) ->
                {
                    buf.getBytes(offset, state, read[0], length);
                    read[0] += length;
                }, 1);
                if (0 == fragments)
                {
                    if (snapshotImage.isClosed() || snapshotImage.isEndOfStream())
                    {
                        throw new IllegalStateException("snapshot ended after " + read[0] + " bytes");
                    }
                    cluster.idleStrategy().idle();
                }
            }
            count = state.getLong(0);
            hash = state.getLong(8);
        }
        line("start " + cluster.logPosition());
    }

    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
        line("open " + cluster.logPosition() + " " + timestamp + " " + session.id());
    }

    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
        line("close " + cluster.logPosition() + " " + timestamp + " " + session.id() + " " + closeReason.value());
    }

    public void onSessionMessage(
        final ClientSession session,
        final long timestamp,
        final DirectBuffer message,
        final int offset,
        final int length,
        final Header header)
    {
        final byte[] payload = new byte[length];
        message.getBytes(offset, payload);
        final long position = header.position();
        line("msg " + position + " " + timestamp + " " + (null == session ? "service" : Long.toString(session.id())) +
            " " + Long.toHexString(fnv(payload, 0, length)));
        if (null == session)
        {
            return;
        }
        if (length == 5 && "close".equals(new String(payload, StandardCharsets.US_ASCII)))
        {
            cluster.closeClientSession(session.id());
            return;
        }
        if (length == 8)
        {
            session.offer(message, offset, length);
            count++;
            if (count % 10 == 0)
            {
                while (!cluster.scheduleTimer(position * 2 + cluster.context().serviceId(), timestamp + 1))
                {
                    cluster.idleStrategy().idle();
                }
            }
            if (count % 25 == 0)
            {
                final int bytes = buffer.putStringWithoutLengthAscii(0, "svc" + cluster.context().serviceId() + ":" + position);
                while (cluster.offer(buffer, 0, bytes) < 0)
                {
                    cluster.idleStrategy().idle();
                }
            }
        }
    }

    public void onTimerEvent(final long correlationId, final long timestamp)
    {
        line("timer " + cluster.logPosition() + " " + timestamp + " " + correlationId);
    }

    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
        final UnsafeBuffer state = new UnsafeBuffer(new byte[16]);
        state.putLong(0, count);
        state.putLong(8, hash);
        while (snapshotPublication.offer(state, 0, 16) < 0)
        {
            cluster.idleStrategy().idle();
        }
    }

    public void onRoleChange(final Cluster.Role newRole)
    {
    }

    public void onTerminate(final Cluster cluster)
    {
        try
        {
            trace.close();
        }
        catch (final IOException ignore)
        {
        }
    }

    public void onNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final int leaderMemberId,
        final int logSessionId,
        final java.util.concurrent.TimeUnit timeUnit,
        final int appVersion)
    {
        line("term " + leadershipTermId + " " + logPosition + " " + timestamp);
    }
}
