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
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import io.aeron.shadow.org.HdrHistogram.Histogram; // HdrHistogram, as aeron-all ships it
import org.agrona.DirectBuffer;
import org.agrona.concurrent.HighResolutionClock;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The Java twin of rusteron-system-tests's bench service: every client
 * message is echoed, and the time from its log timestamp (the consensus
 * module's nanosecond cluster clock) to its apply is recorded, for messages
 * the client marked as measured (byte 8 is 1). At termination the histogram
 * goes to a report file, one line in microseconds:
 * {@code NAME COUNT P50 P90 P99 P99.9 P99.99 MAX}.
 */
public final class BenchService implements ClusteredService
{
    private final String name;
    private final String report;
    private final Histogram histogram = new Histogram(10_000_000_000L, 3);
    private Cluster cluster;

    public BenchService(final String name, final String report)
    {
        this.name = name;
        this.report = report;
    }

    static String reportLine(final String name, final Histogram histogram)
    {
        return String.format("%s %d %.1f %.1f %.1f %.1f %.1f %.1f",
            name,
            histogram.getTotalCount(),
            histogram.getValueAtPercentile(50) / 1000.0,
            histogram.getValueAtPercentile(90) / 1000.0,
            histogram.getValueAtPercentile(99) / 1000.0,
            histogram.getValueAtPercentile(99.9) / 1000.0,
            histogram.getValueAtPercentile(99.99) / 1000.0,
            histogram.getMaxValue() / 1000.0);
    }

    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;
    }

    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
    }

    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
    }

    public void onSessionMessage(
        final ClientSession session,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        final long now = HighResolutionClock.epochNanos();
        if (length > 8 && buffer.getByte(offset + 8) == 1)
        {
            histogram.recordValue(Math.min(Math.max(0, now - timestamp), histogram.getHighestTrackableValue()));
        }
        if (null != session)
        {
            long result;
            while ((result = session.offer(buffer, offset, length)) == Publication.BACK_PRESSURED ||
                result == Publication.ADMIN_ACTION)
            {
                cluster.idleStrategy().idle();
            }
        }
    }

    public void onTimerEvent(final long correlationId, final long timestamp)
    {
    }

    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
    }

    public void onRoleChange(final Cluster.Role newRole)
    {
    }

    public void onTerminate(final Cluster cluster)
    {
        try
        {
            Files.writeString(Path.of(report), reportLine(name, histogram) + "\n");
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }
}
