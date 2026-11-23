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

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.Header;
import io.aeron.shadow.org.HdrHistogram.Histogram; // HdrHistogram, as aeron-all ships it
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * An open-loop client of a bench cluster: sends 32-byte messages at RATE a
 * second, busy-spinning, for WARMUP_S then MEASURE_S seconds, and times each
 * echo from the moment its message was due to be sent, not from when it was:
 * a stall delays what is queued behind it, and the delay counts (no
 * coordinated omission). A message carries the time it was due (bytes 0-7)
 * and whether it is measured (byte 8, 1 after the warm-up), which the
 * services read too. Writes its report line, in microseconds, to REPORT:
 * {@code NAME COUNT P50 P90 P99 P99.9 P99.99 MAX}, and exits 0 when every
 * echo came back.
 *
 * <pre>
 * java BenchClient.java AERON_DIR PORT_BASE NODES RATE WARMUP_S MEASURE_S REPORT NAME
 * </pre>
 */
public final class BenchClient implements EgressListener
{
    private final Histogram histogram = new Histogram(10_000_000_000L, 3);
    private long received;

    public static void main(final String[] args) throws Exception
    {
        final String aeronDir = args[0];
        final int portBase = Integer.parseInt(args[1]);
        final int nodes = Integer.parseInt(args[2]);
        final long rate = Long.parseLong(args[3]);
        final long warmupNs = TimeUnit.SECONDS.toNanos(Long.parseLong(args[4]));
        final long measureNs = TimeUnit.SECONDS.toNanos(Long.parseLong(args[5]));
        final String report = args[6];
        final String name = args[7];

        final BenchClient client = new BenchClient();
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[32]);
        final long intervalNs = TimeUnit.SECONDS.toNanos(1) / rate;
        long sent = 0;

        try (AeronCluster cluster = AeronCluster.connect(new AeronCluster.Context()
            .aeronDirectoryName(aeronDir)
            .egressListener(client)
            .egressChannel("aeron:udp?endpoint=localhost:0")
            .ingressChannel("aeron:udp?term-length=64k")
            .ingressEndpoints(JavaNode.ingressEndpoints(portBase, nodes))
            .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10))))
        {
            final long start = System.nanoTime();
            final long measureFrom = start + warmupNs;
            final long end = measureFrom + measureNs;
            long due = start;
            while (due < end)
            {
                if (System.nanoTime() >= due)
                {
                    buffer.putLong(0, due);
                    buffer.putByte(8, (byte)(due >= measureFrom ? 1 : 0));
                    if (cluster.offer(buffer, 0, 32) > 0)
                    {
                        sent++;
                        due += intervalNs;
                    }
                }
                cluster.pollEgress();
            }
            final long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (client.received < sent && System.nanoTime() < drainDeadline)
            {
                cluster.pollEgress();
            }
        }

        final String line = BenchService.reportLine(name, client.histogram);
        Files.writeString(Path.of(report), line + "\n");
        System.out.println(line + " (sent " + sent + ", received " + client.received + ")");
        System.exit(client.received == sent ? 0 : 1);
    }

    public void onMessage(
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        final long now = System.nanoTime();
        received++;
        if (buffer.getByte(offset + 8) == 1)
        {
            histogram.recordValue(Math.min(now - buffer.getLong(offset), histogram.getHighestTrackableValue()));
        }
    }
}
