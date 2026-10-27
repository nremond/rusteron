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
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.concurrent.TimeUnit;

/**
 * A Java Aeron Cluster 1.53.3 client on an external media driver: sends COUNT
 * messages, each its sequence number, and waits for each echo in order.
 * Prints {@code OK} and exits 0 when all came back, exits 1 otherwise.
 *
 * <pre>
 * java EchoClient.java AERON_DIR PORT_BASE NODES COUNT
 * </pre>
 */
public final class EchoClient implements EgressListener
{
    private long received;
    private long outOfOrder;

    public static void main(final String[] args)
    {
        final String aeronDir = args[0];
        final int portBase = Integer.parseInt(args[1]);
        final int nodes = Integer.parseInt(args[2]);
        final long count = Long.parseLong(args[3]);

        final EchoClient client = new EchoClient();
        final IdleStrategy idle = new BackoffIdleStrategy();
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(8);
        final long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);

        try (AeronCluster cluster = AeronCluster.connect(new AeronCluster.Context()
            .aeronDirectoryName(aeronDir)
            .egressListener(client)
            .egressChannel("aeron:udp?endpoint=localhost:0")
            .ingressChannel("aeron:udp?term-length=64k")
            .ingressEndpoints(JavaNode.ingressEndpoints(portBase, nodes))
            .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10))))
        {
            for (long sent = 0; sent < count; )
            {
                buffer.putLong(0, sent);
                if (cluster.offer(buffer, 0, 8) > 0)
                {
                    sent++;
                }
                cluster.pollEgress();
            }
            while (client.received < count && System.nanoTime() < deadlineNs)
            {
                idle.idle(cluster.pollEgress());
            }
            System.out.println((client.received == count && client.outOfOrder == 0 ? "OK" : "FAILED") +
                " received=" + client.received + " outOfOrder=" + client.outOfOrder +
                " leader=" + cluster.leaderMemberId());
        }
        System.exit(client.received == count && client.outOfOrder == 0 ? 0 : 1);
    }

    public void onMessage(
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        if (buffer.getLong(offset) != received)
        {
            outOfOrder++;
        }
        received++;
    }
}
