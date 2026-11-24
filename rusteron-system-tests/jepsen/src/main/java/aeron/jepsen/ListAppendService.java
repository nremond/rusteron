/*
 * Copyright 2026 the rusteron authors. Apache-2.0.
 */
package aeron.jepsen;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Elle's list-append state machine: lists of longs by key. Every transaction
 * goes through the log, reads too, and is answered on the client's egress by
 * the leader's service. Its snapshot is the lists, a key a message after a
 * count.
 */
public final class ListAppendService implements ClusteredService
{
    private final Map<Long, List<Long>> lists = new TreeMap<>();
    private final ExpandableArrayBuffer out = new ExpandableArrayBuffer(4096);
    private final IdleStrategy idle = new BackoffIdleStrategy();
    private Cluster cluster;

    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;
        if (null != snapshotImage)
        {
            load(snapshotImage);
        }
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
        final int count = buffer.getInt(offset + 8);
        out.putLong(0, buffer.getLong(offset));
        out.putInt(8, count);
        int in = offset + 12;
        int at = 12;
        for (int i = 0; i < count; i++)
        {
            final byte type = buffer.getByte(in);
            final long key = buffer.getLong(in + 1);
            final long value = buffer.getLong(in + 9);
            in += 17;
            out.putByte(at, type);
            out.putLong(at + 1, key);
            out.putLong(at + 9, value);
            if (type == Txn.APPEND)
            {
                lists.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
                out.putInt(at + 17, 0);
                at += Txn.OP_LENGTH;
            }
            else
            {
                final List<Long> list = lists.getOrDefault(key, List.of());
                out.putInt(at + 17, list.size());
                at += Txn.OP_LENGTH;
                for (final long v : list)
                {
                    out.putLong(at, v);
                    at += 8;
                }
            }
        }
        if (null != session)
        {
            // A follower's offer succeeds at once; the leader's may meet back pressure.
            for (int attempt = 0; attempt < 3 && session.offer(out, 0, at) < 0; attempt++)
            {
                idle.idle();
            }
            idle.reset();
        }
    }

    public void onTimerEvent(final long correlationId, final long timestamp)
    {
    }

    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1024);
        buffer.putLong(0, lists.size());
        offer(snapshotPublication, buffer, 8);
        for (final Map.Entry<Long, List<Long>> e : lists.entrySet())
        {
            buffer.putLong(0, e.getKey());
            buffer.putInt(8, e.getValue().size());
            int at = 12;
            for (final long v : e.getValue())
            {
                buffer.putLong(at, v);
                at += 8;
            }
            offer(snapshotPublication, buffer, at);
        }
    }

    private void offer(final ExclusivePublication publication, final DirectBuffer buffer, final int length)
    {
        while (publication.offer(buffer, 0, length) < 0)
        {
            if (publication.isClosed())
            {
                throw new IllegalStateException("snapshot publication closed");
            }
            idle.idle();
        }
        idle.reset();
    }

    private void load(final Image image)
    {
        final long[] expected = { -1 };
        final int[] loaded = { 0 };
        while (expected[0] < 0 || loaded[0] < expected[0])
        {
            final int fragments = image.poll(
                (buffer, offset, length, header) ->
                {
                    if (expected[0] < 0)
                    {
                        expected[0] = buffer.getLong(offset);
                        return;
                    }
                    final long key = buffer.getLong(offset);
                    final int n = buffer.getInt(offset + 8);
                    final List<Long> list = new ArrayList<>(n);
                    for (int j = 0; j < n; j++)
                    {
                        list.add(buffer.getLong(offset + 12 + 8 * j));
                    }
                    lists.put(key, list);
                    loaded[0]++;
                },
                16);
            if (0 == fragments)
            {
                if (image.isClosed() || image.isEndOfStream())
                {
                    throw new IllegalStateException("snapshot ended after " + loaded[0] + " of " + expected[0]);
                }
                cluster.idleStrategy().idle();
            }
        }
    }

    public void onRoleChange(final Cluster.Role newRole)
    {
    }

    public void onTerminate(final Cluster cluster)
    {
    }
}
