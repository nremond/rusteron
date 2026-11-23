/*
 * Copyright 2026 the rusteron authors. Apache-2.0.
 */
package aeron.jepsen;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * A transaction of Elle's list-append workload, as the client and the
 * service exchange it. A request is its id, its micro-ops' count, then each
 * micro-op: its type, key and value. The response repeats the request's id
 * and micro-ops, each followed by the values a read saw (none for an append).
 */
public final class Txn
{
    public static final byte APPEND = 0;
    public static final byte READ = 1;

    /** A micro-op's encoded length before the values a response carries. */
    static final int OP_LENGTH = 1 + 8 + 8 + 4;

    private Txn()
    {
    }

    /** Encodes a request: `ops` as [type, key, value] triples. */
    public static int encodeRequest(final MutableDirectBuffer buffer, final long requestId, final long[][] ops)
    {
        buffer.putLong(0, requestId);
        buffer.putInt(8, ops.length);
        int offset = 12;
        for (final long[] op : ops)
        {
            buffer.putByte(offset, (byte)op[0]);
            buffer.putLong(offset + 1, op[1]);
            buffer.putLong(offset + 9, op[2]);
            offset += 17;
        }
        return offset;
    }

    /** The request id a message starts with. */
    public static long requestId(final DirectBuffer buffer, final int offset)
    {
        return buffer.getLong(offset);
    }

    /** A response's reads: for each micro-op, the values a read saw, or null for an append. */
    public static List<long[]> decodeResponse(final DirectBuffer buffer, final int offset)
    {
        final int count = buffer.getInt(offset + 8);
        final List<long[]> reads = new ArrayList<>(count);
        int at = offset + 12;
        for (int i = 0; i < count; i++)
        {
            final byte type = buffer.getByte(at);
            final int n = buffer.getInt(at + 17);
            at += OP_LENGTH;
            if (type == READ)
            {
                final long[] values = new long[n];
                for (int j = 0; j < n; j++)
                {
                    values[j] = buffer.getLong(at);
                    at += 8;
                }
                reads.add(values);
            }
            else
            {
                reads.add(null);
            }
        }
        return reads;
    }
}
