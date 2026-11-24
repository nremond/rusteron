/*
 * Copyright 2026 the rusteron authors. Apache-2.0.
 */
package aeron.jepsen;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * One Jepsen worker's connection to the cluster: an Aeron Cluster session,
 * connected again whenever it is lost. All workers share an embedded media
 * driver and Aeron client. A transaction ends one of three ways: answered,
 * {@link NotApplied} (it never left), or {@link Indeterminate} (it left, and
 * no answer came).
 */
public final class AppendClient implements AutoCloseable
{
    /** The transaction was not sent: it cannot have been applied. */
    public static final class NotApplied extends Exception
    {
        public NotApplied(final String message)
        {
            super(message);
        }
    }

    /** The transaction was sent and no answer came: it may have been applied. */
    public static final class Indeterminate extends Exception
    {
        public Indeterminate(final String message)
        {
            super(message);
        }
    }

    private static MediaDriver driver;
    private static Aeron aeron;

    private static synchronized Aeron aeron()
    {
        if (null == aeron)
        {
            driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .termBufferSparseFile(true));
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        }
        return aeron;
    }

    private final String ingressEndpoints;
    private final String egressChannel;
    private final ExpandableArrayBuffer request = new ExpandableArrayBuffer(1024);
    private final Map<Long, List<long[]>> responses = new HashMap<>();
    private final IdleStrategy idle = new BackoffIdleStrategy(10, 20, 1_000, 100_000);
    private AeronCluster cluster;
    private long nextRequestId = ThreadLocalRandom.current().nextLong(1L << 40) << 16;

    /**
     * @param ingressEndpoints the members' ingress, as `0=host:port,1=host:port,...`
     * @param egressHost       where this host is reachable from the members
     */
    public AppendClient(final String ingressEndpoints, final String egressHost)
    {
        this.ingressEndpoints = ingressEndpoints;
        this.egressChannel = "aeron:udp?endpoint=" + egressHost + ":0";
    }

    private void connect() throws NotApplied
    {
        try
        {
            cluster = AeronCluster.connect(new AeronCluster.Context()
                .aeron(aeron())
                .ownsAeronClient(false)
                .ingressChannel("aeron:udp?term-length=64k")
                .ingressEndpoints(ingressEndpoints)
                .egressChannel(egressChannel)
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .egressListener((clusterSessionId, timestamp, buffer, offset, length, header) ->
                    responses.put(Txn.requestId(buffer, offset), Txn.decodeResponse(buffer, offset))));
        }
        catch (final RuntimeException ex)
        {
            cluster = null;
            throw new NotApplied("connect: " + ex.getMessage());
        }
    }

    private void disconnect()
    {
        CloseHelper.quietClose(cluster);
        cluster = null;
        responses.clear();
    }

    /**
     * Runs a transaction: `ops` as [type, key, value]. Returns, for each
     * micro-op, the values a read saw, or null for an append.
     */
    public synchronized List<long[]> invoke(final long[][] ops, final long timeoutMs)
        throws NotApplied, Indeterminate
    {
        if (null == cluster || cluster.isClosed())
        {
            connect();
        }
        final long requestId = nextRequestId++;
        final int length = Txn.encodeRequest(request, requestId, ops);
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (true)
        {
            final long result = cluster.offer(request, 0, length);
            if (result > 0)
            {
                break;
            }
            if ((result != Publication.BACK_PRESSURED && result != Publication.ADMIN_ACTION) ||
                System.nanoTime() >= deadline)
            {
                disconnect();
                throw new NotApplied("offer: " + result);
            }
            cluster.pollEgress();
            idle.idle();
        }
        idle.reset();
        while (System.nanoTime() < deadline)
        {
            cluster.pollEgress();
            final List<long[]> response = responses.remove(requestId);
            if (null != response)
            {
                return response;
            }
            if (cluster.isClosed())
            {
                disconnect();
                throw new Indeterminate("session closed");
            }
            idle.idle();
        }
        // A session that answers nothing for a whole timeout is started afresh.
        disconnect();
        throw new Indeterminate("timed out");
    }

    public synchronized void close()
    {
        disconnect();
    }
}
