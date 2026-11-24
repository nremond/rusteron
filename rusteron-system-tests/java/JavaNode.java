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

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.NanosecondClusterClock;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.Aeron.NULL_VALUE;

/**
 * One node of a Java Aeron Cluster 1.54.2 on an external media driver (the C
 * driver from rusteron-media-driver): an archive, a consensus module, and the
 * Java services asked for. Services that are not Java (the Rust container) are
 * counted in {@code serviceCount} but started by someone else.
 *
 * <pre>
 * java JavaNode.java ROOT NODE_ID NODES AERON_DIR PORT_BASE SERVICE_COUNT JAVA_SERVICES
 * </pre>
 *
 * JAVA_SERVICES is a comma-separated list of {@code id=kind}, kind being
 * {@code echo}, {@code oracle} or {@code bench}; {@code -} for none. Prints
 * {@code READY} once launched and runs until the cluster terminates or the
 * process is signalled. Prints {@code ELECTION STATE Nms} each time the node's
 * election state counter changes, whichever consensus module allocated it, N
 * the time since the node started: a node restarted has rejoined once it
 * prints {@code ELECTION CLOSED}.
 *
 * <p>With {@code -Dcluster.bench=true}, the node is set up for measuring: the
 * consensus module's cluster clock in nanoseconds, and every agent (archive,
 * consensus module, services) busy-spinning.
 *
 * <p>With {@code -Dcluster.archive.external=true}, the node starts no archive:
 * one runs beside it on its driver (the Rust archive, from the scripts), with
 * the settings below.
 *
 * <p>With {@code -Dcluster.consensus.external=true}, the node starts no
 * consensus module: the Rust one runs beside it (from the scripts), and the
 * node ends when its Java services are terminated, its archive closed only
 * once that module has ended ({@code consensus-module.pid}, which the scripts
 * write), as a Java node closes its consensus module before its archive.
 */
public final class JavaNode
{
    static final int ARCHIVE_CONTROL = 1;
    static final int INGRESS = 2;
    static final int CONSENSUS = 3;
    static final int LOG = 4;
    static final int CATCHUP = 5;
    static final int PORTS_PER_NODE = 100;
    static final String LOCAL_CONTROL = "aeron:ipc?term-length=64k";

    public static void main(final String[] args)
    {
        // The barrier's shutdown hook holds the JVM until the barrier is closed: closed on every way out, a node
        // that failed to launch exits too.
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        try
        {
            run(args, barrier);
        }
        finally
        {
            barrier.close();
        }
    }

    static void run(final String[] args, final ShutdownSignalBarrier barrier)
    {
        final File root = new File(args[0]);
        final int nodeId = Integer.parseInt(args[1]);
        final int nodes = Integer.parseInt(args[2]);
        final String aeronDir = args[3];
        final int portBase = Integer.parseInt(args[4]);
        final int serviceCount = Integer.parseInt(args[5]);
        final String javaServices = args[6];

        final File dir = new File(root, "node" + nodeId);
        final boolean bench = Boolean.getBoolean("cluster.bench");
        final boolean externalArchive = Boolean.getBoolean("cluster.archive.external");
        final boolean externalConsensus = Boolean.getBoolean("cluster.consensus.external");

        final Archive.Context archiveContext = new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(new File(dir, "archive"))
            .archiveId(nodeId)
            .controlChannel(udp(port(portBase, nodeId, ARCHIVE_CONTROL)))
            .localControlChannel(LOCAL_CONTROL)
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .errorHandler(Throwable::printStackTrace);

        final AeronArchive.Context archiveClient = new AeronArchive.Context()
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(LOCAL_CONTROL)
            .controlResponseChannel(LOCAL_CONTROL)
            .aeronDirectoryName(aeronDir);

        final ConsensusModule.Context consensusContext = new ConsensusModule.Context()
            .aeronDirectoryName(aeronDir)
            .clusterMemberId(nodeId)
            .clusterMembers(members(portBase, nodes))
            .clusterDir(new File(dir, "cluster"))
            .serviceCount(serviceCount)
            .ingressChannel("aeron:udp?term-length=64k")
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .archiveContext(archiveClient.clone())
            .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(5))
            .leaderHeartbeatTimeoutNs(TimeUnit.MILLISECONDS.toNanos(1_000))
            .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
            .electionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500))
            .terminationHook(barrier::signalAll)
            .errorHandler(Throwable::printStackTrace);

        if (bench)
        {
            archiveContext.idleStrategySupplier(BusySpinIdleStrategy::new);
            consensusContext.clusterClock(new NanosecondClusterClock()).idleStrategySupplier(BusySpinIdleStrategy::new);
        }

        watchElection(aeronDir);
        final Archive archive = externalArchive ? null : Archive.launch(archiveContext);
        final ConsensusModule consensusModule = externalConsensus ? null : ConsensusModule.launch(consensusContext);
        final List<ClusteredServiceContainer> containers = new ArrayList<>();
        if (!"-".equals(javaServices))
        {
            for (final String spec : javaServices.split(","))
            {
                final String[] idAndKind = spec.split("=");
                final int serviceId = Integer.parseInt(idAndKind[0]);
                final String kind = idAndKind[1];
                final String file = new File(root, "node" + nodeId + "-service" + serviceId).getPath();
                final ClusteredServiceContainer.Context serviceContext = new ClusteredServiceContainer.Context()
                    .aeronDirectoryName(aeronDir)
                    .archiveContext(archiveClient.clone())
                    .clusterDir(new File(dir, "cluster"))
                    .serviceId(serviceId)
                    .isRespondingService(!kind.endsWith("-silent"))
                    .clusteredService(service(kind, file))
                    .terminationHook(externalConsensus ? barrier::signalAll : () -> {})
                    .errorHandler(Throwable::printStackTrace);
                if (bench)
                {
                    serviceContext.idleStrategySupplier(BusySpinIdleStrategy::new);
                }
                containers.add(ClusteredServiceContainer.launch(serviceContext));
            }
        }

        System.out.println("READY node=" + nodeId);
        System.out.flush();
        barrier.await();

        containers.forEach(CloseHelper::quietClose);
        if (externalConsensus && null != archive)
        {
            awaitExternalConsensusModule(dir);
        }
        CloseHelper.quietCloseAll(consensusModule, archive);
        System.out.println("STOPPED node=" + nodeId);
    }

    /**
     * Waits, 30 s at most, for the external consensus module whose pid the scripts wrote to
     * {@code consensus-module.pid} to end: until then it may still need this node's archive, its termination waiting
     * on the other members, and one whose local archive goes ends as failed, as Java's does.
     *
     * @param dir of the node.
     */
    static void awaitExternalConsensusModule(final File dir)
    {
        final File pidFile = new File(dir, "consensus-module.pid");
        if (!pidFile.exists())
        {
            return;
        }

        try
        {
            final long pid = Long.parseLong(Files.readString(pidFile.toPath()).trim());
            final ProcessHandle process = ProcessHandle.of(pid).orElse(null);
            if (null != process)
            {
                process.onExit().get(30, TimeUnit.SECONDS);
            }
        }
        catch (final Exception ex)
        {
            System.err.println("consensus module not awaited: " + ex);
        }
    }

    /**
     * Prints the election state counter's value as it changes, read from the
     * driver's CnC file every 10 ms on a daemon thread: the scripts wait on
     * it, since {@code READY} comes before the node's first election ends.
     */
    static void watchElection(final String aeronDir)
    {
        final MappedByteBuffer cnc = IoUtil.mapExistingFile(new File(aeronDir, CncFileDescriptor.CNC_FILE), "cnc");
        final UnsafeBuffer metaData = CncFileDescriptor.createMetaDataBuffer(cnc);
        final CountersReader counters = new CountersReader(
            CncFileDescriptor.createCountersMetaDataBuffer(cnc, metaData),
            CncFileDescriptor.createCountersValuesBuffer(cnc, metaData));
        final long startNs = System.nanoTime();
        final Thread thread = new Thread(
            () ->
            {
                long last = NULL_VALUE;
                while (true)
                {
                    final long[] state = { NULL_VALUE };
                    counters.forEach((counterId, typeId, keyBuffer, label) ->
                    {
                        if (ConsensusModule.Configuration.ELECTION_STATE_TYPE_ID == typeId)
                        {
                            state[0] = counters.getCounterValue(counterId);
                        }
                    });
                    if (NULL_VALUE != state[0] && last != state[0])
                    {
                        last = state[0];
                        System.out.println("ELECTION " + ElectionState.get(last) + " " +
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs) + "ms");
                        System.out.flush();
                    }
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                }
            },
            "election-watch");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * A service: {@code echo}; {@code oracle}, the twin of the Rust test service,
     * tracing to {@code FILE.trace}; or {@code bench}, the twin of the Rust bench
     * service, reporting to {@code FILE.bench}. {@code -silent} after any answers
     * no client.
     */
    static ClusteredService service(final String kind, final String file)
    {
        switch (kind.replace("-silent", ""))
        {
            case "echo":
                return new EchoService();
            case "oracle":
                return new OracleService(file + ".trace");
            case "bench":
                return new BenchService("java-apply", file + ".bench");
            default:
                throw new IllegalArgumentException("unknown service kind: " + kind);
        }
    }

    static int port(final int portBase, final int nodeId, final int offset)
    {
        return portBase + nodeId * PORTS_PER_NODE + offset;
    }

    static String udp(final int port)
    {
        return new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .termLength(64 * 1024)
            .endpoint("localhost:" + port)
            .build();
    }

    static String members(final int portBase, final int nodes)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nodes; i++)
        {
            sb.append(i)
                .append(",localhost:").append(port(portBase, i, INGRESS))
                .append(",localhost:").append(port(portBase, i, CONSENSUS))
                .append(",localhost:").append(port(portBase, i, LOG))
                .append(",localhost:").append(port(portBase, i, CATCHUP))
                .append(",localhost:").append(port(portBase, i, ARCHIVE_CONTROL))
                .append('|');
        }
        return sb.toString();
    }

    static String ingressEndpoints(final int portBase, final int nodes)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nodes; i++)
        {
            sb.append(i).append("=localhost:").append(port(portBase, i, INGRESS)).append(',');
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
}
