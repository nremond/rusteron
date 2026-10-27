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
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.CloseHelper;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * One node of a Java Aeron Cluster 1.53.3 on an external media driver (the C
 * driver from rusteron-media-driver): an archive, a consensus module, and the
 * Java services asked for. Services that are not Java (the Rust container) are
 * counted in {@code serviceCount} but started by someone else.
 *
 * <pre>
 * java JavaNode.java ROOT NODE_ID NODES AERON_DIR PORT_BASE SERVICE_COUNT JAVA_SERVICES
 * </pre>
 *
 * JAVA_SERVICES is a comma-separated list of {@code id=kind}, kind being
 * {@code echo}; {@code -} for none. Prints {@code READY} once launched and
 * runs until the cluster terminates or the process is signalled.
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
        final File root = new File(args[0]);
        final int nodeId = Integer.parseInt(args[1]);
        final int nodes = Integer.parseInt(args[2]);
        final String aeronDir = args[3];
        final int portBase = Integer.parseInt(args[4]);
        final int serviceCount = Integer.parseInt(args[5]);
        final String javaServices = args[6];

        final File dir = new File(root, "node" + nodeId);
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

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

        final Archive archive = Archive.launch(archiveContext);
        final ConsensusModule consensusModule = ConsensusModule.launch(consensusContext);
        final List<ClusteredServiceContainer> containers = new ArrayList<>();
        if (!"-".equals(javaServices))
        {
            for (final String spec : javaServices.split(","))
            {
                final String[] idAndKind = spec.split("=");
                final int serviceId = Integer.parseInt(idAndKind[0]);
                containers.add(ClusteredServiceContainer.launch(new ClusteredServiceContainer.Context()
                    .aeronDirectoryName(aeronDir)
                    .archiveContext(archiveClient.clone())
                    .clusterDir(new File(dir, "cluster"))
                    .serviceId(serviceId)
                    .clusteredService(service(idAndKind[1]))
                    .terminationHook(() -> {})
                    .errorHandler(Throwable::printStackTrace)));
            }
        }

        System.out.println("READY node=" + nodeId);
        System.out.flush();
        barrier.await();

        containers.forEach(CloseHelper::quietClose);
        CloseHelper.quietCloseAll(consensusModule, archive);
        System.out.println("STOPPED node=" + nodeId);
        // The barrier's shutdown hook waits for it to be closed before the JVM may exit on a signal.
        barrier.close();
    }

    static ClusteredService service(final String kind)
    {
        if ("echo".equals(kind))
        {
            return new EchoService();
        }
        throw new IllegalArgumentException("unknown service kind: " + kind);
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
