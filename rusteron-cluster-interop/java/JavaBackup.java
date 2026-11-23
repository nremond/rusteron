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

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterBackupEventsListener;
import io.aeron.cluster.ClusterMember;
import io.aeron.cluster.RecordingLog;
import org.agrona.CloseHelper;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Java's ClusterBackup 1.54.2 with an archive of its own, on an external media
 * driver, beside a cluster of {@link JavaNode}s: the oracle Rust's
 * {@code cluster_backup} is compared with. It takes the slot of node
 * {@code SLOT}'s ports: its archive's control channel, the consensus endpoint
 * members answer it on, the catch-up endpoint their archives replay to.
 *
 * <pre>
 * java JavaBackup.java DIR AERON_DIR PORT_BASE NODES SLOT SOURCE_TYPE
 * </pre>
 *
 * Prints {@code READY} once launched, {@code RESPONSE member=N snapshots=K} as
 * members answer, and {@code PROGRESS recordingId=R position=P} as the live log
 * grows: the lines {@code cluster_backup --progress} prints. Runs until
 * signalled.
 */
public final class JavaBackup implements ClusterBackupEventsListener
{
    public static void main(final String[] args)
    {
        final File dir = new File(args[0]);
        final String aeronDir = args[1];
        final int portBase = Integer.parseInt(args[2]);
        final int nodes = Integer.parseInt(args[3]);
        final int slot = Integer.parseInt(args[4]);
        final ClusterBackup.SourceType sourceType = ClusterBackup.SourceType.valueOf(args[5]);

        final StringBuilder consensusEndpoints = new StringBuilder();
        for (int n = 0; n < nodes; n++)
        {
            consensusEndpoints.append(n == 0 ? "" : ",").append(endpoint(portBase, n, JavaNode.CONSENSUS));
        }

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        Archive archive = null;
        ClusterBackup backup = null;
        try
        {
            archive = Archive.launch(new Archive.Context()
                .aeronDirectoryName(aeronDir)
                .archiveDir(new File(dir, "archive"))
                .archiveId(slot)
                .controlChannel(JavaNode.udp(JavaNode.port(portBase, slot, JavaNode.ARCHIVE_CONTROL)))
                .localControlChannel(JavaNode.LOCAL_CONTROL)
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .recordingEventsEnabled(false)
                .deleteArchiveOnStart(true)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .errorHandler(Throwable::printStackTrace));
            backup = ClusterBackup.launch(new ClusterBackup.Context()
                .aeronDirectoryName(aeronDir)
                .clusterDir(new File(dir, "cluster"))
                .deleteDirOnStart(true)
                .clusterConsensusEndpoints(consensusEndpoints.toString())
                .consensusChannel(
                    "aeron:udp?term-length=64k|endpoint=" + endpoint(portBase, slot, JavaNode.CONSENSUS))
                .catchupEndpoint(endpoint(portBase, slot, JavaNode.CATCHUP))
                .sourceType(sourceType)
                .clusterBackupCoolDownIntervalNs(TimeUnit.SECONDS.toNanos(1))
                .archiveContext(new AeronArchive.Context()
                    .aeronDirectoryName(aeronDir)
                    .controlRequestChannel(JavaNode.LOCAL_CONTROL)
                    .controlResponseChannel(JavaNode.LOCAL_CONTROL))
                .clusterArchiveContext(new AeronArchive.Context()
                    .aeronDirectoryName(aeronDir)
                    .controlRequestChannel("aeron:udp?term-length=64k")
                    .controlResponseChannel("aeron:udp?endpoint=localhost:0"))
                .eventsListener(new JavaBackup())
                .errorHandler(Throwable::printStackTrace));

            System.out.println("READY");
            System.out.flush();
            barrier.await();
        }
        finally
        {
            CloseHelper.quietCloseAll(backup, archive);
            barrier.close();
        }
        System.out.println("STOPPED");
    }

    static String endpoint(final int portBase, final int node, final int offset)
    {
        return "localhost:" + JavaNode.port(portBase, node, offset);
    }

    public void onBackupQuery()
    {
    }

    public void onPossibleFailure(final Exception ex)
    {
        System.out.println("POSSIBLE FAILURE " + ex.getMessage());
        System.out.flush();
    }

    public void onBackupResponse(
        final ClusterMember[] clusterMembers,
        final ClusterMember logSourceMember,
        final List<RecordingLog.Snapshot> snapshotsToRetrieve)
    {
        System.out.println("RESPONSE member=" + logSourceMember.id() + " snapshots=" + snapshotsToRetrieve.size());
        System.out.flush();
    }

    public void onUpdatedRecordingLog(
        final RecordingLog recordingLog, final List<RecordingLog.Snapshot> snapshotsRetrieved)
    {
    }

    public void onLiveLogProgress(final long recordingId, final long recordingPosCounterId, final long logPosition)
    {
        System.out.println("PROGRESS recordingId=" + recordingId + " position=" + logPosition);
        System.out.flush();
    }
}
