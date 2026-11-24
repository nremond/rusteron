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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.service.ClusterCounters;
import io.aeron.cluster.service.RecoveryState;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.cluster.ConsensusModule.Configuration.SERVICE_ID;

/**
 * Writes the fixtures of rusteron-cluster-core's consensus module file tests
 * with Aeron 1.54.2's own code: {@code RecordingLog.ensureCoherent} and
 * {@code removeEntry} are package-private, hence this package.
 *
 * <ul>
 * <li>{@code recording_log/}: a recording log changed as {@code ops.txt}
 * lists, one operation a line, tab-separated, and the file it makes; the
 * Rust recording log replays them and must write the same file and hold the
 * same entries ({@code entries.txt}, Java's {@code Entry.toString}). Then a
 * standby snapshot appended and an entry removed ({@code
 * removed.log}, {@code removed.txt}), for the Rust log to read back.</li>
 * <li>{@code plans/}: recording logs and the recovery plan Java makes of each
 * ({@code plans.txt}), its log a real recording of an embedded archive whose
 * extent {@code extent.txt} gives.</li>
 * <li>{@code node_state/}: a new {@code node-state.dat}'s first 64 bytes, then
 * the same after a candidate term proposed.</li>
 * <li>{@code counters.txt}: the counters a consensus module allocates, on an
 * embedded driver: type, key, label.</li>
 * </ul>
 *
 * <pre>
 * javac -cp aeron-all-1.54.2.jar -d out java/io/aeron/cluster/ConsensusFixtures.java
 * java -cp out:aeron-all-1.54.2.jar io.aeron.cluster.ConsensusFixtures tests/fixtures
 * </pre>
 */
public final class ConsensusFixtures
{
    private static final List<String> OPS = new ArrayList<>();

    public static void main(final String[] args) throws IOException
    {
        final File root = new File(args[0]);
        recordingLog(new File(root, "recording_log"));
        nodeState(new File(root, "node_state"));
        final File dir = Files.createTempDirectory("consensus-fixtures").toFile();
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(new File(dir, "aeron").getAbsolutePath())
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);
        final Archive.Context archiveCtx = new Archive.Context()
            .aeronDirectoryName(driverCtx.aeronDirectoryName())
            .archiveDir(new File(dir, "archive"))
            .controlChannelEnabled(false)
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .archiveClientContext(new AeronArchive.Context().controlResponseChannel("aeron:udp?endpoint=localhost:0"))
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(true);
        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverCtx.aeronDirectoryName()));
            AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .controlRequestChannel(archiveCtx.localControlChannel())
                .controlResponseChannel(archiveCtx.localControlChannel())))
        {
            plans(new File(root, "plans"), aeron, archive);
            counters(new File(root, "counters.txt"), aeron);
        }
    }

    private static void op(final String... fields)
    {
        OPS.add(String.join("\t", fields));
    }

    private static void recordingLog(final File dir) throws IOException
    {
        final File work = Files.createTempDirectory("recording-log").toFile();
        Files.createDirectories(dir.toPath());
        try (RecordingLog log = new RecordingLog(work, true))
        {
            appendTerm(log, 10, 0, 0, 100);
            appendSnapshot(log, 11, 0, 0, 1_024, 150, 1);
            appendSnapshot(log, 12, 0, 0, 1_024, 150, 0);
            appendSnapshot(log, 13, 0, 0, 1_024, 150, SERVICE_ID);
            appendTerm(log, 10, 1, 2_048, 200);
            appendSnapshot(log, 14, 1, 2_048, 3_072, 250, 1);
            appendSnapshot(log, 15, 1, 2_048, 3_072, 250, 0);
            appendSnapshot(log, 16, 1, 2_048, 3_072, 250, SERVICE_ID);
            op("invalidateLatestSnapshot", Boolean.toString(log.invalidateLatestSnapshot()));
            ensureCoherent(log, 10, NULL_VALUE, 0, 3, 4_096, 5_120, 300);
            appendSnapshot(log, 17, 1, 2_048, 3_072, 260, 0);
            appendSnapshot(log, 18, 3, 4_096, 5_120, 350, 1);
            appendSnapshot(log, 19, 3, 4_096, 5_120, 350, 0);
            appendSnapshot(log, 20, 3, 4_096, 5_120, 350, SERVICE_ID);
            op("commitLogPosition", "3", "6144");
            log.commitLogPosition(3, 6_144);
            appendTerm(log, 10, 4, 6_144, 400);
            // A standby's set, one service's entry with an endpoint over several records, then the latest
            // snapshot invalidated: the standby's set, the latest of any kind.
            appendStandbySnapshot(log, 22, 4, 6_144, 8_192, 450, 1, "localhost:21301");
            appendStandbySnapshot(log, 23, 4, 6_144, 8_192, 450, 0, "localhost:21301");
            appendStandbySnapshot(log, 24, 4, 6_144, 8_192, 450, SERVICE_ID, "localhost:21301");
            appendStandbySnapshot(log, 25, 4, 6_144, 9_216, 460, 0, "a".repeat(3_078) + "x");
            op("invalidateLatestSnapshot", Boolean.toString(log.invalidateLatestSnapshot()));
            Files.write(new File(dir, "ops.txt").toPath(), OPS);
            Files.copy(new File(work, RecordingLog.RECORDING_LOG_FILE_NAME).toPath(),
                new File(dir, "recording.log").toPath(), StandardCopyOption.REPLACE_EXISTING);
            entries(log, new File(dir, "entries.txt"));

            log.appendStandbySnapshot(21, 4, 6_144, 7_168, 450, SERVICE_ID, "localhost:20114");
            log.removeEntry(0, 2);
            Files.copy(new File(work, RecordingLog.RECORDING_LOG_FILE_NAME).toPath(),
                new File(dir, "removed.log").toPath(), StandardCopyOption.REPLACE_EXISTING);
            entries(log, new File(dir, "removed.txt"));
        }
    }

    private static void appendTerm(
        final RecordingLog log, final long recordingId, final long term, final long base, final long timestamp)
    {
        op("appendTerm", Long.toString(recordingId), Long.toString(term), Long.toString(base),
            Long.toString(timestamp));
        log.appendTerm(recordingId, term, base, timestamp);
    }

    private static void appendSnapshot(
        final RecordingLog log,
        final long recordingId,
        final long term,
        final long base,
        final long position,
        final long timestamp,
        final int serviceId)
    {
        op("appendSnapshot", Long.toString(recordingId), Long.toString(term), Long.toString(base),
            Long.toString(position), Long.toString(timestamp), Integer.toString(serviceId));
        log.appendSnapshot(recordingId, term, base, position, timestamp, serviceId);
    }

    private static void appendStandbySnapshot(
        final RecordingLog log,
        final long recordingId,
        final long term,
        final long base,
        final long position,
        final long timestamp,
        final int serviceId,
        final String endpoint)
    {
        op("appendStandbySnapshot", Long.toString(recordingId), Long.toString(term), Long.toString(base),
            Long.toString(position), Long.toString(timestamp), Integer.toString(serviceId), endpoint);
        log.appendStandbySnapshot(recordingId, term, base, position, timestamp, serviceId, endpoint);
    }

    private static void ensureCoherent(
        final RecordingLog log,
        final long recordingId,
        final long initialTerm,
        final long initialBase,
        final long term,
        final long base,
        final long position,
        final long timestamp)
    {
        op("ensureCoherent", Long.toString(recordingId), Long.toString(initialTerm), Long.toString(initialBase),
            Long.toString(term), Long.toString(base), Long.toString(position), Long.toString(timestamp));
        log.ensureCoherent(recordingId, initialTerm, initialBase, term, base, position, 1, timestamp, 0);
    }

    private static void entries(final RecordingLog log, final File file) throws IOException
    {
        final List<String> lines = new ArrayList<>();
        lines.add("nextEntryIndex=" + log.nextEntryIndex());
        for (final RecordingLog.Entry entry : log.entries())
        {
            lines.add(entry.toString());
        }
        Files.write(file.toPath(), lines);
    }

    /**
     * Recovery plans for two services, the log a recording of three messages from position 0.
     */
    private static void plans(final File dir, final Aeron aeron, final AeronArchive archive) throws IOException
    {
        Files.createDirectories(dir.toPath());
        final String channel = "aeron:ipc?term-length=64k|alias=log";
        final long subscriptionId = archive.startRecording(channel, 100, SourceLocation.LOCAL);
        final long recordingId;
        try (ExclusivePublication publication = aeron.addExclusivePublication(channel, 100))
        {
            final CountersReader counters = aeron.countersReader();
            int counterId;
            while (NULL_VALUE == (counterId = RecordingPos.findCounterIdBySession(
                counters, publication.sessionId(), archive.archiveId())))
            {
                Thread.yield();
            }
            recordingId = RecordingPos.getRecordingId(counters, counterId);
            final UnsafeBuffer message = new UnsafeBuffer(new byte[200]);
            for (int i = 0; i < 3; i++)
            {
                while (publication.offer(message) < 0)
                {
                    Thread.yield();
                }
            }
            while (counters.getCounterValue(counterId) < publication.position())
            {
                Thread.yield();
            }
        }
        archive.stopRecording(subscriptionId);
        while (archive.getStopPosition(recordingId) == NULL_VALUE)
        {
            Thread.yield();
        }
        final List<String> extent = new ArrayList<>();
        archive.listRecording(recordingId,
            (controlSessionId, correlationId, id, startTimestamp, stopTimestamp, startPosition, stopPosition,
            initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
            originalChannel, sourceIdentity) -> extent.add(String.join("\t", Long.toString(id),
            Long.toString(startPosition), Long.toString(stopPosition), Integer.toString(initialTermId),
            Integer.toString(termBufferLength), Integer.toString(mtuLength), Integer.toString(sessionId))));
        Files.write(new File(dir, "extent.txt").toPath(), extent);

        final List<String> plans = new ArrayList<>();
        plan(dir, plans, archive, "empty", NULL_VALUE, log -> {});
        plan(dir, plans, archive, "replicated", recordingId, log -> {});
        plan(dir, plans, archive, "term", NULL_VALUE, log -> log.appendTerm(recordingId, 0, 0, 100));
        plan(dir, plans, archive, "snapshot", NULL_VALUE, log ->
        {
            log.appendTerm(recordingId, 0, 0, 100);
            log.appendSnapshot(30, 0, 0, 224, 150, 1);
            log.appendSnapshot(31, 0, 0, 224, 150, 0);
            log.appendSnapshot(32, 0, 0, 224, 150, SERVICE_ID);
        });
        plan(dir, plans, archive, "invalidated", NULL_VALUE, log ->
        {
            log.appendTerm(recordingId, 0, 0, 100);
            log.appendSnapshot(30, 0, 0, 224, 150, 1);
            log.appendSnapshot(31, 0, 0, 224, 150, 0);
            log.appendSnapshot(32, 0, 0, 224, 150, SERVICE_ID);
            log.appendTerm(recordingId, 1, 448, 200);
            log.appendSnapshot(33, 1, 448, 672, 250, 1);
            log.appendSnapshot(34, 1, 448, 672, 250, 0);
            log.appendSnapshot(35, 1, 448, 672, 250, SERVICE_ID);
            log.invalidateLatestSnapshot();
        });
        plan(dir, plans, archive, "terms", NULL_VALUE, log ->
        {
            log.ensureCoherent(recordingId, NULL_VALUE, 0, 2, 0, NULL_VALUE, 1, 100, 0);
            log.ensureCoherent(recordingId, NULL_VALUE, 0, 3, 448, 672, 1, 200, 0);
        });
        Files.write(new File(dir, "plans.txt").toPath(), plans);
    }

    interface Setup
    {
        void apply(RecordingLog log);
    }

    private static void plan(
        final File dir,
        final List<String> plans,
        final AeronArchive archive,
        final String name,
        final long replicatedRecordingId,
        final Setup setup) throws IOException
    {
        final File work = Files.createTempDirectory("plan").toFile();
        try (RecordingLog log = new RecordingLog(work, true))
        {
            setup.apply(log);
            final Path file = new File(work, RecordingLog.RECORDING_LOG_FILE_NAME).toPath();
            Files.copy(file, new File(dir, name + ".log").toPath(), StandardCopyOption.REPLACE_EXISTING);
            plans.add(name + "\t" + replicatedRecordingId + "\t" +
                log.createRecoveryPlan(archive, 2, replicatedRecordingId));
        }
    }

    private static void nodeState(final File dir) throws IOException
    {
        Files.createDirectories(dir.toPath());
        final File work = Files.createTempDirectory("node-state").toFile();
        final File file = new File(work, NodeStateFile.FILENAME);
        try (NodeStateFile nodeState = new NodeStateFile(work, true, 0))
        {
            Files.write(new File(dir, "new.hex").toPath(), List.of(hex(file, 64), Long.toString(file.length())));
            nodeState.proposeMaxCandidateTermId(5, 8_192, 1_700_000_000_000L);
            nodeState.proposeMaxCandidateTermId(4, 0, 0);
        }
        Files.write(new File(dir, "proposed.hex").toPath(), List.of(hex(file, 64)));
    }

    private static String hex(final File file, final int length) throws IOException
    {
        final byte[] bytes = Files.readAllBytes(file.toPath());
        final StringBuilder hex = new StringBuilder();
        for (int i = 0; i < length; i++)
        {
            hex.append(String.format("%02x", bytes[i]));
        }
        for (int i = length; i < bytes.length; i++)
        {
            if (bytes[i] != 0)
            {
                throw new IllegalStateException("byte " + i + " not zero");
            }
        }
        return hex.toString();
    }

    /**
     * The counters as ConsensusModule.Context.conclude allocates them, for cluster 3, and a recovery state.
     */
    private static void counters(final File file, final Aeron aeron) throws IOException
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
        final List<Counter> counters = new ArrayList<>();
        counters.add(ClusterCounters.allocateVersioned(aeron, buffer, "Cluster Errors",
            ConsensusModule.Configuration.CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID, 3, ConsensusModuleVersion.VERSION,
            ConsensusModuleVersion.GIT_SHA));
        final Object[][] named = {
            {"Consensus Module state", ConsensusModule.Configuration.CONSENSUS_MODULE_STATE_TYPE_ID},
            {"Cluster election state", ConsensusModule.Configuration.ELECTION_STATE_TYPE_ID},
            {"Cluster election count", io.aeron.AeronCounters.CLUSTER_ELECTION_COUNT_TYPE_ID},
            {"Cluster leadership term id", io.aeron.AeronCounters.CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID},
            {"Cluster node role", ConsensusModule.Configuration.CLUSTER_NODE_ROLE_TYPE_ID},
            {"Cluster commit-pos:", ConsensusModule.Configuration.COMMIT_POSITION_TYPE_ID},
            {"Cluster control toggle", ConsensusModule.Configuration.CONTROL_TOGGLE_TYPE_ID},
            {"Node control toggle", NodeControl.CONTROL_TOGGLE_TYPE_ID},
            {"Cluster snapshot count", ConsensusModule.Configuration.SNAPSHOT_COUNTER_TYPE_ID},
            {"Cluster timed out client count", ConsensusModule.Configuration.CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID},
            {"Cluster max cycle time in ns", io.aeron.AeronCounters.CLUSTER_MAX_CYCLE_TIME_TYPE_ID},
            {"Cluster work cycle time exceeded count: threshold=" +
                org.agrona.SystemUtil.formatDuration(ConsensusModule.Configuration.CYCLE_THRESHOLD_DEFAULT_NS),
                io.aeron.AeronCounters.CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID},
            {"Total max snapshot duration in ns", io.aeron.AeronCounters.CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID},
            {"Total max snapshot duration exceeded count: threshold=" + org.agrona.SystemUtil.formatDuration(
                ConsensusModule.Configuration.TOTAL_SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS),
                io.aeron.AeronCounters.CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID},
        };
        for (final Object[] counter : named)
        {
            counters.add(ClusterCounters.allocate(aeron, buffer, (String)counter[0], (Integer)counter[1], 3));
        }
        counters.add(RecoveryState.allocate(aeron, 2, 4_096, 1_700_000_000_000L, 3, 41, 42));

        final CountersReader reader = aeron.countersReader();
        final List<String> lines = new ArrayList<>();
        for (final Counter counter : counters)
        {
            final int id = counter.id();
            final StringBuilder key = new StringBuilder();
            reader.forEach((counterId, typeId, keyBuffer, label) ->
            {
                if (counterId == id)
                {
                    final int length = RecoveryState.RECOVERY_STATE_TYPE_ID == typeId ?
                        32 + 8 * keyBuffer.getInt(28) : 4;
                    for (int i = 0; i < length; i++)
                    {
                        key.append(String.format("%02x", keyBuffer.getByte(i)));
                    }
                }
            });
            lines.add(String.join("\t", Integer.toString(reader.getCounterTypeId(id)), key.toString(),
                reader.getCounterLabel(id)));
        }
        Files.write(file.toPath(), lines);
    }
}
