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
package io.aeron.archive;

import io.aeron.archive.codecs.RecordingState;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Writes the fixtures of rusteron-archive-core's catalog tests with Aeron
 * 1.54.2's own {@code Catalog}, which is package-private: hence this package.
 *
 * <ul>
 * <li>{@code catalog/archive.catalog}: a catalog of 4 KiB grown by the
 * recordings added, changed as {@code catalog/ops.txt} lists, one operation
 * a line, tab-separated: the Rust catalog replays them and must write the
 * same file.</li>
 * <li>{@code recovery/<case>/}: segment files left by a recording the archive
 * did not stop, and in {@code recovery/cases.txt} the stop position Java
 * recovers from each, whether the last fragment straddles a page half
 * written, and the stop position Java recovers when told to truncate such a
 * fragment, as {@code ArchiveTool verify} can be: the segment it leaves is
 * in {@code recovery/<case>/truncated/}.</li>
 * <li>{@code verify/before/}: an archive of recordings whole, damaged in
 * each way {@code ArchiveTool verify} reports, invalid, deleted, and one with
 * a torn last fragment; {@code verify/after/} the same archive once the tool
 * verified it as a cluster node runs it (no option, truncation confirmed), and
 * in {@code verify/cases.txt} each recording's name, its state and stop
 * position after, and the tool's error for it, if any
 * ({@code verify/report.txt} has the tool's whole report).</li>
 * </ul>
 *
 * <pre>
 * javac -cp aeron-all-1.54.2.jar -d out java/io/aeron/archive/CatalogFixtures.java
 * java -cp out:aeron-all-1.54.2.jar io.aeron.archive.CatalogFixtures tests/fixtures
 * </pre>
 */
public final class CatalogFixtures
{
    static final int TERM = 64 * 1024;
    static final int SEGMENT = 64 * 1024;
    private static final List<String> OPS = new ArrayList<>();

    public static void main(final String[] args) throws IOException
    {
        final File root = new File(args[0]);
        catalog(new File(root, "catalog"));
        recovery(new File(root, "recovery"));
        verify(new File(root, "verify"));
    }

    private static void catalog(final File dir) throws IOException
    {
        deleteRecursively(dir);
        Files.createDirectories(dir.toPath());
        try (Catalog catalog = new Catalog(dir, null, 0, 4096, () -> 1_700_000_009_999L, null, null))
        {
            for (int i = 0; i < 12; i++)
            {
                // Channels of different lengths, some long, so the catalog grows twice.
                final String stripped = "aeron:udp?endpoint=localhost:" + (20_000 + i) + "|alias=" + "x".repeat(i * 37);
                final String original = stripped + "|tags=" + i + "," + (i + 1) + "|session-id=" + (-1000 - i);
                final String source = i % 3 == 0 ? "aeron:ipc" : "127.0.0.1:" + (40_000 + i);
                add(catalog, 1_024L * i, i % 2 == 0 ? -1 : 1_024L * i + 4_096, 1_700_000_000_000L + i,
                    i % 2 == 0 ? -1 : 1_700_000_000_500L + i, -i * 7, SEGMENT, TERM, 1_408, -123_456 + i, 100 + i,
                    stripped, original, source);
            }
            catalog.recordingStopped(0, 65_536, 1_700_000_001_000L);
            OPS.add("stop\t0\t65536\t1700000001000");
            catalog.extendRecording(1, 77, 88, -99);
            OPS.add("extend\t1\t77\t88\t-99");
            catalog.stopPosition(2, 2_048);
            OPS.add("stop_position\t2\t2048");
            catalog.startPosition(3, 131_072);
            OPS.add("start_position\t3\t131072");
            catalog.changeState(4, RecordingState.DELETED);
            OPS.add("state\t4\t2");
            add(catalog, 0, -1, 1_700_000_002_000L, -1, 5, SEGMENT, TERM, 1_408, 7, 200, "aeron:ipc?alias=last",
                "aeron:ipc?alias=last|session-id=7", "aeron:ipc");
            catalog.recordingStopped(12, 1_024, 1_700_000_003_000L);
            OPS.add("stop\t12\t1024\t1700000003000");
            // Longer: the entries after it move along. Shorter: in place, its old tail zeroed.
            final String longer = "aeron:udp?endpoint=replaced:1|alias=" + "y".repeat(200);
            replace(catalog, 2, 4_096, 8_192, 1_700_000_004_000L, 1_700_000_005_000L, 9, SEGMENT, TERM, 1_408, 77,
                300, longer, longer + "|tags=1", "1.2.3.4:5");
            replace(catalog, 5, 0, 0, 1_700_000_006_000L, 1_700_000_006_000L, 1, SEGMENT, TERM, 1_408, 7, 301,
                "aeron:ipc", "aeron:ipc", "aeron:ipc");
        }
        Files.write(new File(dir, "ops.txt").toPath(), OPS);
    }

    private static void add(
        final Catalog catalog, final long start, final long stop, final long startTs, final long stopTs,
        final int initialTermId, final int segment, final int term, final int mtu, final int sessionId,
        final int streamId, final String stripped, final String original, final String source)
    {
        catalog.addNewRecording(
            start, stop, startTs, stopTs, initialTermId, segment, term, mtu, sessionId, streamId, stripped, original,
            source);
        OPS.add(String.join("\t", "add", Long.toString(start), Long.toString(stop), Long.toString(startTs),
            Long.toString(stopTs), Integer.toString(initialTermId), Integer.toString(segment), Integer.toString(term),
            Integer.toString(mtu), Integer.toString(sessionId), Integer.toString(streamId), stripped, original,
            source));
    }

    private static void replace(
        final Catalog catalog, final long id, final long start, final long stop, final long startTs,
        final long stopTs, final int initialTermId, final int segment, final int term, final int mtu,
        final int sessionId, final int streamId, final String stripped, final String original, final String source)
    {
        catalog.replaceRecording(
            id, start, stop, startTs, stopTs, initialTermId, segment, term, mtu, sessionId, streamId, stripped,
            original, source);
        OPS.add(String.join("\t", "replace", Long.toString(id), Long.toString(start), Long.toString(stop),
            Long.toString(startTs), Long.toString(stopTs), Integer.toString(initialTermId), Integer.toString(segment),
            Integer.toString(term), Integer.toString(mtu), Integer.toString(sessionId), Integer.toString(streamId),
            stripped, original, source));
    }

    private static void recovery(final File dir) throws IOException
    {
        deleteRecursively(dir);
        Files.createDirectories(dir.toPath());
        final List<String> cases = new ArrayList<>();
        // name, start position, the highest segment's base, its frames (lengths; negative: padding) from its offset.
        recoveryCase(dir, cases, "frames-from-start", 0, 0, new int[]{ 100, 2_000, 33, 1_408 }, false);
        recoveryCase(dir, cases, "mid-term-start", 3 * TERM + 1_024, 3 * TERM, new int[]{ 500, 64, -3_000, 900 }, false);
        recoveryCase(dir, cases, "later-segment", 1_024, 2 * SEGMENT, new int[]{ 4_000, 4_000, 4_000 }, false);
        recoveryCase(dir, cases, "straddle-half-written", 0, 0, new int[]{ 4_000, 3_000 }, true);
        recoveryCase(dir, cases, "straddle-written", 0, 0, new int[]{ 4_000, 3_000 }, false);
        recoveryCase(dir, cases, "padding-last", 0, 0, new int[]{ 1_000, -(SEGMENT - 1_024) }, false);
        recoveryCase(dir, cases, "empty-segment", 2 * TERM + 4_096, 2 * TERM, new int[]{}, false);
        cases.add(String.join("\t", "no-segment", Integer.toString(TERM + 64), Integer.toString(TERM), "-", "-",
            Long.toString(Catalog.computeStopPosition(dir, null, TERM + 64, TERM, SEGMENT, null, buffer(), f -> false)),
            "0", Integer.toString(TERM + 64)));
        Files.write(new File(dir, "cases.txt").toPath(), cases);
    }

    /**
     * Writes one segment file of frames from the start's term offset (or 0 in a later segment), the frames'
     * payload non-zero, a padding frame's header only, as the archive writes them; a frame cut short leaves
     * the rest of its pages empty. Records Java's recovered stop position.
     */
    private static void recoveryCase(
        final File root, final List<String> cases, final String name, final long start, final long base,
        final int[] frames, final boolean cutLastShort) throws IOException
    {
        final File dir = new File(root, name);
        Files.createDirectories(dir.toPath());
        final long startTermBase = start - (start & (TERM - 1));
        int offset = base == startTermBase ? (int)(start & (TERM - 1)) : 0;
        final ByteBuffer file = ByteBuffer.allocate(SEGMENT).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < frames.length; i++)
        {
            final boolean padding = frames[i] < 0;
            final int length = Math.abs(frames[i]);
            final int aligned = (length + 31) & ~31;
            file.putInt(offset, length);
            file.put(offset + 4, (byte)0);
            file.put(offset + 5, (byte)0xC0);
            file.putShort(offset + 6, (short)(padding ? 0 : 1));
            file.putInt(offset + 8, (int)((base + offset) & (TERM - 1)));
            file.putInt(offset + 12, 4242);
            file.putInt(offset + 16, 100);
            if (!padding)
            {
                final boolean last = i == frames.length - 1;
                final int written = last && cutLastShort ? Math.min(length, 4_096 - (offset % 4_096)) : length;
                for (int b = 32; b < written; b++)
                {
                    file.put(offset + b, (byte)(1 + (b % 251)));
                }
            }
            offset += aligned;
        }
        final String segment = Archive.segmentFileName(42, base);
        try (RandomAccessFile out = new RandomAccessFile(new File(dir, segment), "rw"))
        {
            out.setLength(SEGMENT);
            out.write(file.array());
        }
        final boolean[] straddled = { false };
        final long stop = Catalog.computeStopPosition(dir, segment, start, TERM, SEGMENT, null, buffer(), f ->
        {
            straddled[0] = true;
            return false;
        });
        // Told to truncate, as ArchiveTool verify can be: on a copy, the stop Java recovers and the file it leaves.
        long truncatedStop = stop;
        if (straddled[0])
        {
            final File truncated = new File(dir, "truncated");
            Files.createDirectories(truncated.toPath());
            Files.copy(new File(dir, segment).toPath(), new File(truncated, segment).toPath());
            truncatedStop = Catalog.computeStopPosition(
                truncated, segment, start, TERM, SEGMENT, null, buffer(), f -> true);
        }
        cases.add(String.join("\t", name, Long.toString(start), Integer.toString(TERM), segment, Long.toString(base),
            Long.toString(stop), straddled[0] ? "1" : "0", Long.toString(truncatedStop)));
    }

    private static final int INITIAL_TERM_ID = 5;
    private static final long TIMESTAMP = 1_700_000_009_999L;

    /**
     * Recordings as the archive leaves them, damaged in each way the tool tells, then verified on a copy by
     * Aeron's own {@code ArchiveTool.verify}, without options and confirming truncation, as a cluster node
     * runs it before its archive starts.
     */
    private static void verify(final File dir) throws IOException
    {
        deleteRecursively(dir);
        final File before = new File(dir, "before");
        Files.createDirectories(before.toPath());
        final List<String> names = new ArrayList<>();
        final int[] frames = { 100, 2_000, 64 };
        final long end = 128 + 2_016 + 64;
        try (Catalog catalog = new Catalog(before, null, 0, 4096, () -> TIMESTAMP, null, null))
        {
            names.add("whole-stopped");
            segment(before, add(catalog, 0, end), 0, 0, frames, false, -1, b -> {});
            names.add("whole-active");
            segment(before, add(catalog, 1_024, -1), 1_024, 0, frames, false, -1, b -> {});
            names.add("stop-behind");
            segment(before, add(catalog, 0, 128), 0, 0, frames, false, -1, b -> {});
            names.add("stop-ahead");
            segment(before, add(catalog, 0, 8_192), 0, 0, frames, false, -1, b -> {});
            names.add("negative-start");
            add(catalog, -64, -1);
            names.add("unaligned-start");
            add(catalog, 100, -1);
            names.add("stop-before-start");
            add(catalog, 4_096, 1_024);
            names.add("unaligned-stop");
            add(catalog, 0, 200);
            names.add("past-last-segment");
            segment(before, add(catalog, 0, SEGMENT + 64), 0, 0, frames, false, -1, b -> {});
            names.add("wrong-term-id");
            segment(before, add(catalog, 0, end), 0, 0, frames, false, -1, b -> b.putInt(128 + 20, 99));
            names.add("wrong-stream");
            segment(before, add(catalog, 0, end), 0, 0, frames, false, -1, b -> b.putInt(16, 101));
            names.add("data-cut-short");
            segment(before, add(catalog, 0, 128), 0, 0, frames, false, 128 + 40, b -> {});
            names.add("later-segment");
            final long laterId = add(catalog, 1_024, 2L * SEGMENT + 8_000);
            segment(before, laterId, 1_024, 0, new int[]{ 1_000, -(SEGMENT - 2_048) }, false, -1, b -> {});
            segment(before, laterId, 1_024, SEGMENT, new int[]{ -SEGMENT }, false, -1, b -> {});
            segment(before, laterId, 1_024, 2L * SEGMENT, new int[]{ 4_000, 4_000 }, false, -1, b -> {});
            names.add("empty-last-segment");
            final long emptyId = add(catalog, 0, -1);
            segment(before, emptyId, 0, 0, new int[]{ 1_024, -(SEGMENT - 1_024) }, false, -1, b -> {});
            segment(before, emptyId, 0, SEGMENT, new int[]{}, false, 0, b -> {});
            names.add("torn-active");
            segment(before, add(catalog, 0, -1), 0, 0, new int[]{ 4_000, 3_000 }, true, -1, b -> {});
            names.add("invalid-whole");
            final long invalidId = add(catalog, 0, end);
            segment(before, invalidId, 0, 0, frames, false, -1, b -> {});
            catalog.changeState(invalidId, RecordingState.INVALID);
            names.add("invalid-without-files");
            catalog.changeState(add(catalog, 0, 0), RecordingState.INVALID);
            names.add("deleted");
            final long deletedId = add(catalog, 0, end);
            segment(before, deletedId, 0, 0, frames, false, -1, b -> {});
            catalog.changeState(deletedId, RecordingState.DELETED);
        }

        final File after = new File(dir, "after");
        Files.createDirectories(after.toPath());
        for (final File file : before.listFiles())
        {
            Files.copy(file.toPath(), new File(after, file.getName()).toPath());
        }
        final ByteArrayOutputStream report = new ByteArrayOutputStream();
        ArchiveTool.verify(new PrintStream(report, true, StandardCharsets.UTF_8), after,
            EnumSet.noneOf(ArchiveTool.VerifyOption.class), null, () -> TIMESTAMP, file -> true);
        final String text = report.toString(StandardCharsets.UTF_8);
        Files.writeString(new File(dir, "report.txt").toPath(), text);
        final Map<Long, String> errors = new TreeMap<>();
        final Matcher error = Pattern.compile("(?m)^\\(recordingId=(\\d+)[^)]*\\) ERR: (.*)$").matcher(text);
        while (error.find())
        {
            errors.put(Long.parseLong(error.group(1)), error.group(2));
        }
        final List<String> cases = new ArrayList<>();
        try (Catalog catalog = new Catalog(after, () -> TIMESTAMP))
        {
            catalog.forEach((offset, he, hd, e, d) -> cases.add(String.join("\t", Long.toString(d.recordingId()),
                names.get((int)d.recordingId()), hd.state().name(), Long.toString(d.stopPosition()),
                Long.toString(d.stopTimestamp()), errors.getOrDefault(d.recordingId(), "-"))));
        }
        Files.write(new File(dir, "cases.txt").toPath(), cases);
    }

    private static long add(final Catalog catalog, final long start, final long stop)
    {
        return catalog.addNewRecording(start, stop, TIMESTAMP - 1_000, stop == -1 ? -1 : TIMESTAMP - 500,
            INITIAL_TERM_ID, SEGMENT, TERM, 1_408, 4242, 100, "aeron:ipc", "aeron:ipc", "aeron:ipc");
    }

    /**
     * The segment file of recording {@code id} at {@code base}, frames from the start's term offset if it holds
     * the start's term, else from its start: lengths, negative for padding, each header the recording's, data
     * non-zero; a padding frame's header only, as the archive writes them. {@code cutLastShort}: the last
     * frame's pages after its first left empty. {@code fileLength}: the file's length, -1 the segment's.
     */
    private static void segment(
        final File dir, final long id, final long start, final long base, final int[] frames,
        final boolean cutLastShort, final int fileLength, final Consumer<ByteBuffer> tamper) throws IOException
    {
        final long startTermBase = start - (start & (TERM - 1));
        int offset = base == startTermBase ? (int)(start & (TERM - 1)) : 0;
        final ByteBuffer file = ByteBuffer.allocate(SEGMENT).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < frames.length; i++)
        {
            final boolean padding = frames[i] < 0;
            final int length = Math.abs(frames[i]);
            final long position = base + offset;
            file.putInt(offset, length);
            file.put(offset + 5, (byte)0xC0);
            file.putShort(offset + 6, (short)(padding ? 0 : 1));
            file.putInt(offset + 8, (int)(position & (TERM - 1)));
            file.putInt(offset + 12, 4242);
            file.putInt(offset + 16, 100);
            file.putInt(offset + 20, INITIAL_TERM_ID + (int)(position / TERM));
            if (!padding)
            {
                final boolean last = i == frames.length - 1;
                final int written = last && cutLastShort ? Math.min(length, 4_096 - (offset % 4_096)) : length;
                for (int b = 32; b < written; b++)
                {
                    file.put(offset + b, (byte)(1 + (b % 251)));
                }
            }
            offset += (length + 31) & ~31;
        }
        tamper.accept(file);
        try (RandomAccessFile out = new RandomAccessFile(new File(dir, Archive.segmentFileName(id, base)), "rw"))
        {
            final int length = fileLength < 0 ? SEGMENT : fileLength;
            out.setLength(length);
            out.write(file.array(), 0, length);
        }
    }

    private static UnsafeBuffer buffer()
    {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(1 << 20));
    }

    private static void deleteRecursively(final File dir) throws IOException
    {
        if (dir.exists())
        {
            try (var paths = Files.walk(dir.toPath()))
            {
                paths.sorted(java.util.Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
        }
    }
}
