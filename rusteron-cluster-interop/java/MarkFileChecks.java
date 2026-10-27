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

import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.IoUtil;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.nio.MappedByteBuffer;

/**
 * What Java sees of a running Rust container's mark file, with Aeron 1.53.3's
 * own classes:
 * <ol>
 *     <li>{@link ClusterMarkFile} opens it: a container, of the service and
 *     member expected, alive.</li>
 *     <li>Its activity timestamp, read with volatile loads while the Rust
 *     container rewrites it every second, is never torn.</li>
 *     <li>A Java container for the same service refuses to start over it.</li>
 * </ol>
 * Prints {@code MARKS OK} and exits 0, or names what failed and exits 1.
 *
 * <pre>
 * java MarkFileChecks.java CLUSTER_DIR AERON_DIR SERVICE_ID MEMBER_ID WATCH_MS
 * </pre>
 */
public final class MarkFileChecks
{
    public static void main(final String[] args)
    {
        final File clusterDir = new File(args[0]);
        final String aeronDir = args[1];
        final int serviceId = Integer.parseInt(args[2]);
        final int memberId = Integer.parseInt(args[3]);
        final long watchMs = Long.parseLong(args[4]);
        final String fileName = ClusterMarkFile.markFilenameForService(serviceId);

        try (ClusterMarkFile markFile = new ClusterMarkFile(
            clusterDir, fileName, SystemEpochClock.INSTANCE, 10_000, System.out::println))
        {
            final var decoder = markFile.decoder();
            check(decoder.componentType() == ClusterComponentType.CONTAINER, "type " + decoder.componentType());
            check(decoder.serviceId() == serviceId, "serviceId " + decoder.serviceId());
            check(markFile.memberId() == memberId, "memberId " + markFile.memberId());
            check(decoder.pid() > 0, "pid " + decoder.pid());
            final long age = System.currentTimeMillis() - markFile.activityTimestampVolatile();
            check(age >= 0 && age < 3_000, "activity " + age + " ms old");
            System.out.println("mark file: pid=" + decoder.pid() + " memberId=" + markFile.memberId() +
                " activity " + age + " ms old, " + decoder.aeronDirectory());
        }

        final MappedByteBuffer mapped = IoUtil.mapExistingFile(new File(clusterDir, fileName), "mark file");
        try
        {
            final UnsafeBuffer buffer = new UnsafeBuffer(mapped);
            final int offset = 8 + 8; // SBE header, then the version before the activity timestamp
            final long deadline = System.currentTimeMillis() + watchMs;
            long last = buffer.getLongVolatile(offset);
            long reads = 0;
            long changes = 0;
            while (System.currentTimeMillis() < deadline)
            {
                final long value = buffer.getLongVolatile(offset);
                reads++;
                if (value != last)
                {
                    changes++;
                    final long skew = Math.abs(System.currentTimeMillis() - value);
                    check(value > last && skew < 3_000, "torn or stale activity " + value + " after " + last);
                    last = value;
                }
            }
            check(changes >= watchMs / 1_000 - 1, "activity changed " + changes + " times in " + watchMs + " ms");
            System.out.println("activity read " + reads + " times, changed " + changes + " times, never torn");
        }
        finally
        {
            IoUtil.unmap(mapped);
        }

        try (ClusteredServiceContainer container = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDir)
                .clusterDir(clusterDir)
                .serviceId(serviceId)
                .clusteredService(new EchoService())))
        {
            check(false, "a Java container started over the live Rust one: " + container);
        }
        catch (final RuntimeException ex)
        {
            check(String.valueOf(ex.getMessage()).contains("active"), "unexpected refusal: " + ex);
            System.out.println("a Java container refuses: " + ex.getMessage());
        }
        System.out.println("MARKS OK");
    }

    private static void check(final boolean condition, final String what)
    {
        if (!condition)
        {
            System.out.println("MARKS FAILED: " + what);
            System.exit(1);
        }
    }
}
