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

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Counter;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static io.aeron.CommonContext.*;

/**
 * Writes the fixtures of rusteron-archive's conductor tests with Aeron
 * 1.54.2's own code, from this package for what is package-private:
 *
 * <ul>
 * <li>{@code channels.txt}: for each channel a client may name, what the
 * archive makes of it, tab-separated: the channel, its stripped channel
 * ({@code ArchiveConductor.strippedChannelBuilder}), its recording key
 * ({@code makeKey}), and the response channel a control session would
 * answer on ({@code newControlSession}'s steps, with the default control
 * stream), and the publication channel a replay of it would take
 * ({@code startReplay}'s steps, from position 131104 of a stream of initial
 * term 7, 64 KiB terms, MTU 1408); an error as {@code !} and its message.</li>
 * <li>{@code counters.txt}: the counters the archive allocates, allocated on
 * an embedded driver and read back: type, key in hex, label.</li>
 * </ul>
 */
public final class ConductorFixtures
{
    private static final String[] CHANNELS = {
        "aeron:ipc",
        "aeron:ipc?term-length=64k",
        "aeron:ipc?alias=log|term-length=64k",
        "aeron:ipc?tags=9|alias=other",
        "aeron:udp?endpoint=localhost:20121",
        "aeron:udp?endpoint=localhost:20121|alias=foo|term-length=64k|session-id=5|tags=1,2",
        "aeron-spy:aeron:udp?endpoint=localhost:40123|alias=a",
        "aeron:udp?control=localhost:9000|control-mode=dynamic|endpoint=localhost:9001|interface=127.0.0.1|ttl=4|" +
            "cc=static|fc=min|gtag=7|so-sndbuf=131072|so-rcvbuf=2m|rcv-wnd=65536|rejoin=TRUE|group=false|" +
            "tether=yes|alias=x|mtu=8k|sparse=true|linger=0|eos=false|reliable=false|init-term-id=1|term-id=2|" +
            "term-offset=0|nak-delay=1ms",
        "aeron:udp?endpoint=localhost:1|session-id=tag:77",
        "aeron:udp?endpoint=localhost:1|media-rcv-ts-offset=reserved|channel-rcv-ts-offset=8|" +
            "channel-snd-ts-offset=16|response-endpoint=localhost:5|response-correlation-id=prototype|stream-id=3",
        "aeron:udp?control-mode=response|control=localhost:10001",
        "aeron:udp?endpoint=localhost:1|alias=a|alias=b",
        "aeron:udp?endpoint=a=b",
        "aeron:ipc?mtu=4096|term-length=1m|sparse=false",
        "aeron:udp?endpoint=localhost:1|so-sndbuf=3000|rcv-wnd=1g",
        "udp?x=y",
        "aeron:tcp",
        "aeron:tcp?x=y",
        "aeron:ipc?",
        "aeron:ipc?|x=1",
        "aeron:ipc?=1",
        "aeron:ip|c",
        "aeron:udp?endpoint=localhost:1|control-mode=bad",
        "aeron:udp?endpoint=x|ttl=300",
        "aeron:udp?endpoint=x|session-id=abc",
        "aeron:udp?endpoint=x|gtag=zz",
        "aeron:udp?endpoint=x|response-correlation-id=-2",
        "aeron:udp?endpoint=x|channel-snd-ts-offset=abc",
        "aeron:ipc?mtu=33",
        "aeron:ipc?term-length=1000",
        "aeron:udp?endpoint=localhost:1|linger=12000|eos=TRUE|sparse=false|alias=r",
        "aeron:ipc?linger=5s",
        "aeron:ipc?linger=-1",
    };

    public static void main(final String[] args) throws Exception
    {
        final File root = new File(args[0]);
        channels(new File(root, "channels.txt"));
        counters(new File(root, "counters.txt"));
    }

    private interface Step
    {
        String run() throws Exception;
    }

    private static String outcome(final Step step)
    {
        try
        {
            return step.run();
        }
        catch (final Exception ex)
        {
            final Throwable cause = ex instanceof java.lang.reflect.InvocationTargetException ? ex.getCause() : ex;
            return "!" + cause.getMessage();
        }
    }

    private static void channels(final File file) throws Exception
    {
        final Method makeKey = ArchiveConductor.class.getDeclaredMethod("makeKey", int.class, ChannelUri.class);
        makeKey.setAccessible(true);
        final Archive.Context ctx = new Archive.Context();
        final List<String> lines = new ArrayList<>();
        for (final String channel : CHANNELS)
        {
            final String stripped = outcome(() ->
                ArchiveConductor.strippedChannelBuilder(ChannelUri.parse(channel)).build());
            final String key = outcome(() -> (String)makeKey.invoke(null, 1001, ChannelUri.parse(channel)));
            final String response = outcome(() -> responseChannel(ctx, channel, 4242));
            final String udp = outcome(() -> Boolean.toString(ChannelUri.parse(channel).isUdp()));
            final String replay = outcome(() -> replayChannel(ctx, channel));
            lines.add(String.join("\t", channel, stripped, key, response, udp, replay));
        }
        Files.write(file.toPath(), lines);
    }

    /**
     * {@code ArchiveConductor.newControlSession}'s response channel, step by step.
     */
    private static String responseChannel(final Archive.Context ctx, final String channel, final long imageId)
    {
        final ChannelUri channelUri = ChannelUri.parse(channel);
        final String mtuStr = channelUri.get(MTU_LENGTH_PARAM_NAME);
        final int mtuLength = null == mtuStr ?
            ctx.controlMtuLength() : (int)SystemUtil.parseSize(MTU_LENGTH_PARAM_NAME, mtuStr);
        final String termLengthStr = channelUri.get(TERM_LENGTH_PARAM_NAME);
        final int termLength = null == termLengthStr ?
            ctx.controlTermBufferLength() : (int)SystemUtil.parseSize(TERM_LENGTH_PARAM_NAME, termLengthStr);
        final String isSparseStr = channelUri.get(SPARSE_PARAM_NAME);
        final boolean isSparse = null == isSparseStr ?
            ctx.controlTermBufferSparse() : Boolean.parseBoolean(isSparseStr);
        final ChannelUriStringBuilder urlBuilder = ArchiveConductor.strippedChannelBuilder(channelUri)
            .termLength(termLength)
            .sparse(isSparse)
            .mtu(mtuLength);
        if (CONTROL_MODE_RESPONSE.equals(channelUri.get(MDC_CONTROL_MODE_PARAM_NAME)))
        {
            urlBuilder.responseCorrelationId(imageId);
        }
        return urlBuilder.build();
    }

    /**
     * {@code ArchiveConductor.startReplay}'s replay channel, step by step.
     */
    private static String replayChannel(final Archive.Context ctx, final String channel)
    {
        final ChannelUri channelUri = ChannelUri.parse(ChannelUri.parse(channel).toString());
        final ChannelUriStringBuilder builder = ArchiveConductor.strippedChannelBuilder(channelUri)
            .initialPosition(131104, 7, 64 * 1024)
            .eos(channelUri)
            .sparse(channelUri)
            .mtu(1408);
        final String lingerValue = channelUri.get(LINGER_PARAM_NAME);
        builder.linger(null != lingerValue ? Long.parseLong(lingerValue) : ctx.replayLingerTimeoutNs());
        return builder.build();
    }

    private static void counters(final File file) throws IOException
    {
        final File dir = Files.createTempDirectory("conductor-fixtures").toFile();
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(new File(dir, "aeron").getAbsolutePath())
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);
        final List<String> lines = new ArrayList<>();
        try (MediaDriver driver = MediaDriver.launchEmbedded(driverCtx);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverCtx.aeronDirectoryName())))
        {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
            final List<Counter> counters = new ArrayList<>();
            counters.add(ArchiveCounters.allocateErrorCounter(aeron, buffer, 42));
            counters.add(ArchiveCounters.allocate(aeron, buffer, 102, "Archive Control Sessions", 42));
            counters.add(ArchiveCounters.allocate(aeron, buffer, 103,
                "archive-conductor max cycle time in ns: SHARED", -7));
            counters.add(RecordingPos.allocate(
                aeron, buffer, 42, 0, 77, 1001, "aeron:ipc?alias=log", "aeron:ipc"));
            counters.add(RecordingPos.allocate(
                aeron, buffer, Long.MIN_VALUE, 12345678901L, -5, 7, "aeron:udp?endpoint=localhost:20121|alias=" +
                "x".repeat(400), "127.0.0.1:" + "9".repeat(120)));
            final long registrationId = ControlSessionCounter.allocate(aeron, buffer, 42, 7000,
                "name=test version=1.54.2 commit=481d4271ad sourceIdentity=aeron:ipc sessionId=-321");
            Counter session;
            while (null == (session = aeron.getCounter(registrationId)))
            {
                Thread.yield();
            }
            counters.add(session);

            final CountersReader reader = aeron.countersReader();
            for (final Counter counter : counters)
            {
                final int id = counter.id();
                final StringBuilder key = new StringBuilder();
                reader.forEach((counterId, typeId, keyBuffer, label) ->
                {
                    if (counterId == id)
                    {
                        final int length = keyLength(typeId, keyBuffer);
                        for (int i = 0; i < length; i++)
                        {
                            key.append(String.format("%02x", keyBuffer.getByte(i)));
                        }
                    }
                });
                lines.add(String.join("\t", Integer.toString(reader.getCounterTypeId(id)), key.toString(),
                    reader.getCounterLabel(id)));
            }
        }
        Files.write(file.toPath(), lines);
    }

    /**
     * The key's length, which the counters do not keep: the archive's are of fixed length but for rec-pos.
     */
    private static int keyLength(final int typeId, final org.agrona.DirectBuffer key)
    {
        switch (typeId)
        {
            case RecordingPos.RECORDING_POSITION_TYPE_ID:
                return 16 + key.getInt(12) + 8;
            case ControlSessionCounter.TYPE_ID:
                return 16;
            default:
                return 8;
        }
    }
}
