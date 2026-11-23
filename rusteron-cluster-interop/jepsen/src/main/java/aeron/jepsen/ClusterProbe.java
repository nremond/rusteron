/*
 * Copyright 2026 the rusteron authors. Apache-2.0.
 */
package aeron.jepsen;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.cluster.ClusterControl;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

/**
 * Reads a member's cluster counters on its media driver, whichever
 * consensus module (Java's or Rust's) allocated them, for the nemeses:
 *
 * <pre>
 * ClusterProbe AERON_DIR state
 * ClusterProbe AERON_DIR await FIELD VALUE TIMEOUT_MS    (exit 0 once FIELD == VALUE, 1 at the timeout)
 * ClusterProbe AERON_DIR toggle snapshot
 * </pre>
 *
 * Fields: role (0 follower, 1 candidate, 2 leader), election (its state, 17
 * when none), module (0 init, 1 active, 2 suspended, 3 snapshot...), term,
 * commit.
 */
public final class ClusterProbe
{
    public static void main(final String[] args) throws InterruptedException
    {
        final String dir = args[0];
        final String command = args[1];
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(dir).driverTimeoutMs(2_000)))
        {
            final CountersReader counters = aeron.countersReader();
            switch (command)
            {
                case "state" -> System.out.println(state(counters));
                case "await" ->
                {
                    final int typeId = typeId(args[2]);
                    final long value = Long.parseLong(args[3]);
                    final long deadline = System.currentTimeMillis() + Long.parseLong(args[4]);
                    while (System.currentTimeMillis() < deadline)
                    {
                        if (value(counters, typeId) == value)
                        {
                            System.out.println(state(counters));
                            return;
                        }
                        Thread.sleep(2);
                    }
                    System.out.println("timeout " + state(counters));
                    System.exit(1);
                }
                case "toggle" ->
                {
                    final int id = find(counters, AeronCounters.CLUSTER_CONTROL_TOGGLE_TYPE_ID);
                    final boolean toggled = id >= 0 && ClusterControl.ToggleState.valueOf(args[2].toUpperCase())
                        .toggle(new AtomicCounter(counters.valuesBuffer(), id));
                    System.out.println((toggled ? "toggled " : "not toggled ") + state(counters));
                    if (!toggled)
                    {
                        System.exit(1);
                    }
                }
                default -> throw new IllegalArgumentException("unknown command " + command);
            }
        }
    }

    static String state(final CountersReader counters)
    {
        return "role=" + value(counters, AeronCounters.CLUSTER_NODE_ROLE_TYPE_ID) +
            " election=" + value(counters, AeronCounters.CLUSTER_ELECTION_STATE_TYPE_ID) +
            " module=" + value(counters, AeronCounters.CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) +
            " term=" + value(counters, AeronCounters.CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID) +
            " commit=" + value(counters, AeronCounters.CLUSTER_COMMIT_POSITION_TYPE_ID);
    }

    static int typeId(final String field)
    {
        return switch (field)
        {
            case "role" -> AeronCounters.CLUSTER_NODE_ROLE_TYPE_ID;
            case "election" -> AeronCounters.CLUSTER_ELECTION_STATE_TYPE_ID;
            case "module" -> AeronCounters.CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID;
            case "term" -> AeronCounters.CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID;
            case "commit" -> AeronCounters.CLUSTER_COMMIT_POSITION_TYPE_ID;
            default -> throw new IllegalArgumentException("unknown field " + field);
        };
    }

    static long value(final CountersReader counters, final int typeId)
    {
        final int id = find(counters, typeId);
        return id < 0 ? Long.MIN_VALUE : counters.getCounterValue(id);
    }

    /** The counter of `typeId` for cluster 0. */
    static int find(final CountersReader counters, final int typeId)
    {
        final int[] found = { -1 };
        counters.forEach((counterId, type, keyBuffer, label) ->
        {
            if (found[0] < 0 && type == typeId && keyBuffer.getInt(0) == 0)
            {
                found[0] = counterId;
            }
        });
        return found[0];
    }
}
