# rusteron-archive-server

Aeron Archive's server on Aeron's C client, wire- and on-disk-compatible
with Aeron 1.54.2, for what Aeron Cluster asks of its archive. One thread,
as Java's SHARED mode: the conductor of `rusteron-archive-core`, the
recorder (an image's blocks into its segment files), the catalog's file,
the mark file `archive-mark.dat`, the archive's counters.

```text
aeron_archive --aeron-dir DIR --archive-dir DIR [--archive-id N] [--control-channel URI] ...
```

It serves control sessions, recording (start, extend, stop, list,
positions), replay (plain, bounded by a counter, following a recording
being written), truncation and purge, and replication from another
archive as Aeron Cluster uses it (a new copy of a stopped recording, an
existing copy caught up to a position); it recovers a recording's stop
position after a crash. `interop.sh` runs Java's archive client against
it, kills it mid-recording and restarts it, has it replicate from a Java
and a Rust archive, and runs Java's `ArchiveTool verify` on what it wrote
(`interop.sh java`: the same against Java's archive, which then
replicates from the Rust one).

`aeron-tests/run.sh` builds Aeron 1.54.2's own C archive client tests and
runs them against this archive (`aeron-tests/java.in` stands in for the
`java` they launch Java's archive with: Aeron's C media driver and
`aeron_archive`, the `-D` properties mapped). 61 of the 80 pass; the 19
others use what the archive does not serve: an authenticator rejecting
null credentials (2), response channels and replay tokens (12), segment
detach, attach and purge (3), channel update (1), listing recording
subscriptions (1). `docs/design.md` has the design, the tests, what Java
clusters on it showed and its measurements. Apache-2.0, as Aeron.
