# rusteron-archive-core

Aeron Archive 1.54.2's server as sans-IO state machines: no clock, no
randomness, no I/O. Wire- and on-disk-compatible with Java's.

- `catalog`: `archive.catalog`, held as an image of the file and the ranges
  to write back.
- `segments`: the segment files a recording is written to, and the stop
  position recovered from them after a crash.
- `conductor`: control sessions (connect, answers, pings, timeouts), the
  recordings they start, extend and stop, listings, replays, truncations,
  purges and replications: Java's `ArchiveConductor` in its SHARED mode.
- `replay`: a replay session's reads and offers, as Java's
  `ReplaySession`, without its I/O.
- `protocol`, `client`, `channel`, `counters`: the requests and responses
  from either side, channel URIs as the archive strips and keys them, the
  counters it allocates.
- `session`: a client session with an archive, as a consensus module keeps
  one: connected as Java's `AsyncConnect` connects, its requests answered by
  correlation id, the archive's signals routed.

The shell that drives it on Aeron's C client is `rusteron-archive-server`.
The tests check it against Java's own code: `fixtures.sh` runs
`java/io/aeron/archive/*Fixtures.java` to write `tests/fixtures`.
Apache-2.0, as Aeron.
