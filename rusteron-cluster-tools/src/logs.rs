//! Every node's cluster log, compared byte for byte as its archive replays
//! it ([`check`]): members' and standbys' alike, a check to end a test run
//! with. Each node's newest recording of the log stream is replayed over the
//! range all of them hold, from the latest start to the least recorded
//! position, and its CRC-32C is kept at each MiB of log ([`Checkpoints`]).
//! The first MiB where a node's log is unlike the first node's says where to
//! look ([`first_difference`]). A log can differ where no service state
//! does: a torn fragment at a log's end, for one.

/// How often, in bytes of log, a replay's CRC is kept.
pub const CHECKPOINT: i64 = 1 << 20;

/// A replay's running CRC-32C, from the range's start, kept at the end of
/// the first fragment at or past each multiple of [`CHECKPOINT`], and at the
/// range's end.
#[derive(Debug)]
pub struct Checkpoints {
    pub positions: Vec<i64>,
    pub crcs: Vec<u32>,
    crc: u32,
    next: i64,
}

impl Checkpoints {
    pub fn new(from: i64) -> Self {
        Self {
            positions: Vec::new(),
            crcs: Vec::new(),
            crc: 0,
            next: (from / CHECKPOINT + 1) * CHECKPOINT,
        }
    }

    /// Takes a fragment's payload, `position` the log's position after it.
    pub fn on_fragment(&mut self, payload: &[u8], position: i64) {
        self.crc = crc32c::crc32c_append(self.crc, payload);
        if position >= self.next {
            self.keep(position);
            self.next = (position / CHECKPOINT + 1) * CHECKPOINT;
        }
    }

    /// The replay reached the range's end: its CRC there, unless kept already.
    pub fn end(&mut self, position: i64) {
        if self.positions.last() != Some(&position) {
            self.keep(position);
        }
    }

    pub fn crc(&self) -> u32 {
        self.crc
    }

    fn keep(&mut self, position: i64) {
        self.positions.push(position);
        self.crcs.push(self.crc);
    }
}

/// Where `other` first departs from `reference`: the last position both
/// agree to, or the range's start `from`, and the position of the first
/// checkpoint unlike; `None` if every checkpoint is alike.
pub fn first_difference(from: i64, reference: &Checkpoints, other: &Checkpoints) -> Option<(i64, i64)> {
    let mut agreed = from;
    for i in 0..reference.positions.len().max(other.positions.len()) {
        let r = reference.positions.get(i).zip(reference.crcs.get(i));
        let o = other.positions.get(i).zip(other.crcs.get(i));
        if r.is_none() || r != o {
            let at = r.or(o).map_or(agreed, |(p, _)| *p);
            return Some((agreed, at));
        }
        agreed = reference.positions[i];
    }
    None
}

pub use node::{CheckOptions, check};

mod node {
    use std::time::{Duration, Instant};

    use rusteron_archive::*;

    use super::{Checkpoints, first_difference};

    /// How long a replay may go without a fragment before it is given up.
    const STALL: Duration = Duration::from_secs(30);

    /// Where the logs are, and where their replays come to.
    #[derive(Clone, Debug)]
    pub struct CheckOptions {
        /// Each node's cluster archive's control endpoint, `host:port`: node `n` is `archives[n]`.
        pub archives: Vec<String>,
        /// The host the replays come to, this client's.
        pub client_host: String,
        /// The cluster log's stream: Aeron's default is 100.
        pub log_stream: i32,
        /// Node `n`'s replay comes to this port + `n`, on stream 7300 + `n`.
        pub replay_port_base: u32,
    }

    /// A node's log recording: its id, where it starts, and how far it is recorded.
    struct Recording {
        recording_id: i64,
        start: i64,
        recorded: i64,
    }

    fn error<E: std::fmt::Debug>(what: &str) -> impl Fn(E) -> String + '_ {
        move |e| format!("{what}: {e:?}")
    }

    /// Compares the logs of the nodes whose archives `options` names, a
    /// replay at a time to its client host through `aeron`'s driver. Prints a
    /// `logs` line for the result, and one for each node unlike the first or
    /// unread; true if all are the same.
    pub fn check(aeron: &Aeron, options: &CheckOptions) -> bool {
        let started = Instant::now();
        let client_host = options.client_host.as_str();
        let mut recordings = Vec::new();
        for (n, archive) in options.archives.iter().enumerate() {
            match connect(aeron, archive, client_host).and_then(|a| log_recording(&a, options.log_stream)) {
                Ok(Some(r)) => recordings.push(r),
                Ok(None) => println!("logs       node {n}'s archive has no log recording"),
                Err(e) => println!("logs       node {n}'s archive did not answer: {e}"),
            }
        }
        if recordings.len() < options.archives.len() {
            println!("logs       FAILED: not every node's log could be read");
            return false;
        }
        let from = recordings.iter().map(|r| r.start).max().unwrap_or(0);
        let to = recordings.iter().map(|r| r.recorded).min().unwrap_or(0);
        if from >= to {
            println!("logs       FAILED: no range every node holds (from {from} to {to})");
            return false;
        }
        let mut ok = true;
        let mut replays = Vec::new();
        for (n, archive) in options.archives.iter().enumerate() {
            let replay_endpoint = (
                client_host,
                options.replay_port_base.saturating_add(n as u32),
                7_300 + n as i32,
            );
            let replayed = connect(aeron, archive, client_host)
                .and_then(|archive| replay(aeron, &archive, recordings[n].recording_id, from, to, replay_endpoint));
            match replayed {
                Ok(checkpoints) => replays.push(Some(checkpoints)),
                Err(e) => {
                    println!("logs       node {n}'s log: {e}");
                    replays.push(None);
                    ok = false;
                }
            }
        }
        if let Some(reference) = &replays[0] {
            for (n, other) in replays.iter().enumerate().skip(1) {
                if let Some((agreed, at)) = other.as_ref().and_then(|o| first_difference(from, reference, o)) {
                    println!("logs       node {n}'s log differs from node 0's between {agreed} and {at}");
                    ok = false;
                }
            }
            if ok {
                let ids: Vec<String> = recordings
                    .iter()
                    .enumerate()
                    .map(|(n, r)| format!("{n}:{}", r.recording_id))
                    .collect();
                println!(
                    "logs       the same on every node from {from} to {to}, {} checkpoints, CRC-32C {:08x}; recordings {}; \
                     in {:.1} s",
                    reference.positions.len(),
                    reference.crc(),
                    ids.join(", "),
                    started.elapsed().as_secs_f64()
                );
                return true;
            }
        }
        println!("logs       FAILED");
        false
    }

    fn connect(aeron: &Aeron, archive: &str, client_host: &str) -> Result<AeronArchive, String> {
        let context = AeronArchiveContext::new().map_err(error("archive context"))?;
        context.set_aeron(aeron).map_err(error("archive context"))?;
        let control = format!("aeron:udp?endpoint={archive}");
        context
            .set_control_request_channel(&control.into_c_string())
            .map_err(error("control channel"))?;
        let response = format!("aeron:udp?endpoint={client_host}:0");
        context
            .set_control_response_channel(&response.into_c_string())
            .map_err(error("response channel"))?;
        AeronArchiveAsyncConnect::new_with_aeron(&context, aeron)
            .map_err(error("archive connect"))?
            .poll_blocking(Duration::from_secs(10))
            .map_err(error("archive connect"))
    }

    /// The archive's newest recording of the log stream, if any.
    fn log_recording(archive: &AeronArchive, log_stream: i32) -> Result<Option<Recording>, String> {
        let Some(newest) = archive
            .collect_recordings(|d| d.stream_id() == log_stream)
            .map_err(error("recordings"))?
            .into_iter()
            .max_by_key(|d| d.recording_id)
        else {
            return Ok(None);
        };
        let recorded = match archive.get_recording_position(newest.recording_id) {
            Ok(position) if position >= 0 => position,
            _ => archive
                .get_stop_position(newest.recording_id)
                .map_err(error("stop position"))?,
        };
        Ok(Some(Recording {
            recording_id: newest.recording_id,
            start: newest.start_position,
            recorded,
        }))
    }

    /// Replays `recording_id` from `from` to `to` to `(host, port, stream)`, keeping its checkpoints.
    fn replay(
        aeron: &Aeron,
        archive: &AeronArchive,
        recording_id: i64,
        from: i64,
        to: i64,
        (host, port, stream): (&str, u32, i32),
    ) -> Result<Checkpoints, String> {
        let channel = format!("aeron:udp?endpoint={host}:{port}");
        let params = AeronArchiveReplayParams::builder()
            .position(from)
            .length(to - from)
            .build()
            .map_err(error("replay params"))?;
        let session = archive
            .start_replay(recording_id, &channel.clone().into_c_string(), stream, &params)
            .map_err(error("replay"))? as i32;
        let replay_channel = ChannelUri::add_session_id(&channel, session);
        let subscription = aeron
            .async_add_subscription(&replay_channel.into_c_string(), stream, Handlers::NONE, Handlers::NONE)
            .map_err(error("replay subscription"))?
            .poll_blocking(Duration::from_secs(10))
            .map_err(error("replay subscription"))?;
        let mut checkpoints = Checkpoints::new(from);
        let mut last = Instant::now();
        let mut position = from;
        while position < to {
            let image = subscription.image_by_session_id(session);
            let fragments = image.as_ref().map_or(0, |image| {
                image
                    .poll_fn(
                        |payload: &[u8], header: &AeronHeader| checkpoints.on_fragment(payload, header.position()),
                        256,
                    )
                    .map_or(0, |n| n.max(0))
            });
            if let Some(image) = &image {
                position = image.position();
            }
            if fragments > 0 {
                last = Instant::now();
            } else {
                let closed = image.as_ref().is_some_and(|image| image.is_closed());
                if closed || last.elapsed() > STALL {
                    return Err(format!(
                        "the replay of recording {recording_id} stopped at {position} of {to}"
                    ));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        if position != to {
            return Err(format!(
                "the replay of recording {recording_id} went past {to} to {position}"
            ));
        }
        checkpoints.end(to);
        Ok(checkpoints)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: i64 = CHECKPOINT;
    /// A fragment's room in the log: its header and 64 bytes, aligned.
    const FRAME: i64 = 96;

    /// A log of 64-byte fragments from `from` to `to`, one every [`FRAME`];
    /// the fragment holding `flip_at` has a byte changed, and the one holding
    /// `skip_at` is not delivered.
    fn replay(from: i64, to: i64, flip_at: i64, skip_at: i64) -> Checkpoints {
        let mut checkpoints = Checkpoints::new(from);
        let mut start = from;
        while start < to {
            let end = start + FRAME;
            let mut payload: Vec<u8> = (0..64).map(|i| (start / FRAME + i) as u8).collect();
            if (start..end).contains(&flip_at) {
                payload[7] = !payload[7];
            }
            if !(start..end).contains(&skip_at) {
                checkpoints.on_fragment(&payload, end);
            }
            start = end;
        }
        checkpoints.end(to);
        checkpoints
    }

    #[test]
    fn the_first_mib_where_two_logs_part_is_found() {
        let to = FRAME * 40_000;
        let log = replay(0, to, -1, -1);
        assert_eq!(log.positions.len(), 4, "1, 2 and 3 MiB, and the end");
        assert!(
            (MIB..MIB + FRAME).contains(&log.positions[0]),
            "the fragment past 1 MiB"
        );
        assert_eq!(log.positions[3], to);
        assert_eq!(first_difference(0, &log, &replay(0, to, -1, -1)), None);

        let flipped = replay(0, to, 2 * MIB + MIB / 2, -1);
        assert_eq!(
            first_difference(0, &log, &flipped),
            Some((log.positions[1], log.positions[2])),
            "between 2 and 3 MiB"
        );
        let skipped = replay(0, to, -1, MIB / 2);
        assert_eq!(
            first_difference(0, &log, &skipped),
            Some((0, log.positions[0])),
            "a fragment missing before 1 MiB"
        );
        let last = replay(0, to, to - 1, -1);
        assert_eq!(
            first_difference(0, &log, &last),
            Some((log.positions[2], to)),
            "the last fragment unlike"
        );
    }

    #[test]
    fn a_range_from_mid_log_is_checkpointed_from_the_next_mib() {
        let from = MIB + MIB / 2 + 32;
        let to = from + FRAME * 20_000;
        let log = replay(from, to, -1, -1);
        assert_eq!(log.positions.len(), 3, "2 and 3 MiB, and the end");
        assert!((2 * MIB..2 * MIB + FRAME).contains(&log.positions[0]));
        assert_eq!(
            first_difference(from, &log, &replay(from, to, from + 10, -1)),
            Some((from, log.positions[0])),
            "unlike from the range's start"
        );
    }

    #[test]
    fn its_crc_is_java_s() {
        // java.util.zip.CRC32C of "123456789", the check value of CRC-32C, taken in two fragments.
        let mut checkpoints = Checkpoints::new(0);
        checkpoints.on_fragment(b"1234", 32);
        checkpoints.on_fragment(b"56789", 64);
        assert_eq!(checkpoints.crc(), 0xe306_9283);
    }
}
