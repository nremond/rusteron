//! A service's snapshot: the container's part (markers around the client
//! sessions) as Java writes it, then the service's own bytes, which the
//! container moves without looking at them.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};

use rusteron_cluster_codecs::cluster::{
    Decoder, WriteBuf, client_session_codec::ClientSessionDecoder, client_session_codec::ClientSessionEncoder,
    cluster_time_unit::ClusterTimeUnit, message_header_codec, snapshot_mark::SnapshotMark,
    snapshot_marker_codec::SnapshotMarkerDecoder, snapshot_marker_codec::SnapshotMarkerEncoder,
};

use super::api::{ClientSession, TimeUnit};
use super::decode;

const HEADER: usize = message_header_codec::ENCODED_LENGTH;
/// The type id a service container's snapshot markers carry.
pub const SERVICE_SNAPSHOT_TYPE_ID: i64 = 2;

/// What a pull from a snapshot source gave.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pull {
    /// This many bytes were written to the buffer.
    Data(usize),
    /// Nothing yet: pull again later.
    Pending,
    /// The snapshot is complete.
    Done,
    /// The snapshot cannot be completed.
    Failed(String),
}

/// What finishing a snapshot sink gave.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Finish {
    /// Still restoring: ask again later.
    Pending,
    Done,
    Failed(String),
}

/// The bytes of a snapshot being taken, pulled by the container without
/// blocking, a publication's payload at a time.
pub trait SnapshotSource {
    fn pull(&mut self, buf: &mut [u8]) -> Pull;
}

/// Where the bytes of a snapshot being loaded go, in the chunks they were
/// taken in.
pub trait SnapshotSink {
    fn push(&mut self, chunk: &[u8]) -> Result<(), String>;
    /// The last chunk has been pushed.
    fn finish(&mut self) -> Finish;
}

/// A snapshot already in memory.
#[derive(Debug, Default)]
pub struct BytesSource {
    bytes: Vec<u8>,
    at: usize,
}

impl BytesSource {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes, at: 0 }
    }
}

impl SnapshotSource for BytesSource {
    fn pull(&mut self, buf: &mut [u8]) -> Pull {
        let rest = &self.bytes[self.at..];
        if rest.is_empty() {
            return Pull::Done;
        }
        let n = rest.len().min(buf.len());
        buf[..n].copy_from_slice(&rest[..n]);
        self.at += n;
        Pull::Data(n)
    }
}

/// A sink that keeps the snapshot in memory, for whoever holds the other handle.
pub fn vec_sink() -> (Box<dyn SnapshotSink>, Rc<RefCell<Vec<u8>>>) {
    let bytes = Rc::new(RefCell::new(Vec::new()));
    (Box::new(VecSink(bytes.clone())), bytes)
}

struct VecSink(Rc<RefCell<Vec<u8>>>);

impl SnapshotSink for VecSink {
    fn push(&mut self, chunk: &[u8]) -> Result<(), String> {
        self.0.borrow_mut().extend_from_slice(chunk);
        Ok(())
    }

    fn finish(&mut self) -> Finish {
        Finish::Done
    }
}

enum Chunk {
    Data(Vec<u8>),
    Done,
    Failed(String),
}

/// A snapshot written on another thread: what that thread writes to the
/// [`SnapshotWriter`], the container pulls from the [`ChannelSource`].
/// `chunks` bounds what is in flight between them, `chunk_length` bytes each.
pub fn channel_source(chunks: usize, chunk_length: usize) -> (SnapshotWriter, ChannelSource) {
    let (tx, rx) = sync_channel(chunks);
    (
        SnapshotWriter {
            tx: Some(tx),
            buffer: Vec::with_capacity(chunk_length),
            chunk_length,
        },
        ChannelSource {
            rx,
            current: Vec::new(),
            at: 0,
        },
    )
}

/// The writing end of [`channel_source`]. Dropped without [`Self::finish`],
/// it fails the snapshot.
pub struct SnapshotWriter {
    tx: Option<SyncSender<Chunk>>,
    buffer: Vec<u8>,
    chunk_length: usize,
}

impl SnapshotWriter {
    /// The snapshot is complete.
    pub fn finish(mut self) -> std::io::Result<()> {
        self.send_buffer()?;
        self.send(Chunk::Done)?;
        self.tx = None;
        Ok(())
    }

    /// The snapshot cannot be completed.
    pub fn fail(mut self, reason: impl Into<String>) {
        let _ = self.send(Chunk::Failed(reason.into()));
        self.tx = None;
    }

    fn send(&self, chunk: Chunk) -> std::io::Result<()> {
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| std::io::Error::other("snapshot writer already finished"))?;
        tx.send(chunk)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "snapshot no longer wanted"))
    }

    fn send_buffer(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::replace(&mut self.buffer, Vec::with_capacity(self.chunk_length));
        self.send(Chunk::Data(chunk))
    }
}

impl std::io::Write for SnapshotWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = buf.len().min(self.chunk_length - self.buffer.len());
        self.buffer.extend_from_slice(&buf[..n]);
        if self.buffer.len() == self.chunk_length {
            self.send_buffer()?;
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.send_buffer()
    }
}

impl Drop for SnapshotWriter {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Chunk::Failed("snapshot writer dropped before finishing".into()));
        }
    }
}

/// The container's end of [`channel_source`].
pub struct ChannelSource {
    rx: Receiver<Chunk>,
    current: Vec<u8>,
    at: usize,
}

impl SnapshotSource for ChannelSource {
    fn pull(&mut self, buf: &mut [u8]) -> Pull {
        if self.at == self.current.len() {
            match self.rx.try_recv() {
                Ok(Chunk::Data(chunk)) => {
                    self.current = chunk;
                    self.at = 0;
                }
                Ok(Chunk::Done) => return Pull::Done,
                Ok(Chunk::Failed(reason)) => return Pull::Failed(reason),
                Err(TryRecvError::Empty) => return Pull::Pending,
                Err(TryRecvError::Disconnected) => return Pull::Failed("snapshot writer gone".into()),
            }
        }
        let rest = &self.current[self.at..];
        let n = rest.len().min(buf.len());
        buf[..n].copy_from_slice(&rest[..n]);
        self.at += n;
        Pull::Data(n)
    }
}

pub(crate) fn cluster_time_unit(unit: TimeUnit) -> ClusterTimeUnit {
    match unit {
        TimeUnit::Millis => ClusterTimeUnit::MILLIS,
        TimeUnit::Micros => ClusterTimeUnit::MICROS,
        TimeUnit::Nanos => ClusterTimeUnit::NANOS,
    }
}

/// As Java's `ClusterClock.map`: a time unit not given is milliseconds.
pub(crate) fn time_unit(unit: ClusterTimeUnit) -> TimeUnit {
    match unit {
        ClusterTimeUnit::MICROS => TimeUnit::Micros,
        ClusterTimeUnit::NANOS => TimeUnit::Nanos,
        _ => TimeUnit::Millis,
    }
}

/// A snapshot marker, as `SnapshotTaker.markSnapshot` writes it.
pub(crate) fn marker(
    log_position: i64,
    leadership_term_id: i64,
    mark: SnapshotMark,
    time_unit: TimeUnit,
    app_version: i32,
) -> Vec<u8> {
    let mut bytes = vec![0; HEADER + 40];
    let mut e = SnapshotMarkerEncoder::default()
        .wrap(WriteBuf::new(&mut bytes), HEADER)
        .header(0)
        .parent()
        .expect("parent");
    e.type_id(SERVICE_SNAPSHOT_TYPE_ID)
        .log_position(log_position)
        .leadership_term_id(leadership_term_id)
        .index(0)
        .mark(mark)
        .time_unit(cluster_time_unit(time_unit))
        .app_version(app_version);
    bytes
}

/// A client session in a snapshot, as `ServiceSnapshotTaker.snapshotSession` writes it.
pub(crate) fn client_session(session: &ClientSession) -> Vec<u8> {
    let length = HEADER + 12 + 4 + session.response_channel.len() + 4 + session.encoded_principal.len();
    let mut bytes = vec![0; length];
    let mut e = ClientSessionEncoder::default()
        .wrap(WriteBuf::new(&mut bytes), HEADER)
        .header(0)
        .parent()
        .expect("parent");
    e.cluster_session_id(session.id)
        .response_stream_id(session.response_stream_id)
        .response_channel(session.response_channel.as_bytes())
        .encoded_principal(&session.encoded_principal);
    bytes
}

/// Reads a snapshot as it is replayed: the markers and sessions the
/// container wrote, then the service's bytes, into its sink.
pub(crate) struct Loader {
    in_snapshot: bool,
    after_end: bool,
    pub(crate) app_version: i32,
    pub(crate) time_unit: TimeUnit,
    pub(crate) sessions: Vec<ClientSession>,
    pub(crate) sink: Box<dyn SnapshotSink>,
}

impl Loader {
    pub(crate) fn new(sink: Box<dyn SnapshotSink>) -> Self {
        Self {
            in_snapshot: false,
            after_end: false,
            app_version: 0,
            time_unit: TimeUnit::Millis,
            sessions: Vec::new(),
            sink,
        }
    }

    /// Whether the container's part is over.
    pub(crate) fn is_after_end(&self) -> bool {
        self.after_end
    }

    pub(crate) fn on_message(&mut self, message: &[u8]) -> Result<(), String> {
        if self.after_end {
            return self.sink.push(message);
        }
        let header = decode::header(message)?;
        match header.template_id {
            100 => {
                let d: SnapshotMarkerDecoder<'_> = decode::body(message, &header, 40, |b, h| {
                    SnapshotMarkerDecoder::default().wrap(b, HEADER, h.block_length, h.version)
                })?;
                if d.type_id() != SERVICE_SNAPSHOT_TYPE_ID {
                    return Err(format!("unexpected snapshot type: {}", d.type_id()));
                }
                match d.mark() {
                    SnapshotMark::BEGIN => {
                        if self.in_snapshot {
                            return Err("already in snapshot".into());
                        }
                        self.in_snapshot = true;
                        self.app_version = d.app_version().unwrap_or(0);
                        self.time_unit = time_unit(d.time_unit());
                    }
                    SnapshotMark::END => {
                        if !self.in_snapshot {
                            return Err("missing begin snapshot".into());
                        }
                        self.after_end = true;
                    }
                    _ => {}
                }
            }
            102 => {
                let mut d = decode::body(message, &header, 12, |b, h| {
                    ClientSessionDecoder::default().wrap(b, HEADER, h.block_length, h.version)
                })?;
                let id = d.cluster_session_id();
                let response_stream_id = d.response_stream_id();
                decode::check_var(message, d.get_limit())?;
                let c = d.response_channel_decoder();
                let response_channel = String::from_utf8_lossy(d.response_channel_slice(c)).into_owned();
                decode::check_var(message, d.get_limit())?;
                let p = d.encoded_principal_decoder();
                let encoded_principal = d.encoded_principal_slice(p).to_vec();
                self.sessions.push(ClientSession {
                    id,
                    response_stream_id,
                    response_channel,
                    encoded_principal,
                    closing: false,
                    connected: false,
                });
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn a_bytes_source_hands_out_its_bytes_then_ends() {
        let mut source = BytesSource::new((0..10).collect());
        let mut buf = [0; 4];
        assert_eq!(source.pull(&mut buf), Pull::Data(4));
        assert_eq!(source.pull(&mut buf), Pull::Data(4));
        assert_eq!(source.pull(&mut buf), Pull::Data(2));
        assert_eq!(&buf[..2], &[8, 9]);
        assert_eq!(source.pull(&mut buf), Pull::Done);
    }

    #[test]
    fn a_channel_source_takes_what_another_thread_writes() {
        let (mut writer, mut source) = channel_source(2, 3);
        let mut buf = [0; 2];
        assert_eq!(source.pull(&mut buf), Pull::Pending);
        let thread = std::thread::spawn(move || {
            writer.write_all(b"abcdefg").unwrap();
            writer.finish().unwrap();
        });
        let mut got = Vec::new();
        loop {
            match source.pull(&mut buf) {
                Pull::Data(n) => got.extend_from_slice(&buf[..n]),
                Pull::Pending => std::thread::yield_now(),
                Pull::Done => break,
                Pull::Failed(e) => panic!("{e}"),
            }
        }
        thread.join().unwrap();
        assert_eq!(got, b"abcdefg");
    }

    #[test]
    fn a_writer_dropped_unfinished_fails_the_snapshot() {
        let (writer, mut source) = channel_source(2, 3);
        drop(writer);
        assert!(matches!(source.pull(&mut [0; 4]), Pull::Failed(_)));
    }

    #[test]
    fn the_loader_reads_back_what_the_taker_writes() {
        let session = ClientSession {
            id: 9,
            response_stream_id: 102,
            response_channel: "aeron:udp?endpoint=localhost:20121".into(),
            encoded_principal: vec![1, 2],
            closing: false,
            connected: false,
        };
        let (sink, bytes) = vec_sink();
        let mut loader = Loader::new(sink);
        loader
            .on_message(&marker(4_096, 3, SnapshotMark::BEGIN, TimeUnit::Nanos, 7))
            .unwrap();
        loader.on_message(&client_session(&session)).unwrap();
        loader
            .on_message(&marker(4_096, 3, SnapshotMark::END, TimeUnit::Nanos, 7))
            .unwrap();
        assert!(loader.is_after_end());
        loader.on_message(b"service state").unwrap();
        assert_eq!(loader.sessions, vec![session]);
        assert_eq!((loader.app_version, loader.time_unit), (7, TimeUnit::Nanos));
        assert_eq!(&*bytes.borrow(), b"service state");
    }

    #[test]
    fn the_loader_refuses_an_end_before_a_begin_and_a_short_session() {
        let (sink, _) = vec_sink();
        let mut loader = Loader::new(sink);
        assert!(
            loader
                .on_message(&marker(0, 0, SnapshotMark::END, TimeUnit::Millis, 0))
                .is_err()
        );
        let session = ClientSession {
            id: 1,
            response_stream_id: 1,
            response_channel: "aeron:ipc".into(),
            encoded_principal: vec![],
            closing: false,
            connected: false,
        };
        let bytes = client_session(&session);
        assert!(loader.on_message(&bytes[..bytes.len() - 3]).is_err());
    }
}
