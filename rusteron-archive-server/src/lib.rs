//! Aeron Archive's server on Aeron's C client, wire- and on-disk-compatible
//! with Aeron 1.54.2: the archive's conductor ([`rusteron_archive_core`])
//! driven on one thread, as Java's SHARED mode, the client's conductor run
//! on that thread too.
//!
//! Each duty cycle reads the wall clock once, then: every millisecond, the
//! client's conductor, the mark file and the gauges; the images that came
//! and went; the control requests; the control sessions' work; the
//! replayers, segment files onto replays' publications; the recorders, an
//! image's blocks into its segment files; the catalog's changes into its
//! file. The conductor's I/O runs as it asks for it.

pub mod counters;
pub mod markfile;
pub mod recorder;
pub mod replayer;

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusteron_archive::fragment_assembler::{Fragment, PollAction, Reassembler};
use rusteron_archive::*;
use rusteron_archive_core::catalog::{CATALOG_FILE_NAME, Catalog, DEFAULT_CAPACITY, NULL_POSITION, RecordingState};
use rusteron_archive_core::conductor::{
    Action, Conductor, Config, Counters, EraseError, ExpectedFrame, ImageInfo, Io, Offer, RecordingError, ReplayLimit,
    RequestImage, ResponseChannels, SubscriptionToken,
};
use rusteron_archive_core::counters as archive_counters;
use rusteron_archive_core::protocol::error_code;
use rusteron_archive_core::replay::{ReplayError, ReplaySession};
use rusteron_archive_core::segments::{
    Recovery, compute_stop_position, parse_segment_file_name, segment_file_name, truncate_torn_fragment,
};
use rusteron_archive_core::verify::{Verdict, verify_recording, wants_more};

use counters::{add, load, propose_max, store};
use markfile::{ArchiveHeader, ArchiveMarkFile, UPDATE_INTERVAL_MS};
use recorder::{Polled, Recorder, Sync, WriteStats};
use replayer::{LimitCounter, ReadStats, Replayed, Replayer};

const ADD_TIMEOUT: Duration = Duration::from_secs(10);
/// Java polls each control subscription for this many fragments a cycle.
const CONTROL_FRAGMENT_LIMIT: usize = 10;
/// A duty cycle longer than this is counted.
const CYCLE_THRESHOLD_NS: i64 = 100_000_000;

/// How the archive's thread waits when a duty cycle found nothing to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Idle {
    /// Never waits: lowest latency, a core of its own.
    Spin,
    /// Spins, yields, then parks up to a millisecond, as Aeron's default.
    Backoff,
}

/// Where an archive keeps its files and listens, and how it runs. The
/// defaults are Java's.
#[derive(Clone, Debug)]
pub struct ArchiveContext {
    pub aeron_dir: String,
    pub archive_dir: PathBuf,
    /// The mark file's directory: the archive's unless said.
    pub mark_file_dir: Option<PathBuf>,
    /// The control channel over UDP; none, as Java's
    /// `aeron.archive.control.channel.enabled=false`.
    pub control_channel: Option<String>,
    pub control_stream_id: i32,
    pub local_control_channel: String,
    pub local_control_stream_id: i32,
    /// Where a replication receives the source archive's replay.
    pub replication_channel: String,
    /// Where a replication hears from the source archive, unless its
    /// request says (Java's `archiveClientContext.controlResponseChannel`).
    pub archive_client_response_channel: String,
    /// Named in the mark file; recording events are not sent.
    pub recording_events_channel: String,
    pub recording_events_stream_id: i32,
    /// `None`: the driver's next correlation id, as Java's.
    pub archive_id: Option<i64>,
    pub catalog_capacity: usize,
    /// 0: no sync; 1: the data after each write; 2: and the metadata.
    pub file_sync_level: i32,
    pub catalog_file_sync_level: i32,
    pub error_buffer_length: usize,
    pub file_page_size: usize,
    /// A recording left active whose last fragment a crash cut short across a
    /// page boundary: truncated before it, as `ArchiveTool verify` truncates
    /// one when told to. Off, the archive refuses to start, as Java's does.
    pub truncate_torn_fragments: bool,
    /// Every recording checked at start as `ArchiveTool verify` checks it
    /// ([`rusteron_archive_core::verify`]): a damaged one marked invalid, the
    /// archive listing it no more; a whole one's stop position where its
    /// frames end; an invalid one found whole valid again. A torn last
    /// fragment is truncated if `truncate_torn_fragments`, else damage.
    pub verify: bool,
    /// The conductor's settings; its archive id and first session id are set at launch.
    pub config: Config,
    pub idle: Idle,
}

impl ArchiveContext {
    pub fn new(aeron_dir: impl Into<String>, archive_dir: impl Into<PathBuf>) -> Self {
        Self {
            aeron_dir: aeron_dir.into(),
            archive_dir: archive_dir.into(),
            mark_file_dir: None,
            control_channel: None,
            control_stream_id: 10,
            local_control_channel: "aeron:ipc?term-length=64k".into(),
            local_control_stream_id: 10,
            replication_channel: "aeron:udp?endpoint=localhost:0".into(),
            archive_client_response_channel: "aeron:udp?endpoint=localhost:0".into(),
            recording_events_channel: String::new(),
            recording_events_stream_id: 30,
            archive_id: None,
            catalog_capacity: DEFAULT_CAPACITY,
            file_sync_level: 0,
            catalog_file_sync_level: 0,
            error_buffer_length: 1024 * 1024,
            file_page_size: 4_096,
            truncate_torn_fragments: false,
            verify: false,
            config: Config::default(),
            idle: Idle::Backoff,
        }
    }
}

/// Why an archive could not start or run.
#[derive(Debug)]
pub enum ArchiveError {
    Aeron(String),
    Io(io::Error),
    Catalog(String),
}

impl std::fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Aeron(e) => write!(f, "aeron: {e}"),
            Self::Io(e) => write!(f, "{e}"),
            Self::Catalog(e) => write!(f, "catalog: {e}"),
        }
    }
}

impl std::error::Error for ArchiveError {}

impl From<AeronCError> for ArchiveError {
    fn from(e: AeronCError) -> Self {
        Self::Aeron(e.to_string())
    }
}

impl From<io::Error> for ArchiveError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

fn epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(unix)]
fn write_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, bytes, offset)
}

/// The bytes the archive's file system has for an unprivileged writer.
#[cfg(unix)]
fn usable_space(dir: &Path) -> Option<i64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is a NUL-terminated string and `stat` room for the result.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: statvfs succeeded, so it filled `stat`.
    let stat = unsafe { stat.assume_init() };
    Some((stat.f_bavail as i64).saturating_mul(stat.f_frsize as i64))
}

/// An image as the client's callbacks report it, for the duty cycle to
/// hand to the conductor.
enum ImageEvent {
    Available { subscription_id: i64, image: ImageInfo },
    Unavailable { subscription_id: i64, session_id: i32 },
}

/// Images as the client's conductor reports them, behind a lock: Aeron
/// keeps the callbacks, so they must be `Send` (rusteron 0.3).
#[derive(Clone, Default)]
struct ImageEvents(Arc<Mutex<Vec<ImageEvent>>>);

impl ImageEvents {
    fn lock(&self) -> MutexGuard<'_, Vec<ImageEvent>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn image_info<S: ResourceStorage<bindings::aeron_image_t>>(image: &AeronImage<S>) -> Option<ImageInfo> {
    let c = image.get_constants().ok()?;
    Some(ImageInfo {
        correlation_id: c.correlation_id(),
        session_id: c.session_id(),
        source_identity: c.source_identity().to_owned(),
        initial_term_id: c.initial_term_id(),
        term_buffer_length: c.term_buffer_length() as i32,
        mtu_length: c.mtu_length() as i32,
        join_position: c.join_position(),
    })
}

fn registration_id<S: ResourceStorage<bindings::aeron_subscription_t>>(subscription: &AeronSubscription<S>) -> i64 {
    subscription.get_constants().map(|c| c.registration_id()).unwrap_or(-1)
}

impl AeronAvailableImageCallback for ImageEvents {
    fn handle_aeron_on_available_image(&mut self, subscription: &AeronSubscriptionRef, image: &AeronImageRef) {
        if let Some(info) = image_info(image) {
            self.lock().push(ImageEvent::Available {
                subscription_id: registration_id(subscription),
                image: info,
            });
        }
    }
}

impl AeronUnavailableImageCallback for ImageEvents {
    fn handle_aeron_on_unavailable_image(&mut self, subscription: &AeronSubscriptionRef, image: &AeronImageRef) {
        let session_id = image.get_constants().map(|c| c.session_id()).unwrap_or(0);
        self.lock().push(ImageEvent::Unavailable {
            subscription_id: registration_id(subscription),
            session_id,
        });
    }
}

/// A control subscription: requests from clients' publications, one
/// message at a time per image.
struct ControlSubscription {
    subscription: AeronSubscription,
    registration_id: i64,
    channel: String,
    stream_id: i32,
    images: HashMap<i32, RequestImage>,
    reassemblers: HashMap<i32, Reassembler>,
}

/// A control session's publication and counter, being added or added.
#[derive(Default)]
struct SessionIo {
    adding_publication: Option<AeronAsyncAddExclusivePublication>,
    publication: Option<AeronExclusivePublication>,
    adding_counter: Option<AeronAsyncAddCounter>,
    counter: Option<AeronCounter>,
}

/// A replication's links to its source archive: its requests' publication,
/// the subscription the source answers on.
#[derive(Default)]
struct SourceLink {
    adding_publication: Option<AeronAsyncAddExclusivePublication>,
    publication: Option<AeronExclusivePublication>,
    adding_subscription: Option<AeronAsyncAddSubscription>,
    subscription: Option<AeronSubscription>,
    reassembler: Reassembler,
    /// The response channel told to the core, once its port was resolved.
    linked: bool,
}

fn offer_result(result: Result<i64, AeronOfferError>) -> Offer {
    match result {
        Ok(_) => Offer::Sent,
        Err(AeronOfferError::NotConnected) => Offer::NotConnected,
        Err(AeronOfferError::BackPressured | AeronOfferError::AdminAction) => Offer::Retry,
        Err(AeronOfferError::MaxPositionExceeded) => Offer::MaxPositionExceeded,
        Err(_) => Offer::Closed,
    }
}

/// The shell's publications and subscriptions, as the conductor's work needs them.
struct Publications<'a> {
    sessions: &'a HashMap<i64, SessionIo>,
    links: &'a HashMap<i64, SourceLink>,
    subscriptions: &'a HashMap<SubscriptionToken, AeronSubscription>,
}

impl ResponseChannels for Publications<'_> {
    fn offer(&mut self, control_session_id: i64, message: &[u8]) -> Offer {
        match self
            .sessions
            .get(&control_session_id)
            .and_then(|s| s.publication.as_ref())
        {
            Some(publication) => offer_result(publication.offer(message)),
            None => Offer::NotConnected,
        }
    }

    fn is_connected(&self, control_session_id: i64) -> bool {
        self.sessions
            .get(&control_session_id)
            .and_then(|s| s.publication.as_ref())
            .is_some_and(|p| p.is_connected())
    }

    fn offer_source(&mut self, replication_id: i64, message: &[u8]) -> Offer {
        match self.links.get(&replication_id).and_then(|l| l.publication.as_ref()) {
            Some(publication) => offer_result(publication.offer(message)),
            None => Offer::NotConnected,
        }
    }

    /// Its request publication: the source's answers come once it is asked.
    fn source_connected(&self, replication_id: i64) -> bool {
        self.links
            .get(&replication_id)
            .and_then(|l| l.publication.as_ref())
            .is_some_and(|p| p.is_connected())
    }

    fn resolved_endpoint(&self, subscription: SubscriptionToken) -> Option<String> {
        let endpoint = self
            .subscriptions
            .get(&subscription)?
            .resolved_endpoint_as_string(256)
            .ok()?;
        (!endpoint.is_empty() && !endpoint.ends_with(":0")).then_some(endpoint)
    }
}

/// Java's archive client's defaults on a control channel: a term of 64 KiB,
/// the driver's MTU, sparse, and the connection's own session id.
fn conclude_control_channel(channel: &str, session_id: i32) -> Result<String, String> {
    let mut uri = rusteron_archive_core::channel::ChannelUri::parse(channel)?;
    for (key, value) in [("term-length", "65536"), ("mtu", "1408"), ("sparse", "true")] {
        if uri.get(key).is_none() {
            uri.put(key, value);
        }
    }
    uri.put("session-id", &session_id.to_string());
    Ok(uri.to_string())
}

/// The driver's counters, for the counters a request names.
struct ReaderCounters<'a>(&'a AeronCountersReaderRef);

/// Aeron's state of a counter in use.
const RECORD_ALLOCATED: i32 = 1;

impl Counters for ReaderCounters<'_> {
    fn value(&self, counter_id: i32) -> Option<i64> {
        (self.0.counter_state(counter_id).ok() == Some(RECORD_ALLOCATED)).then(|| load(self.0, counter_id))
    }
}

/// The counters the archive keeps, Java's.
struct ArchiveCounters {
    errors: AeronCounter,
    max_cycle_time: AeronCounter,
    cycle_time_exceeded: AeronCounter,
    control_sessions: AeronCounter,
    recording_sessions: AeronCounter,
    replay_sessions: AeronCounter,
    max_write_time: AeronCounter,
    total_write_bytes: AeronCounter,
    total_write_time: AeronCounter,
    max_read_time: AeronCounter,
    total_read_bytes: AeronCounter,
    total_read_time: AeronCounter,
}

/// An archive, running.
pub struct Archive {
    core: Conductor,
    ctx: ArchiveContext,
    aeron: Aeron,
    archive_dir: PathBuf,
    catalog_file: File,
    mark: ArchiveMarkFile,
    events: ImageEvents,
    image_handler: SharedHandler<ImageEvents>,
    control: Vec<ControlSubscription>,
    sessions: HashMap<i64, SessionIo>,
    source_links: HashMap<i64, SourceLink>,
    subscriptions: HashMap<SubscriptionToken, AeronSubscription>,
    tokens: HashMap<i64, SubscriptionToken>,
    recorders: BTreeMap<i64, Recorder>,
    adding_replays: HashMap<SubscriptionToken, AeronAsyncAddExclusivePublication>,
    replay_publications: HashMap<SubscriptionToken, AeronExclusivePublication>,
    replayers: BTreeMap<i64, Replayer>,
    replay_buffer: Vec<u8>,
    counters: ArchiveCounters,
    sync: Sync,
    cached_ms: i64,
    mark_deadline_ms: i64,
    idle_count: u32,
}

/// The highest segment file of a recording in `dir`: its base and bytes.
fn last_segment(dir: &Path, recording_id: i64) -> io::Result<Option<(i64, Vec<u8>)>> {
    let mut last = None;
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name();
        if let Some((id, base)) = name.to_str().and_then(parse_segment_file_name)
            && id == recording_id
            && last.is_none_or(|l| base > l)
        {
            last = Some(base);
        }
    }
    let Some(base) = last else { return Ok(None) };
    Ok(Some((
        base,
        std::fs::read(dir.join(segment_file_name(recording_id, base)))?,
    )))
}

/// Opens the catalog, or creates it, and recovers the stop positions of the
/// recordings left active from their last segment (Java's `refreshCatalog`);
/// a last fragment cut short across a page truncated if `truncate`, else
/// refused. If `verify`, every recording is checked first ([`verify_catalog`]),
/// as a Java node runs `ArchiveTool verify` before its archive starts: a
/// damaged recording left active is marked invalid then, not recovered.
fn open_catalog(
    dir: &Path,
    capacity: usize,
    truncate: bool,
    verify: bool,
    now_ms: i64,
) -> Result<(Catalog, File), ArchiveError> {
    let path = dir.join(CATALOG_FILE_NAME);
    let existing = path.exists();
    let mut catalog = if existing {
        Catalog::open(std::fs::read(&path)?, capacity).map_err(|e| ArchiveError::Catalog(e.to_string()))?
    } else {
        Catalog::create(capacity).map_err(|e| ArchiveError::Catalog(e.to_string()))?
    };
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;
    if verify {
        verify_catalog(dir, &mut catalog, truncate, now_ms)?;
    }
    for d in catalog.unstopped() {
        let segment = last_segment(dir, d.recording_id)?;
        let recovered = compute_stop_position(
            d.start_position,
            d.term_buffer_length,
            d.segment_file_length,
            segment.as_ref().map(|(base, bytes)| (*base, bytes.as_slice())),
        );
        let stop = match (recovered, &segment) {
            (Ok(stop), _) => stop,
            (Err(Recovery::PageStraddle { fragment_offset }), Some((base, _))) if truncate => {
                let name = segment_file_name(d.recording_id, *base);
                let file = OpenOptions::new().write(true).open(dir.join(&name))?;
                truncate_torn_fragment(&file, fragment_offset, d.segment_file_length as usize)?;
                log::warn!(
                    "{name} ends in a fragment cut short across a page boundary: truncated at {fragment_offset}"
                );
                (base + fragment_offset as i64).max(d.start_position)
            }
            (Err(Recovery::PageStraddle { fragment_offset }), _) => {
                return Err(ArchiveError::Catalog(format!(
                    "found potentially incomplete last fragment straddling page boundary in recording {} at \
                     offset {fragment_offset}: run ArchiveTool verify for corrective action",
                    d.recording_id
                )));
            }
        };
        catalog
            .recover_stop_position(d.recording_id, stop, now_ms)
            .map_err(|e| ArchiveError::Catalog(e.to_string()))?;
    }
    if !existing {
        file.set_len(catalog.len() as u64)?;
    }
    for (offset, bytes) in catalog.take_writes() {
        write_at(&file, &bytes, offset)?;
    }
    file.sync_all()?;
    Ok((catalog, file))
}

/// `ArchiveTool verify` on every recording, valid or invalid, as Java's
/// tool does it without options ([`verify_recording`]): a damaged one marked
/// invalid; a whole one's stop position set where its frames end, if the
/// catalog's differs (one left active is stopped there); an invalid one
/// found whole, with segment files, valid again. A torn last fragment is truncated if `truncate`, as the tool
/// truncates one when told to, and the recording checked again; else it is
/// damage. Logs what it finds and changes, and a line for the whole.
fn verify_catalog(dir: &Path, catalog: &mut Catalog, truncate: bool, now_ms: i64) -> io::Result<()> {
    let mut last_segments: BTreeMap<i64, i64> = BTreeMap::new();
    for entry in std::fs::read_dir(dir)? {
        if let Some((id, base)) = entry?.file_name().to_str().and_then(parse_segment_file_name) {
            let last = last_segments.entry(id).or_insert(base);
            *last = (*last).max(base);
        }
    }
    let (mut checked, mut damaged, mut recovered, mut corrected, mut revalidated) = (0, 0, 0, 0, 0);
    for (state, d) in catalog.entries() {
        let state = if state == RecordingState::Valid as i32 {
            RecordingState::Valid
        } else if state == RecordingState::Invalid as i32 {
            RecordingState::Invalid
        } else {
            continue;
        };
        checked += 1;
        let id = d.recording_id;
        let base = last_segments.get(&id).copied();
        let name = base.map(|base| segment_file_name(id, base));
        let start_term_offset = d.start_position & (i64::from(d.term_buffer_length) - 1);
        let from = match base {
            Some(base) if base == d.start_position - start_term_offset => start_term_offset as usize,
            _ => 0,
        };
        let read = |name: &str| read_frames(&dir.join(name), from, d.segment_file_length as usize);
        let mut segment = name.as_deref().map(read).transpose()?;
        let verdict = loop {
            match verify_recording(&d, base.zip(segment.as_deref())) {
                Verdict::Torn { fragment_offset } if truncate => {
                    let name = name.as_deref().expect("a torn fragment is in a segment file");
                    let file = OpenOptions::new().write(true).open(dir.join(name))?;
                    truncate_torn_fragment(&file, fragment_offset, d.segment_file_length as usize)?;
                    log::warn!(
                        "{name} ends in a fragment cut short across a page boundary: truncated at {fragment_offset}"
                    );
                    segment = Some(read(name)?);
                }
                Verdict::Torn { fragment_offset } => {
                    break Err(format!(
                        "found potentially incomplete last fragment straddling page boundary in {} at offset \
                         {fragment_offset}",
                        name.as_deref().unwrap_or("its last segment")
                    ));
                }
                Verdict::Damaged(message) => break Err(message),
                Verdict::Whole { stop_position } => break Ok(stop_position),
            }
        };
        match verdict {
            Err(message) => {
                damaged += 1;
                let file = name.map(|n| format!(", file={n}")).unwrap_or_default();
                log::warn!("(recordingId={id}{file}) ERR: {message}: the recording is marked invalid");
                catalog.verified(id, None, RecordingState::Invalid);
            }
            Ok(stop) => {
                let stop = (stop != d.stop_position).then_some((stop, now_ms));
                match stop {
                    Some(_) if d.stop_position == NULL_POSITION => recovered += 1,
                    Some((position, _)) => {
                        corrected += 1;
                        log::warn!(
                            "(recordingId={id}) its frames end at {position}: stop position {} corrected",
                            d.stop_position
                        );
                    }
                    None => {}
                }
                let state = if state == RecordingState::Invalid && base.is_some() {
                    revalidated += 1;
                    log::warn!("(recordingId={id}) invalid, found whole: valid again");
                    RecordingState::Valid
                } else {
                    state
                };
                catalog.verified(id, stop, state);
            }
        }
    }
    log::info!(
        "verified {checked} recordings: {damaged} damaged, {recovered} left active stopped where their frames \
         end, {corrected} stop positions corrected, {revalidated} valid again"
    );
    Ok(())
}

/// A segment file's bytes from its start to its frames' end from `from`
/// ([`wants_more`]), or to its end: what [`verify_recording`] reads of it.
fn read_frames(path: &Path, from: usize, segment_length: usize) -> io::Result<Vec<u8>> {
    use std::io::Read;
    const CHUNK: usize = 1 << 20;
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    while let Some(wanted) = wants_more(&bytes, from, segment_length) {
        let target = wanted.max(bytes.len() + CHUNK).min(segment_length.max(wanted));
        let read = (&mut file)
            .take((target - bytes.len()) as u64)
            .read_to_end(&mut bytes)?;
        if read == 0 {
            break;
        }
    }
    Ok(bytes)
}

/// [`open_catalog`] on a thread of its own, the client's conductor and the
/// mark file kept alive meanwhile. A recovery that reads and syncs large
/// segments on a busy disk can outlast a client's patience with a conductor
/// left uninvoked (the driver's timeout, 10 s): the client would close, and
/// the archive with it. Java's archive recovers on its conductor's thread
/// too, after its client connects; its ArchiveTool truncates before it starts.
fn recover_catalog(
    aeron: &Aeron,
    mark: &ArchiveMarkFile,
    dir: &Path,
    capacity: usize,
    truncate: bool,
    verify: bool,
    now_ms: i64,
) -> Result<(Catalog, File), ArchiveError> {
    let started = Instant::now();
    let opening = {
        let dir = dir.to_path_buf();
        std::thread::Builder::new()
            .name("catalog".into())
            .spawn(move || open_catalog(&dir, capacity, truncate, verify, now_ms))?
    };
    let mut mark_deadline_ms = now_ms + UPDATE_INTERVAL_MS;
    while !opening.is_finished() {
        aeron.main_do_work()?;
        let now_ms = epoch_ms();
        if now_ms >= mark_deadline_ms {
            mark_deadline_ms = now_ms + UPDATE_INTERVAL_MS;
            mark.update_activity(now_ms)?;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let opened = opening
        .join()
        .map_err(|_| ArchiveError::Catalog("the catalog's recovery panicked".into()))?;
    if started.elapsed() >= Duration::from_secs(1) {
        log::info!("catalog opened in {} ms", started.elapsed().as_millis());
    }
    opened
}

fn find_counter(counters: &AeronCountersReaderRef, type_id: i32, key: &[u8]) -> bool {
    let mut found = false;
    counters.foreach_counter_fn(|_value, _id, t, k, _label| {
        found |= t == type_id && k.starts_with(key);
    });
    found
}

impl Archive {
    /// Connects to the media driver, opens the catalog, writes the mark file,
    /// allocates the counters and subscribes to the control channels.
    pub fn launch(mut ctx: ArchiveContext) -> Result<Self, ArchiveError> {
        let context = AeronContext::new()?;
        context.set_dir(&ctx.aeron_dir.as_str().into_c_string())?;
        context.set_use_conductor_agent_invoker(true)?;
        context.set_client_name(&"archive".into_c_string())?;
        let aeron = Aeron::new(&context)?;
        aeron.start()?;

        let archive_id = ctx.archive_id.unwrap_or_else(|| aeron.next_correlation_id());
        if find_counter(
            &aeron.counters_reader(),
            archive_counters::ARCHIVE_ERROR_COUNT_TYPE_ID,
            &archive_id.to_le_bytes(),
        ) {
            return Err(ArchiveError::Aeron(format!(
                "found existing archive for archiveId={archive_id}"
            )));
        }

        let now_ms = epoch_ms();
        std::fs::create_dir_all(&ctx.archive_dir)?;
        let archive_dir = ctx.archive_dir.canonicalize()?;
        let header = ArchiveHeader {
            start_timestamp_ms: now_ms,
            pid: std::process::id() as i64,
            control_stream_id: ctx.control_stream_id,
            local_control_stream_id: ctx.local_control_stream_id,
            events_stream_id: ctx.recording_events_stream_id,
            error_buffer_length: ctx.error_buffer_length as i32,
            archive_id,
            control_channel: ctx.control_channel.clone().unwrap_or_default(),
            local_control_channel: ctx.local_control_channel.clone(),
            events_channel: ctx.recording_events_channel.clone(),
            aeron_directory: ctx.aeron_dir.clone(),
        };
        let mark_dir = ctx.mark_file_dir.clone().unwrap_or_else(|| archive_dir.clone());
        let mark = ArchiveMarkFile::create(&mark_dir, &header, ctx.file_page_size, now_ms)?;
        let (catalog, catalog_file) = recover_catalog(
            &aeron,
            &mark,
            &archive_dir,
            ctx.catalog_capacity,
            ctx.truncate_torn_fragments,
            ctx.verify,
            now_ms,
        )?;

        let (key, label) = archive_counters::error_counter(archive_id);
        let errors = aeron.add_counter(archive_counters::ARCHIVE_ERROR_COUNT_TYPE_ID, &key, &label, ADD_TIMEOUT)?;
        let mut others = Vec::new();
        for (type_id, name) in archive_counters::archive_counters() {
            let (key, label) = archive_counters::archive_counter(name, archive_id);
            others.push(aeron.add_counter(type_id, &key, &label, ADD_TIMEOUT)?);
        }
        let mut others = others.into_iter();
        let mut next = || others.next().expect("eleven counters");
        let counters = ArchiveCounters {
            errors,
            max_cycle_time: next(),
            cycle_time_exceeded: next(),
            control_sessions: next(),
            recording_sessions: next(),
            replay_sessions: next(),
            max_write_time: next(),
            total_write_bytes: next(),
            total_write_time: next(),
            max_read_time: next(),
            total_read_bytes: next(),
            total_read_time: next(),
        };

        let events = ImageEvents::default();
        let image_handler = SharedHandler::new_shared(events.clone());
        let mut channels = Vec::new();
        if let Some(channel) = &ctx.control_channel {
            let sparse = ctx.config.control.sparse;
            channels.push((format!("{channel}|sparse={sparse}"), ctx.control_stream_id));
        }
        channels.push((ctx.local_control_channel.clone(), ctx.local_control_stream_id));
        let mut control = Vec::new();
        for (channel, stream_id) in channels {
            let subscription = aeron.add_subscription(
                &channel.as_str().into_c_string(),
                stream_id,
                Some(&image_handler),
                Some(&image_handler),
                ADD_TIMEOUT,
            )?;
            control.push(ControlSubscription {
                registration_id: registration_id(&subscription),
                subscription,
                channel,
                stream_id,
                images: HashMap::new(),
                reassemblers: HashMap::new(),
            });
        }

        ctx.config.archive_id = archive_id;
        ctx.config.replication_channel = ctx.replication_channel.clone();
        ctx.config.archive_client_response_channel = ctx.archive_client_response_channel.clone();
        // Java's first session id: a random int.
        ctx.config.first_control_session_id = i64::from(
            (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0)
                ^ std::process::id().rotate_left(16))
                & 0x7FFF_FFFF,
        );
        let mut core = Conductor::new(ctx.config.clone(), catalog);
        core.on_tick(now_ms);
        let sync = Sync::from_level(ctx.file_sync_level);
        log::info!("archive {archive_id} in {} on {}", archive_dir.display(), ctx.aeron_dir);
        let replay_buffer = vec![0; ctx.config.file_io_max_length as usize];
        Ok(Self {
            core,
            ctx,
            aeron,
            archive_dir,
            catalog_file,
            mark,
            events,
            image_handler,
            control,
            sessions: HashMap::new(),
            source_links: HashMap::new(),
            subscriptions: HashMap::new(),
            tokens: HashMap::new(),
            recorders: BTreeMap::new(),
            adding_replays: HashMap::new(),
            replay_publications: HashMap::new(),
            replayers: BTreeMap::new(),
            replay_buffer,
            counters,
            sync,
            cached_ms: now_ms,
            mark_deadline_ms: now_ms + UPDATE_INTERVAL_MS,
            idle_count: 0,
        })
    }

    pub fn archive_id(&self) -> i64 {
        self.core.config().archive_id
    }

    pub fn conductor(&self) -> &Conductor {
        &self.core
    }

    /// Runs duty cycles until `running` says stop, then closes the archive.
    /// Runs duty cycles until `running` says stop or the media driver is
    /// gone, then closes the archive.
    pub fn run(mut self, running: impl Fn() -> bool) {
        while running() && !self.aeron.is_closed() {
            self.do_work();
        }
        if self.aeron.is_closed() {
            log::warn!("the media driver is gone");
        }
        self.close();
    }

    /// One duty cycle, then idling if it found nothing to do.
    pub fn do_work(&mut self) -> usize {
        let started = Instant::now();
        let now_ms = epoch_ms();
        let mut work = 0;
        if now_ms != self.cached_ms {
            self.cached_ms = now_ms;
            self.core.on_tick(now_ms);
            work += self.slow_tick(now_ms);
        }
        work += self.drain_image_events();
        work += self.poll_control();
        work += self.poll_sources();
        work += self.core.do_work(&mut Publications {
            sessions: &self.sessions,
            links: &self.source_links,
            subscriptions: &self.subscriptions,
        });
        work += self.execute_actions();
        work += self.poll_replayers();
        work += self.poll_recorders();
        self.flush_catalog();

        let cycle_ns = started.elapsed().as_nanos() as i64;
        propose_max(&self.counters.max_cycle_time, cycle_ns);
        if cycle_ns > CYCLE_THRESHOLD_NS {
            add(&self.counters.cycle_time_exceeded, 1);
        }
        self.idle(work);
        work
    }

    fn idle(&mut self, work: usize) {
        if work > 0 {
            self.idle_count = 0;
            return;
        }
        self.idle_count = self.idle_count.saturating_add(1);
        match self.ctx.idle {
            Idle::Spin => std::hint::spin_loop(),
            Idle::Backoff => match self.idle_count {
                0..=10 => std::hint::spin_loop(),
                11..=20 => std::thread::yield_now(),
                n => std::thread::sleep(Duration::from_micros((1u64 << (n - 20).min(10)).min(1_000))),
            },
        }
    }

    fn record_error(&mut self, message: &str) {
        log::warn!("{message}");
        add(&self.counters.errors, 1);
        if let Err(e) = self.mark.record_error(message, epoch_ms()) {
            log::warn!("failed to record an error in the mark file: {e}");
        }
    }

    /// Every millisecond: the client's conductor (adds complete, images come
    /// and go), the adds under way, the mark file, the gauges.
    fn slow_tick(&mut self, now_ms: i64) -> usize {
        let mut work = match self.aeron.main_do_work() {
            Ok(n) => n.max(0) as usize,
            Err(e) => {
                self.record_error(&format!("ERROR - conductor: {e}"));
                0
            }
        };
        work += self.poll_adds();
        if now_ms >= self.mark_deadline_ms {
            self.mark_deadline_ms = now_ms + UPDATE_INTERVAL_MS;
            if let Err(e) = self.mark.update_activity(now_ms) {
                self.record_error(&format!("ERROR - mark file: {e}"));
            }
        }
        store(
            &self.counters.control_sessions,
            self.core.control_session_count() as i64,
        );
        store(
            &self.counters.recording_sessions,
            self.core.recording_session_count() as i64,
        );
        store(&self.counters.replay_sessions, self.replayers.len() as i64);
        work
    }

    /// The control sessions' publications and counters being added.
    fn poll_adds(&mut self) -> usize {
        let mut done = Vec::new();
        for (&id, session) in self.sessions.iter_mut() {
            if let Some(adding) = &session.adding_publication {
                match adding.poll() {
                    Ok(Some(publication)) => {
                        session.publication = Some(publication);
                        session.adding_publication = None;
                        done.push(Io::ResponsePublication {
                            control_session_id: id,
                            result: Ok(()),
                        });
                    }
                    Ok(None) => {}
                    Err(e) => {
                        session.adding_publication = None;
                        done.push(Io::ResponsePublication {
                            control_session_id: id,
                            result: Err((false, e.to_string())),
                        });
                    }
                }
            }
            if let Some(adding) = &session.adding_counter {
                match adding.poll() {
                    Ok(Some(counter)) => {
                        store(&counter, id);
                        session.counter = Some(counter);
                        session.adding_counter = None;
                        done.push(Io::ControlSessionCounter {
                            control_session_id: id,
                            result: Ok(()),
                        });
                    }
                    Ok(None) => {}
                    Err(e) => {
                        session.adding_counter = None;
                        done.push(Io::ControlSessionCounter {
                            control_session_id: id,
                            result: Err(e.to_string()),
                        });
                    }
                }
            }
        }
        let mut ready = Vec::new();
        for (&token, adding) in &self.adding_replays {
            match adding.poll() {
                Ok(Some(publication)) => ready.push((token, Ok(publication))),
                Ok(None) => {}
                Err(e) => ready.push((token, Err(e.to_string()))),
            }
        }
        for (publication, result) in ready {
            self.adding_replays.remove(&publication);
            let result = match result {
                Ok(p) => {
                    let session_id = p.get_constants().map(|c| c.session_id()).unwrap_or(0);
                    self.replay_publications.insert(publication, p);
                    Ok(session_id)
                }
                Err(e) => Err((false, e)),
            };
            done.push(Io::ReplayPublication { publication, result });
        }
        let work = done.len();
        for io in done {
            self.core.on_io(io);
        }
        work + self.execute_actions()
    }

    /// Images the client's conductor saw come and go since last asked.
    fn drain_image_events(&mut self) -> usize {
        let events = std::mem::take(&mut *self.events.lock());
        let work = events.len();
        for event in events {
            match event {
                ImageEvent::Available { subscription_id, image } => {
                    if let Some(c) = self.control.iter_mut().find(|c| c.registration_id == subscription_id) {
                        c.images.insert(
                            image.session_id,
                            RequestImage {
                                correlation_id: image.correlation_id,
                                session_id: image.session_id,
                                source_identity: image.source_identity,
                            },
                        );
                    } else if let Some(&subscription) = self.tokens.get(&subscription_id) {
                        self.core.on_io(Io::RecordingImage { subscription, image });
                        self.execute_actions();
                    }
                }
                ImageEvent::Unavailable {
                    subscription_id,
                    session_id,
                } => {
                    if let Some(c) = self.control.iter_mut().find(|c| c.registration_id == subscription_id) {
                        c.reassemblers.remove(&session_id);
                        if let Some(image) = c.images.remove(&session_id) {
                            self.core.on_control_image_unavailable(&image, c.stream_id, &c.channel);
                        }
                    }
                }
            }
        }
        work
    }

    /// The control requests, each served as it comes.
    fn poll_control(&mut self) -> usize {
        let mut work = 0;
        for i in 0..self.control.len() {
            let mut messages: Vec<(i32, Vec<u8>)> = Vec::new();
            let c = &mut self.control[i];
            let reassemblers = &mut c.reassemblers;
            let polled = c.subscription.poll_fn(
                |buffer, header| {
                    let Ok(values) = header.get_values() else { return };
                    let frame = values.frame();
                    let session_id = frame.session_id();
                    let fragment = Fragment {
                        payload: buffer,
                        flags: frame.flags(),
                        term_offset: frame.term_offset(),
                        position: header.position(),
                    };
                    reassemblers
                        .entry(session_id)
                        .or_default()
                        .on_fragment(fragment, |message, _| {
                            messages.push((session_id, message.to_vec()));
                            PollAction::Continue
                        });
                },
                CONTROL_FRAGMENT_LIMIT,
            );
            work += polled.map(|n| n.max(0) as usize).unwrap_or(0);
            if messages.is_empty() {
                continue;
            }
            if let Some(space) = usable_space(&self.archive_dir) {
                self.core.set_usable_space(space);
            }
            for (session_id, message) in messages {
                let Some(image) = self.request_image(i, session_id) else {
                    continue;
                };
                self.core
                    .on_control_message(&image, &message, &ReaderCounters(&self.aeron.counters_reader()));
                self.execute_actions();
            }
        }
        work
    }

    /// The image a control request came on, by its session.
    fn request_image(&mut self, control: usize, session_id: i32) -> Option<RequestImage> {
        let c = &mut self.control[control];
        if let Some(image) = c.images.get(&session_id) {
            return Some(image.clone());
        }
        let info = image_info(&c.subscription.image_by_session_id(session_id)?)?;
        let image = RequestImage {
            correlation_id: info.correlation_id,
            session_id,
            source_identity: info.source_identity,
        };
        c.images.insert(session_id, image.clone());
        Some(image)
    }

    fn execute_actions(&mut self) -> usize {
        let mut work = 0;
        while let Some(action) = self.core.poll_action() {
            work += 1;
            self.execute(action);
        }
        work
    }

    fn execute(&mut self, action: Action) {
        log::debug!("{action:?}");
        match action {
            Action::AddResponsePublication {
                control_session_id,
                channel,
                stream_id,
            } => match self
                .aeron
                .async_add_exclusive_publication(&channel.as_str().into_c_string(), stream_id)
            {
                Ok(adding) => {
                    self.sessions.entry(control_session_id).or_default().adding_publication = Some(adding);
                }
                Err(e) => self.core.on_io(Io::ResponsePublication {
                    control_session_id,
                    result: Err((false, e.to_string())),
                }),
            },
            Action::AddControlSessionCounter {
                control_session_id,
                key,
                label,
            } => match self.aeron.async_add_counter(
                archive_counters::ARCHIVE_CONTROL_SESSION_TYPE_ID,
                &key,
                truncate_label(&label),
            ) {
                Ok(adding) => {
                    self.sessions.entry(control_session_id).or_default().adding_counter = Some(adding);
                }
                Err(e) => self.core.on_io(Io::ControlSessionCounter {
                    control_session_id,
                    result: Err(e.to_string()),
                }),
            },
            Action::CloseControlSession { control_session_id } => {
                if let Some(session) = self.sessions.remove(&control_session_id) {
                    if let Some(publication) = session.publication {
                        publication.revoke_on_close();
                        let _ = publication.close();
                    }
                    if let Some(counter) = session.counter {
                        let _ = counter.close();
                    }
                }
            }
            Action::RejectImage {
                image_correlation_id,
                reason,
            } => self.reject_image(image_correlation_id, &reason),
            Action::AddRecordingSubscription {
                subscription,
                channel,
                stream_id,
            } => {
                let result = self
                    .aeron
                    .add_subscription(
                        &channel.as_str().into_c_string(),
                        stream_id,
                        Some(&self.image_handler),
                        Handlers::NONE,
                        ADD_TIMEOUT,
                    )
                    .map(|s| {
                        let registration_id = registration_id(&s);
                        self.subscriptions.insert(subscription, s);
                        self.tokens.insert(registration_id, subscription);
                        registration_id
                    })
                    .map_err(|e| e.to_string());
                self.core.on_io(Io::RecordingSubscription { subscription, result });
            }
            Action::CloseRecordingSubscription { subscription } => {
                let left: Vec<i64> = self
                    .recorders
                    .values()
                    .filter(|r| r.subscription == subscription)
                    .map(|r| r.recording_id)
                    .collect();
                for recording_id in left {
                    self.stop_recorder(recording_id, None);
                }
                if let Some(s) = self.subscriptions.remove(&subscription) {
                    self.tokens.remove(&registration_id(&s));
                    let _ = s.close();
                }
            }
            Action::StartRecorder {
                recording_id,
                subscription,
                image_correlation_id,
                writer,
                block_length_limit,
                counter_key,
                counter_label,
            } => {
                if let Err(error) = self.start_recorder(
                    recording_id,
                    subscription,
                    image_correlation_id,
                    writer,
                    block_length_limit,
                    &counter_key,
                    &counter_label,
                ) {
                    self.core.on_io(Io::RecorderStopped {
                        recording_id,
                        position: writer.position(),
                        error,
                    });
                }
            }
            Action::AbortRecorder { recording_id } => self.stop_recorder(recording_id, None),
            Action::AddReplayPublication {
                publication,
                channel,
                stream_id,
            } => match self
                .aeron
                .async_add_exclusive_publication(&channel.as_str().into_c_string(), stream_id)
            {
                Ok(adding) => {
                    self.adding_replays.insert(publication, adding);
                }
                Err(e) => self.core.on_io(Io::ReplayPublication {
                    publication,
                    result: Err((false, e.to_string())),
                }),
            },
            Action::StartReplay {
                replay_session_id,
                publication,
                session,
                limit,
            } => self.start_replayer(replay_session_id, publication, *session, limit),
            Action::AbortReplay {
                replay_session_id,
                revoke,
            } => {
                if let Some(replayer) = self.replayers.get_mut(&replay_session_id) {
                    if revoke {
                        replayer.abort();
                    } else {
                        replayer.stop();
                    }
                }
            }
            Action::ListSegmentFiles { recording_id } => {
                let prefix = format!("{recording_id}-");
                let file_names = std::fs::read_dir(&self.archive_dir)
                    .map(|entries| {
                        entries
                            .filter_map(|e| e.ok()?.file_name().into_string().ok())
                            .filter(|name| name.starts_with(&prefix))
                            .collect()
                    })
                    .unwrap_or_default();
                self.core.on_io(Io::SegmentFiles {
                    recording_id,
                    file_names,
                });
            }
            Action::EraseSegment {
                recording_id,
                file_name,
                segment_offset,
                segment_length,
                expected,
            } => {
                let path = self.archive_dir.join(file_name);
                let result = erase_segment(&path, segment_offset, segment_length, expected);
                self.core.on_io(Io::SegmentErased { recording_id, result });
            }
            Action::ConnectSourceArchive {
                replication_id,
                request_channel,
                request_stream_id,
                response_channel,
                response_stream_id,
            } => {
                let session_id = self.aeron.next_correlation_id() as i32;
                let added = conclude_control_channel(&request_channel, session_id)
                    .and_then(|request| {
                        let response = conclude_control_channel(&response_channel, session_id)?;
                        Ok((request, response))
                    })
                    .and_then(|(request, response)| {
                        let adding_subscription = self
                            .aeron
                            .async_add_subscription(
                                &response.as_str().into_c_string(),
                                response_stream_id,
                                Handlers::NONE,
                                Handlers::NONE,
                            )
                            .map_err(|e| e.to_string())?;
                        let adding_publication = self
                            .aeron
                            .async_add_exclusive_publication(&request.as_str().into_c_string(), request_stream_id)
                            .map_err(|e| e.to_string())?;
                        Ok(SourceLink {
                            adding_publication: Some(adding_publication),
                            adding_subscription: Some(adding_subscription),
                            ..SourceLink::default()
                        })
                    });
                match added {
                    Ok(link) => {
                        self.source_links.insert(replication_id, link);
                    }
                    Err(message) => self.core.on_io(Io::SourceLinked {
                        replication_id,
                        result: Err(message),
                    }),
                }
            }
            Action::CloseSourceArchive { replication_id } => {
                if let Some(link) = self.source_links.remove(&replication_id) {
                    if let Some(p) = link.publication {
                        let _ = p.close();
                    }
                    if let Some(s) = link.subscription {
                        let _ = s.close();
                    }
                }
            }
            Action::DeleteSegmentFile {
                recording_id,
                file_name,
            } => {
                let result = delete_segment_file(&self.archive_dir, &file_name);
                self.core.on_io(Io::SegmentFileDeleted { recording_id, result });
            }
            Action::Error { message } => self.record_error(&message),
        }
    }

    fn reject_image(&mut self, correlation_id: i64, reason: &str) {
        for c in &self.control {
            let count = c.subscription.image_count().unwrap_or(0).max(0) as usize;
            for i in 0..count {
                if let Some(image) = c.subscription.image_at_index(i)
                    && image
                        .get_constants()
                        .is_ok_and(|k| k.correlation_id() == correlation_id)
                {
                    let _ = image.reject(&reason.into_c_string());
                    return;
                }
            }
        }
    }

    /// The image of a recording subscription, retained, by its correlation id.
    fn recording_image(&self, subscription: SubscriptionToken, correlation_id: i64) -> Option<AeronImage> {
        let s = self.subscriptions.get(&subscription)?;
        let count = s.image_count().unwrap_or(0).max(0) as usize;
        (0..count).filter_map(|i| s.image_at_index(i)).find(|image| {
            image
                .get_constants()
                .is_ok_and(|k| k.correlation_id() == correlation_id)
        })
    }

    /// `startRecordingSession`'s I/O: the position counter, the image, the
    /// segment file. `Err(None)`: the image is already gone.
    #[allow(clippy::too_many_arguments)]
    fn start_recorder(
        &mut self,
        recording_id: i64,
        subscription: SubscriptionToken,
        image_correlation_id: i64,
        writer: rusteron_archive_core::segments::SegmentWriter,
        block_length_limit: usize,
        key: &[u8],
        label: &str,
    ) -> Result<(), Option<RecordingError>> {
        let counter = self
            .aeron
            .add_counter(
                archive_counters::RECORDING_POSITION_TYPE_ID,
                key,
                truncate_label(label),
                ADD_TIMEOUT,
            )
            .map_err(|e| {
                Some(RecordingError {
                    code: error_code::GENERIC,
                    message: format!("ERROR - {e}"),
                })
            })?;
        store(&counter, writer.position());
        let Some(image) = self.recording_image(subscription, image_correlation_id) else {
            let _ = counter.close();
            return Err(None);
        };
        let recorder = Recorder::start(
            recording_id,
            subscription,
            image,
            counter,
            writer,
            &self.archive_dir,
            self.sync,
            block_length_limit,
        )
        .map_err(Some)?;
        self.recorders.insert(recording_id, recorder);
        Ok(())
    }

    /// Stops a recorder, writing nothing more, and reports where it stopped.
    fn stop_recorder(&mut self, recording_id: i64, error: Option<RecordingError>) {
        if let Some(recorder) = self.recorders.remove(&recording_id) {
            let position = recorder.position();
            drop(recorder);
            self.core.on_io(Io::RecorderStopped {
                recording_id,
                position,
                error,
            });
        }
    }

    /// A replay's publication is there: the replayer starts, following the
    /// counter its limit names.
    fn start_replayer(
        &mut self,
        replay_session_id: i64,
        publication: SubscriptionToken,
        session: ReplaySession,
        limit: ReplayLimit,
    ) {
        let Some(p) = self.replay_publications.remove(&publication) else {
            log::debug!("replay {replay_session_id}: no publication {publication}");
            return self.core.on_io(Io::ReplayEnded {
                replay_session_id,
                error: None,
            });
        };
        let limit = match limit {
            ReplayLimit::None => LimitCounter::None,
            ReplayLimit::Counter(id) => LimitCounter::Counter {
                id,
                registration_id: self.aeron.counters_reader().counter_registration_id(id).unwrap_or(-1),
            },
            ReplayLimit::Recording(recording_id) => {
                match self.recorders.get(&recording_id).and_then(|r| r.counter_constants()) {
                    Some((id, registration_id)) => LimitCounter::Counter { id, registration_id },
                    None => LimitCounter::Ended(self.core.catalog().stop_position(recording_id).unwrap_or(0)),
                }
            }
        };
        match Replayer::new(p, session, limit, &self.archive_dir) {
            Ok(replayer) => {
                self.replayers.insert(replay_session_id, replayer);
            }
            Err(message) => self.core.on_io(Io::ReplayEnded {
                replay_session_id,
                error: Some(ReplayError {
                    code: error_code::GENERIC,
                    message,
                }),
            }),
        }
    }

    /// The replications' links: added, their response channel resolved; and
    /// what the sources sent.
    fn poll_sources(&mut self) -> usize {
        if self.source_links.is_empty() {
            return 0;
        }
        let mut work = 0;
        let mut linked = Vec::new();
        let mut messages: Vec<(i64, Vec<u8>)> = Vec::new();
        for (&id, link) in self.source_links.iter_mut() {
            if let Some(adding) = &link.adding_publication {
                match adding.poll() {
                    Ok(Some(p)) => {
                        link.publication = Some(p);
                        link.adding_publication = None;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        link.adding_publication = None;
                        linked.push((id, Err(e.to_string())));
                    }
                }
            }
            if let Some(adding) = &link.adding_subscription {
                match adding.poll() {
                    Ok(Some(s)) => {
                        link.subscription = Some(s);
                        link.adding_subscription = None;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        link.adding_subscription = None;
                        linked.push((id, Err(e.to_string())));
                    }
                }
            }
            let Some(subscription) = &link.subscription else {
                continue;
            };
            if !link.linked && link.publication.is_some() {
                // The source answers on the port the subscription was given.
                if let Ok(channel) = subscription.try_resolve_channel_endpoint_port_as_string(4_096)
                    && !channel.is_empty()
                    && !channel.contains(":0|")
                    && !channel.ends_with(":0")
                {
                    link.linked = true;
                    linked.push((id, Ok(channel)));
                }
            }
            let reassembler = &mut link.reassembler;
            let polled = subscription.poll_fn(
                |buffer, header| {
                    let Ok(values) = header.get_values() else { return };
                    let frame = values.frame();
                    let fragment = Fragment {
                        payload: buffer,
                        flags: frame.flags(),
                        term_offset: frame.term_offset(),
                        position: header.position(),
                    };
                    reassembler.on_fragment(fragment, |message, _| {
                        messages.push((id, message.to_vec()));
                        PollAction::Continue
                    });
                },
                CONTROL_FRAGMENT_LIMIT,
            );
            work += polled.map(|n| n.max(0) as usize).unwrap_or(0);
        }
        for (replication_id, result) in linked {
            self.core.on_io(Io::SourceLinked { replication_id, result });
        }
        for (replication_id, message) in messages {
            self.core.on_io(Io::SourceMessage {
                replication_id,
                message,
            });
        }
        work + self.execute_actions()
    }

    /// Each replay's cycle: at most one read and one block.
    fn poll_replayers(&mut self) -> usize {
        if self.replayers.is_empty() {
            return 0;
        }
        let mut work = 0;
        let mut stats = ReadStats::default();
        let mut started = Vec::new();
        let mut ended = Vec::new();
        let counters = self.aeron.counters_reader();
        for (&id, replayer) in self.replayers.iter_mut() {
            match replayer.poll(self.cached_ms, &counters, &mut self.replay_buffer, &mut stats) {
                Replayed::Idle => {}
                Replayed::Worked => work += 1,
                Replayed::Started => started.push(id),
                Replayed::Ended {
                    error,
                    revoke,
                    started: first,
                } => {
                    if first {
                        started.push(id);
                    }
                    ended.push((id, error, revoke));
                }
            }
        }
        if stats.bytes > 0 {
            add(&self.counters.total_read_bytes, stats.bytes);
            add(&self.counters.total_read_time, stats.time_ns);
            propose_max(&self.counters.max_read_time, stats.max_time_ns);
        }
        work += started.len() + ended.len();
        for replay_session_id in started {
            log::debug!("replay {replay_session_id}: started");
            self.core.on_io(Io::ReplayStarted { replay_session_id });
        }
        for (replay_session_id, error, revoke) in ended {
            log::debug!("replay {replay_session_id}: ended {error:?}, revoked {revoke}");
            if let Some(replayer) = self.replayers.remove(&replay_session_id) {
                replayer.close(revoke);
            }
            self.core.on_io(Io::ReplayEnded {
                replay_session_id,
                error,
            });
        }
        work + self.execute_actions()
    }

    fn poll_recorders(&mut self) -> usize {
        let mut work = 0;
        let mut stats = WriteStats::default();
        let mut ended = Vec::new();
        for (&id, recorder) in self.recorders.iter_mut() {
            match recorder.poll(&mut stats) {
                Polled::Recorded(position) => {
                    work += 1;
                    self.core.on_recorded(id, position);
                }
                Polled::Idle => {}
                Polled::Ended(error) => ended.push((id, error)),
            }
        }
        if stats.bytes > 0 {
            add(&self.counters.total_write_bytes, stats.bytes);
            add(&self.counters.total_write_time, stats.time_ns);
            propose_max(&self.counters.max_write_time, stats.max_time_ns);
        }
        work += ended.len();
        for (id, error) in ended {
            self.stop_recorder(id, error);
        }
        work + self.execute_actions()
    }

    fn flush_catalog(&mut self) {
        let writes = self.core.take_catalog_writes();
        if writes.is_empty() {
            return;
        }
        let mut result = writes
            .iter()
            .try_for_each(|(offset, bytes)| write_at(&self.catalog_file, bytes, *offset));
        if result.is_ok() {
            result = match Sync::from_level(self.ctx.catalog_file_sync_level) {
                Sync::None => Ok(()),
                Sync::Data => self.catalog_file.sync_data(),
                Sync::All => self.catalog_file.sync_all(),
            };
        }
        if let Err(e) = result {
            self.record_error(&format!("ERROR - catalog: {e}"));
        }
    }

    /// Stops the recordings where they are, writes the catalog, and leaves
    /// the mark file for the next archive.
    pub fn close(mut self) {
        for (_, replayer) in std::mem::take(&mut self.replayers) {
            replayer.close(true);
        }
        let ids: Vec<i64> = self.recorders.keys().copied().collect();
        for id in ids {
            self.stop_recorder(id, None);
        }
        self.execute_actions();
        self.flush_catalog();
        let _ = self.catalog_file.sync_all();
        let _ = self.mark.signal_terminated();
    }
}

/// Aeron's labels are 380 bytes at most.
fn truncate_label(label: &str) -> &str {
    let mut end = label.len().min(archive_counters::MAX_LABEL_LENGTH);
    while !label.is_char_boundary(end) {
        end -= 1;
    }
    &label[..end]
}

#[cfg(unix)]
fn read_exact_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, bytes, offset)
}

/// Java's `eraseRemainingSegment`: the frame at the truncation's position
/// checked, the segment cut there, its length kept (the rest a hole).
fn erase_segment(path: &Path, offset: u64, segment_length: i32, expected: ExpectedFrame) -> Result<(), EraseError> {
    let io = |e: io::Error| match e.kind() {
        io::ErrorKind::NotFound => EraseError::Io(format!("{} (No such file or directory)", path.display())),
        io::ErrorKind::UnexpectedEof => EraseError::Io("failed to read fragment header".into()),
        _ => EraseError::Io(e.to_string()),
    };
    let file = OpenOptions::new().read(true).write(true).open(path).map_err(io)?;
    let mut header = [0u8; 32];
    read_exact_at(&file, &mut header, offset).map_err(io)?;
    let i32_at = |at: usize| i32::from_le_bytes(header[at..at + 4].try_into().expect("4 bytes"));
    if i32_at(8) != expected.term_offset || i32_at(20) != expected.term_id || i32_at(16) != expected.stream_id {
        return Err(EraseError::NotAligned);
    }
    file.set_len(offset).map_err(io)?;
    write_at(&file, &[0], segment_length as u64 - 1).map_err(io)?;
    Ok(())
}

/// `DeleteSegmentsSession`'s delete: the file, or its `.del` if it is gone.
fn delete_segment_file(dir: &Path, name: &str) -> Result<(), String> {
    let path = dir.join(name);
    if std::fs::remove_file(&path).is_ok() {
        return Ok(());
    }
    if path.exists() {
        return Err(format!("unable to delete segment file: {}", path.display()));
    }
    if !name.ends_with(".del") {
        let del = dir.join(format!("{name}.del"));
        if std::fs::remove_file(&del).is_err() && del.exists() {
            return Err(format!("unable to delete segment file: {}", del.display()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusteron_archive_core::catalog::Descriptor;

    /// Aeron 1.54.2's recovery fixture whose last fragment a crash cut short
    /// across a page (`rusteron-archive-core/java/.../CatalogFixtures.java`).
    fn fixture(file: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../rusteron-archive-core/tests/fixtures/recovery")
            .join(file)
    }

    /// An archive directory whose one recording was left active, its segment
    /// the fixture's.
    fn torn_archive(name: &str) -> (PathBuf, i64) {
        let dir = std::env::temp_dir().join(format!("rusteron-torn-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut catalog = Catalog::create(4_096).unwrap();
        let id = catalog
            .add_recording(&Descriptor {
                control_session_id: 0,
                correlation_id: 0,
                recording_id: -1,
                start_timestamp: 1,
                stop_timestamp: NULL_POSITION,
                start_position: 0,
                stop_position: NULL_POSITION,
                initial_term_id: 0,
                segment_file_length: 64 * 1024,
                term_buffer_length: 64 * 1024,
                mtu_length: 1_408,
                session_id: 1,
                stream_id: 100,
                stripped_channel: "aeron:ipc".into(),
                original_channel: "aeron:ipc".into(),
                source_identity: "aeron:ipc".into(),
            })
            .unwrap();
        let file = File::create(dir.join(CATALOG_FILE_NAME)).unwrap();
        file.set_len(catalog.len() as u64).unwrap();
        for (offset, bytes) in catalog.take_writes() {
            write_at(&file, &bytes, offset).unwrap();
        }
        std::fs::copy(
            fixture("straddle-half-written/42-0.rec"),
            dir.join(segment_file_name(id, 0)),
        )
        .unwrap();
        (dir, id)
    }

    #[test]
    fn a_torn_fragment_stops_the_start_unless_truncated_as_java_truncates_it() {
        let (dir, id) = torn_archive("refused");
        let refused = open_catalog(&dir, 4_096, false, false, 2).map(|_| ()).unwrap_err();
        assert!(refused.to_string().contains("straddling page boundary"), "{refused}");
        std::fs::remove_dir_all(&dir).unwrap();

        let (dir, id_truncated) = torn_archive("truncated");
        assert_eq!(id, id_truncated);
        let (catalog, _) = open_catalog(&dir, 4_096, true, false, 2).unwrap();
        let java = std::fs::read_to_string(fixture("cases.txt")).unwrap();
        let java_stop = java
            .lines()
            .find_map(|l| l.strip_prefix("straddle-half-written\t"))
            .and_then(|l| l.split('\t').nth(6))
            .unwrap();
        assert_eq!(catalog.descriptor(id).unwrap().stop_position.to_string(), java_stop);
        assert_eq!(
            std::fs::read(dir.join(segment_file_name(id, 0))).unwrap(),
            std::fs::read(fixture("straddle-half-written/truncated/42-0.rec")).unwrap()
        );
        // Opened again, the recording is stopped: nothing left to recover.
        let (again, _) = open_catalog(&dir, 4_096, false, false, 3).unwrap();
        assert_eq!(again.descriptor(id).unwrap().stop_position.to_string(), java_stop);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The archive started with `verify` on Java's `verify/before/`
    /// (`CatalogFixtures.java`): every recording's state, stop position and
    /// stop timestamp as Aeron's `ArchiveTool verify` left them in
    /// `verify/after/`, the torn segment truncated as the tool truncated it,
    /// and so written to the catalog's file. But for the recording whose last
    /// segment file is empty, which the archive's recovery reads as holding
    /// no frame: valid, stopped where the file starts, where the tool fails
    /// to read a header and marks it invalid.
    #[test]
    fn verifying_at_start_leaves_the_catalog_as_archive_tool_verify_does() {
        let java = Path::new(env!("CARGO_MANIFEST_DIR")).join("../rusteron-archive-core/tests/fixtures/verify");
        let dir = std::env::temp_dir().join(format!("rusteron-verify-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for entry in std::fs::read_dir(java.join("before")).unwrap() {
            let entry = entry.unwrap();
            std::fs::copy(entry.path(), dir.join(entry.file_name())).unwrap();
        }
        let now = 1_700_000_009_999;
        let (catalog, _) = open_catalog(&dir, 4_096, true, true, now).unwrap();
        let reopened = Catalog::open(std::fs::read(dir.join(CATALOG_FILE_NAME)).unwrap(), 4_096).unwrap();
        assert_eq!(reopened.entries(), catalog.entries(), "written to the file");
        let entries: BTreeMap<i64, (i32, Descriptor)> = catalog
            .entries()
            .into_iter()
            .map(|(state, d)| (d.recording_id, (state, d)))
            .collect();
        let cases = std::fs::read_to_string(java.join("cases.txt")).unwrap();
        for line in cases.lines() {
            let f: Vec<&str> = line.split('\t').collect();
            let (id, name) = (f[0].parse::<i64>().unwrap(), f[1]);
            let (expected_state, expected_stop, expected_timestamp) = if name == "empty-last-segment" {
                ("VALID", "65536".to_string(), now.to_string())
            } else {
                (f[2], f[3].to_string(), f[4].to_string())
            };
            let (state, d) = &entries[&id];
            let state = ["INVALID", "VALID", "DELETED"][*state as usize];
            assert_eq!(state, expected_state, "{name}");
            assert_eq!(d.stop_position.to_string(), expected_stop, "{name}");
            assert_eq!(d.stop_timestamp.to_string(), expected_timestamp, "{name}");
            assert_eq!(catalog.has_recording(id), state == "VALID", "{name}: listed if valid");
        }
        let torn = cases.lines().find(|l| l.contains("\ttorn-active\t")).unwrap();
        let file = segment_file_name(torn.split('\t').next().unwrap().parse().unwrap(), 0);
        assert_eq!(
            std::fs::read(dir.join(&file)).unwrap(),
            std::fs::read(java.join("after").join(&file)).unwrap(),
            "truncated as the tool truncated it"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
