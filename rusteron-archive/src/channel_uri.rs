//! Channel URIs as Aeron 1.54.2's Java client parses and rebuilds them
//! (`ChannelUri`, `ChannelUriStringBuilder`), for what the archive does with
//! a channel a client names:
//!
//! - the **stripped channel** a recording subscribes to and the catalog
//!   keeps, its parameters in the builder's order, the ones a recording has
//!   no use for dropped (`ArchiveConductor.strippedChannelBuilder`);
//! - the **key** that makes a second recording of the same stream a
//!   duplicate (`ArchiveConductor.makeKey`);
//! - a control session's **response channel**, the stripped channel with
//!   the control stream's MTU, term length and sparseness.
//!
//! Errors carry Java's messages: the archive sends them to the client.

#![forbid(unsafe_code)]
// The crate allows these lints for its generated bindings; this module is checked.
#![warn(
    clippy::all,
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    unsafe_op_in_unsafe_fn,
    unused_unsafe,
    unused_variables
)]

const SPY_PREFIX: &str = "aeron-spy:";
const SPY_QUALIFIER: &str = "aeron-spy";
const AERON_PREFIX: &str = "aeron:";
const TAG_PREFIX: &str = "tag:";
const RESERVED_OFFSET: &str = "reserved";
const PROTOTYPE_CORRELATION_ID: &str = "prototype";

/// The longest URI Java parses.
pub const MAX_URI_LENGTH: usize = 4_095;
pub const UDP_MEDIA: &str = "udp";
pub const IPC_MEDIA: &str = "ipc";

const KILO: i64 = 1_024;
const MEGA: i64 = 1_024 * 1_024;
const GIGA: i64 = 1_024 * 1_024 * 1_024;
const TERM_MIN_LENGTH: i64 = 64 * 1_024;
const TERM_MAX_LENGTH: i64 = 1_024 * 1_024 * 1_024;
const FRAME_ALIGNMENT: i64 = 32;

/// A parsed channel: its prefix (`aeron-spy` or none), its media, its
/// parameters, a repeated key's last value kept.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelUri {
    pub prefix: String,
    pub media: String,
    params: Vec<(String, String)>,
}

#[derive(Clone, Copy)]
enum State {
    Media,
    Key,
    Value,
}

impl ChannelUri {
    /// Parses `uri` as `ChannelUri.parse`.
    pub fn parse(uri: &str) -> Result<Self, String> {
        let length = uri.chars().count();
        if length > MAX_URI_LENGTH {
            let head: String = uri.chars().take(MAX_URI_LENGTH).collect();
            return Err(format!(
                "URI length ({length}) exceeds max supported length ({MAX_URI_LENGTH}): {head}"
            ));
        }
        let (prefix, rest) = match uri.strip_prefix(SPY_PREFIX) {
            Some(rest) => (SPY_QUALIFIER, rest),
            None => ("", uri),
        };
        let Some(rest) = rest.strip_prefix(AERON_PREFIX) else {
            return Err(format!("Aeron URIs must start with 'aeron:', found: {uri}"));
        };
        let start = uri.len() - rest.len();
        let mut params: Vec<(String, String)> = Vec::new();
        let mut put = |key: String, value: String| match params.iter_mut().find(|(k, _)| *k == key) {
            Some(entry) => entry.1 = value,
            None => params.push((key, value)),
        };
        let mut media = String::new();
        let mut key = String::new();
        let mut builder = String::new();
        let mut state = State::Media;
        for (i, c) in uri[start..].chars().enumerate() {
            let i = i + uri[..start].chars().count();
            match state {
                State::Media => match c {
                    '?' => {
                        media = std::mem::take(&mut builder);
                        state = State::Key;
                    }
                    ':' | '|' | '=' => {
                        return Err(format!(
                            "encountered '{c}' within media definition at index {i} in {uri}"
                        ));
                    }
                    _ => builder.push(c),
                },
                State::Key => match c {
                    '=' if builder.is_empty() => {
                        return Err(format!("empty key not allowed at index {i} in {uri}"));
                    }
                    '=' => {
                        key = std::mem::take(&mut builder);
                        state = State::Value;
                    }
                    '|' => return Err(format!("invalid end of key at index {i} in {uri}")),
                    _ => builder.push(c),
                },
                State::Value => match c {
                    '|' => {
                        put(std::mem::take(&mut key), std::mem::take(&mut builder));
                        state = State::Key;
                    }
                    _ => builder.push(c),
                },
            }
        }
        match state {
            State::Media => {
                if builder != UDP_MEDIA && builder != IPC_MEDIA {
                    return Err(format!("unknown media: {builder}"));
                }
                media = builder;
            }
            State::Value => put(key, builder),
            State::Key => return Err(format!("no more input found, state=PARAMS_KEY in {uri}")),
        }
        Ok(ChannelUri {
            prefix: prefix.to_owned(),
            media,
            params,
        })
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.params.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    pub fn is_udp(&self) -> bool {
        self.media == UDP_MEDIA
    }

    /// Sets a parameter, replacing its value if it has one.
    pub fn put(&mut self, key: &str, value: &str) {
        match self.params.iter_mut().find(|(k, _)| k == key) {
            Some(entry) => entry.1 = value.to_owned(),
            None => self.params.push((key.to_owned(), value.to_owned())),
        }
    }

    /// `replaceEndpointWildcardPort`: an endpoint's port 0 becomes the port
    /// the subscription was given, from its `resolved` endpoint.
    pub fn replace_endpoint_wildcard_port(&mut self, resolved: &str) -> Result<(), String> {
        let Some(endpoint) = self.get("endpoint").map(str::to_owned) else {
            return Ok(());
        };
        let Some((_, port)) = resolved.rsplit_once(':') else {
            return Err(format!("No port specified on resolvedEndpoint={resolved}"));
        };
        if port == "0" {
            return Err(format!("Wildcard port specified on resolvedEndpoint={resolved}"));
        }
        if let Some(host) = endpoint.strip_suffix(":0") {
            self.put("endpoint", &format!("{host}:{port}"));
        }
        Ok(())
    }
}

impl std::fmt::Display for ChannelUri {
    /// The URI again, its parameters in the order they came.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.prefix.is_empty() {
            write!(f, "{}:", self.prefix)?;
        }
        write!(f, "aeron:{}", self.media)?;
        for (i, (k, v)) in self.params.iter().enumerate() {
            write!(f, "{}{k}={v}", if i == 0 { '?' } else { '|' })?;
        }
        Ok(())
    }
}

/// `Long.parseLong`, its message on failure.
fn parse_long(s: &str) -> Result<i64, String> {
    let digits = s.strip_prefix(['-', '+']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("For input string: \"{s}\""));
    }
    s.parse::<i64>().map_err(|_| format!("For input string: \"{s}\""))
}

fn parse_int(s: &str) -> Result<i32, String> {
    let digits = s.strip_prefix(['-', '+']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("For input string: \"{s}\""));
    }
    s.parse::<i32>()
        .map_err(|_| format!("For input string: \"{s}\" under radix 10"))
}

/// Agrona's `SystemUtil.parseSize`: a number of bytes, with an optional
/// `k`, `m` or `g` suffix (binary).
pub fn parse_size(name: &str, value: &str) -> Result<i64, String> {
    let Some(last) = value.chars().last() else {
        return Err(format!("{name}: empty value"));
    };
    if last.is_ascii_digit() {
        return parse_long(value);
    }
    let number = &value[..value.len() - last.len_utf8()];
    let n: i64 = number.parse().map_err(|_| format!("error parsing int: {number}"))?;
    let multiplier = match last {
        'k' | 'K' => KILO,
        'm' | 'M' => MEGA,
        'g' | 'G' => GIGA,
        _ => return Err(format!("{name}: {value} should end with: k, m, or g.")),
    };
    n.checked_mul(multiplier)
        .ok_or_else(|| format!("{name} would overflow a long: {value}"))
}

/// Agrona's `SystemUtil.formatSize`: the largest binary suffix that divides
/// it exactly.
pub fn format_size(size: i64) -> String {
    for (multiplier, suffix) in [(GIGA, 'g'), (MEGA, 'm'), (KILO, 'k')] {
        if size >= multiplier && size % multiplier == 0 {
            return format!("{}{suffix}", size / multiplier);
        }
    }
    size.to_string()
}

/// Agrona's `SystemUtil.formatDuration`: the largest unit that divides it
/// exactly.
pub fn format_duration(ns: i64) -> String {
    for (unit, suffix) in [(1_000_000_000, "s"), (1_000_000, "ms"), (1_000, "us")] {
        if ns >= unit && ns % unit == 0 {
            return format!("{}{suffix}", ns / unit);
        }
    }
    format!("{ns}ns")
}

/// `Boolean.valueOf`: true for "true" in any case, else false.
fn boolean(value: &str) -> String {
    value.eq_ignore_ascii_case("true").to_string()
}

/// A channel's parameters as `ChannelUriStringBuilder` holds them, validated
/// and normalised as its setters do; [`ChannelBuilder::build`] writes them
/// in its order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChannelBuilder {
    pub media: String,
    tags: Option<String>,
    endpoint: Option<String>,
    interface: Option<String>,
    control: Option<String>,
    control_mode: Option<String>,
    mtu: Option<i64>,
    term_length: Option<i64>,
    initial_term_id: Option<i32>,
    term_id: Option<i32>,
    term_offset: Option<i32>,
    session_id: Option<String>,
    ttl: Option<String>,
    linger_ns: Option<i64>,
    alias: Option<String>,
    cc: Option<String>,
    fc: Option<String>,
    group_tag: Option<String>,
    sparse: Option<bool>,
    eos: Option<String>,
    tether: Option<String>,
    group: Option<String>,
    rejoin: Option<String>,
    so_sndbuf: Option<i64>,
    so_rcvbuf: Option<i64>,
    rcv_wnd: Option<i64>,
    media_rcv_ts_offset: Option<String>,
    channel_rcv_ts_offset: Option<String>,
    channel_snd_ts_offset: Option<String>,
    response_endpoint: Option<String>,
    response_correlation_id: Option<String>,
    stream_id: Option<String>,
}

fn size_param(uri: &ChannelUri, name: &str) -> Result<Option<i64>, String> {
    let Some(value) = uri.get(name) else { return Ok(None) };
    let size = parse_size(name, value)?;
    if size > i64::from(i32::MAX) {
        return Err(format!("value exceeds maximum permitted: value={size}"));
    }
    if size < 0 {
        return Err(format!("`{name}` value cannot be negative: {size}"));
    }
    Ok(Some(size))
}

fn timestamp_offset(uri: &ChannelUri, key: &str, what: &str) -> Result<Option<String>, String> {
    let Some(value) = uri.get(key) else { return Ok(None) };
    if value != RESERVED_OFFSET && parse_int(value).is_err() {
        return Err(format!(
            "{what} must be a number or the value '{RESERVED_OFFSET}' found: {value}"
        ));
    }
    Ok(Some(value.to_owned()))
}

impl ChannelBuilder {
    /// `ArchiveConductor.strippedChannelBuilder(uri)`: the setters in its
    /// order, the first that refuses a value failing the whole.
    pub fn stripped(uri: &ChannelUri) -> Result<Self, String> {
        let raw = |key: &str| uri.get(key).map(str::to_owned);
        if uri.media != UDP_MEDIA && uri.media != IPC_MEDIA {
            return Err(format!("invalid media: {}", uri.media));
        }
        let mut b = ChannelBuilder {
            media: uri.media.clone(),
            endpoint: raw("endpoint"),
            interface: raw("interface"),
            control: raw("control"),
            ..Default::default()
        };
        if let Some(mode) = uri.get("control-mode") {
            if !matches!(mode, "manual" | "dynamic" | "response") {
                return Err(format!("invalid control mode: {mode}"));
            }
            b.control_mode = Some(mode.to_owned());
        }
        b.tags = raw("tags");
        b.rejoin = uri.get("rejoin").map(boolean);
        b.group = uri.get("group").map(boolean);
        b.tether = uri.get("tether").map(boolean);
        b.fc = raw("fc");
        if let Some(value) = uri.get("gtag") {
            let tag = parse_long(value).map_err(|_| "'gtag' must be a valid long value".to_owned())?;
            b.group_tag = Some(tag.to_string());
        }
        b.cc = raw("cc");
        b.so_rcvbuf = size_param(uri, "so-rcvbuf")?;
        b.so_sndbuf = size_param(uri, "so-sndbuf")?;
        b.rcv_wnd = size_param(uri, "rcv-wnd")?;
        b.channel_snd_ts_offset = timestamp_offset(uri, "channel-snd-ts-offset", "channelSendTimestampOffset")?;
        b.channel_rcv_ts_offset = timestamp_offset(uri, "channel-rcv-ts-offset", "channelReceiveTimestampOffset")?;
        b.media_rcv_ts_offset = timestamp_offset(uri, "media-rcv-ts-offset", "mediaReceiveTimestampOffset")?;
        if let Some(value) = uri.get("session-id") {
            b.session_id = Some(match value.strip_prefix(TAG_PREFIX) {
                Some(tag) => format!(
                    "{TAG_PREFIX}{}",
                    parse_long(tag).map_err(|_| format!("error parsing long: {value}"))?
                ),
                None => parse_int(value)
                    .map_err(|_| "'session-id' must be a valid integer".to_owned())?
                    .to_string(),
            });
        }
        b.alias = raw("alias");
        if let Some(value) = uri.get("response-correlation-id") {
            if value != PROTOTYPE_CORRELATION_ID && !parse_long(value).is_ok_and(|id| id >= -1) {
                return Err(format!(
                    "responseCorrelationId must be a number greater than or equal to -1, or the value \
                     '{PROTOTYPE_CORRELATION_ID}' found: {value}"
                ));
            }
            b.response_correlation_id = Some(value.to_owned());
        }
        b.response_endpoint = raw("response-endpoint");
        if let Some(value) = uri.get("ttl") {
            let ttl = parse_int(value).map_err(|_| "'ttl' must be a value integer".to_owned())?;
            if !(0..=255).contains(&ttl) {
                return Err(format!("TTL not in range 0-255: {ttl}"));
            }
            b.ttl = Some(ttl.to_string());
        }
        if let Some(value) = uri.get("stream-id") {
            let id = parse_int(value).map_err(|_| "'stream-id' must be a valid integer".to_owned())?;
            b.stream_id = Some(id.to_string());
        }
        Ok(b)
    }

    /// `termLength(Integer)`: a power of two from 64 KiB to 1 GiB.
    pub fn term_length(mut self, term_length: i64) -> Result<Self, String> {
        if term_length < TERM_MIN_LENGTH {
            return Err(format!(
                "Term length less than min length of {TERM_MIN_LENGTH}: length={term_length}"
            ));
        }
        if term_length > TERM_MAX_LENGTH {
            return Err(format!(
                "Term length more than max length of {TERM_MAX_LENGTH}: length={term_length}"
            ));
        }
        if term_length & (term_length - 1) != 0 {
            return Err(format!("Term length not a power of 2: length={term_length}"));
        }
        self.term_length = Some(term_length);
        Ok(self)
    }

    /// `mtu(Integer)`: from 32 to 65,504, a multiple of the frame alignment.
    pub fn mtu(mut self, mtu: i64) -> Result<Self, String> {
        if !(32..=65_504).contains(&mtu) {
            return Err(format!("MTU not in range 32-65504: {mtu}"));
        }
        if mtu & (FRAME_ALIGNMENT - 1) != 0 {
            return Err(format!("MTU not a multiple of FRAME_ALIGNMENT: mtu={mtu}"));
        }
        self.mtu = Some(mtu);
        Ok(self)
    }

    /// `initialPosition`: a publication that starts at `position` of a
    /// stream of `initial_term_id` and `term_length`.
    pub fn initial_position(mut self, position: i64, initial_term_id: i32, term_length: i32) -> Result<Self, String> {
        if position < 0 {
            return Err(format!("invalid position={position} < 0"));
        }
        if position & (FRAME_ALIGNMENT - 1) != 0 {
            return Err(format!(
                "invalid position={position} does not have frame alignment={FRAME_ALIGNMENT}"
            ));
        }
        let bits = term_length.trailing_zeros();
        self.initial_term_id = Some(initial_term_id);
        self.term_id = Some(initial_term_id.wrapping_add((position >> bits) as i32));
        self.term_offset = Some((position & (i64::from(term_length) - 1)) as i32);
        self.term_length = Some(i64::from(term_length));
        Ok(self)
    }

    pub fn linger(mut self, linger_ns: i64) -> Self {
        self.linger_ns = Some(linger_ns);
        self
    }

    pub fn sparse(mut self, sparse: bool) -> Self {
        self.sparse = Some(sparse);
        self
    }

    pub fn response_correlation_id(mut self, id: i64) -> Self {
        self.response_correlation_id = Some(id.to_string());
        self
    }

    /// `build()`: `aeron:<media>?`, the parameters set, in the builder's
    /// order, a trailing `|` or `?` dropped.
    pub fn build(&self) -> String {
        let mut s = format!("aeron:{}?", self.media);
        let mut put = |name: &str, value: Option<&str>| {
            if let Some(value) = value {
                s.push_str(name);
                s.push('=');
                s.push_str(value);
                s.push('|');
            }
        };
        let size = |v: Option<i64>| v.map(format_size);
        put("tags", self.tags.as_deref());
        put("endpoint", self.endpoint.as_deref());
        put("interface", self.interface.as_deref());
        put("control", self.control.as_deref());
        put("control-mode", self.control_mode.as_deref());
        put("mtu", size(self.mtu).as_deref());
        put("term-length", size(self.term_length).as_deref());
        put("init-term-id", self.initial_term_id.map(|v| v.to_string()).as_deref());
        put("term-id", self.term_id.map(|v| v.to_string()).as_deref());
        put("term-offset", self.term_offset.map(|v| v.to_string()).as_deref());
        put("session-id", self.session_id.as_deref());
        put("ttl", self.ttl.as_deref());
        put("linger", self.linger_ns.map(format_duration).as_deref());
        put("alias", self.alias.as_deref());
        put("cc", self.cc.as_deref());
        put("fc", self.fc.as_deref());
        put("gtag", self.group_tag.as_deref());
        put("sparse", self.sparse.map(|b| b.to_string()).as_deref());
        put("eos", self.eos.as_deref());
        put("tether", self.tether.as_deref());
        put("group", self.group.as_deref());
        put("rejoin", self.rejoin.as_deref());
        put("so-sndbuf", size(self.so_sndbuf).as_deref());
        put("so-rcvbuf", size(self.so_rcvbuf).as_deref());
        put("rcv-wnd", size(self.rcv_wnd).as_deref());
        put("media-rcv-ts-offset", self.media_rcv_ts_offset.as_deref());
        put("channel-rcv-ts-offset", self.channel_rcv_ts_offset.as_deref());
        put("channel-snd-ts-offset", self.channel_snd_ts_offset.as_deref());
        put("response-endpoint", self.response_endpoint.as_deref());
        put("response-correlation-id", self.response_correlation_id.as_deref());
        put("stream-id", self.stream_id.as_deref());
        if s.ends_with(['|', '?']) {
            s.pop();
        }
        s
    }
}

/// The channel a recording subscribes to and the catalog keeps.
pub fn stripped_channel(uri: &ChannelUri) -> Result<String, String> {
    Ok(ChannelBuilder::stripped(uri)?.build())
}

/// `ArchiveConductor.makeKey`: the stream and the parameters that tell one
/// publication's stream from another's, raw.
pub fn recording_key(stream_id: i32, uri: &ChannelUri) -> String {
    let mut key = format!("{stream_id}:{}?", uri.media);
    for name in ["endpoint", "interface", "control", "session-id", "tags"] {
        if let Some(value) = uri.get(name) {
            key.push_str(name);
            key.push('=');
            key.push_str(value);
            key.push('|');
        }
    }
    key.pop();
    key
}

/// The control stream's settings a response channel falls back on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlStream {
    pub mtu_length: i64,
    pub term_length: i64,
    pub sparse: bool,
}

/// A control session's response channel, as `ArchiveConductor.newControlSession`
/// builds it: the client's channel stripped, with its MTU, term length and
/// sparseness, else the control stream's; a response-mode channel answers
/// the request image (`request_image_correlation_id`).
pub fn response_channel(
    channel: &str,
    control: ControlStream,
    request_image_correlation_id: i64,
) -> Result<String, String> {
    let uri = ChannelUri::parse(channel)?;
    let mtu = match uri.get("mtu") {
        Some(v) => parse_size("mtu", v)?,
        None => control.mtu_length,
    };
    let term_length = match uri.get("term-length") {
        Some(v) => parse_size("term-length", v)?,
        None => control.term_length,
    };
    let sparse = match uri.get("sparse") {
        Some(v) => v.eq_ignore_ascii_case("true"),
        None => control.sparse,
    };
    let mut builder = ChannelBuilder::stripped(&uri)?
        .term_length(term_length)?
        .sparse(sparse)
        .mtu(mtu)?;
    if uri.get("control-mode") == Some("response") {
        builder = builder.response_correlation_id(request_image_correlation_id);
    }
    Ok(builder.build())
}

/// A recording's settings a replay's publication takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayStream {
    pub initial_term_id: i32,
    pub term_length: i32,
    pub mtu_length: i32,
}

/// A replay's publication channel, as `ArchiveConductor.startReplay` builds
/// it: the requested channel stripped, starting at `position` of the
/// recording's stream, its EOS and sparseness if asked, the recording's
/// MTU, the requested linger (raw ns) or `default_linger_ns`.
pub fn replay_channel(
    channel: &str,
    position: i64,
    recording: ReplayStream,
    default_linger_ns: i64,
) -> Result<String, String> {
    let uri = ChannelUri::parse(channel)?;
    let mut builder =
        ChannelBuilder::stripped(&uri)?.initial_position(position, recording.initial_term_id, recording.term_length)?;
    builder.eos = uri.get("eos").map(boolean);
    builder.sparse = uri.get("sparse").map(|v| v.eq_ignore_ascii_case("true"));
    let mut builder = builder.mtu(i64::from(recording.mtu_length))?;
    let linger = match uri.get("linger") {
        Some(v) => parse_long(v)?,
        None => default_linger_ns,
    };
    if linger < 0 {
        return Err(format!("`linger` value cannot be negative: {linger}"));
    }
    builder = builder.linger(linger);
    Ok(builder.build())
}
