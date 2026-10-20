//! JSON config (serde) with the same keys and defaults as the C++
//! `config.json`, plus the combined-stream URL builder.

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Candidate WebSocket base URLs (combined-stream endpoint, no query).
    /// Tried in order; the first `max_endpoints` that complete the handshake
    /// are used. The same URL may appear twice to get two independent
    /// connections to one host.
    pub endpoints: Vec<String>,
    pub max_endpoints: usize,
    pub symbols: Vec<String>,
    /// 0 => run until SIGINT/SIGTERM.
    pub duration_s: u64,
    pub out: String,
    /// Empty => raw dump disabled.
    pub raw_out: String,
    /// Per queue (exact; rtrb does not round).
    pub queue_capacity: usize,
    pub ping_interval_ms: u64,
    pub connect_timeout_s: u64,
    /// A feed (no frame) or a symbol (no accepted message) with no update for
    /// this long is flagged stale. 0 disables.
    pub stale_after_ms: u64,
    /// Readers spin on the socket instead of blocking; one core per feed.
    pub reader_busy_poll: bool,
    /// CPU ids for [reader0, reader1, ..., parser, persist]; -1 or missing =
    /// unpinned.
    pub pin_cpus: Vec<i32>,
    /// Proactively replace each connection after this long (Binance closes
    /// every stream at the 24 h mark). Make-before-break: the new socket is
    /// opened before the old one is dropped. 0 disables.
    pub reconnect_after_s: u64,
    /// Feed i rotates its connection at reconnect_after_s + i * stagger, so
    /// redundant feeds never rotate at the same moment.
    pub reconnect_stagger_s: u64,
    /// Cap on the exponential back-off between failed reconnect attempts.
    pub reconnect_backoff_max_s: u64,
    /// Start a new output file (and raw dump) at every wall-clock boundary of
    /// this many seconds (86400 = daily at 00:00 UTC). Files are named
    /// `<stem>.<YYYYMMDDTHHMMSSZ>.<ext>` and never overwritten. 0 => a single
    /// file at exactly `out` (overwritten on start; for short test runs).
    pub rotate_s: u64,
    /// Flush buffered records to the OS this often, bounding loss on a crash.
    pub flush_interval_ms: u64,
    /// Progress line period when stdout is not a terminal.
    pub status_interval_s: u64,
    /// Reservoir size for latency / jitter / lag percentiles. Bounds memory
    /// on long runs: beyond this many samples the percentiles are estimated
    /// from a uniform random sample.
    pub max_samples: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            endpoints: vec!["wss://data-stream.binance.vision/stream".into()],
            max_endpoints: 2,
            symbols: vec!["btcusdt".into()],
            duration_s: 60,
            out: "data/market_data.l2bin".into(),
            raw_out: String::new(),
            queue_capacity: 1 << 16,
            ping_interval_ms: 1000,
            connect_timeout_s: 10,
            stale_after_ms: 5000,
            reader_busy_poll: false,
            pin_cpus: Vec::new(),
            reconnect_after_s: 23 * 3600,
            reconnect_stagger_s: 300,
            reconnect_backoff_max_s: 30,
            rotate_s: 86_400,
            flush_interval_ms: 1000,
            status_interval_s: 30,
            max_samples: 1 << 20,
        }
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Config, String> {
        let f = std::fs::File::open(path).map_err(|e| format!("config {path}: {e}"))?;
        let mut c: Config =
            serde_json::from_reader(std::io::BufReader::new(f)).map_err(|e| format!("config {path}: {e}"))?;
        c.normalize()?;
        Ok(c)
    }

    pub fn normalize(&mut self) -> Result<(), String> {
        for s in &mut self.symbols {
            *s = s.to_ascii_lowercase();
        }
        if self.endpoints.is_empty() {
            return Err("config: endpoints is empty".into());
        }
        if self.symbols.is_empty() {
            return Err("config: symbols is empty".into());
        }
        if self.max_endpoints < 1 {
            return Err("config: max_endpoints must be >= 1".into());
        }
        if self.symbols.len() > 65535 {
            return Err("config: too many symbols".into());
        }
        if self.ping_interval_ms < 1 {
            self.ping_interval_ms = 1000;
        }
        if self.queue_capacity < 16 {
            self.queue_capacity = 16;
        }
        if self.max_samples < 1024 {
            self.max_samples = 1024;
        }
        if self.reconnect_after_s > 0 && self.reconnect_after_s < 60 {
            return Err("config: reconnect_after_s must be 0 or >= 60".into());
        }
        if self.rotate_s > 0 && self.rotate_s < 60 {
            return Err("config: rotate_s must be 0 or >= 60".into());
        }
        if self.reconnect_backoff_max_s < 1 {
            self.reconnect_backoff_max_s = 1;
        }
        if self.status_interval_s < 1 {
            self.status_interval_s = 1;
        }
        Ok(())
    }
}

/// base + "?streams=btcusdt@trade/btcusdt@depth@100ms/ethusdt@trade/..."
pub fn build_stream_url(base: &str, symbols: &[String]) -> String {
    let mut url = String::from(base);
    url.push_str(if base.contains('?') { "&streams=" } else { "?streams=" });
    for (i, s) in symbols.iter().enumerate() {
        if i > 0 {
            url.push('/');
        }
        url.push_str(&format!("{s}@trade/{s}@depth@100ms"));
    }
    url
}
