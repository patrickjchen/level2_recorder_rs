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
