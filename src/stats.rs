//! Percentiles, counters, and staleness bookkeeping. Each stats struct is
//! owned by exactly one stage thread and handed back to main on join; the
//! only live-shared state is `Live` (atomics for the progress line).

use std::sync::atomic::{AtomicBool, AtomicUsize};

#[derive(Clone, Copy, Debug, Default)]
pub struct Pcts {
    pub n: usize,
    pub min: i64,
    pub p1: i64,
    pub p50: i64,
    pub p90: i64,
    pub p99: i64,
    pub p999: i64,
    pub max: i64,
}

pub fn percentiles(src: &[i64]) -> Pcts {
    let mut p = Pcts { n: src.len(), ..Default::default() };
    if src.is_empty() {
        return p;
    }
    let mut v = src.to_vec();
    v.sort_unstable();
    let at = |q: f64| v[(q * (v.len() - 1) as f64) as usize];
    p.min = v[0];
    p.p1 = at(0.01);
    p.p50 = at(0.50);
    p.p90 = at(0.90);
    p.p99 = at(0.99);
    p.p999 = at(0.999);
    p.max = v[v.len() - 1];
    p
}

pub fn print_pcts(p: &Pcts, include_p1: bool, indent: &str) {
    if p.n == 0 {
        return;
    }
    println!("{indent}min  : {} us", p.min);
    if include_p1 {
        println!("{indent}p1   : {} us", p.p1);
    }
    println!("{indent}p50  : {} us", p.p50);
    println!("{indent}p90  : {} us", p.p90);
    println!("{indent}p99  : {} us", p.p99);
    println!("{indent}p99.9: {} us", p.p999);
    println!("{indent}max  : {} us", p.max);
}

/// Staleness bookkeeping shared in shape by feeds and symbols. Owned by one
/// thread; the live flag is mirrored into `Live` by the caller.
#[derive(Debug, Default)]
pub struct StaleTrack {
    pub stale: bool,
    pub last_update_us: i64, // steady clock
    pub stale_since_us: i64, // steady clock
    pub episodes: usize,
    pub total_ms: i64,
    pub max_ms: i64,
}

impl StaleTrack {
    /// Returns true on the transition into stale.
    pub fn check(&mut self, now_us: i64, threshold_us: i64) -> bool {
        // Note: no `last_update_us == 0` guard — the steady clock starts at 0
        // in this process, and every owner initializes last_update_us at
        // start, so 0 is a legitimate "updated at t0" value.
        if threshold_us <= 0 || self.stale {
            return false;
        }
        if now_us - self.last_update_us < threshold_us {
            return false;
        }
        self.stale = true;
        self.stale_since_us = self.last_update_us + threshold_us;
        self.episodes += 1;
        true
    }

    /// Call on every update. Returns the stale duration (ms) if this update
    /// ends a stale episode, else -1.
    pub fn touch(&mut self, now_us: i64) -> i64 {
        self.last_update_us = now_us;
        if !self.stale {
            return -1;
        }
        self.stale = false;
        let ms = (now_us - self.stale_since_us) / 1000;
        self.total_ms += ms;
        self.max_ms = self.max_ms.max(ms);
        ms
    }

    /// At shutdown, close an open episode so totals include it.
    pub fn finish(&mut self, now_us: i64) {
        if self.stale {
            let ms = (now_us - self.stale_since_us) / 1000;
            self.total_ms += ms;
            self.max_ms = self.max_ms.max(ms);
        }
    }
}

/// Owned by a reader thread; returned on join.
#[derive(Debug, Default)]
pub struct ReaderStats {
    pub url: String,
    pub frames: usize,
    pub bytes: usize,
    pub dropped: bool, // connection died (fatal socket error)
    pub stale: StaleTrack,
    pub rtts_us: Vec<i64>, // PING/PONG round trips
}

/// Per-feed arbitration counters, owned by the parser thread.
#[derive(Debug, Default)]
pub struct FeedArbStats {
    pub accepted: usize,       // this feed's copy arrived first
    pub dups: usize,           // already delivered by another feed
    pub dup_lag_us: Vec<i64>,  // how far behind the losing copy was
    pub misordered: usize,     // dup whose copy was received EARLIER than the accepted one
}

#[derive(Debug, Default)]
pub struct SymbolStats {
    pub msgs: usize,
    pub last_exch_ms: i64,
    pub last_seq: u64,
    pub stale: StaleTrack,
}

/// Owned by the parser thread; returned on join.
#[derive(Debug)]
pub struct ParseStats {
    pub msgs: usize,
    pub trades: usize,
    pub depth: usize,
    pub levels: usize,
    pub unknown: usize,
    pub gaps: usize,
    pub stale_markers: usize,
    pub feeds: Vec<FeedArbStats>,
    pub symbols: Vec<SymbolStats>,
    pub latencies_us: Vec<i64>, // recv - exch (skew-contaminated)
    pub jitters_us: Vec<i64>,   // (recv_dt - exch_dt) between consecutive accepted msgs
    prev_recv_us: i64,
    prev_exch_ms: i64,
}

impl ParseStats {
    pub fn new(n_feeds: usize, n_symbols: usize) -> Self {
        ParseStats {
            msgs: 0,
            trades: 0,
            depth: 0,
            levels: 0,
            unknown: 0,
            gaps: 0,
            stale_markers: 0,
            feeds: (0..n_feeds).map(|_| FeedArbStats::default()).collect(),
            symbols: (0..n_symbols).map(|_| SymbolStats::default()).collect(),
            latencies_us: Vec::with_capacity(1 << 20),
            jitters_us: Vec::with_capacity(1 << 20),
            prev_recv_us: 0,
            prev_exch_ms: 0,
        }
    }

    pub fn add(&mut self, recv_us: i64, exch_ms: i64) {
        self.latencies_us.push(recv_us - exch_ms * 1000);
        if self.prev_recv_us != 0 {
            let recv_dt = recv_us - self.prev_recv_us;
            let exch_dt = (exch_ms - self.prev_exch_ms) * 1000;
            self.jitters_us.push(recv_dt - exch_dt);
        }
        self.prev_recv_us = recv_us;
        self.prev_exch_ms = exch_ms;
    }
}

/// Live counters for the main thread's progress line. Written with relaxed
/// stores by the stages, read by main; never used for control flow.
pub struct Live {
    pub accepted: AtomicUsize,
    pub records: AtomicUsize,
    pub feed_stale: Vec<AtomicBool>,
    pub symbol_stale: Vec<AtomicBool>,
}

impl Live {
    pub fn new(n_feeds: usize, n_symbols: usize) -> Self {
        Live {
            accepted: AtomicUsize::new(0),
            records: AtomicUsize::new(0),
            feed_stale: (0..n_feeds).map(|_| AtomicBool::new(false)).collect(),
            symbol_stale: (0..n_symbols).map(|_| AtomicBool::new(false)).collect(),
        }
    }
}
