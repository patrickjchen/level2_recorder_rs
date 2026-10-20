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

/// Fixed-capacity uniform sample of a stream (Vitter's Algorithm R), so
/// percentiles over a multi-day run cost bounded memory. Exact while fewer
/// than `cap` values have been seen; an unbiased estimate after that.
#[derive(Debug)]
pub struct Reservoir {
    buf: Vec<i64>,
    cap: usize,
    total: u64,
    rng: u64,
}

impl Reservoir {
    pub fn new(cap: usize) -> Self {
        let cap = cap.max(1);
        Reservoir { buf: Vec::with_capacity(cap.min(1 << 16)), cap, total: 0, rng: 0x9E37_79B9_7F4A_7C15 }
    }

    #[inline]
    pub fn push(&mut self, v: i64) {
        self.total += 1;
        if self.buf.len() < self.cap {
            self.buf.push(v);
            return;
        }
        // Replace a random slot with probability cap / total.
        let j = self.next_u64() % self.total;
        if (j as usize) < self.cap {
            self.buf[j as usize] = v;
        }
    }

    #[inline]
    fn next_u64(&mut self) -> u64 {
        // xorshift64*: cheap, good enough for sampling.
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn total(&self) -> u64 {
        self.total
    }
    pub fn sampled(&self) -> usize {
        self.buf.len()
    }
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }
    pub fn is_estimated(&self) -> bool {
        self.total > self.buf.len() as u64
    }

    /// Percentiles over the sample; `n` reports the total seen, not the sample size.
    pub fn percentiles(&self) -> Pcts {
        let mut p = percentiles(&self.buf);
        p.n = self.total as usize;
        p
    }
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
#[derive(Debug)]
pub struct ReaderStats {
    pub url: String,
    pub frames: usize,
    pub bytes: usize,
    pub disconnects: usize,      // unplanned: socket error / server CLOSE
    pub rotations: usize,        // planned make-before-break reconnects
    pub connect_failures: usize, // failed (re)connect attempts
    pub connected: bool,         // state at exit
    pub stale: StaleTrack,
    pub rtts: Reservoir, // PING/PONG round trips (us)
}

impl ReaderStats {
    pub fn new(url: String, max_samples: usize) -> Self {
        ReaderStats {
            url,
            frames: 0,
            bytes: 0,
            disconnects: 0,
            rotations: 0,
            connect_failures: 0,
            connected: true,
            stale: StaleTrack::default(),
            rtts: Reservoir::new(max_samples),
        }
    }
}

/// Per-feed arbitration counters, owned by the parser thread.
#[derive(Debug)]
pub struct FeedArbStats {
    pub accepted: usize,    // this feed's copy arrived first
    pub dups: usize,        // already delivered by another feed
    pub dup_lag: Reservoir, // how far behind the losing copy was (us)
    pub misordered: usize,  // dup whose copy was received EARLIER than the accepted one
}

impl FeedArbStats {
    pub fn new(max_samples: usize) -> Self {
        FeedArbStats { accepted: 0, dups: 0, dup_lag: Reservoir::new(max_samples), misordered: 0 }
    }
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
    pub latencies: Reservoir, // recv - exch, us (skew-contaminated)
    pub jitters: Reservoir,   // (recv_dt - exch_dt) between consecutive accepted msgs, us
    prev_recv_us: i64,
    prev_exch_ms: i64,
}

impl ParseStats {
    pub fn new(n_feeds: usize, n_symbols: usize, max_samples: usize) -> Self {
        ParseStats {
            msgs: 0,
            trades: 0,
            depth: 0,
            levels: 0,
            unknown: 0,
            gaps: 0,
            stale_markers: 0,
            feeds: (0..n_feeds).map(|_| FeedArbStats::new(max_samples)).collect(),
            symbols: (0..n_symbols).map(|_| SymbolStats::default()).collect(),
            latencies: Reservoir::new(max_samples),
            jitters: Reservoir::new(max_samples),
            prev_recv_us: 0,
            prev_exch_ms: 0,
        }
    }

    pub fn add(&mut self, recv_us: i64, exch_ms: i64) {
        self.latencies.push(recv_us - exch_ms * 1000);
        if self.prev_recv_us != 0 {
            let recv_dt = recv_us - self.prev_recv_us;
            let exch_dt = (exch_ms - self.prev_exch_ms) * 1000;
            self.jitters.push(recv_dt - exch_dt);
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
    pub reconnects: AtomicUsize, // planned + unplanned, all feeds
    pub files: AtomicUsize,      // output files opened so far
    pub feed_stale: Vec<AtomicBool>,
    pub feed_down: Vec<AtomicBool>, // disconnected and retrying
    pub symbol_stale: Vec<AtomicBool>,
}

impl Live {
    pub fn new(n_feeds: usize, n_symbols: usize) -> Self {
        Live {
            accepted: AtomicUsize::new(0),
            records: AtomicUsize::new(0),
            reconnects: AtomicUsize::new(0),
            files: AtomicUsize::new(0),
            feed_stale: (0..n_feeds).map(|_| AtomicBool::new(false)).collect(),
            feed_down: (0..n_feeds).map(|_| AtomicBool::new(false)).collect(),
            symbol_stale: (0..n_symbols).map(|_| AtomicBool::new(false)).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservoir_exact_below_cap_then_bounded() {
        let mut r = Reservoir::new(100);
        for i in 0..100 {
            r.push(i);
        }
        assert!(!r.is_estimated());
        assert_eq!(r.percentiles().max, 99);
        for i in 100..100_000 {
            r.push(i);
        }
        assert_eq!(r.sampled(), 100);
        assert_eq!(r.total(), 100_000);
        assert!(r.is_estimated());
        let p = r.percentiles();
        assert_eq!(p.n, 100_000);
        // Uniform sample of 0..100k: the median should land near 50k.
        assert!(p.p50 > 20_000 && p.p50 < 80_000, "p50={}", p.p50);
    }
}
