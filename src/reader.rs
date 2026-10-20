//! One reader thread per connection. Each owns its `WsClient` and its own
//! SPSC `Sender` to the parser, so two feeds never share a lock or a socket
//! loop and a stall on one can't delay the other's receive timestamp.
//!
//! Loop: blocking `recv()` with a 100 ms timeout (or non-blocking spin with
//! `busy_poll`); PING once a second; stale check on every tick. Each message
//! is stamped (recv_ts_us, endpoint) and pushed.
//!
//! Connection lifetime. Binance closes every stream at the 24 h mark, so a
//! long-running reader never treats a socket as permanent:
//!
//! * **Planned rotation** (`reconnect_after`): make-before-break. A fresh
//!   socket is opened while the old one is still live, then they are swapped.
//!   Anything both sockets deliver is a duplicate for the parser's sequence
//!   arbitration, so there is no gap and no double-counting. Feeds are
//!   staggered by main so redundant feeds never rotate at the same moment.
//! * **Unplanned loss** (socket error, server CLOSE, failed PING): reconnect
//!   with exponential back-off, forever, until `running` clears or the
//!   deadline passes. The feed shows as stale/down meanwhile; the other feed
//!   carries on. The sender is kept, so the parser sees one continuous queue.
//!
//! Sequence watermarks live in the parser and survive reconnects, so a
//! reconnected feed cannot re-deliver anything already recorded.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tungstenite::Bytes;

use crate::queue::Sender;
use crate::stats::{Live, ReaderStats};
use crate::time::steady_us;
use crate::ws_client::WsClient;

/// Queue item from a reader: one complete WebSocket text message, as received.
pub struct RawMsg {
    pub recv_ts_us: i64,
    pub endpoint: u8,
    pub data: Bytes,
}

pub struct Reader<'a> {
    pub ws: WsClient,
    pub id: u8,
    pub out: Sender<RawMsg>,
    pub running: &'a AtomicBool,
    pub live: &'a Live,
    /// `None` => run until `running` clears.
    pub deadline: Option<Instant>,
    pub ping_interval: Duration,
    pub stale_after_us: i64,
    pub busy_poll: bool,
    pub connect_timeout: Duration,
    /// Planned rotation period for this feed (already staggered); `None` = never.
    pub reconnect_after: Option<Duration>,
    pub backoff_max: Duration,
    pub max_samples: usize,
}

/// Retry a failed planned rotation this much later (the old socket is still up).
const ROTATE_RETRY: Duration = Duration::from_secs(60);
const BACKOFF_START: Duration = Duration::from_millis(500);

impl<'a> Reader<'a> {
    pub fn run(mut self) -> ReaderStats {
        let mut st = ReaderStats::new(self.ws.url().to_string(), self.max_samples);
        self.apply_io_mode();
        let now = Instant::now();
        let mut next_ping = now + self.ping_interval;
        let mut next_rotate = self.reconnect_after.map(|d| now + d);
        st.stale.last_update_us = steady_us();
        let id = self.id;

        'outer: while self.running.load(Ordering::Relaxed) {
            let now = Instant::now();
            if self.deadline.is_some_and(|d| now >= d) {
                break;
            }
            if next_rotate.is_some_and(|t| now >= t) {
                self.rotate(&mut st, &mut next_ping, &mut next_rotate);
            }
            if now >= next_ping {
                if let Err(e) = self.ws.send_ping() {
                    if !self.reconnect(&mut st, &e, &mut next_ping, &mut next_rotate) {
                        break;
                    }
                    continue;
                }
                next_ping = now + self.ping_interval;
            }

            match self.ws.recv() {
                Ok(Some((data, recv_ts_us))) => {
                    st.frames += 1;
                    st.bytes += data.len();
                    if let Some(rtt) = self.ws.take_rtt() {
                        st.rtts.push(rtt);
                    }
                    let ms = st.stale.touch(steady_us());
                    if ms >= 0 {
                        self.live.feed_stale[id as usize].store(false, Ordering::Relaxed);
                        eprintln!("\n[stale] feed {id} recovered after {ms} ms");
                    }
                    if !self.out.push(RawMsg { recv_ts_us, endpoint: id, data }) {
                        break 'outer; // parser gone
                    }
                }
                Ok(None) => {
                    if let Some(rtt) = self.ws.take_rtt() {
                        st.rtts.push(rtt);
                    }
                    if self.busy_poll {
                        std::hint::spin_loop();
                    }
                }
                Err(e) => {
                    if !self.reconnect(&mut st, &e, &mut next_ping, &mut next_rotate) {
                        break;
                    }
                    continue;
                }
            }

            self.check_stale(&mut st);
        }
        st.stale.finish(steady_us());
        st // `self.out` drops here => queue closed
    }

    fn apply_io_mode(&mut self) {
        if self.busy_poll {
            self.ws.set_nonblocking(true);
        } else {
            self.ws.set_read_timeout(Some(Duration::from_millis(100)));
        }
    }

    fn check_stale(&mut self, st: &mut ReaderStats) {
        if self.stale_after_us > 0 && st.stale.check(steady_us(), self.stale_after_us) {
            self.live.feed_stale[self.id as usize].store(true, Ordering::Relaxed);
            eprintln!("\n[stale] feed {}: no data for {} ms", self.id, self.stale_after_us / 1000);
        }
    }

    /// Planned make-before-break rotation. On failure the old socket stays in
    /// use and we try again in `ROTATE_RETRY`.
    fn rotate(&mut self, st: &mut ReaderStats, next_ping: &mut Instant, next_rotate: &mut Option<Instant>) {
        let id = self.id;
        let url = self.ws.url().to_string();
        match WsClient::connect(&url, self.connect_timeout) {
            Ok(fresh) => {
                let old = std::mem::replace(&mut self.ws, fresh);
                self.apply_io_mode();
                old.close();
                st.rotations += 1;
                self.live.reconnects.fetch_add(1, Ordering::Relaxed);
                let now = Instant::now();
                *next_ping = now + self.ping_interval;
                *next_rotate = self.reconnect_after.map(|d| now + d);
                eprintln!("\n[reader] feed {id}: rotated connection (planned, #{})", st.rotations);
            }
            Err(e) => {
                st.connect_failures += 1;
                *next_rotate = Some(Instant::now() + ROTATE_RETRY);
                eprintln!(
                    "\n[reader] feed {id}: planned rotation failed ({e}); keeping current socket, retry in {} s",
                    ROTATE_RETRY.as_secs()
                );
            }
        }
    }

    /// Unplanned loss: reconnect with back-off. Returns false when the reader
    /// should exit instead (stop requested or deadline passed).
    fn reconnect(
        &mut self,
        st: &mut ReaderStats,
        reason: &str,
        next_ping: &mut Instant,
        next_rotate: &mut Option<Instant>,
    ) -> bool {
        let id = self.id;
        let url = self.ws.url().to_string();
        st.disconnects += 1;
        st.connected = false;
        self.live.feed_down[id as usize].store(true, Ordering::Relaxed);
        eprintln!("\n[reader] feed {id} ({url}) lost: {reason}; reconnecting");

        let mut backoff = BACKOFF_START;
        let mut attempt = 0usize;
        loop {
            if !self.running.load(Ordering::Relaxed) || self.deadline.is_some_and(|d| Instant::now() >= d) {
                return false;
            }
            attempt += 1;
            match WsClient::connect(&url, self.connect_timeout) {
                Ok(fresh) => {
                    self.ws = fresh;
                    self.apply_io_mode();
                    st.connected = true;
                    self.live.feed_down[id as usize].store(false, Ordering::Relaxed);
                    self.live.reconnects.fetch_add(1, Ordering::Relaxed);
                    let now = Instant::now();
                    *next_ping = now + self.ping_interval;
                    *next_rotate = self.reconnect_after.map(|d| now + d);
                    eprintln!("\n[reader] feed {id}: reconnected (attempt {attempt}, disconnect #{})", st.disconnects);
                    return true;
                }
                Err(e) => {
                    st.connect_failures += 1;
                    eprintln!("\n[reader] feed {id}: reconnect attempt {attempt} failed ({e}); retry in {:.1} s", backoff.as_secs_f64());
                    self.sleep_interruptible(backoff);
                    backoff = (backoff * 2).min(self.backoff_max);
                }
            }
            // Keep stale bookkeeping honest while down.
            self.check_stale(st);
        }
    }

    fn sleep_interruptible(&self, d: Duration) {
        let end = Instant::now() + d;
        while self.running.load(Ordering::Relaxed) {
            let now = Instant::now();
            if now >= end {
                break;
            }
            std::thread::sleep((end - now).min(Duration::from_millis(100)));
        }
    }
}
