//! One reader thread per connection. Each owns its `WsClient` and its own
//! SPSC `Sender` to the parser, so two feeds never share a lock or a socket
//! loop and a stall on one can't delay the other's receive timestamp.
//!
//! Loop: blocking `recv()` with a 100 ms timeout (or non-blocking spin with
//! `busy_poll`); PING once a second; stale check on every tick. Each message
//! is stamped (recv_ts_us, endpoint) and pushed. On a fatal socket error the
//! reader logs, drops its sender (closing the queue) and exits; the other
//! feed carries on.

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
    pub deadline: Instant,
    pub ping_interval: Duration,
    pub stale_after_us: i64,
    pub busy_poll: bool,
}

impl<'a> Reader<'a> {
    pub fn run(mut self) -> ReaderStats {
        let mut st = ReaderStats { url: self.ws.url().to_string(), ..Default::default() };
        if self.busy_poll {
            self.ws.set_nonblocking(true);
        } else {
            self.ws.set_read_timeout(Some(Duration::from_millis(100)));
        }
        let mut next_ping = Instant::now() + self.ping_interval;
        st.stale.last_update_us = steady_us();
        let id = self.id;

        'outer: while self.running.load(Ordering::Relaxed) {
            let now = Instant::now();
            if now >= self.deadline {
                break;
            }
            if now >= next_ping {
                if let Err(e) = self.ws.send_ping() {
                    eprintln!("\n[reader] feed {id} ({}) dropped: {e}", st.url);
                    st.dropped = true;
                    break;
                }
                next_ping += self.ping_interval;
            }

            match self.ws.recv() {
                Ok(Some((data, recv_ts_us))) => {
                    st.frames += 1;
                    st.bytes += data.len();
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
                    if self.busy_poll {
                        std::hint::spin_loop();
                    }
                }
                Err(e) => {
                    eprintln!("\n[reader] feed {id} ({}) dropped: {e}", st.url);
                    st.dropped = true;
                    break;
                }
            }

            if self.stale_after_us > 0 && st.stale.check(steady_us(), self.stale_after_us) {
                self.live.feed_stale[id as usize].store(true, Ordering::Relaxed);
                eprintln!("\n[stale] feed {id}: no data for {} ms", self.stale_after_us / 1000);
            }
        }
        st.stale.finish(steady_us());
        st.rtts_us = self.ws.take_rtts();
        st // `self.out` drops here => queue closed
    }
}
