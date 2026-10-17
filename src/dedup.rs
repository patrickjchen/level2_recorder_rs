//! Sequence-number arbitration across redundant feeds (classic A/B feed
//! arbitration). Per (symbol, stream-type) we keep a high-water mark: a
//! message is accepted iff its sequence exceeds the highest accepted so far.
//!
//! Binance sequences are strictly increasing per symbol on every connection
//! (trade id `t` by +1; depth `u`, with the next event's `U` == previous
//! `u`+1), so anything at or below the watermark was already delivered by
//! another connection. The first copy to arrive wins, whichever feed it came
//! from. A small ring of recent accepted (seq, recv_ts) per stream lets a
//! duplicate report how far behind the losing feed was.

use crate::parser::{MsgType, ParsedMsg};

const RING: usize = 256;

#[derive(Clone, Copy, Default)]
struct Entry {
    seq: u64,
    recv_ts_us: i64,
}

struct Stream {
    hwm: u64,
    ring: [Entry; RING],
    pos: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct Verdict {
    pub accepted: bool,
    pub gap: bool,          // accepted but sequence skipped (feed loss)
    pub dup_lag_us: Option<i64>, // duplicate: recv(dup) - recv(winner), if found
}

pub struct Deduper {
    streams: Vec<Stream>,
}

impl Deduper {
    pub fn new(n_symbols: usize) -> Self {
        Deduper {
            streams: (0..n_symbols * 2).map(|_| Stream { hwm: 0, ring: [Entry::default(); RING], pos: 0 }).collect(),
        }
    }

    pub fn check(&mut self, m: &ParsedMsg, recv_ts_us: i64) -> Verdict {
        let is_trade = m.msg_type == Some(MsgType::Trade);
        let s = &mut self.streams[m.symbol_id as usize * 2 + if is_trade { 0 } else { 1 }];
        let mut v = Verdict { accepted: false, gap: false, dup_lag_us: None };

        if s.hwm != 0 && m.seq <= s.hwm {
            for k in 0..RING {
                let e = &s.ring[(s.pos + RING - 1 - k) % RING];
                if e.seq == m.seq {
                    v.dup_lag_us = Some(recv_ts_us - e.recv_ts_us);
                    break;
                }
                if e.seq == 0 || e.seq < m.seq {
                    break;
                }
            }
            return v;
        }

        v.accepted = true;
        if s.hwm != 0 {
            let expect_first = s.hwm + 1;
            let got_first = if is_trade { m.seq } else { m.first_seq };
            v.gap = got_first != expect_first;
        }
        s.hwm = m.seq;
        s.ring[s.pos] = Entry { seq: m.seq, recv_ts_us };
        s.pos = (s.pos + 1) % RING;
        v
    }
}
