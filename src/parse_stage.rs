//! Parser thread: N reader queues -> merge by recv_ts -> parse (sonic-rs) ->
//! de-duplicate across feeds -> fixed-size binary Records -> persister
//! queue. Optionally forwards the accepted copy's raw JSON to a raw queue.
//!
//! Merge: with one SPSC queue per reader, the parser peeks every head and
//! takes the one with the smallest recv_ts_us, so "first copy wins" is judged
//! by receive time, not by which queue happened to be drained first. The
//! remaining race is a copy that was received earlier but not yet pushed when
//! the other head was taken; it is bounded by one push (microseconds) and is
//! counted as `misordered` on the losing feed rather than hidden.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::dedup::Deduper;
use crate::parser::{LevelFx, MsgType, ParsedMsg, Parser};
use crate::queue::{Receiver, Sender};
use crate::reader::RawMsg;
use crate::record::{Record, FIXED_SCALE, REC_ADD, REC_CANCEL, REC_STALE, REC_TRADE, SIDE_BUY, SIDE_SELL};
use crate::stats::{Live, ParseStats};
use crate::time::{steady_us, wall_us};

pub struct ParseStage<'a> {
    pub symbols: &'a [String],
    pub inputs: Vec<Receiver<RawMsg>>,
    pub out: Sender<Record>,
    pub raw_out: Option<Sender<RawMsg>>,
    pub running: &'a AtomicBool,
    pub live: &'a Live,
    pub stale_after_us: i64,
}

impl<'a> ParseStage<'a> {
    pub fn run(mut self) -> ParseStats {
        let n_feeds = self.inputs.len();
        let parser = Parser::new(self.symbols);
        let mut dedup = Deduper::new(self.symbols.len());
        let mut st = ParseStats::new(n_feeds, self.symbols.len());
        let mut m = ParsedMsg::default();

        let t0 = steady_us();
        for s in &mut st.symbols {
            s.stale.last_update_us = t0;
        }
        let mut last_stale_check = t0;
        let mut idle_spins = 0u32;

        loop {
            // Pick the queue whose head was received earliest. Read "closed"
            // before peeking so a close after the final push can't hide it.
            let mut best: Option<(usize, i64)> = None;
            let mut all_closed = true;
            for (i, q) in self.inputs.iter().enumerate() {
                let closed = q.is_closed();
                match q.peek() {
                    Some(h) => {
                        all_closed = false;
                        if best.map_or(true, |(_, ts)| h.recv_ts_us < ts) {
                            best = Some((i, h.recv_ts_us));
                        }
                    }
                    None => {
                        if !closed {
                            all_closed = false;
                        }
                    }
                }
            }

            let Some((best, _)) = best else {
                if all_closed {
                    break;
                }
                let now = steady_us();
                if now - last_stale_check >= 1000 {
                    self.check_stale(now, &mut st);
                    last_stale_check = now;
                }
                idle_spins += 1;
                if idle_spins < 256 {
                    std::thread::yield_now();
                } else {
                    std::thread::sleep(Duration::from_micros(50));
                }
                continue;
            };
            idle_spins = 0;
            let Some(raw) = self.inputs[best].pop() else { continue }; // single consumer: never None after peek
            let ep = raw.endpoint as usize;

            if parser.parse(&raw.data, &mut m).is_none() {
                st.unknown += 1;
                continue;
            }
            let v = dedup.check(&m, raw.recv_ts_us);
            if !v.accepted {
                st.feeds[ep].dups += 1;
                if let Some(lag) = v.dup_lag_us {
                    st.feeds[ep].dup_lag_us.push(lag);
                    if lag < 0 {
                        st.feeds[ep].misordered += 1;
                    }
                }
                continue;
            }
            st.feeds[ep].accepted += 1;
            if v.gap {
                st.gaps += 1;
            }
            st.add(raw.recv_ts_us, m.exch_ts_ms);
            st.msgs += 1;
            self.live.accepted.store(st.msgs, Ordering::Relaxed);

            let now = steady_us();
            let sym = &mut st.symbols[m.symbol_id as usize];
            sym.msgs += 1;
            sym.last_exch_ms = m.exch_ts_ms;
            sym.last_seq = m.seq;
            let ms = sym.stale.touch(now);
            if ms >= 0 {
                self.live.symbol_stale[m.symbol_id as usize].store(false, Ordering::Relaxed);
                eprintln!("\n[stale] symbol {} recovered after {ms} ms", self.symbols[m.symbol_id as usize]);
            }
            if now - last_stale_check >= 1000 {
                self.check_stale(now, &mut st);
                last_stale_check = now;
            }

            let base = Record {
                exch_ts_ms: m.exch_ts_ms,
                recv_ts_us: raw.recv_ts_us,
                seq: m.seq,
                symbol_id: m.symbol_id,
                endpoint: raw.endpoint,
                ..Default::default()
            };

            let ok = match m.msg_type {
                Some(MsgType::Trade) => {
                    st.trades += 1;
                    self.out.push(Record {
                        rtype: REC_TRADE,
                        side: if m.buyer_is_maker { SIDE_SELL } else { SIDE_BUY },
                        price: m.px,
                        qty: m.qty,
                        ..base
                    })
                }
                Some(MsgType::Depth) => {
                    st.depth += 1;
                    let mut ok = true;
                    for lv in &m.bids {
                        ok &= Self::emit(&mut self.out, &base, SIDE_BUY, lv, &mut st);
                    }
                    for lv in &m.asks {
                        ok &= Self::emit(&mut self.out, &base, SIDE_SELL, lv, &mut st);
                    }
                    ok
                }
                None => true,
            };
            if !ok {
                eprintln!("\n[parse] persister gone, stopping");
                self.running.store(false, Ordering::Relaxed);
                break;
            }

            if let Some(r) = self.raw_out.as_mut() {
                r.push(raw);
            }
        }
        let t1 = steady_us();
        for s in &mut st.symbols {
            s.stale.finish(t1);
        }
        st // `out` / `raw_out` drop here => downstream queues closed
    }

    fn emit(out: &mut Sender<Record>, base: &Record, side: u8, lv: &LevelFx, st: &mut ParseStats) -> bool {
        st.levels += 1;
        out.push(Record {
            rtype: if lv.qty == 0 { REC_CANCEL } else { REC_ADD },
            side,
            price: lv.px,
            qty: lv.qty,
            ..*base
        })
    }

    /// Flag symbols with no accepted message for stale_after_ms and write an
    /// 'X' marker into the stream so the file itself shows the gap.
    fn check_stale(&mut self, now: i64, st: &mut ParseStats) {
        if self.stale_after_us <= 0 {
            return;
        }
        for i in 0..st.symbols.len() {
            let s = &mut st.symbols[i];
            if !s.stale.check(now, self.stale_after_us) {
                continue;
            }
            self.live.symbol_stale[i].store(true, Ordering::Relaxed);
            eprintln!(
                "\n[stale] symbol {}: no update for {} ms (last seq {})",
                self.symbols[i],
                self.stale_after_us / 1000,
                s.last_seq
            );
            let rec = Record {
                exch_ts_ms: s.last_exch_ms,
                recv_ts_us: wall_us(),
                seq: s.last_seq,
                symbol_id: i as u16,
                rtype: REC_STALE,
                side: b'-',
                price: 0,
                qty: (self.stale_after_us / 1000) * FIXED_SCALE,
                ..Default::default()
            };
            self.out.push(rec);
            st.stale_markers += 1;
        }
    }
}
