//! sonic-rs (SIMD) parser for the Binance combined-stream wrapper
//!   {"stream":"<sym>@trade","data":{...}}
//! into a binary `ParsedMsg` with fixed-point prices. The wrapper is parsed
//! once with `data` kept as a lazy raw slice; the stream suffix picks the
//! payload struct, so no key-order assumptions and no tag buffering.

use std::collections::HashMap;

use serde::Deserialize;
use sonic_rs::LazyValue;

use crate::record::parse_fixed8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MsgType {
    Trade,
    Depth,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LevelFx {
    pub px: i64,
    pub qty: i64,
}

/// Binary form of one exchange message. Reused across calls (level vecs keep
/// their capacity).
#[derive(Debug, Default)]
pub struct ParsedMsg {
    pub msg_type: Option<MsgType>,
    pub symbol_id: u16,
    pub exch_ts_ms: i64,
    pub seq: u64,       // trade: t ; depth: u (final update id)
    pub first_seq: u64, // depth: U (first update id); 0 for trades
    pub px: i64,
    pub qty: i64,
    pub buyer_is_maker: bool,
    pub bids: Vec<LevelFx>,
    pub asks: Vec<LevelFx>,
}

#[derive(Deserialize)]
struct Wrapper<'a> {
    #[serde(borrow)]
    stream: &'a str,
    #[serde(borrow)]
    data: LazyValue<'a>,
}

// Binance trade: e, E, s, t, p, q, T, m, M
#[derive(Deserialize)]
struct TradeMsg<'a> {
    t: u64,
    #[serde(borrow)]
    p: &'a str,
    #[serde(borrow)]
    q: &'a str,
    #[serde(rename = "T")]
    trade_time: i64,
    m: bool,
}

// Binance depthUpdate: e, E, s, U, u, b, a
#[derive(Deserialize)]
struct DepthMsg<'a> {
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "U")]
    first: u64,
    u: u64,
    #[serde(borrow)]
    b: Vec<[&'a str; 2]>,
    #[serde(borrow)]
    a: Vec<[&'a str; 2]>,
}

pub struct Parser {
    symbol_ids: HashMap<String, u16>,
}

impl Parser {
    pub fn new(symbols: &[String]) -> Self {
        Parser { symbol_ids: symbols.iter().enumerate().map(|(i, s)| (s.clone(), i as u16)).collect() }
    }

    /// `None` for unrecognized / malformed messages (caller counts Unknown).
    pub fn parse(&self, raw: &[u8], m: &mut ParsedMsg) -> Option<MsgType> {
        m.msg_type = None;
        let w: Wrapper = sonic_rs::from_slice(raw).ok()?;
        let at = w.stream.find('@')?;
        m.symbol_id = *self.symbol_ids.get(&w.stream[..at])?;
        let payload = w.data.as_raw_str();

        if w.stream.ends_with("@trade") {
            let t: TradeMsg = sonic_rs::from_str(payload).ok()?;
            m.seq = t.t;
            m.first_seq = 0;
            m.px = parse_fixed8(t.p);
            m.qty = parse_fixed8(t.q);
            m.exch_ts_ms = t.trade_time;
            m.buyer_is_maker = t.m;
            m.msg_type = Some(MsgType::Trade);
        } else if w.stream.contains("@depth") {
            let d: DepthMsg = sonic_rs::from_str(payload).ok()?;
            m.exch_ts_ms = d.event_time;
            m.first_seq = d.first;
            m.seq = d.u;
            m.bids.clear();
            m.asks.clear();
            m.bids.extend(d.b.iter().map(|[p, q]| LevelFx { px: parse_fixed8(p), qty: parse_fixed8(q) }));
            m.asks.extend(d.a.iter().map(|[p, q]| LevelFx { px: parse_fixed8(p), qty: parse_fixed8(q) }));
            m.msg_type = Some(MsgType::Depth);
        }
        m.msg_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_trade_and_depth() {
        let p = Parser::new(&["btcusdt".to_string(), "ethusdt".to_string()]);
        let mut m = ParsedMsg::default();
        let trade = br#"{"stream":"ethusdt@trade","data":{"e":"trade","E":1792239466790,"s":"ETHUSDT","t":6770677107,"p":"81553.92000000","q":"0.00121000","T":1792239466786,"m":true,"M":true}}"#;
        assert_eq!(p.parse(trade, &mut m), Some(MsgType::Trade));
        assert_eq!(m.symbol_id, 1);
        assert_eq!(m.seq, 6770677107);
        assert_eq!(m.exch_ts_ms, 1792239466786);
        assert_eq!(m.px, 8_155_392_000_000);
        assert_eq!(m.qty, 121_000);
        assert!(m.buyer_is_maker);

        let depth = br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1792235233014,"s":"BTCUSDT","U":101561903372,"u":101561903379,"b":[["73510.86000000","0.00150000"],["65342.99000000","0.00330000"]],"a":[["81799.21000000","0.00000000"]]}}"#;
        assert_eq!(p.parse(depth, &mut m), Some(MsgType::Depth));
        assert_eq!(m.symbol_id, 0);
        assert_eq!((m.first_seq, m.seq), (101561903372, 101561903379));
        assert_eq!(m.bids.len(), 2);
        assert_eq!(m.asks[0].qty, 0);
        assert_eq!(m.bids[0].px, 7_351_086_000_000);

        assert_eq!(p.parse(br#"{"result":null,"id":1}"#, &mut m), None);
        assert_eq!(p.parse(br#"{"stream":"xxxusdt@trade","data":{}}"#, &mut m), None);
    }
}
