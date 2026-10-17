//! l2_recorder — multi-threaded Binance L2 recorder (Rust port of
//! `Extra/l2_recorder`, branch `multi-reader`).
//!
//! One reader thread per feed, one parser/dedup thread, one persister, joined
//! by lock-free SPSC queues (`rtrb`). See README.md.

pub mod config;
pub mod dedup;
pub mod parse_stage;
pub mod parser;
pub mod persist;
pub mod queue;
pub mod reader;
pub mod record;
pub mod stats;
pub mod time;
pub mod ws_client;
