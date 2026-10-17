//! Fixed-point helpers, the 48-byte binary `Record`, and the `.l2bin` file
//! header. Byte-identical to the C++ `src/record.h`.

use bytemuck::{Pod, Zeroable};

/// 1e8: Binance prints 8 decimals, so int64 * 1e8 is exact.
pub const FIXED_SCALE: i64 = 100_000_000;

/// "73510.86000000" -> 7351086000000. No sign handling (Binance never sends
/// one); more than 8 fractional digits are truncated.
pub fn parse_fixed8(s: &str) -> i64 {
    let b = s.as_bytes();
    let mut i = 0;
    let mut ip: i64 = 0;
    while i < b.len() && b[i] != b'.' {
        ip = ip * 10 + (b[i].wrapping_sub(b'0')) as i64;
        i += 1;
    }
    let mut fp: i64 = 0;
    let mut nd = 0;
    if i < b.len() {
        i += 1; // skip '.'
        while i < b.len() && nd < 8 {
            fp = fp * 10 + (b[i].wrapping_sub(b'0')) as i64;
            i += 1;
            nd += 1;
        }
    }
    while nd < 8 {
        fp *= 10;
        nd += 1;
    }
    ip * FIXED_SCALE + fp
}

pub fn format_fixed8(v: i64) -> String {
    let neg = v < 0;
    let v = v.abs();
    format!("{}{}.{:08}", if neg { "-" } else { "" }, v / FIXED_SCALE, v % FIXED_SCALE)
}

pub const REC_TRADE: u8 = b'T';
pub const REC_ADD: u8 = b'A';
pub const REC_CANCEL: u8 = b'C';
/// Stale marker: no accepted message for this symbol for `stale_after_ms`.
/// exch_ts_ms/seq = last seen for the symbol, recv_ts_us = detection time,
/// side = '-', price = 0, qty = threshold in ms (fixed-point, so it dumps as
/// "2000.00000000"). Recovery is implicit: the next normal record.
pub const REC_STALE: u8 = b'X';
pub const SIDE_BUY: u8 = b'B';
pub const SIDE_SELL: u8 = b'S';

/// 48 bytes, naturally aligned, host-endian (little-endian on x86-64).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct Record {
    pub exch_ts_ms: i64, // trade: T (trade time); depth: E (event time)
    pub recv_ts_us: i64, // local recv of the carrying frame
    pub price: i64,      // fixed-point 1e8
    pub qty: i64,        // fixed-point 1e8 (0 => cancel)
    pub seq: u64,        // trade id (t) or depth final update id (u)
    pub symbol_id: u16,  // index into the file's symbol table
    pub rtype: u8,       // REC_*
    pub side: u8,        // SIDE_* (trade: 'S' means buyer-is-maker, as in the Python)
    pub endpoint: u8,    // connection that delivered the accepted copy
    pub pad: [u8; 3],
}
const _: () = assert!(std::mem::size_of::<Record>() == 48, "Record must stay 48 bytes");

pub const MAGIC: [u8; 8] = *b"L2BIN\0\0\0";
pub const FORMAT_VERSION: u32 = 1;
pub const SYMBOL_NAME_LEN: u32 = 16; // NUL-padded lowercase names

/// File layout: FileHeader | n_symbols * symbol_name_len bytes | Record...
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct FileHeader {
    pub magic: [u8; 8],
    pub version: u32,
    pub record_size: u32,
    pub n_symbols: u32,
    pub symbol_name_len: u32,
    pub start_ts_us: i64, // wall clock when the file was opened
}
const _: () = assert!(std::mem::size_of::<FileHeader>() == 32);

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed8_roundtrip() {
        assert_eq!(parse_fixed8("73510.86000000"), 7_351_086_000_000);
        assert_eq!(parse_fixed8("0.00000000"), 0);
        assert_eq!(parse_fixed8("1"), FIXED_SCALE);
        assert_eq!(parse_fixed8("0.5"), 50_000_000);
        assert_eq!(format_fixed8(7_351_086_000_000), "73510.86000000");
        assert_eq!(format_fixed8(150_000), "0.00150000");
    }
}
