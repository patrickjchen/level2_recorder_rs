//! Clock helpers: wall clock for timestamps that go into the file, steady
//! clock for intervals (staleness, pings).

use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

static START: OnceLock<Instant> = OnceLock::new();

/// Microseconds since the Unix epoch (system_clock).
#[inline]
pub fn wall_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// Monotonic microseconds since first use (steady_clock). Only differences
/// are meaningful.
#[inline]
pub fn steady_us() -> i64 {
    START.get_or_init(Instant::now).elapsed().as_micros() as i64
}
