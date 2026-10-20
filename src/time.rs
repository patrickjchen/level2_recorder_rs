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

/// `YYYYMMDDTHHMMSSZ` for a wall-clock microsecond timestamp (UTC). Used for
/// rotated file names and log lines; no chrono dependency.
pub fn utc_stamp(us: i64) -> String {
    let secs = us.div_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z", sod / 3600, (sod % 3600) / 60, sod % 60)
}

/// Days since 1970-01-01 -> (year, month, day). Howard Hinnant's algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_stamp_known_values() {
        assert_eq!(utc_stamp(0), "19700101T000000Z");
        assert_eq!(utc_stamp(1_700_000_000_000_000), "20231114T221320Z");
        assert_eq!(utc_stamp(951_782_400_000_000), "20000229T000000Z"); // leap day
        assert_eq!(utc_stamp(-1), "19691231T235959Z");
    }
}
