//! Persister thread: record queue -> `.l2bin` (FileHeader + symbol table +
//! Records) through a 1 MiB buffered writer, in batches. Optionally also
//! drains the raw queue into a "<recv_ts_us>\t<json>\n" dump. Runs until
//! every input queue is closed and drained, then flushes.
//!
//! Long runs:
//! * **Rotation** (`rotate_s`): at every wall-clock boundary (e.g. 00:00 UTC
//!   for 86400) the current files are flushed, fsynced and closed, and new
//!   ones opened. Each `.l2bin` carries its own header + symbol table, so
//!   every file stands alone for `l2_dump` / `l2_to_csv`. Rotated names are
//!   `<stem>.<YYYYMMDDTHHMMSSZ>.<ext>` (open time, UTC) and an existing file
//!   is never truncated: on a name clash a `-1`, `-2`, ... suffix is added,
//!   so a supervisor restart cannot clobber data.
//! * **Periodic flush** (`flush_interval_ms`) bounds what a crash or
//!   SIGKILL can lose to roughly that interval instead of a full 1 MiB
//!   buffer. fsync happens on rotation and at exit.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, ErrorKind, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::queue::Receiver;
use crate::reader::RawMsg;
use crate::record::{FileHeader, Record, FORMAT_VERSION, MAGIC, SYMBOL_NAME_LEN};
use crate::stats::Live;
use crate::time::{utc_stamp, wall_us};

pub struct Persister<'a> {
    pub out_path: String,
    pub raw_path: String, // empty => disabled
    pub symbols: &'a [String],
    pub input: Receiver<Record>,
    pub raw_input: Option<Receiver<RawMsg>>,
    pub running: &'a AtomicBool,
    pub live: &'a Live,
    /// 0 => single file at exactly `out_path`.
    pub rotate_s: u64,
    pub flush_interval: Duration,
}

pub struct PersistResult {
    pub records: usize,
    pub raw_lines: usize,
    /// Every `.l2bin` written, in order.
    pub files: Vec<String>,
}

struct Sink {
    path: String,
    w: BufWriter<File>,
}

impl Sink {
    fn flush(&mut self) -> Result<(), String> {
        self.w.flush().map_err(|e| format!("flush failed: {}: {e}", self.path))
    }
    /// flush + fsync: used on rotation and at exit.
    fn sync(&mut self) -> Result<(), String> {
        self.flush()?;
        self.w.get_ref().sync_all().map_err(|e| format!("fsync failed: {}: {e}", self.path))
    }
}

/// `data/x.l2bin` -> (`data/x`, `.l2bin`); `data/raw` -> (`data/raw`, ``).
/// Only the file name is split, so a dotted directory is left alone.
fn split_ext(path: &str) -> (String, String) {
    let p = Path::new(path);
    let name = p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => {
            let cut = path.len() - ext.len() - 1;
            (path[..cut].to_string(), format!(".{ext}"))
        }
        _ => (path.to_string(), String::new()),
    }
}

/// `data/x.l2bin` + `20261019T134205Z` -> `data/x.20261019T134205Z.l2bin`.
pub fn rotated_path(base: &str, stamp: &str) -> String {
    let (stem, ext) = split_ext(base);
    format!("{stem}.{stamp}{ext}")
}

impl<'a> Persister<'a> {
    pub fn run(mut self) -> Result<PersistResult, String> {
        let r = self.run_inner();
        if r.is_err() {
            self.running.store(false, Ordering::Relaxed);
        }
        r
    }

    /// Opens a file for this stage. Rotating mode never overwrites; single-file
    /// mode keeps the historical behaviour (truncate `out`).
    fn open(&self, base: &str) -> Result<Sink, String> {
        if self.rotate_s == 0 {
            let f = File::create(base).map_err(|e| format!("could not open {base}: {e}"))?;
            return Ok(Sink { path: base.to_string(), w: BufWriter::with_capacity(1 << 20, f) });
        }
        let stamped = rotated_path(base, &utc_stamp(wall_us()));
        let (stem, ext) = split_ext(&stamped);
        let mut candidate = stamped.clone();
        for n in 1..1000 {
            match OpenOptions::new().write(true).create_new(true).open(&candidate) {
                Ok(f) => return Ok(Sink { path: candidate, w: BufWriter::with_capacity(1 << 20, f) }),
                Err(e) if e.kind() == ErrorKind::AlreadyExists => candidate = format!("{stem}-{n}{ext}"),
                Err(e) => return Err(format!("could not open {candidate}: {e}")),
            }
        }
        Err(format!("could not find a free name for {stamped}"))
    }

    fn open_records(&self, files: &mut Vec<String>) -> Result<Sink, String> {
        let mut s = self.open(&self.out_path)?;
        self.write_header(&mut s.w, &s.path)?;
        files.push(s.path.clone());
        self.live.files.store(files.len(), Ordering::Relaxed);
        Ok(s)
    }

    fn next_boundary_us(&self, now_us: i64) -> i64 {
        let period = self.rotate_s as i64 * 1_000_000;
        now_us - now_us.rem_euclid(period) + period
    }

    fn run_inner(&mut self) -> Result<PersistResult, String> {
        let mut files = Vec::new();
        let mut rec = self.open_records(&mut files)?;
        let mut raw = if self.raw_path.is_empty() { None } else { Some(self.open(&self.raw_path)?) };

        let mut next_rotate_us = if self.rotate_s > 0 { self.next_boundary_us(wall_us()) } else { i64::MAX };
        let mut last_flush = Instant::now();
        let mut dirty = false;

        const BATCH: usize = 1024;
        let mut batch: Vec<Record> = Vec::with_capacity(BATCH);
        let mut records = 0usize;
        let mut raw_lines = 0usize;

        loop {
            // Read the closed flags BEFORE draining.
            let closed_rec = self.input.is_closed();
            let closed_raw = self.raw_input.as_ref().is_none_or(|q| q.is_closed());

            batch.clear();
            while batch.len() < BATCH {
                match self.input.pop() {
                    Some(r) => batch.push(r),
                    None => break,
                }
            }
            if !batch.is_empty() {
                rec.w
                    .write_all(bytemuck::cast_slice(&batch))
                    .map_err(|e| format!("write failed: {}: {e}", rec.path))?;
                records += batch.len();
                dirty = true;
                self.live.records.store(records, Ordering::Relaxed);
            }

            let mut got_raw = false;
            if let (Some(q), Some(rw)) = (self.raw_input.as_mut(), raw.as_mut()) {
                while let Some(rm) = q.pop() {
                    write!(rw.w, "{}\t", rm.recv_ts_us)
                        .and_then(|_| rw.w.write_all(&rm.data))
                        .and_then(|_| rw.w.write_all(b"\n"))
                        .map_err(|e| format!("write failed: {}: {e}", rw.path))?;
                    raw_lines += 1;
                    got_raw = true;
                    dirty = true;
                }
            }

            let now_us = wall_us();
            if now_us >= next_rotate_us {
                rec.sync()?;
                if let Some(rw) = raw.as_mut() {
                    rw.sync()?;
                }
                let old = std::mem::replace(&mut rec, self.open_records(&mut files)?);
                drop(old);
                if raw.is_some() {
                    raw = Some(self.open(&self.raw_path)?);
                }
                eprintln!("\n[persist] rotated -> {}", rec.path);
                next_rotate_us = self.next_boundary_us(now_us);
                last_flush = Instant::now();
                dirty = false;
            } else if dirty && last_flush.elapsed() >= self.flush_interval {
                rec.flush()?;
                if let Some(rw) = raw.as_mut() {
                    rw.flush()?;
                }
                last_flush = Instant::now();
                dirty = false;
            }

            if batch.is_empty() && !got_raw {
                if closed_rec && closed_raw {
                    break;
                }
                std::thread::sleep(Duration::from_micros(100));
            }
        }

        rec.sync()?;
        if let Some(rw) = raw.as_mut() {
            rw.sync()?;
        }
        Ok(PersistResult { records, raw_lines, files })
    }

    fn write_header(&self, w: &mut impl Write, path: &str) -> Result<(), String> {
        let h = FileHeader {
            magic: MAGIC,
            version: FORMAT_VERSION,
            record_size: std::mem::size_of::<Record>() as u32,
            n_symbols: self.symbols.len() as u32,
            symbol_name_len: SYMBOL_NAME_LEN,
            start_ts_us: wall_us(),
        };
        w.write_all(bytemuck::bytes_of(&h)).map_err(|e| format!("write failed: {path}: {e}"))?;
        for s in self.symbols {
            let mut name = [0u8; SYMBOL_NAME_LEN as usize];
            let n = s.len().min(name.len() - 1);
            name[..n].copy_from_slice(&s.as_bytes()[..n]);
            w.write_all(&name).map_err(|e| format!("write failed: {path}: {e}"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotated_path_inserts_stamp_before_extension() {
        assert_eq!(rotated_path("data/market_data.l2bin", "20261019T000000Z"), "data/market_data.20261019T000000Z.l2bin");
        assert_eq!(rotated_path("market_data.l2bin", "S"), "market_data.S.l2bin");
        assert_eq!(rotated_path("data/raw", "S"), "data/raw.S");
        assert_eq!(rotated_path("/abs/a.b.jsonl", "S"), "/abs/a.b.S.jsonl");
        assert_eq!(rotated_path("dir.v2/raw", "S"), "dir.v2/raw.S");
        assert_eq!(split_ext("data/x.S.l2bin"), ("data/x.S".to_string(), ".l2bin".to_string()));
    }
}
