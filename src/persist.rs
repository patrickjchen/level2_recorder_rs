//! Persister thread: record queue -> `.l2bin` (FileHeader + symbol table +
//! Records) through a 1 MiB buffered writer, in batches. Optionally also
//! drains the raw queue into a "<recv_ts_us>\t<json>\n" dump. Runs until
//! every input queue is closed and drained, then flushes.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::queue::Receiver;
use crate::reader::RawMsg;
use crate::record::{FileHeader, Record, FORMAT_VERSION, MAGIC, SYMBOL_NAME_LEN};
use crate::stats::Live;
use crate::time::wall_us;

pub struct Persister<'a> {
    pub out_path: String,
    pub raw_path: String, // empty => disabled
    pub symbols: &'a [String],
    pub input: Receiver<Record>,
    pub raw_input: Option<Receiver<RawMsg>>,
    pub running: &'a AtomicBool,
    pub live: &'a Live,
}

pub struct PersistResult {
    pub records: usize,
    pub raw_lines: usize,
}

impl<'a> Persister<'a> {
    pub fn run(mut self) -> Result<PersistResult, String> {
        let r = self.run_inner();
        if r.is_err() {
            self.running.store(false, Ordering::Relaxed);
        }
        r
    }

    fn run_inner(&mut self) -> Result<PersistResult, String> {
        let f = File::create(&self.out_path).map_err(|e| format!("could not open {}: {e}", self.out_path))?;
        let mut w = BufWriter::with_capacity(1 << 20, f);
        self.write_header(&mut w)?;

        let mut raw_w = if self.raw_path.is_empty() {
            None
        } else {
            let f = File::create(&self.raw_path).map_err(|e| format!("could not open {}: {e}", self.raw_path))?;
            Some(BufWriter::with_capacity(1 << 20, f))
        };

        const BATCH: usize = 1024;
        let mut batch: Vec<Record> = Vec::with_capacity(BATCH);
        let mut records = 0usize;
        let mut raw_lines = 0usize;

        loop {
            // Read the closed flags BEFORE draining.
            let closed_rec = self.input.is_closed();
            let closed_raw = self.raw_input.as_ref().map_or(true, |q| q.is_closed());

            batch.clear();
            while batch.len() < BATCH {
                match self.input.pop() {
                    Some(r) => batch.push(r),
                    None => break,
                }
            }
            if !batch.is_empty() {
                w.write_all(bytemuck::cast_slice(&batch)).map_err(|e| format!("write failed: {}: {e}", self.out_path))?;
                records += batch.len();
                self.live.records.store(records, Ordering::Relaxed);
            }

            let mut got_raw = false;
            if let (Some(q), Some(rw)) = (self.raw_input.as_mut(), raw_w.as_mut()) {
                while let Some(rm) = q.pop() {
                    write!(rw, "{}\t", rm.recv_ts_us).and_then(|_| rw.write_all(&rm.data)).and_then(|_| rw.write_all(b"\n"))
                        .map_err(|e| format!("write failed: {}: {e}", self.raw_path))?;
                    raw_lines += 1;
                    got_raw = true;
                }
            }

            if batch.is_empty() && !got_raw {
                if closed_rec && closed_raw {
                    break;
                }
                std::thread::sleep(Duration::from_micros(100));
            }
        }

        w.flush().map_err(|e| format!("flush failed: {}: {e}", self.out_path))?;
        if let Some(rw) = raw_w.as_mut() {
            rw.flush().map_err(|e| format!("flush failed: {}: {e}", self.raw_path))?;
        }
        Ok(PersistResult { records, raw_lines })
    }

    fn write_header(&self, w: &mut impl Write) -> Result<(), String> {
        let h = FileHeader {
            magic: MAGIC,
            version: FORMAT_VERSION,
            record_size: std::mem::size_of::<Record>() as u32,
            n_symbols: self.symbols.len() as u32,
            symbol_name_len: SYMBOL_NAME_LEN,
            start_ts_us: wall_us(),
        };
        w.write_all(bytemuck::bytes_of(&h)).map_err(|e| format!("write failed: {}: {e}", self.out_path))?;
        for s in self.symbols {
            let mut name = [0u8; SYMBOL_NAME_LEN as usize];
            let n = s.len().min(name.len() - 1);
            name[..n].copy_from_slice(&s.as_bytes()[..n]);
            w.write_all(&name).map_err(|e| format!("write failed: {}: {e}", self.out_path))?;
        }
        Ok(())
    }
}
