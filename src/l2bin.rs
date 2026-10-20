//! Reading `.l2bin` files and writing them out as CSV. Shared by `l2_dump`
//! (stdout) and `l2_to_csv` (file).

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::record::{format_fixed8, FileHeader, Record, FORMAT_VERSION, MAGIC};

pub const CSV_HEADER: &str = "symbol,exch_ts_ms,recv_ts_us,latency_us,endpoint,seq,type,side,price,quantity";

pub struct L2binReader<R: Read> {
    pub header: FileHeader,
    pub symbols: Vec<String>,
    inner: R,
}

impl L2binReader<BufReader<File>> {
    pub fn open(path: &str) -> Result<Self, String> {
        let f = File::open(path).map_err(|e| format!("{path}: {e}"))?;
        Self::new(BufReader::with_capacity(1 << 20, f)).map_err(|e| format!("{path}: {e}"))
    }
}

impl<R: Read> L2binReader<R> {
    pub fn new(mut inner: R) -> Result<Self, String> {
        let mut hb = [0u8; std::mem::size_of::<FileHeader>()];
        inner.read_exact(&mut hb).map_err(|_| "truncated header".to_string())?;
        let header: FileHeader = bytemuck::pod_read_unaligned(&hb);
        if header.magic != MAGIC {
            return Err("not an l2bin file".into());
        }
        if header.version != FORMAT_VERSION || header.record_size as usize != std::mem::size_of::<Record>() {
            return Err(format!("unsupported version {} / record size {}", header.version, header.record_size));
        }
        let mut symbols = Vec::with_capacity(header.n_symbols as usize);
        let mut name = vec![0u8; header.symbol_name_len as usize];
        for _ in 0..header.n_symbols {
            inner.read_exact(&mut name).map_err(|_| "truncated header".to_string())?;
            let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
            symbols.push(String::from_utf8_lossy(&name[..end]).into_owned());
        }
        Ok(L2binReader { header, symbols, inner })
    }

    /// Next record, or `None` at EOF (a trailing partial record is ignored).
    pub fn next_record(&mut self) -> Option<Record> {
        let mut rb = [0u8; std::mem::size_of::<Record>()];
        self.inner.read_exact(&mut rb).ok()?;
        Some(bytemuck::pod_read_unaligned(&rb))
    }

    pub fn symbol(&self, id: u16) -> &str {
        self.symbols.get(id as usize).map(String::as_str).unwrap_or("?")
    }
}

/// Writes one CSV row for `rec`.
pub fn write_csv_row(w: &mut impl Write, sym: &str, rec: &Record) -> io::Result<()> {
    writeln!(
        w,
        "{},{},{},{},{},{},{},{},{},{}",
        sym,
        rec.exch_ts_ms,
        rec.recv_ts_us,
        rec.recv_ts_us - rec.exch_ts_ms * 1000,
        rec.endpoint,
        rec.seq,
        rec.rtype as char,
        rec.side as char,
        format_fixed8(rec.price),
        format_fixed8(rec.qty)
    )
}

/// Converts an entire `.l2bin` to CSV on `w`. Returns the number of rows.
pub fn write_csv<R: Read>(reader: &mut L2binReader<R>, w: &mut impl Write, limit: Option<u64>) -> io::Result<u64> {
    writeln!(w, "{CSV_HEADER}")?;
    let mut n = 0u64;
    while limit.is_none_or(|l| n < l) {
        let Some(rec) = reader.next_record() else { break };
        write_csv_row(w, reader.symbol(rec.symbol_id), &rec)?;
        n += 1;
    }
    w.flush()?;
    Ok(n)
}

/// Converts `in_path` to `out_path` (a file). Returns rows written.
pub fn convert_file(in_path: &str, out_path: &str, limit: Option<u64>) -> Result<u64, String> {
    let mut r = L2binReader::open(in_path)?;
    if let Some(dir) = Path::new(out_path).parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir).map_err(|e| format!("{out_path}: {e}"))?;
        }
    }
    let f = File::create(out_path).map_err(|e| format!("{out_path}: {e}"))?;
    let mut w = BufWriter::with_capacity(1 << 20, f);
    write_csv(&mut r, &mut w, limit).map_err(|e| format!("{out_path}: {e}"))
}

/// "data/x.l2bin" -> "data/x.csv"; any other extension gets ".csv" appended.
pub fn default_csv_path(in_path: &str) -> String {
    match in_path.strip_suffix(".l2bin") {
        Some(stem) => format!("{stem}.csv"),
        None => format!("{in_path}.csv"),
    }
}
