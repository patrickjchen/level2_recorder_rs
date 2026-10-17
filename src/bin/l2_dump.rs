//! l2_dump — print an .l2bin recording as CSV (same columns as the C++ tool).
//!
//!   l2_dump FILE [--limit N] [--header-only]
//!
//! symbol,exch_ts_ms,recv_ts_us,latency_us,endpoint,seq,type,side,price,quantity

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};

use clap::Parser;
use l2_recorder::record::{format_fixed8, FileHeader, Record, FORMAT_VERSION, MAGIC};

#[derive(Parser)]
#[command(about = "Print an .l2bin recording as CSV")]
struct Cli {
    file: String,
    #[arg(long)]
    limit: Option<u64>,
    #[arg(long)]
    header_only: bool,
}

fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(&cli) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn run(cli: &Cli) -> Result<(), String> {
    let f = File::open(&cli.file).map_err(|e| format!("{}: {e}", cli.file))?;
    let mut r = BufReader::with_capacity(1 << 20, f);

    let mut hb = [0u8; std::mem::size_of::<FileHeader>()];
    r.read_exact(&mut hb).map_err(|_| format!("{}: truncated header", cli.file))?;
    let h: FileHeader = bytemuck::pod_read_unaligned(&hb);
    if h.magic != MAGIC {
        return Err(format!("{}: not an l2bin file", cli.file));
    }
    if h.version != FORMAT_VERSION || h.record_size as usize != std::mem::size_of::<Record>() {
        return Err(format!("{}: unsupported version {} / record size {}", cli.file, h.version, h.record_size));
    }
    let mut symbols = Vec::with_capacity(h.n_symbols as usize);
    let mut name = vec![0u8; h.symbol_name_len as usize];
    for _ in 0..h.n_symbols {
        r.read_exact(&mut name).map_err(|_| "truncated header".to_string())?;
        let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
        symbols.push(String::from_utf8_lossy(&name[..end]).into_owned());
    }

    let out = io::stdout();
    let mut w = BufWriter::with_capacity(1 << 20, out.lock());

    if cli.header_only {
        writeln!(w, "version={} record_size={} n_symbols={} start_ts_us={}", h.version, h.record_size, h.n_symbols, h.start_ts_us).ok();
        for (i, s) in symbols.iter().enumerate() {
            writeln!(w, "  [{i}] {s}").ok();
        }
        return Ok(());
    }

    writeln!(w, "symbol,exch_ts_ms,recv_ts_us,latency_us,endpoint,seq,type,side,price,quantity").ok();
    let mut rb = [0u8; std::mem::size_of::<Record>()];
    let mut n = 0u64;
    while cli.limit.map_or(true, |l| n < l) {
        if r.read_exact(&mut rb).is_err() {
            break;
        }
        let rec: Record = bytemuck::pod_read_unaligned(&rb);
        let sym = symbols.get(rec.symbol_id as usize).map(String::as_str).unwrap_or("?");
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
        .map_err(|e| e.to_string())?;
        n += 1;
    }
    w.flush().ok();
    Ok(())
}
