//! l2_dump — print an .l2bin recording as CSV on stdout (same columns as
//! the C++ tool). Use `l2_to_csv` to write a .csv file directly.
//!
//!   l2_dump FILE [--limit N] [--header-only]

use std::io::{self, BufWriter, Write};

use clap::Parser;
use l2_recorder::l2bin::{write_csv, L2binReader};

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
    let mut r = L2binReader::open(&cli.file)?;
    let out = io::stdout();
    let mut w = BufWriter::with_capacity(1 << 20, out.lock());

    if cli.header_only {
        let h = &r.header;
        writeln!(w, "version={} record_size={} n_symbols={} start_ts_us={}", h.version, h.record_size, h.n_symbols, h.start_ts_us).ok();
        for (i, s) in r.symbols.iter().enumerate() {
            writeln!(w, "  [{i}] {s}").ok();
        }
        return Ok(());
    }
    write_csv(&mut r, &mut w, cli.limit).map_err(|e| e.to_string())?;
    Ok(())
}
