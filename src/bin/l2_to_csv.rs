//! l2_to_csv — convert an .l2bin recording to a CSV file.
//!
//!   l2_to_csv IN.l2bin [OUT.csv] [--limit N]
//!
//! OUT defaults to IN with the extension replaced by .csv. Columns:
//!   symbol,exch_ts_ms,recv_ts_us,latency_us,endpoint,seq,type,side,price,quantity

use clap::Parser;
use l2_recorder::l2bin::{convert_file, default_csv_path};

#[derive(Parser)]
#[command(about = "Convert an .l2bin recording to a CSV file")]
struct Cli {
    /// Input .l2bin file
    input: String,
    /// Output .csv file (default: input with .l2bin -> .csv)
    output: Option<String>,
    /// Stop after N records
    #[arg(long)]
    limit: Option<u64>,
}

fn main() {
    let cli = Cli::parse();
    let out = cli.output.clone().unwrap_or_else(|| default_csv_path(&cli.input));
    match convert_file(&cli.input, &out, cli.limit) {
        Ok(n) => println!("wrote {n} rows -> {out}"),
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    }
}
