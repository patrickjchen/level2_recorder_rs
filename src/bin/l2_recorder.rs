//! l2_recorder — multi-threaded Binance L2 recorder.
//!
//! One reader thread per feed, one parser, one persister, all joined by
//! lock-free SPSC queues (every queue has exactly one producer and consumer):
//!
//!   [reader0] feed 0 --q1[0]: RawMsg--+
//!                                     +--> [parser] --q2: Record--> [persist]
//!   [reader1] feed 1 --q1[1]: RawMsg--+       |
//!                                             +--q3: RawMsg (optional raw dump)-->
//!
//! Config comes from a JSON file (default ./config.json); a few CLI flags
//! override it. See README.md.

use std::io::{IsTerminal, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser as ClapParser;

use l2_recorder::config::{build_stream_url, Config};
use l2_recorder::parse_stage::ParseStage;
use l2_recorder::persist::{PersistResult, Persister};
use l2_recorder::queue::{channel, QueueGauge};
use l2_recorder::reader::{RawMsg, Reader};
use l2_recorder::record::Record;
use l2_recorder::stats::{print_pcts, Live, ParseStats, ReaderStats, Reservoir};
use l2_recorder::time::{utc_stamp, wall_us};
use l2_recorder::ws_client::WsClient;

#[derive(ClapParser)]
#[command(about = "Binance L2 recorder: N reader threads -> parse/dedup -> binary persist")]
struct Cli {
    /// JSON config (other flags override the corresponding keys)
    #[arg(long, default_value = "config.json")]
    config: String,
    /// Comma-separated symbols, e.g. btcusdt,ethusdt
    #[arg(long, alias = "symbol")]
    symbols: Option<String>,
    /// Seconds to run; 0 = until SIGINT/SIGTERM
    #[arg(long)]
    duration: Option<u64>,
    #[arg(long)]
    out: Option<String>,
    #[arg(long)]
    raw_out: Option<String>,
}

fn main() {
    std::process::exit(run());
}

fn load_config(cli: &Cli) -> Result<Config, String> {
    let mut cfg = if Path::new(&cli.config).exists() {
        Config::load(&cli.config)?
    } else if cli.config != "config.json" {
        return Err(format!("config not found: {}", cli.config));
    } else {
        eprintln!("config.json not found, using built-in defaults");
        Config::default()
    };
    if let Some(s) = &cli.symbols {
        cfg.symbols = s.split(',').filter(|x| !x.is_empty()).map(str::to_string).collect();
    }
    if let Some(d) = cli.duration {
        cfg.duration_s = d;
    }
    if let Some(o) = &cli.out {
        cfg.out = o.clone();
    }
    if let Some(r) = &cli.raw_out {
        cfg.raw_out = r.clone();
    }
    cfg.normalize()?;
    Ok(cfg)
}

fn pin_to_cpu(cfg: &Config, slot: usize, name: &str) {
    let Some(&cpu) = cfg.pin_cpus.get(slot) else { return };
    if cpu < 0 {
        return;
    }
    if !core_affinity::set_for_current(core_affinity::CoreId { id: cpu as usize }) {
        eprintln!("[{name}] could not pin to cpu {cpu}");
    }
}

fn run() -> i32 {
    // rustls needs exactly one process-level crypto provider; select ring.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cli = Cli::parse();
    let cfg = match load_config(&cli) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return 1;
        }
    };

    let running = Arc::new(AtomicBool::new(true));
    {
        let r = running.clone();
        if let Err(e) = ctrlc::set_handler(move || r.store(false, Ordering::Relaxed)) {
            eprintln!("signal handler: {e}");
        }
    }

    for p in [&cfg.out, &cfg.raw_out] {
        if p.is_empty() {
            continue;
        }
        if let Some(dir) = Path::new(p).parent() {
            if !dir.as_os_str().is_empty() {
                std::fs::create_dir_all(dir).ok();
            }
        }
    }

    // --- Connect: first `max_endpoints` candidates that complete the handshake.
    let mut conns: Vec<WsClient> = Vec::new();
    for base in &cfg.endpoints {
        if conns.len() >= cfg.max_endpoints {
            break;
        }
        let url = build_stream_url(base, &cfg.symbols);
        match WsClient::connect(&url, Duration::from_secs(cfg.connect_timeout_s)) {
            Ok(ws) => {
                println!("feed {}: connected {base}", conns.len());
                conns.push(ws);
            }
            Err(e) => eprintln!("feed skip: {base} ({e})"),
        }
    }
    if conns.is_empty() {
        eprintln!("no endpoint reachable");
        return 1;
    }
    let n_feeds = conns.len();
    // duration 0 (or one too large for Instant) => run until signalled.
    let deadline = if cfg.duration_s == 0 { None } else { Instant::now().checked_add(Duration::from_secs(cfg.duration_s)) };
    println!(
        "recording {} symbol(s) on {} feed(s) {} -> {}{}",
        cfg.symbols.len(),
        n_feeds,
        match deadline {
            Some(_) => format!("for {} s", cfg.duration_s),
            None => "until stopped (SIGINT/SIGTERM)".to_string(),
        },
        cfg.out,
        if cfg.rotate_s > 0 { format!(" (rotating every {} s)", cfg.rotate_s) } else { String::new() }
    );

    // --- Pipeline: one reader thread + one queue per feed.
    let live = Live::new(n_feeds, cfg.symbols.len());
    let stale_after_us = cfg.stale_after_ms as i64 * 1000;

    let mut q1_rx = Vec::new();
    let mut q1_gauges: Vec<Arc<QueueGauge>> = Vec::new();
    let mut readers = Vec::new();
    for (i, ws) in conns.into_iter().enumerate() {
        let (tx, rx, gauge) = channel::<RawMsg>(cfg.queue_capacity);
        q1_rx.push(rx);
        q1_gauges.push(gauge);
        readers.push(Reader {
            ws,
            id: i as u8,
            out: tx,
            running: &running,
            live: &live,
            deadline,
            ping_interval: Duration::from_millis(cfg.ping_interval_ms),
            stale_after_us,
            busy_poll: cfg.reader_busy_poll,
            connect_timeout: Duration::from_secs(cfg.connect_timeout_s),
            // Stagger so redundant feeds never rotate together.
            reconnect_after: (cfg.reconnect_after_s > 0)
                .then(|| Duration::from_secs(cfg.reconnect_after_s + i as u64 * cfg.reconnect_stagger_s)),
            backoff_max: Duration::from_secs(cfg.reconnect_backoff_max_s),
            max_samples: cfg.max_samples,
        });
    }
    let (q2_tx, q2_rx, q2_gauge) = channel::<Record>(cfg.queue_capacity);
    let (q3_tx, q3_rx) = if cfg.raw_out.is_empty() {
        (None, None)
    } else {
        let (tx, rx, _) = channel::<RawMsg>(cfg.queue_capacity);
        (Some(tx), Some(rx))
    };

    let parse = ParseStage {
        symbols: &cfg.symbols,
        inputs: q1_rx,
        out: q2_tx,
        raw_out: q3_tx,
        running: &running,
        live: &live,
        stale_after_us,
        max_samples: cfg.max_samples,
    };
    let persist = Persister {
        out_path: cfg.out.clone(),
        raw_path: cfg.raw_out.clone(),
        symbols: &cfg.symbols,
        input: q2_rx,
        raw_input: q3_rx,
        running: &running,
        live: &live,
        rotate_s: cfg.rotate_s,
        flush_interval: Duration::from_millis(cfg.flush_interval_ms),
    };

    // Thread slots for pin_cpus: [reader0..readerN-1, parser, persist].
    let (reader_stats, parse_stats, persist_result): (Vec<ReaderStats>, ParseStats, Result<PersistResult, String>) =
        thread::scope(|s| {
            let cfg_ref = &cfg;
            let h_persist = thread::Builder::new()
                .name("l2-persist".into())
                .spawn_scoped(s, move || {
                    pin_to_cpu(cfg_ref, n_feeds + 1, "l2-persist");
                    persist.run()
                })
                .expect("spawn persist");
            let h_parse = thread::Builder::new()
                .name("l2-parse".into())
                .spawn_scoped(s, move || {
                    pin_to_cpu(cfg_ref, n_feeds, "l2-parse");
                    parse.run()
                })
                .expect("spawn parse");
            let h_readers: Vec<_> = readers
                .into_iter()
                .enumerate()
                .map(|(i, r)| {
                    let name = format!("l2-reader{i}");
                    thread::Builder::new()
                        .name(name.clone())
                        .spawn_scoped(s, move || {
                            pin_to_cpu(cfg_ref, i, &name);
                            r.run()
                        })
                        .expect("spawn reader")
                })
                .collect();

            // Progress line from main; the stages never touch stdout. On a
            // terminal: a \r line twice a second. Redirected to a log: one
            // timestamped line per status_interval_s.
            let tty = std::io::stdout().is_terminal();
            let status_every = if tty { Duration::from_millis(500) } else { Duration::from_secs(cfg.status_interval_s) };
            let mut next_status = Instant::now();
            while running.load(Ordering::Relaxed) && deadline.is_none_or(|d| Instant::now() < d) {
                if Instant::now() >= next_status {
                    print_status(&live, &q1_gauges, &q2_gauge, n_feeds, tty);
                    next_status = Instant::now() + status_every;
                }
                thread::sleep(Duration::from_millis(200));
            }

            // Shutdown cascades: each reader drops its sender -> parser exits
            // when all are closed and drained, drops q2/q3 -> persister
            // drains and exits.
            let rs: Vec<ReaderStats> = h_readers.into_iter().map(|h| h.join().expect("reader thread")).collect();
            let ps = h_parse.join().expect("parse thread");
            let pr = h_persist.join().expect("persist thread");
            (rs, ps, pr)
        });

    println!("\ndone.");
    let (records, raw_lines, files, persist_ok) = match persist_result {
        Ok(r) => (r.records, r.raw_lines, r.files, true),
        Err(e) => {
            eprintln!("[l2-persist] fatal: {e}");
            (0, 0, Vec::new(), false)
        }
    };
    let q1_peak = q1_gauges.iter().map(|g| g.peak.load(Ordering::Relaxed)).max().unwrap_or(0);
    print_summary(&cfg, &reader_stats, &parse_stats, records, raw_lines, &files, q1_peak, q2_gauge.peak.load(Ordering::Relaxed));

    if persist_ok {
        0
    } else {
        2
    }
}

fn print_status(live: &Live, q1_gauges: &[Arc<QueueGauge>], q2_gauge: &QueueGauge, n_feeds: usize, tty: bool) {
    let stale_feeds = live.feed_stale.iter().filter(|f| f.load(Ordering::Relaxed)).count();
    let down_feeds = live.feed_down.iter().filter(|f| f.load(Ordering::Relaxed)).count();
    let stale_syms = live.symbol_stale.iter().filter(|f| f.load(Ordering::Relaxed)).count();
    let q1: usize = q1_gauges.iter().map(|g| g.len.load(Ordering::Relaxed)).sum();
    let body = format!(
        "feeds={}/{}{}{}  q1={}  q2={}  accepted={}  records={}  reconnects={}  files={}{}",
        n_feeds - down_feeds,
        n_feeds,
        if down_feeds > 0 { " [DOWN]" } else { "" },
        if stale_feeds > 0 { " [STALE]" } else { "" },
        q1,
        q2_gauge.len.load(Ordering::Relaxed),
        live.accepted.load(Ordering::Relaxed),
        live.records.load(Ordering::Relaxed),
        live.reconnects.load(Ordering::Relaxed),
        live.files.load(Ordering::Relaxed),
        if stale_syms > 0 { "  [symbols STALE]" } else { "" }
    );
    if tty {
        print!("\r{body}   ");
    } else {
        println!("{} {body}", utc_stamp(wall_us()));
    }
    std::io::stdout().flush().ok();
}

fn print_reservoir(label: &str, r: &Reservoir, indent: &str) {
    if r.is_empty() {
        return;
    }
    let p = r.percentiles();
    println!(
        "{indent}{label}: n={}{} min={} p50={} p90={} p99={} max={} us",
        p.n,
        if r.is_estimated() { format!(" (est. from {} samples)", r.sampled()) } else { String::new() },
        p.min,
        p.p50,
        p.p90,
        p.p99,
        p.max
    );
}

#[allow(clippy::too_many_arguments)]
fn print_summary(
    cfg: &Config,
    rs: &[ReaderStats],
    ps: &ParseStats,
    records: usize,
    raw_lines: usize,
    files: &[String],
    q1_peak: usize,
    q2_peak: usize,
) {
    println!("\n=== Feeds ({}) ===", rs.len());
    for (i, r) in rs.iter().enumerate() {
        let a = &ps.feeds[i];
        println!("  [{i}] {}", r.url);
        println!(
            "      frames={} bytes={}  accepted(first)={}  dups={}  misordered={}{}",
            r.frames,
            r.bytes,
            a.accepted,
            a.dups,
            a.misordered,
            if r.connected { "" } else { "  [DOWN at exit]" }
        );
        if r.disconnects + r.rotations + r.connect_failures > 0 {
            println!(
                "      connections: {} unplanned disconnect(s), {} planned rotation(s), {} failed connect attempt(s)",
                r.disconnects, r.rotations, r.connect_failures
            );
        }
        if r.stale.episodes > 0 {
            println!(
                "      STALE: {} episode(s), total {} ms, longest {} ms{}",
                r.stale.episodes,
                r.stale.total_ms,
                r.stale.max_ms,
                if r.stale.stale { " (stale at exit)" } else { "" }
            );
        }
        print_reservoir("PING/PONG RTT", &r.rtts, "      ");
        print_reservoir("lag behind winning feed (dups)", &a.dup_lag, "      ");
    }

    println!("\n=== Pipeline ===");
    println!("  symbols   : {}", cfg.symbols.len());
    println!(
        "  accepted  : {} msgs (trades={}, depth={} -> levels={})",
        ps.msgs, ps.trades, ps.depth, ps.levels
    );
    println!(
        "  unknown   : {}   sequence gaps: {}   stale markers: {}",
        ps.unknown, ps.gaps, ps.stale_markers
    );
    for (i, s) in ps.symbols.iter().enumerate() {
        print!("  {:<12} msgs={}", cfg.symbols[i], s.msgs);
        if s.stale.episodes > 0 {
            print!(
                "  STALE: {} episode(s), total {} ms, longest {} ms{}",
                s.stale.episodes,
                s.stale.total_ms,
                s.stale.max_ms,
                if s.stale.stale { " (stale at exit)" } else { "" }
            );
        }
        println!();
    }
    println!("  stale threshold: {} ms", cfg.stale_after_ms);
    println!("  records   : {} written ({} bytes/rec) in {} file(s)", records, std::mem::size_of::<Record>(), files.len());
    for f in files {
        println!("              {f}");
    }
    if !cfg.raw_out.is_empty() {
        println!("  raw lines : {} -> {}", raw_lines, cfg.raw_out);
    }
    println!(
        "  queue peak: q1={} (max over feeds) q2={} (capacity {})",
        q1_peak, q2_peak, cfg.queue_capacity
    );
    println!(
        "  threads   : {} readers{} + parser + persist{}",
        rs.len(),
        if cfg.reader_busy_poll { " (busy-poll)" } else { "" },
        if cfg.pin_cpus.is_empty() { "" } else { ", pinned" }
    );

    let est = |r: &Reservoir| if r.is_estimated() { format!(" [estimated from {} of {} samples]", r.sampled(), r.total()) } else { String::new() };
    println!("\n--- Latency (recv - exchange T/E, microseconds; skew-contaminated){} ---", est(&ps.latencies));
    print_pcts(&ps.latencies.percentiles(), false, "  ");
    println!("\n--- Inter-arrival jitter (recv_dt - exch_dt, skew-free){} ---", est(&ps.jitters));
    let j = ps.jitters.percentiles();
    print_pcts(&j, true, "  ");
    if j.n > 0 {
        println!("  spread (p99 - p1): {} us", j.p99 - j.p1);
    }
}
