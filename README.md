# l2_recorder (Rust)

Lives at `Extra/level2_recorder_rs`. Rust port of `Extra/l2_recorder` (branch
`multi-reader`): a multi-threaded
recorder for Binance L2 market data (trades + `depth@100ms` diffs). Streams
from one or two redundant WebSocket feeds, de-duplicates across them by
sequence number, converts to a fixed-size binary record, and persists. The
output file is **byte-identical** to the C++ `.l2bin`, so either project's
`l2_dump` reads either project's files.

## Architecture — one reader per feed, one parser, one persister

```
 [l2-reader0] feed 0 ──q1[0]: RawMsg──┐
                                      ├──▶ [l2-parse] ──q2: Record (48 B)──▶ [l2-persist]
 [l2-reader1] feed 1 ──q1[1]: RawMsg──┘   merge by recv_ts                   FileHeader +
  own socket, own thread                  sonic-rs (SIMD JSON)               symbol table +
  recv_ts + endpoint id                   sequence-number de-dup             batched write
                                          fixed-point binary
                                               │
                                               └──── q3: RawMsg (only if raw_out set) ──▶ raw .jsonl
```

| Thread         | File                 | Does                                                                                                   |
|----------------|----------------------|--------------------------------------------------------------------------------------------------------|
| `l2-reader<i>` | `src/reader.rs`      | Owns one `tungstenite` connection. Blocking `read()` with a 100 ms timeout (or non-blocking spin with `reader_busy_poll`), PING once a second. Stamps `recv_ts_us` right after the message is read. Pushes to its own queue. Reconnects on loss (back-off) and rotates the socket proactively before Binance's 24 h cut (make-before-break); the queue stays open throughout. |
| `l2-parse`     | `src/parse_stage.rs` | Peeks every reader queue and takes the head with the earliest `recv_ts_us`, so "first copy wins" is judged by receive time. `Parser` (`src/parser.rs`) → `Deduper` (`src/dedup.rs`) → one `Record` per trade / per book level. Latency, jitter, per-feed win/dup stats, symbol staleness. |
| `l2-persist`   | `src/persist.rs`     | Writes header + symbol table, then records in batches of 1024 through a 1 MiB `BufWriter`. Optional raw dump. Flushes every `flush_interval_ms`; rotates files at wall-clock boundaries (`rotate_s`) with fsync, never overwriting. |

Queues are `rtrb` rings wrapped in `src/queue.rs`: bounded, lock-free, exactly
one producer and one consumer each. Giving each reader its own queue keeps the
pipeline lock-free with two feeds — no MPSC queue, no shared socket loop. A
producer never drops: a full queue back-pressures (spin, then yield).
**Close = drop the sender.** Consumers read `is_closed()` *before* draining,
so a close issued after the final push can never hide that item. Shutdown
cascades reader → parser → persister through those drops.

**Merge race.** The parser compares queue *heads*. A copy received earlier on
feed B but not yet pushed when feed A's head was taken loses by one push
(microseconds). It is reported as `misordered` on the losing feed, and shows
as a small negative lag in the dup-lag percentiles.

### Crate map (C++ → Rust)

| C++                            | Rust                                         |
|--------------------------------|----------------------------------------------|
| `SpscQueue` (own ring)         | `rtrb` (`src/queue.rs` adds close + gauge)   |
| simdjson on-demand             | `sonic-rs` + serde derive, borrowed `&str`; wrapper parsed once with `data` as a `LazyValue`, payload picked by stream suffix |
| libcurl WebSocket              | `tungstenite` (sync) + `rustls` (`ring`)     |
| `std::thread` + `pthread_setname_np` | `std::thread::Builder::name` in `thread::scope` |
| `pthread_setaffinity_np`       | `core_affinity`                              |
| POD `Record` + `fwrite`        | `#[repr(C)]` + `bytemuck::Pod` + `BufWriter` |
| simdjson DOM config            | `serde_json`, `#[serde(default)]`            |
| `std::signal`                  | `ctrlc` → `AtomicBool`                       |
| hand-rolled CLI                | `clap` derive                                |

### Feeds and de-duplication

`endpoints` are tried in order; the first `max_endpoints` (default 2) that
complete the handshake are used. The same URL may be listed twice: two
independent TCP/TLS connections to one host still give redundancy.

De-dup is classic A/B feed arbitration (`src/dedup.rs`): per `(symbol,
stream)` a high-water mark of the highest accepted sequence (`t` for trades,
`u` for depth). A message is accepted iff above the mark; the first copy to
arrive wins. The deduper also reports **sequence gaps** (loss on *every*
feed; expected 0) and, per duplicate, how far behind the losing feed was.

### Stale detection

`stale_after_ms` (default 5000, 0 = off) at two levels: a **feed** with no
frame for that long (reader), and a **symbol** with no accepted message for
that long (parser). Both log entry/recovery to stderr, show `[STALE]` on the
progress line, and are totalled in the summary. Symbol staleness also writes
an `'X'` marker record into the file (`exch_ts_ms`/`seq` = last seen,
`recv_ts_us` = detection time, `qty` = threshold in ms).

### Running unattended

Designed to run for weeks under a supervisor (systemd, docker `--restart`):

* **`duration_s: 0`** runs until `SIGINT`/`SIGTERM`; both flush, fsync and
  print the summary. Exit code 0 on a clean stop, 1 for config/connect
  errors at startup, 2 if the persister hit a fatal I/O error (disk full,
  unwritable path) — the pipeline stops and the supervisor restarts it.
* **Connections are not permanent.** Binance closes every stream at the
  24 h mark. Each reader rotates its socket after `reconnect_after_s`
  (default 23 h) *make-before-break*: the new socket is opened while the old
  one is live, then swapped, so there is no gap; anything both deliver is a
  duplicate for sequence arbitration. Feed *i* rotates at
  `reconnect_after_s + i × reconnect_stagger_s`, so two feeds never rotate
  together. Unplanned loss (socket error, server CLOSE, failed PING)
  reconnects with exponential back-off (0.5 s → `reconnect_backoff_max_s`),
  forever. While a feed is down the status line shows `[DOWN]` and the other
  feed carries on; the sequence watermarks live in the parser, so a
  reconnected feed can't re-deliver anything already written.
* **Files rotate** at every wall-clock boundary of `rotate_s` (default
  86400 = 00:00 UTC). Names are `<stem>.<YYYYMMDDTHHMMSSZ>.<ext>` (open
  time), e.g. `data/market_data.20261019T134205Z.l2bin`; each file has its
  own header + symbol table and stands alone. An existing name is never
  truncated (`-1`, `-2`, … suffix), so a restart can't clobber data.
  `rotate_s: 0` restores the single-file behaviour (`out` overwritten on
  start) for short test runs.
* **Durability.** The 1 MiB write buffer is flushed every
  `flush_interval_ms` (default 1 s), so a SIGKILL/OOM loses at most about
  that much; fsync on rotation and exit. Readers of `.l2bin` ignore a
  trailing partial record.
* **Bounded memory.** Latency, jitter, dup-lag and RTT percentiles use a
  fixed-size reservoir sample (`max_samples`, default 2^20 per series)
  instead of growing vectors; the summary says when a figure is estimated.
  Queues are bounded and back-pressure. Nothing else grows with run time.
* **Logs.** When stdout is not a terminal the progress line becomes one
  timestamped line per `status_interval_s` (default 30 s), e.g.
  `20261019T134235Z feeds=2/2  q1=0  q2=0  accepted=1234  records=56789  reconnects=0  files=1`.
  Events (stale, reconnect, rotation) go to stderr.

### Gaps and retransmission

Binance's WebSocket feed has no retransmission; recovery from a gap is a
fresh REST depth snapshot. Two feeds plus sequence arbitration is the
substitute. See the C++ README for the survey of other venues.

### Binary record (`src/record.rs`)

Identical to the C++ layout:

```
Record (48 B): exch_ts_ms i64 | recv_ts_us i64 | price i64 (×1e8) | qty i64 (×1e8)
               | seq u64 | symbol_id u16 | type u8 ('T','A','C','X') | side u8 ('B','S','-')
               | endpoint u8 | pad [u8;3]
File: FileHeader (32 B: "L2BIN\0\0\0", version, record size, n_symbols, name len, start ts)
      · n_symbols × 16 B NUL-padded names · records
```

`l2_dump FILE` prints CSV on stdout
(`symbol,exch_ts_ms,recv_ts_us,latency_us,endpoint,seq,type,side,price,quantity`);
`--header-only` shows the symbol table. `l2_to_csv IN.l2bin [OUT.csv]` writes
the same CSV to a file (default: `IN` with `.l2bin` replaced by `.csv`).

## Config

`config.json` (default path; `--config PATH`). Same keys as the C++ project:

```json
{
  "endpoints": ["wss://data-stream.binance.vision/stream",
                "wss://stream.binance.com:9443/stream",
                "wss://data-stream.binance.vision/stream"],
  "max_endpoints": 2,
  "symbols": ["btcusdt", "ethusdt"],
  "duration_s": 60,
  "out": "data/market_data.l2bin",
  "raw_out": "",
  "queue_capacity": 65536,
  "ping_interval_ms": 1000,
  "connect_timeout_s": 10,
  "stale_after_ms": 5000,
  "reader_busy_poll": false,
  "pin_cpus": [],
  "reconnect_after_s": 82800,
  "reconnect_stagger_s": 300,
  "reconnect_backoff_max_s": 30,
  "rotate_s": 86400,
  "flush_interval_ms": 1000,
  "status_interval_s": 30,
  "max_samples": 1048576
}
```

`duration_s: 0` = run until signalled. The last seven keys are described
under *Running unattended*. CLI overrides: `--symbols btcusdt,ethusdt`,
`--duration N`, `--out PATH`, `--raw-out PATH`. `SIGINT`/`SIGTERM` stop early
and still flush + print the summary. `pin_cpus` slots are
`[reader0, reader1, …, parser, persist]`.

## Build

```bash
cargo build --release          # .cargo/config.toml sets -C target-cpu=native
cargo test --release           # fixed-point + parser unit tests

./target/release/l2_recorder --duration 60 --out data/test.l2bin   # short test run
./target/release/l2_recorder                                        # ./config.json: run until stopped, daily files
./target/release/l2_dump data/market_data.20261019T134205Z.l2bin | head
./target/release/l2_to_csv data/market_data.20261019T134205Z.l2bin  # -> .csv next to it
```

## Files

```
Cargo.toml                 lib + bins l2_recorder, l2_dump, l2_to_csv
config.json
src/lib.rs
src/config.rs              Config (serde) + stream URL builder
src/record.rs              fixed-point, Record, FileHeader
src/queue.rs               rtrb wrapper: close-on-drop, len/peak gauge
src/stats.rs               percentiles, StaleTrack, per-stage stats, Live
src/time.rs                wall_us / steady_us
src/ws_client.rs           tungstenite + rustls wrapper, PING/PONG RTT
src/reader.rs              reader thread (one per feed)
src/parser.rs              sonic-rs structs -> ParsedMsg
src/dedup.rs               sequence-number feed arbitration
src/l2bin.rs               .l2bin reader + CSV writer (shared by l2_dump / l2_to_csv)
src/parse_stage.rs         parser thread (merges reader queues)
src/persist.rs             persister thread
src/bin/l2_recorder.rs     CLI, connect, threads, summary
src/bin/l2_dump.rs         .l2bin -> CSV on stdout
src/bin/l2_to_csv.rs       .l2bin -> .csv file
```
