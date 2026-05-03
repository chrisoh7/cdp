# CDP

CDP is a Rust baseline for experimenting with parallel `GROUP BY` aggregation over Parquet data. It ships three runnable software strategies plus one placeholder:

- `medium`: per-thread local maps followed by a parallel/adaptive merge
- `large`: a pure shared `DashMap` updated concurrently per record
- `large-buffered`: a shared `DashMap` with per-chunk local combiners before global merge
- `small`: reserved as a hardware-facing placeholder and not runnable in this repo

The repository is structured as a runnable baseline rather than a research scratchpad:

- `cargo run -- --query all --world all` runs the bundled query suite for `medium`, `large`, and `large-buffered`
- `cargo run --release --bin cardinality_throughput -- --world all --max-pow 5 --num-records 1000000` runs the synthetic throughput benchmark for all runnable software worlds
- `cargo test` validates output against DuckDB

## What is included

- A reusable library in [`src/lib.rs`](/Users/hyunseokoh/hyunseoo/notes/cdp/src/lib.rs)
- The three execution strategies in [`src/worlds/`](/Users/hyunseokoh/hyunseoo/notes/cdp/src/worlds)
- A query runner CLI in [`src/main.rs`](/Users/hyunseokoh/hyunseoo/notes/cdp/src/main.rs)
- A cardinality benchmark binary in [`src/bin/cardinality_throughput.rs`](/Users/hyunseokoh/hyunseoo/notes/cdp/src/bin/cardinality_throughput.rs)
- DuckDB-backed regression tests in [`tests/test_compare_duckdb.rs`](/Users/hyunseokoh/hyunseoo/notes/cdp/tests/test_compare_duckdb.rs)

## Quick Start

Build and run the full query suite:

```bash
cargo run --release -- --query all --world all
```

Run one query against one strategy:

```bash
cargo run --release -- --query q2 --world medium --limit 5
```

Generate the synthetic throughput CSV and the line plot `throughput.png`:

```bash
./run_throughput.sh --world all --max-pow 5 --records 1000000
```

Generate the stacked timing plot `throughput_stacked.png` and refresh `throughput.png` from the same benchmark CSV:

```bash
./run_throughput_stacked.sh --world all --max-pow 5 --records 1000000
```

Both scripts also regenerate `throughput.csv`.

## Baseline Behavior

- `medium` is the default CPU baseline and usually the best general-purpose strategy in this repo.
- `large` is the pure shared-state baseline; it is useful for comparison and exposes contention directly.
- `large-buffered` is the optimized shared-state variant when you want the same final global-map structure with a local combiner stage.

The current code emphasizes correctness, stable benchmarking, and clean extensibility. `small` remains in the type system as a placeholder only. If you want to optimize further, start by profiling the pure `large` update path and the `medium` finalize path at very high cardinality.

## Why `large-buffered` Exists

The split between `large` and `large-buffered` is intentional.

- `large` answers the question: what happens if every record updates one shared global map directly?
- `large-buffered` answers the question: how much can that same global-map design improve if each worker first combines duplicates locally?

These two variants should not be conflated. Once local pre-aggregation is introduced, the benchmark no longer measures pure shared-map contention. That optimization is useful in practice, but it changes the meaning of the world. Keeping both variants makes the benchmark easier to interpret:

- `large` is the conceptual baseline
- `large-buffered` is the optimized derivative
- the performance gap between them quantifies the cost of raw shared-state contention

## Data and Queries

Bundled queries cover:

- NYC Taxi count/avg by `passenger_count`
- NYC Taxi grouped by `(passenger_count, year)`
- NYC Taxi grouped by `(passenger_count, year, rounded distance)`
- Environmental sensors count by day
- Brown logs average CPU by machine name hash

Data-loading helpers normalize keys to `u64`-based representations:

- integers are cast to `u64`
- floating keys use `f64::to_bits()`
- timestamps use epoch microseconds, optionally transformed into buckets
- strings are hashed with FNV-64

## Dataset Provenance

- `src/data/yellow_tripdata_2025-01.parquet` comes from the NYC TLC trip record data source:
  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- ClickHouse also maintains public dataset references and examples around NYC Taxi:
  https://datasets.clickhouse.com/index.html
  https://clickhouse.com/jp/integrations/gcs
- The environmental sensors and Brown logs files under `src/data/subsets/` are local benchmark subsets rather than canonical upstream distributions.

For fuller provenance and reproducibility notes, see `OVERVIEW.md`.

## Verification

`cargo test` compares the runnable software worlds against DuckDB for:

- single-key `COUNT`
- single-key `AVG`
- multi-key `SUM`

This means the tests fail on real mismatches now; they no longer just print differences.
