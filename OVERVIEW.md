# CDP Overview

## Purpose

CDP evaluates parallel `GROUP BY` implementations over columnar data using a single logical interface and multiple concurrency strategies. The core question is simple: how should aggregation state be partitioned or shared as key cardinality and contention change?

## Core Types

`Record<K>` is the unit of work:

```rust
Record { key: K, value: f64 }
```

`AggState` tracks in-flight aggregation:

- `Sum`
- `Min`
- `Max`
- `Avg { sum, count }`
- `Count`

`WorldType` dispatches to the aggregation strategy:

- `Small`
- `Medium`
- `Large`
- `LargeBuffered`

## Execution Strategies

### Small

`src/worlds/small.rs`

- This is a placeholder only
- It is retained to reflect the hardware-oriented design space
- In the software baseline, selecting it returns an explicit unsupported error or panic

### Medium

`src/worlds/medium.rs`

- Each worker builds a local `HashMap` using `FnvBuildHasher`
- Finalization selects between tree-reduce and scatter-based merging
- This is the main baseline for CPU throughput in the repo

### Large

`src/worlds/large.rs`

- All workers update a shared `DashMap`
- There is no merge phase beyond final materialization
- This keeps memory use lower than per-thread duplication but is far more sensitive to contention

### LargeBuffered

`src/worlds/large.rs`

- All workers still converge into one shared `DashMap`
- Each chunk performs local pre-aggregation before touching the shared map
- This is intentionally a separate variant because it improves performance by reducing the contention that the pure `Large` world is meant to expose

## Rationale For The Split

Originally, the optimized global-map path lived inside `Large`. That made the code faster, but it also weakened the interpretation of the benchmark. A world called `Large` is supposed to represent direct concurrent updates into one shared global structure. Once a local combiner is inserted in front of that structure, the concurrency story changes materially.

The repository now keeps both forms on purpose:

- `Large` is the pure shared-state baseline
- `LargeBuffered` is the optimized variant with local pre-aggregation

This separation serves two goals:

- preserve benchmark honesty by keeping the pure contention case visible
- preserve practical performance work by retaining the optimized version as an explicit alternative

In other words, `LargeBuffered` is not just an implementation detail. It is a different experimental point in the design space, and it is documented separately so users can reason about its performance correctly.

## Data Pipeline

Helpers in `src/worlds/util.rs` read only the requested Parquet columns, normalize them into `Record<K>` values, and then feed them into a chosen world.

Supported key normalization:

- signed and unsigned integers to `u64`
- timestamps to epoch microseconds
- floating keys to raw bit patterns
- strings to FNV-64 hashes

Supported transform helpers:

- `to_year_from_epoch_micros`
- `to_yyyymmdd_from_epoch_micros`
- `round_from_f64_bits`

## Entry Points

### Query Runner

`cargo run --release -- --query all --world all`

Runs the bundled real-data workloads and prints:

- load time
- update time
- finalize time
- total time
- group count
- a small sample of results

### Synthetic Benchmark

`cargo run --release --bin cardinality_throughput -- --world all --max-pow 5 --num-records 1000000`

Generates synthetic input with controlled key cardinality and prints CSV:

```text
world,cardinality,throughput_records_per_sec,t_update,t_finalize
```

To turn that benchmark into visual plots, use the bundled scripts:

```bash
./run_throughput.sh --world all --max-pow 5 --records 1000000
```

This writes:

- `throughput.csv`
- `throughput.png`

For the stacked timing view:

```bash
./run_throughput_stacked.sh --world all --max-pow 5 --records 1000000
```

This writes:

- `throughput.csv`
- `throughput_stacked.png`
- `throughput.png`

## Tests

`tests/test_compare_duckdb.rs` validates the runnable software implementations against DuckDB for:

- `COUNT(*) GROUP BY passenger_count`
- `AVG(total_amount) GROUP BY passenger_count`
- multi-key `SUM(total_amount)`

The tests now assert on mismatches instead of logging-only behavior.
