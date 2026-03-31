# CMU DB Group :: Conditional Data Processing (CDP)

I recently joined one of CMU Database Group's projects, led by Prof. Jignesh Patel and Prof. James Hoe. The project aims to use FPGAs to accelerate GROUP BY aggregations—and ultimately other streaming groupby queries—using something called Conditional Data Processing (CDP). In a nutshell, CDP uses different processing methods based on the data.

## The Three Worlds

The project defines three worlds: three methods for GROUP BY aggregation based on the cardinality of the dataset. Switching from one to another to achieve a best-of-any-world solution is our approach to overcoming the limitations of brittle FPGA accelerators.

- **Small:** CAM-based, low-latency, brittle at high cardinality. Per-thread local maps with a serial merge step.
- **Medium:** Per-thread local hash maps; partial aggregates are merged via parallel tree-reduction at the end. Zero contention during the update phase.
- **Large:** A single global hash map using lock-based concurrent access (`scc::HashMap`), so threads may contend. No merge step needed.

My primary job is to build baseline implementations for the three worlds in Rust and benchmark them across distribution streams of increasing cardinalities. See `OVERVIEW.md` for a full technical breakdown of the codebase.

To generate a throughput vs. cardinality plot, run `./run_throughput.sh`.
For the stacked update/finalize plot, run `./run_throughput_stacked.sh`.

## Current Performance Notes

These are the implementation choices that currently benchmark best:

- `medium` world uses per-thread local `HashMap`s with `FnvBuildHasher`, then parallel tree-reduces partial maps during the finalize step.
- `medium` chunk size is derived from dataset size and Rayon thread count (`records / threads`).
- `large` world uses a single global `scc::HashMap` with `records.par_chunks(4096)` for updates and pre-sizes both maps to reduce reallocations.

## Key Types

```rust
pub struct Record<K> {
    pub key: K,
    pub value: f64,
}
```
Generic key-value pair. Keys are normalized to `u64` after loading (integers cast directly, floats stored as bit-patterns, timestamps as epoch-microseconds, strings hashed via FNV-64). Composite keys are `(u64, u64)` or `(u64, u64, u64)` tuples.

```rust
pub enum AggState {
    Sum(f64),
    Min(f64),
    Max(f64),
    Avg { sum: f64, count: usize },
    Count(f64),
}
```
Internal per-key aggregation state. Implements `init`, `update`, `merge`, and `finalize`. The `Avg` variant defers division until `finalize` to keep `merge` associative.

```rust
pub enum WorldType {
    Small,
    Medium,
    Large,
}
```
Dispatches GROUP BY queries to the correct world. Exposes `groupby_agg_from_path` (single-key), `groupby_agg_two_keys_from_path_with_transforms`, and `groupby_agg_three_keys_from_path_with_transforms` for composite-key queries with optional per-key transform functions.

## Active Queries

All queries run on both Medium and Large worlds. Dataset files live under `src/data/`.

| Query | Dataset | Description |
|---|---|---|
| Q1 | NYC Taxi | `SELECT passenger_count, COUNT(*) GROUP BY passenger_count` |
| Q2 | NYC Taxi | `SELECT passenger_count, AVG(total_amount) GROUP BY passenger_count` |
| Q3 | NYC Taxi | `SELECT passenger_count, toYear(pickup_datetime), COUNT(*) GROUP BY ...` |
| Q4 | NYC Taxi | Three-key: passenger count × year × rounded distance, ordered by year and count desc |
| Q5 | Environmental Sensors | `SELECT toYYYYMMDD(timestamp) AS day, COUNT() GROUP BY day ORDER BY day ASC` |
| Q6 | Brown Logs | `SELECT machine_name, AVG(COALESCE(cpu_user, 0.0)) GROUP BY machine_name` |

## Testing Infrastructure

Correctness is validated by comparing CDP output against DuckDB's query executor. In `tests/test_compare_duckdb.rs`, the `compare_with_duckdb` function runs the specified query on both engines, casts both results to the same type, and does per-entry comparison with a 1e-6 tolerance. Run `cargo test` to execute.
