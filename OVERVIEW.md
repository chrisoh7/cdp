# CDP: Columnar Data Processing — Codebase Overview

## What This Is

CDP is a Rust research benchmark for evaluating **parallel GROUP BY aggregation strategies** on columnar data. The central question it explores: *given a large dataset in columnar (Parquet) format, what is the fastest way to compute a GROUP BY aggregation across multiple CPU cores, and how does the optimal strategy change with data characteristics?*

The system implements the same logical operation — GROUP BY with aggregation — under three distinct concurrency models ("worlds"), then measures and compares their throughput.

---

## Core Abstractions

### Record\<K\>
```
Record { key: K, value: f64 }
```
The universal unit of work. Every query reduces a Parquet file to a flat list of `(key, value)` pairs. The key is always `u64` after type normalization (integers cast directly, floats stored as bit-patterns, timestamps as epoch-microseconds, strings hashed via FNV-64). The value is always `f64`.

### AggState
An enum that tracks running aggregation state for a single key:
- `Sum(f64)` — running total
- `Min(f64)` / `Max(f64)` — running extrema
- `Avg { sum: f64, count: usize }` — deferred average (avoids division until finalize)
- `Count(f64)` — incremented by 1.0 per record

**Methods:** `init`, `update`, `merge` (for combining two partial states), `finalize` (emits the scalar result).

### WorldType
Enum (`Small`, `Medium`, `Large`) that dispatches to one of the three parallelism strategies. Exposes a unified interface regardless of strategy:
- `groupby_agg_from_path` — single-key query from Parquet
- `groupby_agg_two_keys_from_path_with_transforms` — two-key composite key with optional per-key transforms
- `groupby_agg_three_keys_from_path_with_transforms` — three-key composite key
- `groupby_agg<K>` — in-memory aggregation over pre-loaded `Vec<Record<K>>`

---

## The Three Worlds (Parallelism Strategies)

All three use Rayon for thread-pool management. The difference is entirely in **how concurrent writes to the hash map are handled**.

### Medium World — Per-Thread Local Maps, Parallel Tree-Merge
*File: `src/worlds/medium.rs`*

**Update phase:** Data is chunked `1:1` with threads. Each thread builds a completely independent `HashMap` (using FNV hasher for integer-heavy keys). Zero lock contention.

**Finalize phase:** The per-thread partial maps are merged using Rayon's parallel tree-reduction (`par_iter().reduce()`). At each level, the smaller map is merged into the larger to minimize insertion cost.

**Memory:** O(T × C) where T = thread count, C = cardinality.
**Behavior:** Near-linear throughput scaling with thread count for high-cardinality data. Merge overhead becomes significant at low cardinality.

---

### Large World — Single Global Concurrent HashMap
*File: `src/worlds/large.rs`*

**Update phase:** All threads write directly to a single shared `scc::HashMap` (from the `scc` crate). Each insertion uses `entry_sync()` — an optimistic compare-and-swap that falls back to locking only on contention.

**Finalize phase:** A single serial pass via `iter_sync()` to finalize each `AggState` into a scalar.

**Memory:** O(C) — minimal, since there is only one copy of the map.
**Behavior:** Low contention at low cardinality (few keys → few contested buckets). Degrades under high cardinality and high thread counts because hot-key contention serializes threads.

---

### Small World — Per-Thread Local Maps, Serial Merge
*File: `src/worlds/small.rs`*

Structurally identical to Medium during the update phase. The finalize differs: maps are sorted by size and merged serially (smallest-into-largest), avoiding the coordination overhead of parallel tree-reduction.

**Variants:**
- `small_world()` — serial merge
- `small_world_parallel_merge()` — parallel tree-merge (identical to Medium's strategy)
- `small_world_adaptive()` — selects serial merge for ≤4 threads, parallel for more

**Note:** Small World is currently wired into the `WorldType` enum but not invoked in the main benchmark (`WorldType::Small` dispatches to `unimplemented!()`). It exists as an experimental variant.

---

## Data Pipeline

```
Parquet file
    │
    ▼  (util.rs)
Read column(s) → normalize to u64 key(s) + f64 value
    │   • Int/UInt → cast to Int64/UInt64 → u64
    │   • Timestamp(μs) → epoch micros as u64
    │   • Float64 → stored as f64::to_bits() for key, direct for value
    │   • String (Utf8) → FNV-64 hash
    │
    ▼  (optional per-key transforms)
    │   • to_year_from_epoch_micros: timestamp → year integer
    │   • to_yyyymmdd_from_epoch_micros: timestamp → YYYYMMDD integer
    │   • round_from_f64_bits: float-as-bits → nearest integer
    │   • User-supplied closures (Box<dyn Fn(u64) -> u64>)
    │
    ▼
Vec<Record<K>>  (K = u64, (u64,u64), or (u64,u64,u64))
    │
    ▼  (WorldType dispatch)
Medium / Large / Small world aggregation
    │
    ▼
HashMap<K, f64>  — final result
```

### Parquet Reader Functions (`src/worlds/util.rs`)

| Function | Key type | Value | Notes |
|---|---|---|---|
| `read_parquet_to_records` | single u64 | f64 | General key+value |
| `read_parquet_single_column` | single u64 | 1.0 | COUNT(*) queries |
| `read_parquet_to_records_two_keys_with_transforms` | (u64, u64) | f64 | Composite keys + transforms |
| `read_parquet_to_records_three_keys_with_transforms` | (u64, u64, u64) | f64 | Three-key queries + transforms |
| `read_sensors_yyyymmdd_count` | u64 (YYYYMMDD) | 1.0 | Handles Timestamp or string datetime |
| `read_parquet_string_key_to_records` | u64 (FNV hash) | f64 | String groupby keys; NULLs → 0.0 |

---

## Benchmark Harness

`src/bin/cardinality_throughput.rs` — a separate binary that measures throughput (records/sec) as a function of **key cardinality** (ranging from 10^1 to 10^N).

- Generates synthetic `Vec<Record<u64>>` with configurable N records and cardinality
- Runs all three worlds or a selected one
- Splits timing into `t_update` (aggregation phase) and `t_finalize` (merge/snapshot phase)
- Outputs CSV (`world, cardinality, throughput, t_update, t_finalize`)

**CLI:**
```
--num-records N     default: 1,000,000
--max-pow P         default: 4  (cardinality up to 10^P)
--agg {sum|min|max|avg|count}
--world {Small|Medium|Large|All}
```

---

## Active Queries (`src/main.rs`)

All queries run on both Medium and Large worlds.

| Query | Dataset | SQL equivalent |
|---|---|---|
| Q1 | NYC Taxi | `SELECT passenger_count, COUNT(*) GROUP BY passenger_count` |
| Q2 | NYC Taxi | `SELECT passenger_count, AVG(total_amount) GROUP BY passenger_count` |
| Q3 | NYC Taxi | `SELECT passenger_count, toYear(pickup_datetime), COUNT(*) GROUP BY ...` |
| Q4 | NYC Taxi | `SELECT passenger_count, toYear(pickup_datetime), round(trip_distance), COUNT(*) GROUP BY ... ORDER BY year, count DESC` |
| Q5 | Environmental Sensors | `SELECT toYYYYMMDD(timestamp) AS day, COUNT() GROUP BY day ORDER BY day ASC` |
| Q6 | Brown University Logs | `SELECT machine_name, AVG(COALESCE(cpu_user, 0.0)) AS cpu GROUP BY machine_name` |

Q3 and Q4 exercise the transform pipeline: timestamps are converted to year or YYYYMMDD integers before being used as hash-map keys, allowing time-bucketed GROUP BY without changing the u64 key architecture.

---

## Dependencies of Note

| Crate | Role |
|---|---|
| `rayon` | Thread-pool, `par_chunks`, parallel tree-reduce |
| `scc` | Lock-free concurrent HashMap (`entry_sync`, `iter_sync`) used by Large World |
| `arrow` / `parquet` | Columnar in-memory format and Parquet file I/O |
| `fnv` | FNV-64 hasher for integer keys (lower overhead than SipHash for non-adversarial inputs) |
| `mimalloc` | Default global allocator (Microsoft's cache-friendly allocator; improves HashMap-heavy workloads) |
| `tikv-jemallocator` | Optional alternative allocator (feature flag `jemalloc`) |
| `chrono` | Timestamp arithmetic for date-bucketing transforms |
| `dashmap` | Available but not yet used in main execution paths |
| `duckdb` | Bundled; available for validation / reference query execution |

---

## Datasets

### NYC Taxi (Yellow Cab, January 2025)
**File:** `src/data/yellow_tripdata_2025-01.parquet` (~59 MB)
**Source:** NYC Taxi & Limousine Commission open data

The primary benchmark dataset. Used for Q1–Q4.

Key columns used:
- `passenger_count` (UInt8) — low-cardinality group key (~10 distinct values)
- `total_amount` (Float64) — aggregation target
- `tpep_pickup_datetime` (Timestamp, microsecond) — bucketed to year for Q3/Q4
- `trip_distance` (Float64) — rounded to integer distance buckets for Q4

Cardinality profile: single-key queries have very low cardinality (~10); three-key queries expand to ~hundreds of combinations. This makes it a good test of low-cardinality behavior.

---

### Environmental Sensors (June 2019, 200k row subset)
**File:** `src/data/subsets/environmental_sensors_2019_06_subset_200k.parquet`
**Source:** Open sensor network data (particulate matter / weather sensors)

Used for Q5. Columns relevant to the query:
- `timestamp` — datetime of the measurement; converted to YYYYMMDD integer key
- `temperature`, `pressure`, `altitude` — physical measurements (available but not used in Q5)

The dataset spans multiple days within June 2019, so GROUP BY day produces ~30 buckets — very low cardinality, stressing the finalize/merge path relative to the update path.

---

### Brown University System Logs (200k row subset)
**File:** `src/data/subsets/brown_mgbench1_subset_200k.parquet`
**Source:** Brown University mgBench benchmark (server monitoring logs)

Used for Q6. Columns relevant to the query:
- `machine_name` (String) — group key; hashed to u64 via FNV-64 since the framework uses integer keys throughout
- `cpu_user` (Float64) — user-space CPU utilization; NULL values coerced to 0.0 (COALESCE semantics)

Additional columns present but unused: `cpu_idle`, `cpu_system`, `bytes_in`, `bytes_out`, `mem_*`, `disk_*`, `log_time`, `machine_group`. The dataset covers machines logged from 2016 onward. Cardinality is low (tens of distinct machines), making this dataset similar to the Sensors dataset in aggregation structure.
