# Source Layout

- `lib.rs`: library entry point
- `main.rs`: query runner CLI over bundled datasets
- `bin/cardinality_throughput.rs`: synthetic throughput benchmark
- `worlds/`: aggregation implementations, types, Parquet readers, and the `small` placeholder

The library code lives under `worlds/` and is shared by both binaries and the test suite.
