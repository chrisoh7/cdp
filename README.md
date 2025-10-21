# CMU DB Group :: Conditional Data Processing (CDP)

I recently joined one of CMU Database Group’s projects, led by Prof. Jignesh Patel. The project aims to use FPGAs to accelerate Groupby aggs—and ultimately other streaming groupby queries—using something called Conditional Data Processing (CDP). In a nutshell, CDP is using different processing methods based on the data. 

## The Three Worlds
More specifically, the project aims to define the three worlds: three methods for groupby agg based on the cardinality of the dataset. Switching from one to another to achieve a best-of-any-world solution is our approach to overcoming the limitations of brittle FPGA accelerators. 

- Small: CAM-based small, brittle, low-latency solution
- Medium: hash tables per thread, merge intermediate aggregates at the end
- Large: a single global hash table using locks, thus contention

My primary job is to build baseline codes for the three worlds in Rust and run distribution streams of increasing cardinalities. I’ll try to keep this document up-to-date.

To generate a throughput vs. cardinality plot of the baseline code, run `./run_throughput_benchmark.sh`. 

## Type Definition
```
pub struct Record {
    pub key: u64,
    pub value: f64,
}
```
Each key-value pair is represented as a struct of `u64` and `f64` pairs. Check out the generic-key branch for a multi-key groupby agg implementation. 
```
pub enum AggState {
    Sum(f64),
    Min(f64),
    Max(f64),
    Avg { sum: f64, count: usize },
    Count(f64),
}
```
The internal groupby aggregate implementation holds the values wrapped by the `AggState` enum, which makes it convenient for storing and pattern matching values, especially sum-count pairs for `AVG()` operations. Also, `AggState` implements `init`, `update`, `merge`, and `finalize` which differs between agg operations. 
```
pub enum WorldType {
    Small, 
    Medium, 
    Large,
}
```
`WorldType` represents which solution among the three worlds to run. It implements `groupby_agg_from_path` and `groupby_agg`, which does an internal pattern matching to call the correct version of groupby agg.  

## Testing Infrastructure
We test for correctness by comparing our groupby agg result with DuckDB's. In `/tests/test_compare_duckdb.rs`, the `compare_with_duckdb` module runs the specified query on the CDP implementation and DuckDB's query executor, casts both results into the same type, and does per-entry comparison. Run `cargo test` to run the unit tests. 
