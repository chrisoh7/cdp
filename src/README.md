# CDP :: Rust Baseline

The Rust Baseline code covers all three worlds, but medium and large are implemented first. Some common util functions that we can use are `get_agg_op`, `get_agg_id`, and `get_reduce_op`. Given the aggregation type (min, max, sum, avg, count), they return the aggregation/reduction operators and aggregate identity (e.g. 0 for sum, 1 for count, etc.). 

Multithreading is done using iterator-based `rayon` tools. 

1. Medium World Solution: 
    
    Each thread inserts a streamed key-value pair into its local hash map, where the aggregation happens. In the finalize step, we merge the maps across threads and return the final hash map. 
    
2. Large World Solution:
    
    Each thread inserts into a single global hash map (using `scc::HashMap`), thus contention can happen.  In the finalize step, we return the map. 
    

The NYC Taxi dataset will be used to test the correctness of the implementation. More specifically, my initial goal is to run Q1-Q3 from the ClickHouse docs linked below. 

- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- https://clickhouse.com/docs/getting-started/example-datasets/nyc-taxi

Queries:

Q1: `SELECT cab_type, count(*) FROM trips_mergetree GROUP BY cab_type;` 

Q2: `SELECT passenger_count, avg(total_amount) FROM trips_mergetree GROUP BY passenger_count;` 

Q3: `SELECT passenger_count, toYear(pickup_date) AS year, count(*) FROM trips_mergetree GROUP BY passenger_count, year;`
