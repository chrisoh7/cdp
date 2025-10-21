mod worlds;
use worlds::types::WorldType;

fn main() -> parquet::errors::Result<()> {
    let path = "/Users/hyunseokoh/hyunseoo/notes/cdp/src/data/yellow_tripdata_2025-01.parquet";

    // Q1: SELECT passenger_count, count(*) FROM trips_mergetree GROUP BY passenger_count;

    // MEDIUM WORLD
    let result_q1_medium = WorldType::Medium
        .groupby_agg_from_path("passenger_count", "*", "count", path)?;

    println!("Q1 Result: {:?}", result_q1_medium);

    // LARGE WORLD
    let result_q1_large = WorldType::Large
        .groupby_agg_from_path("passenger_count", "*", "count", path)?;
    println!("Q1 Result: {:?}", result_q1_large);
    
    
    
    // Q2: SELECT passenger_count, avg(total_amount) FROM trips_mergetree GROUP BY passenger_count;

    // MEDIUM WORLD
    let result_q2_medium = WorldType::Medium
        .groupby_agg_from_path("passenger_count", "total_amount", "avg", path)?;
    println!("Q2 Result: {:?}", result_q2_medium);

    // LARGE WORLD
    let result_q2_large = WorldType::Large
        .groupby_agg_from_path("passenger_count", "total_amount", "avg", path)?;
    println!("Q2 Result: {:?}", result_q2_large);

    // TODO: support multiple keys
    // Q3: SELECT passenger_count, toYear(pickup_date) AS year, count(*) FROM trips_mergetree GROUP BY passenger_count, year;

    Ok(())
}