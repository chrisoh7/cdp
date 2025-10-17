mod worlds;
use worlds::types::WorldType;

fn main() -> parquet::errors::Result<()> {
    let path = "/Users/hyunseokoh/hyunseoo/notes/cdp/src/data/yellow_tripdata_2025-01.parquet";

    // Q1: SELECT cab_type, count(*) FROM trips_mergetree GROUP BY cab_type;
    // TODO: find cab_type; cab_type is not found, therefore replace with passenger_count instead for now
    let result_q1 = WorldType::Medium
        .groupby("passenger_count", "*", "count", path)?;
    println!("Q1 Result: {:?}", result_q1);
    
    // Q2: SELECT passenger_count, avg(total_amount) FROM trips_mergetree GROUP BY passenger_count;
    let result_q2 = WorldType::Medium
        .groupby("passenger_count", "total_amount", "avg", path)?;
    println!("Q2 Result: {:?}", result_q2);


    // Q3: SELECT passenger_count, toYear(pickup_date) AS year, count(*) FROM trips_mergetree GROUP BY passenger_count, year;


    Ok(())
}