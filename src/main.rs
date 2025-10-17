mod worlds;

use worlds::util::{Record, WorldType};
use crate::worlds::util::read_parquet_to_records;

fn main() -> parquet::errors::Result<()> {
    let path = "/Users/hyunseokoh/hyunseoo/notes/cdp/src/data/yellow_tripdata_2025-01.parquet";

    let result_q1 = WorldType::Medium.groupby("passenger_count", "*", "count", path)?;
    println!("Q1 Result: {:?}", result_q1);

    let result_q2 = WorldType::Medium.groupby("passenger_count", "total_amount", "avg", path)?;
    println!("Q2 Result: {:?}", result_q2);

    Ok(())
}