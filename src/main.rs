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
    let keys = vec![String::from("passenger_count"), String::from("payment_type")];
    let vals = vec![String::from("total_amount")];
    let aggs = vec![String::from("avg")];

    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    let val_refs: Vec<&str> = vals.iter().map(|s| s.as_str()).collect();
    let agg_refs: Vec<&str> = aggs.iter().map(|s| s.as_str()).collect();

    let result_q3 = WorldType::Medium.groupby_multi(&key_refs, &val_refs, &agg_refs, path)?;

    println!("Q3 Result: {:?}", result_q3);

    Ok(())
}


// fn main() {
//     use crate::worlds::medium::groupby_multi;
//     use crate::worlds::types::MultiRecord;

//     let records = vec![
//         MultiRecord { keys: vec![1, 2020, 3], values: vec![10.0, 2.0] },
//         MultiRecord { keys: vec![1, 2020, 3], values: vec![20.0, 4.0] },
//         MultiRecord { keys: vec![2, 2021, 3], values: vec![30.0, 6.0] },
//     ];

//     let aggs = ["sum", "avg"];
//     let result = groupby_multi(&records, &aggs);

//     for (k, v) in result.iter() {
//         println!("Key {:?} => Values {:?}", k, v);
//     }
// }