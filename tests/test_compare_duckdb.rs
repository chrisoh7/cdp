// use anyhow::Result;
// use cdp::worlds::types::WorldType; // adjust path if module is local
// use duckdb;
// use std::collections::HashMap;
// use std::fmt::Display;

// // Runs both cdp's custom engine and DuckDB for a given query, and compares outputs.
// fn compare_with_duckdb(
//     world: WorldType,
//     key: &str,
//     val: &str,
//     agg: &str,
//     path: &str,
// ) -> Result<()> {
//     // CDP implementation (currently single-key u64 from parquet)
//     let my_result: HashMap<u64, f64> = world.groupby_agg_from_path(key, val, agg, path)?;

//     // Set up in-memory DuckDB
//     let conn = duckdb::Connection::open_in_memory()?;
//     conn.execute(
//         &format!("CREATE TABLE trips AS SELECT * FROM parquet_scan('{}')", path),
//         [],
//     )?;

//     // Construct reference SQL
//     let sql = if agg == "count" {
//         format!("SELECT {key}, count(*) FROM trips GROUP BY {key}")
//     } else {
//         format!("SELECT {key}, {agg}({val}) FROM trips GROUP BY {key}")
//     };

//     // Run DuckDB query
//     let ref_result = run_duckdb_agg(&conn, &sql)?;

//     // Compare results
//     compare_maps(
//         &format!("{agg}({val}) GROUP BY {key} [{:?}]", world),
//         &my_result,
//         &ref_result,
//     );
//     Ok(())
// }

// fn run_duckdb_agg(conn: &duckdb::Connection, sql: &str) -> Result<HashMap<u64, f64>> {
//     let mut stmt = conn.prepare(sql)?;
//     let mut rows = stmt.query([])?;
//     let mut map = HashMap::new();

//     while let Some(row) = rows.next()? {
//         let key: Option<i64> = row.get(0)?;
//         let val: Option<f64> = row.get(1)?;
//         if let (Some(k), Some(v)) = (key, val) {
//             map.insert(k as u64, v);
//         }
//     }
//     Ok(map)
// }

// fn compare_maps<K>(name: &str, mine: &HashMap<K, f64>, reference: &HashMap<K, f64>)
// where
//     K: Eq + std::hash::Hash + Display,
// {
//     println!("\n==== Comparing {} ====", name);

//     let mut mismatches = 0;
//     for (k, ref_v) in reference {
//         let my_v = mine.get(k).copied().unwrap_or(f64::NAN);
//         if (my_v - ref_v).abs() > 1e-6 {
//             println!("Key {} mismatch: mine = {:.6}, ref = {:.6}", k, my_v, ref_v);
//             mismatches += 1;
//         }
//     }
//     for k in mine.keys() {
//         if !reference.contains_key(k) {
//             println!("Extra key in my result: {}", k);
//             mismatches += 1;
//         }
//     }
//     if mismatches == 0 {
//         println!("Success: {} matches reference output!", name);
//     } else {
//         println!("Failure: {} had {} mismatches.", name, mismatches);
//     }
// }

// #[test]
// fn test_groupby_count() -> Result<()> {
//     let path = "/Users/hyunseokoh/hyunseoo/notes/cdp/src/data/yellow_tripdata_2025-01.parquet";
//     compare_with_duckdb(WorldType::Medium, "passenger_count", "*", "count", path)
// }

// #[test]
// fn test_groupby_avg() -> Result<()> {
//     let path = "/Users/hyunseokoh/hyunseoo/notes/cdp/src/data/yellow_tripdata_2025-01.parquet";
//     compare_with_duckdb(WorldType::Medium, "passenger_count", "total_amount", "avg", path)
// }


use anyhow::Result;
use cdp::worlds::types::{Aggregation, Record, WorldType}; // adjust path if module is local
use cdp::worlds::util::read_parquet_to_records;
use duckdb;
use std::collections::HashMap;
use std::fmt::Debug;

// Runs both cdp's custom engine and DuckDB for a given query, and compares outputs.
fn compare_with_duckdb(
    world: WorldType,
    key: &str,
    val: &str,
    agg: Aggregation,
    path: &str,
) -> Result<()> {
    // CDP implementation (currently single-key u64 from parquet)
    let my_result: HashMap<u64, f64> = world.groupby_agg_from_path(key, val, agg, path)?;

    // Set up in-memory DuckDB
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute(
        &format!("CREATE TABLE trips AS SELECT * FROM parquet_scan('{}')", path),
        [],
    )?;

    // Construct reference SQL
    let sql = if matches!(agg, Aggregation::Count) {
        format!("SELECT {key}, count(*) FROM trips GROUP BY {key}")
    } else {
        format!("SELECT {key}, {agg}({val}) FROM trips GROUP BY {key}")
    };

    // Run DuckDB query
    let ref_result = run_duckdb_agg(&conn, &sql)?;

    // Compare results
    compare_maps(
        &format!("{agg}({val}) GROUP BY {key} [{:?}]", world),
        &my_result,
        &ref_result,
    );
    Ok(())
}

fn run_duckdb_agg(conn: &duckdb::Connection, sql: &str) -> Result<HashMap<u64, f64>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;
    let mut map = HashMap::new();

    while let Some(row) = rows.next()? {
        let key: Option<i64> = row.get(0)?;
        let val: Option<f64> = row.get(1)?;
        if let (Some(k), Some(v)) = (key, val) {
            map.insert(k as u64, v);
        }
    }
    Ok(map)
}

// Generic comparator that now works for composite keys too.
fn compare_maps<K>(name: &str, mine: &HashMap<K, f64>, reference: &HashMap<K, f64>)
where
    K: Eq + std::hash::Hash + Debug,
{
    println!("\n==== Comparing {} ====", name);

    let mut mismatches = 0;
    for (k, ref_v) in reference {
        let my_v = mine.get(k).copied().unwrap_or(f64::NAN);
        if (my_v - ref_v).abs() > 1e-6 {
            println!("Key {:?} mismatch: mine = {:.6}, ref = {:.6}", k, my_v, ref_v);
            mismatches += 1;
        }
    }
    for k in mine.keys() {
        if !reference.contains_key(k) {
            println!("Extra key in my result: {:?}", k);
            mismatches += 1;
        }
    }
    if mismatches == 0 {
        println!("Success: {} matches reference output!", name);
    } else {
        println!("Failure: {} had {} mismatches.", name, mismatches);
    }
}

#[test]
fn test_groupby_count() -> Result<()> {
    let path = "/Users/hyunseokoh/hyunseoo/notes/cdp/src/data/yellow_tripdata_2025-01.parquet";
    compare_with_duckdb(
        WorldType::Medium,
        "passenger_count",
        "*",
        Aggregation::Count,
        path,
    )
}

#[test]
fn test_groupby_avg() -> Result<()> {
    let path = "/Users/hyunseokoh/hyunseoo/notes/cdp/src/data/yellow_tripdata_2025-01.parquet";
    compare_with_duckdb(
        WorldType::Medium,
        "passenger_count",
        "total_amount",
        Aggregation::Avg,
        path,
    )
}

/// Multi-key test:
///   SELECT passenger_count, total_amount, SUM(total_amount)
///   FROM trips
///   GROUP BY passenger_count, total_amount;
///
/// CDP side:
///   key   = (passenger_count, total_amount_bits)
///   value = total_amount
#[test]
fn test_groupby_sum_multikey_passenger_amount() -> Result<()> {
    let path = "/Users/hyunseokoh/hyunseoo/notes/cdp/src/data/yellow_tripdata_2025-01.parquet";

    // -------- CDP side (our engine) --------
    // Load single-key records: key = passenger_count, value = total_amount
    let single_key_records: Vec<Record<u64>> =
        read_parquet_to_records(path, "passenger_count", "total_amount")?;

    // Build multi-key records: (passenger_count, total_amount_bits) -> sum(total_amount)
    type MultiKey = (u64, u64); // (passenger_count, total_amount.to_bits())
    let records_q3: Vec<Record<MultiKey>> = single_key_records
        .into_iter()
        .map(|r| Record {
            key: (r.key, r.value.to_bits()),
            value: r.value,
        })
        .collect();

    let my_result: HashMap<MultiKey, f64> =
        WorldType::Medium.groupby_agg(&records_q3, Aggregation::Sum);

    // -------- DuckDB side (reference) --------
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute(
        &format!("CREATE TABLE trips AS SELECT * FROM parquet_scan('{}')", path),
        [],
    )?;

    // Same logical query as above
    let sql = "SELECT passenger_count, total_amount, SUM(total_amount) \
               FROM trips GROUP BY passenger_count, total_amount";
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;
    let mut ref_result: HashMap<MultiKey, f64> = HashMap::new();

    while let Some(row) = rows.next()? {
        let passenger_count: Option<i64> = row.get(0)?;
        let amount: Option<f64> = row.get(1)?;
        let sum_amount: Option<f64> = row.get(2)?;

        if let (Some(pc), Some(amt), Some(sum_v)) = (passenger_count, amount, sum_amount) {
            let key = (pc as u64, amt.to_bits());
            ref_result.insert(key, sum_v);
        }
    }

    compare_maps(
        "SUM(total_amount) GROUP BY passenger_count, total_amount [Medium]",
        &my_result,
        &ref_result,
    );

    Ok(())
}
