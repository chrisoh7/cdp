use anyhow::{bail, Result};
use cdp::worlds::types::{Aggregation, Record, WorldType};
use cdp::worlds::util::read_parquet_to_records;
use duckdb::Connection;
use std::collections::HashMap;
use std::fmt::Debug;

const TAXI_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/data/yellow_tripdata_2025-01.parquet"
);

fn all_worlds() -> [WorldType; 3] {
    [
        WorldType::Medium,
        WorldType::Large,
        WorldType::LargeBuffered,
    ]
}

fn compare_single_key_with_duckdb(
    world: WorldType,
    key: &str,
    val: &str,
    agg: Aggregation,
    path: &str,
) -> Result<()> {
    let my_result: HashMap<u64, f64> = world.groupby_agg_from_path(key, val, agg, path)?;

    let conn = Connection::open_in_memory()?;
    conn.execute(
        &format!("CREATE TABLE trips AS SELECT * FROM parquet_scan('{path}')"),
        [],
    )?;

    let sql = if matches!(agg, Aggregation::Count) {
        format!("SELECT {key}, count(*) FROM trips GROUP BY {key}")
    } else {
        format!("SELECT {key}, {agg}({val}) FROM trips GROUP BY {key}")
    };

    let ref_result = run_duckdb_u64_agg(&conn, &sql)?;
    compare_maps(
        &format!("{agg}({val}) GROUP BY {key} [{world}]"),
        &my_result,
        &ref_result,
    )
}

fn run_duckdb_u64_agg(conn: &Connection, sql: &str) -> Result<HashMap<u64, f64>> {
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

fn compare_maps<K>(name: &str, mine: &HashMap<K, f64>, reference: &HashMap<K, f64>) -> Result<()>
where
    K: Eq + std::hash::Hash + Debug,
{
    let mut mismatches = Vec::new();

    for (k, ref_v) in reference {
        let my_v = mine.get(k).copied().unwrap_or(f64::NAN);
        if (my_v - ref_v).abs() > 1e-6 {
            mismatches.push(format!(
                "key {:?}: mine={:.6} reference={:.6}",
                k, my_v, ref_v
            ));
        }
    }

    for k in mine.keys() {
        if !reference.contains_key(k) {
            mismatches.push(format!("extra key in result: {:?}", k));
        }
    }

    if mismatches.is_empty() {
        return Ok(());
    }

    let preview = mismatches
        .into_iter()
        .take(10)
        .collect::<Vec<_>>()
        .join("; ");
    bail!("{name} mismatched DuckDB output: {preview}")
}

#[test]
fn test_groupby_count_all_worlds() -> Result<()> {
    for world in all_worlds() {
        compare_single_key_with_duckdb(
            world,
            "passenger_count",
            "*",
            Aggregation::Count,
            TAXI_PATH,
        )?;
    }
    Ok(())
}

#[test]
fn test_groupby_avg_all_worlds() -> Result<()> {
    for world in all_worlds() {
        compare_single_key_with_duckdb(
            world,
            "passenger_count",
            "total_amount",
            Aggregation::Avg,
            TAXI_PATH,
        )?;
    }
    Ok(())
}

#[test]
fn test_groupby_sum_multikey_all_worlds() -> Result<()> {
    let single_key_records: Vec<Record<u64>> =
        read_parquet_to_records(TAXI_PATH, "passenger_count", "total_amount")?;

    type MultiKey = (u64, u64);
    let records: Vec<Record<MultiKey>> = single_key_records
        .into_iter()
        .map(|record| Record {
            key: (record.key, record.value.to_bits()),
            value: record.value,
        })
        .collect();

    let conn = Connection::open_in_memory()?;
    conn.execute(
        &format!("CREATE TABLE trips AS SELECT * FROM parquet_scan('{TAXI_PATH}')"),
        [],
    )?;

    let mut stmt = conn.prepare(
        "SELECT passenger_count, total_amount, SUM(total_amount)
         FROM trips
         GROUP BY passenger_count, total_amount",
    )?;
    let mut rows = stmt.query([])?;
    let mut ref_result: HashMap<MultiKey, f64> = HashMap::new();

    while let Some(row) = rows.next()? {
        let passenger_count: Option<i64> = row.get(0)?;
        let amount: Option<f64> = row.get(1)?;
        let sum_amount: Option<f64> = row.get(2)?;

        if let (Some(pc), Some(amt), Some(sum_v)) = (passenger_count, amount, sum_amount) {
            ref_result.insert((pc as u64, amt.to_bits()), sum_v);
        }
    }

    for world in all_worlds() {
        let my_result: HashMap<MultiKey, f64> = world.groupby_agg(&records, Aggregation::Sum);
        compare_maps(
            &format!("SUM(total_amount) GROUP BY passenger_count, total_amount [{world}]"),
            &my_result,
            &ref_result,
        )?;
    }

    Ok(())
}
