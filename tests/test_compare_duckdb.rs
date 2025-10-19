use anyhow::Result;
use cdp::worlds::types::WorldType; // adjust path if module is local
use duckdb;
use std::collections::HashMap;

// Runs both cdp's custom engine and DuckDB for a given query, and compares outputs.
fn compare_with_duckdb(
    world: WorldType,
    key: &str,
    val: &str,
    agg: &str,
    path: &str,
) -> Result<()> {
    
    // CDP implementation
    let my_result = world.groupby(key, val, agg, path)?;

    // Set up in-memory DuckDB
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute(
        &format!("CREATE TABLE trips AS SELECT * FROM parquet_scan('{}')", path),
        [],
    )?;

    // Construct reference SQL
    let sql = if agg == "count" {
        format!("SELECT {key}, count(*) FROM trips GROUP BY {key}")
    } else {
        format!("SELECT {key}, {agg}({val}) FROM trips GROUP BY {key}")
    };

    // Run DuckDB query
    let ref_result = run_duckdb_agg(&conn, &sql)?;

    // Compare results
    compare_maps(&format!("{agg}({val}) GROUP BY {key} [{:?}]", world), &my_result, &ref_result);
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

fn compare_maps(name: &str, mine: &HashMap<u64, f64>, reference: &HashMap<u64, f64>) {
    println!("\n==== Comparing {} ====", name);

    let mut mismatches = 0;
    for (k, ref_v) in reference {
        let my_v = mine.get(k).copied().unwrap_or(f64::NAN);
        if (my_v - ref_v).abs() > 1e-6 {
            println!("Key {} mismatch: mine = {:.6}, ref = {:.6}", k, my_v, ref_v);
            mismatches += 1;
        }
    }
    for k in mine.keys() {
        if !reference.contains_key(k) {
            println!("Extra key in my result: {}", k);
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
    compare_with_duckdb(WorldType::Medium, "passenger_count", "*", "count", path)
}

#[test]
fn test_groupby_avg() -> Result<()> {
    let path = "/Users/hyunseokoh/hyunseoo/notes/cdp/src/data/yellow_tripdata_2025-01.parquet";
    compare_with_duckdb(WorldType::Medium, "passenger_count", "total_amount", "avg", path)
}
