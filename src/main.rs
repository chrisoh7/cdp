mod worlds;
use mimalloc::MiMalloc;
use std::cmp::Ordering;
use std::time::Instant;
use worlds::types::{Aggregation, WorldType};
use worlds::util::{
    read_parquet_single_column, read_parquet_string_key_to_records,
    read_parquet_to_records, read_parquet_to_records_three_keys_with_transforms,
    read_parquet_to_records_two_keys_with_transforms, read_sensors_yyyymmdd_count,
    round_from_f64_bits, to_year_from_epoch_micros,
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> parquet::errors::Result<()> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/src/data/yellow_tripdata_2025-01.parquet");

    println!("{:-<60}", "");
    println!("NYC TAXI QUERIES  (Medium = per-thread local maps + parallel tree-merge)");
    println!("                  (Large  = single global concurrent hashmap)");
    println!("{:-<60}", "");

    // ── Q1: SELECT passenger_count, count(*) ─────────────────────────────────
    let t = Instant::now();
    let q1_records = read_parquet_single_column(path, "passenger_count")?;
    let t_load = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let result_q1_medium = WorldType::Medium.groupby_agg(&q1_records, Aggregation::Count);
    let t_med = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let result_q1_large = WorldType::Large.groupby_agg(&q1_records, Aggregation::Count);
    let t_large = t.elapsed().as_secs_f64() * 1000.0;

    println!(
        "Q1 count(*) by passenger_count\n  load={t_load:.1}ms  medium={t_med:.1}ms  large={t_large:.1}ms  [n={}]",
        q1_records.len()
    );
    println!("  Medium: {:?}", result_q1_medium);
    println!("  Large:  {:?}", result_q1_large);

    // ── Q2: SELECT passenger_count, avg(total_amount) ─────────────────────────
    let t = Instant::now();
    let q2_records = read_parquet_to_records(path, "passenger_count", "total_amount")?;
    let t_load = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let result_q2_medium = WorldType::Medium.groupby_agg(&q2_records, Aggregation::Avg);
    let t_med = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let result_q2_large = WorldType::Large.groupby_agg(&q2_records, Aggregation::Avg);
    let t_large = t.elapsed().as_secs_f64() * 1000.0;

    println!(
        "\nQ2 avg(total_amount) by passenger_count\n  load={t_load:.1}ms  medium={t_med:.1}ms  large={t_large:.1}ms  [n={}]",
        q2_records.len()
    );
    println!("  Medium: {:?}", result_q2_medium);
    println!("  Large:  {:?}", result_q2_large);

    // ── Q3: SELECT passenger_count, toYear(pickup_datetime), count(*) ─────────
    let t = Instant::now();
    let q3_records = read_parquet_to_records_two_keys_with_transforms(
        path,
        "passenger_count",
        "tpep_pickup_datetime",
        "total_amount",
        &[None, Some(Box::new(to_year_from_epoch_micros))],
    )?;
    let t_load = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let result_q3_medium = WorldType::Medium.groupby_agg(&q3_records, Aggregation::Count);
    let t_med = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let result_q3_large = WorldType::Large.groupby_agg(&q3_records, Aggregation::Count);
    let t_large = t.elapsed().as_secs_f64() * 1000.0;

    println!(
        "\nQ3 count(*) by (passenger_count, year)\n  load={t_load:.1}ms  medium={t_med:.1}ms  large={t_large:.1}ms  [n={}]",
        q3_records.len()
    );
    println!("  Medium: {:?}", result_q3_medium);
    println!("  Large:  {:?}", result_q3_large);

    // Q3 inverted: key order swapped, avg aggregation
    let t = Instant::now();
    let q3_inv_records = read_parquet_to_records_two_keys_with_transforms(
        path,
        "tpep_pickup_datetime",
        "passenger_count",
        "total_amount",
        &[Some(Box::new(to_year_from_epoch_micros)), None],
    )?;
    let t_load_inv = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let result_q3_inv = WorldType::Large.groupby_agg(&q3_inv_records, Aggregation::Avg);
    let t_large_inv = t.elapsed().as_secs_f64() * 1000.0;

    println!(
        "\nQ3 (swapped) avg(total_amount) by (year, passenger_count)\n  load={t_load_inv:.1}ms  large={t_large_inv:.1}ms  [n={}]",
        q3_inv_records.len()
    );
    println!("  Large: {:?}", result_q3_inv);

    // ── Q4: three-key count, ordered by year then count desc ──────────────────
    let t = Instant::now();
    let q4_records = read_parquet_to_records_three_keys_with_transforms(
        path,
        "passenger_count",
        "tpep_pickup_datetime",
        "trip_distance",
        "total_amount",
        &[
            None,
            Some(Box::new(to_year_from_epoch_micros)),
            Some(Box::new(round_from_f64_bits)),
        ],
    )?;
    let t_load = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let mut result_q4_medium: Vec<((u64, u64, u64), f64)> =
        WorldType::Medium.groupby_agg(&q4_records, Aggregation::Count).into_iter().collect();
    let t_med = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let mut result_q4_large: Vec<((u64, u64, u64), f64)> =
        WorldType::Large.groupby_agg(&q4_records, Aggregation::Count).into_iter().collect();
    let t_large = t.elapsed().as_secs_f64() * 1000.0;

    let sort_q4 = |v: &mut Vec<((u64, u64, u64), f64)>| {
        v.sort_by(|a, b| {
            let year_cmp = a.0 .1.cmp(&b.0 .1);
            if year_cmp != Ordering::Equal { return year_cmp; }
            b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal)
        });
    };
    sort_q4(&mut result_q4_medium);
    sort_q4(&mut result_q4_large);

    println!(
        "\nQ4 count(*) by (passenger_count, year, round(distance)) ORDER BY year, count DESC\n  load={t_load:.1}ms  medium={t_med:.1}ms  large={t_large:.1}ms  [n={}, {} groups]",
        q4_records.len(), result_q4_medium.len()
    );
    // Print only the top 10 rows to keep output clean
    println!("  Medium top-10: {:?}", &result_q4_medium[..result_q4_medium.len().min(10)]);
    println!("  Large  top-10: {:?}", &result_q4_large[..result_q4_large.len().min(10)]);

    // ── Q5: sensors count by YYYYMMDD ─────────────────────────────────────────
    println!("\n{:-<60}", "");
    println!("SENSORS / BROWN QUERIES");
    println!("{:-<60}", "");

    let sensors_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/data/subsets/environmental_sensors_2019_06_subset_200k.parquet"
    );

    let t = Instant::now();
    let sensor_records = read_sensors_yyyymmdd_count(sensors_path, "timestamp")?;
    let t_load = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let result_q5_medium = WorldType::Medium.groupby_agg(&sensor_records, Aggregation::Count);
    let t_med = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let result_q5_large = WorldType::Large.groupby_agg(&sensor_records, Aggregation::Count);
    let t_large = t.elapsed().as_secs_f64() * 1000.0;

    let mut q5_sorted: Vec<(u64, f64)> = result_q5_medium.into_iter().collect();
    q5_sorted.sort_by_key(|(day, _)| *day);

    println!(
        "\nQ5 sensors count(*) by toYYYYMMDD(timestamp)\n  load={t_load:.1}ms  medium={t_med:.1}ms  large={t_large:.1}ms  [n={}]",
        sensor_records.len()
    );
    println!("  By day: {:?}", q5_sorted);
    let _ = result_q5_large;

    // ── Q6: brown avg(cpu_user) by machine_name ───────────────────────────────
    let brown_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/data/subsets/brown_mgbench1_subset_200k.parquet"
    );

    let t = Instant::now();
    let brown_records = read_parquet_string_key_to_records(brown_path, "machine_name", "cpu_user")?;
    let t_load = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let result_q6_medium = WorldType::Medium.groupby_agg(&brown_records, Aggregation::Avg);
    let t_med = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let result_q6_large = WorldType::Large.groupby_agg(&brown_records, Aggregation::Avg);
    let t_large = t.elapsed().as_secs_f64() * 1000.0;

    println!(
        "\nQ6 brown avg(COALESCE(cpu_user,0)) by machine_name (keys=FNV hashes)\n  load={t_load:.1}ms  medium={t_med:.1}ms  large={t_large:.1}ms  [n={}]",
        brown_records.len()
    );
    println!("  Medium: {:?}", result_q6_medium);
    println!("  Large:  {:?}", result_q6_large);

    Ok(())
}
