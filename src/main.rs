mod worlds;
use mimalloc::MiMalloc;
use std::cmp::Ordering;
use worlds::types::{Aggregation, WorldType};
use worlds::util::{
    read_parquet_string_key_to_records, read_sensors_yyyymmdd_count, round_from_f64_bits,
    to_year_from_epoch_micros,
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> parquet::errors::Result<()> {
    let path = "/Users/hyunseokoh/hyunseoo/notes/cdp/src/data/yellow_tripdata_2025-01.parquet";

    // Q1: SELECT passenger_count, count(*) FROM trips_mergetree GROUP BY passenger_count;

    // MEDIUM WORLD
    let result_q1_medium = WorldType::Medium.groupby_agg_from_path(
        "passenger_count",
        "*",
        Aggregation::Count,
        path,
    )?;

    println!("Q1 Medium Result: {:?}", result_q1_medium);

    // LARGE WORLD
    let result_q1_large =
        WorldType::Large.groupby_agg_from_path("passenger_count", "*", Aggregation::Count, path)?;
    println!("Q1 Large Result: {:?}", result_q1_large);

    // Q2: SELECT passenger_count, avg(total_amount) FROM trips_mergetree GROUP BY passenger_count;

    // MEDIUM WORLD
    let result_q2_medium = WorldType::Medium.groupby_agg_from_path(
        "passenger_count",
        "total_amount",
        Aggregation::Avg,
        path,
    )?;
    println!("Q2 Medium Result: {:?}", result_q2_medium);

    // LARGE WORLD
    let result_q2_large = WorldType::Large.groupby_agg_from_path(
        "passenger_count",
        "total_amount",
        Aggregation::Avg,
        path,
    )?;
    println!("Q2 Large Result: {:?}", result_q2_large);

    // Q3: SELECT passenger_count, toYear(pickup_date) AS year, count(*) FROM trips_mergetree GROUP BY passenger_count, year;

    let result_q3_medium = WorldType::Medium
        .groupby_agg_two_keys_from_path_with_transforms(
            "passenger_count",
            "tpep_pickup_datetime", // Timestamp(Microsecond, None)
            "total_amount",         // TODO: enable stars for count(*)
            Aggregation::Count,
            path,
            &[None, Some(Box::new(to_year_from_epoch_micros))],
        )
        .unwrap();

    println!("Q3 Medium Result: {:?}", result_q3_medium);

    let result_q3_large = WorldType::Large
        .groupby_agg_two_keys_from_path_with_transforms(
            "passenger_count",
            "tpep_pickup_datetime", // Timestamp(Microsecond, None)
            "total_amount",         // TODO: enable stars for count(*)
            Aggregation::Count,
            path,
            &[None, Some(Box::new(to_year_from_epoch_micros))],
        )
        .unwrap();

    println!("Q3 Large Result: {:?}", result_q3_large);

    let result_q3_inv_large = WorldType::Large
        .groupby_agg_two_keys_from_path_with_transforms(
            "tpep_pickup_datetime", // Timestamp(Microsecond, None)
            "passenger_count",
            "total_amount", // TODO: enable stars for count(*)
            Aggregation::Avg,
            path,
            &[Some(Box::new(to_year_from_epoch_micros)), None],
        )
        .unwrap();

    println!("Q3 (order swapped) Large Result: {:?}", result_q3_inv_large);

    // Q4:
    // SELECT passenger_count, toYear(pickup_date) AS year, round(trip_distance) AS distance, count(*)
    // FROM trips_mergetree
    // GROUP BY passenger_count, year, distance
    // ORDER BY year, count(*) DESC;
    let mut result_q4_medium: Vec<((u64, u64, u64), f64)> = WorldType::Medium
        .groupby_agg_three_keys_from_path_with_transforms(
            "passenger_count",
            "tpep_pickup_datetime",
            "trip_distance",
            "total_amount", // any non-null numeric column for COUNT(*)
            Aggregation::Count,
            path,
            &[
                None,
                Some(Box::new(to_year_from_epoch_micros)),
                Some(Box::new(round_from_f64_bits)),
            ],
        )?
        .into_iter()
        .collect();

    result_q4_medium.sort_by(|a, b| {
        let year_cmp = a.0 .1.cmp(&b.0 .1);
        if year_cmp != Ordering::Equal {
            return year_cmp;
        }
        b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal)
    });
    println!("Q4 Medium Result: {:?}", result_q4_medium);

    let mut result_q4_large: Vec<((u64, u64, u64), f64)> = WorldType::Large
        .groupby_agg_three_keys_from_path_with_transforms(
            "passenger_count",
            "tpep_pickup_datetime",
            "trip_distance",
            "total_amount", // any non-null numeric column for COUNT(*)
            Aggregation::Count,
            path,
            &[
                None,
                Some(Box::new(to_year_from_epoch_micros)),
                Some(Box::new(round_from_f64_bits)),
            ],
        )?
        .into_iter()
        .collect();

    result_q4_large.sort_by(|a, b| {
        let year_cmp = a.0 .1.cmp(&b.0 .1);
        if year_cmp != Ordering::Equal {
            return year_cmp;
        }
        b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal)
    });
    println!("Q4 Large Result: {:?}", result_q4_large);

    // Q5: SELECT toYYYYMMDD(timestamp) AS day, count() FROM sensors GROUP BY day ORDER BY day ASC
    let sensors_path = "/Users/hyunseokoh/hyunseoo/notes/cdp/src/data/subsets/environmental_sensors_2019_06_subset_200k.parquet";
    let sensor_records = read_sensors_yyyymmdd_count(sensors_path, "timestamp")?;
    println!("Loaded {} sensor records", sensor_records.len());

    let result_q5_medium = WorldType::Medium.groupby_agg(&sensor_records, Aggregation::Count);
    let mut result_q5_medium_sorted: Vec<(u64, f64)> = result_q5_medium.into_iter().collect();
    result_q5_medium_sorted.sort_by_key(|(day, _)| *day);
    println!("Q5 Medium Result (sensors count by day): {:?}", result_q5_medium_sorted);

    let result_q5_large = WorldType::Large.groupby_agg(&sensor_records, Aggregation::Count);
    let mut result_q5_large_sorted: Vec<(u64, f64)> = result_q5_large.into_iter().collect();
    result_q5_large_sorted.sort_by_key(|(day, _)| *day);
    println!("Q5 Large Result (sensors count by day): {:?}", result_q5_large_sorted);

    // Q6: SELECT machine_name, AVG(COALESCE(cpu_user, 0.0)) AS cpu FROM logs1 GROUP BY machine_name
    let brown_path = "/Users/hyunseokoh/hyunseoo/notes/cdp/src/data/subsets/brown_mgbench1_subset_200k.parquet";
    let brown_records = read_parquet_string_key_to_records(brown_path, "machine_name", "cpu_user")?;
    println!("Loaded {} brown records", brown_records.len());

    let result_q6_medium = WorldType::Medium.groupby_agg(&brown_records, Aggregation::Avg);
    println!("Q6 Medium Result (brown avg cpu by machine, keys are FNV hashes of machine names): {:?}", result_q6_medium);

    let result_q6_large = WorldType::Large.groupby_agg(&brown_records, Aggregation::Avg);
    println!("Q6 Large Result (brown avg cpu by machine, keys are FNV hashes of machine names): {:?}", result_q6_large);

    Ok(())
}
