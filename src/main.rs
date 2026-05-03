use anyhow::{bail, Result};
use cdp::worlds::types::{Aggregation, Record, TimedResult, WorldType};
use cdp::worlds::util::{
    read_parquet_single_column, read_parquet_string_key_to_records, read_parquet_to_records,
    read_parquet_to_records_three_keys_with_transforms,
    read_parquet_to_records_two_keys_with_transforms, read_sensors_yyyymmdd_count,
    round_from_f64_bits, to_year_from_epoch_micros,
};
use clap::Parser;
use mimalloc::MiMalloc;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Instant;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const TAXI_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/data/yellow_tripdata_2025-01.parquet"
);
const SENSORS_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/data/subsets/environmental_sensors_2019_06_subset_200k.parquet"
);
const BROWN_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/data/subsets/brown_mgbench1_subset_200k.parquet"
);

#[derive(Parser, Debug)]
#[command(name = "cdp")]
#[command(about = "Run CDP baseline queries against bundled Parquet datasets")]
struct Args {
    /// Query to run: q1, q2, q3, q4, q5, q6, or all
    #[arg(long, default_value = "all")]
    query: String,

    /// World to run: small, medium, large, large-buffered, or all
    #[arg(long, default_value = "all")]
    world: String,

    /// Number of rows to print from each result set
    #[arg(long, default_value_t = 10)]
    limit: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let worlds = parse_worlds(&args.world)?;
    let limit = args.limit.max(1);

    match args.query.to_lowercase().as_str() {
        "q1" => run_q1(&worlds, limit)?,
        "q2" => run_q2(&worlds, limit)?,
        "q3" => run_q3(&worlds, limit)?,
        "q4" => run_q4(&worlds, limit)?,
        "q5" => run_q5(&worlds, limit)?,
        "q6" => run_q6(&worlds, limit)?,
        "all" => {
            run_q1(&worlds, limit)?;
            run_q2(&worlds, limit)?;
            run_q3(&worlds, limit)?;
            run_q4(&worlds, limit)?;
            run_q5(&worlds, limit)?;
            run_q6(&worlds, limit)?;
        }
        other => bail!("unsupported query '{other}', expected q1..q6 or all"),
    }

    Ok(())
}

fn parse_worlds(input: &str) -> Result<Vec<WorldType>> {
    if input.eq_ignore_ascii_case("all") {
        Ok(vec![
            WorldType::Medium,
            WorldType::Large,
            WorldType::LargeBuffered,
        ])
    } else {
        let world = input
            .parse::<WorldType>()
            .map_err(|err| anyhow::anyhow!(err))?;
        match world {
            WorldType::Small => Err(anyhow::anyhow!(
                "world 'small' is a hardware placeholder and is not runnable in this software baseline"
            )),
            WorldType::Medium | WorldType::Large | WorldType::LargeBuffered => Ok(vec![world]),
        }
    }
}

fn run_q1(worlds: &[WorldType], limit: usize) -> Result<()> {
    let started = Instant::now();
    let records = read_parquet_single_column(TAXI_PATH, "passenger_count")?;
    let load_ms = started.elapsed().as_secs_f64() * 1000.0;

    print_section(
        "Q1 count(*) by passenger_count",
        records.len(),
        load_ms,
        run_worlds(worlds, &records, Aggregation::Count, limit),
    );
    Ok(())
}

fn run_q2(worlds: &[WorldType], limit: usize) -> Result<()> {
    let started = Instant::now();
    let records = read_parquet_to_records(TAXI_PATH, "passenger_count", "total_amount")?;
    let load_ms = started.elapsed().as_secs_f64() * 1000.0;

    print_section(
        "Q2 avg(total_amount) by passenger_count",
        records.len(),
        load_ms,
        run_worlds(worlds, &records, Aggregation::Avg, limit),
    );
    Ok(())
}

fn run_q3(worlds: &[WorldType], limit: usize) -> Result<()> {
    let started = Instant::now();
    let records = read_parquet_to_records_two_keys_with_transforms(
        TAXI_PATH,
        "passenger_count",
        "tpep_pickup_datetime",
        "total_amount",
        &[None, Some(Box::new(to_year_from_epoch_micros))],
    )?;
    let load_ms = started.elapsed().as_secs_f64() * 1000.0;

    print_section(
        "Q3 count(*) by (passenger_count, year)",
        records.len(),
        load_ms,
        run_worlds(worlds, &records, Aggregation::Count, limit),
    );
    Ok(())
}

fn run_q4(worlds: &[WorldType], limit: usize) -> Result<()> {
    let started = Instant::now();
    let records = read_parquet_to_records_three_keys_with_transforms(
        TAXI_PATH,
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
    let load_ms = started.elapsed().as_secs_f64() * 1000.0;

    println!("\nQ4 count(*) by (passenger_count, year, round(distance))");
    println!("  load={load_ms:.1}ms records={}", records.len());

    for world in worlds {
        let started = Instant::now();
        let timing = world.groupby_agg_timed(&records, Aggregation::Count);
        let total_ms = started.elapsed().as_secs_f64() * 1000.0;
        let group_count = timing.result.len();
        let sample = format_q4_sample(timing.result, limit);
        println!(
            "  {:<6} update={:.1}ms finalize={:.1}ms total={total_ms:.1}ms groups={} sample={sample}",
            world,
            timing.t_update * 1000.0,
            timing.t_finalize * 1000.0,
            group_count,
        );
    }

    Ok(())
}

fn run_q5(worlds: &[WorldType], limit: usize) -> Result<()> {
    let started = Instant::now();
    let records = read_sensors_yyyymmdd_count(SENSORS_PATH, "timestamp")?;
    let load_ms = started.elapsed().as_secs_f64() * 1000.0;

    print_section(
        "Q5 sensors count(*) by toYYYYMMDD(timestamp)",
        records.len(),
        load_ms,
        run_worlds(worlds, &records, Aggregation::Count, limit),
    );
    Ok(())
}

fn run_q6(worlds: &[WorldType], limit: usize) -> Result<()> {
    let started = Instant::now();
    let records = read_parquet_string_key_to_records(BROWN_PATH, "machine_name", "cpu_user")?;
    let load_ms = started.elapsed().as_secs_f64() * 1000.0;

    print_section(
        "Q6 avg(COALESCE(cpu_user, 0.0)) by machine_name_hash",
        records.len(),
        load_ms,
        run_worlds(worlds, &records, Aggregation::Avg, limit),
    );
    Ok(())
}

fn run_worlds<K>(
    worlds: &[WorldType],
    records: &[Record<K>],
    agg: Aggregation,
    limit: usize,
) -> Vec<RunSummary>
where
    K: Clone + Debug + Ord + Send + Sync + std::hash::Hash + Eq,
{
    worlds
        .iter()
        .copied()
        .map(|world| {
            let started = Instant::now();
            let timing: TimedResult<K> = world.groupby_agg_timed(records, agg);
            let total_ms = started.elapsed().as_secs_f64() * 1000.0;
            let group_count = timing.result.len();
            RunSummary {
                world,
                update_ms: timing.t_update * 1000.0,
                finalize_ms: timing.t_finalize * 1000.0,
                total_ms,
                group_count,
                sample: format_sample(timing.result, limit),
            }
        })
        .collect()
}

fn print_section(title: &str, rows: usize, load_ms: f64, runs: Vec<RunSummary>) {
    println!("\n{title}");
    println!("  load={load_ms:.1}ms records={rows}");
    for run in runs {
        println!(
            "  {:<6} update={:.1}ms finalize={:.1}ms total={:.1}ms groups={} sample={}",
            run.world, run.update_ms, run.finalize_ms, run.total_ms, run.group_count, run.sample
        );
    }
}

fn format_sample<K>(map: HashMap<K, f64>, limit: usize) -> String
where
    K: Ord + Debug,
{
    let mut rows: Vec<(K, f64)> = map.into_iter().collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    format!("{:?}", &rows[..rows.len().min(limit)])
}

fn format_q4_sample(map: HashMap<(u64, u64, u64), f64>, limit: usize) -> String {
    let mut rows: Vec<((u64, u64, u64), f64)> = map.into_iter().collect();
    rows.sort_by(|a, b| {
        let year_cmp = a.0 .1.cmp(&b.0 .1);
        if year_cmp != Ordering::Equal {
            return year_cmp;
        }
        b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal)
    });
    format!("{:?}", &rows[..rows.len().min(limit)])
}

struct RunSummary {
    world: WorldType,
    update_ms: f64,
    finalize_ms: f64,
    total_ms: f64,
    group_count: usize,
    sample: String,
}
