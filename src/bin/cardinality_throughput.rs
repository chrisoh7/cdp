use anyhow::Result;
use clap::Parser;
use rand::distributions::{Distribution, Uniform};
use rand::thread_rng;
use std::time::Instant;

use cdp::worlds::types::{Record, WorldType, TimedResult};

/// Benchmark throughput vs cardinality for CDP groupby implementations.
#[derive(Parser, Debug)]
#[command(name = "cardinality_throughput")]
#[command(about = "Measure throughput vs cardinality for CDP groupby implementations", long_about = None)]
struct Args {
    /// Number of records to generate per trial
    #[arg(short = 'n', long, default_value_t = 1_000_000)]
    num_records: usize,

    /// Maximum power of 10 for cardinality (e.g. 4 -> 10^4 = 10000)
    #[arg(short = 'p', long, default_value_t = 4)]
    max_pow: u32,

    /// Aggregation function (sum, count, avg, etc.)
    #[arg(short = 'a', long, default_value = "sum")]
    agg: String,

    /// World type (Small, Medium, Large, or All)
    #[arg(short = 'w', long, default_value = "Medium")]
    world: String,
}

fn all_worlds() -> Vec<WorldType> {
    // TODO: vec![WorldType::Small, WorldType::Medium, WorldType::Large]
    vec![WorldType::Medium, WorldType::Large]
}

fn parse_world(name: &str) -> Vec<WorldType> {
    match name.to_lowercase().as_str() {
        "small" => vec![WorldType::Small],
        "medium" => vec![WorldType::Medium],
        "large" => vec![WorldType::Large],
        "all" => all_worlds(),
        _ => {
            eprintln!("⚠️ Unknown world type '{}', defaulting to Medium", name);
            vec![WorldType::Medium]
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let num_records = args.num_records;
    let max_pow = args.max_pow;
    let agg = args.agg;
    let worlds = parse_world(&args.world);

    let mut rng = thread_rng();

    // Updated CSV header
    println!("world,cardinality,throughput_records_per_sec,t_medium,t_finalize");

    for world in worlds {
        for power in 1..=max_pow {
            let card = 10_i32.pow(power);
            let dist = Uniform::new(0, card);

            // Generate random records
            let records: Vec<Record> = (0..num_records)
                .map(|_| Record {
                    key: dist.sample(&mut rng) as u64,
                    value: 1.0,
                })
                .collect();

            // Time using groupby_agg_timed
            let start = Instant::now();
            let timing: TimedResult = world.groupby_agg_timed(&records, &agg);
            let total_elapsed = start.elapsed().as_secs_f64();

            let throughput = num_records as f64 / total_elapsed;

            println!(
                "{:?},{},{:.3},{:.6},{:.6}",
                world, card, throughput, timing.t_update, timing.t_finalize
            );
        }
    }

    Ok(())
}
