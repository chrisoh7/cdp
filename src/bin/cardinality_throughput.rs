use anyhow::{bail, Result};
use clap::Parser;
use rand::distributions::{Distribution, Uniform};
use rand::thread_rng;
use std::time::Instant;

use cdp::worlds::types::{Aggregation, Record, TimedResult, WorldType};

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

    /// World type (Medium, Large, or All)
    #[arg(short = 'w', long, default_value = "Medium")]
    world: String,
}

fn all_worlds() -> Vec<WorldType> {
    vec![WorldType::Medium, WorldType::Large]
}

fn parse_world(name: &str) -> Result<Vec<WorldType>> {
    if name.eq_ignore_ascii_case("all") {
        Ok(all_worlds())
    } else {
        match name.parse::<WorldType>() {
            Ok(WorldType::Medium) => Ok(vec![WorldType::Medium]),
            Ok(WorldType::Large) => Ok(vec![WorldType::Large]),
            Ok(WorldType::Small) => {
                bail!("world 'small' is a hardware placeholder and is not runnable in this software baseline")
            }
            Err(_) => bail!("unsupported world '{name}', expected medium, large, or all"),
        }
    }
}

/// Convert internal world names to display labels
fn world_label(world: &WorldType) -> &'static str {
    match world {
        WorldType::Medium => "PerThreadLocal",
        WorldType::Large => "Global",
        WorldType::Small => "Small",
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let num_records = args.num_records;
    let max_pow = args.max_pow;
    let agg: Aggregation = args
        .agg
        .parse()
        .unwrap_or_else(|e| panic!("invalid aggregation '{}': {}", args.agg, e));
    let worlds = parse_world(&args.world)?;

    let mut rng = thread_rng();

    // CSV header
    println!("world,cardinality,throughput_records_per_sec,t_update,t_finalize");

    for world in worlds {
        for power in 1..=max_pow {
            let card = 10_u64.pow(power);
            let dist = Uniform::new(0_u64, card);

            // Generate random records with u64 key
            let records: Vec<Record<u64>> = (0..num_records)
                .map(|_| Record {
                    key: dist.sample(&mut rng),
                    value: 1.0,
                })
                .collect();

            // Time using groupby_agg_timed
            let start = Instant::now();
            let timing: TimedResult<u64> = world.groupby_agg_timed(&records, agg);
            let total_elapsed = start.elapsed().as_secs_f64();

            let throughput = num_records as f64 / total_elapsed;

            // Use mapped display label instead of Debug name
            println!(
                "{},{},{:.3},{:.6},{:.6}",
                world_label(&world),
                card, // u64, prints as integer
                throughput,
                timing.t_update,
                timing.t_finalize
            );
        }
    }

    Ok(())
}
