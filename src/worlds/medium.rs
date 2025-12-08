use rayon::prelude::*;
use std::collections::HashMap;
use std::hash::Hash;
use std::time::Instant;

use super::types::{Aggregation, AggState, Record, TimedResult};

pub fn medium_world<K>(records: &[Record<K>], agg: Aggregation) -> HashMap<K, AggState>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    medium_world_core(records, agg, false).0
}

/// Wrapper for end-to-end groupby aggregation (medium world)
pub fn groupby_agg<K>(records: &[Record<K>], agg: Aggregation) -> HashMap<K, f64>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let groups = medium_world(records, agg);

    // Finalize to scalar results
    let mut result = HashMap::new();
    for (k, state) in groups {
        result.insert(k, state.finalize());
    }

    result
}

pub fn groupby_agg_timed<K>(records: &[Record<K>], agg: Aggregation) -> TimedResult<K>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let (groups, t_update, t_finalize) = medium_world_timed(records, agg);

    let mut result = HashMap::new();
    for (k, state) in groups {
        result.insert(k, state.finalize());
    }

    TimedResult {
        result,
        t_update,
        t_finalize,
    }
}

pub fn medium_world_timed<K>(
    records: &[Record<K>],
    agg: Aggregation,
) -> (HashMap<K, AggState>, f64, f64)
where
    K: Eq + Hash + Clone + Send + Sync,
{
    medium_world_core(records, agg, true)
}

fn medium_world_core<K>(
    records: &[Record<K>],
    agg: Aggregation,
    record_timing: bool,
) -> (HashMap<K, AggState>, f64, f64)
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let start_update = record_timing.then(|| Instant::now());
    let partials: Vec<HashMap<K, AggState>> = records
        .par_chunks(10_000)
        .map(|chunk| {
            let mut local: HashMap<K, AggState> = HashMap::new();
            for r in chunk {
                local
                    .entry(r.key.clone())
                    .and_modify(|state: &mut AggState| state.update(r.value))
                    .or_insert(AggState::init(agg, r.value));
            }
            local
        })
        .collect();
    let t_update = start_update
        .map(|start| start.elapsed().as_secs_f64())
        .unwrap_or(0.0);

    let start_finalize = record_timing.then(|| Instant::now());
    let result = partials.into_par_iter().reduce(
        || HashMap::new(),
        |mut a, b| {
            for (k, state_b) in b {
                a.entry(k)
                    .and_modify(|state_a| state_a.merge(state_b.clone()))
                    .or_insert(state_b);
            }
            a
        },
    );
    let t_finalize = start_finalize
        .map(|start| start.elapsed().as_secs_f64())
        .unwrap_or(0.0);

    (result, t_update, t_finalize)
}
