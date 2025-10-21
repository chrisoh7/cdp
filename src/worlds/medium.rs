
use rayon::prelude::*;
use std::collections::HashMap;
use super::types::{Record, AggState, TimedResult};
use std::time::Instant;


pub fn medium_world(records: &[Record], agg: &str) -> HashMap<u64, AggState> {
    records
        .par_iter()
        .fold(
            || HashMap::<u64, AggState>::new(),
            |mut local, r| {
                local
                    .entry(r.key)
                    .and_modify(|state| state.update(r.value))
                    .or_insert(AggState::init(agg, r.value));
                local
            },
        )
        .reduce(
            || HashMap::new(),
            |mut a, b| {
                for (k, state_b) in b {
                    a.entry(k)
                        .and_modify(|state_a| state_a.merge(state_b.clone()))
                        .or_insert(state_b);
                }
                a
            },
        )
}


/// Wrapper for end-to-end groupby aggregation (medium world)
pub fn groupby_agg(records: &[Record], agg: &str) -> HashMap<u64, f64> {
    let groups = medium_world(records, agg);

    // Finalize to scalar results
    let mut result = HashMap::new();
    for (k, state) in groups {
        result.insert(k, state.finalize());
    }

    result
}


pub fn groupby_agg_timed(records: &[Record], agg: &str) -> TimedResult {
    let (groups, t_update, t_finalize) = medium_world_timed(records, agg);

    let mut result = HashMap::new();
    for (k, state) in groups {
        result.insert(k, state.finalize());
    }

    TimedResult { result, t_update, t_finalize }
}

pub fn medium_world_timed(records: &[Record], agg: &str) -> (HashMap<u64, AggState>, f64, f64) {
    // 1️. Fold phase (per-thread local aggregation)
    let start_update = Instant::now();
    let partials: Vec<HashMap<u64, AggState>> = records
        .par_chunks(10_000) 
        .map(|chunk| {
            let mut local = HashMap::new();
            for r in chunk {
                local
                    .entry(r.key)
                    .and_modify(|state: &mut AggState| state.update(r.value))
                    .or_insert(AggState::init(agg, r.value));
            }
            local
        })
        .collect();
    let t_update = start_update.elapsed().as_secs_f64();

    // 2. Reduce phase (merge partial maps)
    let start_finalize: Instant = Instant::now();
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
    let t_finalize = start_finalize.elapsed().as_secs_f64();

    (result, t_update, t_finalize)
}
