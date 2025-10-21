use rayon::prelude::*;
use scc::HashMap as ConcurrentHashMap;
use std::collections::HashMap;
use super::types::{Record, AggState, TimedResult};
use std::time::Instant;

// Large world: single global concurrent hashmap, updated by all threads.
pub fn large_world(records: &[Record], agg: &str) -> ConcurrentHashMap<u64, AggState> {
    let hashmap: ConcurrentHashMap<u64, AggState> = ConcurrentHashMap::default();

    // Each thread atomically updates its key entry
    records.par_iter().for_each(|r| {
        hashmap
            .entry_sync(r.key)
            .and_modify(|state| {
                state.update(r.value);
            })
            .or_insert_with(|| AggState::init(agg, r.value));
    });

    hashmap
}
pub fn groupby_agg(records: &[Record], agg: &str) -> HashMap<u64, f64> {
    let hashmap = large_world(records, agg);

    // 2️. Snapshot + finalize in a single pass
    let mut result = HashMap::new();
    hashmap.iter_sync(|k, v| {
        result.insert(*k, v.finalize());
        true
    });

    result
}

pub fn groupby_agg_timed(records: &[Record], agg: &str) -> TimedResult {
    let (groups, t_update, t_finalize ) = large_world_timed(records, agg);

    let mut result = HashMap::new();
    groups.iter_sync(|k, v| {
        result.insert(*k, v.finalize());
        true
    });

    TimedResult { result, t_update, t_finalize }
}


pub fn large_world_timed(records: &[Record], agg: &str) -> (ConcurrentHashMap<u64, AggState>, f64, f64) {
    let result: ConcurrentHashMap<u64, AggState> = ConcurrentHashMap::default();
    let start_update = Instant::now();
    // Each thread atomically updates its key entry
    records.par_iter().for_each(|r| {
        result
            .entry_sync(r.key)
            .and_modify(|state| {
                state.update(r.value);
            })
            .or_insert_with(|| AggState::init(agg, r.value));
    });
    let t_update = start_update.elapsed().as_secs_f64();

    let t_finalize = 0.0;

    (result, t_update, t_finalize)
}
