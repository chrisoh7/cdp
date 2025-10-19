use rayon::prelude::*;
use scc::HashMap as ConcurrentHashMap;
use std::collections::HashMap;
use super::types::{Record, AggState};

// Large world: single global concurrent hashmap, updated by all threads.
pub fn large_world(records: &[Record], agg: &str) -> HashMap<u64, AggState> {
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

    // Snapshot into a standard HashMap using iter_sync
    let mut out = HashMap::new();
    hashmap.iter_sync(|k, v| {
        out.insert(*k, v.clone());
        true // keep scanning
    });

    out
}

// Wrapper for end-to-end groupby aggregation (large world)
pub fn groupby_agg(records: &[Record], agg: &str) -> HashMap<u64, f64> {
    let groups = large_world(records, agg);

    // Finalize to scalar results
    let mut result = HashMap::new();
    for (k, state) in groups {
        result.insert(k, state.finalize());
    }

    result
}
