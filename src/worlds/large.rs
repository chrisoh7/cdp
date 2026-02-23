use rayon::prelude::*;
use scc::HashMap as ConcurrentHashMap;
use std::collections::HashMap;
use std::hash::Hash;
use std::time::Instant;

use super::types::{AggState, Aggregation, Record, TimedResult};

// Large world: single global concurrent hashmap, updated by all threads.
pub fn large_world<K>(records: &[Record<K>], agg: Aggregation) -> ConcurrentHashMap<K, AggState>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let hashmap: ConcurrentHashMap<K, AggState> = ConcurrentHashMap::with_capacity(records.len());

    // Each thread atomically updates its key entry
    records.par_chunks(4096).for_each(|chunk| {
        for r in chunk {
            hashmap
                .entry_sync(r.key.clone())
                .and_modify(|state| {
                    state.update(r.value);
                })
                .or_insert_with(|| AggState::init(agg, r.value));
        }
    });

    hashmap
}

pub fn groupby_agg<K>(records: &[Record<K>], agg: Aggregation) -> HashMap<K, f64>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let hashmap = large_world(records, agg);

    // Snapshot + finalize in a single pass
    let mut result = HashMap::with_capacity(hashmap.len());
    hashmap.iter_sync(|k, v| {
        result.insert(k.clone(), v.finalize());
        true
    });

    result
}

pub fn groupby_agg_timed<K>(records: &[Record<K>], agg: Aggregation) -> TimedResult<K>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let (groups, t_update, t_finalize) = large_world_timed(records, agg);

    let mut result = HashMap::with_capacity(groups.len());
    groups.iter_sync(|k, v| {
        result.insert(k.clone(), v.finalize());
        true
    });

    TimedResult {
        result,
        t_update,
        t_finalize,
    }
}

pub fn large_world_timed<K>(
    records: &[Record<K>],
    agg: Aggregation,
) -> (ConcurrentHashMap<K, AggState>, f64, f64)
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let result: ConcurrentHashMap<K, AggState> = ConcurrentHashMap::with_capacity(records.len());

    let start_update = Instant::now();
    // Each thread atomically updates its key entry
    records.par_chunks(4096).for_each(|chunk| {
        for r in chunk {
            result
                .entry_sync(r.key.clone())
                .and_modify(|state| {
                    state.update(r.value);
                })
                .or_insert_with(|| AggState::init(agg, r.value));
        }
    });
    let t_update = start_update.elapsed().as_secs_f64();

    // If you later add a separate finalize timing, measure it here.
    let t_finalize = 0.0;

    (result, t_update, t_finalize)
}
