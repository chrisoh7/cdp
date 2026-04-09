use dashmap::DashMap;
use rayon::prelude::*;
use std::collections::HashMap;
use std::hash::Hash;
use std::time::Instant;

use super::types::{AggState, Aggregation, Record, TimedResult};

// Large world: single global sharded-lock hashmap (DashMap), updated by all threads.
//
// DashMap partitions the key space into N independent shards (default = num_cpus × 4),
// each guarded by an RwLock. This eliminates the per-entry EBR cost of scc::HashMap and
// avoids extreme contention collapse on low-cardinality queries.

pub fn large_world<K>(records: &[Record<K>], agg: Aggregation) -> DashMap<K, AggState>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let map: DashMap<K, AggState> = DashMap::new();

    records.par_chunks(4096).for_each(|chunk| {
        for r in chunk {
            map.entry(r.key.clone())
                .and_modify(|state| state.update(r.value))
                .or_insert_with(|| AggState::init(agg, r.value));
        }
    });

    map
}

pub fn groupby_agg<K>(records: &[Record<K>], agg: Aggregation) -> HashMap<K, f64>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let map = large_world(records, agg);

    let mut result = HashMap::with_capacity(map.len());
    for entry in map {
        result.insert(entry.0, entry.1.finalize());
    }
    result
}

pub fn groupby_agg_timed<K>(records: &[Record<K>], agg: Aggregation) -> TimedResult<K>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let (map, t_update, t_finalize) = large_world_timed(records, agg);

    let mut result = HashMap::with_capacity(map.len());
    for entry in map {
        result.insert(entry.0, entry.1.finalize());
    }

    TimedResult {
        result,
        t_update,
        t_finalize,
    }
}

pub fn large_world_timed<K>(
    records: &[Record<K>],
    agg: Aggregation,
) -> (DashMap<K, AggState>, f64, f64)
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let map: DashMap<K, AggState> = DashMap::new();

    let start_update = Instant::now();
    records.par_chunks(4096).for_each(|chunk| {
        for r in chunk {
            map.entry(r.key.clone())
                .and_modify(|state| state.update(r.value))
                .or_insert_with(|| AggState::init(agg, r.value));
        }
    });
    let t_update = start_update.elapsed().as_secs_f64();

    let t_finalize = 0.0;
    (map, t_update, t_finalize)
}
