use scc::HashMap as ConcurrentHashMap;
use std::collections::HashMap;
use rayon::prelude::*;
use super::util::{Record, get_agg_op, get_agg_id};


// Large world: single global concurrent hashmap, updated by all threads.
pub fn large_world(
    records: &[Record],
    agg_op: &(dyn Fn(f64, f64) -> f64 + Send + Sync),
    agg_id: &(dyn Fn(f64) -> f64 + Send + Sync),
) -> HashMap<u64, f64> {
    let hashmap: ConcurrentHashMap<u64, f64> = ConcurrentHashMap::default();

    // Each thread atomically updates its key entry
    records.par_iter().for_each(|r| {
        hashmap.entry_sync(r.key)
            .and_modify(|v| *v = agg_op(*v, r.value))
            .or_insert(agg_id(r.value));
    });

    // Snapshot into a standard HashMap using iter_sync.
    let mut out = HashMap::new();
    hashmap.iter_sync(|k, v| {
        out.insert(*k, *v);
        true // keep iterating
    });

    out
}

pub fn groupby_agg(records: &[Record], agg: &str) -> HashMap<u64, f64> {
    // Large-world groupby agg
    large_world(records, &get_agg_op(agg), &get_agg_id(agg))
}
