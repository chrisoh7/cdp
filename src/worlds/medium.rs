use fnv::FnvBuildHasher;
use rayon::prelude::*;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::Hash;
use std::time::Instant;

use super::types::{AggState, Aggregation, Record, TimedResult};

pub fn medium_world<K>(records: &[Record<K>], agg: Aggregation) -> HashMap<K, AggState>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let (groups, _, _) = medium_world_core(records, agg, false);
    into_std_hashmap(groups)
}

/// Wrapper for end-to-end groupby aggregation (medium world)
pub fn groupby_agg<K>(records: &[Record<K>], agg: Aggregation) -> HashMap<K, f64>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let (groups, _, _) = medium_world_core(records, agg, false);

    // Finalize to scalar results
    let mut result = HashMap::with_capacity(groups.len());
    for (k, state) in groups {
        result.insert(k, state.finalize());
    }

    result
}

pub fn groupby_agg_timed<K>(records: &[Record<K>], agg: Aggregation) -> TimedResult<K>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let (groups, t_update, t_finalize) = medium_world_core(records, agg, true);

    let mut result = HashMap::with_capacity(groups.len());
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
    let (groups, t_update, t_finalize) = medium_world_core(records, agg, true);
    (into_std_hashmap(groups), t_update, t_finalize)
}

fn medium_world_core<K>(
    records: &[Record<K>],
    agg: Aggregation,
    record_timing: bool,
) -> (HashMap<K, AggState, FnvBuildHasher>, f64, f64)
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let start_update = record_timing.then(|| Instant::now());

    // Chunk size: one chunk per worker (rounded up) to keep partial map count low.
    let threads = rayon::current_num_threads().max(1);
    let chunk_size = ((records.len() + threads - 1) / threads).max(1);

    // Update phase: build thread-local partial maps to avoid shared lock contention.
    let partials: Vec<HashMap<K, AggState, FnvBuildHasher>> = records
        .par_chunks(chunk_size)
        .map(|chunk| {
            // Reserve aggressively to reduce rehashing in high-cardinality chunks.
            let mut local = HashMap::with_capacity_and_hasher(
                chunk.len().saturating_div(4).max(64),
                FnvBuildHasher::default(),
            );
            for r in chunk {
                local
                    .entry(r.key.clone())
                    .and_modify(|state: &mut AggState| state.update(r.value))
                    .or_insert_with(|| AggState::init(agg, r.value));
            }
            local
        })
        .collect();

    let t_update = start_update
        .map(|start| start.elapsed().as_secs_f64())
        .unwrap_or(0.0);

    let start_finalize = record_timing.then(|| Instant::now());
    // Finalize phase: merge partial maps in parallel (tree reduction).
    let merged: HashMap<K, AggState, FnvBuildHasher> =
        partials
            .into_par_iter()
            .reduce(
                || HashMap::with_hasher(FnvBuildHasher::default()),
                merge_two_partials,
            );

    let t_finalize = start_finalize
        .map(|start| start.elapsed().as_secs_f64())
        .unwrap_or(0.0);

    (merged, t_update, t_finalize)
}

fn merge_two_partials<K>(
    mut a: HashMap<K, AggState, FnvBuildHasher>,
    mut b: HashMap<K, AggState, FnvBuildHasher>,
) -> HashMap<K, AggState, FnvBuildHasher>
where
    K: Eq + Hash,
{
    // Merge the smaller map into the larger map to reduce probes and reallocations.
    if a.len() < b.len() {
        std::mem::swap(&mut a, &mut b);
    }
    a.reserve(b.len());
    for (k, state) in b.drain() {
        match a.entry(k) {
            Entry::Occupied(mut slot) => slot.get_mut().merge(state),
            Entry::Vacant(slot) => {
                slot.insert(state);
            }
        }
    }
    a
}

fn into_std_hashmap<K>(
    input: HashMap<K, AggState, FnvBuildHasher>,
) -> HashMap<K, AggState>
where
    K: Eq + Hash,
{
    let mut output = HashMap::with_capacity(input.len());
    output.extend(input);
    output
}
