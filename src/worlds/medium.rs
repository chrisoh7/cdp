use fnv::FnvBuildHasher;
use rayon::prelude::*;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::time::Instant;

use super::types::{AggState, Aggregation, Record, TimedResult};

pub fn medium_world<K>(records: &[Record<K>], agg: Aggregation) -> HashMap<K, AggState>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let (partitions, _, _) = medium_world_core(records, agg, false);
    let total = partitions.iter().map(|p| p.len()).sum();
    let mut output: HashMap<K, AggState> = HashMap::with_capacity(total);
    for partition in partitions {
        output.extend(partition);
    }
    output
}

pub fn groupby_agg<K>(records: &[Record<K>], agg: Aggregation) -> HashMap<K, f64>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let (partitions, _, _) = medium_world_core(records, agg, false);
    let total = partitions.iter().map(|p| p.len()).sum();
    let mut result = HashMap::with_capacity(total);
    for partition in partitions {
        for (k, state) in partition {
            result.insert(k, state.finalize());
        }
    }
    result
}

pub fn groupby_agg_timed<K>(records: &[Record<K>], agg: Aggregation) -> TimedResult<K>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let (partitions, t_update, t_finalize) = medium_world_core(records, agg, true);
    let total = partitions.iter().map(|p| p.len()).sum();
    let mut result = HashMap::with_capacity(total);
    for partition in partitions {
        for (k, state) in partition {
            result.insert(k, state.finalize());
        }
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
    let (partitions, t_update, t_finalize) = medium_world_core(records, agg, true);
    let total = partitions.iter().map(|p| p.len()).sum();
    let mut output: HashMap<K, AggState> = HashMap::with_capacity(total);
    for partition in partitions {
        output.extend(partition);
    }
    (output, t_update, t_finalize)
}

fn medium_world_core<K>(
    records: &[Record<K>],
    agg: Aggregation,
    record_timing: bool,
) -> (Vec<HashMap<K, AggState, FnvBuildHasher>>, f64, f64)
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let start_update = record_timing.then(|| Instant::now());

    let threads = rayon::current_num_threads().max(1);
    let chunk_size = ((records.len() + threads - 1) / threads).max(1);

    // Update phase: one thread-local partial map per chunk, zero contention.
    let partials: Vec<HashMap<K, AggState, FnvBuildHasher>> = records
        .par_chunks(chunk_size)
        .map(|chunk| {
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

    // Finalize phase: adaptive strategy selected by saturation ratio.
    //
    // Saturation = sum(partial.len()) / N:
    //   Low  (<80%) → many repeated keys → small final map → tree-reduce stays
    //                  cache-hot through all log₂T merge levels.
    //   High (≥80%) → mostly unique keys → final map ≈ N entries → tree-reduce's
    //                  root merge thrashes L3; partition scatter keeps each
    //                  thread's working set at 1/T the size.
    let total_partial_entries: usize = partials.iter().map(|p| p.len()).sum();
    let use_scatter = records.len() > 0 && total_partial_entries * 5 > records.len() * 4; // saturation > 80 %

    let partitions = if use_scatter {
        finalize_scatter(partials, threads)
    } else {
        finalize_tree_reduce(partials)
    };

    let t_finalize = start_finalize
        .map(|start| start.elapsed().as_secs_f64())
        .unwrap_or(0.0);

    (partitions, t_update, t_finalize)
}

// ── Finalize strategies ───────────────────────────────────────────────────────

/// Tree-reduce: merge partial maps pairwise in parallel (log₂T rounds).
/// Best when cardinality is low — partial maps are small and stay in L3.
fn finalize_tree_reduce<K>(
    partials: Vec<HashMap<K, AggState, FnvBuildHasher>>,
) -> Vec<HashMap<K, AggState, FnvBuildHasher>>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let merged = partials.into_par_iter().reduce(
        || HashMap::with_hasher(FnvBuildHasher::default()),
        merge_two_partials,
    );
    vec![merged]
}

/// Partition scatter: thread t owns keys where hash(key) % T == t.
/// Each thread scans all partial maps but writes to its own output — no
/// shared state, merge work is O(N/T) per thread.
/// Best when cardinality is high — limits each thread's working set to 1/T.
fn finalize_scatter<K>(
    partials: Vec<HashMap<K, AggState, FnvBuildHasher>>,
    threads: usize,
) -> Vec<HashMap<K, AggState, FnvBuildHasher>>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let total: usize = partials.iter().map(|p| p.len()).sum();
    let hasher_builder = FnvBuildHasher::default();
    (0..threads)
        .into_par_iter()
        .map(|t| {
            let capacity = (total / threads).max(16);
            let mut out: HashMap<K, AggState, FnvBuildHasher> =
                HashMap::with_capacity_and_hasher(capacity, FnvBuildHasher::default());
            for partial in &partials {
                for (k, state) in partial.iter() {
                    let mut h = hasher_builder.build_hasher();
                    k.hash(&mut h);
                    if (h.finish() as usize) % threads == t {
                        match out.entry(k.clone()) {
                            Entry::Occupied(mut slot) => slot.get_mut().merge(state.clone()),
                            Entry::Vacant(slot) => {
                                slot.insert(state.clone());
                            }
                        }
                    }
                }
            }
            out
        })
        .collect()
}

fn merge_two_partials<K>(
    mut a: HashMap<K, AggState, FnvBuildHasher>,
    mut b: HashMap<K, AggState, FnvBuildHasher>,
) -> HashMap<K, AggState, FnvBuildHasher>
where
    K: Eq + Hash,
{
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
