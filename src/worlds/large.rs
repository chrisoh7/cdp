use dashmap::{mapref::entry::Entry, DashMap};
use fnv::FnvBuildHasher;
use rayon::prelude::*;
use std::collections::HashMap;
use std::hash::Hash;
use std::time::Instant;

use super::types::{AggState, Aggregation, Record, TimedResult};

type GlobalMap<K> = DashMap<K, AggState, FnvBuildHasher>;

const RAW_CHUNK_SIZE: usize = 4_096;
const MIN_BUFFERED_CHUNK_SIZE: usize = 16_384;

pub fn large_world<K>(records: &[Record<K>], agg: Aggregation) -> GlobalMap<K>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let (map, _, _) = large_world_core(records, agg, false);
    map
}

pub fn groupby_agg<K>(records: &[Record<K>], agg: Aggregation) -> HashMap<K, f64>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let map = large_world(records, agg);
    finalize_map(map)
}

pub fn groupby_agg_timed<K>(records: &[Record<K>], agg: Aggregation) -> TimedResult<K>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let (map, t_update, t_finalize) = large_world_timed(records, agg);

    TimedResult {
        result: finalize_map(map),
        t_update,
        t_finalize,
    }
}

pub fn large_world_timed<K>(records: &[Record<K>], agg: Aggregation) -> (GlobalMap<K>, f64, f64)
where
    K: Eq + Hash + Clone + Send + Sync,
{
    large_world_core(records, agg, true)
}

pub fn large_buffered_world<K>(records: &[Record<K>], agg: Aggregation) -> GlobalMap<K>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let (map, _, _) = large_buffered_world_core(records, agg, false);
    map
}

pub fn buffered_groupby_agg<K>(records: &[Record<K>], agg: Aggregation) -> HashMap<K, f64>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let map = large_buffered_world(records, agg);
    finalize_map(map)
}

pub fn buffered_groupby_agg_timed<K>(records: &[Record<K>], agg: Aggregation) -> TimedResult<K>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let (map, t_update, t_finalize) = large_buffered_world_timed(records, agg);

    TimedResult {
        result: finalize_map(map),
        t_update,
        t_finalize,
    }
}

pub fn large_buffered_world_timed<K>(
    records: &[Record<K>],
    agg: Aggregation,
) -> (GlobalMap<K>, f64, f64)
where
    K: Eq + Hash + Clone + Send + Sync,
{
    large_buffered_world_core(records, agg, true)
}

fn large_world_core<K>(
    records: &[Record<K>],
    agg: Aggregation,
    record_timing: bool,
) -> (GlobalMap<K>, f64, f64)
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let threads = rayon::current_num_threads().max(1);
    let shard_amount = (threads * 4).next_power_of_two().max(2);
    let estimated_groups = records.len().saturating_div(4).max(64);
    let map: GlobalMap<K> = DashMap::with_capacity_and_hasher_and_shard_amount(
        estimated_groups,
        FnvBuildHasher::default(),
        shard_amount,
    );

    let start_update = record_timing.then(Instant::now);

    records.par_chunks(RAW_CHUNK_SIZE).for_each(|chunk| {
        for record in chunk {
            map.entry(record.key.clone())
                .and_modify(|state| state.update(record.value))
                .or_insert_with(|| AggState::init(agg, record.value));
        }
    });

    let t_update = start_update
        .map(|start| start.elapsed().as_secs_f64())
        .unwrap_or(0.0);
    (map, t_update, 0.0)
}

fn large_buffered_world_core<K>(
    records: &[Record<K>],
    agg: Aggregation,
    record_timing: bool,
) -> (GlobalMap<K>, f64, f64)
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let threads = rayon::current_num_threads().max(1);
    let shard_amount = (threads * 4).next_power_of_two().max(2);
    let chunk_size = ((records.len() + threads - 1) / threads).max(MIN_BUFFERED_CHUNK_SIZE);
    let estimated_groups = records.len().saturating_div(4).max(64);
    let map: GlobalMap<K> = DashMap::with_capacity_and_hasher_and_shard_amount(
        estimated_groups,
        FnvBuildHasher::default(),
        shard_amount,
    );

    let start_update = record_timing.then(Instant::now);

    records.par_chunks(chunk_size).for_each(|chunk| {
        let mut local = HashMap::with_capacity_and_hasher(
            chunk.len().saturating_div(4).max(64),
            FnvBuildHasher::default(),
        );

        for record in chunk {
            local
                .entry(record.key.clone())
                .and_modify(|state: &mut AggState| state.update(record.value))
                .or_insert_with(|| AggState::init(agg, record.value));
        }

        for (key, state) in local {
            match map.entry(key) {
                Entry::Occupied(mut slot) => slot.get_mut().merge(state),
                Entry::Vacant(slot) => {
                    slot.insert(state);
                }
            }
        }
    });

    let t_update = start_update
        .map(|start| start.elapsed().as_secs_f64())
        .unwrap_or(0.0);
    (map, t_update, 0.0)
}

fn finalize_map<K>(map: GlobalMap<K>) -> HashMap<K, f64>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    let mut result = HashMap::with_capacity(map.len());
    for entry in map {
        result.insert(entry.0, entry.1.finalize());
    }
    result
}
