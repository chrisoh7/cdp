use std::collections::HashMap;
use std::hash::Hash;

use super::types::{AggState, Aggregation, Record, TimedResult};

const SMALL_WORLD_PLACEHOLDER: &str =
    "WorldType::Small is a hardware placeholder and is not implemented in this software baseline.";

pub fn groupby_agg<K>(_records: &[Record<K>], _agg: Aggregation) -> HashMap<K, f64>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    panic!("{SMALL_WORLD_PLACEHOLDER}")
}

pub fn groupby_agg_timed<K>(_records: &[Record<K>], _agg: Aggregation) -> TimedResult<K>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    panic!("{SMALL_WORLD_PLACEHOLDER}")
}

pub fn small_world<K>(_records: &[Record<K>], _agg: Aggregation) -> HashMap<K, AggState>
where
    K: Eq + Hash + Clone + Send + Sync,
{
    panic!("{SMALL_WORLD_PLACEHOLDER}")
}
