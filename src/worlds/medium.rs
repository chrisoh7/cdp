
use rayon::prelude::*;
use std::collections::HashMap;
use super::types::{Record, AggState, AggRowState, MultiRecord};

pub fn medium_world(records: &[Record], agg: &str) -> HashMap<u64, AggState> {
    records
        .par_iter()
        .fold(
            || HashMap::<u64, AggState>::new(),
            |mut local, r| {
                local
                    .entry(r.key)
                    .and_modify(|state| state.update(r.value))
                    .or_insert(AggState::new_from_agg(agg, r.value));
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


pub fn medium_world_multi(records: &[MultiRecord], aggs: &[&str]) 
    -> HashMap<Vec<u64>, AggRowState> {
    records
        .par_iter()
        .fold(
            || HashMap::<Vec<u64>, AggRowState>::new(),
            |mut local, r| {
                local
                    .entry(r.keys.clone())
                    .and_modify(|state| state.update(&r.values))
                    .or_insert(AggRowState::new(aggs, &r.values));
                local
            },
        )
        .reduce(
            || HashMap::new(),
            |mut a, b| {
                for (k, v) in b {
                    a.entry(k)
                        .and_modify(|state| state.merge(v.clone()))
                        .or_insert(v);
                }
                a
            },
        )
}

pub fn groupby_multi(records: &[MultiRecord], aggs: &[&str]) 
    -> HashMap<Vec<u64>, Vec<f64>> {
    let groups = medium_world_multi(records, aggs);

    // Finalize results into numeric vectors
    let mut result = HashMap::new();
    for (k, state) in groups {
        result.insert(k, state.finalize());
    }

    result
}
