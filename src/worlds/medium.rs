
use rayon::prelude::*;
use std::collections::HashMap;
use super::util::{Record, get_agg_op, get_agg_id, get_reduce_op, AggState};

pub fn groupby_agg(records: &[Record], agg: &str) -> HashMap<u64, AggState> {
    // Medium-world groupby agg
    medium_world(records, &agg)
}

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