use std::collections::HashMap;
use super::util::{read_parquet_single_column, read_parquet_to_records};

//-------------------Type Definition-------------------//

#[derive(Clone, Debug)]
pub struct Record {
    pub key: u64,
    pub value: f64,
}

#[derive(Clone, Debug)]
pub enum AggState {
    Sum(f64),
    Min(f64),
    Max(f64),
    Avg { sum: f64, count: usize },
    Count(f64),
}

#[derive(Clone, Debug)]
pub enum WorldType {
    Small, 
    Medium, 
    Large,
}

#[derive(Clone, Debug)]
pub struct MultiRecord {
    pub keys: Vec<u64>,
    pub values: Vec<f64>,
}

#[derive(Clone, Debug)]
pub struct AggRowState {
    pub states: Vec<AggState>,
}

//-------------------Type Implementation-------------------//

impl AggState {
    // Create an initial state from a value and aggregation type
    pub fn new_from_agg(agg: &str, value: f64) -> Self {
        match agg {
            "sum" => AggState::Sum(value),
            "min" => AggState::Min(value),
            "max" => AggState::Max(value),
            "avg" => AggState::Avg { sum: value, count: 1 },
            "count" => AggState::Count(1.0),
            _ => panic!("unsupported aggregation: {}", agg),
        }
    }

    // Update the running aggregate with a new record value
    pub fn update(&mut self, value: f64) {
        match self {
            AggState::Sum(v) => *v += value,
            AggState::Min(v) => *v = v.min(value),
            AggState::Max(v) => *v = v.max(value),
            AggState::Avg { sum, count } => {
                *sum += value;
                *count += 1;
            }
            AggState::Count(v) => *v += 1.0,
        }
    }

    // Merge another AggState of the same kind
    pub fn merge(&mut self, other: AggState) {
        match (self, other) {
            (AggState::Sum(a), AggState::Sum(b)) => *a += b,
            (AggState::Min(a), AggState::Min(b)) => *a = a.min(b),
            (AggState::Max(a), AggState::Max(b)) => *a = a.max(b),
            (
                AggState::Avg { sum: s1, count: c1 },
                AggState::Avg { sum: s2, count: c2 },
            ) => {
                *s1 += s2;
                *c1 += c2;
            },
            (AggState::Count(a), AggState::Count(b)) => *a += b,
            _ => panic!("Mismatched aggregate types during merge"),
        }
    }

    // Compute the final output value
    pub fn finalize(&self) -> f64 {
        match self {
            AggState::Sum(v) => *v,
            AggState::Min(v) => *v,
            AggState::Max(v) => *v,
            AggState::Avg { sum, count } => *sum / *count as f64,
            AggState::Count(v) => *v,
        }
    }
}

impl WorldType {
    pub fn groupby(
        &self,
        key: &str,
        val: &str,
        agg: &str,
        path: &str,
    ) -> parquet::errors::Result<HashMap<u64, f64>> {
        let records = match agg {
            "count" => read_parquet_single_column(path, key)?,
            _ => read_parquet_to_records(path, key, val)?,
        };
        println!("Loaded {} records", records.len());

        let result = match self {
            // Self::Small => super::small::groupby_agg(&records, agg),
            Self::Medium => super::medium::groupby_agg(&records, agg),
            Self::Large => super::large::groupby_agg(&records, agg),
            _ => panic!("WorldType not implemented yet"),
        };

        Ok(result)
    }

    pub fn groupby_multi(
        &self,
        keys: &[&str],
        vals: &[&str],
        aggs: &[&str],
        path: &str,
    ) -> parquet::errors::Result<HashMap<Vec<u64>, Vec<f64>>> {
        // 1️. Read the Parquet file into Vec<MultiRecord>
        let records = super::util::read_parquet_to_multirecords(path, keys, vals)?;
        println!(
            "Loaded {} records with {} keys and {} values",
            records.len(),
            keys.len(),
            vals.len()
        );

        // 2️. Dispatch to the correct world implementation
        let result = match self {
            Self::Medium => super::medium::groupby_multi(&records, aggs),
            _ => panic!("WorldType not implemented yet"),
        };

        Ok(result)
    }
}

impl AggRowState {
    pub fn new(aggs: &[&str], values: &[f64]) -> Self {
        let states = aggs
            .iter()
            .zip(values.iter())
            .map(|(agg, &val)| AggState::new_from_agg(agg, val))
            .collect();
        Self { states }
    }

    pub fn update(&mut self, values: &[f64]) {
        for (state, &val) in self.states.iter_mut().zip(values.iter()) {
            state.update(val);
        }
    }

    pub fn merge(&mut self, other: AggRowState) {
        for (s1, s2) in self.states.iter_mut().zip(other.states.into_iter()) {
            s1.merge(s2);
        }
    }

    pub fn finalize(&self) -> Vec<f64> {
        self.states.iter().map(|s| s.finalize()).collect()
    }
}
