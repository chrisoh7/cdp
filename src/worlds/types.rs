use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;

use super::util::{
    read_parquet_single_column, read_parquet_to_records,
    read_parquet_to_records_three_keys_with_transforms,
    read_parquet_to_records_two_keys_with_transforms,
};

//-------------------Type Definition-------------------//

#[derive(Clone, Debug)]
pub struct Record<K> {
    pub key: K,
    pub value: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Aggregation {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

#[derive(Clone, Debug)]
pub enum AggState {
    Sum(f64),
    Min(f64),
    Max(f64),
    Avg { sum: f64, count: usize },
    Count(f64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorldType {
    Small,
    Medium,
    Large,
}

pub struct TimedResult<K> {
    pub result: HashMap<K, f64>,
    pub t_update: f64,
    pub t_finalize: f64,
}

// Convenience aliases for the common single-key case
pub type U64Record = Record<u64>;
pub type U64PairRecord = Record<(u64, u64)>;
pub type U64TripleRecord = Record<(u64, u64, u64)>;
pub type U64TimedResult = TimedResult<u64>;

//-------------------Type Implementation-------------------//

impl Aggregation {
    pub fn as_str(self) -> &'static str {
        match self {
            Aggregation::Sum => "sum",
            Aggregation::Min => "min",
            Aggregation::Max => "max",
            Aggregation::Avg => "avg",
            Aggregation::Count => "count",
        }
    }
}

impl fmt::Display for Aggregation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Aggregation {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sum" => Ok(Aggregation::Sum),
            "min" => Ok(Aggregation::Min),
            "max" => Ok(Aggregation::Max),
            "avg" => Ok(Aggregation::Avg),
            "count" => Ok(Aggregation::Count),
            other => Err(format!("unsupported aggregation: {other}")),
        }
    }
}

impl WorldType {
    pub fn as_str(self) -> &'static str {
        match self {
            WorldType::Small => "small",
            WorldType::Medium => "medium",
            WorldType::Large => "large",
        }
    }
}

impl fmt::Display for WorldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for WorldType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "small" => Ok(WorldType::Small),
            "medium" => Ok(WorldType::Medium),
            "large" => Ok(WorldType::Large),
            other => Err(format!("unsupported world: {other}")),
        }
    }
}

impl AggState {
    // Create an initial state from a value and aggregation type
    pub fn init(agg: Aggregation, value: f64) -> Self {
        match agg {
            Aggregation::Sum => AggState::Sum(value),
            Aggregation::Min => AggState::Min(value),
            Aggregation::Max => AggState::Max(value),
            Aggregation::Avg => AggState::Avg {
                sum: value,
                count: 1,
            },
            Aggregation::Count => AggState::Count(1.0),
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
            (AggState::Avg { sum: s1, count: c1 }, AggState::Avg { sum: s2, count: c2 }) => {
                *s1 += s2;
                *c1 += c2;
            }
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
    /// Convenience helper that still assumes a single-column u64 key.
    /// This keeps your parquet path unchanged for now.
    pub fn groupby_agg_from_path(
        &self,
        key: &str,
        val: &str,
        agg: Aggregation,
        path: &str,
    ) -> parquet::errors::Result<HashMap<u64, f64>> {
        // These helpers should return Vec<Record<u64>> now.
        let records: Vec<U64Record> = match agg {
            Aggregation::Count => read_parquet_single_column(path, key)?,
            _ => read_parquet_to_records(path, key, val)?,
        };
        Ok(self.groupby_agg(&records, agg))
    }

    /// Two-key version with optional transform lambdas on each key.
    pub fn groupby_agg_two_keys_from_path_with_transforms(
        &self,
        key1: &str,
        key2: &str,
        val: &str,
        agg: Aggregation,
        path: &str,
        key_transforms: &[Option<Box<dyn Fn(u64) -> u64>>],
    ) -> parquet::errors::Result<HashMap<(u64, u64), f64>> {
        let records: Vec<U64PairRecord> = read_parquet_to_records_two_keys_with_transforms(
            path,
            key1,
            key2,
            val,
            key_transforms,
        )?;
        Ok(self.groupby_agg(&records, agg))
    }

    /// Three-key version with optional transform lambdas on each key.
    pub fn groupby_agg_three_keys_from_path_with_transforms(
        &self,
        key1: &str,
        key2: &str,
        key3: &str,
        val: &str,
        agg: Aggregation,
        path: &str,
        key_transforms: &[Option<Box<dyn Fn(u64) -> u64>>],
    ) -> parquet::errors::Result<HashMap<(u64, u64, u64), f64>> {
        let records: Vec<U64TripleRecord> = read_parquet_to_records_three_keys_with_transforms(
            path,
            key1,
            key2,
            key3,
            val,
            key_transforms,
        )?;
        Ok(self.groupby_agg(&records, agg))
    }

    /// Generic in-memory groupby over arbitrary key type K.
    pub fn groupby_agg<K>(&self, records: &[Record<K>], agg: Aggregation) -> HashMap<K, f64>
    where
        K: Eq + Hash + Clone + Send + Sync,
    {
        match self {
            Self::Small => super::small::groupby_agg(records, agg),
            Self::Medium => super::medium::groupby_agg(records, agg),
            Self::Large => super::large::groupby_agg(records, agg),
        }
    }

    /// Convenience helper using u64 key when loading from parquet.
    pub fn groupby_agg_timed_from_path(
        &self,
        key: &str,
        val: &str,
        agg: Aggregation,
        path: &str,
    ) -> parquet::errors::Result<U64TimedResult> {
        let records: Vec<U64Record> = match agg {
            Aggregation::Count => read_parquet_single_column(path, key)?,
            _ => read_parquet_to_records(path, key, val)?,
        };
        Ok(self.groupby_agg_timed(&records, agg))
    }

    /// Generic timed groupby for arbitrary key type K.
    pub fn groupby_agg_timed<K>(&self, records: &[Record<K>], agg: Aggregation) -> TimedResult<K>
    where
        K: Eq + Hash + Clone + Send + Sync,
    {
        match self {
            Self::Small => super::small::groupby_agg_timed(records, agg),
            Self::Medium => super::medium::groupby_agg_timed(records, agg),
            Self::Large => super::large::groupby_agg_timed(records, agg),
        }
    }
}
