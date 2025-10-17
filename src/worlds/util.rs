use arrow::array::Array;
use std::fs::File;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::array::{Int64Array, Float64Array};
use std::collections::HashMap;


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

pub fn read_parquet_to_records(path: &str, key_str: &str, val_str: &str) -> parquet::errors::Result<Vec<Record>> {
    let file = File::open(path)?;
    
    // 1️. Create a RecordBatch reader directly
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut record_batch_reader = builder.build()?;

    let mut records = Vec::new();

    // 2️. Iterate over RecordBatches
    while let Some(batch_result) = record_batch_reader.next() {
        let batch = batch_result?;
        let schema = batch.schema();

        let key_idx = schema.index_of(key_str).unwrap();
        let val_idx = schema.index_of(val_str).unwrap();

        let key_array = batch
            .column(key_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let val_array = batch
            .column(val_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            if key_array.is_null(i) || val_array.is_null(i) {
                continue;
            }
            records.push(Record {
                key: key_array.value(i) as u64,
                value: val_array.value(i),
            });
        }
    }

    Ok(records)
}

// Special case for COUNT(*)
pub fn read_parquet_single_column(path: &str, key_str: &str) -> parquet::errors::Result<Vec<Record>> {
    let file = File::open(path)?;
    
    // 1️. Create a RecordBatch reader directly
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut record_batch_reader = builder.build()?;

    let mut records = Vec::new();

    // 2️. Iterate over RecordBatches
    while let Some(batch_result) = record_batch_reader.next() {
        let batch = batch_result?;
        let schema = batch.schema();

        let key_idx = schema.index_of(key_str).unwrap();

        let key_array = batch
            .column(key_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            if key_array.is_null(i) {
                continue;
            }
            records.push(Record {
                key: key_array.value(i) as u64,
                value: 1.0,
            });
        }
    }

    Ok(records)
}

#[derive(Clone, Debug)]
pub enum WorldType {
    Small, 
    Medium, 
    Large,
}

impl WorldType {
    pub fn groupby(
        &self,
        key: &str,
        val: &str,
        agg: &str,
        path: &str,
    ) -> parquet::errors::Result<HashMap<u64, f64>> {
        if agg == "count" {
            let records = read_parquet_single_column(path, key)?;
            println!("Loaded {} records", records.len());
        }
        let records = match agg {
            "count" => read_parquet_single_column(path, key)?,
            _ => read_parquet_to_records(path, key, val)?,
        };
        // let records = read_parquet_to_records(path, key, val)?;
        println!("Loaded {} records", records.len());

        let result = match self {
            // Self::Small => super::small::groupby_agg(&records, agg),
            Self::Medium => super::medium::groupby_agg(&records, agg),
            Self::Large => super::large::groupby_agg(&records, agg),
            _ => panic!("WorldType not implemented yet"),
        };

        Ok(result)
    }
}
