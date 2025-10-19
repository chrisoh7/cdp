use arrow::array::Array;
use std::fs::File;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::array::*;
use super::types::Record;
use arrow::datatypes::DataType;
use arrow::compute::cast;
use std::sync::Arc;
use parquet::errors::ParquetError;

/// Normalize an Arrow column to the given target type, if needed.
/// Returns an `Arc<dyn Array>` of that target type.
pub fn normalize_column(
    arr: &Arc<dyn Array>,
    target: &DataType,
) -> Result<Arc<dyn Array>, ParquetError> {
    let current = arr.data_type();

    if current == target {
        return Ok(Arc::clone(arr));
    }

    match (current, target) {
        // allow safe numeric promotions (e.g., Float64 -> Int64, UInt64 -> Int64)
        (DataType::Float64, DataType::Int64)
        | (DataType::UInt64, DataType::Int64)
        | (DataType::Int64, DataType::Float64)
        | (DataType::UInt64, DataType::Float64)
        | (DataType::Int32, DataType::Float64)
        | (DataType::UInt32, DataType::Float64) => {
            cast(arr, target).map_err(|e| ParquetError::General(format!("cast error: {}", e)))
        }

        _ => Err(ParquetError::General(format!(
            "Unsupported cast: {:?} -> {:?}",
            current, target
        ))),
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

        // Normalize key
        let key_col = batch.column(key_idx);
        let normalized_keys = normalize_column(key_col, &DataType::Int64)?;
        let key_array = normalized_keys
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("normalized_keys should be Int64Array");

        // Normalize value
        let val_col = batch.column(val_idx);
        let normalized_vals = normalize_column(val_col, &DataType::Float64)?;
        let val_array = normalized_vals
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("normalized_vals should be Float64Array");

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
