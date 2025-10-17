use arrow::array::Array;
use std::fs::File;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::array::{Int64Array, Float64Array};
use super::types::{Record, MultiRecord};

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

// TODO: check count star
pub fn read_parquet_to_multirecords(
    path: &str,
    keys: &[&str],
    vals: &[&str],
) -> parquet::errors::Result<Vec<MultiRecord>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.with_batch_size(1024).build()?;
    let mut records = Vec::new();

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;
        let schema = batch.schema();

        // column indices for keys and vals
        let key_indices: Vec<_> = keys.iter().map(|c| schema.index_of(c).unwrap()).collect();
        let val_indices: Vec<_> = vals.iter().map(|c| schema.index_of(c).unwrap()).collect();

        for i in 0..batch.num_rows() {
            let mut record = MultiRecord {
                keys: Vec::new(),
                values: Vec::new(),
            };

            // extract key columns
            // TODO: add null check / failure check for downcast / add a is_valid bitmap to records
            for &idx in &key_indices {
                let col = batch.column(idx);
                // let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                // record.keys.push(arr.value(i) as u64);

                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    if arr.is_valid(i) {
                        record.keys.push(arr.value(i) as u64);
                        continue;
                    } 
                }
                record.keys.push(0);
            }

            // extract value columns
            // TODO: add null check / failure check for downcast
            for &idx in &val_indices {
                let col = batch.column(idx);
                let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                record.values.push(arr.value(i) as f64);
            }

            records.push(record);
        }
    }

    Ok(records)
}
