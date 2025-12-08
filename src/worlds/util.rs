use std::fs::File;
use std::sync::Arc;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::ParquetError;

use arrow::array::{Array, Int64Array, Float64Array, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::compute::cast;

use chrono::{DateTime, Utc, Datelike};

use super::types::Record;

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

    cast(arr, target).map_err(|e| ParquetError::General(format!("cast error: {}", e)))
}

/// Single-key reader: key column -> u64, value column -> f64
pub fn read_parquet_to_records(
    path: &str,
    key_str: &str,
    val_str: &str,
) -> parquet::errors::Result<Vec<Record<u64>>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut record_batch_reader = builder.build()?;

    let mut records: Vec<Record<u64>> = Vec::new();

    while let Some(batch_result) = record_batch_reader.next() {
        let batch = batch_result?;
        let schema = batch.schema();

        let key_idx = schema.index_of(key_str).unwrap();
        let val_idx = schema.index_of(val_str).unwrap();

        // key: normalize to Int64
        let key_col = batch.column(key_idx);
        let normalized_keys = normalize_column(key_col, &DataType::Int64)?;
        let key_array = normalized_keys
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("normalized_keys should be Int64Array");

        // value: normalize to Float64
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

/// Single-key reader for COUNT(*): key column -> u64, value always 1.0
pub fn read_parquet_single_column(
    path: &str,
    key_str: &str,
) -> parquet::errors::Result<Vec<Record<u64>>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut record_batch_reader = builder.build()?;

    let mut records: Vec<Record<u64>> = Vec::new();

    while let Some(batch_result) = record_batch_reader.next() {
        let batch = batch_result?;
        let schema = batch.schema();

        let key_idx = schema.index_of(key_str).unwrap();

        let key_col = batch.column(key_idx);
        let normalized_keys = normalize_column(key_col, &DataType::Int64)?;
        let key_array = normalized_keys
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("normalized_keys should be Int64Array");

        for i in 0..batch.num_rows() {
            if key_array.is_null(i) {
                continue;
            }
            records.push(Record {
                key: key_array.value(i) as u64,
                value: 1.0, // COUNT(*)
            });
        }
    }

    Ok(records)
}

/// Two-key reader without transforms: convenience wrapper.
/// key = (key1, key2), value = f64
pub fn read_parquet_to_records_two_keys(
    path: &str,
    key1_str: &str,
    key2_str: &str,
    val_str: &str,
) -> parquet::errors::Result<Vec<Record<(u64, u64)>>> {
    read_parquet_to_records_two_keys_with_transforms(
        path,
        key1_str,
        key2_str,
        val_str,
        &[
            None, // key1 unchanged
            None, // key2 unchanged
        ],
    )
}

enum KeyColumn {
    Signed(Int64Array),
    Unsigned(UInt64Array),
    TimestampMicros(TimestampMicrosecondArray),
}

impl KeyColumn {
    fn from_field(
        column: &Arc<dyn Array>,
        dtype: &DataType,
    ) -> Result<Self, ParquetError> {
        match dtype {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64 => {
                let normalized = normalize_column(column, &DataType::Int64)?;
                let arr = normalized
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("normalized Int64 array")
                    .clone();
                Ok(KeyColumn::Signed(arr))
            }
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => {
                let normalized = normalize_column(column, &DataType::UInt64)?;
                let arr = normalized
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("normalized UInt64 array")
                    .clone();
                Ok(KeyColumn::Unsigned(arr))
            }
            DataType::Timestamp(_, tz) => {
                let normalized = normalize_column(
                    column,
                    &DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                )?;
                let arr = normalized
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("normalized timestamp micros array")
                    .clone();
                Ok(KeyColumn::TimestampMicros(arr))
            }
            other => Err(ParquetError::General(format!(
                "Unsupported key type for two-key reader: {:?}",
                other
            ))),
        }
    }

    #[inline]
    fn is_null(&self, idx: usize) -> bool {
        match self {
            KeyColumn::Signed(arr) => arr.is_null(idx),
            KeyColumn::Unsigned(arr) => arr.is_null(idx),
            KeyColumn::TimestampMicros(arr) => arr.is_null(idx),
        }
    }

    #[inline]
    fn value_u64(&self, idx: usize) -> u64 {
        match self {
            KeyColumn::Signed(arr) => arr.value(idx) as u64,
            KeyColumn::Unsigned(arr) => arr.value(idx),
            KeyColumn::TimestampMicros(arr) => arr.value(idx) as u64,
        }
    }
}

/// Two-key reader with optional per-key transforms over integer or timestamp columns.
pub fn read_parquet_to_records_two_keys_with_transforms(
    path: &str,
    key1_str: &str,
    key2_str: &str,
    val_str: &str,
    key_transforms: &[Option<Box<dyn Fn(u64) -> u64>>],
) -> parquet::errors::Result<Vec<Record<(u64, u64)>>> {
    assert!(
        key_transforms.len() == 2,
        "expected 2 key transforms (one per key)"
    );

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut record_batch_reader = builder.build()?;

    let mut records: Vec<Record<(u64, u64)>> = Vec::new();

    while let Some(batch_result) = record_batch_reader.next() {
        let batch = batch_result?;
        let schema = batch.schema();

        let key1_idx = schema.index_of(key1_str).unwrap();
        let key2_idx = schema.index_of(key2_str).unwrap();
        let val_idx = schema.index_of(val_str).unwrap();

        let key1_col = batch.column(key1_idx);
        let key1_data = KeyColumn::from_field(key1_col, schema.field(key1_idx).data_type())?;

        let key2_col = batch.column(key2_idx);
        let key2_data = KeyColumn::from_field(key2_col, schema.field(key2_idx).data_type())?;

        let val_col = batch.column(val_idx);
        let val_norm = normalize_column(val_col, &DataType::Float64)?;
        let val_array = val_norm
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("val_norm should be Float64Array");

        for i in 0..batch.num_rows() {
            if key1_data.is_null(i) || key2_data.is_null(i) || val_array.is_null(i) {
                continue;
            }

            let mut k1 = key1_data.value_u64(i);
            let mut k2 = key2_data.value_u64(i);
            let v = val_array.value(i);

            if let Some(f) = &key_transforms[0] {
                k1 = f(k1);
            }
            if let Some(f) = &key_transforms[1] {
                k2 = f(k2);
            }

            records.push(Record { key: (k1, k2), value: v });
        }
    }

    Ok(records)
}

/// Convert microsecond timestamps since epoch -> year (UTC).
pub fn to_year_from_epoch_micros(ts_micros: u64) -> u64 {
    let ts_i64 = ts_micros as i64;
    let secs = ts_i64 / 1_000_000;
    let micros_rem = ts_i64 % 1_000_000;
    let nanos = (micros_rem as i64 * 1000) as u32;

    let dt = DateTime::<Utc>::from_timestamp(secs, nanos)
        .expect("invalid timestamp");

    dt.year() as u64
}
