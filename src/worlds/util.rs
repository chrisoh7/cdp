use std::fs::File;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::ParquetError;

use arrow::array::{Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray, UInt64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};

use chrono::{DateTime, Datelike, NaiveDateTime, Utc};
use fnv::FnvHasher;

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
    Float64(Float64Array),
}

impl KeyColumn {
    fn from_field(column: &Arc<dyn Array>, dtype: &DataType) -> Result<Self, ParquetError> {
        match dtype {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let normalized = normalize_column(column, &DataType::Int64)?;
                let arr = normalized
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("normalized Int64 array")
                    .clone();
                Ok(KeyColumn::Signed(arr))
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
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
            DataType::Float32 | DataType::Float64 => {
                let normalized = normalize_column(column, &DataType::Float64)?;
                let arr = normalized
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("normalized Float64 array")
                    .clone();
                Ok(KeyColumn::Float64(arr))
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
            KeyColumn::Float64(arr) => arr.is_null(idx),
        }
    }

    #[inline]
    fn value_u64(&self, idx: usize) -> u64 {
        match self {
            KeyColumn::Signed(arr) => arr.value(idx) as u64,
            KeyColumn::Unsigned(arr) => arr.value(idx),
            KeyColumn::TimestampMicros(arr) => arr.value(idx) as u64,
            KeyColumn::Float64(arr) => arr.value(idx).to_bits(),
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

            records.push(Record {
                key: (k1, k2),
                value: v,
            });
        }
    }

    Ok(records)
}

/// Three-key reader with optional per-key transforms over integer/timestamp/float key columns.
///
/// Float keys are passed to transforms as `f64::to_bits`; use `round_from_f64_bits` for
/// SQL-like `round(...)` style grouping.
pub fn read_parquet_to_records_three_keys_with_transforms(
    path: &str,
    key1_str: &str,
    key2_str: &str,
    key3_str: &str,
    val_str: &str,
    key_transforms: &[Option<Box<dyn Fn(u64) -> u64>>],
) -> parquet::errors::Result<Vec<Record<(u64, u64, u64)>>> {
    assert!(
        key_transforms.len() == 3,
        "expected 3 key transforms (one per key)"
    );

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut record_batch_reader = builder.build()?;

    let mut records: Vec<Record<(u64, u64, u64)>> = Vec::new();

    while let Some(batch_result) = record_batch_reader.next() {
        let batch = batch_result?;
        let schema = batch.schema();

        let key1_idx = schema.index_of(key1_str).unwrap();
        let key2_idx = schema.index_of(key2_str).unwrap();
        let key3_idx = schema.index_of(key3_str).unwrap();
        let val_idx = schema.index_of(val_str).unwrap();

        let key1_col = batch.column(key1_idx);
        let key1_data = KeyColumn::from_field(key1_col, schema.field(key1_idx).data_type())?;

        let key2_col = batch.column(key2_idx);
        let key2_data = KeyColumn::from_field(key2_col, schema.field(key2_idx).data_type())?;

        let key3_col = batch.column(key3_idx);
        let key3_data = KeyColumn::from_field(key3_col, schema.field(key3_idx).data_type())?;

        let val_col = batch.column(val_idx);
        let val_norm = normalize_column(val_col, &DataType::Float64)?;
        let val_array = val_norm
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("val_norm should be Float64Array");

        for i in 0..batch.num_rows() {
            if key1_data.is_null(i)
                || key2_data.is_null(i)
                || key3_data.is_null(i)
                || val_array.is_null(i)
            {
                continue;
            }

            let mut k1 = key1_data.value_u64(i);
            let mut k2 = key2_data.value_u64(i);
            let mut k3 = key3_data.value_u64(i);
            let v = val_array.value(i);

            if let Some(f) = &key_transforms[0] {
                k1 = f(k1);
            }
            if let Some(f) = &key_transforms[1] {
                k2 = f(k2);
            }
            if let Some(f) = &key_transforms[2] {
                k3 = f(k3);
            }

            records.push(Record {
                key: (k1, k2, k3),
                value: v,
            });
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

    let dt = DateTime::<Utc>::from_timestamp(secs, nanos).expect("invalid timestamp");

    dt.year() as u64
}

/// Round a float key encoded as `f64::to_bits` to the nearest integer bucket.
pub fn round_from_f64_bits(bits: u64) -> u64 {
    f64::from_bits(bits).round() as u64
}

/// Convert microsecond timestamps since epoch -> YYYYMMDD integer (e.g. 20190601).
pub fn to_yyyymmdd_from_epoch_micros(ts_micros: u64) -> u64 {
    let ts_i64 = ts_micros as i64;
    let secs = ts_i64 / 1_000_000;
    let micros_rem = ts_i64 % 1_000_000;
    let nanos = (micros_rem * 1000) as u32;
    let dt = DateTime::<Utc>::from_timestamp(secs, nanos).expect("invalid timestamp");
    (dt.year() as u64) * 10_000 + (dt.month() as u64) * 100 + (dt.day() as u64)
}

/// Reads a timestamp column from parquet, converts each value to a YYYYMMDD integer key,
/// and returns count records (value = 1.0) for COUNT(*) GROUP BY day queries.
///
/// Handles both native Timestamp columns and string columns ("2019-06-01T00:00:00").
pub fn read_sensors_yyyymmdd_count(
    path: &str,
    timestamp_col: &str,
) -> parquet::errors::Result<Vec<Record<u64>>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut record_batch_reader = builder.build()?;
    let mut records = Vec::new();

    while let Some(batch_result) = record_batch_reader.next() {
        let batch = batch_result?;
        let schema = batch.schema();
        let ts_idx = schema.index_of(timestamp_col).unwrap();
        let ts_col = batch.column(ts_idx);
        let dtype = schema.field(ts_idx).data_type();

        if matches!(dtype, DataType::Utf8 | DataType::LargeUtf8) {
            // String timestamp: normalize to Utf8 then parse "2019-06-01T00:00:00"
            let norm = normalize_column(ts_col, &DataType::Utf8)?;
            let arr = norm
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("expected StringArray after Utf8 normalization");
            for i in 0..batch.num_rows() {
                if arr.is_null(i) {
                    continue;
                }
                let s = arr.value(i);
                let ndt = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                    .expect("invalid datetime string in sensors");
                let epoch_micros = ndt.and_utc().timestamp_micros() as u64;
                let day = to_yyyymmdd_from_epoch_micros(epoch_micros);
                records.push(Record { key: day, value: 1.0 });
            }
        } else {
            // Numeric / native Timestamp: use KeyColumn for type dispatch
            let key_data = KeyColumn::from_field(ts_col, dtype)?;
            for i in 0..batch.num_rows() {
                if key_data.is_null(i) {
                    continue;
                }
                let ts_micros = key_data.value_u64(i);
                let day = to_yyyymmdd_from_epoch_micros(ts_micros);
                records.push(Record { key: day, value: 1.0 });
            }
        }
    }

    Ok(records)
}

/// Reads a string key column + float value column from parquet.
/// The string key is hashed to u64 via FNV for use as a groupby key.
/// Null values in the value column are treated as 0.0 (COALESCE semantics).
pub fn read_parquet_string_key_to_records(
    path: &str,
    key_str: &str,
    val_str: &str,
) -> parquet::errors::Result<Vec<Record<u64>>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut record_batch_reader = builder.build()?;
    let mut records = Vec::new();

    while let Some(batch_result) = record_batch_reader.next() {
        let batch = batch_result?;
        let schema = batch.schema();
        let key_idx = schema.index_of(key_str).unwrap();
        let val_idx = schema.index_of(val_str).unwrap();

        let key_col = batch.column(key_idx);
        let key_norm = normalize_column(key_col, &DataType::Utf8)?;
        let key_arr = key_norm
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("key column must be Utf8/StringArray");

        let val_col = batch.column(val_idx);
        let val_norm = normalize_column(val_col, &DataType::Float64)?;
        let val_arr = val_norm
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("val_norm should be Float64Array");

        for i in 0..batch.num_rows() {
            if key_arr.is_null(i) {
                continue;
            }
            let s = key_arr.value(i);
            let mut h = FnvHasher::default();
            s.hash(&mut h);
            // COALESCE(cpu_user, 0.0): treat null values as 0.0
            let val = if val_arr.is_null(i) { 0.0 } else { val_arr.value(i) };
            records.push(Record {
                key: h.finish(),
                value: val,
            });
        }
    }

    Ok(records)
}
