use std::fs::File;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::errors::ParquetError;

use arrow::array::{
    Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};

use chrono::{DateTime, Datelike, NaiveDateTime, Utc};
use fnv::FnvHasher;

use super::types::Record;

// Batch size used for all Parquet readers.
// 65 536 rows/batch reduces per-batch overhead ~64× vs the default (1 024).
const BATCH_SIZE: usize = 65_536;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Normalize an Arrow column to the given target type, if needed.
pub fn normalize_column(
    arr: &Arc<dyn Array>,
    target: &DataType,
) -> Result<Arc<dyn Array>, ParquetError> {
    if arr.data_type() == target {
        return Ok(Arc::clone(arr));
    }
    cast(arr, target).map_err(|e| ParquetError::General(format!("cast error: {}", e)))
}

/// Build a sorted-unique projection list and return the within-list positions
/// for each requested original-schema index.
///
/// Example: orig_indices = [5, 1, 5]  →  proj_cols = [1, 5],  positions = [1, 0, 1]
fn build_projection(orig_indices: &[usize]) -> (Vec<usize>, Vec<usize>) {
    let mut proj: Vec<usize> = orig_indices.to_vec();
    proj.sort();
    proj.dedup();
    let positions = orig_indices
        .iter()
        .map(|&oi| proj.iter().position(|&pi| pi == oi).unwrap())
        .collect();
    (proj, positions)
}

// ── Single-key readers ────────────────────────────────────────────────────────

/// Single-key reader: key column → u64, value column → f64.
/// Only decodes the two requested columns (column projection).
pub fn read_parquet_to_records(
    path: &str,
    key_str: &str,
    val_str: &str,
) -> parquet::errors::Result<Vec<Record<u64>>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    let total_rows = builder.metadata().file_metadata().num_rows() as usize;
    let file_schema = builder.schema().clone();

    let key_orig = file_schema.index_of(key_str).unwrap();
    let val_orig = file_schema.index_of(val_str).unwrap();

    let (proj, positions) = build_projection(&[key_orig, val_orig]);
    let key_proj = positions[0];
    let val_proj = positions[1];

    let mask = ProjectionMask::roots(builder.parquet_schema(), proj);
    let mut reader = builder
        .with_projection(mask)
        .with_batch_size(BATCH_SIZE)
        .build()?;

    let mut records: Vec<Record<u64>> = Vec::with_capacity(total_rows);

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;

        let normalized_keys = normalize_column(batch.column(key_proj), &DataType::Int64)?;
        let key_array = normalized_keys
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let normalized_vals = normalize_column(batch.column(val_proj), &DataType::Float64)?;
        let val_array = normalized_vals
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

/// COUNT(*) reader: only decodes the key column; sets value = 1.0 for every row.
pub fn read_parquet_single_column(
    path: &str,
    key_str: &str,
) -> parquet::errors::Result<Vec<Record<u64>>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    let total_rows = builder.metadata().file_metadata().num_rows() as usize;
    let file_schema = builder.schema().clone();

    let key_orig = file_schema.index_of(key_str).unwrap();
    let mask = ProjectionMask::roots(builder.parquet_schema(), [key_orig]);
    let mut reader = builder
        .with_projection(mask)
        .with_batch_size(BATCH_SIZE)
        .build()?;

    let mut records: Vec<Record<u64>> = Vec::with_capacity(total_rows);

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;
        let normalized_keys = normalize_column(batch.column(0), &DataType::Int64)?;
        let key_array = normalized_keys
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

/// Two-key reader without transforms: convenience wrapper.
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
        &[None, None],
    )
}

// ── KeyColumn: type-dispatch helper for multi-key readers ─────────────────────

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
                let arr = normalize_column(column, &DataType::Int64)?
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone();
                Ok(KeyColumn::Signed(arr))
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                let arr = normalize_column(column, &DataType::UInt64)?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .clone();
                Ok(KeyColumn::Unsigned(arr))
            }
            DataType::Timestamp(_, tz) => {
                let arr = normalize_column(
                    column,
                    &DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                )?
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .clone();
                Ok(KeyColumn::TimestampMicros(arr))
            }
            DataType::Float32 | DataType::Float64 => {
                let arr = normalize_column(column, &DataType::Float64)?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .clone();
                Ok(KeyColumn::Float64(arr))
            }
            other => Err(ParquetError::General(format!(
                "Unsupported key type: {:?}",
                other
            ))),
        }
    }

    #[inline]
    fn is_null(&self, idx: usize) -> bool {
        match self {
            KeyColumn::Signed(a) => a.is_null(idx),
            KeyColumn::Unsigned(a) => a.is_null(idx),
            KeyColumn::TimestampMicros(a) => a.is_null(idx),
            KeyColumn::Float64(a) => a.is_null(idx),
        }
    }

    #[inline]
    fn value_u64(&self, idx: usize) -> u64 {
        match self {
            KeyColumn::Signed(a) => a.value(idx) as u64,
            KeyColumn::Unsigned(a) => a.value(idx),
            KeyColumn::TimestampMicros(a) => a.value(idx) as u64,
            KeyColumn::Float64(a) => a.value(idx).to_bits(),
        }
    }
}

// ── Multi-key readers ─────────────────────────────────────────────────────────

/// Two-key reader with optional per-key transforms.
/// Projects only the three required columns; schema lookups happen once before
/// the batch loop.
pub fn read_parquet_to_records_two_keys_with_transforms(
    path: &str,
    key1_str: &str,
    key2_str: &str,
    val_str: &str,
    key_transforms: &[Option<Box<dyn Fn(u64) -> u64>>],
) -> parquet::errors::Result<Vec<Record<(u64, u64)>>> {
    assert_eq!(key_transforms.len(), 2, "expected 2 key transforms");

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    let total_rows = builder.metadata().file_metadata().num_rows() as usize;
    let file_schema = builder.schema().clone();

    let key1_orig = file_schema.index_of(key1_str).unwrap();
    let key2_orig = file_schema.index_of(key2_str).unwrap();
    let val_orig = file_schema.index_of(val_str).unwrap();

    // Cache data types from the file schema (projection doesn't change types)
    let key1_dtype = file_schema.field(key1_orig).data_type().clone();
    let key2_dtype = file_schema.field(key2_orig).data_type().clone();

    let (proj, positions) = build_projection(&[key1_orig, key2_orig, val_orig]);
    let (key1_proj, key2_proj, val_proj) = (positions[0], positions[1], positions[2]);

    let mask = ProjectionMask::roots(builder.parquet_schema(), proj);
    let mut reader = builder
        .with_projection(mask)
        .with_batch_size(BATCH_SIZE)
        .build()?;

    let mut records: Vec<Record<(u64, u64)>> = Vec::with_capacity(total_rows);

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;

        let key1_data = KeyColumn::from_field(batch.column(key1_proj), &key1_dtype)?;
        let key2_data = KeyColumn::from_field(batch.column(key2_proj), &key2_dtype)?;

        let val_norm = normalize_column(batch.column(val_proj), &DataType::Float64)?;
        let val_array = val_norm.as_any().downcast_ref::<Float64Array>().unwrap();

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

/// Three-key reader with optional per-key transforms.
/// Projects only the four required columns; schema lookups happen once before
/// the batch loop.
pub fn read_parquet_to_records_three_keys_with_transforms(
    path: &str,
    key1_str: &str,
    key2_str: &str,
    key3_str: &str,
    val_str: &str,
    key_transforms: &[Option<Box<dyn Fn(u64) -> u64>>],
) -> parquet::errors::Result<Vec<Record<(u64, u64, u64)>>> {
    assert_eq!(key_transforms.len(), 3, "expected 3 key transforms");

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    let total_rows = builder.metadata().file_metadata().num_rows() as usize;
    let file_schema = builder.schema().clone();

    let key1_orig = file_schema.index_of(key1_str).unwrap();
    let key2_orig = file_schema.index_of(key2_str).unwrap();
    let key3_orig = file_schema.index_of(key3_str).unwrap();
    let val_orig = file_schema.index_of(val_str).unwrap();

    let key1_dtype = file_schema.field(key1_orig).data_type().clone();
    let key2_dtype = file_schema.field(key2_orig).data_type().clone();
    let key3_dtype = file_schema.field(key3_orig).data_type().clone();

    let (proj, positions) = build_projection(&[key1_orig, key2_orig, key3_orig, val_orig]);
    let (key1_proj, key2_proj, key3_proj, val_proj) =
        (positions[0], positions[1], positions[2], positions[3]);

    let mask = ProjectionMask::roots(builder.parquet_schema(), proj);
    let mut reader = builder
        .with_projection(mask)
        .with_batch_size(BATCH_SIZE)
        .build()?;

    let mut records: Vec<Record<(u64, u64, u64)>> = Vec::with_capacity(total_rows);

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;

        let key1_data = KeyColumn::from_field(batch.column(key1_proj), &key1_dtype)?;
        let key2_data = KeyColumn::from_field(batch.column(key2_proj), &key2_dtype)?;
        let key3_data = KeyColumn::from_field(batch.column(key3_proj), &key3_dtype)?;

        let val_norm = normalize_column(batch.column(val_proj), &DataType::Float64)?;
        let val_array = val_norm.as_any().downcast_ref::<Float64Array>().unwrap();

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

// ── Transform functions ───────────────────────────────────────────────────────

/// Microsecond epoch timestamp → year (UTC).
pub fn to_year_from_epoch_micros(ts_micros: u64) -> u64 {
    let ts_i64 = ts_micros as i64;
    let secs = ts_i64 / 1_000_000;
    let nanos = ((ts_i64 % 1_000_000) * 1_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos)
        .expect("invalid timestamp")
        .year() as u64
}

/// Float key encoded as `f64::to_bits` → nearest integer bucket.
pub fn round_from_f64_bits(bits: u64) -> u64 {
    f64::from_bits(bits).round() as u64
}

/// Microsecond epoch timestamp → YYYYMMDD integer (e.g. 20190601).
pub fn to_yyyymmdd_from_epoch_micros(ts_micros: u64) -> u64 {
    let ts_i64 = ts_micros as i64;
    let secs = ts_i64 / 1_000_000;
    let nanos = ((ts_i64 % 1_000_000) * 1_000) as u32;
    let dt = DateTime::<Utc>::from_timestamp(secs, nanos).expect("invalid timestamp");
    (dt.year() as u64) * 10_000 + (dt.month() as u64) * 100 + (dt.day() as u64)
}

// ── Specialised readers ───────────────────────────────────────────────────────

/// Sensors: reads a timestamp column (Timestamp or Utf8 "2019-06-01T00:00:00"),
/// converts to YYYYMMDD key, returns COUNT records (value = 1.0).
pub fn read_sensors_yyyymmdd_count(
    path: &str,
    timestamp_col: &str,
) -> parquet::errors::Result<Vec<Record<u64>>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    let total_rows = builder.metadata().file_metadata().num_rows() as usize;
    let file_schema = builder.schema().clone();

    let ts_orig = file_schema.index_of(timestamp_col).unwrap();
    let ts_dtype = file_schema.field(ts_orig).data_type().clone();

    let mask = ProjectionMask::roots(builder.parquet_schema(), [ts_orig]);
    let mut reader = builder
        .with_projection(mask)
        .with_batch_size(BATCH_SIZE)
        .build()?;

    let mut records: Vec<Record<u64>> = Vec::with_capacity(total_rows);

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;
        let ts_col = batch.column(0); // only column after projection

        if matches!(ts_dtype, DataType::Utf8 | DataType::LargeUtf8) {
            let norm = normalize_column(ts_col, &DataType::Utf8)?;
            let arr = norm.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..batch.num_rows() {
                if arr.is_null(i) {
                    continue;
                }
                let ndt = NaiveDateTime::parse_from_str(arr.value(i), "%Y-%m-%dT%H:%M:%S")
                    .expect("invalid datetime string");
                let day = to_yyyymmdd_from_epoch_micros(ndt.and_utc().timestamp_micros() as u64);
                records.push(Record {
                    key: day,
                    value: 1.0,
                });
            }
        } else {
            let key_data = KeyColumn::from_field(ts_col, &ts_dtype)?;
            for i in 0..batch.num_rows() {
                if key_data.is_null(i) {
                    continue;
                }
                let day = to_yyyymmdd_from_epoch_micros(key_data.value_u64(i));
                records.push(Record {
                    key: day,
                    value: 1.0,
                });
            }
        }
    }

    Ok(records)
}

/// Brown logs: reads a string key column (hashed to u64 via FNV) and a float
/// value column. Null values in the value column are coerced to 0.0 (COALESCE).
pub fn read_parquet_string_key_to_records(
    path: &str,
    key_str: &str,
    val_str: &str,
) -> parquet::errors::Result<Vec<Record<u64>>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    let total_rows = builder.metadata().file_metadata().num_rows() as usize;
    let file_schema = builder.schema().clone();

    let key_orig = file_schema.index_of(key_str).unwrap();
    let val_orig = file_schema.index_of(val_str).unwrap();

    let (proj, positions) = build_projection(&[key_orig, val_orig]);
    let (key_proj, val_proj) = (positions[0], positions[1]);

    let mask = ProjectionMask::roots(builder.parquet_schema(), proj);
    let mut reader = builder
        .with_projection(mask)
        .with_batch_size(BATCH_SIZE)
        .build()?;

    let mut records: Vec<Record<u64>> = Vec::with_capacity(total_rows);

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;

        let key_norm = normalize_column(batch.column(key_proj), &DataType::Utf8)?;
        let key_arr = key_norm.as_any().downcast_ref::<StringArray>().unwrap();

        let val_norm = normalize_column(batch.column(val_proj), &DataType::Float64)?;
        let val_arr = val_norm.as_any().downcast_ref::<Float64Array>().unwrap();

        for i in 0..batch.num_rows() {
            if key_arr.is_null(i) {
                continue;
            }
            let mut h = FnvHasher::default();
            key_arr.value(i).hash(&mut h);
            let val = if val_arr.is_null(i) {
                0.0
            } else {
                val_arr.value(i)
            };
            records.push(Record {
                key: h.finish(),
                value: val,
            });
        }
    }

    Ok(records)
}
