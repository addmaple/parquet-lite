use parquet2::{
  compression::CompressionOptions,
  encoding::{hybrid_rle, Encoding},
  metadata::{Descriptor, SchemaDescriptor},
  page::{CompressedPage, DataPage, DataPageHeader, DataPageHeaderV1, Page},
  read::levels::get_bit_width,
  schema::types::{
    FieldInfo, IntegerType, ParquetType, PhysicalType, PrimitiveLogicalType, PrimitiveType,
    TimeUnit,
  },
  schema::Repetition,
  write::{Compressor, DynIter, DynStreamingIterator, FileWriter, Version, WriteOptions},
};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::io::Cursor;
use wasm_bindgen::prelude::*;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ColumnSchema {
  pub name: String,
  #[serde(rename = "type")]
  pub col_type: String,
  #[serde(default)]
  pub nullable: bool,
  #[serde(default, rename = "logicalType", alias = "logical_type")]
  pub logical_type: Option<String>,
  // Decimal-specific fields
  #[serde(default)]
  pub precision: Option<u8>,
  #[serde(default)]
  pub scale: Option<u8>,
  // Integer-specific fields
  #[serde(default, rename = "bitWidth", alias = "bit_width")]
  pub bit_width: Option<u8>,
  #[serde(default, rename = "isSigned", alias = "is_signed")]
  pub is_signed: Option<bool>,
  // Enum-specific fields
  #[serde(default, rename = "enumValues", alias = "enum_values")]
  pub enum_values: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WriteConfig {
  #[serde(default = "default_compression")]
  pub compression: String,
  #[serde(default = "default_row_group_size")]
  pub row_group_size: usize,
  #[serde(default = "default_version")]
  pub version: String,
}

fn default_compression() -> String {
  "snappy".to_string()
}

fn default_row_group_size() -> usize {
  10000
}

fn default_version() -> String {
  "v1".to_string()
}

impl Default for WriteConfig {
  fn default() -> Self {
    Self {
      compression: default_compression(),
      row_group_size: default_row_group_size(),
      version: default_version(),
    }
  }
}

fn get_compression(name: &str) -> CompressionOptions {
  match name.to_lowercase().as_str() {
    "snappy" => CompressionOptions::Snappy,
    "none" | "uncompressed" => CompressionOptions::Uncompressed,
    _ => CompressionOptions::Snappy,
  }
}

fn get_physical_type(type_name: &str) -> PhysicalType {
  match type_name.to_lowercase().as_str() {
    "int32" | "int" | "integer" => PhysicalType::Int32,
    "int64" | "long" | "bigint" => PhysicalType::Int64,
    "float" | "float32" => PhysicalType::Float,
    "double" | "float64" => PhysicalType::Double,
    "boolean" | "bool" => PhysicalType::Boolean,
    "string" | "utf8" | "text" => PhysicalType::ByteArray,
    "bytes" | "binary" => PhysicalType::ByteArray,
    _ => PhysicalType::ByteArray,
  }
}

fn get_logical_type(
  logical_type_str: &str,
  physical_type: PhysicalType,
  precision: Option<u8>,
  scale: Option<u8>,
  bit_width: Option<u8>,
  is_signed: Option<bool>,
) -> Option<PrimitiveLogicalType> {
  match logical_type_str.to_lowercase().as_str() {
    "date" => {
      if matches!(physical_type, PhysicalType::Int32) {
        Some(PrimitiveLogicalType::Date)
      } else {
        None
      }
    }
    "time_millis" | "time_milliseconds" => {
      if matches!(physical_type, PhysicalType::Int32) {
        Some(PrimitiveLogicalType::Time {
          unit: TimeUnit::Milliseconds,
          is_adjusted_to_utc: false,
        })
      } else {
        None
      }
    }
    "time_micros" | "time_microseconds" => {
      if matches!(physical_type, PhysicalType::Int64) {
        Some(PrimitiveLogicalType::Time {
          unit: TimeUnit::Microseconds,
          is_adjusted_to_utc: false,
        })
      } else {
        None
      }
    }
    "timestamp_millis" | "timestamp_milliseconds" => {
      if matches!(physical_type, PhysicalType::Int64) {
        Some(PrimitiveLogicalType::Timestamp {
          unit: TimeUnit::Milliseconds,
          is_adjusted_to_utc: false,
        })
      } else {
        None
      }
    }
    "timestamp_micros" | "timestamp_microseconds" => {
      if matches!(physical_type, PhysicalType::Int64) {
        Some(PrimitiveLogicalType::Timestamp {
          unit: TimeUnit::Microseconds,
          is_adjusted_to_utc: false,
        })
      } else {
        None
      }
    }
    "utf8" | "string" => {
      if matches!(physical_type, PhysicalType::ByteArray) {
        Some(PrimitiveLogicalType::String)
      } else {
        None
      }
    }
    "json" => {
      if matches!(physical_type, PhysicalType::ByteArray) {
        Some(PrimitiveLogicalType::Json)
      } else {
        None
      }
    }
    "bson" => {
      if matches!(physical_type, PhysicalType::ByteArray) {
        Some(PrimitiveLogicalType::Bson)
      } else {
        None
      }
    }
    "decimal" => {
      // Decimal requires precision and scale
      if let (Some(prec), Some(sc)) = (precision, scale) {
        match physical_type {
          PhysicalType::Int32
          | PhysicalType::Int64
          | PhysicalType::ByteArray
          | PhysicalType::FixedLenByteArray(_) => {
            Some(PrimitiveLogicalType::Decimal(prec as usize, sc as usize))
          }
          _ => None,
        }
      } else {
        None
      }
    }
    "enum" => {
      if matches!(physical_type, PhysicalType::ByteArray) {
        Some(PrimitiveLogicalType::Enum)
      } else {
        None
      }
    }
    "integer" | "int" => {
      // Integer requires bit_width and is_signed
      if let (Some(bw), Some(signed)) = (bit_width, is_signed) {
        if matches!(physical_type, PhysicalType::Int32 | PhysicalType::Int64) {
          let int_type = match (bw, signed) {
            (8, true) => IntegerType::Int8,
            (8, false) => IntegerType::UInt8,
            (16, true) => IntegerType::Int16,
            (16, false) => IntegerType::UInt16,
            (32, true) => IntegerType::Int32,
            (32, false) => IntegerType::UInt32,
            (64, true) => IntegerType::Int64,
            (64, false) => IntegerType::UInt64,
            _ => return None,
          };
          Some(PrimitiveLogicalType::Integer(int_type))
        } else {
          None
        }
      } else {
        None
      }
    }
    "uuid" => {
      // UUID requires FixedLenByteArray(16) per Parquet spec
      // We'll accept ByteArray and convert it, but warn that FixedLenByteArray is preferred
      if matches!(physical_type, PhysicalType::FixedLenByteArray(16)) {
        Some(PrimitiveLogicalType::Uuid)
      } else if matches!(physical_type, PhysicalType::ByteArray) {
        // Note: Parquet spec prefers FixedLenByteArray(16) for UUID
        // But we'll allow ByteArray for convenience - users should use FixedLenByteArray for best compatibility
        None // Don't allow UUID on ByteArray - it causes errors in parquet2
      } else {
        None
      }
    }
    _ => None,
  }
}

fn build_schema_fields(columns: &[ColumnSchema]) -> Result<Vec<ParquetType>, JsError> {
  columns
        .iter()
        .map(|col| {
            let physical_type = get_physical_type(&col.col_type);
            let logical_type = col.logical_type.as_ref()
                .and_then(|lt| get_logical_type(
                    lt,
                    physical_type,
                    col.precision,
                    col.scale,
                    col.bit_width,
                    col.is_signed,
                ));

            // Validate UUID: must use FixedLenByteArray(16), not ByteArray
            if let Some(lt_str) = col.logical_type.as_ref() {
                if lt_str.to_lowercase() == "uuid" && matches!(physical_type, PhysicalType::ByteArray) {
                    return Err(JsError::new("Cannot annotate Uuid from ByteArray. UUID requires FixedLenByteArray(16) per Parquet spec."));
                }
            }

            Ok(ParquetType::PrimitiveType(PrimitiveType {
                field_info: FieldInfo {
                    name: col.name.clone(),
                    repetition: if col.nullable {
                        Repetition::Optional
                    } else {
                        Repetition::Required
                    },
                    id: None,
                },
                logical_type,
                converted_type: None,
                physical_type,
            }))
        })
        .collect()
}

fn create_descriptor(col: &ColumnSchema) -> Descriptor {
  let physical_type = get_physical_type(&col.col_type);
  let logical_type = col.logical_type.as_ref().and_then(|lt| {
    get_logical_type(
      lt,
      physical_type,
      col.precision,
      col.scale,
      col.bit_width,
      col.is_signed,
    )
  });
  let primitive_type = PrimitiveType {
    field_info: FieldInfo {
      name: col.name.clone(),
      repetition: if col.nullable {
        Repetition::Optional
      } else {
        Repetition::Required
      },
      id: None,
    },
    logical_type,
    converted_type: None,
    physical_type,
  };

  Descriptor {
    primitive_type,
    max_def_level: if col.nullable { 1 } else { 0 },
    max_rep_level: 0,
  }
}

fn encode_int32_column(values: &[i32]) -> Vec<u8> {
  // Pre-allocate exact size for better performance
  let mut bytes = Vec::with_capacity(values.len() * 4);
  bytes.extend(values.iter().flat_map(|v| v.to_le_bytes()));
  bytes
}

fn encode_int64_column(values: &[i64]) -> Vec<u8> {
  let mut bytes = Vec::with_capacity(values.len() * 8);
  bytes.extend(values.iter().flat_map(|v| v.to_le_bytes()));
  bytes
}

fn encode_float_column(values: &[f32]) -> Vec<u8> {
  let mut bytes = Vec::with_capacity(values.len() * 4);
  bytes.extend(values.iter().flat_map(|v| v.to_le_bytes()));
  bytes
}

fn encode_double_column(values: &[f64]) -> Vec<u8> {
  let mut bytes = Vec::with_capacity(values.len() * 8);
  bytes.extend(values.iter().flat_map(|v| v.to_le_bytes()));
  bytes
}

fn encode_boolean_column(values: &[bool]) -> Vec<u8> {
  // Parquet uses bit-packed booleans
  let mut bytes = vec![0u8; values.len().div_ceil(8)];
  for (i, &v) in values.iter().enumerate() {
    if v {
      bytes[i / 8] |= 1 << (i % 8);
    }
  }
  bytes
}

fn encode_string_column(values: &[String]) -> Vec<u8> {
  // Pre-allocate capacity: 4 bytes per length + average string size
  let estimated_size = values.len() * 4 + values.iter().map(|s| s.len()).sum::<usize>();
  let mut bytes = Vec::with_capacity(estimated_size);
  for s in values {
    let s_bytes = s.as_bytes();
    bytes.extend_from_slice(&(s_bytes.len() as u32).to_le_bytes());
    bytes.extend_from_slice(s_bytes);
  }
  bytes
}

fn convert_date_to_value(
  date: &js_sys::Date,
  logical_type: Option<&PrimitiveLogicalType>,
) -> Result<i64, JsError> {
  const MILLIS_PER_DAY: i64 = 86_400_000;
  const MICROS_PER_MILLI: i64 = 1_000;
  const NANOS_PER_MILLI: i64 = 1_000_000;

  let timestamp_ms = date.get_time() as i64;
  match logical_type {
    Some(PrimitiveLogicalType::Date) => {
      // Days since Unix epoch (UTC)
      Ok(timestamp_ms.div_euclid(MILLIS_PER_DAY))
    }
    Some(PrimitiveLogicalType::Timestamp { unit, .. }) => match unit {
      TimeUnit::Milliseconds => Ok(timestamp_ms),
      TimeUnit::Microseconds => Ok(timestamp_ms * MICROS_PER_MILLI),
      TimeUnit::Nanoseconds => Ok(timestamp_ms * NANOS_PER_MILLI),
    },
    Some(PrimitiveLogicalType::Time { unit, .. }) => {
      // Time of day: milliseconds since UTC midnight
      let total_ms = timestamp_ms.rem_euclid(MILLIS_PER_DAY);
      match unit {
        TimeUnit::Milliseconds => Ok(total_ms),
        TimeUnit::Microseconds => Ok(total_ms * MICROS_PER_MILLI),
        TimeUnit::Nanoseconds => Ok(total_ms * NANOS_PER_MILLI),
      }
    }
    _ => Err(JsError::new(
      "Date conversion not supported for this logical type",
    )),
  }
}

fn convert_js_value_to_target_type(
  value: &JsValue,
  physical_type: PhysicalType,
  logical_type: Option<&PrimitiveLogicalType>,
) -> Result<JsValue, JsError> {
  // Check if it's a Date-like object by looking for getTime()
  if let Some(lt) = logical_type {
    if let Ok(has_get_time) = js_sys::Reflect::has(value, &JsValue::from_str("getTime")) {
      if has_get_time {
        let date_obj = js_sys::Date::from(value.clone());
        if date_obj.get_time().is_finite() {
          match lt {
            PrimitiveLogicalType::Date => {
              let days = convert_date_to_value(&date_obj, Some(lt))?;
              let as_i32 = i32::try_from(days)
                .map_err(|_| JsError::new("Date value out of range for INT32"))?;
              return Ok(JsValue::from(as_i32));
            }
            PrimitiveLogicalType::Timestamp { .. } => {
              let timestamp = convert_date_to_value(&date_obj, Some(lt))?;
              return Ok(JsValue::from_f64(timestamp as f64));
            }
            PrimitiveLogicalType::Time { .. } => {
              let time = convert_date_to_value(&date_obj, Some(lt))?;
              return match physical_type {
                PhysicalType::Int32 => {
                  let as_i32 = i32::try_from(time)
                    .map_err(|_| JsError::new("Time value out of range for INT32"))?;
                  Ok(JsValue::from(as_i32))
                }
                PhysicalType::Int64 => Ok(JsValue::from_f64(time as f64)),
                _ => Err(JsError::new(
                  "Time logical type requires INT32 or INT64 column",
                )),
              };
            }
            _ => {}
          }
        }
      }
    }
  }

  // Check if it's an Object and logical type is JSON
  if let Some(PrimitiveLogicalType::Json) = logical_type {
    if value.is_object() && !value.is_null() && !value.is_undefined() {
      // Try to stringify the object
      let json_str = js_sys::JSON::stringify(value)
        .map_err(|_| JsError::new("Failed to stringify object to JSON"))?;
      return Ok(json_str.into());
    }
  }

  // Return as-is if no conversion needed
  Ok(value.clone())
}

fn convert_enum_indices_to_strings<I>(
  enum_values: &[String],
  indices: I,
  capacity: usize,
) -> Result<Vec<String>, JsError>
where
  I: IntoIterator<Item = usize>,
{
  if enum_values.is_empty() {
    return Err(JsError::new(
      "enumValues must contain at least one entry when using index arrays",
    ));
  }

  let max_index = enum_values.len() - 1;
  let mut string_values = Vec::with_capacity(capacity);
  for idx in indices {
    let value = enum_values.get(idx).ok_or_else(|| {
      JsError::new(&format!(
        "Enum index {} out of range (max: {})",
        idx, max_index
      ))
    })?;
    string_values.push(value.clone());
  }
  Ok(string_values)
}

fn js_value_to_enum_index(value: &JsValue) -> Result<usize, JsError> {
  if value.is_null() || value.is_undefined() {
    return Err(JsError::new(
      "Enum index array cannot contain null or undefined values",
    ));
  }

  let idx_f64 = value
    .as_f64()
    .ok_or_else(|| JsError::new("Enum index array must contain only numbers"))?;

  if !idx_f64.is_finite() {
    return Err(JsError::new("Enum index must be a finite number"));
  }
  if idx_f64 < 0.0 {
    return Err(JsError::new("Enum index must be non-negative"));
  }
  if idx_f64.fract() != 0.0 {
    return Err(JsError::new("Enum index must be an integer value"));
  }

  let idx = idx_f64 as usize;
  if (idx as f64) != idx_f64 {
    return Err(JsError::new(
      "Enum index is too large to fit on this platform",
    ));
  }

  Ok(idx)
}

fn encode_values_from_js(
  physical_type: PhysicalType,
  values_js: JsValue,
  logical_type: Option<&PrimitiveLogicalType>,
  enum_values: Option<&[String]>,
) -> Result<Vec<u8>, JsError> {
  // First, check for TypedArrays based on physical type (works even without logical type)
  // This handles common cases like Int32Array, Float32Array, Float64Array, Uint32Array
  if let Ok(ctor) = js_sys::Reflect::get(&values_js, &JsValue::from_str("constructor")) {
    if let Ok(ctor_name_js) = js_sys::Reflect::get(&ctor, &JsValue::from_str("name")) {
      if let Some(ctor_name) = ctor_name_js.as_string() {
        // Handle TypedArrays based on physical type
        match (ctor_name.as_str(), physical_type) {
          ("Int32Array", PhysicalType::Int32) => {
            let arr = js_sys::Int32Array::from(values_js.clone());
            let values: Vec<i32> = arr.to_vec();
            return Ok(encode_int32_column(&values));
          }
          ("Uint32Array", PhysicalType::Int32) => {
            // Reinterpret u32 bits as i32 - handles values > 2^31
            let arr = js_sys::Uint32Array::from(values_js.clone());
            let values_u32: Vec<u32> = arr.to_vec();
            let values: Vec<i32> = values_u32.into_iter().map(|v| v as i32).collect();
            return Ok(encode_int32_column(&values));
          }
          ("Float32Array", PhysicalType::Float) => {
            let arr = js_sys::Float32Array::from(values_js.clone());
            let values: Vec<f32> = arr.to_vec();
            return Ok(encode_float_column(&values));
          }
          ("Float64Array", PhysicalType::Double) => {
            let arr = js_sys::Float64Array::from(values_js.clone());
            let values: Vec<f64> = arr.to_vec();
            return Ok(encode_double_column(&values));
          }
          ("BigInt64Array", PhysicalType::Int64) => {
            let arr = js_sys::BigInt64Array::from(values_js.clone());
            let len = arr.length() as usize;
            let mut values = Vec::with_capacity(len);
            for i in 0..len {
              values.push(arr.get_index(i as u32));
            }
            return Ok(encode_int64_column(&values));
          }
          ("BigUint64Array", PhysicalType::Int64) => {
            let arr = js_sys::BigUint64Array::from(values_js.clone());
            let len = arr.length() as usize;
            let mut values = Vec::with_capacity(len);
            for i in 0..len {
              values.push(arr.get_index(i as u32) as i64);
            }
            return Ok(encode_int64_column(&values));
          }
          _ => {}
        }
      }
    }
  }

  // Check if it's a TypedArray - optimize for Integer logical types
  // This handles smaller integer types (Int8, Uint8, Int16, Uint16) with proper widths
  if let Some(PrimitiveLogicalType::Integer(int_type)) = logical_type {
    let (bit_width, is_signed): (usize, bool) = (*int_type).into();

    // Check constructor name to detect TypedArray type
    if let Ok(ctor) = js_sys::Reflect::get(&values_js, &JsValue::from_str("constructor")) {
      if let Ok(ctor_name_js) = js_sys::Reflect::get(&ctor, &JsValue::from_str("name")) {
        if let Some(ctor_name) = ctor_name_js.as_string() {
          match (ctor_name.as_str(), bit_width, is_signed) {
            ("Uint8Array", 8, false) => {
              let arr = js_sys::Uint8Array::from(values_js.clone());
              // Use to_vec() for bulk copy - much faster than get_index() in a loop
              let values_u8: Vec<u8> = arr.to_vec();
              let values: Vec<i32> = values_u8.into_iter().map(|v| v as i32).collect();
              return Ok(encode_int32_column(&values));
            }
            ("Int8Array", 8, true) => {
              let arr = js_sys::Int8Array::from(values_js.clone());
              // Use to_vec() for bulk copy
              let values_i8: Vec<i8> = arr.to_vec();
              let values: Vec<i32> = values_i8.into_iter().map(|v| v as i32).collect();
              return Ok(encode_int32_column(&values));
            }
            ("Uint16Array", 16, false) => {
              let arr = js_sys::Uint16Array::from(values_js.clone());
              // Use to_vec() for bulk copy
              let values_u16: Vec<u16> = arr.to_vec();
              let values: Vec<i32> = values_u16.into_iter().map(|v| v as i32).collect();
              return Ok(encode_int32_column(&values));
            }
            ("Int16Array", 16, true) => {
              let arr = js_sys::Int16Array::from(values_js.clone());
              // Use to_vec() for bulk copy
              let values_i16: Vec<i16> = arr.to_vec();
              let values: Vec<i32> = values_i16.into_iter().map(|v| v as i32).collect();
              return Ok(encode_int32_column(&values));
            }
            ("Uint32Array", 32, false) => {
              let arr = js_sys::Uint32Array::from(values_js.clone());
              // Use to_vec() for bulk copy
              // Reinterpret u32 bits as i32 (Parquet stores all Int32 as signed)
              let values_u32: Vec<u32> = arr.to_vec();
              let values: Vec<i32> = values_u32.into_iter().map(|v| v as i32).collect();
              return Ok(encode_int32_column(&values));
            }
            ("Int32Array", 32, true) => {
              let arr = js_sys::Int32Array::from(values_js.clone());
              // Use to_vec() for bulk copy - zero-copy when possible
              let values: Vec<i32> = arr.to_vec();
              return Ok(encode_int32_column(&values));
            }
            ("BigUint64Array", 64, false) => {
              let arr = js_sys::BigUint64Array::from(values_js.clone());
              let len = arr.length() as usize;
              let mut values = Vec::with_capacity(len);
              for i in 0..len {
                let val = arr.get_index(i as u32);
                values.push(val as i64);
              }
              return Ok(encode_int64_column(&values));
            }
            ("BigInt64Array", 64, true) => {
              let arr = js_sys::BigInt64Array::from(values_js.clone());
              let len = arr.length() as usize;
              let mut values = Vec::with_capacity(len);
              for i in 0..len {
                let val = arr.get_index(i as u32);
                values.push(val);
              }
              return Ok(encode_int64_column(&values));
            }
            _ => {}
          }
        }
      }
    }
  }

  // Check if this is an enum with index array BEFORE conversion
  if let Some(PrimitiveLogicalType::Enum) = logical_type {
    if let Some(enum_vals) = enum_values {
      // Try to detect TypedArray first (more efficient)
      if let Ok(ctor) = js_sys::Reflect::get(&values_js, &JsValue::from_str("constructor")) {
        if let Ok(ctor_name_js) = js_sys::Reflect::get(&ctor, &JsValue::from_str("name")) {
          if let Some(ctor_name) = ctor_name_js.as_string() {
            match ctor_name.as_str() {
              "Uint8Array" => {
                let arr = js_sys::Uint8Array::from(values_js.clone());
                let len = arr.length() as usize;
                let indices = arr.to_vec().into_iter().map(|idx| idx as usize);
                let string_values = convert_enum_indices_to_strings(enum_vals, indices, len)?;
                return Ok(encode_string_column(&string_values));
              }
              "Uint16Array" => {
                let arr = js_sys::Uint16Array::from(values_js.clone());
                let len = arr.length() as usize;
                let indices = arr.to_vec().into_iter().map(|idx| idx as usize);
                let string_values = convert_enum_indices_to_strings(enum_vals, indices, len)?;
                return Ok(encode_string_column(&string_values));
              }
              "Uint32Array" => {
                let arr = js_sys::Uint32Array::from(values_js.clone());
                let len = arr.length() as usize;
                let indices = arr.to_vec().into_iter().map(|idx| idx as usize);
                let string_values = convert_enum_indices_to_strings(enum_vals, indices, len)?;
                return Ok(encode_string_column(&string_values));
              }
              _ => {}
            }
          }
        }
      }

      // Fallback to regular array detection
      let arr = js_sys::Array::from(&values_js);
      if arr.length() > 0 {
        let first_val = arr.get(0);
        // Check if first value is a number by trying to convert it
        if !first_val.is_null() && !first_val.is_undefined() && first_val.as_f64().is_some() {
          let len = arr.length() as usize;
          let mut indices = Vec::with_capacity(len);
          for i in 0..arr.length() {
            let idx = js_value_to_enum_index(&arr.get(i))?;
            indices.push(idx);
          }
          let string_values = convert_enum_indices_to_strings(enum_vals, indices.into_iter(), len)?;
          return Ok(encode_string_column(&string_values));
        }
      }
    }
  }

  // Convert values array if needed
  let arr = js_sys::Array::from(&values_js);
  let converted_values = js_sys::Array::new();

  for i in 0..arr.length() {
    let value = arr.get(i);
    let converted = convert_js_value_to_target_type(&value, physical_type, logical_type)?;
    converted_values.push(&converted);
  }

  let converted_js: JsValue = converted_values.into();

  match physical_type {
    PhysicalType::Int32 => {
      // Check if this is an unsigned integer logical type
      let is_unsigned = matches!(
          logical_type,
          Some(PrimitiveLogicalType::Integer(int_type)) if {
              let (_bit_width, is_signed): (usize, bool) = (*int_type).into();
              !is_signed
          }
      );

      if is_unsigned {
        // For unsigned integers, try to get values from the array directly
        // to avoid serde overflow errors
        let arr = js_sys::Array::from(&converted_js);
        let mut values: Vec<i32> = Vec::with_capacity(arr.length() as usize);
        for i in 0..arr.length() {
          let val = arr.get(i);
          if let Some(n) = val.as_f64() {
            // Reinterpret u32 bits as i32 (this is what Parquet expects)
            let u32_val = n as u32;
            values.push(u32_val as i32);
          } else {
            return Err(JsError::new("Invalid value in unsigned integer array"));
          }
        }
        Ok(encode_int32_column(&values))
      } else {
        let values: Vec<i32> = serde_wasm_bindgen::from_value(converted_js)?;
        Ok(encode_int32_column(&values))
      }
    }
    PhysicalType::Int64 => {
      let values: Vec<i64> = serde_wasm_bindgen::from_value(converted_js)?;
      Ok(encode_int64_column(&values))
    }
    PhysicalType::Float => {
      let values: Vec<f32> = serde_wasm_bindgen::from_value(converted_js)?;
      Ok(encode_float_column(&values))
    }
    PhysicalType::Double => {
      let values: Vec<f64> = serde_wasm_bindgen::from_value(converted_js)?;
      Ok(encode_double_column(&values))
    }
    PhysicalType::Boolean => {
      let values: Vec<bool> = serde_wasm_bindgen::from_value(converted_js)?;
      Ok(encode_boolean_column(&values))
    }
    PhysicalType::ByteArray => {
      let values: Vec<String> = serde_wasm_bindgen::from_value(converted_js)?;
      Ok(encode_string_column(&values))
    }
    _ => Err(JsError::new("Unsupported type")),
  }
}

fn encode_definition_levels(def_levels: Vec<u32>, max_def_level: i16) -> Result<Vec<u8>, JsError> {
  let bit_width = get_bit_width(max_def_level);
  let mut encoded = Vec::new();
  hybrid_rle::encode_u32(&mut encoded, def_levels.into_iter(), bit_width)
    .map_err(|e| JsError::new(&format!("Failed to encode definition levels: {e}")))?;
  Ok(encoded)
}

fn is_null(value: &JsValue) -> bool {
  value.is_null() || value.is_undefined()
}

fn process_nullable_column(
  col_arr: &js_sys::Array,
  start: u32,
  end: u32,
  physical_type: PhysicalType,
  max_def_level: i16,
  logical_type: Option<&PrimitiveLogicalType>,
  enum_values: Option<&[String]>,
) -> Result<(Vec<u8>, Vec<u8>), JsError> {
  let mut def_levels = Vec::with_capacity((end - start) as usize);
  let values = js_sys::Array::new();
  let present_level = max_def_level as u32;

  for idx in start..end {
    let value = col_arr.get(idx);
    if is_null(&value) {
      def_levels.push(0);
    } else {
      def_levels.push(present_level);
      // Convert Date/Object before adding to values array
      let converted = convert_js_value_to_target_type(&value, physical_type, logical_type)?;
      values.push(&converted);
    }
  }

  let values_js: JsValue = values.into();
  let encoded_data = encode_values_from_js(physical_type, values_js, logical_type, enum_values)?;
  let def_buffer = encode_definition_levels(def_levels, max_def_level)?;

  Ok((encoded_data, def_buffer))
}

fn create_data_page(
  encoded_data: Vec<u8>,
  definition_levels: Option<Vec<u8>>,
  num_rows: usize,
  descriptor: Descriptor,
) -> DataPage {
  let mut buffer = Vec::new();

  if descriptor.max_def_level > 0 {
    let def_levels = definition_levels.unwrap_or_default();
    buffer.extend_from_slice(&(def_levels.len() as u32).to_le_bytes());
    buffer.extend_from_slice(&def_levels);
  }

  buffer.extend_from_slice(&encoded_data);

  let header = DataPageHeader::V1(DataPageHeaderV1 {
    num_values: num_rows as i32,
    encoding: Encoding::Plain.into(),
    definition_level_encoding: if descriptor.max_def_level > 0 {
      Encoding::Rle.into()
    } else {
      Encoding::Plain.into()
    },
    repetition_level_encoding: Encoding::Rle.into(),
    statistics: None,
  });

  DataPage::new(header, buffer, descriptor, Some(num_rows))
}

/// Write data to Parquet format
///
/// # Arguments
/// * `schema` - Array of column definitions [{name: "col1", type: "int32", nullable: false}, ...]
/// * `data` - Object with column names as keys and arrays as values {col1: [1,2,3], col2: ["a","b","c"]}
/// * `config` - Optional configuration {compression: "snappy", row_group_size: 10000}
#[wasm_bindgen(js_name = writeParquet)]
pub fn write_parquet(
  schema_js: JsValue,
  data_js: JsValue,
  config_js: Option<JsValue>,
) -> Result<Vec<u8>, JsError> {
  let columns: Vec<ColumnSchema> = serde_wasm_bindgen::from_value(schema_js)?;
  let config: WriteConfig = config_js
    .map(serde_wasm_bindgen::from_value)
    .transpose()?
    .unwrap_or_default();

  let compression = get_compression(&config.compression);

  // Create schema descriptor
  let fields = build_schema_fields(&columns)?;
  let schema_descriptor = SchemaDescriptor::new("schema".to_string(), fields);

  let version = match config.version.to_lowercase().as_str() {
    "v1" | "1" => Version::V1,
    "v2" | "2" => Version::V2,
    _ => Version::V1, // Default to V1 for compatibility
  };

  let options = WriteOptions {
    write_statistics: false,
    version,
  };

  let mut buffer = Cursor::new(Vec::new());
  let mut writer = FileWriter::new(&mut buffer, schema_descriptor, options, None);

  // Get column data from JS object
  let data_obj =
    js_sys::Object::try_from(&data_js).ok_or_else(|| JsError::new("Data must be an object"))?;

  // Determine number of rows from first column
  let first_col = &columns[0];
  let first_data = js_sys::Reflect::get(data_obj, &JsValue::from_str(&first_col.name))
    .map_err(|_| JsError::new("Failed to get column data"))?;
  let first_arr = js_sys::Array::from(&first_data);
  let total_rows = first_arr.length() as usize;

  // Process in row groups
  let mut row_offset = 0;
  while row_offset < total_rows {
    let rows_in_group = std::cmp::min(config.row_group_size, total_rows - row_offset);

    // Build column iterators for this row group
    let mut column_iters: Vec<
      Result<
        DynStreamingIterator<'static, CompressedPage, parquet2::error::Error>,
        parquet2::error::Error,
      >,
    > = Vec::new();

    for col in &columns {
      let col_data = js_sys::Reflect::get(data_obj, &JsValue::from_str(&col.name))
        .map_err(|_| JsError::new(&format!("Failed to get column: {}", col.name)))?;

      let start = row_offset as u32;
      let end = (row_offset + rows_in_group) as u32;

      let descriptor = create_descriptor(col);
      let physical_type = get_physical_type(&col.col_type);
      let logical_type = col.logical_type.as_ref().and_then(|lt| {
        get_logical_type(
          lt,
          physical_type,
          col.precision,
          col.scale,
          col.bit_width,
          col.is_signed,
        )
      });
      let enum_values_ref = col.enum_values.as_deref();

      // Check if this is a TypedArray BEFORE converting to js_sys::Array
      // (conversion loses type information)
      let is_typed_array =
        if let Ok(ctor) = js_sys::Reflect::get(&col_data, &JsValue::from_str("constructor")) {
          if let Ok(ctor_name_js) = js_sys::Reflect::get(&ctor, &JsValue::from_str("name")) {
            if let Some(ctor_name) = ctor_name_js.as_string() {
              matches!(
                ctor_name.as_str(),
                "Uint8Array"
                  | "Int8Array"
                  | "Uint16Array"
                  | "Int16Array"
                  | "Uint32Array"
                  | "Int32Array"
                  | "Float32Array"
                  | "Float64Array"
                  | "BigInt64Array"
                  | "BigUint64Array"
              )
            } else {
              false
            }
          } else {
            false
          }
        } else {
          false
        };

      let (encoded_data, def_levels) = if col.nullable {
        let col_arr = js_sys::Array::from(&col_data);
        process_nullable_column(
          &col_arr,
          start,
          end,
          physical_type,
          descriptor.max_def_level,
          logical_type.as_ref(),
          enum_values_ref,
        )?
      } else {
        // For TypedArrays, use subarray to preserve type; for regular arrays, use slice
        let slice_value = if is_typed_array {
          if let Ok(subarray_fn) = js_sys::Reflect::get(&col_data, &JsValue::from_str("subarray")) {
            if let Some(func) = subarray_fn.dyn_ref::<js_sys::Function>() {
              func
                .call2(&col_data, &JsValue::from(start), &JsValue::from(end))
                .unwrap_or_else(|_| {
                  let col_arr = js_sys::Array::from(&col_data);
                  col_arr.slice(start, end).into()
                })
            } else {
              let col_arr = js_sys::Array::from(&col_data);
              col_arr.slice(start, end).into()
            }
          } else {
            let col_arr = js_sys::Array::from(&col_data);
            col_arr.slice(start, end).into()
          }
        } else {
          let col_arr = js_sys::Array::from(&col_data);
          col_arr.slice(start, end).into()
        };
        (
          encode_values_from_js(
            physical_type,
            slice_value,
            logical_type.as_ref(),
            enum_values_ref,
          )?,
          Vec::new(),
        )
      };
      let definition_levels = if col.nullable { Some(def_levels) } else { None };

      let page = create_data_page(encoded_data, definition_levels, rows_in_group, descriptor);

      // Compress the page and create an iterator
      let pages = vec![Ok(Page::Data(page))];
      let page_iter = DynIter::new(pages.into_iter());
      let compressor: Compressor<_> = Compressor::new_from_vec(page_iter, compression, vec![]);

      column_iters.push(Ok(DynStreamingIterator::new(compressor)));
    }

    let row_group_iter = DynIter::new(column_iters.into_iter());
    writer
      .write(row_group_iter)
      .map_err(|e| JsError::new(&format!("Failed to write row group: {:?}", e)))?;

    row_offset += rows_in_group;
  }

  writer
    .end(None)
    .map_err(|e| JsError::new(&format!("Failed to finalize file: {:?}", e)))?;

  Ok(buffer.into_inner())
}

/// Get the version of this library
#[wasm_bindgen(js_name = getVersion)]
pub fn get_version() -> String {
  env!("CARGO_PKG_VERSION").to_string()
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_get_physical_type() {
    assert_eq!(get_physical_type("int32"), PhysicalType::Int32);
    assert_eq!(get_physical_type("INT32"), PhysicalType::Int32);
    assert_eq!(get_physical_type("string"), PhysicalType::ByteArray);
    assert_eq!(get_physical_type("double"), PhysicalType::Double);
  }

  #[test]
  fn test_get_compression() {
    matches!(get_compression("snappy"), CompressionOptions::Snappy);
    matches!(get_compression("none"), CompressionOptions::Uncompressed);
  }

  #[test]
  fn test_encode_int32() {
    let values = vec![1i32, 2, 3];
    let encoded = encode_int32_column(&values);
    assert_eq!(encoded.len(), 12); // 3 * 4 bytes
  }

  #[test]
  fn test_encode_string() {
    let values = vec!["hello".to_string(), "world".to_string()];
    let encoded = encode_string_column(&values);
    // 4 bytes length + 5 bytes "hello" + 4 bytes length + 5 bytes "world"
    assert_eq!(encoded.len(), 18);
  }
}
