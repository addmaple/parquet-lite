use parquet2::{
    encoding::{
        bitpacked,
        hybrid_rle::{Decoder as HybridRleDecoder, HybridEncoded},
        Encoding,
    },
    page::{split_buffer, DictPage, DataPage},
    read::{decompress, get_page_iterator, read_metadata, levels::get_bit_width},
    schema::types::{PhysicalType, PrimitiveLogicalType},
    schema::Repetition,
};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use wasm_bindgen::prelude::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
    pub nullable: bool,
    #[serde(default)]
    pub logical_type: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ParquetMetadata {
    pub num_rows: u64,
    pub num_row_groups: usize,
    pub columns: Vec<ColumnInfo>,
}

fn physical_type_to_string(pt: PhysicalType) -> String {
    match pt {
        PhysicalType::Boolean => "boolean",
        PhysicalType::Int32 => "int32",
        PhysicalType::Int64 => "int64",
        PhysicalType::Int96 => "int96",
        PhysicalType::Float => "float",
        PhysicalType::Double => "double",
        PhysicalType::ByteArray => "string",
        PhysicalType::FixedLenByteArray(_) => "bytes",
    }
    .to_string()
}

fn logical_type_to_string(lt: &PrimitiveLogicalType) -> String {
    match lt {
        PrimitiveLogicalType::Date => "date".to_string(),
        PrimitiveLogicalType::Time { unit, .. } => {
            match unit {
                parquet2::schema::types::TimeUnit::Milliseconds => "time_millis".to_string(),
                parquet2::schema::types::TimeUnit::Microseconds => "time_micros".to_string(),
                parquet2::schema::types::TimeUnit::Nanoseconds => "time_nanos".to_string(),
            }
        }
        PrimitiveLogicalType::Timestamp { unit, .. } => {
            match unit {
                parquet2::schema::types::TimeUnit::Milliseconds => "timestamp_millis".to_string(),
                parquet2::schema::types::TimeUnit::Microseconds => "timestamp_micros".to_string(),
                parquet2::schema::types::TimeUnit::Nanoseconds => "timestamp_nanos".to_string(),
            }
        }
        PrimitiveLogicalType::String => "utf8".to_string(),
        PrimitiveLogicalType::Json => "json".to_string(),
        PrimitiveLogicalType::Bson => "bson".to_string(),
        PrimitiveLogicalType::Decimal(precision, scale) => format!("decimal({},{})", precision, scale),
        PrimitiveLogicalType::Enum => "enum".to_string(),
        PrimitiveLogicalType::Integer(int_type) => {
            let (bit_width, is_signed): (usize, bool) = (*int_type).into();
            format!("integer({},{})", bit_width, if is_signed { "signed" } else { "unsigned" })
        }
        PrimitiveLogicalType::Uuid => "uuid".to_string(),
        _ => "unknown".to_string(),
    }
}

fn decode_rle_value(bytes: &[u8], bit_width: usize) -> u32 {
    let mut value = 0u32;
    for (i, byte) in bytes.iter().enumerate() {
        value |= (*byte as u32) << (i * 8);
    }
    if bit_width >= 32 {
        value
    } else {
        let mask = (1u32 << bit_width) - 1;
        value & mask
    }
}

fn decode_definition_levels(
    data: &[u8],
    max_def_level: i16,
    total_values: usize,
) -> Result<Option<Vec<u32>>, JsError> {
    if max_def_level == 0 || total_values == 0 {
        return Ok(None);
    }

    let bit_width = get_bit_width(max_def_level);
    if bit_width == 0 {
        return Ok(Some(vec![0; total_values]));
    }

    if data.is_empty() {
        return Ok(Some(vec![max_def_level as u32; total_values]));
    }

    let decoder = HybridRleDecoder::new(data, bit_width as usize);
    let mut levels = Vec::with_capacity(total_values);

    for run in decoder {
        let run = run
            .map_err(|e| JsError::new(&format!("Failed to decode definition levels: {:?}", e)))?;
        match run {
            HybridEncoded::Bitpacked(values) => {
                let remaining = total_values - levels.len();
                if remaining == 0 {
                    break;
                }
                let mut iter = bitpacked::Decoder::<u32>::try_new(values, bit_width as usize, remaining)
                    .map_err(|e| {
                        JsError::new(&format!(
                            "Failed to decode bitpacked definition levels: {:?}",
                            e
                        ))
                    })?;
                levels.extend(iter.by_ref().take(remaining));
            }
            HybridEncoded::Rle(bytes, run_len) => {
                let value = decode_rle_value(bytes, bit_width as usize);
                let take = run_len.min(total_values - levels.len());
                levels.extend(std::iter::repeat_n(value, take));
            }
        }

        if levels.len() >= total_values {
            break;
        }
    }

    if levels.len() < total_values {
        levels.resize(total_values, max_def_level as u32);
    }

    Ok(Some(levels))
}

fn convert_value_to_date(
    value: &JsValue,
    logical_type: Option<&PrimitiveLogicalType>,
    _physical_type: PhysicalType,
) -> Result<JsValue, JsError> {
    if let Some(lt) = logical_type {
        match lt {
            PrimitiveLogicalType::Date => {
                if let Some(days_f64) = value.as_f64() {
                    let days_i32 = days_f64 as i32;
                    let timestamp_ms = (days_i32 as i64) * 86_400_000;
                    let date = js_sys::Date::new(&JsValue::from_f64(timestamp_ms as f64));
                    return Ok(date.into());
                }
            }
            PrimitiveLogicalType::Timestamp { unit, .. } => {
                if let Some(timestamp_f64) = value.as_f64() {
                    let timestamp_ms = match unit {
                        parquet2::schema::types::TimeUnit::Milliseconds => timestamp_f64 as i64,
                        parquet2::schema::types::TimeUnit::Microseconds => (timestamp_f64 / 1_000.0) as i64,
                        parquet2::schema::types::TimeUnit::Nanoseconds => (timestamp_f64 / 1_000_000.0) as i64,
                    };
                    let date = js_sys::Date::new(&JsValue::from_f64(timestamp_ms as f64));
                    return Ok(date.into());
                }
            }
            PrimitiveLogicalType::Time { unit, .. } => {
                if let Some(time_f64) = value.as_f64() {
                    let time_ms = match unit {
                        parquet2::schema::types::TimeUnit::Milliseconds => time_f64 as i64,
                        parquet2::schema::types::TimeUnit::Microseconds => (time_f64 / 1_000.0) as i64,
                        parquet2::schema::types::TimeUnit::Nanoseconds => (time_f64 / 1_000_000.0) as i64,
                    };
                    let date = js_sys::Date::new(&JsValue::from_f64(time_ms as f64));
                    return Ok(date.into());
                }
            }
            _ => {}
        }
    }
    Ok(value.clone())
}

fn convert_json_string_to_object(value: &JsValue) -> Result<JsValue, JsError> {
    if let Some(json_str) = value.as_string() {
        let parsed = js_sys::JSON::parse(&json_str)
            .map_err(|_| JsError::new("Failed to parse JSON string"))?;
        return Ok(parsed);
    }
    Ok(value.clone())
}

fn append_values_with_nulls<I>(
    target: &js_sys::Array,
    def_levels: Option<&[u32]>,
    max_def_level: i16,
    mut values: I,
    logical_type: Option<&PrimitiveLogicalType>,
    physical_type: PhysicalType,
) -> Result<(), JsError>
where
    I: Iterator<Item = JsValue>,
{
    if let Some(levels) = def_levels {
        let present_level = max_def_level as u32;
        for &level in levels {
            if level == present_level {
                let value = values
                    .next()
                    .ok_or_else(|| JsError::new("Missing value for definition level"))?;
                
                let converted = if let Some(PrimitiveLogicalType::Json) = logical_type {
                    convert_json_string_to_object(&value)?
                } else {
                    convert_value_to_date(&value, logical_type, physical_type)?
                };
                
                target.push(&converted);
            } else {
                target.push(&JsValue::NULL);
            }
        }

        if values.next().is_some() {
            return Err(JsError::new(
                "Too many values provided for definition levels",
            ));
        }
    } else {
        for value in values {
            let converted = if let Some(PrimitiveLogicalType::Json) = logical_type {
                convert_json_string_to_object(&value)?
            } else {
                convert_value_to_date(&value, logical_type, physical_type)?
            };
            
            target.push(&converted);
        }
    }

    Ok(())
}

#[inline]
fn slice_to_array<const N: usize>(slice: &[u8]) -> Result<[u8; N], JsError> {
    slice.try_into()
        .map_err(|_| JsError::new(&format!("Invalid slice length for array conversion (expected {}, got {})", N, slice.len())))
}

fn decode_fixed_width_values<T, F>(
    buffer: &[u8],
    count: usize,
    width: usize,
    mut convert: F,
) -> Result<Vec<T>, JsError>
where
    F: FnMut(&[u8]) -> Result<T, JsError>,
{
    let mut offset = 0;
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + width > buffer.len() {
            return Err(JsError::new("Unexpected end of buffer while decoding values"));
        }
        let value = convert(&buffer[offset..offset + width])?;
        result.push(value);
        offset += width;
    }
    Ok(result)
}

fn decode_boolean_values(buffer: &[u8], count: usize) -> Result<Vec<bool>, JsError> {
    let mut result = Vec::with_capacity(count);
    for i in 0..count {
        let byte_idx = i / 8;
        let bit_idx = i % 8;
        if byte_idx >= buffer.len() {
            return Err(JsError::new("Unexpected end of buffer while decoding booleans"));
        }
        let value = (buffer[byte_idx] >> bit_idx) & 1 == 1;
        result.push(value);
    }
    Ok(result)
}

fn decode_binary_values(buffer: &[u8], count: usize) -> Result<Vec<String>, JsError> {
    let mut offset = 0;
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 4 > buffer.len() {
            return Err(JsError::new(
                "Unexpected end of buffer while decoding byte array length",
            ));
        }
        let len_bytes: [u8; 4] = buffer[offset..offset + 4]
            .try_into()
            .map_err(|_| JsError::new("Invalid buffer slice for length"))?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        offset += 4;
        if offset + len > buffer.len() {
            return Err(JsError::new(
                "Unexpected end of buffer while decoding byte array value",
            ));
        }
        let value = String::from_utf8_lossy(&buffer[offset..offset + len]).to_string();
        result.push(value);
        offset += len;
    }
    Ok(result)
}

fn decode_fixed_len_byte_array(buffer: &[u8], count: usize, size: usize) -> Result<Vec<Vec<u8>>, JsError> {
    let mut result = Vec::with_capacity(count);
    for i in 0..count {
        let start = i * size;
        let end = start + size;
        if end > buffer.len() {
            return Err(JsError::new("Unexpected end of buffer while decoding fixed-len byte array"));
        }
        result.push(buffer[start..end].to_vec());
    }
    Ok(result)
}

fn decode_dictionary_page(dict_page: &DictPage, physical_type: PhysicalType, logical_type: Option<&PrimitiveLogicalType>) -> Result<Vec<JsValue>, JsError> {
    let buffer = &dict_page.buffer;
    let num_values = dict_page.num_values;
    
    match physical_type {
        PhysicalType::Int32 => {
            let values = decode_fixed_width_values(buffer, num_values, 4, |chunk| {
                let arr = slice_to_array::<4>(chunk)?;
                Ok(i32::from_le_bytes(arr))
            })?;
            Ok(values.into_iter().map(JsValue::from).collect())
        }
        PhysicalType::Int64 => {
            let values = decode_fixed_width_values(buffer, num_values, 8, |chunk| {
                let arr = slice_to_array::<8>(chunk)?;
                Ok(i64::from_le_bytes(arr))
            })?;
            let use_bigint = !matches!(
                logical_type,
                Some(PrimitiveLogicalType::Timestamp { .. } | PrimitiveLogicalType::Time { .. })
            );
            Ok(values.into_iter().map(|v| {
                if use_bigint {
                    let bigint = js_sys::BigInt::from(v);
                    bigint.into()
                } else {
                    JsValue::from(v as f64)
                }
            }).collect())
        }
        PhysicalType::Float => {
            let values = decode_fixed_width_values(buffer, num_values, 4, |chunk| {
                let arr = slice_to_array::<4>(chunk)?;
                Ok(f32::from_le_bytes(arr))
            })?;
            Ok(values.into_iter().map(JsValue::from).collect())
        }
        PhysicalType::Double => {
            let values = decode_fixed_width_values(buffer, num_values, 8, |chunk| {
                let arr = slice_to_array::<8>(chunk)?;
                Ok(f64::from_le_bytes(arr))
            })?;
            Ok(values.into_iter().map(JsValue::from).collect())
        }
        PhysicalType::ByteArray => {
            let values = decode_binary_values(buffer, num_values)?;
            Ok(values.into_iter().map(|v| JsValue::from_str(&v)).collect())
        }
        PhysicalType::FixedLenByteArray(size) => {
            let values = decode_fixed_len_byte_array(buffer, num_values, size)?;
            Ok(values.into_iter().map(|v| {
                let arr = js_sys::Uint8Array::new_with_length(v.len() as u32);
                arr.copy_from(&v);
                arr.into()
            }).collect())
        }
        _ => Err(JsError::new("Unsupported physical type for dictionary")),
    }
}

fn decode_dictionary_indices(
    data_page: &DataPage,
    present_values: usize,
) -> Result<Vec<usize>, JsError> {
    let (_, _, values_buffer) = split_buffer(data_page)
        .map_err(|e| JsError::new(&format!("Failed to split buffer: {:?}", e)))?;
    
    if values_buffer.is_empty() {
        return Ok(vec![]);
    }
    
    let bit_width = values_buffer[0] as usize;
    if bit_width == 0 {
        return Ok(vec![0; present_values]);
    }
    
    let rle_data = &values_buffer[1..];
    let decoder = HybridRleDecoder::new(rle_data, bit_width);
    let mut indices = Vec::with_capacity(present_values);
    
    for run in decoder {
        let run = run.map_err(|e| JsError::new(&format!("Failed to decode dictionary indices: {:?}", e)))?;
        match run {
            HybridEncoded::Bitpacked(values) => {
                let remaining = present_values.saturating_sub(indices.len());
                if remaining == 0 {
                    break;
                }
                let iter = bitpacked::Decoder::<u32>::try_new(values, bit_width, remaining)
                    .map_err(|e| JsError::new(&format!("Failed to decode bitpacked indices: {:?}", e)))?;
                indices.extend(iter.map(|v| v as usize));
            }
            HybridEncoded::Rle(bytes, run_len) => {
                let value = decode_rle_value(bytes, bit_width) as usize;
                let take = run_len.min(present_values.saturating_sub(indices.len()));
                indices.extend(std::iter::repeat_n(value, take));
            }
        }
        
        if indices.len() >= present_values {
            break;
        }
    }
    
    Ok(indices)
}

/// Read metadata from a Parquet file (lite version)
#[wasm_bindgen(js_name = readMetadata)]
pub fn read_parquet_metadata(data: &[u8]) -> Result<JsValue, JsError> {
    let mut cursor = Cursor::new(data);
    let metadata = read_metadata(&mut cursor)
        .map_err(|e| JsError::new(&format!("Failed to read metadata: {:?}", e)))?;

    let columns: Vec<ColumnInfo> = metadata
        .schema()
        .fields()
        .iter()
        .filter_map(|field| {
            if let parquet2::schema::types::ParquetType::PrimitiveType(pt) = field {
                Some(ColumnInfo {
                    name: pt.field_info.name.clone(),
                    col_type: physical_type_to_string(pt.physical_type),
                    nullable: pt.field_info.repetition != Repetition::Required,
                    logical_type: pt.logical_type.as_ref().map(logical_type_to_string),
                })
            } else {
                None
            }
        })
        .collect();

    let num_rows: u64 = metadata.row_groups.iter().map(|rg| rg.num_rows() as u64).sum();

    let result = ParquetMetadata {
        num_rows,
        num_row_groups: metadata.row_groups.len(),
        columns,
    };

    serde_wasm_bindgen::to_value(&result).map_err(|e| JsError::new(&format!("Serialization error: {:?}", e)))
}

/// Read a Parquet file and return data (lite version - no delta encoding, no nested types)
#[wasm_bindgen(js_name = readParquet)]
pub fn read_parquet(data: &[u8], columns: Option<Vec<String>>) -> Result<JsValue, JsError> {
    let mut cursor = Cursor::new(data);
    let metadata = read_metadata(&mut cursor)
        .map_err(|e| JsError::new(&format!("Failed to read metadata: {:?}", e)))?;

    let schema = metadata.schema();
    let result = js_sys::Object::new();

    let columns_to_read: Vec<&str> = if let Some(ref cols) = columns {
        cols.iter().map(|s| s.as_str()).collect()
    } else {
        schema
            .fields()
            .iter()
            .filter_map(|f| {
                if let parquet2::schema::types::ParquetType::PrimitiveType(pt) = f {
                    Some(pt.field_info.name.as_str())
                } else {
                    None
                }
            })
            .collect()
    };

    for row_group in &metadata.row_groups {
        for (col_idx, field) in schema.fields().iter().enumerate() {
            if let parquet2::schema::types::ParquetType::PrimitiveType(primitive_type) = field {
                let col_name = &primitive_type.field_info.name;
                
                if !columns_to_read.contains(&col_name.as_str()) {
                    continue;
                }

                let column_meta = &row_group.columns()[col_idx];
                
                let mut reader = Cursor::new(data);
                let pages = get_page_iterator(column_meta, &mut reader, None, vec![], usize::MAX)
                    .map_err(|e| JsError::new(&format!("Failed to read pages: {:?}", e)))?;

                let values_array = js_sys::Array::new();
                let logical_type = primitive_type.logical_type.as_ref();
                let physical_type = primitive_type.physical_type;
                
                let mut dictionary: Option<Vec<JsValue>> = None;

                for page_result in pages {
                    let page = page_result
                        .map_err(|e| JsError::new(&format!("Failed to read page: {:?}", e)))?;
                    
                    let page = decompress(page, &mut vec![])
                        .map_err(|e| JsError::new(&format!("Failed to decompress: {:?}", e)))?;

                    match page {
                        parquet2::page::Page::Dict(dict_page) => {
                            dictionary = Some(decode_dictionary_page(&dict_page, physical_type, logical_type)?);
                        }
                        parquet2::page::Page::Data(data_page) => {
                            let descriptor = data_page.descriptor.clone();
                            let (_, def_levels, values_buffer) = split_buffer(&data_page)
                                .map_err(|e| JsError::new(&format!("Failed to parse page buffer: {:?}", e)))?;

                            let definition_levels = decode_definition_levels(
                                def_levels,
                                descriptor.max_def_level,
                                data_page.num_values(),
                            )?;

                            let present_level = descriptor.max_def_level as u32;
                            let present_values = definition_levels
                                .as_ref()
                                .map(|levels| {
                                    levels
                                        .iter()
                                        .filter(|&&level| level == present_level)
                                        .count()
                                })
                                .unwrap_or(data_page.num_values());
                            
                            let page_encoding = data_page.encoding();
                            
                            let is_dict_encoded = page_encoding == Encoding::PlainDictionary 
                                || page_encoding == Encoding::RleDictionary;
                            
                            if is_dict_encoded {
                                if let Some(ref dict) = dictionary {
                                    let indices = decode_dictionary_indices(&data_page, present_values)?;
                                    let values: Vec<JsValue> = indices.iter().map(|&idx| {
                                        dict.get(idx).cloned().unwrap_or(JsValue::NULL)
                                    }).collect();
                                    
                                    append_values_with_nulls(
                                        &values_array,
                                        definition_levels.as_deref(),
                                        descriptor.max_def_level,
                                        values.into_iter(),
                                        logical_type,
                                        physical_type,
                                    )?;
                                } else {
                                    return Err(JsError::new("Dictionary-encoded page without dictionary"));
                                }
                            } else {
                                // Plain-encoded page - decode values directly
                                match physical_type {
                                    PhysicalType::Int32 => {
                                        let values = decode_fixed_width_values(
                                            values_buffer,
                                            present_values,
                                            4,
                                            |chunk| {
                                                let arr = slice_to_array::<4>(chunk)?;
                                                Ok(i32::from_le_bytes(arr))
                                            },
                                        )?;
                                        
                                        let is_unsigned = matches!(
                                            logical_type,
                                            Some(PrimitiveLogicalType::Integer(int_type)) if {
                                                let (_bit_width, is_signed): (usize, bool) = (*int_type).into();
                                                !is_signed
                                            }
                                        );
                                        
                                        append_values_with_nulls(
                                            &values_array,
                                            definition_levels.as_deref(),
                                            descriptor.max_def_level,
                                            values.into_iter().map(|v| {
                                                if is_unsigned {
                                                    JsValue::from(v as u32)
                                                } else {
                                                    JsValue::from(v)
                                                }
                                            }),
                                            logical_type,
                                            physical_type,
                                        )?;
                                    }
                                    PhysicalType::Int64 => {
                                        let values = decode_fixed_width_values(
                                            values_buffer,
                                            present_values,
                                            8,
                                            |chunk| {
                                                let arr = slice_to_array::<8>(chunk)?;
                                                Ok(i64::from_le_bytes(arr))
                                            },
                                        )?;
                                        let use_bigint = !matches!(
                                            logical_type,
                                            Some(PrimitiveLogicalType::Timestamp { .. }
                                                | PrimitiveLogicalType::Time { .. })
                                        );
                                        append_values_with_nulls(
                                            &values_array,
                                            definition_levels.as_deref(),
                                            descriptor.max_def_level,
                                            values.into_iter().map(|v| {
                                                if use_bigint {
                                                    let bigint = js_sys::BigInt::from(v);
                                                    bigint.into()
                                                } else {
                                                    JsValue::from(v as f64)
                                                }
                                            }),
                                            logical_type,
                                            physical_type,
                                        )?;
                                    }
                                    PhysicalType::Float => {
                                        let values = decode_fixed_width_values(
                                            values_buffer,
                                            present_values,
                                            4,
                                            |chunk| {
                                                let arr = slice_to_array::<4>(chunk)?;
                                                Ok(f32::from_le_bytes(arr))
                                            },
                                        )?;
                                        append_values_with_nulls(
                                            &values_array,
                                            definition_levels.as_deref(),
                                            descriptor.max_def_level,
                                            values.into_iter().map(JsValue::from),
                                            logical_type,
                                            physical_type,
                                        )?;
                                    }
                                    PhysicalType::Double => {
                                        let values = decode_fixed_width_values(
                                            values_buffer,
                                            present_values,
                                            8,
                                            |chunk| {
                                                let arr = slice_to_array::<8>(chunk)?;
                                                Ok(f64::from_le_bytes(arr))
                                            },
                                        )?;
                                        append_values_with_nulls(
                                            &values_array,
                                            definition_levels.as_deref(),
                                            descriptor.max_def_level,
                                            values.into_iter().map(JsValue::from),
                                            logical_type,
                                            physical_type,
                                        )?;
                                    }
                                    PhysicalType::Boolean => {
                                        let values = decode_boolean_values(values_buffer, present_values)?;
                                        append_values_with_nulls(
                                            &values_array,
                                            definition_levels.as_deref(),
                                            descriptor.max_def_level,
                                            values.into_iter().map(JsValue::from),
                                            logical_type,
                                            physical_type,
                                        )?;
                                    }
                                    PhysicalType::ByteArray => {
                                        let values = decode_binary_values(values_buffer, present_values)?;
                                        append_values_with_nulls(
                                            &values_array,
                                            definition_levels.as_deref(),
                                            descriptor.max_def_level,
                                            values
                                                .into_iter()
                                                .map(|v| JsValue::from_str(&v)),
                                            logical_type,
                                            physical_type,
                                        )?;
                                    }
                                    PhysicalType::FixedLenByteArray(size) => {
                                        let values = decode_fixed_len_byte_array(values_buffer, present_values, size)?;
                                        append_values_with_nulls(
                                            &values_array,
                                            definition_levels.as_deref(),
                                            descriptor.max_def_level,
                                            values.into_iter().map(|v| {
                                                let arr = js_sys::Uint8Array::new_with_length(v.len() as u32);
                                                arr.copy_from(&v);
                                                arr.into()
                                            }),
                                            logical_type,
                                            physical_type,
                                        )?;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }

                let existing = js_sys::Reflect::get(&result, &JsValue::from_str(col_name))
                    .unwrap_or(JsValue::UNDEFINED);
                
                if existing.is_undefined() {
                    js_sys::Reflect::set(&result, &JsValue::from_str(col_name), &values_array)
                        .map_err(|_| JsError::new("Failed to set result property"))?;
                } else {
                    let existing_arr: &js_sys::Array = existing.unchecked_ref();
                    for i in 0..values_array.length() {
                        existing_arr.push(&values_array.get(i));
                    }
                }
            }
        }
    }

    Ok(result.into())
}

/// Get the version of this library
#[wasm_bindgen(js_name = getVersion)]
pub fn get_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}



