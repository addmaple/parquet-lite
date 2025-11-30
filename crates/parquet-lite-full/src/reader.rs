use parquet2::{
    encoding::{
        bitpacked,
        delta_bitpacked,
        delta_length_byte_array,
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

fn decode_levels(
    data: &[u8],
    max_level: i16,
    total_values: usize,
    level_name: &str,
) -> Result<Option<Vec<u32>>, JsError> {
    if max_level == 0 || total_values == 0 {
        return Ok(None);
    }

    let bit_width = get_bit_width(max_level);
    if bit_width == 0 {
        return Ok(Some(vec![0; total_values]));
    }

    if data.is_empty() {
        return Ok(Some(vec![max_level as u32; total_values]));
    }

    let decoder = HybridRleDecoder::new(data, bit_width as usize);
    let mut levels = Vec::with_capacity(total_values);

    for run in decoder {
        let run = run
            .map_err(|e| JsError::new(&format!("Failed to decode {} levels: {:?}", level_name, e)))?;
        match run {
            HybridEncoded::Bitpacked(values) => {
                let remaining = total_values - levels.len();
                if remaining == 0 {
                    break;
                }
                let mut iter = bitpacked::Decoder::<u32>::try_new(values, bit_width as usize, remaining)
                    .map_err(|e| {
                        JsError::new(&format!(
                            "Failed to decode bitpacked {} levels: {:?}",
                            level_name, e
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
        levels.resize(total_values, max_level as u32);
    }

    Ok(Some(levels))
}

fn decode_definition_levels(
    data: &[u8],
    max_def_level: i16,
    total_values: usize,
) -> Result<Option<Vec<u32>>, JsError> {
    decode_levels(data, max_def_level, total_values, "definition")
}

fn decode_repetition_levels(
    data: &[u8],
    max_rep_level: i16,
    total_values: usize,
) -> Result<Option<Vec<u32>>, JsError> {
    decode_levels(data, max_rep_level, total_values, "repetition")
}

/// Group flat values into nested arrays based on repetition levels
/// rep_level 0 = start a new top-level list
/// rep_level > 0 = continue the current list
fn group_by_repetition(values: &js_sys::Array, rep_levels: &[u32]) -> js_sys::Array {
    let result = js_sys::Array::new();
    
    if rep_levels.is_empty() || values.length() == 0 {
        return result;
    }
    
    let mut current_list = js_sys::Array::new();
    let values_len = values.length() as usize;
    
    for (i, &rep_level) in rep_levels.iter().enumerate() {
        if i >= values_len {
            break;
        }
        
        if rep_level == 0 && current_list.length() > 0 {
            // Start of new row - push current list and start fresh
            result.push(&current_list);
            current_list = js_sys::Array::new();
        }
        
        // Add value to current list
        current_list.push(&values.get(i as u32));
    }
    
    // Don't forget the last list
    if current_list.length() > 0 {
        result.push(&current_list);
    }
    
    result
}

fn convert_value_to_date(
    value: &JsValue,
    logical_type: Option<&PrimitiveLogicalType>,
    _physical_type: PhysicalType,
) -> Result<JsValue, JsError> {
    if let Some(lt) = logical_type {
        match lt {
            PrimitiveLogicalType::Date => {
                // Date: days since Unix epoch (INT32) -> Date object
                // Value should be a number (i32 converted to JsValue)
                if let Some(days_f64) = value.as_f64() {
                    let days_i32 = days_f64 as i32;
                    let timestamp_ms = (days_i32 as i64) * 86_400_000;
                    let date = js_sys::Date::new(&JsValue::from_f64(timestamp_ms as f64));
                    return Ok(date.into());
                }
            }
            PrimitiveLogicalType::Timestamp { unit, .. } => {
                // Timestamp: milliseconds/microseconds/nanoseconds since Unix epoch (INT64) -> Date object
                // Value is already converted to f64 in the iterator
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
                // Time: milliseconds/microseconds/nanoseconds since midnight -> Date object (at epoch + time)
                // Can be INT32 or INT64 depending on unit
                if let Some(time_f64) = value.as_f64() {
                    let time_ms = match unit {
                        parquet2::schema::types::TimeUnit::Milliseconds => time_f64 as i64,
                        parquet2::schema::types::TimeUnit::Microseconds => (time_f64 / 1_000.0) as i64,
                        parquet2::schema::types::TimeUnit::Nanoseconds => (time_f64 / 1_000_000.0) as i64,
                    };
                    // Create date at epoch + time offset
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
                
                // Convert based on logical type
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
            // Convert based on logical type
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

// Helper functions for safe array conversions
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

/// Decode a dictionary page to extract the dictionary values
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
            // Use BigInt unless it's a timestamp/time that needs f64 for Date conversion
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

/// Decode dictionary-encoded indices from a data page
fn decode_dictionary_indices(
    data_page: &DataPage,
    present_values: usize,
) -> Result<Vec<usize>, JsError> {
    let (_, _, values_buffer) = split_buffer(data_page)
        .map_err(|e| JsError::new(&format!("Failed to split buffer: {:?}", e)))?;
    
    // Dictionary indices are RLE/bit-packed encoded
    // First 4 bytes are the bit width
    if values_buffer.is_empty() {
        return Ok(vec![]);
    }
    
    let bit_width = values_buffer[0] as usize;
    if bit_width == 0 {
        // All values are the same (index 0)
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

/// Read metadata from a Parquet file
/// 
/// # Arguments
/// * `data` - Uint8Array containing the Parquet file data
#[wasm_bindgen(js_name = readMetadata)]
pub fn read_parquet_metadata(data: &[u8]) -> Result<JsValue, JsError> {
    let mut cursor = Cursor::new(data);
    let metadata = read_metadata(&mut cursor)
        .map_err(|e| JsError::new(&format!("Failed to read metadata: {:?}", e)))?;

    // Use schema.columns() to get all leaf columns including nested
    let columns: Vec<ColumnInfo> = metadata
        .schema()
        .columns()
        .iter()
        .map(|cd| {
            let pt = &cd.descriptor.primitive_type;
            ColumnInfo {
                name: cd.path_in_schema.join("."),
                col_type: physical_type_to_string(pt.physical_type),
                nullable: pt.field_info.repetition != Repetition::Required,
                logical_type: pt.logical_type.as_ref().map(logical_type_to_string),
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

/// Read a Parquet file and return data as a JavaScript object
/// 
/// # Arguments
/// * `data` - Uint8Array containing the Parquet file data
/// * `columns` - Optional array of column names to read (reads all if not specified)
#[wasm_bindgen(js_name = readParquet)]
pub fn read_parquet(data: &[u8], columns: Option<Vec<String>>) -> Result<JsValue, JsError> {
    let mut cursor = Cursor::new(data);
    let metadata = read_metadata(&mut cursor)
        .map_err(|e| JsError::new(&format!("Failed to read metadata: {:?}", e)))?;

    let schema = metadata.schema();
    let result = js_sys::Object::new();
    
    // Use schema.columns() for leaf primitives (handles nested types correctly)
    let leaf_columns = schema.columns();

    // Determine which columns to read (use path_in_schema for nested)
    let columns_to_read: Vec<String> = if let Some(ref cols) = columns {
        cols.clone()
    } else {
        leaf_columns
            .iter()
            .map(|cd| cd.path_in_schema.join("."))
            .collect()
    };

    for row_group in &metadata.row_groups {
        for (col_idx, col_descriptor) in leaf_columns.iter().enumerate() {
            let primitive_type = &col_descriptor.descriptor.primitive_type;
            let max_rep_level = col_descriptor.descriptor.max_rep_level;
            
            // Use path for column name (for nested: "list.element", for flat: "column_name")
            let col_name = col_descriptor.path_in_schema.join(".");
            
            if !columns_to_read.iter().any(|c| c == &col_name || col_descriptor.path_in_schema.first().map(|s| s.as_str()) == Some(c.as_str())) {
                continue;
            }

            let column_meta = &row_group.columns()[col_idx];
            
            // Read column data
            let mut reader = Cursor::new(data);
            let pages = get_page_iterator(column_meta, &mut reader, None, vec![], usize::MAX)
                .map_err(|e| JsError::new(&format!("Failed to read pages: {:?}", e)))?;

            let values_array = js_sys::Array::new();
            let all_rep_levels: std::cell::RefCell<Vec<u32>> = std::cell::RefCell::new(Vec::new());
            let logical_type = primitive_type.logical_type.as_ref();
            let physical_type = primitive_type.physical_type;
            
            // Dictionary for this column (if dictionary-encoded)
            let mut dictionary: Option<Vec<JsValue>> = None;

            for page_result in pages {
                let page = page_result
                    .map_err(|e| JsError::new(&format!("Failed to read page: {:?}", e)))?;
                
                let page = decompress(page, &mut vec![])
                    .map_err(|e| JsError::new(&format!("Failed to decompress: {:?}", e)))?;

                match page {
                    parquet2::page::Page::Dict(dict_page) => {
                        // Store the dictionary for later use
                        dictionary = Some(decode_dictionary_page(&dict_page, physical_type, logical_type)?);
                    }
                    parquet2::page::Page::Data(data_page) => {
                        let descriptor = data_page.descriptor.clone();
                        let (rep_levels_buf, def_levels, values_buffer) = split_buffer(&data_page)
                            .map_err(|e| JsError::new(&format!("Failed to parse page buffer: {:?}", e)))?;
                        
                        // Decode repetition levels for nested types
                        if max_rep_level > 0 {
                            if let Some(rep_levels) = decode_repetition_levels(
                                rep_levels_buf,
                                max_rep_level,
                                data_page.num_values(),
                            )? {
                                all_rep_levels.borrow_mut().extend(rep_levels);
                            }
                        }

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
                        
                        // Get the page encoding via data_page.encoding() method
                        let page_encoding = data_page.encoding();
                        
                        let is_dict_encoded = page_encoding == Encoding::PlainDictionary 
                            || page_encoding == Encoding::RleDictionary;
                        let is_delta_packed = page_encoding == Encoding::DeltaBinaryPacked;
                        let is_delta_length_byte_array = page_encoding == Encoding::DeltaLengthByteArray;
                        let is_delta_byte_array = page_encoding == Encoding::DeltaByteArray;
                        
                        if is_dict_encoded {
                            // Dictionary-encoded page - decode indices and look up values
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
                        } else if is_delta_packed {
                            // Delta binary packed encoding - for Int32/Int64
                            match physical_type {
                                PhysicalType::Int32 | PhysicalType::Int64 => {
                                    let decoder = delta_bitpacked::Decoder::try_new(values_buffer)
                                        .map_err(|e| JsError::new(&format!("Delta decode error: {:?}", e)))?;
                                    
                                    let values: Vec<i64> = decoder
                                        .collect::<Result<Vec<_>, _>>()
                                        .map_err(|e| JsError::new(&format!("Delta decode error: {:?}", e)))?;
                                    
                                    // Check if this is an unsigned integer logical type
                                    let is_unsigned = matches!(
                                        logical_type,
                                        Some(PrimitiveLogicalType::Integer(int_type)) if {
                                            let (_bit_width, is_signed): (usize, bool) = (*int_type).into();
                                            !is_signed
                                        }
                                    );
                                    
                                    let use_bigint = matches!(physical_type, PhysicalType::Int64) && !matches!(
                                        logical_type,
                                        Some(PrimitiveLogicalType::Timestamp { .. } | PrimitiveLogicalType::Time { .. })
                                    );
                                    
                                    append_values_with_nulls(
                                        &values_array,
                                        definition_levels.as_deref(),
                                        descriptor.max_def_level,
                                        values.into_iter().map(|v| {
                                            if use_bigint {
                                                js_sys::BigInt::from(v).into()
                                            } else if is_unsigned {
                                                JsValue::from(v as u64)
                                            } else if matches!(physical_type, PhysicalType::Int32) {
                                                JsValue::from(v as i32)
                                            } else {
                                                JsValue::from(v as f64)
                                            }
                                        }),
                                        logical_type,
                                        physical_type,
                                    )?;
                                }
                                _ => {
                                    return Err(JsError::new(&format!(
                                        "Delta binary packed encoding not supported for {:?}",
                                        physical_type
                                    )));
                                }
                            }
                        } else if is_delta_length_byte_array {
                            // Delta length byte array encoding - lengths are delta encoded, values follow
                            let mut decoder = delta_length_byte_array::Decoder::try_new(values_buffer)
                                .map_err(|e| JsError::new(&format!("Delta length decode error: {:?}", e)))?;
                            
                            // Collect lengths first
                            let lengths: Vec<i32> = decoder.by_ref()
                                .collect::<Result<Vec<_>, _>>()
                                .map_err(|e| JsError::new(&format!("Delta length decode error: {:?}", e)))?;
                            
                            // Get the raw value bytes
                            let all_values = decoder.into_values();
                            
                            // Split by lengths to get individual strings
                            let mut offset = 0usize;
                            let values: Vec<String> = lengths.iter().map(|&len| {
                                let len = len as usize;
                                let end = offset + len;
                                let s = if end <= all_values.len() {
                                    String::from_utf8_lossy(&all_values[offset..end]).into_owned()
                                } else {
                                    String::new()
                                };
                                offset = end;
                                s
                            }).collect();
                            
                            append_values_with_nulls(
                                &values_array,
                                definition_levels.as_deref(),
                                descriptor.max_def_level,
                                values.into_iter().map(|v| JsValue::from_str(&v)),
                                logical_type,
                                physical_type,
                            )?;
                        } else if is_delta_byte_array {
                            // Delta byte array encoding - for strings with common prefixes
                            let mut prefix_decoder = parquet2::encoding::delta_byte_array::Decoder::try_new(values_buffer)
                                .map_err(|e| JsError::new(&format!("Delta byte array decode error: {:?}", e)))?;
                            
                            // Get prefix lengths
                            let prefixes: Vec<u32> = prefix_decoder.by_ref()
                                .collect::<Result<Vec<_>, _>>()
                                .map_err(|e| JsError::new(&format!("Delta byte array prefix error: {:?}", e)))?;
                            
                            // Move to lengths decoder
                            let mut length_decoder = prefix_decoder.into_lengths()
                                .map_err(|e| JsError::new(&format!("Delta byte array lengths error: {:?}", e)))?;
                            
                            // Get suffix lengths
                            let suffix_lengths: Vec<i32> = length_decoder.by_ref()
                                .collect::<Result<Vec<_>, _>>()
                                .map_err(|e| JsError::new(&format!("Delta byte array suffix error: {:?}", e)))?;
                            
                            // Get suffix values
                            let suffix_values = length_decoder.values();
                            
                            // Reconstruct strings by applying prefixes
                            let mut values: Vec<String> = Vec::with_capacity(prefixes.len());
                            let mut suffix_offset = 0usize;
                            let mut previous = String::new();
                            
                            for (i, &prefix_len) in prefixes.iter().enumerate() {
                                let suffix_len = suffix_lengths.get(i).copied().unwrap_or(0) as usize;
                                let suffix_end = suffix_offset + suffix_len;
                                
                                // Start with prefix from previous string
                                let prefix = if (prefix_len as usize) <= previous.len() {
                                    &previous[..prefix_len as usize]
                                } else {
                                    &previous[..]
                                };
                                
                                // Append suffix
                                let suffix = if suffix_end <= suffix_values.len() {
                                    String::from_utf8_lossy(&suffix_values[suffix_offset..suffix_end])
                                } else {
                                    std::borrow::Cow::Borrowed("")
                                };
                                
                                let current = format!("{}{}", prefix, suffix);
                                values.push(current.clone());
                                previous = current;
                                suffix_offset = suffix_end;
                            }
                            
                            append_values_with_nulls(
                                &values_array,
                                definition_levels.as_deref(),
                                descriptor.max_def_level,
                                values.into_iter().map(|v| JsValue::from_str(&v)),
                                logical_type,
                                physical_type,
                            )?;
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
                                    
                                    // Check if this is an unsigned integer logical type
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
                                                // Reinterpret i32 bits as u32
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

            // For nested types (max_rep_level > 0), group values into arrays
            let final_values: js_sys::Array = if max_rep_level > 0 {
                let rep_levels = all_rep_levels.borrow();
                if !rep_levels.is_empty() {
                    group_by_repetition(&values_array, &rep_levels)
                } else {
                    values_array
                }
            } else {
                values_array
            };
            
            // Get or create array for this column
            let existing = js_sys::Reflect::get(&result, &JsValue::from_str(&col_name))
                .unwrap_or(JsValue::UNDEFINED);
            
            if existing.is_undefined() {
                js_sys::Reflect::set(&result, &JsValue::from_str(&col_name), &final_values)
                    .map_err(|_| JsError::new("Failed to set result property"))?;
            } else {
                // Append to existing array - use unchecked_ref to get the actual array reference
                let existing_arr: &js_sys::Array = existing.unchecked_ref();
                for i in 0..final_values.length() {
                    existing_arr.push(&final_values.get(i));
                }
            }
        }
    }

    Ok(result.into())
}
