use parquet2::{
    encoding::{
        bitpacked,
        hybrid_rle::{Decoder as HybridRleDecoder, HybridEncoded},
    },
    page::split_buffer,
    read::{decompress, get_page_iterator, read_metadata, levels::get_bit_width},
    schema::types::PhysicalType,
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

fn append_values_with_nulls<I>(
    target: &js_sys::Array,
    def_levels: Option<&[u32]>,
    max_def_level: i16,
    mut values: I,
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
                target.push(&value);
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
            target.push(&value);
        }
    }

    Ok(())
}

fn decode_fixed_width_values<T, F>(
    buffer: &[u8],
    count: usize,
    width: usize,
    mut convert: F,
) -> Result<Vec<T>, JsError>
where
    F: FnMut(&[u8]) -> T,
{
    let mut offset = 0;
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + width > buffer.len() {
            return Err(JsError::new("Unexpected end of buffer while decoding values"));
        }
        let value = convert(&buffer[offset..offset + width]);
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
        let len = u32::from_le_bytes(buffer[offset..offset + 4].try_into().unwrap()) as usize;
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

/// Read metadata from a Parquet file
/// 
/// # Arguments
/// * `data` - Uint8Array containing the Parquet file data
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

    // Determine which columns to read
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
                
                // Read column data
                let mut reader = Cursor::new(data);
                let pages = get_page_iterator(column_meta, &mut reader, None, vec![], usize::MAX)
                    .map_err(|e| JsError::new(&format!("Failed to read pages: {:?}", e)))?;

                let values_array = js_sys::Array::new();

                for page_result in pages {
                    let page = page_result
                        .map_err(|e| JsError::new(&format!("Failed to read page: {:?}", e)))?;
                    
                    let page = decompress(page, &mut vec![])
                        .map_err(|e| JsError::new(&format!("Failed to decompress: {:?}", e)))?;

                    let data_page = match page {
                        parquet2::page::Page::Data(dp) => dp,
                        _ => continue,
                    };

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
                    
                    match primitive_type.physical_type {
                        PhysicalType::Int32 => {
                            let values = decode_fixed_width_values(
                                values_buffer,
                                present_values,
                                4,
                                |chunk| i32::from_le_bytes(chunk.try_into().unwrap()),
                            )?;
                            append_values_with_nulls(
                                &values_array,
                                definition_levels.as_deref(),
                                descriptor.max_def_level,
                                values.into_iter().map(JsValue::from),
                            )?;
                        }
                        PhysicalType::Int64 => {
                            let values = decode_fixed_width_values(
                                values_buffer,
                                present_values,
                                8,
                                |chunk| i64::from_le_bytes(chunk.try_into().unwrap()),
                            )?;
                            append_values_with_nulls(
                                &values_array,
                                definition_levels.as_deref(),
                                descriptor.max_def_level,
                                values.into_iter().map(|v| JsValue::from(v as f64)),
                            )?;
                        }
                        PhysicalType::Float => {
                            let values = decode_fixed_width_values(
                                values_buffer,
                                present_values,
                                4,
                                |chunk| f32::from_le_bytes(chunk.try_into().unwrap()),
                            )?;
                            append_values_with_nulls(
                                &values_array,
                                definition_levels.as_deref(),
                                descriptor.max_def_level,
                                values.into_iter().map(JsValue::from),
                            )?;
                        }
                        PhysicalType::Double => {
                            let values = decode_fixed_width_values(
                                values_buffer,
                                present_values,
                                8,
                                |chunk| f64::from_le_bytes(chunk.try_into().unwrap()),
                            )?;
                            append_values_with_nulls(
                                &values_array,
                                definition_levels.as_deref(),
                                descriptor.max_def_level,
                                values.into_iter().map(JsValue::from),
                            )?;
                        }
                        PhysicalType::Boolean => {
                            let values = decode_boolean_values(values_buffer, present_values)?;
                            append_values_with_nulls(
                                &values_array,
                                definition_levels.as_deref(),
                                descriptor.max_def_level,
                                values.into_iter().map(JsValue::from),
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
                            )?;
                        }
                        _ => {}
                    }
                }

                // Get or create array for this column
                let existing = js_sys::Reflect::get(&result, &JsValue::from_str(col_name))
                    .unwrap_or(JsValue::UNDEFINED);
                
                if existing.is_undefined() {
                    js_sys::Reflect::set(&result, &JsValue::from_str(col_name), &values_array)
                        .map_err(|_| JsError::new("Failed to set result property"))?;
                } else {
                    // Append to existing array
                    let existing_arr = js_sys::Array::from(&existing);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_physical_type_to_string() {
        assert_eq!(physical_type_to_string(PhysicalType::Int32), "int32");
        assert_eq!(physical_type_to_string(PhysicalType::Double), "double");
        assert_eq!(physical_type_to_string(PhysicalType::ByteArray), "string");
    }
}
