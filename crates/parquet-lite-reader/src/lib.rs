use parquet2::{
    read::{
        decompress, get_page_iterator, read_metadata,
    },
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

                    let buffer = data_page.buffer();
                    
                    match primitive_type.physical_type {
                        PhysicalType::Int32 => {
                            for chunk in buffer.chunks_exact(4) {
                                let value = i32::from_le_bytes(chunk.try_into().unwrap());
                                values_array.push(&JsValue::from(value));
                            }
                        }
                        PhysicalType::Int64 => {
                            for chunk in buffer.chunks_exact(8) {
                                let value = i64::from_le_bytes(chunk.try_into().unwrap());
                                values_array.push(&JsValue::from(value as f64));
                            }
                        }
                        PhysicalType::Float => {
                            for chunk in buffer.chunks_exact(4) {
                                let value = f32::from_le_bytes(chunk.try_into().unwrap());
                                values_array.push(&JsValue::from(value));
                            }
                        }
                        PhysicalType::Double => {
                            for chunk in buffer.chunks_exact(8) {
                                let value = f64::from_le_bytes(chunk.try_into().unwrap());
                                values_array.push(&JsValue::from(value));
                            }
                        }
                        PhysicalType::Boolean => {
                            // Parquet uses bit-packed booleans
                            let num_values = data_page.num_values();
                            for i in 0..num_values {
                                let byte_idx = i / 8;
                                let bit_idx = i % 8;
                                if byte_idx < buffer.len() {
                                    let value = (buffer[byte_idx] >> bit_idx) & 1 == 1;
                                    values_array.push(&JsValue::from(value));
                                }
                            }
                        }
                        PhysicalType::ByteArray => {
                            let mut offset = 0;
                            while offset + 4 <= buffer.len() {
                                let len = u32::from_le_bytes(
                                    buffer[offset..offset + 4].try_into().unwrap(),
                                ) as usize;
                                offset += 4;
                                if offset + len <= buffer.len() {
                                    let s = String::from_utf8_lossy(&buffer[offset..offset + len]);
                                    values_array.push(&JsValue::from_str(&s));
                                    offset += len;
                                } else {
                                    break;
                                }
                            }
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
