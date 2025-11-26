use parquet2::{
    compression::CompressionOptions,
    encoding::Encoding,
    metadata::{Descriptor, SchemaDescriptor},
    page::{DataPage, DataPageHeader, DataPageHeaderV1, Page, CompressedPage},
    schema::types::{ParquetType, PhysicalType, PrimitiveType, FieldInfo},
    schema::Repetition,
    write::{
        DynIter, DynStreamingIterator, FileWriter, Version,
        WriteOptions, Compressor,
    },
};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use wasm_bindgen::prelude::*;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ColumnSchema {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WriteConfig {
    #[serde(default = "default_compression")]
    pub compression: String,
    #[serde(default = "default_row_group_size")]
    pub row_group_size: usize,
}

fn default_compression() -> String {
    "snappy".to_string()
}

fn default_row_group_size() -> usize {
    10000
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            compression: default_compression(),
            row_group_size: default_row_group_size(),
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

fn build_schema_fields(columns: &[ColumnSchema]) -> Vec<ParquetType> {
    columns
        .iter()
        .map(|col| {
            let physical_type = get_physical_type(&col.col_type);
            ParquetType::PrimitiveType(PrimitiveType {
                field_info: FieldInfo {
                    name: col.name.clone(),
                    repetition: if col.nullable {
                        Repetition::Optional
                    } else {
                        Repetition::Required
                    },
                    id: None,
                },
                logical_type: None,
                converted_type: None,
                physical_type,
            })
        })
        .collect()
}

fn create_descriptor(col: &ColumnSchema) -> Descriptor {
    let physical_type = get_physical_type(&col.col_type);
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
        logical_type: None,
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
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn encode_int64_column(values: &[i64]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn encode_float_column(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn encode_double_column(values: &[f64]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn encode_boolean_column(values: &[bool]) -> Vec<u8> {
    // Parquet uses bit-packed booleans
    let mut bytes = vec![0u8; (values.len() + 7) / 8];
    for (i, &v) in values.iter().enumerate() {
        if v {
            bytes[i / 8] |= 1 << (i % 8);
        }
    }
    bytes
}

fn encode_string_column(values: &[String]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for s in values {
        let s_bytes = s.as_bytes();
        bytes.extend_from_slice(&(s_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(s_bytes);
    }
    bytes
}

fn create_data_page(
    encoded_data: Vec<u8>,
    num_values: usize,
    descriptor: Descriptor,
) -> DataPage {
    let header = DataPageHeader::V1(DataPageHeaderV1 {
        num_values: num_values as i32,
        encoding: Encoding::Plain.into(),
        definition_level_encoding: Encoding::Plain.into(),
        repetition_level_encoding: Encoding::Plain.into(),
        statistics: None,
    });

    DataPage::new(
        header,
        encoded_data,
        descriptor,
        Some(num_values),
    )
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
        .map(|c| serde_wasm_bindgen::from_value(c))
        .transpose()?
        .unwrap_or_default();

    let compression = get_compression(&config.compression);
    
    // Create schema descriptor
    let fields = build_schema_fields(&columns);
    let schema_descriptor = SchemaDescriptor::new("schema".to_string(), fields);

    let options = WriteOptions {
        write_statistics: false,
        version: Version::V2,
    };

    let mut buffer = Cursor::new(Vec::new());
    let mut writer = FileWriter::new(&mut buffer, schema_descriptor, options, None);

    // Get column data from JS object
    let data_obj = js_sys::Object::try_from(&data_js)
        .ok_or_else(|| JsError::new("Data must be an object"))?;

    // Determine number of rows from first column
    let first_col = &columns[0];
    let first_data = js_sys::Reflect::get(&data_obj, &JsValue::from_str(&first_col.name))
        .map_err(|_| JsError::new("Failed to get column data"))?;
    let first_arr = js_sys::Array::from(&first_data);
    let total_rows = first_arr.length() as usize;

    // Process in row groups
    let mut row_offset = 0;
    while row_offset < total_rows {
        let rows_in_group = std::cmp::min(config.row_group_size, total_rows - row_offset);
        
        // Build column iterators for this row group
        let mut column_iters: Vec<Result<DynStreamingIterator<'static, CompressedPage, parquet2::error::Error>, parquet2::error::Error>> = Vec::new();
        
        for col in &columns {
            let col_data = js_sys::Reflect::get(&data_obj, &JsValue::from_str(&col.name))
                .map_err(|_| JsError::new(&format!("Failed to get column: {}", col.name)))?;
            let col_arr = js_sys::Array::from(&col_data);
            
            // Slice the data for this row group
            let start = row_offset as u32;
            let end = (row_offset + rows_in_group) as u32;
            let slice = col_arr.slice(start, end);
            let slice_value: JsValue = slice.into();
            
            let descriptor = create_descriptor(col);
            let physical_type = get_physical_type(&col.col_type);
            
            let encoded_data = match physical_type {
                PhysicalType::Int32 => {
                    let values: Vec<i32> = serde_wasm_bindgen::from_value(slice_value)?;
                    encode_int32_column(&values)
                }
                PhysicalType::Int64 => {
                    let values: Vec<i64> = serde_wasm_bindgen::from_value(slice_value)?;
                    encode_int64_column(&values)
                }
                PhysicalType::Float => {
                    let values: Vec<f32> = serde_wasm_bindgen::from_value(slice_value)?;
                    encode_float_column(&values)
                }
                PhysicalType::Double => {
                    let values: Vec<f64> = serde_wasm_bindgen::from_value(slice_value)?;
                    encode_double_column(&values)
                }
                PhysicalType::Boolean => {
                    let values: Vec<bool> = serde_wasm_bindgen::from_value(slice_value)?;
                    encode_boolean_column(&values)
                }
                PhysicalType::ByteArray => {
                    let values: Vec<String> = serde_wasm_bindgen::from_value(slice_value)?;
                    encode_string_column(&values)
                }
                _ => return Err(JsError::new("Unsupported type")),
            };

            let page = create_data_page(encoded_data, rows_in_group, descriptor);
            
            // Compress the page and create an iterator
            let pages = vec![Ok(Page::Data(page))];
            let page_iter = DynIter::new(pages.into_iter());
            let compressor: Compressor<_> = Compressor::new_from_vec(page_iter, compression, vec![]);
            
            column_iters.push(Ok(DynStreamingIterator::new(compressor)));
        }
        
        let row_group = DynIter::new(column_iters.into_iter());
        
        writer.write(row_group)
            .map_err(|e| JsError::new(&format!("Write error: {:?}", e)))?;
        
        row_offset += rows_in_group;
    }

    writer.end(None)
        .map_err(|e| JsError::new(&format!("End error: {:?}", e)))?;

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
