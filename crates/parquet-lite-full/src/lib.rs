// Full parquet-lite: combined reader and writer in a single WASM module
// This provides both read and write functionality with shared dependencies

mod reader;
mod writer;

pub use reader::*;
pub use writer::*;

use wasm_bindgen::prelude::*;

/// Get the version of this library
#[wasm_bindgen(js_name = getVersion)]
pub fn get_version() -> String {
  env!("CARGO_PKG_VERSION").to_string()
}
