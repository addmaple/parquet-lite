/**
 * parquet-lite - Lightweight Parquet reader/writer using WebAssembly
 * 
 * This module re-exports both reader and writer functionality.
 * For smaller bundle sizes, import from 'parquet-lite/writer' or 'parquet-lite/reader' directly.
 */

export { writeParquet, initWriter, getWriterVersion } from './writer.js';
export { readParquet, readMetadata, initReader, getReaderVersion } from './reader.js';

