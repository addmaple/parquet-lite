/**
 * parquet-lite/writer - Parquet export functionality
 * 
 * Import this module directly if you only need to write Parquet files.
 */

let wasmModule = null;
let initPromise = null;

// WASM file path relative to this module
const WASM_PATH = new URL('./wasm-writer/parquet_lite_writer_bg.wasm', import.meta.url);

/**
 * Initialize the WASM module.
 * 
 * Called automatically on first use, but can be called manually to:
 * - Pre-load the WASM module
 * - Provide custom WASM bytes (useful for bundlers)
 * 
 * @param {Uint8Array|ArrayBuffer|Response|Promise<Response>} [source] - Optional WASM source
 * @returns {Promise<void>}
 * 
 * @example
 * // Pre-load
 * await initWriter();
 * 
 * // With bundler (e.g., Vite with ?url import)
 * import wasmUrl from 'parquet-lite/dist/wasm-writer/parquet_lite_writer_bg.wasm?url';
 * await initWriter(fetch(wasmUrl));
 * 
 * // With inline WASM bytes
 * await initWriter(wasmBytes);
 */
export async function initWriter(source) {
  if (wasmModule) return;
  if (initPromise && !source) return initPromise;
  
  initPromise = (async () => {
    const wasm = await import('./wasm-writer/parquet_lite_writer.js');
    
    if (source) {
      // User provided WASM source
      await wasm.default(source);
    } else {
      // Check environment - prioritize browser detection for bundler optimization
      const isBrowser = typeof window !== 'undefined' || 
                        typeof self !== 'undefined' ||
                        (typeof import.meta !== 'undefined' && import.meta.env && !import.meta.env.SSR);
      
      if (isBrowser) {
        // Browser: fetch from URL
        await wasm.default(WASM_PATH);
      } else {
        // Node.js: use eval to prevent bundler analysis of dynamic imports
        // This prevents bundlers from warning about fs/url in browser builds
        const loadNodeWasm = new Function(`
          return async function() {
            const fs = await import('fs');
            const url = await import('url');
            const wasmPath = url.fileURLToPath(arguments[0]);
            return fs.readFileSync(wasmPath);
          }
        `)();
        
        try {
          const wasmBytes = await loadNodeWasm(WASM_PATH);
          await wasm.default(wasmBytes);
        } catch (err) {
          // Fallback to URL if fs import fails
          await wasm.default(WASM_PATH);
        }
      }
    }
    
    wasmModule = wasm;
  })();
  
  return initPromise;
}

/**
 * Write data to Parquet format
 * 
 * @param {Array<{name: string, type: string, nullable?: boolean, logicalType?: string, precision?: number, scale?: number, bitWidth?: number, isSigned?: boolean, enumValues?: string[]}>} schema - Column definitions
 *   Supported types: 'int32', 'int64', 'float', 'double', 'boolean', 'string'
 *   Supported logical types: 'date', 'time_millis', 'time_micros', 'timestamp_millis', 'timestamp_micros', 'utf8', 'json', 'bson', 'decimal', 'enum', 'integer', 'uuid'
 *   Logical type parameters:
 *   - decimal: requires precision (number) and scale (number)
 *   - integer: requires bitWidth (8|16|32|64) and isSigned (boolean)
 * @param {Object<string, Array|TypedArray|Date[]|Object[]>} data - Object with column names as keys and arrays as values
 *   TypedArrays supported: Int32Array, BigInt64Array, Float32Array, Float64Array, Uint8Array, Int8Array, Uint16Array, Int16Array, Uint32Array, BigUint64Array
 *   For Integer logical types, matching TypedArrays are optimized (e.g., Uint8Array for integer(8,false), Int8Array for integer(8,true))
 *   Enum logical type: 
 *   - Pass string arrays normally: ['active', 'inactive', 'pending']
 *   - Or use efficient index arrays: Define enumValues in schema, then pass indices [0, 1, 2, 0]
 *   - Index arrays can use TypedArrays (e.g., Uint8Array) for even better performance
 *   Automatic conversions:
 *   - Date objects → date/timestamp values (when logicalType is 'date', 'timestamp_millis', etc.)
 *   - Objects → JSON strings (when logicalType is 'json')
 * @param {Object} [config] - Optional configuration
 * @param {string} [config.compression='snappy'] - Compression: 'snappy' or 'none'
 * @param {number} [config.rowGroupSize=10000] - Rows per row group
 * @param {string} [config.version='v1'] - Parquet version: 'v1' (better compatibility) or 'v2' (better compression)
 * @returns {Promise<Uint8Array>} - Parquet file as bytes
 * 
 * @example
 * const schema = [
 *   { name: 'id', type: 'int32' },
 *   { name: 'name', type: 'string' },
 *   { name: 'score', type: 'double' }
 * ];
 * const data = {
 *   id: [1, 2, 3],
 *   name: ['Alice', 'Bob', 'Charlie'],
 *   score: [95.5, 87.3, 92.1]
 * };
 * const bytes = await writeParquet(schema, data);
 * 
 * // Create a download in browser
 * const blob = new Blob([bytes], { type: 'application/octet-stream' });
 */
export async function writeParquet(schema, data, config = {}) {
  await initWriter();
  return wasmModule.writeParquet(schema, data, config);
}

/**
 * Get the version of the writer WASM module
 * @returns {Promise<string>}
 */
export async function getWriterVersion() {
  await initWriter();
  return wasmModule.getVersion();
}
