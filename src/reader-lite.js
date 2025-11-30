/**
 * parquet-lite/reader-lite - Lightweight Parquet reader (no delta encoding, no nested types)
 * 
 * Import this module if you only need to read simple Parquet files and want minimal bundle size.
 * For files with delta encoding or nested types, use the full reader instead.
 */

let wasmModule = null;
let initPromise = null;

// WASM file path relative to this module
const WASM_PATH = new URL('./wasm-reader-lite/parquet_lite_reader_lite_bg.wasm', import.meta.url);

/**
 * Initialize the WASM module.
 * 
 * Called automatically on first use, but can be called manually to:
 * - Pre-load the WASM module
 * - Provide custom WASM bytes (useful for bundlers)
 * 
 * @param {Uint8Array|ArrayBuffer|Response|Promise<Response>} [source] - Optional WASM source
 * @returns {Promise<void>}
 */
export async function initReader(source) {
  if (wasmModule) return;
  if (initPromise && !source) return initPromise;
  
  initPromise = (async () => {
    const wasm = await import('./wasm-reader-lite/parquet_lite_reader_lite.js');
    
    if (source) {
      await wasm.default(source);
    } else {
      const isBrowser = typeof window !== 'undefined' || 
                        typeof self !== 'undefined' ||
                        (typeof import.meta !== 'undefined' && import.meta.env && !import.meta.env.SSR);
      
      if (isBrowser) {
        await wasm.default(WASM_PATH);
      } else {
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
          await wasm.default(WASM_PATH);
        }
      }
    }
    
    wasmModule = wasm;
  })();
  
  return initPromise;
}

/**
 * Read metadata from a Parquet file
 * 
 * @param {Uint8Array} data - Parquet file bytes
 * @returns {Promise<{numRows: number, numRowGroups: number, columns: Array<{name: string, type: string, nullable: boolean}>}>}
 */
export async function readMetadata(data) {
  await initReader();
  return wasmModule.readMetadata(data);
}

/**
 * Read a Parquet file and return data as a JavaScript object
 * 
 * Note: This lite version does not support delta encoding or nested types.
 * Use the full reader for those features.
 * 
 * @param {Uint8Array} data - Parquet file bytes
 * @param {string[]} [columns] - Optional array of column names to read
 * @returns {Promise<Object<string, Array>>}
 */
export async function readParquet(data, columns = null) {
  await initReader();
  return wasmModule.readParquet(data, columns);
}

/**
 * Get the version of the reader-lite WASM module
 * @returns {Promise<string>}
 */
export async function getReaderVersion() {
  await initReader();
  return wasmModule.getVersion();
}

