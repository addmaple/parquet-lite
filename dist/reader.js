/**
 * parquet-lite/reader - Parquet reading functionality
 *
 * Import this module directly if you only need to read Parquet files.
 */

let wasmModule = null
let initPromise = null

// WASM file path relative to this module
const WASM_PATH = new URL('./wasm-reader/parquet_lite_reader_bg.wasm', import.meta.url)

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
 * await initReader();
 *
 * // With bundler (e.g., Vite with ?url import)
 * import wasmUrl from 'parquet-lite/dist/wasm-reader/parquet_lite_reader_bg.wasm?url';
 * await initReader(fetch(wasmUrl));
 *
 * // With inline WASM bytes
 * await initReader(wasmBytes);
 */
export async function initReader(source) {
  if (wasmModule) return
  if (initPromise && !source) return initPromise

  initPromise = (async () => {
    const wasm = await import('./wasm-reader/parquet_lite_reader.js')

    if (source) {
      // User provided WASM source
      await wasm.default(source)
    } else {
      // Check environment - prioritize browser detection for bundler optimization
      const isBrowser =
        typeof window !== 'undefined' ||
        typeof self !== 'undefined' ||
        (typeof import.meta !== 'undefined' && import.meta.env && !import.meta.env.SSR)

      if (isBrowser) {
        // Browser: fetch from URL
        await wasm.default(WASM_PATH)
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
        `)()

        try {
          const wasmBytes = await loadNodeWasm(WASM_PATH)
          await wasm.default(wasmBytes)
        } catch (err) {
          // Fallback to URL if fs import fails
          await wasm.default(WASM_PATH)
        }
      }
    }

    wasmModule = wasm
  })()

  return initPromise
}

/**
 * Read metadata from a Parquet file
 *
 * @param {Uint8Array} data - Parquet file bytes
 * @returns {Promise<{numRows: number, numRowGroups: number, columns: Array<{name: string, type: string, nullable: boolean}>}>}
 *
 * @example
 * const metadata = await readMetadata(parquetBytes);
 * console.log(`${metadata.numRows} rows, ${metadata.columns.length} columns`);
 */
export async function readMetadata(data) {
  await initReader()
  return wasmModule.readMetadata(data)
}

/**
 * Read a Parquet file and return data as a JavaScript object
 *
 * @param {Uint8Array} data - Parquet file bytes
 * @param {string[]} [columns] - Optional array of column names to read (reads all if not specified)
 * @returns {Promise<Object<string, Array>>} - Object with column names as keys and arrays as values
 *
 * @example
 * const data = await readParquet(parquetBytes);
 * console.log(data.id);    // [1, 2, 3]
 * console.log(data.name);  // ['Alice', 'Bob', 'Charlie']
 *
 * // Read specific columns only
 * const partial = await readParquet(parquetBytes, ['id', 'name']);
 */
export async function readParquet(data, columns = null) {
  await initReader()
  return wasmModule.readParquet(data, columns)
}

/**
 * Get the version of the reader WASM module
 * @returns {Promise<string>}
 */
export async function getReaderVersion() {
  await initReader()
  return wasmModule.getVersion()
}
