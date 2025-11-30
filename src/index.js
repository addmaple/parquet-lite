/**
 * parquet-lite - Lightweight Parquet reader/writer using WebAssembly
 * 
 * This module provides both reader and writer in a single WASM bundle (~568KB combined).
 * For smaller bundle sizes, import from:
 * - 'parquet-lite/writer' (267KB) - write only
 * - 'parquet-lite/reader' (358KB) - read with delta encoding + nested types
 * - 'parquet-lite/reader-lite' (214KB) - read basic parquet files only
 */

let wasmModule = null;
let initPromise = null;

// WASM file path relative to this module
const WASM_PATH = new URL('./wasm-full/parquet_lite_full_bg.wasm', import.meta.url);

/**
 * Initialize the combined WASM module.
 * Called automatically on first use.
 */
async function init(source) {
  if (wasmModule) return;
  if (initPromise && !source) return initPromise;
  
  initPromise = (async () => {
    const wasm = await import('./wasm-full/parquet_lite_full.js');
    
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

// Writer exports
export async function initWriter(source) {
  return init(source);
}

export async function writeParquet(schema, data, config = {}) {
  await init();
  return wasmModule.writeParquet(schema, data, config);
}

export async function getWriterVersion() {
  await init();
  return wasmModule.getVersion();
}

// Reader exports
export async function initReader(source) {
  return init(source);
}

export async function readMetadata(data) {
  await init();
  return wasmModule.readMetadata(data);
}

export async function readParquet(data, columns = null) {
  await init();
  return wasmModule.readParquet(data, columns);
}

export async function getReaderVersion() {
  await init();
  return wasmModule.getVersion();
}

export async function getVersion() {
  await init();
  return wasmModule.getVersion();
}
