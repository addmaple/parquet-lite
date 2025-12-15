/* tslint:disable */
/* eslint-disable */
/**
 * Read metadata from a Parquet file
 *
 * # Arguments
 * * `data` - Uint8Array containing the Parquet file data
 */
export function readMetadata(data: Uint8Array): any;
/**
 * Read a Parquet file and return data as a JavaScript object
 *
 * # Arguments
 * * `data` - Uint8Array containing the Parquet file data
 * * `columns` - Optional array of column names to read (reads all if not specified)
 */
export function readParquet(data: Uint8Array, columns?: string[] | null): any;
/**
 * Get the version of this library
 */
export function getVersion(): string;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly readMetadata: (a: number, b: number) => [number, number, number];
  readonly readParquet: (a: number, b: number, c: number, d: number) => [number, number, number];
  readonly getVersion: () => [number, number];
  readonly __wbindgen_malloc: (a: number, b: number) => number;
  readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
  readonly __wbindgen_exn_store: (a: number) => void;
  readonly __externref_table_alloc: () => number;
  readonly __wbindgen_externrefs: WebAssembly.Table;
  readonly __externref_table_dealloc: (a: number) => void;
  readonly __wbindgen_free: (a: number, b: number, c: number) => void;
  readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;
/**
* Instantiates the given `module`, which can either be bytes or
* a precompiled `WebAssembly.Module`.
*
* @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
*
* @returns {InitOutput}
*/
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
* If `module_or_path` is {RequestInfo} or {URL}, makes a request and
* for everything else, calls `WebAssembly.instantiate` directly.
*
* @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
*
* @returns {Promise<InitOutput>}
*/
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
