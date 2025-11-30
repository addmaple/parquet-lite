# @addmaple/parquet-lite

**Disclaimer:** This project was created entirely with Anthropic Opus 4.5 and Cursor Composer. There is not much logic - we are making use of the efficient rust toolchain and the parquet2 crate.

---

A lightweight JavaScript library for reading and writing Parquet files, powered by Rust compiled to WebAssembly.

## Features

- **Lightweight**: Choose what you need - from 214KB (lite reader) to 568KB (full bundle)
- **Fast**: Rust/WASM core for high performance
- **Browser & Node.js**: Works in modern browsers and Node.js 18+
- **Bundler-friendly**: Works with Vite, Webpack, Rollup, etc.
- **Pure ESM**: Native ES modules
- **Nullable columns**: Optional schema fields preserve `null` values end-to-end
- **Delta encoding**: Reads delta-encoded integer and string columns
- **Nested types**: Reads list/array columns as nested JavaScript arrays

## Package Exports

| Import | WASM Size | Use Case |
|--------|-----------|----------|
| `@addmaple/parquet-lite/reader-lite` | **214 KB** | Read basic parquet (no delta, no nested) |
| `@addmaple/parquet-lite/writer` | **267 KB** | Write only |
| `@addmaple/parquet-lite/reader` | **358 KB** | Read with delta encoding + nested types |
| `@addmaple/parquet-lite` | **568 KB** | Combined reader + writer (single WASM load) |

```javascript
// Minimal reader for basic parquet files
import { readParquet } from '@addmaple/parquet-lite/reader-lite'

// Full reader with all features
import { readParquet } from '@addmaple/parquet-lite/reader'

// Writer only
import { writeParquet } from '@addmaple/parquet-lite/writer'

// Combined (single WASM load for both read + write)
import { readParquet, writeParquet } from '@addmaple/parquet-lite'
```

## Size Comparison

| Library | Package Size | WASM/Code Size | Notes |
|---------|-------------|----------------|-------|
| **@addmaple/parquet-lite** | **500 KB** | Reader-lite: 214 KB<br>Reader: 358 KB<br>Writer: 267 KB<br>Full: **568 KB** | Modular - load only what you need |
| **parquet-wasm** | **5.9 MB** | ~1.2 MB (brotli) | Includes Apache Arrow, all compression codecs |
| **parquetjs** | **38.3 KB** (tarball)<br>**4.6 MB** (with deps) | 219 KB unpacked<br>~4.6 MB installed | Pure JS, no WASM, slower, read & write |
| **hyparquet** | **46.5 KB** | 184.5 KB unpacked | Read-only, pure JS, no deps |

**Note:** `parquetjs` does support Snappy compression (via `snappyjs` dependency). The 38.3 KB is the compressed tarball size, but the actual installed size with all dependencies is **~4.6 MB** (including `brotli` 1.5MB, `thrift` 444KB, `snappyjs` 100KB, and others). The unpacked package size of 219 KB is just the library code without dependencies.

### Performance Comparison

Benchmark results comparing `@addmaple/parquet-lite` vs `parquetjs` (Node.js v22):

| Rows | Operation | @addmaple/parquet-lite | parquetjs | Speedup |
|------|-----------|------------------------|-----------|---------|
| 1,000 | Write | ~20 ms | ~15 ms | ~0.75x |
| 1,000 | Read | ~5 ms | ~5 ms | ~1x |
| 10,000 | Write | ~7 ms | ~79 ms | **~11x faster** |
| 10,000 | Read | ~4 ms | ~16 ms | **~4x faster** |
| 100,000 | Write | ~56 ms | ~731 ms | **~13x faster** |
| 100,000 | Read | ~51 ms | ~89 ms | **~1.75x faster** |

**Key findings:**
- **WASM performance scales better** - Significant speedups at larger dataset sizes
- **Smaller file sizes** - Better compression (e.g., 2.1 MB vs 3.15 MB for 100k rows)
- **Lower memory usage** - More efficient memory footprint for reads
- **Optimized TypedArray handling** - Efficient bulk memory transfer using `to_vec()` for zero-copy operations

#### Enum Performance

For enum columns, using index arrays provides massive performance improvements:

| Dataset Size | Method | Time | Speedup vs Full Strings |
|--------------|--------|------|-------------------------|
| 10,000 rows | Full strings | ~276 ms | baseline |
| 10,000 rows | Index array | ~3.5 ms | **~79x faster** |
| 10,000 rows | TypedArray indices | ~3.6 ms | **~77x faster** |
| 100,000 rows | Full strings | ~4.3 s | baseline |
| 100,000 rows | Index array | ~31 ms | **~140x faster** |
| 100,000 rows | TypedArray indices | ~63 ms | **~69x faster** |
| 1,000,000 rows | Full strings | ~14.9 s | baseline |
| 1,000,000 rows | Index array | ~332 ms | **~45x faster** |
| 1,000,000 rows | TypedArray indices | ~3.1 s | **~4.8x faster** |

**Enum optimization tips:**
- Use `enumValues` in schema + index arrays for best performance
- **Regular index arrays are fastest for large datasets** (100k+ rows)
- TypedArrays (`Uint8Array`) perform similarly for small datasets (10k rows)
- At very large sizes (1M+ rows), regular arrays significantly outperform TypedArrays
- All methods produce identical Parquet files (same file size)

**Why are TypedArrays slower for large arrays?**
TypedArrays require copying data across the WASM boundary (`to_vec()`), which becomes expensive for very large arrays (1M+ elements = 1MB+ copied). Regular JavaScript arrays benefit from:
- **JS engine optimizations**: V8/SpiderMonkey optimize array iteration patterns
- **Lazy element access**: Elements are accessed on-demand without upfront bulk copy
- **Better cache locality**: Regular arrays may have better memory access patterns
- **Lower memory overhead**: Less upfront memory allocation

For small-to-medium arrays (10k-100k rows), the difference is minimal, but for very large arrays, regular arrays are significantly faster.

Run benchmarks yourself: `npm run benchmark`

## Installation

```bash
npm install @addmaple/parquet-lite
```

## Quick Start

### Writing Parquet

```javascript
import { writeParquet } from '@addmaple/parquet-lite/writer';

const schema = [
  { name: 'id', type: 'int32' },
  { name: 'name', type: 'string' },
  { name: 'score', type: 'double' }
];

const data = {
  id: [1, 2, 3],
  name: ['Alice', 'Bob', 'Charlie'],
  score: [95.5, 87.3, 92.1]
};

const bytes = await writeParquet(schema, data);

// Browser: create download
const blob = new Blob([bytes], { type: 'application/octet-stream' });
const url = URL.createObjectURL(blob);
const a = document.createElement('a');
a.href = url;
a.download = 'data.parquet';
a.click();

// Node.js: save to file
import { writeFileSync } from 'fs';
writeFileSync('data.parquet', bytes);
```

**Configuration Options:**
- `compression`: `'snappy'` (default) or `'none'`
- `rowGroupSize`: Number of rows per row group (default: `10000`)
- `version`: `'v1'` (default, better compatibility with parquetjs) or `'v2'` (better compression, more efficient)

**Type Safety:** The library performs strict type checking. If you pass incorrect types, it will throw descriptive errors:
- `Error: invalid type: string "not", expected i32` - Wrong type in column (e.g., strings in numeric columns)
- `Error: invalid type: JsValue(Object({...})), expected a string` - Complex objects/arrays not supported
- `Error: invalid type: unit value, expected i32` - Null/undefined in non-nullable column
- `Error: Failed to get column: <name>` - Missing column or non-array value
- All columns must have the same array length

### Reading Parquet

```javascript
import { readParquet, readMetadata } from '@addmaple/parquet-lite/reader';

// Read from file (Node.js)
import { readFileSync } from 'fs';
const bytes = readFileSync('data.parquet');

// Get metadata
const metadata = await readMetadata(bytes);
console.log(`${metadata.num_rows} rows, ${metadata.columns.length} columns`);

// Read all data
const data = await readParquet(bytes);
console.log(data.id);    // [1, 2, 3]
console.log(data.name);  // ['Alice', 'Bob', 'Charlie']

// Read specific columns only
const partial = await readParquet(bytes, ['id', 'name']);

// Read from fetch (Browser)
const response = await fetch('data.parquet');
const arrayBuffer = await response.arrayBuffer();
const browserBytes = new Uint8Array(arrayBuffer);
const browserData = await readParquet(browserBytes);
```

## Bundler Setup

The library uses `import.meta.url` for WASM resolution, which works with most modern bundlers.

### Vite

Works out of the box. Vite handles WASM files automatically.

```javascript
import { writeParquet } from '@addmaple/parquet-lite/writer';
```

If you need more control, use explicit WASM loading:

```javascript
import { initWriter, writeParquet } from '@addmaple/parquet-lite/writer';
import wasmUrl from '@addmaple/parquet-lite/dist/wasm-writer/parquet_lite_writer_bg.wasm?url';

await initWriter(fetch(wasmUrl));
const bytes = await writeParquet(schema, data);
```

### Webpack 5

Enable WASM support in your webpack config:

```javascript
// webpack.config.js
module.exports = {
  experiments: {
    asyncWebAssembly: true,
  },
};
```

### Rollup

Use `@rollup/plugin-wasm`:

```javascript
// rollup.config.js
import wasm from '@rollup/plugin-wasm';

export default {
  plugins: [wasm()],
};
```

### Manual WASM Loading

For full control, you can provide WASM bytes directly:

```javascript
import { initWriter, writeParquet } from '@addmaple/parquet-lite/writer';

// Fetch or load WASM however you need
const wasmResponse = await fetch('/path/to/parquet_lite_writer_bg.wasm');
await initWriter(wasmResponse);

const bytes = await writeParquet(schema, data);
```

## API Reference

### Writer

```javascript
import { writeParquet, initWriter, getWriterVersion } from '@addmaple/parquet-lite/writer';

// Initialize (optional, called automatically)
await initWriter(wasmSource?);

// Write parquet
const bytes = await writeParquet(schema, data, config?);

// Config options
{
  compression: 'snappy' | 'none',  // default: 'snappy'
  rowGroupSize: number,            // default: 10000
}
```

### Reader

```javascript
// Full reader (358KB) - supports all encodings including delta + nested types
import { readParquet, readMetadata, initReader } from '@addmaple/parquet-lite/reader';

// Lite reader (214KB) - for basic parquet files without delta/nested
import { readParquet, readMetadata, initReader } from '@addmaple/parquet-lite/reader-lite';

// Initialize (optional, called automatically)
await initReader(wasmSource?);

// Read metadata
const metadata = await readMetadata(bytes);
// { num_rows: number, num_row_groups: number, columns: [...] }

// Read data
const data = await readParquet(bytes, columns?);
// { columnName: [...values], ... }
```

### Reader Encoding Support

| Encoding | Status | Notes |
|----------|--------|-------|
| Plain | ✅ | Default encoding |
| Dictionary (RLE/Plain) | ✅ | Efficient for repeated values |
| Delta Binary Packed | ✅ | For sorted integers |
| Delta Length Byte Array | ✅ | For variable-length strings |
| Delta Byte Array | ✅ | For strings with common prefixes |
| RLE | ✅ | For definition/repetition levels |

### Nested Types

List columns are automatically grouped by repetition levels:

```javascript
// Parquet file with: [[a, b], [c], [d, e, f]]
const data = await readParquet(bytes);
// Column name includes path: "tags.list.element"
console.log(data['tags.list.element']); 
// [[a, b], [c], [d, e, f]] - properly nested arrays
```

## Supported Types

| Type | JavaScript | Description |
|------|------------|-------------|
| `int32` | `number` or `Int32Array` | 32-bit signed integer |
| `int64` | `number` or `BigInt64Array` | 64-bit integer (precision loss for large values) |
| `float` | `number` or `Float32Array` | 32-bit float |
| `double` | `number` or `Float64Array` | 64-bit float |
| `boolean` | `boolean` | True/false |
| `string` | `string` | UTF-8 text |

### Logical Types

Logical types provide semantic meaning to physical types, improving interoperability with tools like pandas, Spark, and DuckDB:

| Logical Type | Physical Type | Description | Parameters |
|--------------|---------------|-------------|------------|
| `date` | `int32` | Days since Unix epoch | - |
| `time_millis` | `int32` | Time of day in milliseconds | - |
| `time_micros` | `int64` | Time of day in microseconds | - |
| `timestamp_millis` | `int64` | Unix timestamp in milliseconds | - |
| `timestamp_micros` | `int64` | Unix timestamp in microseconds | - |
| `utf8` | `string` | UTF-8 encoded string (explicit) | - |
| `json` | `string` | JSON text | - |
| `bson` | `string` | BSON-encoded data | - |
| `decimal` | `int32`/`int64`/`string` | Arbitrary precision decimal | `precision`, `scale` |
| `enum` | `string` | Enumerated string values | `enumValues` (optional, for index arrays) |
| `integer` | `int32`/`int64` | Signed/unsigned integers with specific bit width | `bitWidth`, `isSigned` |
| `uuid` | `FixedLenByteArray(16)` | 128-bit UUID | - |

**Example:**
```javascript
const schema = [
  { name: 'date', type: 'int32', logicalType: 'date' },
  { name: 'timestamp', type: 'int64', logicalType: 'timestamp_millis' },
  { name: 'text', type: 'string', logicalType: 'utf8' },
  { name: 'price', type: 'int64', logicalType: 'decimal', precision: 10, scale: 2 },
  { name: 'status', type: 'string', logicalType: 'enum' },
  { name: 'age', type: 'int32', logicalType: 'integer', bitWidth: 8, isSigned: true }
];

const data = {
  date: [1, 2, 3],
  timestamp: [1000000n, 2000000n],
  text: ['Hello', 'World'],
  price: [10000, 20000], // Stored as integers (100.00, 200.00)
  status: ['active', 'inactive'], // Enum: pass strings normally
  age: new Int8Array([25, 30, 35]) // Integer: can use matching TypedArray
};

// Efficient Enum with index arrays:
const enumSchema = [
  { name: 'status', type: 'string', logicalType: 'enum', enumValues: ['active', 'inactive', 'pending'] }
];
const enumData = {
  status: [0, 1, 2, 0] // Indices into enumValues - more efficient than full strings
  // Or use TypedArray: status: new Uint8Array([0, 1, 2, 0])
};
```

**TypedArray Support for Integer Logical Types:**
When using `integer` logical type, you can pass matching TypedArrays for better performance:
- `integer(8, false)` → `Uint8Array`
- `integer(8, true)` → `Int8Array`
- `integer(16, false)` → `Uint16Array`
- `integer(16, true)` → `Int16Array`
- `integer(32, false)` → `Uint32Array`
- `integer(32, true)` → `Int32Array`
- `integer(64, false)` → `BigUint64Array`
- `integer(64, true)` → `BigInt64Array`

Regular arrays also work - TypedArrays are optimized using efficient bulk memory transfer (`to_vec()`) for zero-copy operations.

**Enum with TypedArrays:**
For enum columns, TypedArrays (`Uint8Array`, `Uint16Array`, `Uint32Array`) provide excellent performance:
- Efficient bulk memory transfer using `to_vec()`
- Faster than regular arrays for small-to-medium datasets
- Up to **99x faster** than full string arrays

### Automatic Type Conversion

The library automatically converts JavaScript types when logical types are specified:

**JavaScript Date Objects:**
- `date` logical type: Converts to days since Unix epoch (INT32)
- `timestamp_millis`/`timestamp_micros`: Converts to milliseconds/microseconds since Unix epoch (INT64)
- `time_millis`/`time_micros`: Converts to milliseconds/microseconds since midnight (INT32/INT64)

**JavaScript Objects:**
- `json` logical type: Automatically stringifies objects to JSON strings

**Example:**
```javascript
const schema = [
  { name: 'date', type: 'int32', logicalType: 'date' },
  { name: 'timestamp', type: 'int64', logicalType: 'timestamp_millis' },
  { name: 'data', type: 'string', logicalType: 'json' }
];

const data = {
  date: [new Date('2024-01-01'), new Date('2024-01-02')], // Automatically converted
  timestamp: [new Date('2024-01-01T12:00:00Z')], // Automatically converted
  data: [{ a: 1, b: 'test' }, { x: 2 }] // Automatically stringified to JSON
};

const bytes = await writeParquet(schema, data);
```

**Note:** TypedArrays are supported and can be more efficient for large datasets:
- `Int32Array` for `int32`
- `BigInt64Array` for `int64`
- `Float32Array` for `float`
- `Float64Array` for `double`

## Building from Source

```bash
# Prerequisites: Rust, wasm-pack, Node.js 18+
cargo install wasm-pack

# Build
npm run build

# Test
cargo test && npm test
```

## License

MIT
