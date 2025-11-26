# @addmaple/parquet-lite

**Disclaimer:** This project was created entirely with Anthropic Opus 4.5 and Cursor Composer. There is not much logic - we are making use of the efficient rust toolchain and the parquet2 crate.

---

A lightweight JavaScript library for reading and writing Parquet files, powered by Rust compiled to WebAssembly.

## Features

- **Lightweight**: Separate reader (162KB) and writer (233KB) - only load what you need
- **Fast**: Rust/WASM core for high performance
- **Browser & Node.js**: Works in modern browsers and Node.js 18+
- **Bundler-friendly**: Works with Vite, Webpack, Rollup, etc.
- **Pure ESM**: Native ES modules

## Size Comparison

| Library | Package Size | WASM/Code Size | Notes |
|---------|-------------|----------------|-------|
| **@addmaple/parquet-lite** | **167.5 KB** | Reader: 162 KB<br>Writer: 233 KB<br>Total: **395 KB** | Separate reader/writer modules |
| **parquet-wasm** | **5.9 MB** | ~1.2 MB (brotli) | Includes Apache Arrow, all compression codecs |
| **parquetjs** | **38.3 KB** (tarball)<br>**4.6 MB** (with deps) | 219 KB unpacked<br>~4.6 MB installed | Pure JS, no WASM, slower, read & write |
| **hyparquet** | **46.5 KB** | 184.5 KB unpacked | Read-only, pure JS, no deps |

**Note:** `parquetjs` does support Snappy compression (via `snappyjs` dependency). The 38.3 KB is the compressed tarball size, but the actual installed size with all dependencies is **~4.6 MB** (including `brotli` 1.5MB, `thrift` 444KB, `snappyjs` 100KB, and others). The unpacked package size of 219 KB is just the library code without dependencies.

### Performance Comparison

Benchmark results comparing `@addmaple/parquet-lite` vs `parquetjs` (Node.js v22):

| Rows | Operation | @addmaple/parquet-lite | parquetjs | Speedup |
|------|-----------|------------------------|-----------|---------|
| 1,000 | Write | 20.57 ms | 15.61 ms | 0.76x |
| 1,000 | Read | 4.78 ms | 4.74 ms | 0.99x |
| 10,000 | Write | 6.74 ms | 78.78 ms | **11.68x faster** |
| 10,000 | Read | 4.18 ms | 16.02 ms | **3.84x faster** |
| 100,000 | Write | 56.04 ms | 730.66 ms | **13.04x faster** |
| 100,000 | Read | 50.91 ms | 89.09 ms | **1.75x faster** |

**Key findings:**
- **WASM performance scales better** - Significant speedups at larger dataset sizes
- **Smaller file sizes** - Better compression (e.g., 2.1 MB vs 3.15 MB for 100k rows)
- **Lower memory usage** - More efficient memory footprint for reads

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
import { readParquet, readMetadata, initReader, getReaderVersion } from '@addmaple/parquet-lite/reader';

// Initialize (optional, called automatically)
await initReader(wasmSource?);

// Read metadata
const metadata = await readMetadata(bytes);
// { num_rows: number, num_row_groups: number, columns: [...] }

// Read data
const data = await readParquet(bytes, columns?);
// { columnName: [...values], ... }
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
