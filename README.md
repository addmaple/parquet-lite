# parquet-lite

A lightweight JavaScript library for reading and writing Parquet files, powered by Rust compiled to WebAssembly.

## Features

- **Lightweight**: Separate reader (162KB) and writer (233KB) - only load what you need
- **Fast**: Rust/WASM core for high performance
- **Browser & Node.js**: Works in modern browsers and Node.js 18+
- **Bundler-friendly**: Works with Vite, Webpack, Rollup, etc.
- **Pure ESM**: Native ES modules

## Installation

```bash
npm install parquet-lite
```

## Quick Start

### Writing Parquet

```javascript
import { writeParquet } from 'parquet-lite/writer';

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
import { readParquet, readMetadata } from 'parquet-lite/reader';

// Get metadata
const metadata = await readMetadata(bytes);
console.log(`${metadata.num_rows} rows, ${metadata.columns.length} columns`);

// Read all data
const data = await readParquet(bytes);
console.log(data.id);    // [1, 2, 3]
console.log(data.name);  // ['Alice', 'Bob', 'Charlie']

// Read specific columns only
const partial = await readParquet(bytes, ['id', 'name']);
```

## Bundler Setup

The library uses `import.meta.url` for WASM resolution, which works with most modern bundlers.

### Vite

Works out of the box. Vite handles WASM files automatically.

```javascript
import { writeParquet } from 'parquet-lite/writer';
```

If you need more control, use explicit WASM loading:

```javascript
import { initWriter, writeParquet } from 'parquet-lite/writer';
import wasmUrl from 'parquet-lite/dist/wasm-writer/parquet_lite_writer_bg.wasm?url';

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
import { initWriter, writeParquet } from 'parquet-lite/writer';

// Fetch or load WASM however you need
const wasmResponse = await fetch('/path/to/parquet_lite_writer_bg.wasm');
await initWriter(wasmResponse);

const bytes = await writeParquet(schema, data);
```

## API Reference

### Writer

```javascript
import { writeParquet, initWriter, getWriterVersion } from 'parquet-lite/writer';

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
import { readParquet, readMetadata, initReader, getReaderVersion } from 'parquet-lite/reader';

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
| `int32` | `number` | 32-bit signed integer |
| `int64` | `number` | 64-bit integer (precision loss for large values) |
| `float` | `number` | 32-bit float |
| `double` | `number` | 64-bit float |
| `boolean` | `boolean` | True/false |
| `string` | `string` | UTF-8 text |

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
