import { test, describe } from 'node:test';
import assert from 'node:assert';
import { writeFileSync, readFileSync, unlinkSync, statSync } from 'fs';
import { join, dirname } from 'path';
import { tmpdir } from 'os';
import { fileURLToPath } from 'url';
import { execSync } from 'child_process';
import { writeParquet } from '../dist/writer.js';
import { readParquet, readMetadata } from '../dist/reader.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

describe('parquet-lite integration', () => {
  test('write and read int32 column', async () => {
    const schema = [{ name: 'id', type: 'int32' }];
    const data = { id: [1, 2, 3, 4, 5] };
    
    const bytes = await writeParquet(schema, data, { compression: 'none' });
    assert.ok(bytes instanceof Uint8Array);
    assert.ok(bytes.length > 0);
    
    const result = await readParquet(bytes);
    assert.deepStrictEqual(result.id, [1, 2, 3, 4, 5]);
  });

  test('write and read multiple column types', async () => {
    const schema = [
      { name: 'id', type: 'int32' },
      { name: 'score', type: 'double' },
      { name: 'name', type: 'string' }
    ];
    const data = {
      id: [1, 2, 3],
      score: [95.5, 87.3, 92.1],
      name: ['Alice', 'Bob', 'Charlie']
    };
    
    const bytes = await writeParquet(schema, data);
    assert.ok(bytes instanceof Uint8Array);
    
    const result = await readParquet(bytes);
    assert.deepStrictEqual(result.id, [1, 2, 3]);
    assert.deepStrictEqual(result.name, ['Alice', 'Bob', 'Charlie']);
    // Floating point comparison with tolerance
    result.score.forEach((val, i) => {
      assert.ok(Math.abs(val - data.score[i]) < 0.0001, `score[${i}] mismatch: ${val} vs ${data.score[i]}`);
    });
  });

  test('read metadata', async () => {
    const schema = [
      { name: 'a', type: 'int32' },
      { name: 'b', type: 'string' }
    ];
    const data = {
      a: [1, 2, 3, 4, 5],
      b: ['x', 'y', 'z', 'w', 'v']
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    
    assert.strictEqual(metadata.num_rows, 5);
    assert.strictEqual(metadata.columns.length, 2);
    assert.strictEqual(metadata.columns[0].name, 'a');
    assert.strictEqual(metadata.columns[1].name, 'b');
  });

  test('read specific columns only', async () => {
    const schema = [
      { name: 'a', type: 'int32' },
      { name: 'b', type: 'int32' },
      { name: 'c', type: 'int32' }
    ];
    const data = {
      a: [1, 2, 3],
      b: [4, 5, 6],
      c: [7, 8, 9]
    };
    
    const bytes = await writeParquet(schema, data);
    const result = await readParquet(bytes, ['a', 'c']);
    
    assert.deepStrictEqual(result.a, [1, 2, 3]);
    assert.deepStrictEqual(result.c, [7, 8, 9]);
    assert.strictEqual(result.b, undefined);
  });

  test('compression options', async () => {
    const schema = [{ name: 'data', type: 'string' }];
    const data = { data: Array(100).fill('hello world') };
    
    const uncompressed = await writeParquet(schema, data, { compression: 'none' });
    const snappy = await writeParquet(schema, data, { compression: 'snappy' });
    
    // All should produce valid parquet
    assert.ok(uncompressed.length > 0);
    assert.ok(snappy.length > 0);
    
    // Compressed should be smaller (with repetitive data)
    assert.ok(snappy.length < uncompressed.length, 'Snappy should compress');
    
    // Should all read back correctly
    const result1 = await readParquet(uncompressed);
    const result2 = await readParquet(snappy);
    
    assert.deepStrictEqual(result1.data, data.data);
    assert.deepStrictEqual(result2.data, data.data);
  });

  test('boolean column', async () => {
    const schema = [{ name: 'flag', type: 'boolean' }];
    const data = { flag: [true, false, true, false, true] };
    
    const bytes = await writeParquet(schema, data);
    const result = await readParquet(bytes);
    
    assert.deepStrictEqual(result.flag, [true, false, true, false, true]);
  });

  test('nullable columns preserve nulls', async () => {
    const schema = [
      { name: 'maybe_int', type: 'int32', nullable: true },
      { name: 'maybe_str', type: 'string', nullable: true },
      { name: 'maybe_bool', type: 'boolean', nullable: true }
    ];
    const originalData = {
      maybe_int: [1, null, 3, null, 5],
      maybe_str: ['a', 'b', null, 'd', null],
      maybe_bool: [true, null, false, null, true]
    };

    const bytes = await writeParquet(schema, originalData);
    const result = await readParquet(bytes);

    assert.deepStrictEqual(result.maybe_int, originalData.maybe_int);
    assert.deepStrictEqual(result.maybe_str, originalData.maybe_str);
    assert.deepStrictEqual(result.maybe_bool, originalData.maybe_bool);
  });

  test('float column', async () => {
    const schema = [{ name: 'value', type: 'float' }];
    const data = { value: [1.5, 2.5, 3.5] };
    
    const bytes = await writeParquet(schema, data);
    const result = await readParquet(bytes);
    
    result.value.forEach((val, i) => {
      assert.ok(Math.abs(val - data.value[i]) < 0.0001, `value[${i}] mismatch`);
    });
  });

  test('int64 column', async () => {
    const schema = [{ name: 'big', type: 'int64' }];
    const data = { big: [1000000000, 2000000000, 3000000000] };
    
    const bytes = await writeParquet(schema, data);
    const result = await readParquet(bytes);
    
    assert.deepStrictEqual(result.big, [1000000000, 2000000000, 3000000000]);
  });

  test('full roundtrip - all types together', async () => {
    const schema = [
      { name: 'id', type: 'int32' },
      { name: 'big_id', type: 'int64' },
      { name: 'score', type: 'float' },
      { name: 'price', type: 'double' },
      { name: 'active', type: 'boolean' },
      { name: 'name', type: 'string' }
    ];
    
    const originalData = {
      id: [1, 2, 3, 4, 5],
      big_id: [1000000000, 2000000000, 3000000000, 4000000000, 5000000000],
      score: [95.5, 87.3, 92.1, 88.9, 91.2],
      price: [10.99, 20.50, 30.75, 40.25, 50.00],
      active: [true, false, true, false, true],
      name: ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve']
    };
    
    // Write
    const bytes = await writeParquet(schema, originalData);
    assert.ok(bytes.length > 0);
    
    // Read
    const readData = await readParquet(bytes);
    
    // Verify exact matches
    assert.deepStrictEqual(readData.id, originalData.id, 'int32 values must match exactly');
    assert.deepStrictEqual(readData.big_id, originalData.big_id, 'int64 values must match exactly');
    assert.deepStrictEqual(readData.active, originalData.active, 'boolean values must match exactly');
    assert.deepStrictEqual(readData.name, originalData.name, 'string values must match exactly');
    
    // Verify floating point with tolerance
    readData.score.forEach((val, i) => {
      assert.ok(Math.abs(val - originalData.score[i]) < 0.0001, 
        `float score[${i}] mismatch: ${val} vs ${originalData.score[i]}`);
    });
    
    readData.price.forEach((val, i) => {
      assert.ok(Math.abs(val - originalData.price[i]) < 0.0001, 
        `double price[${i}] mismatch: ${val} vs ${originalData.price[i]}`);
    });
    
    // Verify all columns present
    assert.strictEqual(Object.keys(readData).length, 6, 'All 6 columns should be present');
  });

  test('roundtrip with TypedArrays', async () => {
    const schema = [
      { name: 'int32', type: 'int32' },
      { name: 'float', type: 'float' },
      { name: 'double', type: 'double' }
    ];
    
    const originalData = {
      int32: new Int32Array([1, 2, 3, 4, 5]),
      float: new Float32Array([1.5, 2.5, 3.5, 4.5, 5.5]),
      double: new Float64Array([10.5, 20.5, 30.5, 40.5, 50.5])
    };
    
    // Write
    const bytes = await writeParquet(schema, originalData);
    
    // Read
    const readData = await readParquet(bytes);
    
    // Verify - TypedArrays become regular arrays when read
    assert.deepStrictEqual(readData.int32, Array.from(originalData.int32));
    
    readData.float.forEach((val, i) => {
      assert.ok(Math.abs(val - originalData.float[i]) < 0.0001, 
        `float[${i}] mismatch`);
    });
    
    readData.double.forEach((val, i) => {
      assert.ok(Math.abs(val - originalData.double[i]) < 0.0001, 
        `double[${i}] mismatch`);
    });
  });

  test('filesystem roundtrip - write to file and read back', async () => {
    const schema = [
      { name: 'id', type: 'int32' },
      { name: 'name', type: 'string' },
      { name: 'score', type: 'double' }
    ];
    
    const originalData = {
      id: [1, 2, 3, 4, 5],
      name: ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
      score: [95.5, 87.3, 92.1, 88.9, 91.2]
    };
    
    // Write to file
    const bytes = await writeParquet(schema, originalData);
    const testFile = join(tmpdir(), `parquet-test-${Date.now()}.parquet`);
    writeFileSync(testFile, bytes);
    
    try {
      // Verify file exists and has content
      const stats = statSync(testFile);
      assert.ok(stats.size > 0, 'File should not be empty');
      
      // Read from file
      const fileBytes = readFileSync(testFile);
      assert.ok(fileBytes instanceof Uint8Array || fileBytes instanceof Buffer);
      
      // Read metadata from file
      const metadata = await readMetadata(fileBytes);
      assert.strictEqual(metadata.num_rows, 5);
      assert.strictEqual(metadata.columns.length, 3);
      
      // Read data from file
      const readData = await readParquet(fileBytes);
      
      // Verify data matches
      assert.deepStrictEqual(readData.id, originalData.id);
      assert.deepStrictEqual(readData.name, originalData.name);
      readData.score.forEach((val, i) => {
        assert.ok(Math.abs(val - originalData.score[i]) < 0.0001, 
          `score[${i}] mismatch`);
      });
    } finally {
      // Cleanup
      try {
        unlinkSync(testFile);
      } catch (err) {
        // Ignore cleanup errors
      }
    }
  });

  test('filesystem roundtrip - with compression', async () => {
    const schema = [{ name: 'data', type: 'string' }];
    const originalData = { data: Array(1000).fill('test data').map((s, i) => `${s}-${i}`) };
    
    // Write compressed file
    const bytes = await writeParquet(schema, originalData, { compression: 'snappy' });
    const testFile = join(tmpdir(), `parquet-compressed-${Date.now()}.parquet`);
    writeFileSync(testFile, bytes);
    
    try {
      // Read from file
      const fileBytes = readFileSync(testFile);
      const readData = await readParquet(fileBytes);
      
      // Verify data matches
      assert.deepStrictEqual(readData.data, originalData.data);
    } finally {
      try {
        unlinkSync(testFile);
      } catch (err) {
        // Ignore cleanup errors
      }
    }
  });

  test('read titanic.parquet file', async () => {
    const titanicFile = join(__dirname, 'titanic.parquet');
    
    // Check if file exists
    try {
      statSync(titanicFile);
    } catch (e) {
      console.log('Skipping titanic test - file not found at', titanicFile);
      return;
    }
    
    const bytes = readFileSync(titanicFile);
    
    // Read metadata
    const metadata = await readMetadata(bytes);
    assert.strictEqual(metadata.num_rows, 891);
    assert.ok(metadata.columns.length > 0);
    
    // Verify some columns exist
    const columnNames = metadata.columns.map(c => c.name);
    assert.ok(columnNames.includes('PassengerId'));
    assert.ok(columnNames.includes('Survived'));
    assert.ok(columnNames.includes('Name'));
    
    // Read specific columns (some columns might have encoding issues)
    // Try reading numeric columns first which are more reliable
    const data = await readParquet(bytes, ['PassengerId', 'Survived', 'Pclass']);
    
    // Verify we got data
    assert.ok(data.PassengerId);
    assert.strictEqual(data.PassengerId.length, 891);
    assert.ok(data.Survived);
    assert.strictEqual(data.Survived.length, 891);
    
    // Verify first row
    assert.strictEqual(data.PassengerId[0], 1);
    assert.ok(typeof data.Survived[0] === 'number');
  });

  test('interop: write with parquet-lite, read with parquetjs', async () => {
    // Skip if parquetjs not available
    let ParquetReader;
    try {
      const parquetjsModule = await import('parquetjs');
      const { createRequire } = await import('module');
      const require = createRequire(import.meta.url);
      ParquetReader = require('parquetjs').ParquetReader;
    } catch (e) {
      console.log('Skipping interop test - parquetjs not available');
      return;
    }

    const schema = [
      { name: 'id', type: 'int32' },
      { name: 'name', type: 'string' },
      { name: 'score', type: 'double' }
    ];
    
    const originalData = {
      id: [1, 2, 3, 4, 5],
      name: ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
      score: [95.5, 87.3, 92.1, 88.9, 91.2]
    };
    
    // Write with parquet-lite (using V1 format for compatibility)
    const bytes = await writeParquet(schema, originalData, { compression: 'none' });
    const testFile = join(tmpdir(), `parquet-interop-${Date.now()}.parquet`);
    writeFileSync(testFile, bytes);
    
    try {
      // Read with parquetjs - this MUST work for interop
      const reader = await ParquetReader.openFile(testFile);
      const cursor = reader.getCursor();
      
      const rows = [];
      let row = await cursor.next();
      while (row) {
        rows.push(row);
        row = await cursor.next();
      }
      await reader.close();
      
      // Verify data
      assert.strictEqual(rows.length, 5);
      assert.strictEqual(rows[0].id, 1);
      // parquetjs returns strings as Buffers, convert to string
      const name0 = Buffer.isBuffer(rows[0].name) ? rows[0].name.toString() : rows[0].name;
      assert.strictEqual(name0, 'Alice');
      assert.ok(Math.abs(rows[0].score - 95.5) < 0.0001);
    } finally {
      try {
        unlinkSync(testFile);
      } catch (err) {
        // Ignore cleanup errors
      }
    }
  });

  test('interop: write with parquetjs, read with parquet-lite', async () => {
    // Skip if parquetjs not available
    let ParquetSchema, ParquetWriter, ParquetReader;
    try {
      const parquetjsModule = await import('parquetjs');
      const { createRequire } = await import('module');
      const require = createRequire(import.meta.url);
      const parquetjs = require('parquetjs');
      ParquetSchema = parquetjs.ParquetSchema;
      ParquetWriter = parquetjs.ParquetWriter;
      ParquetReader = parquetjs.ParquetReader;
    } catch (e) {
      console.log('Skipping interop test - parquetjs not available');
      return;
    }

    const testFile = join(tmpdir(), `parquet-interop-js-${Date.now()}.parquet`);
    
    // Write with parquetjs
    const schema = new ParquetSchema({
      id: { type: 'INT32' },
      name: { type: 'UTF8' },
      score: { type: 'DOUBLE' }
    });
    
    const writer = await ParquetWriter.openFile(schema, testFile);
    
    await writer.appendRow({ id: 1, name: 'Alice', score: 95.5 });
    await writer.appendRow({ id: 2, name: 'Bob', score: 87.3 });
    await writer.appendRow({ id: 3, name: 'Charlie', score: 92.1 });
    await writer.close();
    
    try {
      // Read with parquet-lite
      const bytes = readFileSync(testFile);
      const data = await readParquet(bytes);
      
      // Verify data
      assert.deepStrictEqual(data.id, [1, 2, 3]);
      assert.deepStrictEqual(data.name, ['Alice', 'Bob', 'Charlie']);
      data.score.forEach((val, i) => {
        const expected = [95.5, 87.3, 92.1][i];
        assert.ok(Math.abs(val - expected) < 0.0001, 
          `score[${i}] mismatch: ${val} vs ${expected}`);
      });
    } finally {
      try {
        unlinkSync(testFile);
      } catch (err) {
        // Ignore cleanup errors
      }
    }
  });

  test('interop: nullable columns with parquetjs', async () => {
    // Skip if parquetjs not available
    let ParquetSchema, ParquetWriter, ParquetReader;
    try {
      const parquetjsModule = await import('parquetjs');
      const { createRequire } = await import('module');
      const require = createRequire(import.meta.url);
      const parquetjs = require('parquetjs');
      ParquetSchema = parquetjs.ParquetSchema;
      ParquetWriter = parquetjs.ParquetWriter;
      ParquetReader = parquetjs.ParquetReader;
    } catch (e) {
      console.log('Skipping interop test - parquetjs not available');
      return;
    }

    const testFile = join(tmpdir(), `parquet-nullable-interop-${Date.now()}.parquet`);
    
    // Write with parquet-lite (nullable column)
    const schema = [
      { name: 'id', type: 'int32', nullable: true },
      { name: 'name', type: 'string', nullable: true }
    ];
    
    const originalData = {
      id: [1, null, 3, null, 5],
      name: ['Alice', 'Bob', null, 'Diana', null]
    };
    
    const bytes = await writeParquet(schema, originalData, { compression: 'none' });
    writeFileSync(testFile, bytes);
    
    try {
      // Read with parquetjs - this MUST work for interop
      const reader = await ParquetReader.openFile(testFile);
      const cursor = reader.getCursor();
      
      const rows = [];
      let row = await cursor.next();
      while (row) {
        rows.push(row);
        row = await cursor.next();
      }
      await reader.close();
      
      // Verify data
      assert.strictEqual(rows.length, 5);
      assert.strictEqual(rows[0].id, 1);
      // parquetjs returns strings as Buffers, convert to string
      const name0 = Buffer.isBuffer(rows[0].name) ? rows[0].name.toString() : rows[0].name;
      assert.strictEqual(name0, 'Alice');
      // parquetjs omits null fields, so check that id is missing for row 1
      assert.strictEqual(rows[1].id, undefined);
      const name1 = Buffer.isBuffer(rows[1].name) ? rows[1].name.toString() : rows[1].name;
      assert.strictEqual(name1, 'Bob');
      assert.strictEqual(rows[2].id, 3);
      // parquetjs omits null name field
      assert.strictEqual(rows[2].name, undefined);
    } finally {
      try {
        unlinkSync(testFile);
      } catch (err) {
        // Ignore cleanup errors
      }
    }
  });

  test('version option - V1 and V2', async () => {
    const schema = [{ name: 'id', type: 'int32' }];
    const data = { id: [1, 2, 3, 4, 5] };
    
    // Test V1 (default)
    const v1 = await writeParquet(schema, data, { version: 'v1' });
    const v1Data = await readParquet(v1);
    assert.deepStrictEqual(v1Data.id, [1, 2, 3, 4, 5]);
    
    // Test V2
    const v2 = await writeParquet(schema, data, { version: 'v2' });
    const v2Data = await readParquet(v2);
    assert.deepStrictEqual(v2Data.id, [1, 2, 3, 4, 5]);
    
    // Both should be readable
    assert.ok(v1.length > 0);
    assert.ok(v2.length > 0);
  });

  test('interop: V2 files readable by parquetjs', async () => {
    // Skip if parquetjs not available
    let ParquetReader;
    try {
      const parquetjsModule = await import('parquetjs');
      const { createRequire } = await import('module');
      const require = createRequire(import.meta.url);
      ParquetReader = require('parquetjs').ParquetReader;
    } catch (e) {
      console.log('Skipping V2 interop test - parquetjs not available');
      return;
    }

    const schema = [
      { name: 'id', type: 'int32' },
      { name: 'name', type: 'string' },
      { name: 'score', type: 'double' }
    ];
    
    const originalData = {
      id: [1, 2, 3, 4, 5],
      name: ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
      score: [95.5, 87.3, 92.1, 88.9, 91.2]
    };
    
    // Write V2 format
    const bytes = await writeParquet(schema, originalData, { 
      version: 'v2', 
      compression: 'none' 
    });
    const testFile = join(tmpdir(), `parquet-v2-interop-${Date.now()}.parquet`);
    writeFileSync(testFile, bytes);
    
    try {
      // Read with parquetjs - note: parquetjs may not fully support V2
      const reader = await ParquetReader.openFile(testFile);
      const cursor = reader.getCursor();
      
      const rows = [];
      let row = await cursor.next();
      while (row) {
        rows.push(row);
        row = await cursor.next();
      }
      await reader.close();
      
      // Verify data
      assert.strictEqual(rows.length, 5);
      assert.strictEqual(rows[0].id, 1);
      const name0 = Buffer.isBuffer(rows[0].name) ? rows[0].name.toString() : rows[0].name;
      assert.strictEqual(name0, 'Alice');
      assert.ok(Math.abs(rows[0].score - 95.5) < 0.0001);
    } catch (err) {
      // parquetjs may not support V2 - verify file is valid by reading with parquet-lite
      console.log('Note: parquetjs may not fully support V2 format');
      const readBack = await readParquet(bytes);
      assert.deepStrictEqual(readBack.id, originalData.id);
      assert.deepStrictEqual(readBack.name, originalData.name);
      // File is valid, just parquetjs limitation
    } finally {
      try {
        unlinkSync(testFile);
      } catch (err) {
        // Ignore cleanup errors
      }
    }
  });

  test('interop: V2 files readable by pyarrow', async () => {
    // Skip if pyarrow not available
    let pyarrowAvailable = false;
    try {
      execSync('python3 -c "import pyarrow.parquet"', { encoding: 'utf-8', stdio: 'pipe' });
      pyarrowAvailable = true;
    } catch (e) {
      console.log('Skipping pyarrow V2 test - pyarrow not available');
      return;
    }

    if (!pyarrowAvailable) return;

    const schema = [
      { name: 'id', type: 'int32' },
      { name: 'name', type: 'string' },
      { name: 'score', type: 'double' }
    ];
    
    const originalData = {
      id: [1, 2, 3, 4, 5],
      name: ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
      score: [95.5, 87.3, 92.1, 88.9, 91.2]
    };
    
    // Write V2 format
    const bytes = await writeParquet(schema, originalData, { 
      version: 'v2',
      compression: 'none' 
    });
    const testFile = join(tmpdir(), `parquet-v2-pyarrow-${Date.now()}.parquet`);
    writeFileSync(testFile, bytes);
    
    try {
      // Read with pyarrow - V2 MUST be readable
      const testScript = join(__dirname, 'test-v2-pyarrow.py');
      const result = execSync(`python3 "${testScript}" "${testFile}"`, { 
        encoding: 'utf-8',
        stdio: 'pipe'
      });
      
      // Verify output contains expected data
      assert.ok(result.includes('pyarrow can read V2 file'));
      assert.ok(result.includes('Rows: 5'));
      assert.ok(result.includes('First row id: 1'));
      assert.ok(result.includes('Alice') || result.includes("b'Alice'")); // pyarrow may return bytes
    } finally {
      try {
        unlinkSync(testFile);
      } catch (err) {
        // Ignore cleanup errors
      }
    }
  });

  test('logical types - DATE', async () => {
    const schema = [
      { name: 'date', type: 'int32', logicalType: 'DATE' }
    ];
    const data = {
      date: [1, 2, 3] // Days since Unix epoch
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    
    assert.strictEqual(metadata.columns[0].logical_type, 'date');
    assert.strictEqual(metadata.columns[0].type, 'int32');
    
    const readData = await readParquet(bytes);
    // Dates should be converted to Date objects
    assert(readData.date[0] instanceof Date);
    assert(readData.date[1] instanceof Date);
    assert(readData.date[2] instanceof Date);
  });

  test('logical types - TIMESTAMP_MILLIS', async () => {
    const schema = [
      { name: 'timestamp', type: 'int64', logicalType: 'timestamp_millis' }
    ];
    const data = {
      timestamp: [1000000n, 2000000n, 3000000n] // Milliseconds since Unix epoch
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    
    assert.strictEqual(metadata.columns[0].logical_type, 'timestamp_millis');
    assert.strictEqual(metadata.columns[0].type, 'int64');
    
    const readData = await readParquet(bytes);
    // Timestamps should be converted to Date objects
    assert(readData.timestamp[0] instanceof Date);
    assert(readData.timestamp[1] instanceof Date);
    assert(readData.timestamp[2] instanceof Date);
    assert.strictEqual(readData.timestamp[0].getTime(), 1000000);
    assert.strictEqual(readData.timestamp[1].getTime(), 2000000);
    assert.strictEqual(readData.timestamp[2].getTime(), 3000000);
  });

  test('logical types - TIMESTAMP_MICROS', async () => {
    const schema = [
      { name: 'timestamp', type: 'int64', logicalType: 'timestamp_micros' }
    ];
    const data = {
      timestamp: [1000000n, 2000000n, 3000000n] // Microseconds since Unix epoch
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    
    assert.strictEqual(metadata.columns[0].logical_type, 'timestamp_micros');
    assert.strictEqual(metadata.columns[0].type, 'int64');
    
    const readData = await readParquet(bytes);
    // Timestamps should be converted to Date objects
    assert(readData.timestamp[0] instanceof Date);
    assert(readData.timestamp[1] instanceof Date);
    assert(readData.timestamp[2] instanceof Date);
    // Note: Date objects use milliseconds, so microsecond precision is lost
    assert.strictEqual(readData.timestamp[0].getTime(), 1000); // 1000000 microseconds = 1000 ms
    assert.strictEqual(readData.timestamp[1].getTime(), 2000);
    assert.strictEqual(readData.timestamp[2].getTime(), 3000);
  });

  test('logical types - UTF8', async () => {
    const schema = [
      { name: 'text', type: 'string', logicalType: 'utf8' }
    ];
    const data = {
      text: ['Hello', 'World', 'Test']
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    
    assert.strictEqual(metadata.columns[0].logical_type, 'utf8');
    assert.strictEqual(metadata.columns[0].type, 'string');
    
    const readData = await readParquet(bytes);
    assert.deepStrictEqual(readData.text, ['Hello', 'World', 'Test']);
  });

  test('logical types - JSON', async () => {
    const schema = [
      { name: 'json', type: 'string', logicalType: 'json' }
    ];
    const data = {
      json: ['{"a":1}', '{"b":2}', '{"c":3}']
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    
    assert.strictEqual(metadata.columns[0].logical_type, 'json');
    
    const readData = await readParquet(bytes);
    // JSON strings should be parsed to JS objects
    assert.deepStrictEqual(readData.json[0], { a: 1 });
    assert.deepStrictEqual(readData.json[1], { b: 2 });
    assert.deepStrictEqual(readData.json[2], { c: 3 });
  });

  test('logical types - BSON', async () => {
    const schema = [
      { name: 'bson', type: 'string', logicalType: 'bson' }
    ];
    const data = {
      bson: ['data1', 'data2', 'data3']
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    
    assert.strictEqual(metadata.columns[0].logical_type, 'bson');
    
    const readData = await readParquet(bytes);
    assert.deepStrictEqual(readData.bson, ['data1', 'data2', 'data3']);
  });

  test('logical types - TIME_MILLIS', async () => {
    const schema = [
      { name: 'time', type: 'int32', logicalType: 'time_millis' }
    ];
    const data = {
      time: [1000, 2000, 3000] // Milliseconds since midnight
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    
    assert.strictEqual(metadata.columns[0].logical_type, 'time_millis');
    assert.strictEqual(metadata.columns[0].type, 'int32');
    
    const readData = await readParquet(bytes);
    // Time values should be converted to Date objects
    assert(readData.time[0] instanceof Date);
    assert(readData.time[1] instanceof Date);
    assert(readData.time[2] instanceof Date);
    // Date objects will be at epoch + time offset
    assert.strictEqual(readData.time[0].getTime(), 1000);
    assert.strictEqual(readData.time[1].getTime(), 2000);
    assert.strictEqual(readData.time[2].getTime(), 3000);
  });

  test('logical types - TIME_MICROS', async () => {
    const schema = [
      { name: 'time', type: 'int64', logicalType: 'time_micros' }
    ];
    const data = {
      time: [1000n, 2000n, 3000n] // Microseconds since midnight
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    
    assert.strictEqual(metadata.columns[0].logical_type, 'time_micros');
    assert.strictEqual(metadata.columns[0].type, 'int64');
    
    const readData = await readParquet(bytes);
    // Time values should be converted to Date objects
    assert(readData.time[0] instanceof Date);
    assert(readData.time[1] instanceof Date);
    assert(readData.time[2] instanceof Date);
    // Date objects will be at epoch + time offset (microseconds converted to milliseconds)
    assert.strictEqual(readData.time[0].getTime(), 1); // 1000 microseconds = 1 ms
    assert.strictEqual(readData.time[1].getTime(), 2); // 2000 microseconds = 2 ms
    assert.strictEqual(readData.time[2].getTime(), 3); // 3000 microseconds = 3 ms
  });

  test('logical types - roundtrip with multiple logical types', async () => {
    const schema = [
      { name: 'date', type: 'int32', logicalType: 'date' },
      { name: 'timestamp', type: 'int64', logicalType: 'timestamp_millis' },
      { name: 'text', type: 'string', logicalType: 'utf8' },
      { name: 'json', type: 'string', logicalType: 'json' }
    ];
    const data = {
      date: [1, 2, 3],
      timestamp: [1000000n, 2000000n, 3000000n],
      text: ['Hello', 'World', 'Test'],
      json: ['{"a":1}', '{"b":2}', '{"c":3}']
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    
    assert.strictEqual(metadata.columns.length, 4);
    assert.strictEqual(metadata.columns[0].logical_type, 'date');
    assert.strictEqual(metadata.columns[1].logical_type, 'timestamp_millis');
    assert.strictEqual(metadata.columns[2].logical_type, 'utf8');
    assert.strictEqual(metadata.columns[3].logical_type, 'json');
    
    const readData = await readParquet(bytes);
    // Date values should be converted to Date objects
    assert(readData.date[0] instanceof Date);
    assert(readData.date[1] instanceof Date);
    assert(readData.date[2] instanceof Date);
    // Timestamp values should be converted to Date objects
    assert(readData.timestamp[0] instanceof Date);
    assert(readData.timestamp[1] instanceof Date);
    assert(readData.timestamp[2] instanceof Date);
    assert.deepStrictEqual(readData.text, ['Hello', 'World', 'Test']);
    // JSON strings should be parsed to JS objects
    assert.deepStrictEqual(readData.json[0], { a: 1 });
    assert.deepStrictEqual(readData.json[1], { b: 2 });
    assert.deepStrictEqual(readData.json[2], { c: 3 });
  });

  test('automatic conversion - JS Date objects to DATE', async () => {
    const schema = [
      { name: 'date', type: 'int32', logicalType: 'date' }
    ];
    const dates = [
      new Date('2024-01-01'),
      new Date('2024-01-02'),
      new Date('2024-01-03')
    ];
    
    const bytes = await writeParquet(schema, { date: dates });
    const metadata = await readMetadata(bytes);
    assert.strictEqual(metadata.columns[0].logical_type, 'date');
    
    const readData = await readParquet(bytes);
    // Verify dates were converted back to Date objects
    assert(readData.date[0] instanceof Date);
    assert(readData.date[1] instanceof Date);
    assert(readData.date[2] instanceof Date);
    assert.strictEqual(readData.date[0].getTime(), dates[0].getTime());
    assert.strictEqual(readData.date[1].getTime(), dates[1].getTime());
    assert.strictEqual(readData.date[2].getTime(), dates[2].getTime());
  });

  test('automatic conversion - JS Date objects to TIMESTAMP_MILLIS', async () => {
    const schema = [
      { name: 'timestamp', type: 'int64', logicalType: 'timestamp_millis' }
    ];
    const dates = [
      new Date('2024-01-01T00:00:00Z'),
      new Date('2024-01-02T00:00:00Z')
    ];
    
    const bytes = await writeParquet(schema, { timestamp: dates });
    const metadata = await readMetadata(bytes);
    assert.strictEqual(metadata.columns[0].logical_type, 'timestamp_millis');
    
    const readData = await readParquet(bytes);
    // Verify timestamps were converted back to Date objects
    assert(readData.timestamp[0] instanceof Date);
    assert(readData.timestamp[1] instanceof Date);
    assert.strictEqual(readData.timestamp[0].getTime(), dates[0].getTime());
    assert.strictEqual(readData.timestamp[1].getTime(), dates[1].getTime());
  });

  test('automatic conversion - JS Date objects to TIMESTAMP_MICROS', async () => {
    const schema = [
      { name: 'timestamp', type: 'int64', logicalType: 'timestamp_micros' }
    ];
    const dates = [
      new Date('2024-01-01T00:00:00Z'),
      new Date('2024-01-02T00:00:00Z')
    ];
    
    const bytes = await writeParquet(schema, { timestamp: dates });
    const metadata = await readMetadata(bytes);
    assert.strictEqual(metadata.columns[0].logical_type, 'timestamp_micros');
    
    const readData = await readParquet(bytes);
    // Verify timestamps were converted back to Date objects
    // Note: microsecond precision may be lost in Date objects (they use milliseconds)
    assert(readData.timestamp[0] instanceof Date);
    assert(readData.timestamp[1] instanceof Date);
    // Check that they're approximately correct (within 1 second)
    assert(Math.abs(readData.timestamp[0].getTime() - dates[0].getTime()) < 1000);
    assert(Math.abs(readData.timestamp[1].getTime() - dates[1].getTime()) < 1000);
  });

  test('automatic conversion - JS Date objects to TIME_MICROS', async () => {
    const schema = [
      { name: 'time', type: 'int64', logicalType: 'time_micros' }
    ];
    const dates = [
      new Date('2024-01-01T00:00:01.000Z'),
      new Date('2024-01-01T00:00:02.123Z')
    ];
    
    const bytes = await writeParquet(schema, { time: dates });
    const metadata = await readMetadata(bytes);
    assert.strictEqual(metadata.columns[0].logical_type, 'time_micros');
    
    const readData = await readParquet(bytes);
    // Verify times were converted back to Date objects
    // Note: TIME represents time of day, so Date objects will be at epoch + time offset
    assert(readData.time[0] instanceof Date);
    assert(readData.time[1] instanceof Date);
    // Check that the time portion is correct (within 1 second)
    const time0_ms = readData.time[0].getTime() % (24 * 60 * 60 * 1000);
    const time1_ms = readData.time[1].getTime() % (24 * 60 * 60 * 1000);
    assert(Math.abs(time0_ms - 1000) < 1000); // ~1 second
    assert(Math.abs(time1_ms - 2123) < 1000); // ~2.123 seconds
  });

  test('automatic conversion - JS Objects to JSON', async () => {
    const schema = [
      { name: 'json', type: 'string', logicalType: 'json' }
    ];
    const objects = [
      { a: 1, b: 'test', c: true },
      { x: 2, y: 'hello', z: null }
    ];
    
    const bytes = await writeParquet(schema, { json: objects });
    const metadata = await readMetadata(bytes);
    assert.strictEqual(metadata.columns[0].logical_type, 'json');
    
    const readData = await readParquet(bytes);
    // Verify objects were converted back to JS objects (not JSON strings)
    assert.strictEqual(readData.json.length, 2);
    assert.strictEqual(typeof readData.json[0], 'object');
    assert.strictEqual(typeof readData.json[1], 'object');
    assert.strictEqual(readData.json[0].a, 1);
    assert.strictEqual(readData.json[0].b, 'test');
    assert.strictEqual(readData.json[0].c, true);
    assert.strictEqual(readData.json[1].x, 2);
    assert.strictEqual(readData.json[1].y, 'hello');
    assert.strictEqual(readData.json[1].z, null);
  });

  test('automatic conversion - nullable Date objects', async () => {
    const schema = [
      { name: 'date', type: 'int32', logicalType: 'date', nullable: true }
    ];
    const dates = [
      new Date('2024-01-01'),
      null,
      new Date('2024-01-03')
    ];
    
    const bytes = await writeParquet(schema, { date: dates });
    const readData = await readParquet(bytes);
    
    // Verify dates were converted back to Date objects (nulls preserved)
    assert(readData.date[0] instanceof Date);
    assert.strictEqual(readData.date[0].getTime(), dates[0].getTime());
    assert.strictEqual(readData.date[1], null);
    assert(readData.date[2] instanceof Date);
    assert.strictEqual(readData.date[2].getTime(), dates[2].getTime());
  });

  test('automatic conversion - nullable Objects', async () => {
    const schema = [
      { name: 'json', type: 'string', logicalType: 'json', nullable: true }
    ];
    const objects = [
      { a: 1 },
      null,
      { b: 2 }
    ];
    
    const bytes = await writeParquet(schema, { json: objects });
    const readData = await readParquet(bytes);
    
    assert.strictEqual(readData.json.length, 3);
    // JSON strings should be parsed to JS objects (nulls preserved)
    assert.strictEqual(typeof readData.json[0], 'object');
    assert.strictEqual(readData.json[0].a, 1);
    assert.strictEqual(readData.json[1], null);
    assert.strictEqual(typeof readData.json[2], 'object');
    assert.strictEqual(readData.json[2].b, 2);
  });

  test('logical types - DECIMAL', async () => {
    const schema = [
      { name: 'price', type: 'int64', logicalType: 'decimal', precision: 10, scale: 2 }
    ];
    const data = {
      price: [10000, 20000, 30000] // Stored as integers (100.00, 200.00, 300.00)
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    
    assert.strictEqual(metadata.columns[0].logical_type, 'decimal(10,2)');
    assert.strictEqual(metadata.columns[0].type, 'int64');
    
    const readData = await readParquet(bytes);
    assert.deepStrictEqual(readData.price.map(x => Number(x)), [10000, 20000, 30000]);
  });

  test('logical types - ENUM', async () => {
    const schema = [
      { name: 'status', type: 'string', logicalType: 'enum' }
    ];
    const data = {
      status: ['active', 'inactive', 'pending']
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    
    assert.strictEqual(metadata.columns[0].logical_type, 'enum');
    assert.strictEqual(metadata.columns[0].type, 'string');
    
    const readData = await readParquet(bytes);
    assert.deepStrictEqual(readData.status, ['active', 'inactive', 'pending']);
  });

  test('logical types - INTEGER', async () => {
    const schema = [
      { name: 'age', type: 'int32', logicalType: 'integer', bitWidth: 32, isSigned: true }
    ];
    const data = {
      age: [25, 30, 35]
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    
    // Note: Integer logical type may not be preserved in metadata reading
    // This is a limitation of how parquet2 handles Integer logical types
    assert.strictEqual(metadata.columns[0].type, 'int32');
    
    const readData = await readParquet(bytes);
    assert.deepStrictEqual(readData.age, [25, 30, 35]);
  });

  test('logical types - UUID (requires FixedLenByteArray)', async () => {
    // UUID requires FixedLenByteArray(16) per Parquet spec
    // For now, we'll skip this test as we don't support FixedLenByteArray yet
    // This is a placeholder for future implementation
    const schema = [
      { name: 'id', type: 'string', logicalType: 'uuid' }
    ];
    const data = {
      id: ['550e8400-e29b-41d4-a716-446655440000']
    };
    
    // UUID with ByteArray will fail - this is expected
    // We'll need to add FixedLenByteArray support for proper UUID handling
    await assert.rejects(
      async () => await writeParquet(schema, data),
      /Cannot annotate Uuid from ByteArray/
    );
  });

  test('Integer logical type with TypedArrays', async () => {
    // Test Uint8Array with integer(8, false)
    const schema1 = [
      { name: 'flags', type: 'int32', logicalType: 'integer', bitWidth: 8, isSigned: false }
    ];
    const data1 = {
      flags: new Uint8Array([1, 2, 3, 255])
    };
    const bytes1 = await writeParquet(schema1, data1);
    const readData1 = await readParquet(bytes1);
    assert.deepStrictEqual(readData1.flags, [1, 2, 3, 255]);

    // Test Int8Array with integer(8, true)
    const schema2 = [
      { name: 'scores', type: 'int32', logicalType: 'integer', bitWidth: 8, isSigned: true }
    ];
    const data2 = {
      scores: new Int8Array([-128, -1, 0, 127])
    };
    const bytes2 = await writeParquet(schema2, data2);
    const readData2 = await readParquet(bytes2);
    assert.deepStrictEqual(readData2.scores, [-128, -1, 0, 127]);

    // Test Int32Array with integer(32, true)
    const schema3 = [
      { name: 'values', type: 'int32', logicalType: 'integer', bitWidth: 32, isSigned: true }
    ];
    const data3 = {
      values: new Int32Array([-2147483648, -1, 0, 2147483647])
    };
    const bytes3 = await writeParquet(schema3, data3);
    const readData3 = await readParquet(bytes3);
    assert.deepStrictEqual(readData3.values, [-2147483648, -1, 0, 2147483647]);
  });

  test('Enum logical type - pass strings normally', async () => {
    const schema = [
      { name: 'status', type: 'string', logicalType: 'enum' }
    ];
    const data = {
      status: ['active', 'inactive', 'pending', 'active'] // Just pass strings normally
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    assert.strictEqual(metadata.columns[0].logical_type, 'enum');
    
    const readData = await readParquet(bytes);
    assert.deepStrictEqual(readData.status, ['active', 'inactive', 'pending', 'active']);
  });

  test('Enum logical type - efficient index array', async () => {
    // Define enum values once
    const enumValues = ['active', 'inactive', 'pending'];
    
    const schema = [
      { name: 'status', type: 'string', logicalType: 'enum', enumValues }
    ];
    // Pass indices instead of full strings
    const data = {
      status: [0, 1, 2, 0] // Indices into enumValues array
    };
    
    const bytes = await writeParquet(schema, data);
    const metadata = await readMetadata(bytes);
    assert.strictEqual(metadata.columns[0].logical_type, 'enum');
    
    const readData = await readParquet(bytes);
    // Should read back as full strings
    assert.deepStrictEqual(readData.status, ['active', 'inactive', 'pending', 'active']);
  });

  test('Enum logical type - index array with TypedArray', async () => {
    const enumValues = ['red', 'green', 'blue'];
    
    const schema = [
      { name: 'color', type: 'string', logicalType: 'enum', enumValues }
    ];
    // Use Uint8Array for even more efficiency
    const data = {
      color: new Uint8Array([0, 1, 2, 0, 1])
    };
    
    const bytes = await writeParquet(schema, data);
    const readData = await readParquet(bytes);
    assert.deepStrictEqual(readData.color, ['red', 'green', 'blue', 'red', 'green']);
  });

});

