import { test, describe } from 'node:test';
import assert from 'node:assert';
import { writeFileSync, readFileSync, unlinkSync, statSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';
import { writeParquet } from '../dist/writer.js';
import { readParquet, readMetadata } from '../dist/reader.js';

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
});

