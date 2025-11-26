import { test, describe } from 'node:test';
import assert from 'node:assert';
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
});

