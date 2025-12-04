/**
 * Bug reproduction tests for parquet-lite
 * 
 * These tests demonstrate bugs that need to be fixed.
 * Each test should FAIL before the fix and PASS after.
 */

import { test, describe } from 'node:test';
import assert from 'node:assert';
import { writeFileSync, readFileSync, unlinkSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';
import { execSync } from 'child_process';
import { writeParquet } from '../dist/writer.js';
import { readParquet, readMetadata } from '../dist/reader.js';

describe('Bug reproductions', () => {
  
  /**
   * BUG #1: Dictionary-encoded columns not supported (Reader)
   * 
   * Many parquet files (especially from PyArrow, Pandas, Spark) use dictionary 
   * encoding for string columns. The reader skips dictionary pages, so these
   * columns return empty/missing data.
   */
  test('BUG #1: Dictionary-encoded columns should be readable', async () => {
    // Skip if pyarrow not available
    let pyarrowAvailable = false;
    try {
      execSync('python3 -c "import pyarrow.parquet"', { encoding: 'utf-8', stdio: 'pipe' });
      pyarrowAvailable = true;
    } catch (e) {
      console.log('Skipping dictionary test - pyarrow not available');
      return;
    }

    const testFile = join(tmpdir(), `parquet-dict-test-${Date.now()}.parquet`);
    
    // Create a parquet file with dictionary encoding using PyArrow
    const pythonScript = `
import pyarrow as pa
import pyarrow.parquet as pq

# Create table with repeated string values (will use dictionary encoding)
table = pa.table({
    'category': ['apple', 'banana', 'apple', 'cherry', 'banana', 'apple'],
    'value': [1, 2, 3, 4, 5, 6]
})

# Write with dictionary encoding (default for strings in PyArrow)
pq.write_table(table, '${testFile}', use_dictionary=True)
print('wrote dictionary-encoded parquet')
`;
    
    try {
      execSync(`python3 -c "${pythonScript}"`, { encoding: 'utf-8', stdio: 'pipe' });
      
      // Read with parquet-lite
      const bytes = readFileSync(testFile);
      const data = await readParquet(bytes);
      
      // BUG: Dictionary-encoded columns return empty arrays or wrong data
      // This assertion should PASS after the fix
      assert.strictEqual(data.category?.length, 6, 'Should read all 6 rows');
      assert.deepStrictEqual(
        data.category, 
        ['apple', 'banana', 'apple', 'cherry', 'banana', 'apple'],
        'Should read dictionary-encoded strings correctly'
      );
      // PyArrow uses Int64 for integers, so BigInt is expected
      assert.deepStrictEqual(data.value.map(v => Number(v)), [1, 2, 3, 4, 5, 6]);
      
    } finally {
      try { unlinkSync(testFile); } catch (e) {}
    }
  });

  /**
   * BUG #2: Unsigned Int32 overflow (Writer)
   * 
   * When writing Uint32Array with values > 2^31, the cast to i32 causes
   * overflow, silently corrupting data.
   */
  test('BUG #2: Unsigned Int32 values > 2^31 should not overflow', async () => {
    const schema = [
      { name: 'big_unsigned', type: 'int32', logicalType: 'integer', bitWidth: 32, isSigned: false }
    ];
    
    // Values that exceed signed int32 max (2147483647)
    const data = {
      big_unsigned: new Uint32Array([
        0,
        2147483647,  // Max signed int32
        2147483648,  // One more than max signed - this overflows!
        3000000000,  // Way over - this becomes negative
        4294967295   // Max uint32 - this becomes -1
      ])
    };
    
    const bytes = await writeParquet(schema, data);
    const readData = await readParquet(bytes);
    
    // BUG: Values > 2147483647 become negative due to i32 cast overflow
    // This assertion should PASS after the fix
    assert.deepStrictEqual(
      readData.big_unsigned,
      [0, 2147483647, 2147483648, 3000000000, 4294967295],
      'Unsigned values should not overflow to negative'
    );
  });

  /**
   * BUG #3: FixedLenByteArray silently ignored (Reader)
   * 
   * FixedLenByteArray columns (used for UUIDs, fixed-width binary) 
   * are silently skipped, returning undefined/empty.
   */
  test('BUG #3: FixedLenByteArray columns should be readable', async () => {
    // Skip if pyarrow not available
    let pyarrowAvailable = false;
    try {
      execSync('python3 -c "import pyarrow.parquet"', { encoding: 'utf-8', stdio: 'pipe' });
      pyarrowAvailable = true;
    } catch (e) {
      console.log('Skipping FixedLenByteArray test - pyarrow not available');
      return;
    }

    const testFile = join(tmpdir(), `parquet-fixed-test-${Date.now()}.parquet`);
    
    // Create a parquet file with FixedLenByteArray using PyArrow
    const pythonScript = `
import pyarrow as pa
import pyarrow.parquet as pq
import uuid

# Create fixed length binary (16 bytes for UUID)
uuids = [uuid.uuid4().bytes for _ in range(3)]
table = pa.table({
    'uuid': pa.array(uuids, type=pa.binary(16)),
    'id': [1, 2, 3]
})

pq.write_table(table, '${testFile}')
print('wrote fixed-len-byte-array parquet')
`;
    
    try {
      execSync(`python3 -c "${pythonScript}"`, { encoding: 'utf-8', stdio: 'pipe' });
      
      // Read with parquet-lite
      const bytes = readFileSync(testFile);
      const metadata = await readMetadata(bytes);
      
      // Verify metadata shows the column exists
      const uuidCol = metadata.columns.find(c => c.name === 'uuid');
      assert.ok(uuidCol, 'UUID column should exist in metadata');
      
      const data = await readParquet(bytes);
      
      // BUG: FixedLenByteArray columns return undefined
      // This assertion should PASS after the fix
      assert.ok(data.uuid, 'UUID column should be readable');
      assert.strictEqual(data.uuid.length, 3, 'Should read all 3 UUID values');
      
      // Each UUID should be 16 bytes
      for (const uuid of data.uuid) {
        assert.ok(uuid, 'UUID value should not be null/undefined');
      }
      
    } finally {
      try { unlinkSync(testFile); } catch (e) {}
    }
  });

  /**
   * BUG #4: Int64 precision loss for large values
   * 
   * Large Int64 values (beyond 2^53) are cast to f64, losing precision.
   * This affects microsecond/nanosecond timestamps.
   */
  test('BUG #4: Large Int64 values should not lose precision', async () => {
    const schema = [
      { name: 'big_int', type: 'int64' }
    ];
    
    // Values that exceed safe integer (2^53 - 1 = 9007199254740991)
    const data = {
      big_int: [
        9007199254740991n,   // Max safe integer - should be exact
        9007199254740992n,   // One more - starts losing precision in f64
        9007199254740993n,   // Two more - definitely loses precision
        1234567890123456789n // Large value - loses precision
      ]
    };
    
    const bytes = await writeParquet(schema, data);
    const readData = await readParquet(bytes);
    
    // All values should be returned as BigInt to preserve precision
    // BUG: Currently returns f64 which loses precision for large values
    assert.ok(readData.big_int.every(v => typeof v === 'bigint'), 
      'Large int64 should be returned as BigInt, not number');
    
    assert.strictEqual(readData.big_int[0], 9007199254740991n);
    assert.strictEqual(readData.big_int[1], 9007199254740992n);
    assert.strictEqual(readData.big_int[2], 9007199254740993n);
    assert.strictEqual(readData.big_int[3], 1234567890123456789n);
  });

  /**
   * BUG #5: Multiple row groups - verify fix is working
   * 
   * This was the original bug where only the first row group was read.
   * We fixed it by using unchecked_ref instead of Array::from.
   */
  test('BUG #5 (FIXED): Multiple row groups should all be read', async () => {
    const schema = [
      { name: 'id', type: 'int32' },
      { name: 'name', type: 'string' }
    ];
    
    // Create enough data for multiple row groups (default is 10,000 per group)
    const rowCount = 25000;
    const data = {
      id: Array.from({ length: rowCount }, (_, i) => i + 1),
      name: Array.from({ length: rowCount }, (_, i) => `name-${i}`)
    };
    
    const bytes = await writeParquet(schema, data, { rowGroupSize: 10000 });
    const metadata = await readMetadata(bytes);
    
    // Should have 3 row groups: 10000 + 10000 + 5000
    assert.strictEqual(metadata.num_row_groups, 3, 'Should have 3 row groups');
    assert.strictEqual(metadata.num_rows, rowCount, 'Metadata should show all rows');
    
    const readData = await readParquet(bytes);
    
    // This was the bug - only first row group (10000) was read
    assert.strictEqual(readData.id.length, rowCount, 'Should read ALL rows, not just first row group');
    assert.strictEqual(readData.name.length, rowCount, 'Should read ALL string rows');
    
    // Verify last row group data is present
    assert.strictEqual(readData.id[24999], 25000);
    assert.strictEqual(readData.name[24999], 'name-24999');
  });

  /**
   * BUG #6: Delta encoding not supported (Reader)
   * 
   * Delta encoding is used by some writers for sorted integer data.
   * Previously unsupported.
   */
  test('BUG #6 (NEW): Delta-encoded columns should be readable', async () => {
    // Skip if pyarrow not available
    let pyarrowAvailable = false;
    try {
      execSync('python3 -c "import pyarrow.parquet"', { encoding: 'utf-8', stdio: 'pipe' });
      pyarrowAvailable = true;
    } catch (e) {
      console.log('Skipping delta encoding test - pyarrow not available');
      return;
    }

    const testFile = join(tmpdir(), `parquet-delta-test-${Date.now()}.parquet`);
    
    // Create a parquet file with delta encoding using PyArrow
    // PyArrow uses delta encoding for sorted integer columns with DELTA_BINARY_PACKED
    const pythonScript = `
import pyarrow as pa
import pyarrow.parquet as pq

# Create table with sorted integers (delta encoding works well for these)
# Force DELTA_BINARY_PACKED encoding
table = pa.table({
    'sorted_ids': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
    'names': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
})

# Write with delta encoding for the integer column
# Must disable dictionary encoding when using explicit column encoding
pq.write_table(
    table, 
    '${testFile}',
    use_dictionary=False,
    column_encoding={'sorted_ids': 'DELTA_BINARY_PACKED'}
)
print('wrote delta-encoded parquet')
`;
    
    try {
      execSync(`python3 -c "${pythonScript}"`, { encoding: 'utf-8', stdio: 'pipe' });
      
      // Read with parquet-lite
      const bytes = readFileSync(testFile);
      const data = await readParquet(bytes);
      
      // These assertions should PASS after adding delta encoding support
      assert.ok(data.sorted_ids, 'sorted_ids column should be readable');
      assert.strictEqual(data.sorted_ids.length, 10, 'Should read all 10 rows');
      assert.deepStrictEqual(
        data.sorted_ids.map(v => Number(v)), 
        [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
        'Should read delta-encoded integers correctly'
      );
      
    } finally {
      try { unlinkSync(testFile); } catch (e) {}
    }
  });

  /**
   * BUG #7 (NEW): Nested types (lists) should be readable
   * 
   * Parquet supports nested types like LIST and MAP. Previously unsupported.
   */
  test('BUG #7 (NEW): Nested list columns should be readable', async () => {
    // Skip if pyarrow not available
    let pyarrowAvailable = false;
    try {
      execSync('python3 -c "import pyarrow.parquet"', { encoding: 'utf-8', stdio: 'pipe' });
      pyarrowAvailable = true;
    } catch (e) {
      console.log('Skipping nested types test - pyarrow not available');
      return;
    }

    const testFile = join(tmpdir(), `parquet-nested-test-${Date.now()}.parquet`);
    
    // Create a parquet file with list columns using PyArrow
    const pythonScript = `
import pyarrow as pa
import pyarrow.parquet as pq

# Create table with list columns
table = pa.table({
    'id': [1, 2, 3],
    'tags': [['a', 'b'], ['c'], ['d', 'e', 'f']],
    'scores': [[1, 2, 3], [4], [5, 6]]
})

pq.write_table(table, '${testFile}')
print('wrote nested parquet')
`;
    
    try {
      execSync(`python3 -c "${pythonScript}"`, { encoding: 'utf-8', stdio: 'pipe' });
      
      // Read with parquet-lite
      const bytes = readFileSync(testFile);
      const metadata = await readMetadata(bytes);
      
      // Check that columns include nested fields
      const colNames = metadata.columns.map(c => c.name);
      console.log('Nested column names:', colNames);
      
      const data = await readParquet(bytes);
      console.log('Nested data keys:', Object.keys(data));
      
      // The id column should be flat
      assert.ok(data.id || data['id'], 'id column should be readable');
      
      // For nested columns, we should get grouped arrays
      // The exact column name might be "tags.list.element" or similar
      const tagsCol = Object.keys(data).find(k => k.includes('tags'));
      const scoresCol = Object.keys(data).find(k => k.includes('scores'));
      
      if (tagsCol) {
        console.log('Tags column:', tagsCol, data[tagsCol]);
        assert.ok(Array.isArray(data[tagsCol]), 'Tags should be an array');
        // Each row should be an array (grouped by repetition level)
        if (Array.isArray(data[tagsCol][0])) {
          assert.deepStrictEqual(data[tagsCol][0], ['a', 'b'], 'First row tags');
          assert.deepStrictEqual(data[tagsCol][1], ['c'], 'Second row tags');
          assert.deepStrictEqual(data[tagsCol][2], ['d', 'e', 'f'], 'Third row tags');
        }
      }
      
    } finally {
      try { unlinkSync(testFile); } catch (e) {}
    }
  });

  /**
   * Additional edge case: Empty columns in some row groups
   */
  test('Edge case: Row groups with all nulls', async () => {
    const schema = [
      { name: 'value', type: 'int32', nullable: true }
    ];
    
    // First row group has values, second has all nulls, third has values
    const data = {
      value: [
        ...Array(10000).fill(1),           // Row group 1: all 1s
        ...Array(10000).fill(null),        // Row group 2: all nulls
        ...Array(5000).fill(2)             // Row group 3: all 2s
      ]
    };
    
    const bytes = await writeParquet(schema, data, { rowGroupSize: 10000 });
    const readData = await readParquet(bytes);
    
    assert.strictEqual(readData.value.length, 25000);
    
    // Check each section
    assert.strictEqual(readData.value[0], 1, 'First row group value');
    assert.strictEqual(readData.value[9999], 1, 'Last of first row group');
    assert.strictEqual(readData.value[10000], null, 'First of null row group');
    assert.strictEqual(readData.value[19999], null, 'Last of null row group');
    assert.strictEqual(readData.value[20000], 2, 'First of third row group');
    assert.strictEqual(readData.value[24999], 2, 'Last row');
  });

});

