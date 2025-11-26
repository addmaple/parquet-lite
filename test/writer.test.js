import { test, describe } from 'node:test';
import assert from 'node:assert';

// These tests will run once the WASM modules are built
// For now, we test the JS wrapper structure

describe('writer module', () => {
  test('exports expected functions', async () => {
    const writer = await import('../src/writer.js');
    
    assert.strictEqual(typeof writer.initWriter, 'function');
    assert.strictEqual(typeof writer.writeParquet, 'function');
    assert.strictEqual(typeof writer.getWriterVersion, 'function');
  });
});

describe('reader module', () => {
  test('exports expected functions', async () => {
    const reader = await import('../src/reader.js');
    
    assert.strictEqual(typeof reader.initReader, 'function');
    assert.strictEqual(typeof reader.readParquet, 'function');
    assert.strictEqual(typeof reader.readMetadata, 'function');
    assert.strictEqual(typeof reader.getReaderVersion, 'function');
  });
});

describe('main module', () => {
  test('re-exports all functions', async () => {
    const main = await import('../src/index.js');
    
    // Writer exports
    assert.strictEqual(typeof main.initWriter, 'function');
    assert.strictEqual(typeof main.writeParquet, 'function');
    assert.strictEqual(typeof main.getWriterVersion, 'function');
    
    // Reader exports
    assert.strictEqual(typeof main.initReader, 'function');
    assert.strictEqual(typeof main.readParquet, 'function');
    assert.strictEqual(typeof main.readMetadata, 'function');
    assert.strictEqual(typeof main.getReaderVersion, 'function');
  });
});


