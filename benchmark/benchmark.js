import { writeFileSync, readFileSync, unlinkSync } from 'fs';
import { performance } from 'perf_hooks';
import { writeParquet } from '../dist/writer.js';
import { readParquet } from '../dist/reader.js';

// Import parquetjs (CommonJS)
let parquetjs;
try {
  const parquetjsModule = await import('parquetjs');
  // parquetjs is CommonJS, so we need to use createRequire
  const { createRequire } = await import('module');
  const require = createRequire(import.meta.url);
  parquetjs = require('parquetjs');
} catch (err) {
  console.error('Failed to import parquetjs. Install it with: npm install --save-dev parquetjs');
  console.error(err);
  process.exit(1);
}

// Test data
const generateTestData = (numRows) => {
  const data = {
    id: Array.from({ length: numRows }, (_, i) => i),
    name: Array.from({ length: numRows }, (_, i) => `User${i}`),
    score: Array.from({ length: numRows }, (_, i) => Math.random() * 100),
    active: Array.from({ length: numRows }, (_, i) => i % 2 === 0),
    timestamp: Array.from({ length: numRows }, (_, i) => Date.now() + i * 1000),
  };
  return data;
};

const schema = [
  { name: 'id', type: 'int32' },
  { name: 'name', type: 'string' },
  { name: 'score', type: 'double' },
  { name: 'active', type: 'boolean' },
  { name: 'timestamp', type: 'int64' },
];

// Benchmark function
const benchmark = async (name, fn) => {
  // Force garbage collection if available
  if (global.gc) {
    global.gc();
  }
  
  const start = performance.now();
  const startMemory = process.memoryUsage().heapUsed;
  const result = await fn();
  const end = performance.now();
  const endMemory = process.memoryUsage().heapUsed;
  
  // Force garbage collection again
  if (global.gc) {
    global.gc();
  }
  
  return {
    name,
    time: end - start,
    memory: Math.max(0, endMemory - startMemory),
    result,
  };
};

// Test parquet-lite writer
const testParquetLiteWrite = async (data, filename) => {
  const bytes = await writeParquet(schema, data, { compression: 'snappy' });
  writeFileSync(filename, bytes);
  return bytes.length;
};

// Test parquetjs writer
const testParquetjsWrite = async (data, filename) => {
  const { ParquetSchema, ParquetWriter } = parquetjs;
  const parquetSchema = new ParquetSchema({
    id: { type: 'INT32' },
    name: { type: 'UTF8' },
    score: { type: 'DOUBLE' },
    active: { type: 'BOOLEAN' },
    timestamp: { type: 'INT64' },
  });

  const writer = await ParquetWriter.openFile(parquetSchema, filename);
  writer.setRowGroupSize(10000);
  
  for (let i = 0; i < data.id.length; i++) {
    await writer.appendRow({
      id: data.id[i],
      name: data.name[i],
      score: data.score[i],
      active: data.active[i],
      timestamp: data.timestamp[i],
    });
  }
  
  await writer.close();
  const { statSync } = await import('fs');
  const stats = statSync(filename);
  return stats.size;
};

// Test parquet-lite reader
const testParquetLiteRead = async (filename) => {
  const bytes = readFileSync(filename);
  const data = await readParquet(bytes);
  return Object.keys(data).length;
};

// Test parquetjs reader
const testParquetjsRead = async (filename) => {
  const { ParquetReader } = parquetjs;
  const reader = await ParquetReader.openFile(filename);
  const cursor = reader.getCursor();
  let count = 0;
  let row;
  while ((row = await cursor.next())) {
    count++;
  }
  await reader.close();
  return count;
};

// Format bytes
const formatBytes = (bytes) => {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
};

// Format time
const formatTime = (ms) => {
  if (ms < 1) return (ms * 1000).toFixed(2) + ' μs';
  if (ms < 1000) return ms.toFixed(2) + ' ms';
  return (ms / 1000).toFixed(2) + ' s';
};

// Run benchmarks
const runBenchmarks = async () => {
  const testSizes = [1000, 10000, 100000];
  
  console.log('🚀 Parquet Performance Benchmark\n');
  console.log('=' .repeat(80));
  
  for (const numRows of testSizes) {
    console.log(`\n📊 Testing with ${numRows.toLocaleString()} rows\n`);
    const data = generateTestData(numRows);
    
    // Write benchmarks
    console.log('📝 WRITE PERFORMANCE');
    console.log('-'.repeat(80));
    
    const liteWriteFile = `benchmark-lite-${numRows}.parquet`;
    const jsWriteFile = `benchmark-js-${numRows}.parquet`;
    
    const liteWrite = await benchmark('@addmaple/parquet-lite', () => 
      testParquetLiteWrite(data, liteWriteFile)
    );
    
    const jsWrite = await benchmark('parquetjs', () => 
      testParquetjsWrite(data, jsWriteFile)
    );
    
    console.log(`@addmaple/parquet-lite:`);
    console.log(`  Time:     ${formatTime(liteWrite.time)}`);
    console.log(`  Memory:   ${formatBytes(liteWrite.memory)}`);
    console.log(`  File:     ${formatBytes(liteWrite.result)}`);
    
    console.log(`\nparquetjs:`);
    console.log(`  Time:     ${formatTime(jsWrite.time)}`);
    console.log(`  Memory:   ${formatBytes(jsWrite.memory)}`);
    console.log(`  File:     ${formatBytes(jsWrite.result)}`);
    
    const writeSpeedup = jsWrite.time / liteWrite.time;
    console.log(`\n⚡ Speedup: ${writeSpeedup.toFixed(2)}x ${writeSpeedup > 1 ? 'faster' : 'slower'}`);
    
    // Read benchmarks
    console.log('\n📖 READ PERFORMANCE');
    console.log('-'.repeat(80));
    
    const liteRead = await benchmark('@addmaple/parquet-lite', () => 
      testParquetLiteRead(liteWriteFile)
    );
    
    const jsRead = await benchmark('parquetjs', () => 
      testParquetjsRead(jsWriteFile)
    );
    
    console.log(`@addmaple/parquet-lite:`);
    console.log(`  Time:     ${formatTime(liteRead.time)}`);
    console.log(`  Memory:   ${formatBytes(liteRead.memory)}`);
    console.log(`  Columns:  ${liteRead.result}`);
    
    console.log(`\nparquetjs:`);
    console.log(`  Time:     ${formatTime(jsRead.time)}`);
    console.log(`  Memory:   ${formatBytes(jsRead.memory)}`);
    console.log(`  Rows:     ${jsRead.result}`);
    
    const readSpeedup = jsRead.time / liteRead.time;
    console.log(`\n⚡ Speedup: ${readSpeedup.toFixed(2)}x ${readSpeedup > 1 ? 'faster' : 'slower'}`);
    
    // Cleanup
    try {
      unlinkSync(liteWriteFile);
      unlinkSync(jsWriteFile);
    } catch (err) {
      // Ignore cleanup errors
    }
  }
  
  console.log('\n' + '='.repeat(80));
  console.log('\n✅ Benchmark complete!');
};

// Run
runBenchmarks().catch(console.error);

