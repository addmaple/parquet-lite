import { writeParquet } from 'parquet-lite/writer';
import { readParquet, readMetadata } from 'parquet-lite/reader';

const statusEl = document.getElementById('status');
const outputEl = document.getElementById('output');

function log(message, type = 'info') {
  const status = document.createElement('div');
  status.className = `status ${type}`;
  status.textContent = message;
  statusEl.appendChild(status);
  
  outputEl.textContent += message + '\n';
  console.log(message);
}

async function testWriter() {
  try {
    log('Testing writer...', 'info');
    
    const schema = [
      { name: 'id', type: 'int32' },
      { name: 'name', type: 'string' },
      { name: 'score', type: 'double' }
    ];
    
    const data = {
      id: [1, 2, 3, 4, 5],
      name: ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
      score: [95.5, 87.3, 92.1, 88.9, 91.2]
    };
    
    const bytes = await writeParquet(schema, data, { compression: 'snappy' });
    
    log(`✅ Writer test passed! Generated ${bytes.length} bytes`, 'success');
    log(`   First 20 bytes: ${Array.from(bytes.slice(0, 20)).map(b => b.toString(16).padStart(2, '0')).join(' ')}`);
    
    // Test blob creation
    const blob = new Blob([bytes], { type: 'application/octet-stream' });
    log(`   Blob size: ${blob.size} bytes`);
    
    return bytes;
  } catch (error) {
    log(`❌ Writer test failed: ${error.message}`, 'error');
    console.error(error);
    throw error;
  }
}

async function testReader(bytes) {
  try {
    log('Testing reader...', 'info');
    
    const metadata = await readMetadata(bytes);
    log(`✅ Metadata read: ${metadata.num_rows} rows, ${metadata.num_row_groups} row groups`);
    log(`   Columns: ${metadata.columns.map(c => `${c.name} (${c.type})`).join(', ')}`);
    
    const data = await readParquet(bytes);
    log(`✅ Data read successfully`);
    log(`   Sample: id=${data.id.slice(0, 3).join(',')}, name=${data.name.slice(0, 3).join(',')}`);
    
    // Test column selection
    const partial = await readParquet(bytes, ['id', 'score']);
    log(`✅ Column selection works: ${Object.keys(partial).join(', ')}`);
    
    return data;
  } catch (error) {
    log(`❌ Reader test failed: ${error.message}`, 'error');
    console.error(error);
    throw error;
  }
}

async function testFullRoundtrip() {
  try {
    log('Testing full roundtrip (write → read)...', 'info');
    
    const schema = [
      { name: 'id', type: 'int32' },
      { name: 'name', type: 'string' },
      { name: 'active', type: 'boolean' },
      { name: 'score', type: 'double' }
    ];
    
    const originalData = {
      id: [1, 2, 3],
      name: ['Test1', 'Test2', 'Test3'],
      active: [true, false, true],
      score: [99.9, 88.8, 77.7]
    };
    
    const bytes = await writeParquet(schema, originalData);
    log(`   Written ${bytes.length} bytes`);
    
    const readData = await readParquet(bytes);
    
    // Verify data matches
    const idMatch = JSON.stringify(readData.id) === JSON.stringify(originalData.id);
    const nameMatch = JSON.stringify(readData.name) === JSON.stringify(originalData.name);
    const activeMatch = JSON.stringify(readData.active) === JSON.stringify(originalData.active);
    
    if (idMatch && nameMatch && activeMatch) {
      log('✅ Full roundtrip test passed! Data matches perfectly.', 'success');
    } else {
      log('❌ Data mismatch!', 'error');
      log(`   Original: ${JSON.stringify(originalData)}`);
      log(`   Read: ${JSON.stringify(readData)}`);
    }
  } catch (error) {
    log(`❌ Roundtrip test failed: ${error.message}`, 'error');
    console.error(error);
  }
}

// Setup button handlers
document.getElementById('test-writer').addEventListener('click', async () => {
  statusEl.innerHTML = '';
  outputEl.textContent = '';
  await testWriter();
});

document.getElementById('test-reader').addEventListener('click', async () => {
  statusEl.innerHTML = '';
  outputEl.textContent = '';
  const bytes = await testWriter();
  await testReader(bytes);
});

document.getElementById('test-full').addEventListener('click', async () => {
  statusEl.innerHTML = '';
  outputEl.textContent = '';
  await testFullRoundtrip();
});

// Auto-run on load
log('🚀 Parquet Lite Vite Test Ready');
log('   Click buttons above to test, or check console for details');


