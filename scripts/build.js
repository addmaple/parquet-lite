import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, '..');
const dist = join(root, 'dist');

// Ensure dist exists
mkdirSync(dist, { recursive: true });

// Copy JS source files
const srcDir = join(root, 'src');
const files = ['index.js', 'writer.js', 'reader.js'];

for (const file of files) {
  copyFileSync(join(srcDir, file), join(dist, file));
}

console.log('Build complete!');

