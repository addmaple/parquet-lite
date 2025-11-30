import { copyFileSync, mkdirSync, readFileSync, writeFileSync, unlinkSync, existsSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, '..');
const dist = join(root, 'dist');

// Ensure dist exists
mkdirSync(dist, { recursive: true });

// Copy JS source files
const srcDir = join(root, 'src');
const files = ['index.js', 'writer.js', 'reader.js', 'reader-lite.js'];

for (const file of files) {
  const srcPath = join(srcDir, file);
  if (existsSync(srcPath)) {
    copyFileSync(srcPath, join(dist, file));
  }
}

// Remove .gitignore files created by wasm-pack (they prevent npm from including WASM files)
const gitignoreFiles = [
  'wasm-writer/.gitignore',
  'wasm-reader/.gitignore',
  'wasm-reader-lite/.gitignore',
  'wasm-full/.gitignore'
];

for (const file of gitignoreFiles) {
  const path = join(dist, file);
  if (existsSync(path)) unlinkSync(path);
}

console.log('Build complete!');
