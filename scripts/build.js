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
const files = ['index.js', 'writer.js', 'reader.js'];

for (const file of files) {
  copyFileSync(join(srcDir, file), join(dist, file));
}

// Remove .gitignore files created by wasm-pack (they prevent npm from including WASM files)
const wasmWriterGitignore = join(dist, 'wasm-writer', '.gitignore');
const wasmReaderGitignore = join(dist, 'wasm-reader', '.gitignore');
if (existsSync(wasmWriterGitignore)) unlinkSync(wasmWriterGitignore);
if (existsSync(wasmReaderGitignore)) unlinkSync(wasmReaderGitignore);

console.log('Build complete!');

