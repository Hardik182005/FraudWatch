import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const appRoot = fileURLToPath(new URL('.', import.meta.url));
const repoRoot = fileURLToPath(new URL('../..', import.meta.url));
const pkg = (p: string) => fileURLToPath(new URL(`../../packages/${p}`, import.meta.url));

// Internal packages are resolved from source (the repo lives on a volume without symlink support,
// so npm workspace links are disabled; see docs/frontend.md).
export default defineConfig({
  root: appRoot,
  envDir: appRoot,
  plugins: [react()],
  resolve: {
    alias: [
      { find: /^@fraudwatch\/contracts\/react$/, replacement: pkg('contracts/src/react.ts') },
      { find: /^@fraudwatch\/contracts$/, replacement: pkg('contracts/src/index.ts') },
      { find: /^@fraudwatch\/ui$/, replacement: pkg('ui/src/index.ts') },
      { find: /^@fraudwatch\/ui\/styles\.css$/, replacement: pkg('ui/src/styles.css') },
    ],
    dedupe: ['react', 'react-dom', 'three'],
  },
  server: { port: 5174, strictPort: true, fs: { allow: [repoRoot] } },
  preview: { port: 4174, strictPort: true },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    sourcemap: false,
    chunkSizeWarningLimit: 1200,
  },
});
