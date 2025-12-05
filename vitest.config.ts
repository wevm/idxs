import path from 'node:path'
import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    alias: {
      idxs: path.resolve(import.meta.dirname, 'src'),
    },
    include: ['src/**/*.test.ts'],
    globals: true,
    testTimeout: 15_000,
  },
})
