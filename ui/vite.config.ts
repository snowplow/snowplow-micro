import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import checker from 'vite-plugin-checker'
import path from 'path'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    react(),
    checker({
      typescript: true,
    }),
  ],
  base: '/micro/ui',
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  build: {
    outDir: 'out/ui',
  },
  server: {
    proxy: {
      '/micro/events': {
        target: 'http://localhost:9090',
        changeOrigin: true,
        secure: false,
      },
      '/micro/timeline': {
        target: 'http://localhost:9090',
        changeOrigin: true,
        secure: false,
      },
      '/micro/columns': {
        target: 'http://localhost:9090',
        changeOrigin: true,
        secure: false,
      },
      '/micro/columnStats': {
        target: 'http://localhost:9090',
        changeOrigin: true,
        secure: false,
      },
      '/micro/reset': {
        target: 'http://localhost:9090',
        changeOrigin: true,
        secure: false,
      },
      '/micro/auth-config': {
        target: 'http://localhost:9090',
        changeOrigin: true,
        secure: false,
      },
    },
    allowedHosts: [".ngrok-free.app"]
  },
})
