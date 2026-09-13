import { fileURLToPath, URL } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The console talks to the gateway / broker / websocket services directly
// (see src/lib/config.ts). Vite only serves the app itself.
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  server: {
    port: 7100,
    strictPort: false,
  },
  build: {
    // recharts is inherently ~500KB minified; it ships as its own async-able
    // "charts" chunk, so raise the warning budget instead of hiding the split.
    chunkSizeWarningLimit: 600,
    rollupOptions: {
      output: {
        manualChunks: {
          react: ["react", "react-dom"],
          charts: ["recharts"],
        },
      },
    },
  },
});
