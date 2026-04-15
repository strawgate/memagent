import preact from "@preact/preset-vite";
import { defineConfig } from "vite";
import { viteSingleFile } from "vite-plugin-singlefile";

export default defineConfig({
  plugins: [preact(), viteSingleFile()],
  build: {
    outDir: "../crates/logfwd-io/src/dashboard-dist",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/api": "http://127.0.0.1:9090",
      "/admin": "http://127.0.0.1:9090",
      "/live": "http://127.0.0.1:9090",
      "/ready": "http://127.0.0.1:9090",
    },
  },
});
