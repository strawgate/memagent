import { defineConfig, mergeConfig } from "vitest/config";
import viteConfig from "./vite.config";

export default mergeConfig(
  viteConfig,
  defineConfig({
    test: {
      environment: "happy-dom",
      globals: true,
      setupFiles: ["./src/test/setup.ts"],
      coverage: {
        provider: "v8",
        reporter: ["text", "html", "lcov"],
        include: ["src/**/*.{ts,tsx}"],
        exclude: [
          "src/main.tsx",
          "src/test/**",
          "src/**/*.test.{ts,tsx}",
          "src/**/*.spec.{ts,tsx}",
        ],
        thresholds: {
          lines: 25,
          functions: 20,
          branches: 25,
          statements: 25,
        },
      },
      deps: {
        optimizer: {
          web: {
            include: ["@testing-library/preact"],
          },
        },
      },
    },
  })
);
