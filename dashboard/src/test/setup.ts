import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/preact";
import { afterEach, vi } from "vitest";

afterEach(cleanup);

// Stub requestAnimationFrame for happy-dom (it doesn't fire RAF automatically)
vi.stubGlobal("requestAnimationFrame", (cb: FrameRequestCallback) => {
  cb(performance.now());
  return 0;
});
vi.stubGlobal("cancelAnimationFrame", (_id: number) => {});

// Stub ResizeObserver for tests that use it
vi.stubGlobal(
  "ResizeObserver",
  class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
);
