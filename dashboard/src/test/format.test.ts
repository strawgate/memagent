import { test as fcTest } from "@fast-check/vitest";
import fc from "fast-check";
import { fmt, fmtBytes, fmtBytesCompact, fmtCompact, fmtDuration } from "../lib/format";

// ─── fmt ─────────────────────────────────────────────────────────────────────

describe("fmt", () => {
  it("returns '-' for null", () => {
    expect(fmt(null)).toBe("-");
  });

  it("returns '-' for undefined", () => {
    expect(fmt(undefined)).toBe("-");
  });

  it("returns '0' for zero", () => {
    expect(fmt(0)).toBe("0");
  });

  describe("sub-1000 values are rounded integers", () => {
    it("returns '1' for 1", () => expect(fmt(1)).toBe("1"));
    it("returns '999' for 999", () => expect(fmt(999)).toBe("999"));
    it("returns '500' for 500.4 (rounds down)", () => expect(fmt(500.4)).toBe("500"));
    it("returns '501' for 500.6 (rounds up)", () => expect(fmt(500.6)).toBe("501"));
  });

  describe("K suffix at 1,000 threshold", () => {
    it("returns '999' for 999 (still integer range)", () => expect(fmt(999)).toBe("999"));
    it("returns '1.0K' for 1000", () => expect(fmt(1000)).toBe("1.0K"));
    it("returns '1.0K' for 1001 (toFixed(1) truncates)", () => expect(fmt(1001)).toBe("1.0K"));
    it("returns '1.5K' for 1500", () => expect(fmt(1500)).toBe("1.5K"));
    it("returns '999.9K' for 999_900", () => expect(fmt(999_900)).toBe("999.9K"));
  });

  describe("M suffix at 1,000,000 threshold", () => {
    it("returns '1.0M' for 1_000_000", () => expect(fmt(1_000_000)).toBe("1.0M"));
    it("returns '2.5M' for 2_500_000", () => expect(fmt(2_500_000)).toBe("2.5M"));
    it("returns '999.9M' for 999_900_000", () => expect(fmt(999_900_000)).toBe("999.9M"));
  });

  describe("G suffix at 1,000,000,000 threshold", () => {
    it("returns '1.0G' for 1e9", () => expect(fmt(1e9)).toBe("1.0G"));
    it("returns '5.0G' for 5e9", () => expect(fmt(5e9)).toBe("5.0G"));
    it("handles very large values", () => expect(fmt(1e15)).toBe("1000000.0G"));
  });

  describe("negative values", () => {
    it("returns '-1' for -1", () => expect(fmt(-1)).toBe("-1"));
    it("returns '-999' for -999", () => expect(fmt(-999)).toBe("-999"));
    // negative values below -1000 still use integer path because thresholds are checked >= 1e3
    it("returns '-1000' for -1000 (Math.round path)", () => expect(fmt(-1000)).toBe("-1000"));
  });

  describe("property-based: result is always a non-empty string for finite inputs", () => {
    fcTest.prop([fc.float({ noNaN: true, noDefaultInfinity: true })])(
      "fmt of any finite float is a non-empty string",
      (n) => {
        const result = fmt(n);
        expect(typeof result).toBe("string");
        expect(result.length).toBeGreaterThan(0);
      }
    );
  });
});

// ─── fmtCompact ──────────────────────────────────────────────────────────────

describe("fmtCompact", () => {
  it("returns '0' for values below 0.01", () => {
    expect(fmtCompact(0)).toBe("0");
    expect(fmtCompact(0.001)).toBe("0");
    expect(fmtCompact(0.009)).toBe("0");
  });

  it("returns one decimal place for values in [0.01, 1)", () => {
    expect(fmtCompact(0.01)).toBe("0.0");
    expect(fmtCompact(0.5)).toBe("0.5");
    expect(fmtCompact(0.99)).toBe("1.0");
  });

  it("returns integer string for values in [1, 1000)", () => {
    expect(fmtCompact(1)).toBe("1");
    expect(fmtCompact(42)).toBe("42");
    expect(fmtCompact(999)).toBe("999");
    expect(fmtCompact(999.9)).toBe("1000");
  });

  describe("K suffix at 1000 threshold", () => {
    it("returns '1K' for 1000", () => expect(fmtCompact(1000)).toBe("1K"));
    it("returns '1K' for 1499 (toFixed(0) rounds)", () => expect(fmtCompact(1499)).toBe("1K"));
    it("returns '2K' for 1500", () => expect(fmtCompact(1500)).toBe("2K"));
    it("returns '999K' for 999_000", () => expect(fmtCompact(999_000)).toBe("999K"));
  });

  describe("M suffix at 1,000,000 threshold", () => {
    it("returns '1.0M' for 1_000_000", () => expect(fmtCompact(1_000_000)).toBe("1.0M"));
    it("returns '2.5M' for 2_500_000", () => expect(fmtCompact(2_500_000)).toBe("2.5M"));
  });

  describe("property-based: result is always a non-empty string for finite non-negative inputs", () => {
    fcTest.prop([fc.float({ min: 0, noNaN: true, noDefaultInfinity: true })])(
      "fmtCompact of any finite non-negative float is a non-empty string",
      (n) => {
        const result = fmtCompact(n);
        expect(typeof result).toBe("string");
        expect(result.length).toBeGreaterThan(0);
      }
    );
  });
});

// ─── fmtBytes ────────────────────────────────────────────────────────────────

describe("fmtBytes", () => {
  it("returns '-' for null", () => expect(fmtBytes(null)).toBe("-"));
  it("returns '-' for undefined", () => expect(fmtBytes(undefined)).toBe("-"));
  it("returns '-' for 0 (no bytes to show)", () => expect(fmtBytes(0)).toBe("-"));

  describe("bytes range (< 1024)", () => {
    it("returns '1 B' for 1", () => expect(fmtBytes(1)).toBe("1 B"));
    it("returns '512 B' for 512", () => expect(fmtBytes(512)).toBe("512 B"));
    it("returns '1023 B' for 1023", () => expect(fmtBytes(1023)).toBe("1023 B"));
  });

  describe("KB suffix at 1024 threshold", () => {
    it("returns '1.0 KB' for 1024", () => expect(fmtBytes(1024)).toBe("1.0 KB"));
    it("returns '1.5 KB' for 1536", () => expect(fmtBytes(1536)).toBe("1.5 KB"));
    it("returns '1023.9 KB' for 1_047_552 (just under 1 MB)", () =>
      expect(fmtBytes(1_047_552)).toBe("1023.0 KB"));
  });

  describe("MB suffix at 1,048,576 threshold", () => {
    it("returns '1.0 MB' for 1_048_576", () => expect(fmtBytes(1_048_576)).toBe("1.0 MB"));
    it("returns '2.0 MB' for 2_097_152", () => expect(fmtBytes(2_097_152)).toBe("2.0 MB"));
    it("returns '512.0 MB' for 536_870_912", () => expect(fmtBytes(536_870_912)).toBe("512.0 MB"));
  });

  describe("GB suffix at 1,073,741,824 threshold", () => {
    it("returns '1.0 GB' for 1_073_741_824", () => expect(fmtBytes(1_073_741_824)).toBe("1.0 GB"));
    it("returns '4.0 GB' for 4_294_967_296", () => expect(fmtBytes(4_294_967_296)).toBe("4.0 GB"));
  });

  describe("property-based: result is always a non-empty string for positive finite inputs", () => {
    fcTest.prop([fc.integer({ min: 1, max: 1e12 })])(
      "fmtBytes of any positive integer is a non-empty string",
      (n) => {
        const result = fmtBytes(n);
        expect(typeof result).toBe("string");
        expect(result.length).toBeGreaterThan(0);
      }
    );
  });
});

// ─── fmtBytesCompact ─────────────────────────────────────────────────────────

describe("fmtBytesCompact", () => {
  it("returns '0B' for 0", () => expect(fmtBytesCompact(0)).toBe("0B"));
  it("returns '1B' for 1", () => expect(fmtBytesCompact(1)).toBe("1B"));
  it("returns '1023B' for 1023", () => expect(fmtBytesCompact(1023)).toBe("1023B"));

  describe("K suffix at 1024 threshold", () => {
    it("returns '1K' for 1024", () => expect(fmtBytesCompact(1024)).toBe("1K"));
    it("returns '2K' for 2048", () => expect(fmtBytesCompact(2048)).toBe("2K"));
    // toFixed(0) means 1535 → 1K (rounds down), 1536 → 2K (rounds up)
    it("rounds to nearest K", () => {
      expect(fmtBytesCompact(1535)).toBe("1K");
      expect(fmtBytesCompact(1536)).toBe("2K");
    });
  });

  describe("M suffix at 1_048_576 threshold", () => {
    it("returns '1M' for 1_048_576", () => expect(fmtBytesCompact(1_048_576)).toBe("1M"));
    it("returns '10M' for 10_485_760", () => expect(fmtBytesCompact(10_485_760)).toBe("10M"));
  });

  describe("G suffix at 1_073_741_824 threshold", () => {
    it("returns '1.0G' for 1_073_741_824", () =>
      expect(fmtBytesCompact(1_073_741_824)).toBe("1.0G"));
    it("returns '2.5G' for 2_684_354_560", () =>
      expect(fmtBytesCompact(2_684_354_560)).toBe("2.5G"));
  });

  describe("property-based: result is always a non-empty string for non-negative finite inputs", () => {
    fcTest.prop([fc.integer({ min: 0, max: 1e12 })])(
      "fmtBytesCompact of any non-negative integer is a non-empty string",
      (n) => {
        const result = fmtBytesCompact(n);
        expect(typeof result).toBe("string");
        expect(result.length).toBeGreaterThan(0);
      }
    );
  });
});

// ─── fmtDuration ─────────────────────────────────────────────────────────────

describe("fmtDuration", () => {
  it("returns '' for null", () => expect(fmtDuration(null)).toBe(""));
  it("returns '' for undefined", () => expect(fmtDuration(undefined)).toBe(""));

  it("returns seconds string for values < 60", () => {
    expect(fmtDuration(0)).toBe("0s");
    expect(fmtDuration(1)).toBe("1s");
    expect(fmtDuration(59)).toBe("59s");
  });

  describe("minute range [60, 3600)", () => {
    it("returns '1m 0s' for 60", () => expect(fmtDuration(60)).toBe("1m 0s"));
    it("returns '1m 30s' for 90", () => expect(fmtDuration(90)).toBe("1m 30s"));
    it("returns '59m 59s' for 3599", () => expect(fmtDuration(3599)).toBe("59m 59s"));
  });

  describe("hour range [3600, 86400)", () => {
    it("returns '1h 0m' for 3600", () => expect(fmtDuration(3600)).toBe("1h 0m"));
    it("returns '1h 30m' for 5400", () => expect(fmtDuration(5400)).toBe("1h 30m"));
    it("returns '23h 59m' for 86399", () => expect(fmtDuration(86399)).toBe("23h 59m"));
  });

  describe("day range [86400, ...)", () => {
    it("returns '1d 0h' for 86400", () => expect(fmtDuration(86400)).toBe("1d 0h"));
    it("returns '1d 12h' for 129600", () => expect(fmtDuration(129600)).toBe("1d 12h"));
    it("returns '7d 0h' for 604800", () => expect(fmtDuration(604800)).toBe("7d 0h"));
  });

  describe("property-based: result is always a string for finite non-negative inputs", () => {
    fcTest.prop([fc.integer({ min: 0, max: 60 * 60 * 24 * 365 })])(
      "fmtDuration of any non-negative integer seconds is a non-empty string",
      (s) => {
        const result = fmtDuration(s);
        expect(typeof result).toBe("string");
        expect(result.length).toBeGreaterThan(0);
      }
    );
  });
});
