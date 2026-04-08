import { render, screen } from "@testing-library/preact";
import { BottleneckBadge } from "../components/BottleneckBadge";
import type { BottleneckData } from "../types";

describe("BottleneckBadge", () => {
  it("renders output-bound badge with red emoji", () => {
    const b: BottleneckData = { stage: "output", reason: "sink slow" };
    render(<BottleneckBadge b={b} />);
    const badge = screen.getByText(/output-bound/);
    expect(badge).toBeInTheDocument();
    expect(badge.textContent).toContain("🔴");
    expect(badge).toHaveClass("bottleneck-output");
  });

  it("renders input-bound badge with orange emoji", () => {
    const b: BottleneckData = { stage: "input", reason: "source slow" };
    render(<BottleneckBadge b={b} />);
    const badge = screen.getByText(/input-bound/);
    expect(badge).toBeInTheDocument();
    expect(badge.textContent).toContain("🟠");
    expect(badge).toHaveClass("bottleneck-input");
  });

  it("renders transform-bound badge", () => {
    const b: BottleneckData = { stage: "transform", reason: "SQL heavy" };
    render(<BottleneckBadge b={b} />);
    const badge = screen.getByText(/transform-bound/);
    expect(badge).toBeInTheDocument();
    expect(badge.textContent).toContain("🟡");
    expect(badge).toHaveClass("bottleneck-transform");
  });

  it("renders scan-bound badge", () => {
    const b: BottleneckData = { stage: "scan", reason: "slow scan" };
    render(<BottleneckBadge b={b} />);
    const badge = screen.getByText(/scan-bound/);
    expect(badge).toBeInTheDocument();
    expect(badge.textContent).toContain("🟡");
    expect(badge).toHaveClass("bottleneck-scan");
  });

  it("renders healthy/none badge with green emoji", () => {
    const b: BottleneckData = { stage: "none", reason: "all good" };
    render(<BottleneckBadge b={b} />);
    const badge = screen.getByText(/healthy/);
    expect(badge).toBeInTheDocument();
    expect(badge.textContent).toContain("🟢");
    expect(badge).toHaveClass("bottleneck-none");
  });

  it("falls back gracefully for unknown stage", () => {
    // Force an unknown stage via type cast
    const b = { stage: "warp_drive", reason: "warp core breach" } as unknown as BottleneckData;
    render(<BottleneckBadge b={b} />);
    // Falls back to "none" config → "healthy"
    const badge = screen.getByText(/healthy/);
    expect(badge).toBeInTheDocument();
    expect(badge.textContent).toContain("🟢");
  });

  it("shows reason as tooltip title", () => {
    const b: BottleneckData = { stage: "output", reason: "downstream backpressure" };
    render(<BottleneckBadge b={b} />);
    const badge = screen.getByText(/output-bound/);
    expect(badge).toHaveAttribute("title", "downstream backpressure");
  });
});
