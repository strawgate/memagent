import { fireEvent, render, screen } from "@testing-library/preact";
import { Collapsible } from "../components/Collapsible";
import { DataLossIndicator } from "../components/DataLossIndicator";
import { Sparkline } from "../components/Sparkline";
import { SystemStrip } from "../components/SystemStrip";
import type { PipelineData } from "../types";

// ─── Mock TelemetryStore ─────────────────────────────────────────────────────
function makeMockStore(data: Record<string, number[]> = {}) {
  return {
    ingest: vi.fn(),
    selectTimeSeries: vi.fn(({ metricName }: { metricName: string }) => {
      const points = (data[metricName] ?? []).map((v, i) => ({
        timeMs: 1000 + i * 2000,
        value: v,
      }));
      return {
        series: points.length > 0 ? [{ key: "", points }] : [],
      };
    }),
    selectLatestValues: vi.fn(() => ({ rows: [] })),
  };
}

// ─── Test data helpers ───────────────────────────────────────────────────────
function makePipeline(overrides: Partial<PipelineData> = {}): PipelineData {
  return {
    name: "web-logs",
    inputs: [
      {
        name: "input_0",
        type: "file",
        lines_total: 100_000,
        bytes_total: 5_000_000,
        errors: 0,
      },
    ],
    transform: {
      sql: "SELECT * FROM logs",
      lines_in: 100_000,
      lines_out: 99_000,
      filter_drop_rate: 0.01,
    },
    outputs: [
      {
        name: "output_0",
        type: "otlp",
        lines_total: 99_000,
        bytes_total: 4_500_000,
        errors: 0,
      },
    ],
    ...overrides,
  };
}

// ─── Sparkline tests ─────────────────────────────────────────────────────────

describe("Sparkline", () => {
  it("renders a canvas element with correct dimensions", () => {
    const { container } = render(<Sparkline values={[1, 2, 3, 4]} width={80} height={20} />);
    const canvas = container.querySelector("canvas");
    expect(canvas).toBeTruthy();
    expect(canvas?.style.width).toBe("80px");
    expect(canvas?.style.height).toBe("20px");
  });

  it("renders with default dimensions when not specified", () => {
    const { container } = render(<Sparkline values={[1, 2, 3]} />);
    const canvas = container.querySelector("canvas");
    expect(canvas?.style.width).toBe("60px");
    expect(canvas?.style.height).toBe("16px");
  });

  it("applies sparkline CSS class", () => {
    const { container } = render(<Sparkline values={[1, 2]} />);
    const canvas = container.querySelector("canvas");
    expect(canvas?.classList.contains("sparkline")).toBe(true);
  });

  it("renders with empty values without crashing", () => {
    const { container } = render(<Sparkline values={[]} />);
    expect(container.querySelector("canvas")).toBeTruthy();
  });

  it("renders with single value without crashing", () => {
    const { container } = render(<Sparkline values={[42]} />);
    expect(container.querySelector("canvas")).toBeTruthy();
  });

  it("renders with all-zero values without crashing", () => {
    const { container } = render(<Sparkline values={[0, 0, 0, 0]} />);
    expect(container.querySelector("canvas")).toBeTruthy();
  });

  it("accepts area style", () => {
    const { container } = render(<Sparkline values={[1, 2, 3]} style="area" />);
    expect(container.querySelector("canvas")).toBeTruthy();
  });
});

// ─── DataLossIndicator tests ────────────────────────────────────────────────

describe("DataLossIndicator", () => {
  it("shows 0% loss when input equals output", () => {
    const p = makePipeline({
      transform: {
        sql: "",
        lines_in: 1000,
        lines_out: 1000,
        filter_drop_rate: 0,
      },
      outputs: [
        {
          name: "o",
          type: "otlp",
          lines_total: 1000,
          bytes_total: 500,
          errors: 0,
        },
      ],
    });
    render(<DataLossIndicator pipelines={[p]} />);
    expect(screen.getByText("0.000%")).toBeInTheDocument();
    expect(screen.getByText("data loss")).toBeInTheDocument();
  });

  it("shows green severity when no data loss", () => {
    const p = makePipeline({
      transform: {
        sql: "",
        lines_in: 1000,
        lines_out: 1000,
        filter_drop_rate: 0,
      },
      outputs: [
        {
          name: "o",
          type: "otlp",
          lines_total: 1000,
          bytes_total: 500,
          errors: 0,
        },
      ],
    });
    const { container } = render(<DataLossIndicator pipelines={[p]} />);
    const summary = container.querySelector(".data-loss-summary");
    expect(summary?.classList.contains("data-loss-ok")).toBe(true);
  });

  it("shows warning severity for loss > 0% but < 1%", () => {
    const p = makePipeline({
      transform: {
        sql: "",
        lines_in: 10_000,
        lines_out: 9_950,
        filter_drop_rate: 0.005,
      },
      outputs: [
        {
          name: "o",
          type: "otlp",
          lines_total: 9_950,
          bytes_total: 500,
          errors: 0,
        },
      ],
    });
    const { container } = render(<DataLossIndicator pipelines={[p]} />);
    const summary = container.querySelector(".data-loss-summary");
    expect(summary?.classList.contains("data-loss-warn")).toBe(true);
  });

  it("shows error severity for loss >= 1%", () => {
    const p = makePipeline({
      transform: {
        sql: "",
        lines_in: 10_000,
        lines_out: 9_800,
        filter_drop_rate: 0.02,
      },
      outputs: [
        {
          name: "o",
          type: "otlp",
          lines_total: 9_800,
          bytes_total: 500,
          errors: 0,
        },
      ],
    });
    const { container } = render(<DataLossIndicator pipelines={[p]} />);
    const summary = container.querySelector(".data-loss-summary");
    expect(summary?.classList.contains("data-loss-err")).toBe(true);
  });

  it("shows error count when outputs have errors", () => {
    const p = makePipeline({
      outputs: [
        {
          name: "o",
          type: "otlp",
          lines_total: 99_000,
          bytes_total: 500,
          errors: 42,
        },
      ],
    });
    render(<DataLossIndicator pipelines={[p]} />);
    expect(screen.getByText("42 errors")).toBeInTheDocument();
  });

  it("does not show errors text when there are none", () => {
    const p = makePipeline();
    render(<DataLossIndicator pipelines={[p]} />);
    expect(screen.queryByText(/errors/)).not.toBeInTheDocument();
  });

  it("expands to show per-pipeline breakdown on click", () => {
    const p = makePipeline();
    render(<DataLossIndicator pipelines={[p]} />);

    // Table should not be visible initially.
    expect(screen.queryByText("Pipeline")).not.toBeInTheDocument();

    // Click to expand.
    const btn = screen.getByRole("button");
    fireEvent.click(btn);

    // Per-pipeline table should now be visible.
    expect(screen.getByText("Pipeline")).toBeInTheDocument();
    expect(screen.getByText("web-logs")).toBeInTheDocument();
  });

  it("collapses table on second click", () => {
    const p = makePipeline();
    render(<DataLossIndicator pipelines={[p]} />);

    const btn = screen.getByRole("button");
    fireEvent.click(btn); // open
    expect(screen.getByText("Pipeline")).toBeInTheDocument();

    fireEvent.click(btn); // close
    expect(screen.queryByText("Pipeline")).not.toBeInTheDocument();
  });

  it("aggregates across multiple pipelines", () => {
    const p1 = makePipeline({
      name: "pipeline-a",
      transform: {
        sql: "",
        lines_in: 5000,
        lines_out: 4900,
        filter_drop_rate: 0.02,
      },
      outputs: [
        {
          name: "o",
          type: "otlp",
          lines_total: 4900,
          bytes_total: 100,
          errors: 0,
        },
      ],
    });
    const p2 = makePipeline({
      name: "pipeline-b",
      transform: {
        sql: "",
        lines_in: 5000,
        lines_out: 4900,
        filter_drop_rate: 0.02,
      },
      outputs: [
        {
          name: "o",
          type: "otlp",
          lines_total: 4900,
          bytes_total: 100,
          errors: 0,
        },
      ],
    });
    render(<DataLossIndicator pipelines={[p1, p2]} />);
    // 200 dropped out of 10000 = 2.0%
    expect(screen.getByText("2.0%")).toBeInTheDocument();
  });

  it("handles empty pipelines", () => {
    render(<DataLossIndicator pipelines={[]} />);
    expect(screen.getByText("0.000%")).toBeInTheDocument();
  });

  it("has correct aria-expanded attribute", () => {
    const p = makePipeline();
    render(<DataLossIndicator pipelines={[p]} />);
    const btn = screen.getByRole("button");
    expect(btn).toHaveAttribute("aria-expanded", "false");

    fireEvent.click(btn);
    expect(btn).toHaveAttribute("aria-expanded", "true");
  });
});

// ─── Collapsible tests ──────────────────────────────────────────────────────

describe("Collapsible", () => {
  it("renders title", () => {
    render(
      <Collapsible title="Logs">
        <p>log content</p>
      </Collapsible>
    );
    expect(screen.getByText("Logs")).toBeInTheDocument();
  });

  it("is collapsed by default (children not rendered)", () => {
    render(
      <Collapsible title="Logs">
        <p>log content</p>
      </Collapsible>
    );
    expect(screen.queryByText("log content")).not.toBeInTheDocument();
  });

  it("expands on click, showing children", () => {
    render(
      <Collapsible title="Logs">
        <p>log content</p>
      </Collapsible>
    );
    fireEvent.click(screen.getByText("Logs"));
    expect(screen.getByText("log content")).toBeInTheDocument();
  });

  it("collapses on second click, hiding children", () => {
    render(
      <Collapsible title="Logs">
        <p>log content</p>
      </Collapsible>
    );
    fireEvent.click(screen.getByText("Logs")); // open
    expect(screen.getByText("log content")).toBeInTheDocument();

    fireEvent.click(screen.getByText("Logs")); // close
    expect(screen.queryByText("log content")).not.toBeInTheDocument();
  });

  it("starts open when defaultOpen is true", () => {
    render(
      <Collapsible title="Config" defaultOpen>
        <p>config content</p>
      </Collapsible>
    );
    expect(screen.getByText("config content")).toBeInTheDocument();
  });

  it("renders optional badge", () => {
    render(
      <Collapsible title="Batch Monitor" badge={<span>5 recent</span>}>
        <p>traces</p>
      </Collapsible>
    );
    expect(screen.getByText("5 recent")).toBeInTheDocument();
  });

  it("has correct aria-expanded attribute", () => {
    render(
      <Collapsible title="Logs">
        <p>content</p>
      </Collapsible>
    );
    const btn = screen.getByRole("button");
    expect(btn).toHaveAttribute("aria-expanded", "false");

    fireEvent.click(btn);
    expect(btn).toHaveAttribute("aria-expanded", "true");
  });

  it("applies open CSS class when expanded", () => {
    const { container } = render(
      <Collapsible title="Logs">
        <p>content</p>
      </Collapsible>
    );
    const wrapper = container.querySelector(".collapsible");
    expect(wrapper?.classList.contains("open")).toBe(false);

    fireEvent.click(screen.getByText("Logs"));
    expect(wrapper?.classList.contains("open")).toBe(true);
  });
});

// ─── SystemStrip tests ──────────────────────────────────────────────────────

describe("SystemStrip", () => {
  it("renders CPU, Memory, Inflight, and Uptime labels", () => {
    const store = makeMockStore();
    render(<SystemStrip store={store as never} tick={0} uptimeSec={3600} />);
    expect(screen.getByText("CPU")).toBeInTheDocument();
    expect(screen.getByText("Inflight")).toBeInTheDocument();
    expect(screen.getByText("Uptime")).toBeInTheDocument();
  });

  it("displays formatted uptime", () => {
    const store = makeMockStore();
    render(<SystemStrip store={store as never} tick={0} uptimeSec={7261} />);
    expect(screen.getByText("2h 1m")).toBeInTheDocument();
  });

  it("shows CPU percentage from store", () => {
    const store = makeMockStore({
      "logfwd.cpu_percent": [2.5, 3.0, 3.2],
    });
    render(<SystemStrip store={store as never} tick={1} uptimeSec={100} />);
    // Latest value formatted as percentage.
    expect(screen.getByText("3.2%")).toBeInTheDocument();
  });

  it("shows memory value from allocated metric", () => {
    const store = makeMockStore({
      "logfwd.memory.allocated": [41943040, 42991616],
    });
    render(<SystemStrip store={store as never} tick={1} uptimeSec={100} />);
    // Label should be "Alloc" (not RSS) when allocated data is available.
    expect(screen.getByText("Alloc")).toBeInTheDocument();
  });

  it("falls back to RSS label when no allocated data", () => {
    const store = makeMockStore({
      "logfwd.memory.resident": [50_000_000],
    });
    render(<SystemStrip store={store as never} tick={1} uptimeSec={100} />);
    expect(screen.getByText("RSS")).toBeInTheDocument();
  });

  it("renders dividers between cells", () => {
    const store = makeMockStore();
    const { container } = render(<SystemStrip store={store as never} tick={0} uptimeSec={0} />);
    const dividers = container.querySelectorAll(".sys-divider");
    expect(dividers.length).toBe(3);
  });

  it("renders sparklines for CPU, Memory, and Inflight", () => {
    const store = makeMockStore({
      "logfwd.cpu_percent": [1, 2, 3],
      "process.memory.allocated": [100, 200],
      "logfwd.batch.inflight": [2, 3, 4],
    });
    const { container } = render(<SystemStrip store={store as never} tick={1} uptimeSec={100} />);
    // Three sparkline canvases (CPU, Memory, Inflight — Uptime has no sparkline).
    const canvases = container.querySelectorAll("canvas.sparkline");
    expect(canvases.length).toBe(3);
  });
});
