import { useEffect, useRef } from "preact/hooks";

export interface SparklineProps {
  /** Data points to render (most recent last). */
  values: number[];
  /** Width in CSS pixels. Default 60. */
  width?: number;
  /** Height in CSS pixels. Default 16. */
  height?: number;
  /** Line/fill color. Default "var(--accent)". */
  color?: string;
  /** If set, line turns this color when latest value exceeds threshold. */
  warnColor?: string;
  warnThreshold?: number;
  /** If set, line turns this color when latest value exceeds threshold. */
  errColor?: string;
  errThreshold?: number;
  /** "line" (default) or "area" (filled below the line). */
  style?: "line" | "area";
}

/**
 * Tiny inline sparkline chart rendered on a <canvas>.
 * Pure canvas — no charting library overhead.
 */
export function Sparkline({
  values,
  width = 60,
  height = 16,
  color = "var(--accent)",
  warnColor = "var(--warn)",
  warnThreshold,
  errColor = "var(--err)",
  errThreshold,
  style: renderStyle = "line",
}: SparklineProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const dpr = window.devicePixelRatio || 1;
    const w = width * dpr;
    const h = height * dpr;
    canvas.width = w;
    canvas.height = h;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.clearRect(0, 0, w, h);

    const pts = values;
    if (pts.length < 2) return;

    // Determine Y bounds with a small padding.
    let min = Infinity;
    let max = -Infinity;
    for (const v of pts) {
      if (v < min) min = v;
      if (v > max) max = v;
    }
    // Avoid flat line at zero — give a small range.
    if (max === min) {
      max = min + 1;
    }

    const pad = 1 * dpr; // 1px padding
    const drawH = h - pad * 2;
    const drawW = w - pad * 2;

    // Resolve CSS variable colors via computed style.
    const computedStyle = getComputedStyle(canvas);
    const resolve = (c: string) => {
      if (c.startsWith("var(")) {
        const prop = c.slice(4, -1).trim();
        return computedStyle.getPropertyValue(prop).trim() || "#3b82f6";
      }
      return c;
    };

    // Pick color based on thresholds.
    const latest = pts[pts.length - 1];
    let activeColor = resolve(color);
    if (errThreshold != null && latest >= errThreshold) {
      activeColor = resolve(errColor);
    } else if (warnThreshold != null && latest >= warnThreshold) {
      activeColor = resolve(warnColor);
    }

    // Map data to canvas coordinates.
    const toX = (i: number) => pad + (i / (pts.length - 1)) * drawW;
    const toY = (v: number) => pad + drawH - ((v - min) / (max - min)) * drawH;

    ctx.beginPath();
    ctx.moveTo(toX(0), toY(pts[0]));
    for (let i = 1; i < pts.length; i++) {
      ctx.lineTo(toX(i), toY(pts[i]));
    }

    if (renderStyle === "area") {
      // Close the path along the bottom for a filled area.
      ctx.lineTo(toX(pts.length - 1), h - pad);
      ctx.lineTo(toX(0), h - pad);
      ctx.closePath();
      ctx.globalAlpha = 0.25;
      ctx.fillStyle = activeColor;
      ctx.fill();
      ctx.globalAlpha = 1.0;
      // Re-draw the line on top.
      ctx.beginPath();
      ctx.moveTo(toX(0), toY(pts[0]));
      for (let i = 1; i < pts.length; i++) {
        ctx.lineTo(toX(i), toY(pts[i]));
      }
    }

    ctx.strokeStyle = activeColor;
    ctx.lineWidth = 1.5 * dpr;
    ctx.lineJoin = "round";
    ctx.lineCap = "round";
    ctx.stroke();
  }, [values, width, height, color, warnColor, warnThreshold, errColor, errThreshold, renderStyle]);

  return (
    <canvas
      ref={canvasRef}
      width={width}
      height={height}
      style={{
        width: `${width}px`,
        height: `${height}px`,
        verticalAlign: "middle",
        display: "inline-block",
      }}
      class="sparkline"
    />
  );
}
