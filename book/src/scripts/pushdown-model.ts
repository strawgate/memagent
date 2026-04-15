// ================================================================
// Pushdown explorer model — pure data and computation, no DOM.
// ================================================================

export interface Field {
  name: string;
  type: string;
  val: string;
  bytes: number;
  isNum: boolean;
}

export interface Preset {
  name: string;
  desc: string;
  fields: string[] | null;
}

export const FIELDS: Field[] = [
  { name: 'timestamp',    type: 'string', val: '"2026-04-14T10:23:45.123Z"', bytes: 30, isNum: false },
  { name: 'level',        type: 'string', val: '"INFO"',                     bytes: 6,  isNum: false },
  { name: 'message',      type: 'string', val: '"GET /api/users completed"', bytes: 32, isNum: false },
  { name: 'logger',       type: 'string', val: '"http.server"',              bytes: 14, isNum: false },
  { name: 'thread',       type: 'string', val: '"worker-7"',                 bytes: 10, isNum: false },
  { name: 'trace_id',     type: 'string', val: '"a1b2c3d4e5f6..."',          bytes: 36, isNum: false },
  { name: 'span_id',      type: 'string', val: '"f6e5d4c3..."',              bytes: 18, isNum: false },
  { name: 'service',      type: 'string', val: '"api-gateway"',              bytes: 14, isNum: false },
  { name: 'host',         type: 'string', val: '"prod-us-east-2a"',          bytes: 18, isNum: false },
  { name: 'pid',          type: 'int',    val: '48291',                      bytes: 6,  isNum: true  },
  { name: 'method',       type: 'string', val: '"GET"',                      bytes: 5,  isNum: false },
  { name: 'path',         type: 'string', val: '"/api/users?page=2"',        bytes: 22, isNum: false },
  { name: 'status',       type: 'int',    val: '200',                        bytes: 4,  isNum: true  },
  { name: 'duration_ms',  type: 'float',  val: '12.4',                       bytes: 5,  isNum: true  },
  { name: 'bytes_sent',   type: 'int',    val: '8452',                       bytes: 5,  isNum: true  },
  { name: 'user_agent',   type: 'string', val: '"Mozilla/5.0 (Mac...)"',     bytes: 28, isNum: false },
  { name: 'referer',      type: 'string', val: '"https://app.example..."',   bytes: 30, isNum: false },
  { name: 'x_request_id', type: 'string', val: '"req-9f8e7d6c..."',          bytes: 20, isNum: false },
  { name: 'datacenter',   type: 'string', val: '"us-east-2"',                bytes: 12, isNum: false },
  { name: 'version',      type: 'string', val: '"v2.4.1"',                   bytes: 9,  isNum: false },
];

export const TOTAL_FIELDS = FIELDS.length;
export const TOTAL_BYTES = FIELDS.reduce((sum, f) => sum + f.bytes, 0);

export const PRESETS: Preset[] = [
  { name: 'SELECT *',          desc: 'All 20 fields',                                                                        fields: null },
  { name: 'Error triage',      desc: 'level, message, status, path (4 fields)',                                              fields: ['level', 'message', 'status', 'path'] },
  { name: 'Latency analysis',  desc: 'method, path, status, duration_ms, bytes_sent, service (6 fields)',                    fields: ['method', 'path', 'status', 'duration_ms', 'bytes_sent', 'service'] },
  { name: 'Trace correlation', desc: 'trace_id, span_id, service, host, level, message, status, duration_ms, timestamp (9 fields)', fields: ['trace_id', 'span_id', 'service', 'host', 'level', 'message', 'status', 'duration_ms', 'timestamp'] },
  { name: 'Minimal',           desc: 'level only (1 field)',                                                                  fields: ['level'] },
];

// Throughput model — calibrated to published benchmarks.
// Known data points: (1, 3.8), (2, 3.4), (3, 3.4), (6, 2.0), (20, 0.56)
const THROUGHPUT_POINTS: [number, number][] = [
  [1, 3.8], [2, 3.4], [3, 3.4], [6, 2.0], [20, 0.56],
];
export const MAX_THROUGHPUT = 3.8;

export function estimateThroughput(n: number): number {
  if (n === 0) return MAX_THROUGHPUT;
  if (n <= THROUGHPUT_POINTS[0][0]) return THROUGHPUT_POINTS[0][1];
  if (n >= THROUGHPUT_POINTS[THROUGHPUT_POINTS.length - 1][0]) {
    return THROUGHPUT_POINTS[THROUGHPUT_POINTS.length - 1][1];
  }
  for (let i = 0; i < THROUGHPUT_POINTS.length - 1; i++) {
    const lo = THROUGHPUT_POINTS[i], hi = THROUGHPUT_POINTS[i + 1];
    if (n >= lo[0] && n <= hi[0]) {
      const frac = (n - lo[0]) / (hi[0] - lo[0]);
      return lo[1] + frac * (hi[1] - lo[1]);
    }
  }
  return THROUGHPUT_POINTS[THROUGHPUT_POINTS.length - 1][1];
}

export const BASE_THROUGHPUT = estimateThroughput(TOTAL_FIELDS);

/** Count how many fields are explicitly selected (0 = SELECT * mode). */
export function countSelected(selected: Record<string, boolean>): number {
  return Object.values(selected).filter(Boolean).length;
}

export interface RenderData {
  effectiveCount: number;
  throughput: number;
  speedup: number;
  pctOfMax: number;
  skipped: number;
  selectedBytes: number;
}

/** Derive all render-time statistics from the current selection. */
export function computeRenderData(selected: Record<string, boolean>): RenderData {
  const selCount = countSelected(selected);
  const selectAll = selCount === 0;
  const effectiveCount = selectAll ? TOTAL_FIELDS : selCount;
  const throughput = selectAll ? BASE_THROUGHPUT : estimateThroughput(selCount);
  const speedup = throughput / BASE_THROUGHPUT;
  const pctOfMax = (throughput / MAX_THROUGHPUT) * 100;
  const skipped = selectAll ? 0 : TOTAL_FIELDS - selCount;
  let selectedBytes = 0;
  for (const f of FIELDS) {
    if (selectAll || selected[f.name]) selectedBytes += f.bytes;
  }
  return { effectiveCount, throughput, speedup, pctOfMax, skipped, selectedBytes };
}

/** Return true if the current selection exactly matches a preset. */
export function isPresetMatch(preset: Preset, selected: Record<string, boolean>): boolean {
  const selCount = countSelected(selected);
  const selectAll = selCount === 0;
  if (preset.fields === null) return selectAll;
  if (selectAll || selCount !== preset.fields.length) return false;
  return preset.fields.every(f => selected[f]);
}

/** Return a new selection state with the named field toggled.
 *  If called in SELECT * mode (nothing selected), all fields are first made
 *  explicit, then the clicked field is deselected. */
export function toggleField(name: string, selected: Record<string, boolean>): Record<string, boolean> {
  const selCount = countSelected(selected);
  const result: Record<string, boolean> = selCount === 0
    ? Object.fromEntries(FIELDS.map(f => [f.name, true]))
    : { ...selected };
  result[name] = !result[name];
  return result;
}

/** Return a new selection state for a given preset. */
export function applyPreset(preset: Preset): Record<string, boolean> {
  const result: Record<string, boolean> = {};
  if (preset.fields) {
    for (const f of preset.fields) result[f] = true;
  }
  return result;
}
