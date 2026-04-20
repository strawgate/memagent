const ROUTE_DEFS = {
  ramp: {
    type: 'cubic',
    p0: { x: 30, y: 320 },
    p1: { x: 30, y: 295 },
    p2: { x: 180, y: 100 },
    p3: { x: 220, y: 100 },
  },
  highway: {
    type: 'line',
    p0: { x: 30, y: 100 },
    p1: { x: 560, y: 100 },
  },
  exit: {
    type: 'cubic',
    p0: { x: 560, y: 100 },
    p1: { x: 600, y: 100 },
    p2: { x: 730, y: 225 },
    p3: { x: 730, y: 320 },
  },
  cont: {
    type: 'line',
    p0: { x: 560, y: 100 },
    p1: { x: 775, y: 100 },
  },
};

function lerp(a, b, t) {
  return a + (b - a) * t;
}

function cubicPoint(def, t) {
  const mt = 1 - t;
  const mt2 = mt * mt;
  const t2 = t * t;
  return {
    x: mt2 * mt * def.p0.x + 3 * mt2 * t * def.p1.x + 3 * mt * t2 * def.p2.x + t2 * t * def.p3.x,
    y: mt2 * mt * def.p0.y + 3 * mt2 * t * def.p1.y + 3 * mt * t2 * def.p2.y + t2 * t * def.p3.y,
  };
}

function cubicDerivative(def, t) {
  const mt = 1 - t;
  return {
    x: 3 * mt * mt * (def.p1.x - def.p0.x) + 6 * mt * t * (def.p2.x - def.p1.x) + 3 * t * t * (def.p3.x - def.p2.x),
    y: 3 * mt * mt * (def.p1.y - def.p0.y) + 6 * mt * t * (def.p2.y - def.p1.y) + 3 * t * t * (def.p3.y - def.p2.y),
  };
}

function linePoint(def, t) {
  return {
    x: lerp(def.p0.x, def.p1.x, t),
    y: lerp(def.p0.y, def.p1.y, t),
  };
}

function lineDerivative(def) {
  return {
    x: def.p1.x - def.p0.x,
    y: def.p1.y - def.p0.y,
  };
}

function samplePoint(def, t) {
  return def.type === 'line' ? linePoint(def, t) : cubicPoint(def, t);
}

function sampleDerivative(def, t) {
  return def.type === 'line' ? lineDerivative(def) : cubicDerivative(def, t);
}

function buildLut(def) {
  if (def.type === 'line') {
    const dx = def.p1.x - def.p0.x;
    const dy = def.p1.y - def.p0.y;
    const len = Math.sqrt(dx * dx + dy * dy);
    return {
      length: len,
      samples: [
        { t: 0, len: 0 },
        { t: 1, len: len },
      ],
    };
  }

  const samples = [{ t: 0, len: 0 }];
  let prev = cubicPoint(def, 0);
  let total = 0;
  for (let i = 1; i <= 120; i++) {
    const t = i / 120;
    const p = cubicPoint(def, t);
    const dx = p.x - prev.x;
    const dy = p.y - prev.y;
    total += Math.sqrt(dx * dx + dy * dy);
    samples.push({ t, len: total });
    prev = p;
  }
  return { length: total, samples };
}

const LUTS = {};
for (const key in ROUTE_DEFS) {
  LUTS[key] = buildLut(ROUTE_DEFS[key]);
}

function tAtLength(route, s) {
  const lut = LUTS[route];
  const clamped = Math.min(Math.max(0, s), lut.length);
  if (clamped <= 0) return 0;
  if (clamped >= lut.length) return 1;

  let lo = 0;
  let hi = lut.samples.length - 1;
  while (lo + 1 < hi) {
    const mid = (lo + hi) >> 1;
    if (lut.samples[mid].len < clamped) lo = mid;
    else hi = mid;
  }

  const a = lut.samples[lo];
  const b = lut.samples[hi];
  const frac = (clamped - a.len) / Math.max(0.0001, b.len - a.len);
  return lerp(a.t, b.t, frac);
}

export const PATHS = {
  ramp: 'M 30,320 C 30,295 180,100 220,100',
  highway: 'M 30,100 L 560,100',
  exit: 'M 560,100 C 600,100 730,225 730,320',
  cont: 'M 560,100 L 775,100',
};

export const LENGTHS = {
  ramp: LUTS.ramp.length,
  highway: LUTS.highway.length,
  exit: LUTS.exit.length,
  cont: LUTS.cont.length,
};

export const MERGE_POINT = { x: 220, y: 100 };
export const FORK_POINT = { x: 560, y: 100 };
export const MERGE_S = MERGE_POINT.x - ROUTE_DEFS.highway.p0.x;
export const EXIT_GATE_S = LENGTHS.exit * 0.48;

export function pointAt(route, s) {
  return samplePoint(ROUTE_DEFS[route], tAtLength(route, s));
}

export function angleAt(route, s) {
  const d = sampleDerivative(ROUTE_DEFS[route], tAtLength(route, s));
  return Math.atan2(d.y, d.x) * 180 / Math.PI;
}
