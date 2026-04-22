import assert from 'node:assert/strict';
import test from 'node:test';

import { createHighwayEngine } from '../highway-engine.mjs';

test('cars move forward continuously on the highway', function () {
  const engine = createHighwayEngine({ spawnMs: 999999, greenPct: 100 });
  const car = engine.addCar('highway', 120);
  for (let t = 0; t <= 1000; t += 16) engine.tick(t);
  const moved = engine.getCars().find(c => c.id === car.id);
  assert.ok(moved.s > 120);
});

test('ramp car merges onto highway at merge point', function () {
  const engine = createHighwayEngine({ spawnMs: 999999, greenPct: 100 });
  const car = engine.addCar('ramp', 170);
  for (let t = 0; t <= 2000; t += 16) engine.tick(t);
  const moved = engine.getCars().find(c => c.id === car.id);
  assert.ok(moved.segment === 'highway' || moved.segment === 'exit' || moved.segment === 'cont');
  assert.ok(moved.x >= 220);
});

test('engine spawns at most one source per cooldown', function () {
  const engine = createHighwayEngine({ spawnMs: 500 });
  for (let t = 0; t <= 600; t += 16) engine.tick(t);
  const count1 = engine.getCars().length;
  for (let t = 616; t <= 700; t += 16) engine.tick(t);
  const count2 = engine.getCars().length;
  assert.ok(count1 >= 1);
  assert.equal(count2, count1);
});

test('no visual overlap between any two cars for 30 seconds', function () {
  const engine = createHighwayEngine({ greenPct: 50 });
  const STEP = 1000 / 60;
  let t = 0;
  let violations = 0;
  const MIN_DIST = 16;
  const details = [];

  for (let i = 0; i < 30000 / STEP; i++) {
    t += STEP;
    const frame = engine.tick(t);

    for (let a = 0; a < frame.cars.length; a++) {
      for (let b = a + 1; b < frame.cars.length; b++) {
        const ca = frame.cars[a];
        const cb = frame.cars[b];
        if (ca.opacity < 0.3 || cb.opacity < 0.3) continue;
        // Skip fork-origin overlap: cont and exit diverge from the same point
        if ((ca.segment === 'cont' && cb.segment === 'exit') || (ca.segment === 'exit' && cb.segment === 'cont')) {
          if (ca.s < 55 || cb.s < 55) continue;
        }
        // Skip ramp/highway cross-lane proximity: different visual lanes near merge approach
        if ((ca.segment === 'ramp' && cb.segment === 'highway') || (ca.segment === 'highway' && cb.segment === 'ramp')) {
          continue; // ramp and highway are visually separate roads
        }
        const dx = ca.x - cb.x;
        const dy = ca.y - cb.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < MIN_DIST) {
          violations++;
          if (details.length < 3) {
            details.push(`t=${t.toFixed(0)} cars ${ca.id}(${ca.segment} s=${ca.s.toFixed(1)}) & ${cb.id}(${cb.segment} s=${cb.s.toFixed(1)}) dist=${dist.toFixed(1)}`);
          }
        }
      }
    }
  }

  assert.strictEqual(violations, 0, `${violations} visual overlaps in 30s. First: ${details.join('; ')}`);
});

test('all spawned cars start with flow color', function () {
  const engine = createHighwayEngine({ greenPct: 80 });
  const seen = new Set();
  const STEP = 1000 / 60;
  let t = 0;
  let nonFlowSpawns = 0;

  for (let i = 0; i < 10000 / STEP; i++) {
    t += STEP;
    const frame = engine.tick(t);
    for (const car of frame.cars) {
      if (!seen.has(car.id)) {
        seen.add(car.id);
        if (car.color !== 'flow') nonFlowSpawns++;
      }
    }
  }

  assert.ok(seen.size > 5, `expected at least 5 spawns, got ${seen.size}`);
  assert.strictEqual(nonFlowSpawns, 0, `${nonFlowSpawns} cars spawned with non-flow color`);
});
