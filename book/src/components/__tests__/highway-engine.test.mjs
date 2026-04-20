import assert from 'node:assert/strict';
import test from 'node:test';

import { createHighwayEngine } from '../highway-engine.mjs';

test('cars move forward continuously on the highway', function () {
  const engine = createHighwayEngine({ spawnMs: 999999, greenPct: 100 });
  engine.setCycleStart(0);
  const car = engine.addCar('highway', 120);
  for (let t = 0; t <= 1000; t += 16) engine.tick(t);
  const moved = engine.getCars().find(c => c.id === car.id);
  assert.ok(moved.s > 120);
});

test('ramp car merges onto highway at merge point', function () {
  const engine = createHighwayEngine({ spawnMs: 999999, greenPct: 100 });
  engine.setCycleStart(0);
  const car = engine.addCar('ramp', 170);
  for (let t = 0; t <= 2000; t += 16) engine.tick(t);
  const moved = engine.getCars().find(c => c.id === car.id);
  assert.ok(moved.segment === 'highway' || moved.segment === 'exit' || moved.segment === 'cont');
  assert.ok(moved.x >= 220);
});

test('engine spawns at most one source per cooldown', function () {
  const engine = createHighwayEngine({ spawnMs: 500 });
  engine.setCycleStart(0);
  engine.setLastSpawn(0);
  for (let t = 0; t <= 600; t += 16) engine.tick(t);
  const count1 = engine.getCars().length;
  for (let t = 616; t <= 700; t += 16) engine.tick(t);
  const count2 = engine.getCars().length;
  assert.ok(count1 >= 1);
  assert.equal(count2, count1);
});
