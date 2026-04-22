import assert from 'node:assert/strict';
import test from 'node:test';

import { angleAt, EXIT_GATE_S, FORK_POINT, MERGE_POINT, MERGE_S, pointAt } from '../highway-graph.mjs';

test('ramp ends exactly at merge point', function () {
  const p = pointAt('ramp', 1e9);
  assert.equal(Math.round(p.x), MERGE_POINT.x);
  assert.equal(Math.round(p.y), MERGE_POINT.y);
});

test('highway merge distance lands at merge point', function () {
  const p = pointAt('highway', MERGE_S);
  assert.equal(Math.round(p.x), MERGE_POINT.x);
  assert.equal(Math.round(p.y), MERGE_POINT.y);
});

test('exit gate lies on exit curve and has valid angle', function () {
  const p = pointAt('exit', EXIT_GATE_S);
  const angle = angleAt('exit', EXIT_GATE_S);
  assert.ok(p.x > FORK_POINT.x);
  assert.ok(p.y >= FORK_POINT.y);
  assert.ok(Number.isFinite(angle));
});
