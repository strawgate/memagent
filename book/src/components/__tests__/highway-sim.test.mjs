/**
 * Tests for highway-sim.mjs — Node 22 built-in test runner.
 *
 * Run:  node --test book/src/components/__tests__/highway-sim.test.mjs
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createSimulation, fixedScale, DEFAULTS } from '../highway-sim.mjs';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Advance a simulation N ticks, starting from `start` with `dt` between ticks. */
function runTicks(sim, n, start, dt) {
  start = start || 0;
  dt = dt || DEFAULTS.spawnMs + 1; // default: enough time between spawns
  var last;
  for (var i = 0; i < n; i++) {
    last = sim.tick(start + i * dt);
  }
  return last;
}

// ---------------------------------------------------------------------------
// Traffic light cycling
// ---------------------------------------------------------------------------

describe('traffic light', function () {
  it('starts green when elapsed < green portion', function () {
    var sim = createSimulation({ greenPct: 50, cycleTotal: 1000, maxCars: 0 });
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.tick(100); // 100 ms into cycle, green portion = 500ms → green
    assert.equal(sim.isGreen(), true);
  });

  it('turns red when elapsed >= green portion', function () {
    var sim = createSimulation({ greenPct: 50, cycleTotal: 1000, maxCars: 0 });
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.tick(600); // 600ms > 500ms green → red
    assert.equal(sim.isGreen(), false);
  });

  it('cycles back to green after cycleTotal', function () {
    var sim = createSimulation({ greenPct: 50, cycleTotal: 1000, maxCars: 0 });
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.tick(1100); // 1100 % 1000 = 100 → green
    assert.equal(sim.isGreen(), true);
  });

  it('100% green never goes red', function () {
    var sim = createSimulation({ greenPct: 100, cycleTotal: 1000, maxCars: 0 });
    sim.setCycleStart(0);
    sim.exitAuto();
    for (var t = 0; t < 2000; t += 100) {
      sim.tick(t);
      assert.equal(sim.isGreen(), true, 'expected green at t=' + t);
    }
  });

  it('0% green is always red (once past t=0)', function () {
    var sim = createSimulation({ greenPct: 0, cycleTotal: 1000, maxCars: 0 });
    sim.setCycleStart(0);
    sim.exitAuto();
    // At t=0, elapsed=0, greenMs=0 → 0 < 0 is false → red
    sim.tick(0);
    assert.equal(sim.isGreen(), false);
    sim.tick(500);
    assert.equal(sim.isGreen(), false);
  });
});

// ---------------------------------------------------------------------------
// Car spawning
// ---------------------------------------------------------------------------

describe('car spawning', function () {
  it('spawns a car when enough time has passed', function () {
    var sim = createSimulation({ spawnMs: 100 }, fixedScale(1));
    sim.exitAuto();
    var frame = sim.tick(200);
    assert.notEqual(frame.spawned, null, 'should have spawned');
    assert.equal(sim.getCars().length, 1);
  });

  it('respects spawnMs cooldown', function () {
    var sim = createSimulation({ spawnMs: 500 }, fixedScale(1));
    sim.exitAuto();
    sim.tick(600); // first spawn
    var frame = sim.tick(700); // only 100ms later → no spawn
    assert.equal(frame.spawned, null);
    assert.equal(sim.getCars().length, 1);
  });

  it('does not spawn when another car blocks the entry', function () {
    var sim = createSimulation({ spawnMs: 10 }, fixedScale(1));
    sim.exitAuto();
    sim.addCar(5, 0, 1); // parked near d=0
    var frame = sim.tick(1000);
    assert.equal(frame.spawned, null, 'should not spawn — entry blocked');
  });

  it('respects maxCars', function () {
    var sim = createSimulation({ maxCars: 2, spawnMs: 10 }, fixedScale(1));
    sim.exitAuto();
    sim.addCar(200, 3, 1);
    sim.addCar(400, 3, 1);
    var frame = sim.tick(1000);
    assert.equal(frame.spawned, null, 'at maxCars');
  });
});

// ---------------------------------------------------------------------------
// Following distance and hard gap
// ---------------------------------------------------------------------------

describe('following distance', function () {
  it('cars never overlap (hard gap enforced)', function () {
    var sim = createSimulation({ greenPct: 0, cycleTotal: 100, gateD: 300, roadLength: 500 }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    // Place several cars close together
    for (var i = 0; i < 5; i++) {
      sim.addCar(100 + i * 30, 3.5, 1);
    }
    // Run many ticks — they should jam up but never overlap
    for (var t = 0; t < 200; t++) {
      sim.tick(t * 25);
      var sorted = sim.getCars().slice().sort(function (a, b) { return b.d - a.d; });
      for (var j = 1; j < sorted.length; j++) {
        var gap = sorted[j - 1].d - sorted[j].d;
        var minGap = (sorted[j - 1].w + sorted[j].w) / 2 + 6;
        assert.ok(gap >= minGap - 0.01,
          'gap violation: ' + gap.toFixed(2) + ' < ' + minGap.toFixed(2) +
          ' between cars ' + sorted[j - 1].id + ' and ' + sorted[j].id);
      }
    }
  });

  it('trailing car brakes when getting close', function () {
    var sim = createSimulation({ greenPct: 100, cycleTotal: 100, followZone: 12 }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar(40, 0, 1);    // stopped car
    sim.addCar(10, 3.5, 1);  // fast car approaching — gap=30, minGap=26, brakeZone=38
    // Run several ticks so the trailing car enters the braking zone
    for (var t = 0; t < 5; t++) {
      sim.tick(t * 25);
    }
    var cars = sim.getCars().slice().sort(function (a, b) { return a.d - b.d; });
    assert.ok(cars[0].speed < 3.5, 'trailing car should have braked');
  });
});

// ---------------------------------------------------------------------------
// Red light stopping
// ---------------------------------------------------------------------------

describe('red light', function () {
  it('lead car stops at gateD on red', function () {
    var gateD = 300;
    var sim = createSimulation({
      greenPct: 0, cycleTotal: 100, gateD: gateD, roadLength: 500,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar(gateD - 20, 3.5, 1); // approaching the line
    // Run enough ticks for it to reach the line
    for (var t = 0; t < 50; t++) {
      sim.tick(t * 25);
    }
    var car = sim.getCars()[0];
    assert.ok(car.d <= gateD + 0.01, 'car should stop at or before gateD');
    assert.ok(car.speed < 0.1, 'car should be stopped');
  });

  it('cars behind the lead stop via following distance, not light', function () {
    var gateD = 300;
    var sim = createSimulation({
      greenPct: 0, cycleTotal: 100, gateD: gateD, roadLength: 500,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar(gateD - 10, 3.5, 1); // will stop at line
    sim.addCar(gateD - 40, 3.5, 1); // close behind
    sim.addCar(gateD - 70, 3.5, 1); // close behind
    // Run plenty of ticks for cascade to settle
    for (var t = 0; t < 200; t++) {
      sim.tick(t * 25);
    }
    var sorted = sim.getCars().slice().sort(function (a, b) { return b.d - a.d; });
    // All stopped
    for (var i = 0; i < sorted.length; i++) {
      assert.ok(sorted[i].speed < 0.1, 'car ' + sorted[i].id + ' should be stopped');
    }
    // First car at gate
    assert.ok(sorted[0].d <= gateD + 0.01);
    // Others behind with valid gaps
    for (var j = 1; j < sorted.length; j++) {
      assert.ok(sorted[j].d < sorted[j - 1].d);
    }
  });
});

// ---------------------------------------------------------------------------
// Stats computation
// ---------------------------------------------------------------------------

describe('stats', function () {
  it('throughput counts deliveries in last 5 seconds', function () {
    var sim = createSimulation({
      roadLength: 100, rampEnd: 10, gateD: 80, greenPct: 100,
      cycleTotal: 100, spawnMs: 9999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    // Place a car that will exit quickly
    sim.addCar(90, 3.5, 1);
    var frame;
    for (var t = 0; t < 40; t++) {
      frame = sim.tick(t * 25);
    }
    // The car should have exited → throughput > 0
    assert.ok(frame.stats.throughput > 0, 'should have recorded delivery');
  });

  it('stallPct increases when cars are stalled', function () {
    var sim = createSimulation({
      greenPct: 0, cycleTotal: 100, gateD: 200, roadLength: 400,
      spawnMs: 9999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar(190, 0, 1); // already stopped near gate
    var frame;
    for (var t = 0; t < 20; t++) {
      frame = sim.tick(t * 25);
    }
    assert.ok(frame.stats.stallPct > 0, 'stall% should be > 0');
  });
});

// ---------------------------------------------------------------------------
// Auto-sweep
// ---------------------------------------------------------------------------

describe('auto-sweep', function () {
  it('decreases greenPct when direction is -1', function () {
    var sim = createSimulation({ greenPct: 50, maxCars: 0 });
    var initial = sim.getGreenPct();
    sim.tick(0);
    assert.ok(sim.getGreenPct() <= initial, 'should decrease or stay');
  });

  it('bounces at autoMin', function () {
    var sim = createSimulation({ greenPct: 6, autoSpeed: 10, autoMin: 5, autoMax: 95, maxCars: 0 });
    sim.tick(0); // should push below 5 then clamp
    assert.ok(sim.getGreenPct() >= 5, 'should not go below autoMin');
  });

  it('stops when setGreenPct is called', function () {
    var sim = createSimulation({ greenPct: 50, maxCars: 0 });
    sim.tick(0);
    assert.equal(sim.isAuto(), true);
    sim.setGreenPct(70);
    assert.equal(sim.isAuto(), false);
    var before = sim.getGreenPct();
    sim.tick(100);
    assert.equal(sim.getGreenPct(), before, 'should not change in manual mode');
  });
});

// ---------------------------------------------------------------------------
// Car size variety
// ---------------------------------------------------------------------------

describe('car size variety', function () {
  it('cars have different widths with default scale', function () {
    var sim = createSimulation({ maxCars: 20, spawnMs: 1 });
    sim.exitAuto();
    // Spawn several cars
    for (var i = 0; i < 10; i++) {
      sim.addCar(i * 50);
    }
    var cars = sim.getCars();
    var widths = new Set(cars.map(function (c) { return c.w; }));
    // With random scales, extremely unlikely all 10 identical
    assert.ok(widths.size > 1, 'expected varied widths');
  });

  it('fixedScale produces uniform scale but shape-dependent widths', function () {
    var sim = createSimulation({}, fixedScale(1.0));
    // Spawn 4 cars to cover all shapes (sedan, truck, compact, van)
    sim.addCar(0);
    sim.addCar(100);
    sim.addCar(200);
    sim.addCar(300);
    var cars = sim.getCars();
    // All have scale 1.0
    for (var i = 0; i < cars.length; i++) {
      assert.equal(cars[i].scale, 1.0);
    }
    // sedan (id 0) and van (id 3) have w = carW * 1.0
    assert.equal(cars[0].w, DEFAULTS.carW * 1.0); // sedan
    // truck (id 1) has w = carW * 1.3
    assert.equal(cars[1].w, DEFAULTS.carW * 1.3); // truck
    // compact (id 2) has w = carW * 0.8
    assert.equal(cars[2].w, DEFAULTS.carW * 0.8); // compact
  });

  it('gap enforcement accounts for different car sizes', function () {
    var sim = createSimulation({
      greenPct: 0, cycleTotal: 100, gateD: 200, roadLength: 400,
    });
    sim.setCycleStart(0);
    sim.exitAuto();
    // Big car followed by small car
    sim.addCar(190, 0, 1.3);
    sim.addCar(170, 3.5, 0.7);
    for (var t = 0; t < 30; t++) {
      sim.tick(t * 25);
    }
    var sorted = sim.getCars().slice().sort(function (a, b) { return b.d - a.d; });
    var gap = sorted[0].d - sorted[1].d;
    var minGap = (sorted[0].w + sorted[1].w) / 2 + 6;
    assert.ok(gap >= minGap - 0.01, 'gap should respect varied sizes');
  });
});

// ---------------------------------------------------------------------------
// Status messages
// ---------------------------------------------------------------------------

describe('status', function () {
  it('reports flowing when everything is moving', function () {
    var sim = createSimulation({
      greenPct: 100, cycleTotal: 100, roadLength: 500, spawnMs: 9999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar(200, 3.5, 1);
    var frame = sim.tick(0);
    assert.equal(frame.status.level, 'flowing');
  });

  it('reports blocked when ramp stalls', function () {
    var rampEnd = 150;
    var gateD = 300;
    // minGap per car = 20 + 6 = 26 (all scale=1)
    var sim = createSimulation({
      greenPct: 0, cycleTotal: 100, gateD: gateD, roadLength: 400,
      rampEnd: rampEnd, spawnMs: 9999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    // Pack a chain of cars from the gate back past the ramp.
    // Lead car at gate, then spaced at minGap intervals back into ramp zone.
    for (var i = 0; i < 12; i++) {
      sim.addCar(gateD - i * 26, 0, 1);
    }
    // Last car is at gateD - 11*26 = 300 - 286 = 14, well inside rampEnd=150.
    var frame;
    for (var t = 0; t < 20; t++) {
      frame = sim.tick(t * 25);
    }
    assert.equal(frame.status.level, 'blocked');
  });
});
