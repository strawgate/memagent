/**
 * Tests for highway-sim.mjs — Node 22 built-in test runner.
 *
 * Slot-based model: each segment has N slots, each holds at most one car.
 *
 * Run:  node --test book/src/components/__tests__/highway-sim.test.mjs
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createSimulation, fixedScale, DEFAULTS } from '../highway-sim.mjs';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function runTicks(sim, n, start, dt) {
  start = start || 0;
  dt = dt || DEFAULTS.spawnMs + 1;
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
    var frame = sim.tick(100);
    assert.equal(frame.lightIsGreen, true);
  });

  it('turns red when elapsed >= green portion', function () {
    var sim = createSimulation({ greenPct: 50, cycleTotal: 1000, maxCars: 0 });
    sim.setCycleStart(0);
    sim.exitAuto();
    var frame = sim.tick(600);
    assert.equal(frame.lightIsGreen, false);
  });

  it('cycles back to green after cycleTotal', function () {
    var sim = createSimulation({ greenPct: 50, cycleTotal: 1000, maxCars: 0 });
    sim.setCycleStart(0);
    sim.exitAuto();
    var frame = sim.tick(1100);
    assert.equal(frame.lightIsGreen, true);
  });

  it('100% green never goes red', function () {
    var sim = createSimulation({ greenPct: 100, cycleTotal: 1000, maxCars: 0 });
    sim.setCycleStart(0);
    sim.exitAuto();
    for (var t = 0; t < 2000; t += 100) {
      var frame = sim.tick(t);
      assert.equal(frame.lightIsGreen, true, 'expected green at t=' + t);
    }
  });

  it('0% green is always red', function () {
    var sim = createSimulation({ greenPct: 0, cycleTotal: 1000, maxCars: 0 });
    sim.setCycleStart(0);
    sim.exitAuto();
    var frame = sim.tick(0);
    assert.equal(frame.lightIsGreen, false);
    frame = sim.tick(500);
    assert.equal(frame.lightIsGreen, false);
  });
});

// ---------------------------------------------------------------------------
// Car spawning
// ---------------------------------------------------------------------------

describe('car spawning', function () {
  it('spawns a car when enough time has passed', function () {
    var sim = createSimulation({ spawnMs: 100 }, fixedScale(1));
    sim.exitAuto();
    sim.tick(200);
    assert.ok(sim.getCars().length >= 1, 'should have spawned at least one car');
  });

  it('respects spawnMs cooldown', function () {
    var sim = createSimulation({ spawnMs: 500, maxCars: 10 }, fixedScale(1));
    sim.exitAuto();
    sim.tick(600);
    sim.tick(700);
    assert.equal(sim.getCars().length, 1, 'should not have spawned twice');
  });

  it('offsets highway and ramp spawns', function () {
    var sim = createSimulation({ spawnMs: 100, maxCars: 10 }, fixedScale(1));
    sim.exitAuto();

    // First tick: highway fires immediately, ramp is offset by half spawnMs
    sim.tick(200);
    var cars = sim.getCars();
    assert.equal(cars.length, 1, 'only highway should spawn on first tick');
    assert.equal(cars[0].segment, 'highway');

    // At +150ms (half spawnMs offset) ramp fires, highway still on cooldown
    sim.tick(350);
    cars = sim.getCars();
    var segments = cars.map(function (c) { return c.segment; });
    assert.ok(segments.indexOf('ramp') >= 0, 'ramp should have spawned by now');
  });

  it('respects maxCars', function () {
    var sim = createSimulation({ maxCars: 2, spawnMs: 10 }, fixedScale(1));
    sim.exitAuto();
    sim.addCar('highway', 5);
    sim.addCar('ramp', 2);
    sim.tick(1000);
    assert.equal(sim.getCars().length, 2, 'should not exceed maxCars');
  });
});

// ---------------------------------------------------------------------------
// Slot integrity — no overlap
// ---------------------------------------------------------------------------

describe('slot integrity', function () {
  it('no two cars ever share a slot', function () {
    var sim = createSimulation({
      greenPct: 0, cycleTotal: 100, spawnMs: 50, maxCars: 20,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    for (var t = 0; t < 200; t++) {
      var frame = sim.tick(t * 100);
      var occupied = {};
      for (var c = 0; c < frame.cars.length; c++) {
        var car = frame.cars[c];
        var key = car.segment + ':' + car.slot;
        assert.ok(!occupied[key], 'slot collision at ' + key + ' on tick ' + t);
        occupied[key] = true;
      }
    }
  });

  it('addCar rejects occupied slot', function () {
    var sim = createSimulation({}, fixedScale(1));
    sim.addCar('highway', 3);
    assert.throws(function () {
      sim.addCar('highway', 3);
    }, /occupied/);
  });

  it('addCar rejects invalid slot index', function () {
    var sim = createSimulation({}, fixedScale(1));
    assert.throws(function () {
      sim.addCar('highway', 999);
    }, /invalid slot/);
  });
});

// ---------------------------------------------------------------------------
// Segment transitions
// ---------------------------------------------------------------------------

describe('segment transitions', function () {
  it('ramp cars merge onto highway', function () {
    var sim = createSimulation({
      slotsRamp: 5, slotsHwy: 15, mergeSlot: 5,
      greenPct: 100, cycleTotal: 100, spawnMs: 99999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar('ramp', 4); // last ramp slot
    for (var t = 0; t < 10; t++) {
      sim.tick(t * 100);
    }
    var cars = sim.getCars();
    var onHwy = cars.filter(function (c) { return c.segment === 'highway'; });
    assert.ok(onHwy.length >= 1, 'ramp car should have merged onto highway');
  });

  it('highway cars transition to exit', function () {
    var sim = createSimulation({
      slotsHwy: 15, slotsExit: 6,
      greenPct: 100, cycleTotal: 100, spawnMs: 99999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar('highway', 14); // last highway slot
    var transitioned = false;
    for (var t = 0; t < 10; t++) {
      var frame = sim.tick(t * 100);
      var onExit = frame.cars.filter(function (c) { return c.segment === 'exit'; });
      if (onExit.length >= 1 || frame.removedIds.length > 0) {
        transitioned = true;
        break;
      }
    }
    assert.ok(transitioned, 'highway car should have transitioned to exit (or been delivered)');
  });

  it('exit cars are delivered (removed) at end of exit', function () {
    var sim = createSimulation({
      slotsExit: 6, greenPct: 100, cycleTotal: 100, spawnMs: 99999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.setLastSpawn(99999);
    sim.addCar('exit', 5); // last exit slot
    var frame = sim.tick(100);
    assert.ok(frame.removedIds.length > 0, 'car should have been delivered');
  });

  it('ramp merge is blocked when mergeSlot is occupied', function () {
    var sim = createSimulation({
      slotsRamp: 5, slotsHwy: 15, slotsExit: 6, slotsCont: 0,
      mergeSlot: 5, gateSlot: 4,
      greenPct: 0, cycleTotal: 100, spawnMs: 99999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar('ramp', 4);
    // Fill highway from mergeSlot to end and exit (no cont = no overflow)
    for (var i = 5; i < 15; i++) sim.addCar('highway', i);
    for (var e = 0; e < 6; e++) sim.addCar('exit', e);
    sim.tick(100);
    var rampCars = sim.getCars().filter(function (c) { return c.segment === 'ramp'; });
    assert.ok(rampCars.length >= 1, 'ramp car should stay on ramp when merge blocked');
  });

  it('highway car goes to cont when exit is full and light is green', function () {
    var sim = createSimulation({
      slotsHwy: 15, slotsExit: 6, slotsCont: 6, gateSlot: 4,
      greenPct: 100, cycleTotal: 100, spawnMs: 99999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar('highway', 14); // last hwy slot
    // Fill all exit slots — with green light, exit will advance but
    // tryHwyToExit runs BEFORE exit advancement, so exit[0] is still occupied
    for (var i = 0; i < 6; i++) sim.addCar('exit', i);
    sim.tick(100);
    var contCars = sim.getCars().filter(function (c) { return c.segment === 'cont'; });
    assert.ok(contCars.length >= 1, 'car should go to cont when exit is full during green');
  });
});

// ---------------------------------------------------------------------------
// Traffic light gate behavior
// ---------------------------------------------------------------------------

describe('traffic light gate', function () {
  it('cars stop at gateSlot on red', function () {
    var sim = createSimulation({
      slotsExit: 6, gateSlot: 4,
      greenPct: 0, cycleTotal: 100, spawnMs: 99999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar('exit', 2);
    for (var t = 0; t < 20; t++) {
      sim.tick(t * 100);
    }
    var car = sim.getCars().filter(function (c) { return c.segment === 'exit'; })[0];
    assert.ok(car, 'car should still be on exit');
    assert.ok(car.slot <= 4, 'car should be at or before gateSlot');
    assert.equal(car.speed, 0, 'car should be stopped');
  });

  it('cars past gate during green survive when light turns red', function () {
    var sim = createSimulation({
      slotsExit: 6, gateSlot: 3,
      greenPct: 50, cycleTotal: 1000, spawnMs: 99999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    // Place car past the gate
    sim.addCar('exit', 1);

    // Advance during green phase (t < 500ms)
    for (var t = 0; t < 15; t++) {
      sim.tick(t * 30);
    }
    var cars = sim.getCars().filter(function (c) { return c.segment === 'exit'; });
    // Car should have advanced past gateSlot during green
    var pastGate = cars.filter(function (c) { return c.slot > 3; });

    // Now tick into red phase (t > 500ms)
    for (var t2 = 15; t2 < 30; t2++) {
      var frame = sim.tick(t2 * 50);
    }
    // Car that passed gate should still be advancing (not yanked back)
    var remainingExit = frame.cars.filter(function (c) { return c.segment === 'exit'; });
    // If car was delivered, that's fine too — it wasn't yanked back
    if (remainingExit.length > 0) {
      assert.ok(remainingExit[0].pastGate, 'car past gate should have pastGate=true');
    }
    // Key: car should NOT be stuck at gateSlot
  });

  it('red exit blocks highway — no cont bypass during red', function () {
    var sim = createSimulation({
      slotsHwy: 15, slotsExit: 6, slotsCont: 6, gateSlot: 4,
      greenPct: 0, cycleTotal: 100, spawnMs: 99999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar('highway', 14);
    // Fill all exit slots
    for (var i = 0; i < 6; i++) sim.addCar('exit', i);
    sim.tick(100);
    var contCars = sim.getCars().filter(function (c) { return c.segment === 'cont'; });
    assert.equal(contCars.length, 0, 'should not route to cont when light is red');
    var hwyCars = sim.getCars().filter(function (c) { return c.segment === 'highway'; });
    assert.ok(hwyCars.length >= 1, 'car should stay on highway');
  });

  it('backpressure cascades: exit full → highway blocks', function () {
    var sim = createSimulation({
      slotsHwy: 15, slotsExit: 6, slotsCont: 6, gateSlot: 4,
      greenPct: 0, cycleTotal: 100, spawnMs: 99999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.setLastSpawn(0);
    sim.exitAuto();
    // Fill exit
    for (var i = 0; i < 6; i++) sim.addCar('exit', i);
    // Car on highway at the end
    var stuck = sim.addCar('highway', 14);
    sim.tick(100);
    var hwyCars = sim.getCars().filter(function (c) { return c.segment === 'highway'; });
    assert.ok(hwyCars.length > 0, 'car should still be on highway');
    // Find the car we placed at slot 14
    var stuckCar = hwyCars.find(function (c) { return c.id === stuck.id; });
    assert.ok(stuckCar, 'original car should still exist');
    assert.equal(stuckCar.speed, 0, 'highway car should be stopped');
  });
});

// ---------------------------------------------------------------------------
// Car fade
// ---------------------------------------------------------------------------

describe('car fade', function () {
  it('cars past gateSlot on exit fade toward 0', function () {
    var sim = createSimulation({
      slotsExit: 6, gateSlot: 2,
      greenPct: 100, cycleTotal: 100, spawnMs: 99999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar('exit', 4); // past gate
    var frame = sim.tick(0);
    var car = frame.cars.filter(function (c) { return c.segment === 'exit'; })[0];
    assert.ok(car.opacity < 1, 'car past gateSlot should have opacity < 1');
  });

  it('cars before gateSlot on exit have full opacity', function () {
    var sim = createSimulation({
      slotsExit: 6, gateSlot: 4,
      greenPct: 100, cycleTotal: 100, spawnMs: 99999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar('exit', 2);
    var frame = sim.tick(0);
    var car = frame.cars.filter(function (c) { return c.segment === 'exit'; })[0];
    assert.equal(car.opacity, 1, 'car before gateSlot should have full opacity');
  });

  it('cont cars fade over full length', function () {
    var sim = createSimulation({
      slotsCont: 6, spawnMs: 99999,
    }, fixedScale(1));
    sim.exitAuto();
    sim.addCar('cont', 3); // middle of cont
    var frame = sim.tick(0);
    var car = frame.cars.filter(function (c) { return c.segment === 'cont'; })[0];
    assert.ok(car.opacity < 1, 'cont car should be fading');
    assert.ok(car.opacity > 0, 'cont car should not be fully faded in middle');
  });
});

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

describe('stats', function () {
  it('throughput counts deliveries in last 5 seconds', function () {
    var sim = createSimulation({
      slotsExit: 6, greenPct: 100, cycleTotal: 100, spawnMs: 99999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar('exit', 5); // last slot — will be removed
    var frame;
    for (var t = 0; t < 5; t++) {
      frame = sim.tick(t * 100);
    }
    assert.ok(frame.stats.throughput > 0, 'should have recorded delivery');
  });

  it('stallPct increases when cars are stalled', function () {
    var sim = createSimulation({
      slotsExit: 6, gateSlot: 4,
      greenPct: 0, cycleTotal: 100, spawnMs: 99999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar('exit', 3); // near gate, will stop
    var frame;
    for (var t = 0; t < 20; t++) {
      frame = sim.tick(t * 100);
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
    sim.tick(0);
    assert.ok(sim.tick(1).greenPct <= 50, 'should decrease or stay');
  });

  it('bounces at autoMin', function () {
    var sim = createSimulation({ greenPct: 6, autoSpeed: 10, autoMin: 5, autoMax: 100, maxCars: 0 });
    var frame = sim.tick(0);
    assert.ok(frame.greenPct >= 5, 'should not go below autoMin');
  });

  it('stops when setGreenPct is called', function () {
    var sim = createSimulation({ greenPct: 50, maxCars: 0 });
    sim.tick(0);
    assert.equal(sim.isAuto(), true);
    sim.setGreenPct(70);
    assert.equal(sim.isAuto(), false);
    var frame = sim.tick(100);
    assert.equal(frame.greenPct, 70, 'should not change in manual mode');
  });
});

// ---------------------------------------------------------------------------
// Car size variety
// ---------------------------------------------------------------------------

describe('car size variety', function () {
  it('cars have different widths with default scale', function () {
    var sim = createSimulation({ maxCars: 20, spawnMs: 99999 });
    sim.exitAuto();
    for (var i = 0; i < 10; i++) {
      sim.addCar('highway', i);
    }
    var cars = sim.getCars();
    var widths = new Set(cars.map(function (c) { return c.w; }));
    assert.ok(widths.size > 1, 'expected varied widths');
  });

  it('fixedScale produces uniform scale but shape-dependent widths', function () {
    var sim = createSimulation({ slotsHwy: 15 }, fixedScale(1.0));
    sim.addCar('highway', 0);
    sim.addCar('highway', 5);
    sim.addCar('highway', 10);
    sim.addCar('highway', 14);
    var cars = sim.getCars();
    for (var i = 0; i < cars.length; i++) {
      assert.equal(cars[i].scale, 1.0);
    }
    assert.equal(cars[0].w, DEFAULTS.carW * 1.0); // sedan
    assert.equal(cars[1].w, DEFAULTS.carW * 1.3); // truck
    assert.equal(cars[2].w, DEFAULTS.carW * 0.8); // compact
  });
});

// ---------------------------------------------------------------------------
// Status messages
// ---------------------------------------------------------------------------

describe('status', function () {
  it('reports flowing when everything is moving', function () {
    var sim = createSimulation({
      greenPct: 100, cycleTotal: 100, slotsHwy: 15, spawnMs: 99999,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    sim.addCar('highway', 5);
    var frame = sim.tick(0);
    assert.equal(frame.status.level, 'flowing');
  });

  it('reports blocked when entrance is blocked', function () {
    var sim = createSimulation({
      greenPct: 0, cycleTotal: 100,
      slotsHwy: 15, slotsRamp: 5, slotsExit: 6, slotsCont: 0,
      mergeSlot: 5, gateSlot: 4, spawnMs: 10,
      spawnBlockedThreshold: 5, maxCars: 40,
    }, fixedScale(1));
    sim.setCycleStart(0);
    sim.exitAuto();
    // Fill all slots (no cont = no overflow escape)
    for (var i = 0; i < 15; i++) sim.addCar('highway', i);
    for (var j = 0; j < 5; j++) sim.addCar('ramp', j);
    for (var e = 0; e < 6; e++) sim.addCar('exit', e);
    var frame;
    for (var t = 0; t < 20; t++) {
      frame = sim.tick(t * 25);
    }
    assert.equal(frame.status.level, 'blocked');
  });

  it('does not report blocked only because maxCars is reached', function () {
    var sim = createSimulation({
      maxCars: 1, spawnMs: 10, spawnBlockedThreshold: 2,
      greenPct: 100, cycleTotal: 100,
    }, fixedScale(1));
    sim.exitAuto();
    sim.addCar('highway', 5);
    var frame;
    for (var t = 0; t < 10; t++) {
      frame = sim.tick(t * 25);
    }
    assert.notEqual(frame.status.level, 'blocked');
  });
});

// ---------------------------------------------------------------------------
// Slot position mapping
// ---------------------------------------------------------------------------

describe('slot positions', function () {
  it('getSlotD returns evenly spaced positions', function () {
    var sim = createSimulation({ slotsHwy: 15, lenHwy: 530 }, fixedScale(1));
    var d0 = sim.getSlotD('highway', 0);
    var d14 = sim.getSlotD('highway', 14);
    assert.equal(d0, 0);
    assert.ok(Math.abs(d14 - 530) < 0.01, 'last slot should be at segment end');
  });

  it('car targetD matches slot position', function () {
    var sim = createSimulation({}, fixedScale(1));
    var car = sim.addCar('highway', 7);
    var expected = sim.getSlotD('highway', 7);
    assert.equal(car.targetD, expected);
  });

  it('targetD tracks slot position after movement', function () {
    var sim = createSimulation({ greenPct: 100, spawnMs: 99999 }, fixedScale(1));
    sim.exitAuto();
    var car = sim.addCar('highway', 1);
    sim.tick(1000);
    assert.equal(car.targetD, sim.getSlotD('highway', 2));
  });

  it('keeps a car stopped at the last slot when it cannot advance', function () {
    var sim = createSimulation({
      slotsHwy: 3, slotsRamp: 3, slotsExit: 2, slotsCont: 0,
      mergeSlot: 1, gateSlot: 0, greenPct: 0, spawnMs: 99999,
    }, fixedScale(1));
    sim.exitAuto();
    sim.addCar('exit', 0);
    sim.addCar('highway', 1);
    sim.addCar('highway', 2);
    var car = sim.addCar('ramp', 2);
    sim.tick(1000);
    assert.equal(car.slot, 2);
    assert.equal(car.speed, 0);
  });

  it('keeps fade opacity finite when gate slot is at segment end', function () {
    var sim = createSimulation({ slotsExit: 3, gateSlot: 2, greenPct: 100, spawnMs: 99999 }, fixedScale(1));
    sim.exitAuto();
    var car = sim.addCar('exit', 2);
    car.pastGate = true;
    sim.tick(1000);
    assert.equal(Number.isFinite(car.opacity), true);
  });
});

// ---------------------------------------------------------------------------
// Test helper controls
// ---------------------------------------------------------------------------

describe('test helper controls', function () {
  it('rejects unknown segment names', function () {
    var sim = createSimulation({}, fixedScale(1));
    assert.throws(function () {
      sim.addCar('unknown', 0);
    }, /unknown segment/);
    assert.throws(function () {
      sim.getSlots('unknown');
    }, /unknown segment/);
    assert.equal(sim.getCars().length, 0);
  });

  it('newly added stopped cars use stopped color until they move', function () {
    var sim = createSimulation({}, fixedScale(1));
    var car = sim.addCar('highway', 0);
    assert.equal(car.speed, 0);
    assert.equal(car.color, 'stop');
  });
});
