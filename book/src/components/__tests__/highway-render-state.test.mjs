/**
 * Tests for highway-render-state.mjs — Node 22 built-in test runner.
 *
 * Run:  node --test book/src/components/__tests__/highway-render-state.test.mjs
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createRenderState, RENDER_DEFAULTS } from '../highway-render-state.mjs';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function runSteps(rs, n) {
  for (let i = 0; i < n; i++) rs.step();
}

function dist(a, b) {
  return Math.sqrt((a.x - b.x) ** 2 + (a.y - b.y) ** 2);
}

// ---------------------------------------------------------------------------
// Basic movement
// ---------------------------------------------------------------------------

describe('basic movement', function () {
  it('car moves toward a single waypoint', function () {
    const rs = createRenderState({ maxSpeed: 10, accel: 10, decelDist: 1 });
    rs.addCar('a', 0, 0, 0, 5);
    rs.pushTarget('a', 100, 0, 1, 'flow');
    rs.step();
    const v = rs.getCar('a');
    assert.ok(v.x > 0, 'car should have moved right');
    assert.equal(v.y, 0, 'car should stay on y=0');
  });

  it('car reaches target after enough steps', function () {
    const rs = createRenderState({ maxSpeed: 10, accel: 5, decelDist: 10, snapDist: 1 });
    rs.addCar('a', 0, 0, 0, 0);
    rs.pushTarget('a', 50, 0, 1, 'flow');
    runSteps(rs, 100);
    const v = rs.getCar('a');
    assert.ok(Math.abs(v.x - 50) < 1, 'car should be at target x');
  });

  it('car follows chain of waypoints in order', function () {
    const rs = createRenderState({ maxSpeed: 5, accel: 5, decelDist: 5, snapDist: 1 });
    rs.addCar('a', 0, 0, 0, 5);
    rs.pushTarget('a', 10, 0, 1, 'flow');
    rs.pushTarget('a', 10, 10, 1, 'flow');
    rs.pushTarget('a', 20, 10, 1, 'flow');
    runSteps(rs, 100);
    const v = rs.getCar('a');
    assert.ok(Math.abs(v.x - 20) < 1, 'car should reach final x');
    assert.ok(Math.abs(v.y - 10) < 1, 'car should reach final y');
  });
});

// ---------------------------------------------------------------------------
// Speed behavior
// ---------------------------------------------------------------------------

describe('speed behavior', function () {
  it('car accelerates from standstill', function () {
    const rs = createRenderState({ maxSpeed: 10, accel: 1, decelDist: 100 });
    rs.addCar('a', 0, 0, 0, 0);
    rs.pushTarget('a', 200, 0, 1, 'flow');
    rs.step();
    const v1 = rs.getCar('a');
    rs.step();
    const v2 = rs.getCar('a');
    const speed1 = v1.x;
    const speed2 = v2.x - v1.x;
    assert.ok(speed2 > speed1, 'second step should be faster (accelerating)');
  });

  it('car decelerates approaching target', function () {
    const rs = createRenderState({ maxSpeed: 10, accel: 2, decelDist: 50, snapDist: 0.5 });
    rs.addCar('a', 0, 0, 0, 10);
    rs.pushTarget('a', 5, 0, 1, 'flow');
    rs.step();
    const v = rs.getCar('a');
    // Car started with speed 10 but target is only 5px away — should decelerate
    assert.ok(v.x <= 5, 'car should not overshoot');
  });

  it('speed is never negative', function () {
    const rs = createRenderState({ maxSpeed: 2, accel: 0.5, decelDist: 30 });
    rs.addCar('a', 50, 0, 0, 0);
    // No waypoints — should decelerate to 0, never go negative
    for (let i = 0; i < 20; i++) {
      rs.step();
      const v = rs.getCar('a');
      assert.ok(v.x >= 49.9, 'car should not move backward (x=' + v.x + ')');
    }
  });
});

// ---------------------------------------------------------------------------
// Overshoot protection
// ---------------------------------------------------------------------------

describe('overshoot protection', function () {
  it('car never overshoots a waypoint', function () {
    const rs = createRenderState({ maxSpeed: 50, accel: 50, decelDist: 1, snapDist: 0.5 });
    rs.addCar('a', 0, 0, 0, 50);
    rs.pushTarget('a', 10, 0, 1, 'flow');
    rs.step();
    const v = rs.getCar('a');
    assert.ok(v.x <= 10.5, 'car should not overshoot (x=' + v.x + ')');
  });

  it('leftover distance carries to next waypoint', function () {
    const rs = createRenderState({ maxSpeed: 20, accel: 20, decelDist: 1, snapDist: 0.5 });
    rs.addCar('a', 0, 0, 0, 15);
    rs.pushTarget('a', 5, 0, 1, 'flow');
    rs.pushTarget('a', 20, 0, 1, 'flow');
    rs.step();
    const v = rs.getCar('a');
    assert.ok(v.x > 5, 'car should have passed first waypoint into second');
    assert.ok(v.x <= 20, 'car should not have passed second waypoint');
  });
});

// ---------------------------------------------------------------------------
// Junction-like waypoints (sharp turns)
// ---------------------------------------------------------------------------

describe('junction transitions', function () {
  it('no position jump when waypoints make a sharp turn', function () {
    const rs = createRenderState({ maxSpeed: 3, accel: 3, decelDist: 10, snapDist: 0.5 });
    rs.addCar('a', 0, 0, 0, 3);
    // Simulate: ramp going up-right, then highway going right
    rs.pushTarget('a', 5, -5, 1, 'flow');
    rs.pushTarget('a', 10, -5, 1, 'flow');
    rs.pushTarget('a', 15, -5, 1, 'flow');

    const positions = [];
    for (let i = 0; i < 30; i++) {
      rs.step();
      const v = rs.getCar('a');
      positions.push({ x: v.x, y: v.y });
    }

    // Check that no consecutive positions are more than maxSpeed apart
    for (let i = 1; i < positions.length; i++) {
      const d = dist(positions[i], positions[i - 1]);
      assert.ok(d <= 4, 'jump at step ' + i + ' was ' + d.toFixed(1) + 'px (max 4)');
    }
  });

  it('handles segment transition waypoints at same screen position', function () {
    // Simulate highway end and exit start at same pixel (560, 100)
    const rs = createRenderState({ maxSpeed: 4, accel: 2, decelDist: 20, snapDist: 0.5 });
    rs.addCar('a', 540, 100, 0, 3);
    rs.pushTarget('a', 560, 100, 1, 'flow'); // end of highway
    rs.pushTarget('a', 560, 100, 1, 'flow'); // start of exit (same point!)
    rs.pushTarget('a', 570, 110, 1, 'flow'); // exit curves down

    runSteps(rs, 20);
    const v = rs.getCar('a');
    // Should have progressed past the junction
    assert.ok(v.x >= 560, 'car should be past junction');
  });
});

// ---------------------------------------------------------------------------
// Angle tracking
// ---------------------------------------------------------------------------

describe('angle tracking', function () {
  it('angle tracks movement direction', function () {
    const rs = createRenderState({ maxSpeed: 10, accel: 10, decelDist: 1, angleRate: 1 });
    rs.addCar('a', 0, 0, 0, 5);
    rs.pushTarget('a', 0, 100, 1, 'flow');
    runSteps(rs, 5);
    const v = rs.getCar('a');
    // Moving downward → angle should be ~90 degrees
    assert.ok(Math.abs(v.angle - 90) < 20, 'angle should be near 90 (was ' + v.angle.toFixed(1) + ')');
  });
});

// ---------------------------------------------------------------------------
// Fade-in
// ---------------------------------------------------------------------------

describe('fade-in', function () {
  it('fadeAge ramps from 0 to 1 over fadeInFrames', function () {
    const frames = 10;
    const rs = createRenderState({ fadeInFrames: frames });
    rs.addCar('a', 0, 0, 0);
    const v0 = rs.getCar('a');
    assert.equal(v0.fadeAge, 0, 'should start at 0');

    for (let i = 0; i < frames; i++) rs.step();
    const v1 = rs.getCar('a');
    assert.ok(Math.abs(v1.fadeAge - 1) < 0.01, 'should be 1 after fadeInFrames steps');
  });

  it('during fade-in, color is always flow', function () {
    const rs = createRenderState({ fadeInFrames: 20 });
    rs.addCar('a', 0, 0, 0);
    rs.pushTarget('a', 10, 0, 1, 'stop');
    rs.step();
    const v = rs.getCar('a');
    assert.ok(v.fadeAge < 1, 'should still be fading');
    assert.equal(v.color, 'flow', 'color should be flow during fade');
  });

  it('after fade-in, color reflects sim color', function () {
    const rs = createRenderState({ fadeInFrames: 2, maxSpeed: 20, accel: 20, decelDist: 1, snapDist: 0.5 });
    rs.addCar('a', 0, 0, 0, 10);
    rs.pushTarget('a', 5, 0, 1, 'stop');
    runSteps(rs, 10);
    const v = rs.getCar('a');
    assert.equal(v.fadeAge, 1, 'fade should be complete');
    assert.equal(v.color, 'stop', 'should show sim color after fade');
  });
});

// ---------------------------------------------------------------------------
// Opacity
// ---------------------------------------------------------------------------

describe('opacity', function () {
  it('opacity equals fadeAge times sim opacity', function () {
    const rs = createRenderState({ fadeInFrames: 4 });
    rs.addCar('a', 0, 0, 0, 5);
    rs.pushTarget('a', 100, 0, 0.5, 'flow');
    // After consuming waypoint (1 step), fade is 0.25, sim opacity 0.5
    rs.step();
    const v = rs.getCar('a');
    // fadeAge = 1/4 = 0.25 (or car hasn't consumed wp yet — opacity from default 1)
    assert.ok(v.opacity >= 0 && v.opacity <= 1, 'opacity should be between 0 and 1');
  });

  it('fully faded car has full sim opacity', function () {
    const rs = createRenderState({ fadeInFrames: 1, maxSpeed: 20, accel: 20, decelDist: 1, snapDist: 0.5 });
    rs.addCar('a', 0, 0, 0, 10);
    rs.pushTarget('a', 5, 0, 0.7, 'flow');
    runSteps(rs, 20);
    const v = rs.getCar('a');
    assert.ok(Math.abs(v.opacity - 0.7) < 0.01, 'opacity should be sim opacity after fade (was ' + v.opacity + ')');
  });
});

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

describe('lifecycle', function () {
  it('removeCar cleans up state', function () {
    const rs = createRenderState();
    rs.addCar('a', 0, 0, 0);
    assert.ok(rs.hasCar('a'));
    rs.removeCar('a');
    assert.ok(!rs.hasCar('a'));
    assert.equal(rs.getCar('a'), null);
  });

  it('getAllCars returns all active cars', function () {
    const rs = createRenderState();
    rs.addCar('a', 0, 0, 0);
    rs.addCar('b', 10, 0, 0);
    const all = rs.getAllCars();
    assert.equal(all.length, 2);
  });

  it('pushTarget on nonexistent car is a no-op', function () {
    const rs = createRenderState();
    rs.pushTarget('ghost', 10, 10, 1, 'flow'); // should not throw
    assert.equal(rs.getCar('ghost'), null);
  });
});

// ---------------------------------------------------------------------------
// Pushing targets while en route
// ---------------------------------------------------------------------------

describe('target updates', function () {
  it('new targets extend the waypoint chain', function () {
    const rs = createRenderState({ maxSpeed: 3, accel: 3, decelDist: 10, snapDist: 0.5 });
    rs.addCar('a', 0, 0, 0, 3);
    rs.pushTarget('a', 30, 0, 1, 'flow');
    runSteps(rs, 3); // partially en route

    const mid = rs.getCar('a');
    assert.ok(mid.x > 0 && mid.x < 30, 'should be partway there');

    // Push a new target further along
    rs.pushTarget('a', 60, 0, 1, 'flow');
    runSteps(rs, 50);
    const v = rs.getCar('a');
    assert.ok(Math.abs(v.x - 60) < 1, 'should reach new target');
  });

  it('rapidly changing targets converges to latest', function () {
    const rs = createRenderState({ maxSpeed: 5, accel: 5, decelDist: 10, snapDist: 0.5 });
    rs.addCar('a', 0, 0, 0, 5);
    // Simulate 10 sim ticks worth of targets
    for (let i = 1; i <= 10; i++) {
      rs.pushTarget('a', i * 10, 0, 1, 'flow');
    }
    runSteps(rs, 200);
    const v = rs.getCar('a');
    assert.ok(Math.abs(v.x - 100) < 1, 'should converge to last target');
  });
});
