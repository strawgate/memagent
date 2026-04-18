/**
 * Highway backpressure simulation — pure logic, no DOM.
 *
 * Models a single-lane road with:
 *  - Steady car spawning at one end
 *  - A traffic light that cycles red/green near the far end
 *  - Following-distance braking (only the lead car sees the red light;
 *    everyone else brakes because the car ahead slowed)
 *  - Stats: throughput, queue depth, stall %
 *  - Auto-sweep that oscillates greenPct until the user takes over
 *
 * The UI layer (HighwayBackpressure.astro) maps 1-D distance values
 * returned here to SVG coordinates via getPointAtLength().
 */

// Exported so tests can reference them.
export var DEFAULTS = {
  roadLength: 700,
  rampEnd: 150,
  gateD: 600,
  carW: 20,
  minFollowPad: 6,
  speedMax: 3.5,
  spawnMs: 450,
  cycleTotal: 3000,
  greenPct: 80,
  maxCars: 30,
  lerpRate: 0.3,
  brakeRate: 0.35,
  followZone: 12,
  autoSpeed: 0.15,
  autoMin: 5,
  autoMax: 95,
  carScaleMin: 0.7,
  carScaleRange: 0.6,
};

/** Deterministic scale when you want repeatable tests. */
export function fixedScale(s) {
  return function () { return s; };
}

/** Default random scale factory (0.7 – 1.3). */
function randomScale(cfg) {
  return cfg.carScaleMin + Math.random() * cfg.carScaleRange;
}

/**
 * Create a simulation instance.
 *
 * @param {object} [overrides] — any DEFAULTS key to override
 * @param {function} [scaleFn]  — () => number, car size multiplier (for tests)
 * @returns simulation handle
 */
export function createSimulation(overrides, scaleFn) {
  var cfg = Object.assign({}, DEFAULTS, overrides);
  var getScale = scaleFn || function () { return randomScale(cfg); };

  // ---- internal state ----
  var cars = [];
  var nextId = 0;
  var lastSpawn = -Infinity;
  var lightIsGreen = true;
  var cycleStart = 0;
  var greenPct = cfg.greenPct;

  // stats
  var deliveredTimes = [];
  var stalledFrames = 0;
  var totalFrames = 0;

  // auto-sweep
  var autoMode = true;
  var autoVal = greenPct;
  var autoDir = -1;

  // ---- helpers ----

  function minGapBetween(a, b) {
    return (a.w + b.w) / 2 + cfg.minFollowPad;
  }

  function carColor(car) {
    if (car.speed < 0.2) return car.d < cfg.rampEnd ? 'stop' : 'slow';
    if (car.speed < 1.5) return 'slow';
    return 'flow';
  }

  // ---- public API ----

  // Car shape types — visual variety for the highway
  var SHAPES = ['sedan', 'truck', 'compact', 'van'];

  function makeCar(d, speed, scale) {
    var s = scale != null ? scale : getScale();
    var shape = SHAPES[nextId % SHAPES.length];
    var car = {
      id: nextId++,
      d: d,
      speed: speed != null ? speed : cfg.speedMax,
      scale: s,
      w: cfg.carW * s,
      shape: shape,
      color: 'flow',
    };
    car.color = carColor(car);
    return car;
  }

  function addCar(d, speed, scale) {
    var car = makeCar(d, speed, scale);
    cars.push(car);
    return car;
  }

  /** Try to spawn a car at d=0. Returns the car or null. */
  function trySpawn(now) {
    if (cars.length >= cfg.maxCars) return null;
    if (now - lastSpawn < cfg.spawnMs) return null;
    for (var j = 0; j < cars.length; j++) {
      if (cars[j].d < cfg.minFollowPad + cfg.carW) return null;
    }
    var car = addCar(0);
    lastSpawn = now;
    return car;
  }

  function tickLight(now) {
    var elapsed = (now - cycleStart) % cfg.cycleTotal;
    var greenMs = cfg.cycleTotal * greenPct / 100;
    lightIsGreen = elapsed < greenMs;
    return lightIsGreen;
  }

  function tickAuto() {
    if (!autoMode) return;
    autoVal += cfg.autoSpeed * autoDir;
    if (autoVal <= cfg.autoMin) { autoVal = cfg.autoMin; autoDir = 1; }
    if (autoVal >= cfg.autoMax) { autoVal = cfg.autoMax; autoDir = -1; }
    greenPct = Math.round(autoVal);
  }

  /**
   * Advance simulation by one frame.
   * @param {number} now — timestamp in ms (Date.now() or synthetic)
   * @returns {object} frame state for the UI to render
   */
  function tick(now) {
    totalFrames++;
    tickAuto();
    tickLight(now);

    // Spawn
    var spawned = trySpawn(now);

    // Sort front-to-back (highest d first)
    cars.sort(function (a, b) { return b.d - a.d; });

    var rampBlocked = false;
    var stalledCount = 0;
    var removedIds = [];

    for (var i = 0; i < cars.length; i++) {
      var car = cars[i];
      var target = cfg.speedMax;

      // Red light — only the lead car at the line stops.
      if (!lightIsGreen && car.d < cfg.gateD && car.d > cfg.gateD - 30) {
        var someoneCloser = (i > 0 && cars[i - 1].d <= cfg.gateD && cars[i - 1].d > car.d);
        if (!someoneCloser) {
          target = 0;
          if (car.d + car.speed > cfg.gateD) {
            car.d = cfg.gateD;
            car.speed = 0;
          }
        }
      }

      // Following distance — brake for car ahead
      if (i > 0) {
        var ahead = cars[i - 1];
        var gap = ahead.d - car.d;
        var minGap = minGapBetween(car, ahead);
        if (gap < minGap + cfg.followZone) {
          target = Math.min(target, Math.max(0, (gap - minGap) * cfg.brakeRate));
        }
      }

      // Smooth speed change
      car.speed += (target - car.speed) * cfg.lerpRate;
      if (car.speed < 0.02) car.speed = 0;

      car.d += car.speed;

      // Hard gap — never overlap
      if (i > 0) {
        var maxD = cars[i - 1].d - minGapBetween(car, cars[i - 1]);
        if (car.d > maxD) {
          car.d = maxD;
          car.speed = Math.min(car.speed, cars[i - 1].speed);
        }
      }

      // Stall detection
      if (car.speed < 0.2) {
        stalledCount++;
        if (car.d < cfg.rampEnd) rampBlocked = true;
      }

      car.color = carColor(car);

      // Off the end → delivered
      if (car.d > cfg.roadLength + 30) {
        removedIds.push(car.id);
      }
    }

    // Remove delivered cars
    for (var r = removedIds.length - 1; r >= 0; r--) {
      for (var k = cars.length - 1; k >= 0; k--) {
        if (cars[k].id === removedIds[r]) {
          cars.splice(k, 1);
          deliveredTimes.push(now);
          break;
        }
      }
    }

    if (stalledCount > 0) stalledFrames++;

    // Stats
    var cutoff = now - 5000;
    while (deliveredTimes.length > 0 && deliveredTimes[0] < cutoff) deliveredTimes.shift();
    var throughput = Math.round(deliveredTimes.length * 12);

    var queued = 0;
    for (var q = 0; q < cars.length; q++) {
      if (cars[q].speed < 1 && cars[q].d >= cfg.rampEnd && cars[q].d <= cfg.gateD) queued++;
    }

    var stallPct = totalFrames > 0 ? Math.round(stalledFrames / totalFrames * 100) : 0;

    // Status
    var status;
    if (rampBlocked) {
      status = { level: 'blocked', emoji: '\uD83D\uDD34', msg: 'Traffic backed up to the on-ramp \u2014 no new cars can enter' };
    } else if (stalledCount > cars.length * 0.4) {
      status = { level: 'congested', emoji: '\uD83D\uDFE1', msg: 'Highway congested \u2014 cars queuing behind the light' };
    } else {
      status = { level: 'flowing', emoji: '\uD83D\uDFE2', msg: 'Flowing \u2014 traffic moving freely' };
    }

    return {
      cars: cars,
      lightIsGreen: lightIsGreen,
      greenPct: greenPct,
      autoMode: autoMode,
      spawned: spawned,
      removedIds: removedIds,
      stats: { throughput: throughput, queued: queued, stallPct: stallPct },
      status: status,
    };
  }

  return {
    tick: tick,
    getCars: function () { return cars; },
    setGreenPct: function (v) {
      greenPct = v;
      autoMode = false;
      stalledFrames = 0;
      totalFrames = 0;
    },
    exitAuto: function () { autoMode = false; },
    isAuto: function () { return autoMode; },
    getGreenPct: function () { return greenPct; },
    isGreen: function () { return lightIsGreen; },
    setCycleStart: function (t) { cycleStart = t; },
    setLastSpawn: function (t) { lastSpawn = t; },
    addCar: addCar,
    cfg: cfg,
  };
}
