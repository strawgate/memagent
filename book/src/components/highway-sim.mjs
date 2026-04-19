/**
 * Highway backpressure simulation — pure logic, no DOM.
 *
 * Slot-based model: each road segment has a fixed number of slots.
 * Each slot holds at most one car. Cars advance one slot per tick
 * when the next slot is empty.
 *
 * Multi-route layout:
 *   highway: cars enter from left, travel right to the fork
 *   ramp:    cars enter from bottom-left, merge into highway at mergeSlot
 *   exit:    cars leave highway via exit ramp (has traffic light at gateSlot)
 *   cont:    cars continue east past the fork (always flowing, fade out)
 */

export var DEFAULTS = {
  slotsHwy: 15,
  slotsRamp: 5,
  slotsExit: 6,
  slotsCont: 6,
  mergeSlot: 5,       // highway slot where ramp merges in
  gateSlot: 4,        // exit slot where traffic light is
  carW: 20,
  spawnMs: 420,
  cycleTotal: 3000,
  greenPct: 80,
  maxCars: 22,
  autoSpeed: 0.15,
  autoMin: 5,
  autoMax: 100,
  scaleMin: 0.8,
  scaleMax: 1.15,
  spawnBlockedThreshold: 8,
  // Segment d-lengths for rendering (used to compute slot positions)
  lenHwy: 530,
  lenRamp: 180,
  lenExit: 210,
  lenCont: 215,
};

var SHAPES = ['sedan', 'truck', 'compact', 'van'];
var SHAPE_W = { sedan: 1, truck: 1.3, compact: 0.8, van: 1 };

export function fixedScale(s) {
  return function () { return s; };
}

export function createSimulation(overrides, scaleFn) {
  var cfg = {};
  var k;
  for (k in DEFAULTS) cfg[k] = DEFAULTS[k];
  if (overrides) for (k in overrides) cfg[k] = overrides[k];

  var getScale = scaleFn || function () {
    return cfg.scaleMin + Math.random() * (cfg.scaleMax - cfg.scaleMin);
  };

  // Compute slot d-positions for each segment
  function slotPositions(count, segLen) {
    var positions = [];
    var spacing = count > 1 ? segLen / (count - 1) : 0;
    for (var i = 0; i < count; i++) {
      positions.push(i * spacing);
    }
    return positions;
  }

  var hwySlotD = slotPositions(cfg.slotsHwy, cfg.lenHwy);
  var rampSlotD = slotPositions(cfg.slotsRamp, cfg.lenRamp);
  var exitSlotD = slotPositions(cfg.slotsExit, cfg.lenExit);
  var contSlotD = slotPositions(cfg.slotsCont, cfg.lenCont);

  // Slot arrays — null means empty, otherwise holds a car object
  var hwySlots = new Array(cfg.slotsHwy).fill(null);
  var rampSlots = new Array(cfg.slotsRamp).fill(null);
  var exitSlots = new Array(cfg.slotsExit).fill(null);
  var contSlots = new Array(cfg.slotsCont).fill(null);

  var nextId = 0;
  var lightIsGreen = true;
  var greenPct = cfg.greenPct;
  var cycleStart = 0;
  var lastSpawn = -Infinity;
  var spawnSrc = 0;

  var autoMode = true;
  var autoVal = greenPct;
  var autoDir = -1;

  var deliveries = [];
  var stalledFrames = 0;
  var totalFrames = 0;
  var spawnBlockedTicks = 0;

  // ---- helpers ----

  function makeCar(segment, slotIdx, scale) {
    var s = scale != null ? scale : getScale();
    var shape = SHAPES[nextId % SHAPES.length];
    var shapeMul = SHAPE_W[shape] || 1;
    var slotD = getSlotD(segment, slotIdx);
    return {
      id: nextId++,
      segment: segment,
      slot: slotIdx,
      d: slotD,
      targetD: slotD,
      speed: 0,
      scale: s,
      w: cfg.carW * s * shapeMul,
      shape: shape,
      color: 'stop',
      opacity: 1,
      pastGate: false,
    };
  }

  function getSlotD(segment, slotIdx) {
    if (segment === 'highway') return hwySlotD[slotIdx] || 0;
    if (segment === 'ramp') return rampSlotD[slotIdx] || 0;
    if (segment === 'exit') return exitSlotD[slotIdx] || 0;
    if (segment === 'cont') return contSlotD[slotIdx] || 0;
    return 0;
  }

  function getSlots(segment) {
    if (segment === 'highway') return hwySlots;
    if (segment === 'ramp') return rampSlots;
    if (segment === 'exit') return exitSlots;
    if (segment === 'cont') return contSlots;
    return null;
  }

  function getSegLen(segment) {
    if (segment === 'highway') return cfg.lenHwy;
    if (segment === 'ramp') return cfg.lenRamp;
    if (segment === 'exit') return cfg.lenExit;
    if (segment === 'cont') return cfg.lenCont;
    return 0;
  }

  function allCars() {
    var result = [];
    var segments = [hwySlots, rampSlots, exitSlots, contSlots];
    for (var s = 0; s < segments.length; s++) {
      for (var i = 0; i < segments[s].length; i++) {
        if (segments[s][i]) result.push(segments[s][i]);
      }
    }
    return result;
  }

  function totalCars() {
    var count = 0;
    var segments = [hwySlots, rampSlots, exitSlots, contSlots];
    for (var s = 0; s < segments.length; s++) {
      for (var i = 0; i < segments[s].length; i++) {
        if (segments[s][i]) count++;
      }
    }
    return count;
  }

  function carColor(car) {
    if (car.speed < 0.15) return 'stop';
    if (car.speed < 1.5) return 'slow';
    return 'flow';
  }

  // ---- light ----

  function tickLight(now) {
    if (greenPct >= 100) { lightIsGreen = true; return; }
    if (greenPct <= 0) { lightIsGreen = false; return; }
    var elapsed = (now - cycleStart) % cfg.cycleTotal;
    lightIsGreen = elapsed < cfg.cycleTotal * greenPct / 100;
  }

  function tickAuto() {
    if (!autoMode) return;
    autoVal += cfg.autoSpeed * autoDir;
    if (autoVal <= cfg.autoMin) { autoVal = cfg.autoMin; autoDir = 1; }
    if (autoVal >= cfg.autoMax) { autoVal = cfg.autoMax; autoDir = -1; }
    greenPct = Math.round(autoVal);
  }

  // ---- segment advancement ----

  function advanceSegment(slots, slotDArr, segLen, opts) {
    opts = opts || {};
    // Process from the end (highest slot) to start, so each car
    // sees the already-advanced state of the car ahead.
    for (var i = slots.length - 1; i >= 0; i--) {
      var car = slots[i];
      if (!car) continue;

      // Gate check — exit segment only
      if (opts.hasGate && !lightIsGreen && !car.pastGate) {
        if (i >= cfg.gateSlot) {
          // Car is at or past the gate slot but hasn't been marked past gate
          // (placed here manually or reached gate during red) — stop
          car.speed = 0;
          car.targetD = slotDArr[i];
          car.d = car.targetD;
          car.color = carColor(car);
          continue;
        }
      }

      // Try to advance to next slot
      var nextSlot = i + 1;
      if (nextSlot < slots.length && slots[nextSlot] === null) {
        // Gate blocking: can't advance past gateSlot if light is red
        if (opts.hasGate && !lightIsGreen && !car.pastGate && nextSlot > cfg.gateSlot) {
          car.speed = 0;
          car.targetD = slotDArr[i];
          car.d = car.targetD;
          car.color = carColor(car);
          continue;
        }

        // Move forward
        slots[i] = null;
        slots[nextSlot] = car;
        car.slot = nextSlot;
        car.targetD = slotDArr[nextSlot];
        car.speed = 3.5;

        // Mark as past gate when crossing the gate slot
        if (opts.hasGate && nextSlot > cfg.gateSlot) {
          car.pastGate = true;
        }
      } else {
        // Can't advance — stopped
        car.speed = 0;
        car.targetD = slotDArr[i];
      }

      // Fade for exit (past gate) and cont (full length)
      if (opts.fadePastGate && car.pastGate) {
        var gatePos = slotDArr[cfg.gateSlot] || 0;
        var fadeLen = segLen - gatePos;
        car.opacity = fadeLen > 0 ? Math.max(0, 1 - (car.targetD - gatePos) / fadeLen) : 0;
      } else if (opts.fadeFullLen) {
        car.opacity = segLen > 0 ? Math.max(0, 1 - car.targetD / segLen) : 0;
      } else {
        car.opacity = 1;
      }

      car.d = car.targetD;
      car.color = carColor(car);
    }
  }

  // ---- transitions ----

  function tryHwyToExit() {
    var lastHwy = cfg.slotsHwy - 1;
    var car = hwySlots[lastHwy];
    if (!car) return;

    // Prefer exit ramp
    if (exitSlots[0] === null) {
      hwySlots[lastHwy] = null;
      car.segment = 'exit';
      car.slot = 0;
      car.targetD = exitSlotD[0];
      car.pastGate = false;
      exitSlots[0] = car;
      return;
    }

    // Continuation (only when light is green — prevents bypass during red)
    if (lightIsGreen && contSlots[0] === null) {
      hwySlots[lastHwy] = null;
      car.segment = 'cont';
      car.slot = 0;
      car.targetD = contSlotD[0];
      contSlots[0] = car;
      return;
    }

    // Both full — car stays; mark as stopped
    car.speed = 0;
  }

  function tryRampMerge() {
    var lastRamp = cfg.slotsRamp - 1;
    var car = rampSlots[lastRamp];
    if (!car) return;

    // Merge into highway at mergeSlot if empty
    if (hwySlots[cfg.mergeSlot] === null) {
      rampSlots[lastRamp] = null;
      car.segment = 'highway';
      car.slot = cfg.mergeSlot;
      car.targetD = hwySlotD[cfg.mergeSlot];
      hwySlots[cfg.mergeSlot] = car;
    } else {
      car.speed = 0;
    }
  }

  // ---- spawning ----

  function trySpawn(now) {
    if (totalCars() >= cfg.maxCars) return { kind: 'capped' };
    if (now - lastSpawn < cfg.spawnMs) return { kind: 'cooldown' };

    var spawned = false;

    // Alternate preferred source
    var src = spawnSrc;
    spawnSrc = 1 - spawnSrc;

    if (src === 0) {
      if (hwySlots[0] === null) {
        hwySlots[0] = makeCar('highway', 0);
        spawned = true;
      } else if (rampSlots[0] === null) {
        rampSlots[0] = makeCar('ramp', 0);
        spawned = true;
      }
    } else {
      if (rampSlots[0] === null) {
        rampSlots[0] = makeCar('ramp', 0);
        spawned = true;
      } else if (hwySlots[0] === null) {
        hwySlots[0] = makeCar('highway', 0);
        spawned = true;
      }
    }

    if (spawned) {
      lastSpawn = now;
      return { kind: 'spawned' };
    }

    // Both entrances physically blocked
    return { kind: 'blocked' };
  }

  // ---- removal ----

  function removeDelivered(now) {
    var removedIds = [];

    // Exit: remove cars at last slot (delivered) or fully faded
    var lastExit = cfg.slotsExit - 1;
    for (var i = lastExit; i >= 0; i--) {
      var ec = exitSlots[i];
      if (!ec) continue;
      if (i === lastExit || ec.opacity <= 0.01) {
        removedIds.push(ec.id);
        deliveries.push(now);
        exitSlots[i] = null;
      }
    }

    // Cont: remove cars at last slot or fully faded
    var lastCont = cfg.slotsCont - 1;
    for (var j = lastCont; j >= 0; j--) {
      var cc = contSlots[j];
      if (!cc) continue;
      if (j === lastCont || cc.opacity <= 0.01) {
        removedIds.push(cc.id);
        contSlots[j] = null;
      }
    }

    return removedIds;
  }

  // ---- main tick ----

  function tick(now) {
    totalFrames++;
    tickAuto();
    tickLight(now);

    // 1. Remove delivered
    var removedIds = removeDelivered(now);

    // 2. Advance continuation (downstream first — frees slots)
    advanceSegment(contSlots, contSlotD, cfg.lenCont, { fadeFullLen: true });

    // 3. Highway → exit/cont transition (before exit advancement,
    //    so a full exit causes overflow to cont)
    tryHwyToExit();

    // 4. Advance exit
    advanceSegment(exitSlots, exitSlotD, cfg.lenExit, { hasGate: true, fadePastGate: true });

    // 5. Advance highway
    advanceSegment(hwySlots, hwySlotD, cfg.lenHwy);

    // 6. Try transition again (after highway advancement freed the lead car)
    tryHwyToExit();

    // 7. Ramp → highway merge (before ramp advancement)
    tryRampMerge();

    // 8. Advance ramp
    advanceSegment(rampSlots, rampSlotD, cfg.lenRamp);

    // 9. Try merge again
    tryRampMerge();

    // 10. Spawn
    var spawn = trySpawn(now);
    if (spawn.kind === 'blocked') {
      spawnBlockedTicks++;
    } else if (spawn.kind !== 'cooldown') {
      spawnBlockedTicks = 0;
    }

    // Stats
    var cutoff = now - 5000;
    while (deliveries.length > 0 && deliveries[0] < cutoff) deliveries.shift();
    var throughput = Math.round(deliveries.length * 12);

    var all = allCars();
    var stalledCount = 0;
    for (var s = 0; s < all.length; s++) {
      if (all[s].speed < 0.15 && all[s].segment !== 'cont') stalledCount++;
    }
    if (stalledCount > 0) stalledFrames++;

    var stallPct = totalFrames > 0 ? Math.round(stalledFrames / totalFrames * 100) : 0;

    var status;
    if (spawnBlockedTicks >= cfg.spawnBlockedThreshold) {
      status = { level: 'blocked', msg: 'Traffic backed up to the on-ramp \u2014 no new cars can enter' };
    } else if (stalledCount > 3) {
      status = { level: 'congested', msg: 'Highway congested \u2014 cars queuing behind the light' };
    } else {
      status = { level: 'flowing', msg: 'Flowing \u2014 traffic moving freely' };
    }

    return {
      cars: all,
      removedIds: removedIds,
      lightIsGreen: lightIsGreen,
      greenPct: greenPct,
      autoMode: autoMode,
      stats: { throughput: throughput, queued: all.length, stallPct: stallPct },
      status: status,
    };
  }

  return {
    tick: tick,
    getCars: allCars,
    setGreenPct: function (v) {
      greenPct = v;
      autoMode = false;
      stalledFrames = 0;
      totalFrames = 0;
    },
    exitAuto: function () { autoMode = false; },
    isAuto: function () { return autoMode; },
    setCycleStart: function (t) { cycleStart = t; },
    setLastSpawn: function (t) { lastSpawn = t; },
    addCar: function (segment, slotIdx, speed, scale) {
      var slots = getSlots(segment);
      if (!slots) {
        throw new Error('unknown segment ' + segment);
      }
      if (slotIdx < 0 || slotIdx >= slots.length) {
        throw new Error('invalid slot ' + slotIdx + ' for segment ' + segment);
      }
      if (slots[slotIdx] !== null) {
        throw new Error('slot ' + slotIdx + ' on ' + segment + ' is occupied');
      }
      var car = makeCar(segment, slotIdx, scale);
      if (speed != null) car.speed = speed;
      slots[slotIdx] = car;
      return car;
    },
    getSlots: function (segment) {
      var slots = getSlots(segment);
      if (!slots) throw new Error('unknown segment ' + segment);
      return slots.slice();
    },
    getSlotD: getSlotD,
    cfg: cfg,
  };
}
