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

export const DEFAULTS = {
  slotsHwy: 15,
  slotsRamp: 9,
  slotsExit: 6,
  slotsCont: 6,
  mergeSlot: 5,       // highway slot where ramp merges in
  gateSlot: 4,        // exit slot where traffic light is
  carW: 20,
  spawnMs: 700,
  cycleTotal: 3000,
  greenPct: 80,
  maxCars: 22,
  autoSpeed: 0.15,
  autoMin: 5,
  autoMax: 100,
  scaleMin: 0.8,
  scaleMax: 1.15,
  spawnBlockedThreshold: 8,
  lenHwy: 530,
  lenRamp: 180,
  lenExit: 210,
  lenCont: 215,
};

const SHAPES = ['sedan', 'truck', 'compact', 'van'];
const SHAPE_W = { sedan: 1, truck: 1.3, compact: 0.8, van: 1 };

export function fixedScale(s) {
  return function () { return s; };
}

export function createSimulation(overrides, scaleFn) {
  const cfg = {};
  for (const k in DEFAULTS) cfg[k] = DEFAULTS[k];
  if (overrides) for (const k in overrides) cfg[k] = overrides[k];

  const getScale = scaleFn || function () {
    return cfg.scaleMin + Math.random() * (cfg.scaleMax - cfg.scaleMin);
  };

  // Compute evenly-spaced slot positions along a segment
  function slotPositions(count, segLen) {
    const positions = [];
    const spacing = count > 1 ? segLen / (count - 1) : 0;
    for (let i = 0; i < count; i++) {
      positions.push(i * spacing);
    }
    return positions;
  }

  // ---- Segment map — single source of truth per segment ----
  const segs = {
    highway: {
      slots: new Array(cfg.slotsHwy).fill(null),
      slotD: slotPositions(cfg.slotsHwy, cfg.lenHwy),
      len: cfg.lenHwy,
      hasGate: false, fadePastGate: false, fadeFullLen: false,
    },
    ramp: {
      slots: new Array(cfg.slotsRamp).fill(null),
      slotD: slotPositions(cfg.slotsRamp, cfg.lenRamp),
      len: cfg.lenRamp,
      hasGate: false, fadePastGate: false, fadeFullLen: false,
    },
    exit: {
      slots: new Array(cfg.slotsExit).fill(null),
      slotD: slotPositions(cfg.slotsExit, cfg.lenExit),
      len: cfg.lenExit,
      hasGate: true, fadePastGate: true, fadeFullLen: false,
    },
    cont: {
      slots: new Array(cfg.slotsCont).fill(null),
      slotD: slotPositions(cfg.slotsCont, cfg.lenCont),
      len: cfg.lenCont,
      hasGate: false, fadePastGate: false, fadeFullLen: true,
    },
  };

  let nextId = 0;
  let lightIsGreen = true;
  let greenPct = cfg.greenPct;
  let cycleStart = 0;
  let lastSpawnAt = -Infinity;
  let spawnSrc = 0;

  let autoMode = true;
  let autoVal = greenPct;
  let autoDir = -1;

  const deliveries = [];
  let stalledFrames = 0;
  let totalFrames = 0;
  let spawnBlockedTicks = 0;

  // ---- helpers ----

  function moveCar(car, segment, slotIdx) {
    const s = segs[segment];
    car.segment = segment;
    car.slot = slotIdx;
    car.targetD = s.slotD[slotIdx];
  }

  function makeCar(segment, slotIdx, scale) {
    const s = scale != null ? scale : getScale();
    const shape = SHAPES[nextId % SHAPES.length];
    const shapeMul = SHAPE_W[shape] || 1;
    const slotD = segs[segment].slotD[slotIdx] || 0;
    return {
      id: nextId++,
      segment: segment,
      slot: slotIdx,
      targetD: slotD,
      speed: 0,
      stuckTicks: 0,
      scale: s,
      w: cfg.carW * s * shapeMul,
      shape: shape,
      color: 'flow',
      opacity: 1,
      pastGate: false,
    };
  }

  function allCars() {
    const result = [];
    const segNames = ['highway', 'ramp', 'exit', 'cont'];
    for (let s = 0; s < segNames.length; s++) {
      const slots = segs[segNames[s]].slots;
      for (let i = 0; i < slots.length; i++) {
        if (slots[i]) result.push(slots[i]);
      }
    }
    return result;
  }

  function totalCars() {
    let count = 0;
    const segNames = ['highway', 'ramp', 'exit', 'cont'];
    for (let s = 0; s < segNames.length; s++) {
      const slots = segs[segNames[s]].slots;
      for (let i = 0; i < slots.length; i++) {
        if (slots[i]) count++;
      }
    }
    return count;
  }

  function carColor(car) {
    if (car.stuckTicks >= 3) return 'stop';
    if (car.stuckTicks >= 1) return 'slow';
    return 'flow';
  }

  // ---- light ----

  function tickLight(now) {
    if (greenPct >= 100) { lightIsGreen = true; return; }
    if (greenPct <= 0) { lightIsGreen = false; return; }
    const elapsed = (now - cycleStart) % cfg.cycleTotal;
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

  function advanceSegment(seg, segName) {
    const { slots, slotD, len, hasGate, fadePastGate, fadeFullLen } = seg;
    const lastSlot = slots.length - 1;

    for (let i = lastSlot; i >= 0; i--) {
      const car = slots[i];
      if (!car) continue;

      // Gate check — exit segment only
      if (hasGate && !lightIsGreen && !car.pastGate) {
        if (i >= cfg.gateSlot) {
          car.speed = 0;
          car.stuckTicks++;
          moveCar(car, car.segment, i);
          car.color = carColor(car);
          continue;
        }
      }

      // Try to advance to next slot
      const nextSlot = i + 1;
      if (nextSlot < slots.length && slots[nextSlot] === null) {
        // Gate blocking: can't advance past gateSlot if light is red
        if (hasGate && !lightIsGreen && !car.pastGate && nextSlot > cfg.gateSlot) {
          car.speed = 0;
          car.stuckTicks++;
          moveCar(car, car.segment, i);
          car.color = carColor(car);
          continue;
        }

        // Junction hold: don't advance into the last slot if the
        // downstream segment entrance is occupied — prevents visual overlap
        // at the screen-space junction point.
        // For the ramp, hold back an extra slot (lastSlot and lastSlot-1)
        // because the ramp curve approaches the highway closely for the
        // last two positions.
        if (segName === 'ramp' && nextSlot >= lastSlot - 1) {
          const mergeZoneBusy = segs.highway.slots[cfg.mergeSlot] !== null ||
            (cfg.mergeSlot > 0 && segs.highway.slots[cfg.mergeSlot - 1] !== null);
          if (mergeZoneBusy) {
            car.speed = 0;
            car.stuckTicks++;
            moveCar(car, car.segment, i);
            car.color = carColor(car);
            continue;
          }
        }
        if (nextSlot === lastSlot) {
          if (segName === 'highway') {
            const exitOccupied = segs.exit.slots[0] !== null;
            const contOccupied = segs.cont.slots[0] !== null;
            if (exitOccupied && (contOccupied || !lightIsGreen)) {
              car.speed = 0;
              car.stuckTicks++;
              moveCar(car, car.segment, i);
              car.color = carColor(car);
              continue;
            }
          }
        }

        // Move forward
        slots[i] = null;
        slots[nextSlot] = car;
        moveCar(car, car.segment, nextSlot);
        car.speed = 3.5;
        car.stuckTicks = 0;

        // Mark as past gate when crossing the gate slot
        if (hasGate && nextSlot > cfg.gateSlot) {
          car.pastGate = true;
        }
      } else {
        // Can't advance — stopped
        car.speed = 0;
        car.stuckTicks++;
        moveCar(car, car.segment, i);
      }

      // Fade for exit (past gate) and cont (full length)
      if (fadePastGate && car.pastGate) {
        const gatePos = slotD[cfg.gateSlot] || 0;
        const fadeLen = len - gatePos;
        car.opacity = fadeLen > 0 ? Math.max(0, 1 - (car.targetD - gatePos) / fadeLen) : 0;
      } else if (fadeFullLen) {
        car.opacity = len > 0 ? Math.max(0, 1 - car.targetD / len) : 0;
      } else {
        car.opacity = 1;
      }

      car.color = carColor(car);
    }
  }

  // ---- transitions ----

  function tryHwyToExit() {
    const hwy = segs.highway;
    const lastHwy = hwy.slots.length - 1;
    const car = hwy.slots[lastHwy];
    if (!car) return;

    const exitOpen = segs.exit.slots[0] === null;
    const contOpen = lightIsGreen && segs.cont.slots[0] === null;

    // Randomly decide: ~40% exit, ~60% continue straight
    const wantsExit = (car.id % 5) < 2;

    if (wantsExit && exitOpen) {
      hwy.slots[lastHwy] = null;
      moveCar(car, 'exit', 0);
      car.pastGate = false;
      car.stuckTicks = 0;
      segs.exit.slots[0] = car;
      return;
    }
    if (contOpen) {
      hwy.slots[lastHwy] = null;
      moveCar(car, 'cont', 0);
      car.stuckTicks = 0;
      segs.cont.slots[0] = car;
      return;
    }
    // Fallback: take whichever is open
    if (exitOpen) {
      hwy.slots[lastHwy] = null;
      moveCar(car, 'exit', 0);
      car.pastGate = false;
      car.stuckTicks = 0;
      segs.exit.slots[0] = car;
      return;
    }

    // Both full — car stays; mark as stopped
    car.speed = 0;
    car.stuckTicks++;
  }

  function tryRampMerge() {
    const ramp = segs.ramp;
    const hwy = segs.highway;
    const lastRamp = ramp.slots.length - 1;
    const car = ramp.slots[lastRamp];
    if (!car) return;

    // Merge into highway at mergeSlot if empty
    if (hwy.slots[cfg.mergeSlot] === null) {
      ramp.slots[lastRamp] = null;
      moveCar(car, 'highway', cfg.mergeSlot);
      car.stuckTicks = 0;
      hwy.slots[cfg.mergeSlot] = car;
    } else {
      car.speed = 0;
      car.stuckTicks++;
    }
  }

  // ---- spawning ----

  function trySpawn(now) {
    if (totalCars() >= cfg.maxCars) return { kind: 'capped' };

    const hwyBlocked = segs.highway.slots[0] !== null;
    const rampBlocked = segs.ramp.slots[0] !== null;
    if (hwyBlocked && rampBlocked) return { kind: 'blocked' };
    if (now - lastSpawnAt < cfg.spawnMs) return { kind: 'cooldown' };

    const preferred = spawnSrc === 0 ? 'highway' : 'ramp';
    const fallback = preferred === 'highway' ? 'ramp' : 'highway';

    if (segs[preferred].slots[0] === null) {
      segs[preferred].slots[0] = makeCar(preferred, 0);
      lastSpawnAt = now;
      spawnSrc = preferred === 'highway' ? 1 : 0;
      return { kind: 'spawned' };
    }
    if (segs[fallback].slots[0] === null) {
      segs[fallback].slots[0] = makeCar(fallback, 0);
      lastSpawnAt = now;
      spawnSrc = fallback === 'highway' ? 1 : 0;
      return { kind: 'spawned' };
    }

    return { kind: 'blocked' };
  }

  // ---- removal ----

  function removeDelivered(now) {
    const removedIds = [];

    // Exit: remove cars at last slot (delivered) or fully faded
    const exitSlots = segs.exit.slots;
    const lastExit = exitSlots.length - 1;
    for (let i = lastExit; i >= 0; i--) {
      const ec = exitSlots[i];
      if (!ec) continue;
      if (i === lastExit || ec.opacity <= 0.01) {
        removedIds.push(ec.id);
        deliveries.push(now);
        exitSlots[i] = null;
      }
    }

    // Cont: remove cars at last slot or fully faded
    const contSlots = segs.cont.slots;
    const lastCont = contSlots.length - 1;
    for (let j = lastCont; j >= 0; j--) {
      const cc = contSlots[j];
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
    const removedIds = removeDelivered(now);

    // 2. Advance continuation (downstream first — frees slots)
    advanceSegment(segs.cont, 'cont');

    // 3. Highway → exit/cont transition (before exit advancement,
    //    so a full exit causes overflow to cont)
    tryHwyToExit();

    // 4. Advance exit
    advanceSegment(segs.exit, 'exit');

    // 5. Advance highway
    advanceSegment(segs.highway, 'highway');

    // 6. Try transition again (after highway advancement freed the lead car)
    tryHwyToExit();

    // 7. Ramp → highway merge (before ramp advancement)
    tryRampMerge();

    // 8. Advance ramp
    advanceSegment(segs.ramp, 'ramp');

    // 9. Try merge again
    tryRampMerge();

    // 10. Spawn
    const spawn = trySpawn(now);
    if (spawn.kind === 'blocked') {
      spawnBlockedTicks++;
    } else if (spawn.kind !== 'cooldown') {
      spawnBlockedTicks = 0;
    }

    // Stats
    const cutoff = now - 5000;
    while (deliveries.length > 0 && deliveries[0] < cutoff) deliveries.shift();
    const throughput = Math.round(deliveries.length * 12);

    const all = allCars();
    let stalledCount = 0;
    for (let s = 0; s < all.length; s++) {
      if (all[s].stuckTicks >= 3 && all[s].segment !== 'cont') stalledCount++;
    }
    if (stalledCount > 0) stalledFrames++;

    const stallPct = totalFrames > 0 ? Math.round(stalledFrames / totalFrames * 100) : 0;

    let status;
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
    setLastSpawn: function (t) { lastSpawnAt = t; },
    addCar: function (segment, slotIdx, speed, scale) {
      const seg = segs[segment];
      if (!seg) {
        throw new Error('unknown segment ' + segment);
      }
      if (slotIdx < 0 || slotIdx >= seg.slots.length) {
        throw new Error('invalid slot ' + slotIdx + ' for segment ' + segment);
      }
      if (seg.slots[slotIdx] !== null) {
        throw new Error('slot ' + slotIdx + ' on ' + segment + ' is occupied');
      }
      const car = makeCar(segment, slotIdx, scale);
      if (speed != null) car.speed = speed;
      seg.slots[slotIdx] = car;
      return car;
    },
    getSlots: function (segment) {
      const seg = segs[segment];
      if (!seg) throw new Error('unknown segment ' + segment);
      return seg.slots.slice();
    },
    getSlotD: function (segment, slotIdx) {
      const seg = segs[segment];
      if (!seg) return 0;
      return seg.slotD[slotIdx] || 0;
    },
    cfg: cfg,
  };
}
