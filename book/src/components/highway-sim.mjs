/**
 * Highway backpressure simulation — pure logic, no DOM.
 *
 * Multi-route model:
 *   highway: cars enter from left, travel right to the fork
 *   ramp:    cars enter from bottom-left, merge into highway at mergeD
 *   exit:    cars leave highway via exit ramp (has traffic light)
 *   cont:    cars continue east past the fork (fade out, always flowing)
 *
 * At the fork, cars prefer the exit ramp. When the exit backs up,
 * cars that can't exit continue east. Eventually the backup cascades
 * to block the highway and on-ramp entrances.
 */

export var DEFAULTS = {
  lenRamp: 180,
  lenHwy: 530,
  lenExit: 210,
  lenCont: 215,
  mergeD: 170,
  gateD: 130,
  carW: 20,
  minFollowPad: 14,
  speedMax: 3.5,
  spawnMs: 420,
  cycleTotal: 3000,
  greenPct: 80,
  maxCars: 22,
  lerpRate: 0.25,
  brakeRate: 0.3,
  followZone: 18,
  autoSpeed: 0.15,
  autoMin: 5,
  autoMax: 100,
  scaleMin: 0.8,
  scaleMax: 1.15,
  spawnBlockedThreshold: 8,
  exitEntryD: 0,
  contEntryD: 0,
  hwyForkThreshold: 5,
  hwyBlockedOffset: 2,
  rampMergeThreshold: 3,
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

  var nextId = 0;
  var carsRamp = [];
  var carsHwy = [];
  var carsExit = [];
  var carsCont = [];

  var lightIsGreen = true;
  var greenPct = cfg.greenPct;
  var cycleStart = 0;
  var lastSpawn = -Infinity;
  var spawnSrc = 0; // alternates 0=hwy, 1=ramp

  var autoMode = true;
  var autoVal = greenPct;
  var autoDir = -1;

  var deliveries = [];
  var stalledFrames = 0;
  var totalFrames = 0;
  var spawnBlockedTicks = 0;

  // ---- helpers ----

  function makeCar(segment, d, speed, scale) {
    var s = scale != null ? scale : getScale();
    var shape = SHAPES[nextId % SHAPES.length];
    var shapeMul = SHAPE_W[shape] || 1;
    var car = {
      id: nextId++,
      segment: segment,
      d: d != null ? d : 0,
      speed: speed != null ? speed : cfg.speedMax,
      scale: s,
      w: cfg.carW * s * shapeMul,
      shape: shape,
      color: 'flow',
      opacity: 1,
    };
    car.color = carColor(car);
    return car;
  }

  function nextCarWidth(scale) {
    var shape = SHAPES[nextId % SHAPES.length];
    return cfg.carW * scale * (SHAPE_W[shape] || 1);
  }

  function carColor(car) {
    if (car.speed < 0.15) return 'stop';
    if (car.speed < cfg.speedMax * 0.35) return 'slow';
    return 'flow';
  }

  function minGap(a, b) {
    return (a.w + b.w) / 2 + cfg.minFollowPad;
  }

  function allCars() {
    return carsRamp.concat(carsHwy, carsExit, carsCont);
  }

  function totalCars() {
    return carsRamp.length + carsHwy.length + carsExit.length + carsCont.length;
  }

  // ---- light ----

  function tickLight(now) {
    if (greenPct >= 100) { lightIsGreen = true; return; }
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

  // ---- spawning ----

  function hasSpawnSpace(cars, entrantW) {
    for (var j = 0; j < cars.length; j++) {
      if (cars[j].d < (entrantW + cars[j].w) / 2 + cfg.minFollowPad) return false;
    }
    return true;
  }

  function trySpawnAt(segment, cars) {
    var scale = getScale();
    if (!hasSpawnSpace(cars, nextCarWidth(scale))) return null;
    var car = makeCar(segment, 0, null, scale);
    cars.push(car);
    return car;
  }

  function trySpawn(now) {
    if (totalCars() >= cfg.maxCars) return { kind: 'capped' };
    if (now - lastSpawn < cfg.spawnMs) return { kind: 'cooldown' };

    var src = spawnSrc;
    spawnSrc = 1 - spawnSrc;
    var car = null;

    if (src === 0) {
      car = trySpawnAt('highway', carsHwy) || trySpawnAt('ramp', carsRamp);
    } else {
      car = trySpawnAt('ramp', carsRamp) || trySpawnAt('highway', carsHwy);
    }

    if (car) {
      lastSpawn = now;
      return { kind: 'spawned', car: car };
    }
    return { kind: 'blocked' };
  }

  // ---- movement within a segment ----

  function moveSegment(cars, segLen, opts) {
    cars.sort(function (a, b) { return b.d - a.d; });
    var phantoms = opts.phantoms || [];

    // Merge phantoms into a sorted list of obstacles (d descending)
    // so they integrate with the following-distance logic.
    var obstacles = cars.slice();
    for (var p = 0; p < phantoms.length; p++) {
      obstacles.push(phantoms[p]);
    }
    obstacles.sort(function (a, b) { return b.d - a.d; });

    for (var i = 0; i < cars.length; i++) {
      var car = cars[i];
      var target = cfg.speedMax;

      // Gate (light) — only on exit. Hard stop at traffic light.
      if (opts.hasGate && !lightIsGreen && car.d < cfg.gateD + 1) {
        var gateGap = cfg.gateD - car.d;
        if (gateGap < cfg.followZone + car.w / 2) {
          target = Math.min(target, Math.max(0, gateGap * cfg.brakeRate));
        }
      }

      // End-of-segment blocking — smooth braking zone
      if (opts.endBlocked) {
        if (car.d >= segLen - 50) {
          var distToEnd = segLen - car.d;
          target = Math.min(target, Math.max(0, distToEnd * 0.12));
        }
        if (car.d > segLen) {
          car.d -= (car.d - segLen) * 0.5;
          car.speed *= 0.3;
        }
      }

      // Following distance — find nearest obstacle ahead (real or phantom)
      var nearestAhead = null;
      for (var oi = 0; oi < obstacles.length; oi++) {
        var ob = obstacles[oi];
        if (ob === car) continue;
        if (ob.d > car.d && (!nearestAhead || ob.d < nearestAhead.d)) {
          nearestAhead = ob;
        }
      }
      if (nearestAhead) {
        var gap = nearestAhead.d - car.d;
        var mg = (car.w + (nearestAhead.w || cfg.carW)) / 2 + cfg.minFollowPad;
        if (gap < mg + cfg.followZone) {
          target = Math.min(target, Math.max(0, (gap - mg) * cfg.brakeRate));
        }
      }

      // Smooth speed
      car.speed += (target - car.speed) * cfg.lerpRate;
      if (car.speed < 0.03) car.speed = 0;
      car.d += car.speed;

      // Hard gate clamp — car must not overshoot the traffic light
      if (opts.hasGate && !lightIsGreen && car.d > cfg.gateD) {
        car.d = cfg.gateD;
        car.speed = 0;
      }

      // Hard gap enforcement — prevent overlap, match speed of car ahead
      if (nearestAhead) {
        var mg2 = (car.w + (nearestAhead.w || cfg.carW)) / 2 + cfg.minFollowPad;
        if (nearestAhead.d - car.d < mg2) {
          car.d = nearestAhead.d - mg2;
          car.speed = Math.min(car.speed, nearestAhead.speed != null ? nearestAhead.speed : 0);
        }
      }

      if (car.d < 0) car.d = 0;

      // Fade past gate on exit, or full-length fade on continuation
      if (opts.fade && opts.fadeLen) {
        var fadeStart = opts.fadeStart || 0;
        car.opacity = Math.max(0, 1 - (car.d - fadeStart) / opts.fadeLen);
      } else if (opts.fade && car.d > cfg.gateD) {
        car.opacity = Math.max(0, 1 - (car.d - cfg.gateD) / (segLen - cfg.gateD));
      } else {
        car.opacity = 1;
      }

      car.color = carColor(car);
    }

    // Post-loop gap enforcement pass — cars sorted by d descending (leader first)
    // Push each follower back if it overlaps the car ahead
    for (var gi = 1; gi < cars.length; gi++) {
      var ahead2 = cars[gi - 1];
      var behind = cars[gi];
      var reqGap = (ahead2.w + behind.w) / 2 + cfg.minFollowPad;
      if (ahead2.d - behind.d < reqGap) {
        behind.d = ahead2.d - reqGap;
        behind.speed = Math.min(behind.speed, ahead2.speed);
      }
    }
  }

  // ---- phantom obstacle builders ----
  // Project cars from adjacent segments into this segment's coordinate
  // space so the smooth following/braking physics prevents visual overlap
  // at merge and fork junctions.

  function hwyPhantoms() {
    var ph = [];
    // Exit cars near entry → appear at highway d = lenHwy + exitCar.d
    for (var e = 0; e < carsExit.length; e++) {
      if (carsExit[e].d < cfg.exitEntryD + 40) {
        ph.push({ d: cfg.lenHwy + carsExit[e].d, w: carsExit[e].w, speed: carsExit[e].speed });
      }
    }
    // Cont cars near entry → appear at highway d = lenHwy + contCar.d
    for (var c = 0; c < carsCont.length; c++) {
      if (carsCont[c].d < cfg.contEntryD + 40) {
        ph.push({ d: cfg.lenHwy + carsCont[c].d, w: carsCont[c].w, speed: carsCont[c].speed });
      }
    }
    return ph;
  }

  function rampPhantoms() {
    var ph = [];
    // Highway cars near mergeD → appear at ramp d = lenRamp + (hwyCar.d - mergeD)
    for (var h = 0; h < carsHwy.length; h++) {
      var dist = carsHwy[h].d - cfg.mergeD;
      if (Math.abs(dist) < 50) {
        ph.push({ d: cfg.lenRamp + dist, w: carsHwy[h].w, speed: carsHwy[h].speed });
      }
    }
    return ph;
  }

  // ---- transitions ----

  function canEnterExit(enterW) {
    var w = enterW || cfg.carW;
    var lead = null;
    for (var j = 0; j < carsExit.length; j++) {
      if (!lead || carsExit[j].d < lead.d) lead = carsExit[j];
    }
    if (!lead) return true;
    return (lead.d - cfg.exitEntryD) > (w + lead.w) / 2 + cfg.minFollowPad;
  }

  function canMergeAt(d, w) {
    for (var i = 0; i < carsHwy.length; i++) {
      var hw = carsHwy[i];
      var needed = (hw.w + w) / 2 + cfg.minFollowPad;
      if (Math.abs(hw.d - d) < needed) return false;
    }
    return true;
  }

  function canEnterCont(enterW) {
    var w = enterW || cfg.carW;
    var lead = null;
    for (var j = 0; j < carsCont.length; j++) {
      if (!lead || carsCont[j].d < lead.d) lead = carsCont[j];
    }
    if (!lead) return true;
    return (lead.d - cfg.contEntryD) > (w + lead.w) / 2 + cfg.minFollowPad;
  }

  function leadNearEnd(cars, segLen, threshold) {
    var lead = null;
    for (var i = 0; i < cars.length; i++) {
      if (!lead || cars[i].d > lead.d) lead = cars[i];
    }
    if (!lead || lead.d < segLen - threshold) return null;
    return lead;
  }

  function tryHwyToExit() {
    if (carsHwy.length === 0) return;
    carsHwy.sort(function (a, b) { return b.d - a.d; });
    while (carsHwy.length > 0) {
      var front = carsHwy[0];
      if (front.d < cfg.lenHwy - cfg.hwyForkThreshold) break;
      if (canEnterExit(front.w)) {
        carsHwy.splice(0, 1);
        front.segment = 'exit';
        front.d = cfg.exitEntryD;
        front.speed = Math.min(front.speed, cfg.speedMax * 0.8);
        carsExit.push(front);
      } else if (lightIsGreen && canEnterCont(front.w)) {
        carsHwy.splice(0, 1);
        front.segment = 'cont';
        front.d = cfg.contEntryD;
        front.speed = Math.min(front.speed, cfg.speedMax);
        carsCont.push(front);
      } else {
        // Both paths full — block at the fork
        front.d = cfg.lenHwy - cfg.hwyBlockedOffset;
        front.speed = 0;
        break;
      }
    }
  }

  function tryRampMerge() {
    if (carsRamp.length === 0) return;
    carsRamp.sort(function (a, b) { return b.d - a.d; });
    var front = carsRamp[0];
    if (front.d < cfg.lenRamp - cfg.rampMergeThreshold) return;
    if (canMergeAt(cfg.mergeD, front.w)) {
      carsRamp.splice(0, 1);
      front.segment = 'highway';
      front.d = cfg.mergeD;
      carsHwy.push(front);
    } else {
      front.d = cfg.lenRamp;
      front.speed = 0;
    }
  }

  // ---- main tick ----

  function tick(now) {
    totalFrames++;
    tickAuto();
    tickLight(now);

    var removedIds = [];

    // 1. Remove delivered (exit faded, cont off-screen)
    for (var i = carsExit.length - 1; i >= 0; i--) {
      if (carsExit[i].d >= cfg.lenExit || carsExit[i].opacity <= 0.01) {
        removedIds.push(carsExit[i].id);
        deliveries.push(now);
        carsExit.splice(i, 1);
      }
    }
    for (var ic = carsCont.length - 1; ic >= 0; ic--) {
      if (carsCont[ic].d >= cfg.lenCont || carsCont[ic].opacity <= 0.01) {
        removedIds.push(carsCont[ic].id);
        carsCont.splice(ic, 1);
      }
    }

    // 2. Move continuation (always free-flowing, fade out)
    moveSegment(carsCont, cfg.lenCont, { fade: true, fadeStart: 0, fadeLen: cfg.lenCont });

    // 3. Move exit (downstream first — frees space)
    moveSegment(carsExit, cfg.lenExit, { hasGate: true, fade: true });

    // 4. Highway → exit or continuation
    tryHwyToExit();

    // 5. Move highway — block when the lead car has no permitted fork path.
    var hwyLead = leadNearEnd(carsHwy, cfg.lenHwy, cfg.hwyForkThreshold);
    var forkFull = !!hwyLead && !canEnterExit(hwyLead.w)
      && (!lightIsGreen || !canEnterCont(hwyLead.w));
    moveSegment(carsHwy, cfg.lenHwy, { endBlocked: forkFull, phantoms: hwyPhantoms() });

    // 6. Try transition again (car may have moved to end this tick)
    tryHwyToExit();

    // 7. Ramp → highway
    tryRampMerge();

    // 8. Move ramp (with highway phantoms at merge zone)
    var rampLead = leadNearEnd(carsRamp, cfg.lenRamp, cfg.rampMergeThreshold);
    var mergeFull = !!rampLead && !canMergeAt(cfg.mergeD, rampLead.w);
    moveSegment(carsRamp, cfg.lenRamp, { endBlocked: mergeFull, phantoms: rampPhantoms() });

    // 9. Try merge again
    tryRampMerge();

    // 10. Spawn
    var spawn = trySpawn(now);
    if (spawn.kind === 'blocked') {
      spawnBlockedTicks++;
    } else if (spawn.kind !== 'cooldown') {
      // Reset on 'spawned' (car entered) and 'capped' (at capacity but
      // entrances not physically blocked).  Only 'cooldown' preserves
      // the counter — the spawn timer hasn't elapsed so we can't tell
      // whether the entrance is clear.
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

    // Status — only "blocked" when spawn entrance is actually blocked
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
    addCar: function (segment, d, speed, scale) {
      if (segment !== 'ramp' && segment !== 'highway' && segment !== 'exit' && segment !== 'cont') {
        throw new Error('unknown highway simulation segment: ' + segment);
      }
      var car = makeCar(segment, d, speed, scale);
      if (segment === 'ramp') carsRamp.push(car);
      else if (segment === 'highway') carsHwy.push(car);
      else if (segment === 'exit') carsExit.push(car);
      else if (segment === 'cont') carsCont.push(car);
      return car;
    },
    cfg: cfg,
  };
}
