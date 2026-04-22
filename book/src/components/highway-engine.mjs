import { angleAt, EXIT_GATE_S, LENGTHS, MERGE_S, pointAt } from './highway-graph.mjs';

const STEP_MS = 1000 / 60;
const STEP_S = STEP_MS / 1000;
const SHAPES = ['sedan', 'truck', 'compact', 'van'];

const DEFAULTS = {
  spawnMs: 700,
  maxCars: 18,
  greenPct: 75,
  cycleTotal: 3000,
  autoSpeed: 4, // pct per second — slow enough for congestion to visibly build
  autoMin: 0,
  autoMax: 75,
  autoDwell: 3, // seconds to pause at min before sweeping back up
  minGap: 36,
  mergeGap: 36,
  accel: 180,
  brake: 280,
  spawnSpeed: 78,
  laneSpeed: {
    ramp: 135,
    highway: 146,
    exit: 133,
    cont: 146,
  },
};

function clamp(v, min, max) {
  return Math.min(Math.max(v, min), max);
}

function stopSpeed(remaining, brake) {
  if (remaining <= 0) return 0;
  return Math.sqrt(2 * brake * remaining);
}

function lerp(a, b, t) {
  return a + (b - a) * t;
}

const RAMP_STOP_OFFSET = 80;
const STALL_WINDOW_FRAMES = 300;

export function createHighwayEngine(overrides) {
  const cfg = {
    ...DEFAULTS,
    laneSpeed: { ...DEFAULTS.laneSpeed },
  };
  if (overrides) {
    for (const key in overrides) {
      if (key === 'laneSpeed') cfg.laneSpeed = { ...cfg.laneSpeed, ...overrides.laneSpeed };
      else cfg[key] = overrides[key];
    }
  }

  let cars = [];
  let nextId = 0;
  let lastNow = null;
  let cycleStart = 0;
  let lastSpawnHwy = -Infinity;
  let lastSpawnRamp = -Infinity;
  let autoMode = true;
  let autoVal = cfg.greenPct;
  let autoDir = -1;
  let greenPct = cfg.greenPct;
  let lightIsGreen = true;
  let redPassCount = 0;
  let accumulatorMs = 0;
  let deliveries = [];
  let spawnBlockedTicks = 0;
  let stallSamples = [];
  let removedIds = [];

  // Pre-populate: place a few cars so the scene starts with traffic
  function preSeed() {
    const hwPositions = [60, 170, 290, 410];
    for (const s of hwPositions) {
      const car = makeCar('highway', s);
      car.speed = cfg.laneSpeed.highway;
      cars.push(car);
    }
    const rampS = LENGTHS.ramp * 0.35;
    const rcar = makeCar('ramp', rampS);
    rcar.speed = cfg.laneSpeed.ramp;
    cars.push(rcar);
  }
  preSeed();

  function routeChoiceFor(id) {
    return (id % 5) < 2 ? 'cont' : 'exit';
  }

  function makeCar(segment, s) {
    const id = nextId++;
    const shape = SHAPES[id % SHAPES.length];
    return {
      id,
      segment,
      s,
      speed: cfg.spawnSpeed,
      route: routeChoiceFor(id),
      shape,
      scale: lerp(0.85, 1.12, ((id * 37) % 100) / 100),
      opacity: 1,
      color: 'flow',
      mergeCleared: false,
      stopTicks: 0,
    };
  }

  function sortByS(segment) {
    return cars
      .filter(car => car.segment === segment)
      .sort((a, b) => b.s - a.s);
  }

  function branchClear(segment) {
    let nearest = Infinity;
    for (let i = 0; i < cars.length; i++) {
      const car = cars[i];
      if (car.segment === segment) nearest = Math.min(nearest, car.s);
    }
    return nearest > cfg.minGap;
  }

  function mergeAllowed() {
    let behind = Infinity;
    for (let i = 0; i < cars.length; i++) {
      const car = cars[i];
      if (car.segment !== 'highway') continue;
      const dist = Math.abs(car.s - MERGE_S);
      if (dist < behind) behind = dist;
    }
    return behind > cfg.mergeGap;
  }

  let autoDwellRemain = 0;

  function tickAuto(dtS) {
    if (!autoMode) return;
    // Dwell at min before sweeping back up
    if (autoDwellRemain > 0) {
      autoDwellRemain -= dtS;
      return;
    }
    autoVal += cfg.autoSpeed * autoDir * dtS;
    if (autoVal <= cfg.autoMin) {
      autoVal = cfg.autoMin;
      autoDir = 1;
      autoDwellRemain = cfg.autoDwell;
    }
    if (autoVal >= cfg.autoMax) {
      autoVal = cfg.autoMax;
      autoDir = -1;
    }
    greenPct = Math.round(autoVal);
  }

  function tickLight(now) {
    const elapsed = (now - cycleStart) % cfg.cycleTotal;
    const wasGreen = lightIsGreen;
    lightIsGreen = elapsed < cfg.cycleTotal * greenPct / 100;
    // Track red-phase passthrough: reset counter on transition to red
    if (wasGreen && !lightIsGreen) redPassCount = 0;
  }

  function updateCarColors() {
    for (let i = 0; i < cars.length; i++) {
      const car = cars[i];
      if (car.speed < 12) car.color = 'stop';
      else if (car.speed < 48) car.color = 'slow';
      else car.color = 'flow';

      if (car.segment === 'exit' && car.s > EXIT_GATE_S) {
        car.opacity = clamp(1 - (car.s - EXIT_GATE_S) / Math.max(1, LENGTHS.exit - EXIT_GATE_S), 0, 1);
      } else if (car.segment === 'cont') {
        car.opacity = clamp(1 - car.s / Math.max(1, LENGTHS.cont), 0, 1);
      } else {
        car.opacity = 1;
      }
    }
  }

  function fixedStep(now) {
    removedIds = [];
    tickAuto(STEP_S);
    tickLight(now);

    const perSeg = {
      ramp: sortByS('ramp'),
      highway: sortByS('highway'),
      exit: sortByS('exit'),
      cont: sortByS('cont'),
    };

    for (const segment of ['cont', 'exit', 'highway', 'ramp']) {
      const list = perSeg[segment];
      for (let i = 0; i < list.length; i++) {
        const car = list[i];
        const leader = i === 0 ? null : list[i - 1];
        let desired = cfg.laneSpeed[segment];
        const prevS = car.s;

        if (leader) {
          const gap = leader.s - car.s - cfg.minGap;
          desired = Math.min(desired, clamp(gap * 3.2, 0, desired));
        }

        if (segment === 'ramp') {
          // Two-phase merge: stop at the stop line, then drive remaining ramp after clearance
          const stopS = LENGTHS.ramp - RAMP_STOP_OFFSET;
          if (!car.mergeCleared) {
            const remaining = stopS - car.s;
            desired = Math.min(desired, stopSpeed(remaining, cfg.brake));
          }
        } else if (segment === 'exit' && !lightIsGreen && car.s < EXIT_GATE_S) {
          // Let at least 1 car through each red phase
          if (redPassCount >= 1) {
            const remaining = EXIT_GATE_S - car.s - 15;
            desired = Math.min(desired, stopSpeed(remaining, cfg.brake));
          }
        } else if (
          segment === 'exit' &&
          !lightIsGreen &&
          prevS < EXIT_GATE_S &&
          car.s >= EXIT_GATE_S &&
          !car._countedPass
        ) {
          car._countedPass = true;
          redPassCount++;
        } else if (segment === 'highway') {
          const remaining = LENGTHS.highway - car.s - 5;
          if (car.route === 'exit' && !branchClear('exit')) desired = Math.min(desired, stopSpeed(remaining, cfg.brake));
          if ((car.route === 'cont' && !lightIsGreen) || (car.route === 'cont' && !branchClear('cont'))) {
            desired = Math.min(desired, stopSpeed(remaining, cfg.brake));
          }
        }

        if (car.speed < desired) car.speed = Math.min(desired, car.speed + cfg.accel * STEP_S);
        else car.speed = Math.max(desired, car.speed - cfg.brake * STEP_S);
        car.s += car.speed * STEP_S;

        // Hard position clamp: never closer than minGap to the leader
        if (leader) {
          const maxS = leader.s - cfg.minGap;
          if (car.s > maxS) { car.s = maxS; car.speed = Math.min(car.speed, leader.speed); }
        }
      }
    }

    const RAMP_STOP_S = LENGTHS.ramp - RAMP_STOP_OFFSET;
    const RAMP_END_S = LENGTHS.ramp - 5;
    const HWY_STOP_S = LENGTHS.highway - 5;

    for (let i = 0; i < cars.length; i++) {
      const car = cars[i];
      if (car.segment === 'ramp' && !car.mergeCleared && car.s >= RAMP_STOP_S) {
        // Phase 1: stop at the line and wait for a gap + minimum dwell
        if (car.speed < 1) car.stopTicks++;
        if (car.speed < 1 && car.stopTicks >= 10 && mergeAllowed()) {
          car.mergeCleared = true; // proceed through remaining ramp
        } else {
          car.s = RAMP_STOP_S;
          car.speed = Math.max(0, car.speed - cfg.brake * STEP_S);
        }
      } else if (car.segment === 'ramp' && car.s >= RAMP_END_S) {
        // Phase 2: re-check the actual insertion point before joining the highway
        if (mergeAllowed()) {
          car.segment = 'highway';
          car.s = MERGE_S + (car.s - RAMP_END_S);
          car.speed = Math.min(car.speed, cfg.laneSpeed.highway);
        } else {
          car.s = RAMP_END_S;
          car.speed = 0;
        }
      } else if (car.segment === 'highway' && car.s >= HWY_STOP_S) {
        const overflow = car.s - HWY_STOP_S;
        if (car.route === 'exit' && branchClear('exit')) {
          car.segment = 'exit';
          car.s = overflow;
          car.speed = cfg.laneSpeed.exit;
        } else if (car.route === 'cont' && lightIsGreen && branchClear('cont')) {
          car.segment = 'cont';
          car.s = overflow;
          car.speed = cfg.laneSpeed.cont;
        } else {
          car.s = HWY_STOP_S;
          car.speed = 0;
        }
      } else if ((car.segment === 'exit' && car.s >= LENGTHS.exit) || (car.segment === 'cont' && car.s >= LENGTHS.cont)) {
        removedIds.push(car.id);
        deliveries.push(now);
      }
    }

    if (removedIds.length > 0) {
      const removed = new Set(removedIds);
      cars = cars.filter(car => !removed.has(car.id));
    }

    // Spawn highway and ramp independently, offset by half a period
    const spawnPeriod = cfg.spawnMs * 2; // each source spawns at 2× the base rate
    let anySpawned = false;
    let spawnBlocked = false;
    if (cars.length < cfg.maxCars && now - lastSpawnHwy >= spawnPeriod) {
      if (branchClear('highway')) {
        cars.push(makeCar('highway', 0));
        lastSpawnHwy = now;
        anySpawned = true;
      } else {
        spawnBlocked = true;
      }
    }
    if (cars.length < cfg.maxCars && now - lastSpawnRamp >= spawnPeriod) {
      if (branchClear('ramp')) {
        cars.push(makeCar('ramp', 0));
        lastSpawnRamp = now;
        anySpawned = true;
      } else {
        spawnBlocked = true;
      }
    }
    if (spawnBlocked) spawnBlockedTicks++;
    else if (anySpawned) spawnBlockedTicks = 0;

    const stalledCount = cars.filter(car => car.speed < 12 && car.segment === 'highway').length;
    stallSamples.push(stalledCount > 0 ? 1 : 0);
    if (stallSamples.length > STALL_WINDOW_FRAMES) stallSamples.shift();
    updateCarColors();
  }

  function snapshot(now) {
    const cutoff = now - 5000;
    deliveries = deliveries.filter(t => t >= cutoff);
    const throughput = Math.round(deliveries.length * 12);
    const stalledFrames = stallSamples.reduce((sum, sample) => sum + sample, 0);
    const stallPct = stallSamples.length > 0 ? Math.round(stalledFrames / stallSamples.length * 100) : 0;

    let status;
    if (spawnBlockedTicks >= 60) {
      status = { level: 'blocked', msg: 'Heavy Traffic Expected — backed up to the on-ramp' };
    } else if (stallPct > 30) {
      status = { level: 'congested', msg: 'Heavy Traffic Expected — cars queuing behind the light' };
    } else {
      status = { level: 'flowing', msg: 'Flowing — traffic moving freely' };
    }

    return {
      cars: cars.map(car => {
        const p = pointAt(car.segment, car.s);
        return {
          ...car,
          x: p.x,
          y: p.y,
          angle: angleAt(car.segment, car.s),
        };
      }),
      removedIds,
      lightIsGreen,
      greenPct,
      autoMode,
      stats: {
        throughput,
        queued: cars.length,
        stallPct,
      },
      status,
    };
  }

  return {
    cfg: {
      gateS: EXIT_GATE_S,
    },
    tick(now) {
      if (lastNow == null) {
        lastNow = now;
        cycleStart = now;
        lastSpawnHwy = now;
        lastSpawnRamp = now - cfg.spawnMs; // offset: ramp spawns one base period earlier
        tickLight(now);
        return snapshot(now);
      }

      accumulatorMs += Math.min(80, now - lastNow);
      lastNow = now;
      while (accumulatorMs >= STEP_MS) {
        fixedStep(now);
        accumulatorMs -= STEP_MS;
      }
      return snapshot(now);
    },
    addCar(segment, s) {
      const car = makeCar(segment, s != null ? s : 0);
      cars.push(car);
      return car;
    },
    getCars() {
      return snapshot(lastNow != null ? lastNow : Date.now()).cars;
    },
    setGreenPct(v) {
      greenPct = clamp(v, cfg.autoMin, cfg.autoMax);
      autoMode = false;
    },
    exitAuto() {
      autoMode = false;
    },
    isAuto() {
      return autoMode;
    },
    setCycleStart(t) {
      cycleStart = t;
    },
    setLastSpawn(t) {
      lastSpawnHwy = t;
      lastSpawnRamp = t - cfg.spawnMs;
    },
    resetClock() {
      lastNow = null;
      accumulatorMs = 0;
    },
  };
}
