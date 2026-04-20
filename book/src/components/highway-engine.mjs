import { angleAt, EXIT_GATE_S, LENGTHS, MERGE_S, pointAt } from './highway-graph.mjs';

const STEP_MS = 1000 / 60;
const STEP_S = STEP_MS / 1000;
const SHAPES = ['sedan', 'truck', 'compact', 'van'];

const DEFAULTS = {
  spawnMs: 1400,
  maxCars: 18,
  greenPct: 80,
  cycleTotal: 3000,
  autoSpeed: 6, // pct per second
  autoMin: 5,
  autoMax: 100,
  minGap: 28,
  mergeGap: 32,
  accel: 140,
  brake: 220,
  spawnSpeed: 60,
  laneSpeed: {
    ramp: 92,
    highway: 112,
    exit: 102,
    cont: 112,
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
  let lastSpawnAt = -Infinity;
  let nextSpawnSource = 'highway';
  let autoMode = true;
  let autoVal = cfg.greenPct;
  let autoDir = -1;
  let greenPct = cfg.greenPct;
  let lightIsGreen = true;
  let accumulatorMs = 0;
  let deliveries = [];
  let spawnBlockedTicks = 0;
  let totalFrames = 0;
  let stalledFrames = 0;
  let removedIds = [];

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
    let ahead = Infinity;
    let behind = Infinity;
    for (let i = 0; i < cars.length; i++) {
      const car = cars[i];
      if (car.segment !== 'highway') continue;
      if (car.s >= MERGE_S) ahead = Math.min(ahead, car.s - MERGE_S);
      else behind = Math.min(behind, MERGE_S - car.s);
    }
    return ahead > cfg.mergeGap && behind > cfg.mergeGap;
  }

  function tickAuto(dtS) {
    if (!autoMode) return;
    autoVal += cfg.autoSpeed * autoDir * dtS;
    if (autoVal <= cfg.autoMin) {
      autoVal = cfg.autoMin;
      autoDir = 1;
    }
    if (autoVal >= cfg.autoMax) {
      autoVal = cfg.autoMax;
      autoDir = -1;
    }
    greenPct = Math.round(autoVal);
  }

  function tickLight(now) {
    const elapsed = (now - cycleStart) % cfg.cycleTotal;
    lightIsGreen = elapsed < cfg.cycleTotal * greenPct / 100;
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
    totalFrames++;
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

        if (leader) {
          const gap = leader.s - car.s - cfg.minGap;
          desired = Math.min(desired, clamp(gap * 3.2, 0, desired));
        }

        if (segment === 'ramp') {
          const remaining = LENGTHS.ramp - car.s - 8;
          if (!mergeAllowed()) desired = Math.min(desired, stopSpeed(remaining, cfg.brake));
        } else if (segment === 'exit' && !lightIsGreen && car.s < EXIT_GATE_S) {
          const remaining = EXIT_GATE_S - car.s - 6;
          desired = Math.min(desired, stopSpeed(remaining, cfg.brake));
        } else if (segment === 'highway') {
          const remaining = LENGTHS.highway - car.s - 6;
          if (car.route === 'exit' && !branchClear('exit')) desired = Math.min(desired, stopSpeed(remaining, cfg.brake));
          if (car.route === 'cont' && !branchClear('cont')) desired = Math.min(desired, stopSpeed(remaining, cfg.brake));
        }

        if (car.speed < desired) car.speed = Math.min(desired, car.speed + cfg.accel * STEP_S);
        else car.speed = Math.max(desired, car.speed - cfg.brake * STEP_S);
        car.s += car.speed * STEP_S;
      }
    }

    for (let i = 0; i < cars.length; i++) {
      const car = cars[i];
      if (car.segment === 'ramp' && car.s >= LENGTHS.ramp) {
        if (mergeAllowed()) {
          car.segment = 'highway';
          car.s = MERGE_S + (car.s - LENGTHS.ramp);
        } else {
          car.s = LENGTHS.ramp - 0.5;
          car.speed = 0;
        }
      } else if (car.segment === 'highway' && car.s >= LENGTHS.highway) {
        const overflow = car.s - LENGTHS.highway;
        if (car.route === 'exit' && branchClear('exit')) {
          car.segment = 'exit';
          car.s = overflow;
        } else if (car.route === 'cont' && branchClear('cont')) {
          car.segment = 'cont';
          car.s = overflow;
        } else {
          car.s = LENGTHS.highway - 0.5;
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

    if (cars.length < cfg.maxCars && now - lastSpawnAt >= cfg.spawnMs) {
      const preferred = nextSpawnSource;
      const fallback = preferred === 'highway' ? 'ramp' : 'highway';
      const source = branchClear(preferred) ? preferred : (branchClear(fallback) ? fallback : null);
      if (source) {
        cars.push(makeCar(source, 0));
        lastSpawnAt = now;
        nextSpawnSource = source === 'highway' ? 'ramp' : 'highway';
        spawnBlockedTicks = 0;
      } else {
        spawnBlockedTicks++;
      }
    }

    const stalledCount = cars.filter(car => car.speed < 12 && car.segment !== 'cont').length;
    if (stalledCount > 0) stalledFrames++;
    updateCarColors();
  }

  function snapshot(now) {
    const cutoff = now - 5000;
    deliveries = deliveries.filter(t => t >= cutoff);
    const throughput = Math.round(deliveries.length * 12);
    const stallPct = totalFrames > 0 ? Math.round(stalledFrames / totalFrames * 100) : 0;

    let status;
    if (spawnBlockedTicks >= 8) {
      status = { level: 'blocked', msg: 'Traffic backed up to the on-ramp — no new cars can enter' };
    } else if (stallPct > 20) {
      status = { level: 'congested', msg: 'Highway congested — cars queuing behind the light' };
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
        lastSpawnAt = now;
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
      greenPct = clamp(v, 5, 100);
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
      lastSpawnAt = t;
    },
  };
}
