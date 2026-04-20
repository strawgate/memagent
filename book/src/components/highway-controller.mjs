/**
 * Highway backpressure animation controller.
 *
 * Wires together three layers:
 *   1. highway-sim.mjs       — slot-based traffic simulation (pure logic)
 *   2. highway-render-state.mjs — waypoint interpolation (pure logic)
 *   3. This file              — DOM: SVG elements, path sampling, event handling
 *
 * The sim produces (segment, targetD) per car. This file samples the SVG path
 * at that d to get screen (x, y) and pushes it as a waypoint into the render
 * state. The render state smoothly interpolates between waypoints every frame.
 * Segment transitions (ramp→highway, highway→exit) are invisible because
 * waypoints are in screen space — no coordinate-system jumps.
 */

import { createSimulation } from './highway-sim.mjs';
import { createRenderState } from './highway-render-state.mjs';

(function () {
  const carsG = document.getElementById('hw-cars');
  const statusEl = document.getElementById('hw-status');
  const pipelineLabel = document.getElementById('hw-pipeline-label');
  const slider = document.getElementById('hw-green');
  const sliderDetail = document.getElementById('hw-slider-detail');
  const lRed = document.getElementById('hw-l-red');
  const lGreen = document.getElementById('hw-l-green');
  const lightG = document.getElementById('hw-light');
  const lightHousing = document.getElementById('hw-light-housing');
  const stopLine = document.getElementById('hw-stop-line');
  const sTput = document.getElementById('hw-s-tput');
  const sQueue = document.getElementById('hw-s-queue');
  const sStall = document.getElementById('hw-s-stall');

  // ---- Measure SVG paths ----
  const pathRamp = document.getElementById('hw-path-ramp');
  const pathHwy = document.getElementById('hw-path-hwy');
  const pathExit = document.getElementById('hw-path-exit');
  const pathCont = document.getElementById('hw-path-cont');
  if (!carsG || !slider || !pathRamp || !pathHwy || !pathExit || !pathCont) return;

  const lenRamp = pathRamp.getTotalLength();
  const lenHwy = pathHwy.getTotalLength();
  const lenExit = pathExit.getTotalLength();
  const lenCont = pathCont.getTotalLength();

  // ---- Constants ----
  const SIM_FRAMES = 10; // sim advances every 10 render frames (250ms at 25ms/frame)
  const TICK_MS = 25;    // render frame interval (40 fps)
  const CAR_W = 20;
  const CAR_H = 10;

  // ---- Create simulation ----
  const sim = createSimulation({
    lenRamp: lenRamp,
    lenHwy: lenHwy,
    lenExit: lenExit,
    lenCont: lenCont,
    carW: CAR_W,
    greenPct: 80,
    slotsExit: 9,
    autoSpeed: 0.15 * SIM_FRAMES,
  });
  sim.setCycleStart(Date.now());

  // ---- Position traffic light on exit ramp ----
  const gateD = sim.getSlotD('exit', sim.cfg.gateSlot);
  (function () {
    const p = pathExit.getPointAtLength(gateD);
    const p2 = pathExit.getPointAtLength(Math.min(gateD + 3, lenExit));
    const angle = Math.atan2(p2.y - p.y, p2.x - p.x);
    const perpAngle = angle - Math.PI / 2;
    const cosP = Math.cos(perpAngle);
    const sinP = Math.sin(perpAngle);

    stopLine.setAttribute('x1', String((p.x + cosP * 14).toFixed(1)));
    stopLine.setAttribute('y1', String((p.y + sinP * 14).toFixed(1)));
    stopLine.setAttribute('x2', String((p.x - cosP * 14).toFixed(1)));
    stopLine.setAttribute('y2', String((p.y - sinP * 14).toFixed(1)));

    const ox = p.x + cosP * 30;
    const oy = p.y + sinP * 30;
    lightHousing.setAttribute('transform', 'translate(' + ox.toFixed(1) + ',' + oy.toFixed(1) + ')');
    lightG.style.display = '';
  })();

  // ---- Path helpers (DOM-dependent: sample SVG paths) ----
  const pathMap = { ramp: pathRamp, highway: pathHwy, exit: pathExit, cont: pathCont };
  const lenMap = { ramp: lenRamp, highway: lenHwy, exit: lenExit, cont: lenCont };

  function posAt(segment, d) {
    const path = pathMap[segment];
    const len = lenMap[segment];
    return path.getPointAtLength(Math.min(Math.max(0, d), len));
  }

  function angleAt(segment, d) {
    const len = lenMap[segment];
    const p1 = posAt(segment, d);
    const p2 = posAt(segment, Math.min(d + 2, len));
    if (Math.abs(p2.x - p1.x) < 0.01 && Math.abs(p2.y - p1.y) < 0.01) return 0;
    return Math.atan2(p2.y - p1.y, p2.x - p1.x) * 180 / Math.PI;
  }

  // ---- Render state (pure logic — handles all interpolation) ----
  const rs = createRenderState({
    fadeInFrames: 12,
    maxSpeed: 4,
    accel: 0.5,
    decelDist: 30,
    snapDist: 0.5,
    angleRate: 0.25,
  });

  // ---- SVG element management (DOM only — no interpolation logic) ----
  const carEls = {}; // id → { g, body }
  const carLastD = {};   // id → last known targetD (for arc-following waypoints)
  const carLastSeg = {}; // id → last known segment

  function createCarEl(car) {
    let w = CAR_W * car.scale;
    let h = CAR_H * car.scale;
    const s = car.scale;
    const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    const body = document.createElementNS('http://www.w3.org/2000/svg', 'rect');

    if (car.shape === 'truck') {
      w = CAR_W * s * 1.3;
      body.setAttribute('x', String(-w / 2));
      body.setAttribute('y', String(-h / 2));
      body.setAttribute('width', String(w));
      body.setAttribute('height', String(h));
      body.setAttribute('rx', String(2 * s));
    } else if (car.shape === 'compact') {
      w = CAR_W * s * 0.8;
      h = CAR_H * s * 0.85;
      body.setAttribute('x', String(-w / 2));
      body.setAttribute('y', String(-h / 2));
      body.setAttribute('width', String(w));
      body.setAttribute('height', String(h));
      body.setAttribute('rx', String(3.5 * s));
    } else if (car.shape === 'van') {
      h = CAR_H * s * 1.15;
      body.setAttribute('x', String(-w / 2));
      body.setAttribute('y', String(-h / 2));
      body.setAttribute('width', String(w));
      body.setAttribute('height', String(h));
      body.setAttribute('rx', String(2.5 * s));
    } else {
      body.setAttribute('x', String(-w / 2));
      body.setAttribute('y', String(-h / 2));
      body.setAttribute('width', String(w));
      body.setAttribute('height', String(h));
      body.setAttribute('rx', String(3 * s));
    }

    body.setAttribute('fill', 'var(--hw-car-flow)');
    g.appendChild(body);

    // Accent stripe
    const stripe = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    const stripeW = car.shape === 'truck' ? 3 * s : 2.5 * s;
    stripe.setAttribute('x', String(w / 2 - stripeW - 1.5 * s));
    stripe.setAttribute('y', String(-h / 2 + 1.5 * s));
    stripe.setAttribute('width', String(stripeW));
    stripe.setAttribute('height', String(h - 3 * s));
    stripe.setAttribute('rx', '1');
    stripe.setAttribute('fill', 'rgba(255,255,255,0.25)');
    g.appendChild(stripe);

    if (car.shape === 'truck' || car.shape === 'van') {
      const s2 = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      s2.setAttribute('x', String(-w / 2 + 2 * s));
      s2.setAttribute('y', String(-h / 2 + 1.5 * s));
      s2.setAttribute('width', String(2 * s));
      s2.setAttribute('height', String(h - 3 * s));
      s2.setAttribute('rx', '1');
      s2.setAttribute('fill', 'rgba(255,255,255,0.15)');
      g.appendChild(s2);
    }

    carsG.appendChild(g);
    g.setAttribute('data-car-id', String(car.id));
    carEls[car.id] = { g: g, body: body };

    // Register in render state at current screen position.
    const ip = posAt(car.segment, car.targetD);
    const ia = angleAt(car.segment, car.targetD);
    rs.addCar(car.id, ip.x, ip.y, ia, 0);
  }

  function removeCarEl(id) {
    const el = carEls[id];
    if (el && el.g.parentNode) {
      el.g.parentNode.removeChild(el.g);
      delete carEls[id];
    }
    delete carLastD[id];
    delete carLastSeg[id];
    rs.removeCar(id);
  }

  function applyVisuals(id) {
    const el = carEls[id];
    if (!el) return;
    const vis = rs.getCar(id);
    if (!vis) return;

    el.g.setAttribute('transform',
      'translate(' + vis.x.toFixed(1) + ',' + vis.y.toFixed(1) + ') rotate(' + vis.angle.toFixed(1) + ')');

    let fill;
    if (vis.color === 'stop') fill = 'var(--hw-car-stop)';
    else if (vis.color === 'slow') fill = 'var(--hw-car-slow)';
    else fill = 'var(--hw-car-flow)';
    el.body.setAttribute('fill', fill);

    el.g.setAttribute('opacity', vis.opacity.toFixed(2));
  }

  // ---- Main render loop ----
  let lastStatus = '';
  let timer = null;
  let frameCount = 0;
  let lastFrame = null;

  function tick() {
    const now = Date.now();
    frameCount++;

    // Advance simulation every SIM_FRAMES render frames
    if (frameCount % SIM_FRAMES === 0 || !lastFrame) {
      lastFrame = sim.tick(now);

      // Create new car elements, push waypoints for all cars
      const allCars = lastFrame.cars;
      for (let c = 0; c < allCars.length; c++) {
        const car = allCars[c];
        if (!carEls[car.id]) createCarEl(car);
        const tp = posAt(car.segment, car.targetD);
        const prevSeg = carLastSeg[car.id];
        const prevD = carLastD[car.id];
        const opa = car.opacity != null ? car.opacity : 1;

        // For curved segments, push arc-following intermediate waypoints
        // so the render state follows the SVG path instead of cutting corners.
        if (prevSeg === car.segment && prevD != null && prevD < car.targetD &&
            (car.segment === 'ramp' || car.segment === 'exit')) {
          const steps = 3;
          const dStep = (car.targetD - prevD) / steps;
          for (let s = 1; s < steps; s++) {
            const mp = posAt(car.segment, prevD + dStep * s);
            rs.pushTarget(car.id, mp.x, mp.y, opa, car.color);
          }
        }
        rs.pushTarget(car.id, tp.x, tp.y, opa, car.color);
        carLastD[car.id] = car.targetD;
        carLastSeg[car.id] = car.segment;
      }

      // Remove delivered cars
      for (let r = 0; r < lastFrame.removedIds.length; r++) {
        removeCarEl(lastFrame.removedIds[r]);
      }

      // Traffic light
      if (lRed && lGreen) {
        if (lastFrame.lightIsGreen) {
          lRed.setAttribute('fill', '#3a1111');
          lGreen.setAttribute('fill', '#22c55e');
        } else {
          lRed.setAttribute('fill', '#ef4444');
          lGreen.setAttribute('fill', '#113a16');
        }
      }

      // Auto-mode slider sync
      if (lastFrame.autoMode && slider) {
        slider.value = String(lastFrame.greenPct);
        if (sliderDetail) sliderDetail.textContent = 'Auto \u00B7 Green ' + lastFrame.greenPct + '%';
      }

      // Stats
      if (sTput) sTput.textContent = lastFrame.stats.throughput + '/min';
      if (sQueue) sQueue.textContent = String(lastFrame.stats.queued);
      if (sStall) {
        sStall.textContent = lastFrame.stats.stallPct + '%';
        sStall.style.color = lastFrame.stats.stallPct > 50 ? 'var(--hw-car-stop)' : lastFrame.stats.stallPct > 20 ? 'var(--hw-car-slow)' : '';
      }

      // Status label
      const msg = lastFrame.status.msg;
      if (msg !== lastStatus) {
        if (statusEl) statusEl.textContent = msg;
        if (pipelineLabel) {
          pipelineLabel.textContent = msg.toUpperCase();
          const fill = lastFrame.status.level === 'blocked' ? 'var(--hw-car-stop)' : lastFrame.status.level === 'congested' ? 'var(--hw-car-slow)' : 'var(--hw-muted)';
          pipelineLabel.setAttribute('fill', fill);
        }
        lastStatus = msg;
      }
    }

    // Every render frame: advance interpolation and apply to DOM
    rs.step();
    if (lastFrame) {
      for (let u = 0; u < lastFrame.cars.length; u++) {
        applyVisuals(lastFrame.cars[u].id);
      }
    }
  }

  // ---- Slider events ----
  function onSliderInput() {
    const v = parseInt(slider.value, 10);
    sim.setGreenPct(v);
    if (sliderDetail) sliderDetail.textContent = 'Green ' + v + '% \u00B7 Red ' + (100 - v) + '%';
  }
  function exitAuto() { sim.exitAuto(); }

  slider.addEventListener('input', onSliderInput);
  slider.addEventListener('pointerdown', exitAuto);
  slider.addEventListener('mousedown', exitAuto);
  slider.addEventListener('touchstart', exitAuto);

  // ---- Seed initial cars ----
  function seedCars() {
    sim.addCar('highway', 3);
    sim.addCar('highway', 7);
    sim.addCar('highway', 11);
  }

  // ---- Init ----
  if (!window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    sim.setLastSpawn(Date.now());
    seedCars();
    const initial = sim.getCars();
    for (let i = 0; i < initial.length; i++) {
      createCarEl(initial[i]);
      const ip = posAt(initial[i].segment, initial[i].targetD);
      rs.pushTarget(initial[i].id, ip.x, ip.y, 1, 'flow');
    }
    for (let s = 0; s < 5; s++) rs.step();
    for (let i = 0; i < initial.length; i++) applyVisuals(initial[i].id);
    timer = setInterval(tick, TICK_MS);
  } else {
    seedCars();
    const staticCars = sim.getCars();
    for (let j = 0; j < staticCars.length; j++) {
      createCarEl(staticCars[j]);
      const sp = posAt(staticCars[j].segment, staticCars[j].targetD);
      rs.pushTarget(staticCars[j].id, sp.x, sp.y, 1, 'flow');
    }
    for (let s = 0; s < 20; s++) rs.step();
    for (let j = 0; j < staticCars.length; j++) applyVisuals(staticCars[j].id);
    slider.disabled = true;
    if (statusEl) statusEl.textContent = 'Animation paused (reduced motion)';
  }

  // ---- Pause/resume on visibility change ----
  function onVisChange() {
    if (document.hidden) {
      clearInterval(timer);
      timer = null;
    } else if (!timer && !window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
      sim.setCycleStart(Date.now());
      sim.setLastSpawn(Date.now());
      timer = setInterval(tick, TICK_MS);
    }
  }
  document.addEventListener('visibilitychange', onVisChange);

  document.addEventListener('astro:before-swap', function () {
    clearInterval(timer);
    document.removeEventListener('visibilitychange', onVisChange);
  }, { once: true });
})();
