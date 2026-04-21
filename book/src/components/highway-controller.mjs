import { createHighwayEngine } from './highway-engine.mjs';
import { angleAt, EXIT_GATE_S, pointAt } from './highway-graph.mjs';

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

  if (!carsG || !slider || !lightG || !lightHousing || !stopLine) return;

  const CAR_W = 22;
  const CAR_H = 11;
  const carEls = {};

  const engine = createHighwayEngine({
    greenPct: 75,
  });

  (function positionLight() {
    const p = pointAt('exit', EXIT_GATE_S);
    const angle = angleAt('exit', EXIT_GATE_S) * Math.PI / 180;
    const perp = angle - Math.PI / 2;
    const cosP = Math.cos(perp);
    const sinP = Math.sin(perp);

    stopLine.setAttribute('x1', String((p.x + cosP * 14).toFixed(1)));
    stopLine.setAttribute('y1', String((p.y + sinP * 14).toFixed(1)));
    stopLine.setAttribute('x2', String((p.x - cosP * 14).toFixed(1)));
    stopLine.setAttribute('y2', String((p.y - sinP * 14).toFixed(1)));

    const ox = p.x + cosP * 30;
    const oy = p.y + sinP * 30;
    lightHousing.setAttribute('transform', 'translate(' + ox.toFixed(1) + ',' + oy.toFixed(1) + ')');
    lightG.style.display = '';
  })();

  function createCarEl(car) {
    let w = CAR_W * car.scale;
    let h = CAR_H * car.scale;
    const s = car.scale;
    const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    const body = document.createElementNS('http://www.w3.org/2000/svg', 'rect');

    if (car.shape === 'truck') {
      w = CAR_W * s * 1.3;
      body.setAttribute('rx', String(2 * s));
    } else if (car.shape === 'compact') {
      w = CAR_W * s * 0.8;
      h = CAR_H * s * 0.85;
      body.setAttribute('rx', String(3.5 * s));
    } else if (car.shape === 'van') {
      h = CAR_H * s * 1.15;
      body.setAttribute('rx', String(2.5 * s));
    } else {
      body.setAttribute('rx', String(3 * s));
    }

    body.setAttribute('x', String(-w / 2));
    body.setAttribute('y', String(-h / 2));
    body.setAttribute('width', String(w));
    body.setAttribute('height', String(h));
    body.setAttribute('fill', 'var(--hw-car-flow)');
    g.appendChild(body);

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

    g.setAttribute('data-car-id', String(car.id));
    g.setAttribute('class', 'car');
    carsG.appendChild(g);
    carEls[car.id] = { g, body };
  }

  function removeCarEl(id) {
    const el = carEls[id];
    if (!el) return;
    if (el.g.parentNode) el.g.parentNode.removeChild(el.g);
    delete carEls[id];
  }

  function applyCar(car) {
    if (!carEls[car.id]) createCarEl(car);
    const el = carEls[car.id];
    let fill = 'var(--hw-car-flow)';
    if (car.color === 'stop') fill = 'var(--hw-car-stop)';
    else if (car.color === 'slow') fill = 'var(--hw-car-slow)';
    el.body.setAttribute('fill', fill);
    el.g.setAttribute('opacity', car.opacity.toFixed(2));
    el.g.setAttribute('transform',
      'translate(' + car.x.toFixed(1) + ',' + car.y.toFixed(1) + ') rotate(' + car.angle.toFixed(1) + ')');
  }

  function updateFrame(frame) {
    const seen = {};
    for (let i = 0; i < frame.cars.length; i++) {
      const car = frame.cars[i];
      seen[car.id] = true;
      applyCar(car);
    }

    for (const id in carEls) {
      if (!seen[id]) removeCarEl(id);
    }

    if (lRed && lGreen) {
      if (frame.lightIsGreen) {
        lRed.setAttribute('fill', '#3a1111');
        lGreen.setAttribute('fill', '#22c55e');
      } else {
        lRed.setAttribute('fill', '#ef4444');
        lGreen.setAttribute('fill', '#113a16');
      }
    }

    if (frame.autoMode) {
      slider.value = String(frame.greenPct);
      if (sliderDetail) sliderDetail.textContent = 'Auto · Green ' + frame.greenPct + '%';
    }

    if (sTput) sTput.textContent = frame.stats.throughput + '/min';
    if (sQueue) sQueue.textContent = String(frame.stats.queued);
    if (sStall) {
      sStall.textContent = frame.stats.stallPct + '%';
      sStall.style.color = frame.stats.stallPct > 50 ? 'var(--hw-car-stop)' : frame.stats.stallPct > 20 ? 'var(--hw-car-slow)' : '';
    }

    if (statusEl) statusEl.textContent = frame.status.msg;
    if (pipelineLabel) {
      pipelineLabel.textContent = frame.status.msg.toUpperCase();
      const isHeavy = frame.status.level === 'blocked' || frame.status.level === 'congested';
      const fill = frame.status.level === 'blocked' ? 'var(--hw-car-stop)' : frame.status.level === 'congested' ? 'var(--hw-car-slow)' : 'var(--hw-muted)';
      pipelineLabel.setAttribute('fill', fill);
      pipelineLabel.classList.toggle('hw-pulse', isHeavy);
    }
  }

  let rafId = 0;
  function animate(now) {
    updateFrame(engine.tick(now));
    rafId = requestAnimationFrame(animate);
  }

  function onSliderInput() {
    const v = parseInt(slider.value, 10);
    engine.setGreenPct(v);
    if (sliderDetail) sliderDetail.textContent = 'Green ' + v + '% · Red ' + (100 - v) + '%';
  }

  function exitAuto() {
    engine.exitAuto();
  }

  slider.addEventListener('input', onSliderInput);
  slider.addEventListener('pointerdown', exitAuto);
  slider.addEventListener('mousedown', exitAuto);
  slider.addEventListener('touchstart', exitAuto);

  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    updateFrame(engine.tick(Date.now()));
    slider.disabled = true;
    if (statusEl) statusEl.textContent = 'Animation paused (reduced motion)';
  } else {
    rafId = requestAnimationFrame(animate);
  }

  document.addEventListener('visibilitychange', function () {
    if (document.hidden) {
      cancelAnimationFrame(rafId);
      rafId = 0;
    } else if (!rafId && !window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
      engine.resetClock();
      rafId = requestAnimationFrame(animate);
    }
  });

  // Teardown on Astro page swap to prevent leaked rAF and listeners
  document.addEventListener('astro:before-swap', function teardown() {
    cancelAnimationFrame(rafId);
    rafId = 0;
    slider.removeEventListener('input', onSliderInput);
    slider.removeEventListener('pointerdown', exitAuto);
    slider.removeEventListener('mousedown', exitAuto);
    slider.removeEventListener('touchstart', exitAuto);
    document.removeEventListener('astro:before-swap', teardown);
  });
})();
