/**
 * Highway render state — pure interpolation logic, no DOM.
 *
 * Each car accumulates a queue of screen-space waypoints {x, y, opacity, color}.
 * Every render frame, cars advance along their waypoint chain using a 1D velocity
 * chase. This eliminates segment-boundary teleportation because waypoints are
 * sampled from SVG paths and stitched together in screen space — there are no
 * coordinate-system jumps.
 *
 * Usage:
 *   const rs = createRenderState({ fadeInFrames: 12, maxSpeed: 4, ... });
 *   rs.addCar(id, x, y, angle, initSpeed);
 *   // per sim tick:
 *   rs.pushTarget(id, x, y, opacity, color);
 *   // per render frame:
 *   rs.step();
 *   const vis = rs.getCar(id); // { x, y, angle, opacity, color, fadeAge }
 */

export const RENDER_DEFAULTS = {
  fadeInFrames: 12,
  maxSpeed: 4,
  accel: 0.5,
  decelDist: 30,
  snapDist: 0.5,
  angleRate: 0.25,
};

export function createRenderState(overrides) {
  const cfg = {};
  for (const k in RENDER_DEFAULTS) cfg[k] = RENDER_DEFAULTS[k];
  if (overrides) for (const k in overrides) cfg[k] = overrides[k];

  const cars = {}; // id → internal state

  function addCar(id, x, y, angle, initSpeed) {
    cars[id] = {
      waypoints: [],    // [{x, y, opacity, color}]
      x: x,
      y: y,
      speed: initSpeed != null ? initSpeed : 0,
      angle: angle || 0,
      fadeAge: 0,
      // Latest target color/opacity (updated when waypoints are consumed)
      color: 'flow',
      opacity: 1,
    };
  }

  function removeCar(id) {
    delete cars[id];
  }

  function pushTarget(id, x, y, opacity, color) {
    const car = cars[id];
    if (!car) return;
    car.waypoints.push({
      x: x,
      y: y,
      opacity: opacity != null ? opacity : 1,
      color: color || 'flow',
    });
  }

  function stepCar(car) {
    // Determine target: the latest waypoint (skip to newest)
    // We always chase the LAST pushed waypoint — intermediate ones
    // serve as path shape (the car moves through them in order).
    if (car.waypoints.length === 0) {
      // No target — decelerate to stop
      car.speed = Math.max(0, car.speed - cfg.accel * 2);
      return;
    }

    // Current target is the first unconsumed waypoint
    const wp = car.waypoints[0];
    const dx = wp.x - car.x;
    const dy = wp.y - car.y;
    const dist = Math.sqrt(dx * dx + dy * dy);

    if (dist < cfg.snapDist) {
      // Consume this waypoint
      car.x = wp.x;
      car.y = wp.y;
      car.color = wp.color;
      car.opacity = wp.opacity;
      car.waypoints.shift();

      // If more waypoints, don't lose momentum — recurse for remaining distance
      if (car.waypoints.length > 0) {
        return; // will be picked up next frame
      }
      // No more waypoints — hold position
      car.speed = 0;
      return;
    }

    // Velocity chase toward current waypoint
    // Look ahead: total remaining path distance through all waypoints
    let totalDist = dist;
    let prevX = wp.x, prevY = wp.y;
    for (let i = 1; i < car.waypoints.length; i++) {
      const nxt = car.waypoints[i];
      const segDx = nxt.x - prevX;
      const segDy = nxt.y - prevY;
      totalDist += Math.sqrt(segDx * segDx + segDy * segDy);
      prevX = nxt.x;
      prevY = nxt.y;
    }

    // Desired speed: decelerate when total remaining distance is small
    const desired = Math.min(
      cfg.maxSpeed,
      (totalDist / cfg.decelDist) * cfg.maxSpeed,
    );

    // Accelerate or brake
    if (car.speed < desired) {
      car.speed = Math.min(car.speed + cfg.accel, desired);
    } else {
      car.speed = Math.max(car.speed - cfg.accel * 2, desired);
    }

    // Ensure speed is non-negative
    car.speed = Math.max(0, car.speed);

    // Move toward current waypoint
    const dirX = dx / dist;
    const dirY = dy / dist;
    let moveD = car.speed;

    // Don't overshoot current waypoint — consume and continue to next
    if (moveD >= dist) {
      car.x = wp.x;
      car.y = wp.y;
      car.color = wp.color;
      car.opacity = wp.opacity;
      car.waypoints.shift();
      const remaining = moveD - dist;
      // Continue moving toward next waypoint with leftover distance
      if (remaining > 0 && car.waypoints.length > 0) {
        const nxt = car.waypoints[0];
        const ndx = nxt.x - car.x;
        const ndy = nxt.y - car.y;
        const ndist = Math.sqrt(ndx * ndx + ndy * ndy);
        if (ndist > 0.01) {
          const frac = Math.min(remaining / ndist, 1);
          car.x += ndx * frac;
          car.y += ndy * frac;
        }
      }
    } else {
      car.x += dirX * moveD;
      car.y += dirY * moveD;
    }

    // Update color/opacity from latest consumed or nearest waypoint
    // (color stays from last consumed waypoint, already updated above on consume)

    // Angle follows movement direction
    if (car.speed > 0.1) {
      const moveAngle = Math.atan2(
        car.y - (car.y - dirY * moveD),
        car.x - (car.x - dirX * moveD),
      ) * 180 / Math.PI;
      let da = moveAngle - car.angle;
      while (da > 180) da -= 360;
      while (da < -180) da += 360;
      car.angle += da * cfg.angleRate;
    }
  }

  function step() {
    for (const id in cars) {
      const car = cars[id];
      stepCar(car);
      // Advance fade
      car.fadeAge = Math.min(1, car.fadeAge + 1 / cfg.fadeInFrames);
    }
  }

  function getCar(id) {
    const car = cars[id];
    if (!car) return null;

    // During fade-in, force flow color
    const color = car.fadeAge < 1 ? 'flow' : car.color;

    // Composite opacity = fade × sim opacity
    const opacity = car.fadeAge * car.opacity;

    return {
      x: car.x,
      y: car.y,
      angle: car.angle,
      opacity: opacity,
      color: color,
      fadeAge: car.fadeAge,
    };
  }

  function getAllCars() {
    const result = [];
    for (const id in cars) {
      const vis = getCar(id);
      if (vis) {
        vis.id = id;
        result.push(vis);
      }
    }
    return result;
  }

  function hasCar(id) {
    return id in cars;
  }

  return {
    addCar: addCar,
    removeCar: removeCar,
    pushTarget: pushTarget,
    step: step,
    getCar: getCar,
    getAllCars: getAllCars,
    hasCar: hasCar,
  };
}
