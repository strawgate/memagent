async (page) => {
  await page.waitForTimeout(2000);
  return await page.evaluate(() => {
    return new Promise(resolve => {
      const results = [];
      let frame = 0;
      const interval = setInterval(() => {
        const cars = document.querySelectorAll('#hw-cars > *');
        const snapshot = [];
        cars.forEach(c => {
          const t = c.getAttribute('transform');
          if (!t) return;
          const m = t.match(/translate\(([\d.-]+),\s*([\d.-]+)\)/);
          if (m) {
            snapshot.push({ id: c.getAttribute('data-car-id') || '?', x: +m[1], y: +m[2] });
          }
        });
        results.push({ f: frame, cars: snapshot });
        frame++;
        if (frame >= 80) {
          clearInterval(interval);
          const carJumps = {};
          for (let i = 1; i < results.length; i++) {
            const prev = {};
            results[i-1].cars.forEach(c => prev[c.id] = c);
            results[i].cars.forEach(c => {
              if (prev[c.id]) {
                const dx = c.x - prev[c.id].x;
                const dy = c.y - prev[c.id].y;
                const dist = Math.sqrt(dx*dx + dy*dy);
                if (dist > 8) {
                  if (!carJumps[c.id]) carJumps[c.id] = [];
                  carJumps[c.id].push({ f: i, dist: +(dist.toFixed(1)), dx: +(dx.toFixed(1)), dy: +(dy.toFixed(1)) });
                }
              }
            });
          }
          const jumpCount = Object.values(carJumps).reduce((s, a) => s + a.length, 0);
          resolve(JSON.stringify({ totalBigJumps: jumpCount, jumps: carJumps }, null, 2));
        }
      }, 25);
    });
  });
}
