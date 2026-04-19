async page => {
  await page.waitForTimeout(2000);
  const overlaps = [];
  for (let f = 0; f < 200; f++) {
    await page.waitForTimeout(25);
    const cars = await page.evaluate(() => {
      const carsG = document.getElementById('hw-cars');
      if (!carsG) return [];
      return [...carsG.children].map(el => {
        const t = el.getAttribute('transform') || '';
        const m = t.match(/translate\(([\d.-]+),([\d.-]+)\)/);
        return m ? { id: el.id || 'anon', x: +m[1], y: +m[2] } : null;
      }).filter(Boolean);
    });
    for (let i = 0; i < cars.length; i++) {
      for (let j = i+1; j < cars.length; j++) {
        const dx = cars[i].x - cars[j].x;
        const dy = cars[i].y - cars[j].y;
        const dist = Math.sqrt(dx*dx + dy*dy);
        if (dist < 18) {
          overlaps.push({
            frame: f,
            a: { id: cars[i].id, x: +cars[i].x.toFixed(1), y: +cars[i].y.toFixed(1) },
            b: { id: cars[j].id, x: +cars[j].x.toFixed(1), y: +cars[j].y.toFixed(1) },
            dist: +dist.toFixed(1)
          });
        }
      }
    }
  }
  return JSON.stringify({ total: overlaps.length, samples: overlaps.slice(0, 20) }, null, 2);
}
