async page => {
  // Watch for 10 seconds at high frequency, detect any pair <25px apart
  await page.waitForTimeout(2000);
  const overlaps = [];
  const seenPairs = new Set();
  for (let f = 0; f < 400; f++) {
    await page.waitForTimeout(25);
    const cars = await page.evaluate(() => {
      const g = document.getElementById('hw-cars');
      if (!g) return [];
      return [...g.children].map(el => {
        const t = el.getAttribute('transform') || '';
        const m = t.match(/translate\(([\d.-]+),([\d.-]+)\)/);
        const did = el.getAttribute('data-car-id');
        return m ? { id: did, x: +m[1], y: +m[2] } : null;
      }).filter(Boolean);
    });
    for (let i = 0; i < cars.length; i++) {
      for (let j = i+1; j < cars.length; j++) {
        const dx = cars[i].x - cars[j].x;
        const dy = cars[i].y - cars[j].y;
        const dist = Math.sqrt(dx*dx + dy*dy);
        if (dist < 25) {
          const pairKey = [cars[i].id, cars[j].id].sort().join('-');
          if (!seenPairs.has(pairKey)) {
            seenPairs.add(pairKey);
            overlaps.push({
              f, dist: +dist.toFixed(1),
              a: { id: cars[i].id, x: +cars[i].x.toFixed(1), y: +cars[i].y.toFixed(1) },
              b: { id: cars[j].id, x: +cars[j].x.toFixed(1), y: +cars[j].y.toFixed(1) },
            });
          }
        }
      }
    }
  }
  return JSON.stringify({ uniquePairs: overlaps.length, overlaps: overlaps.slice(0, 20) }, null, 2);
}
