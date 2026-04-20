async page => {
  await page.waitForTimeout(3000);
  const overlaps = [];
  for (let f = 0; f < 200; f++) {
    await page.waitForTimeout(25);
    const cars = await page.evaluate(() => {
      const carsG = document.getElementById('hw-cars');
      if (!carsG) return [];
      return [...carsG.children].map(el => {
        const t = el.getAttribute('transform') || '';
        const m = t.match(/translate\(([\d.-]+),([\d.-]+)\)/);
        return m ? { x: +m[1], y: +m[2] } : null;
      }).filter(Boolean);
    });
    // Check overlaps near exit junction (x > 520)
    const exitArea = cars.filter(c => c.x >= 520 && c.x <= 620);
    for (let i = 0; i < exitArea.length; i++) {
      for (let j = i+1; j < exitArea.length; j++) {
        const dx = exitArea[i].x - exitArea[j].x;
        const dy = exitArea[i].y - exitArea[j].y;
        const dist = Math.sqrt(dx*dx + dy*dy);
        if (dist < 18) {
          overlaps.push({ f, a: exitArea[i], b: exitArea[j], d: +dist.toFixed(1) });
        }
      }
    }
  }
  return JSON.stringify({ total: overlaps.length, samples: overlaps.slice(0, 10) }, null, 2);
}
