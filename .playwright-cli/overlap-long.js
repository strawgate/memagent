async page => {
  await page.waitForTimeout(2000);
  const overlaps = [];
  for (let f = 0; f < 100; f++) {
    await page.waitForTimeout(50);
    const cars = await page.evaluate(() => {
      const els = document.querySelectorAll('.car');
      return [...els].map(el => {
        const t = el.getAttribute('transform') || '';
        const m = t.match(/translate\(([\d.-]+),([\d.-]+)\)/);
        return m ? { id: el.id, x: +m[1], y: +m[2] } : null;
      }).filter(Boolean);
    });
    for (let i = 0; i < cars.length; i++) {
      for (let j = i+1; j < cars.length; j++) {
        const dx = cars[i].x - cars[j].x;
        const dy = cars[i].y - cars[j].y;
        const dist = Math.sqrt(dx*dx + dy*dy);
        if (dist < 15) {
          overlaps.push({
            frame: f,
            a: cars[i],
            b: cars[j],
            dist: dist.toFixed(1)
          });
        }
      }
    }
  }
  return JSON.stringify({ total: overlaps.length, samples: overlaps.slice(0, 10) }, null, 2);
}
