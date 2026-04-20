async page => {
  await page.waitForTimeout(3000); // let cars populate
  // Sample 10 frames checking for overlap near merge point (x~200-220, y~100)
  const results = [];
  for (let f = 0; f < 20; f++) {
    await page.waitForTimeout(50);
    const cars = await page.evaluate(() => {
      const els = document.querySelectorAll('.car');
      return [...els].map(el => {
        const t = el.getAttribute('transform') || '';
        const m = t.match(/translate\(([\d.]+),([\d.]+)\)/);
        return m ? { id: el.id, x: +m[1], y: +m[2] } : null;
      }).filter(Boolean);
    });
    // Find cars close to merge area (x: 150-250)
    const mergeArea = cars.filter(c => c.x >= 150 && c.x <= 260);
    if (mergeArea.length >= 2) {
      // Check pairwise distances
      for (let i = 0; i < mergeArea.length; i++) {
        for (let j = i+1; j < mergeArea.length; j++) {
          const dx = mergeArea[i].x - mergeArea[j].x;
          const dy = mergeArea[i].y - mergeArea[j].y;
          const dist = Math.sqrt(dx*dx + dy*dy);
          if (dist < 20) {
            results.push({
              frame: f,
              a: mergeArea[i],
              b: mergeArea[j],
              dist: dist.toFixed(1)
            });
          }
        }
      }
    }
  }
  return JSON.stringify(results, null, 2);
}
