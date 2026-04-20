async page => {
  await page.waitForTimeout(3000);
  // Get path data to understand geometry
  const paths = await page.evaluate(() => {
    const result = {};
    for (const id of ['hwyPath', 'rampPath', 'exitPath', 'contPath']) {
      const el = document.getElementById(id);
      if (el) result[id] = el.getAttribute('d');
    }
    return result;
  });
  
  // Track specific cars near merge area over time
  const frames = [];
  for (let f = 0; f < 40; f++) {
    await page.waitForTimeout(25);
    const cars = await page.evaluate(() => {
      const els = document.querySelectorAll('.car');
      return [...els].map(el => {
        const t = el.getAttribute('transform') || '';
        const m = t.match(/translate\(([\d.-]+),([\d.-]+)\)/);
        if (!m) return null;
        const x = +m[1], y = +m[2];
        // Include cars in the merge region: x 100-250, y 80-180
        if (x >= 100 && x <= 250 && y >= 80 && y <= 180) {
          return { id: el.id, x: x.toFixed(1), y: y.toFixed(1) };
        }
        return null;
      }).filter(Boolean);
    });
    if (cars.length > 0) frames.push({ f, cars });
  }
  return JSON.stringify({ paths, mergeFrames: frames.slice(0, 20) }, null, 2);
}
