async page => {
  await page.waitForTimeout(3000);
  
  const beforeArr = await page.evaluate(() => {
    const g = document.getElementById('hw-cars');
    return [...g.children].map(el => el.getAttribute('data-car-id')).filter(Boolean);
  });
  const beforeSet = new Set(beforeArr);
  
  let newDataId = null;
  for (let w = 0; w < 300 && !newDataId; w++) {
    await page.waitForTimeout(25);
    const nowArr = await page.evaluate(() => {
      const g = document.getElementById('hw-cars');
      return [...g.children].map(el => el.getAttribute('data-car-id')).filter(Boolean);
    });
    for (const did of nowArr) {
      if (!beforeSet.has(did)) { newDataId = did; break; }
    }
  }
  if (!newDataId) return 'timeout';
  
  const frames = [];
  for (let f = 0; f < 50; f++) {
    await page.waitForTimeout(25);
    const data = await page.evaluate((did) => {
      const g = document.getElementById('hw-cars');
      for (const el of g.children) {
        if (el.getAttribute('data-car-id') === did) {
          const t = el.getAttribute('transform') || '';
          const m = t.match(/translate\(([\d.-]+),([\d.-]+)\)\s*rotate\(([\d.-]+)\)/);
          return m ? { x: +m[1], y: +m[2], r: +m[3], op: el.getAttribute('opacity') } : null;
        }
      }
      return null;
    }, newDataId);
    if (data) frames.push({ f, x: +data.x.toFixed(1), y: +data.y.toFixed(1), r: +data.r.toFixed(1), op: data.op });
    else frames.push({ f, gone: true });
  }
  return JSON.stringify({ id: newDataId, frames }, null, 2);
}
