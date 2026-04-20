async page => {
  await page.waitForTimeout(3000);
  
  // Get all current car IDs  
  const getCarIds = () => page.evaluate(() => {
    const g = document.getElementById('hw-cars');
    return [...g.children].map(el => el.id || 'no-id');
  });
  
  // Wait for element count to increase — that's a spawn
  const beforeIds = new Set(await getCarIds());
  
  let newId = null;
  for (let w = 0; w < 300 && !newId; w++) {
    await page.waitForTimeout(25);
    const nowIds = await getCarIds();
    for (const id of nowIds) {
      if (!beforeIds.has(id) && id !== 'no-id') { newId = id; break; }
    }
  }
  if (!newId) return 'timeout';
  
  // Track this specific car by ID for 40 frames
  const frames = [];
  for (let f = 0; f < 40; f++) {
    await page.waitForTimeout(25);
    const data = await page.evaluate((targetId) => {
      const el = document.getElementById(targetId);
      if (!el) return null;
      const t = el.getAttribute('transform') || '';
      const m = t.match(/translate\(([\d.-]+),([\d.-]+)\)\s*rotate\(([\d.-]+)\)/);
      return m ? { x: +m[1], y: +m[2], r: +m[3], op: el.getAttribute('opacity') } : null;
    }, newId);
    if (data) frames.push({ f, x: data.x.toFixed(1), y: data.y.toFixed(1), r: data.r.toFixed(1), op: data.op });
    else frames.push({ f, gone: true });
  }
  return JSON.stringify({ id: newId, frames: frames }, null, 2);
}
