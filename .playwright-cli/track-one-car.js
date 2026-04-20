async page => {
  await page.waitForTimeout(4000);
  // Snapshot all car positions, wait for a new one, track it by index
  const getAll = () => page.evaluate(() => {
    const g = document.getElementById('hw-cars');
    return [...g.children].map((el, i) => {
      const t = el.getAttribute('transform') || '';
      const m = t.match(/translate\(([\d.-]+),([\d.-]+)\)\s*rotate\(([\d.-]+)\)/);
      return m ? { i, x: +m[1], y: +m[2], r: +m[3], op: el.getAttribute('opacity') } : null;
    }).filter(Boolean);
  });
  
  const before = await getAll();
  const beforeCount = before.length;
  
  // Wait for new car
  let newIdx = -1;
  for (let w = 0; w < 200; w++) {
    await page.waitForTimeout(25);
    const now = await getAll();
    if (now.length > beforeCount) {
      newIdx = now.length - 1;
      break;
    }
  }
  if (newIdx === -1) return 'no new car';
  
  // Track by walking the children at that index for 50 frames
  const frames = [];
  for (let f = 0; f < 50; f++) {
    await page.waitForTimeout(25);
    const all = await getAll();
    // Find the car we're tracking — it should still be at newIdx or nearby
    const car = all[newIdx];
    if (car) frames.push({ f, x: car.x.toFixed(1), y: car.y.toFixed(1), r: car.r.toFixed(1), op: car.op });
  }
  return JSON.stringify(frames.slice(0, 30), null, 2);
}
