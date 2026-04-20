async page => {
  await page.waitForTimeout(5000);
  const cars = await page.evaluate(() => {
    const g = document.getElementById('hw-cars');
    return [...g.children].map((el, i) => {
      const t = el.getAttribute('transform') || '';
      const m = t.match(/translate\(([\d.-]+),([\d.-]+)\)/);
      return m ? { i, x: +(+m[1]).toFixed(1), y: +(+m[2]).toFixed(1) } : null;
    }).filter(Boolean);
  });
  // Count distinct positions
  const posMap = {};
  cars.forEach(c => {
    const key = c.x + ',' + c.y;
    posMap[key] = (posMap[key] || 0) + 1;
  });
  return JSON.stringify({ total: cars.length, positions: posMap, sample: cars.slice(0, 10) }, null, 2);
}
