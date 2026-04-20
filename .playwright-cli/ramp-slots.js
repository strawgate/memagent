async page => {
  await page.waitForTimeout(1000);
  const data = await page.evaluate(() => {
    const path = document.getElementById('hw-path-ramp');
    const len = path.getTotalLength();
    const slots = 4;
    const positions = [];
    for (let i = 0; i < slots; i++) {
      const d = i * len / (slots - 1);
      const p = path.getPointAtLength(d);
      positions.push({ slot: i, d: d.toFixed(1), x: p.x.toFixed(1), y: p.y.toFixed(1) });
    }
    // Also check highway slots near merge area
    const hwy = document.getElementById('hw-path-hwy');
    const hwyLen = hwy.getTotalLength();
    const hwySlots = [];
    for (let i = 3; i <= 7; i++) {
      const d = i * hwyLen / 14;
      const p = hwy.getPointAtLength(d);
      hwySlots.push({ slot: i, d: d.toFixed(1), x: p.x.toFixed(1), y: p.y.toFixed(1) });
    }
    return { rampLen: len.toFixed(1), rampSlots: positions, hwySlots };
  });
  return JSON.stringify(data, null, 2);
}
