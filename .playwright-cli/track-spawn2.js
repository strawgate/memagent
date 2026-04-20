async page => {
  // Wait for steady state, then find the next newly spawned car and track it
  await page.waitForTimeout(3000);
  
  // Get current car IDs
  const initialIds = await page.evaluate(() => {
    const carsG = document.getElementById('hw-cars');
    return [...carsG.children].map(el => el.getAttribute('transform'));
  });
  const initialCount = initialIds.length;
  
  // Poll for a new car
  let newCar = null;
  for (let i = 0; i < 100; i++) {
    await page.waitForTimeout(25);
    const current = await page.evaluate((prevCount) => {
      const carsG = document.getElementById('hw-cars');
      const children = [...carsG.children];
      if (children.length > prevCount) {
        const el = children[children.length - 1];
        return { transform: el.getAttribute('transform'), opacity: el.getAttribute('opacity') };
      }
      return null;
    }, initialCount);
    if (current) {
      newCar = current;
      break;
    }
  }
  
  if (!newCar) return JSON.stringify('No new car spawned');
  
  // Track this car (last child) for 25 frames
  const frames = [];
  const carsG = 'hw-cars';
  for (let f = 0; f < 30; f++) {
    await page.waitForTimeout(25);
    const data = await page.evaluate(() => {
      const g = document.getElementById('hw-cars');
      const el = g.children[g.children.length - 1];
      return { t: el.getAttribute('transform'), op: el.getAttribute('opacity') };
    });
    frames.push(data);
  }
  return JSON.stringify(frames, null, 2);
}
