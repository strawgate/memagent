async page => {
  await page.waitForTimeout(2000);
  const info = await page.evaluate(() => {
    const allPaths = document.querySelectorAll('path');
    const pathData = [...allPaths].map(p => ({ id: p.id, d: p.getAttribute('d')?.substring(0, 80), stroke: p.getAttribute('stroke') }));
    const allCars = document.querySelectorAll('.car');
    const carSample = [...allCars].slice(0, 5).map(el => ({
      id: el.id,
      transform: el.getAttribute('transform'),
      fill: el.querySelector('rect')?.getAttribute('fill')
    }));
    return { paths: pathData, cars: carSample, totalCars: allCars.length };
  });
  return JSON.stringify(info, null, 2);
}
