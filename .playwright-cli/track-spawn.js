async page => {
  const data = await page.evaluate(() => {
    return new Promise(resolve => {
      const carsG = document.getElementById('hw-cars');
      const samples = [];
      let tracking = null;
      const obs = new MutationObserver(muts => {
        for (const m of muts) {
          for (const n of m.addedNodes) {
            if (n.tagName === 'g' && !tracking) {
              tracking = n;
            }
          }
        }
      });
      obs.observe(carsG, { childList: true });
      const iv = setInterval(() => {
        if (tracking) {
          samples.push({
            t: tracking.getAttribute('transform'),
            op: tracking.getAttribute('opacity'),
          });
        }
        if (samples.length >= 25) {
          clearInterval(iv);
          obs.disconnect();
          resolve(samples);
        }
      }, 25);
    });
  });
  return JSON.stringify(data, null, 2);
}
