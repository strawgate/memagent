async page => {
  const data = await page.evaluate(() => {
    const pathRamp = document.getElementById('hw-path-ramp');
    const pathHwy = document.getElementById('hw-path-hwy');
    const pathExit = document.getElementById('hw-path-exit');
    const pathCont = document.getElementById('hw-path-cont');
    const lenRamp = pathRamp.getTotalLength();
    const lenHwy = pathHwy.getTotalLength();
    const lenExit = pathExit.getTotalLength();
    const lenCont = pathCont.getTotalLength();
    
    const rampEnd = pathRamp.getPointAtLength(lenRamp);
    const mergeD = (5/14) * lenHwy;
    const hwyMerge = pathHwy.getPointAtLength(mergeD);
    const hwyEnd = pathHwy.getPointAtLength(lenHwy);
    const exitStart = pathExit.getPointAtLength(0);
    const contStart = pathCont.getPointAtLength(0);
    
    // Check slot positions near junctions
    const rampSlots = [];
    for (let i = 0; i < 5; i++) {
      const d = i * lenRamp / 4;
      const p = pathRamp.getPointAtLength(d);
      rampSlots.push({slot: i, d: d.toFixed(1), x: p.x.toFixed(1), y: p.y.toFixed(1)});
    }
    const hwySlots = [];
    for (let i = 0; i < 15; i++) {
      const d = i * lenHwy / 14;
      const p = pathHwy.getPointAtLength(d);
      hwySlots.push({slot: i, d: d.toFixed(1), x: p.x.toFixed(1), y: p.y.toFixed(1)});
    }
    
    return JSON.stringify({
      rampEnd: {x: rampEnd.x.toFixed(1), y: rampEnd.y.toFixed(1)},
      hwyMerge: {x: hwyMerge.x.toFixed(1), y: hwyMerge.y.toFixed(1)},
      hwyEnd: {x: hwyEnd.x.toFixed(1), y: hwyEnd.y.toFixed(1)},
      exitStart: {x: exitStart.x.toFixed(1), y: exitStart.y.toFixed(1)},
      contStart: {x: contStart.x.toFixed(1), y: contStart.y.toFixed(1)},
      rampSlots,
      hwySlots4to6: hwySlots.slice(4, 7),
      hwySlots13to14: hwySlots.slice(13, 15),
    }, null, 2);
  });
  return data;
}
