// ================================================================
// Backpressure pipeline simulation engine — pure state, no DOM.
// ================================================================

export type SourceMode = 'file' | 'udp' | 'otlp';
export type StageState = 'ok' | 'pressured' | 'blocked';

export interface TransitionEvent {
  type: 'transition';
  stage: 'io' | 'cpu' | 'out';
  from: StageState;
  to: StageState;
  /** Present when stage === 'io' */
  sourceMode?: SourceMode;
  /** Present when stage === 'cpu' */
  pipelineFill?: number;
}

export interface OtlpEvent {
  type: 'otlp';
  batch: number;
  status: 200 | 429;
  note: string | null;
}

export type SimEvent = TransitionEvent | OtlpEvent;

// ----- Constants (also imported by the render layer) -----
export const IO_CPU_CAP = 4;
export const PIPELINE_CAP = 16;
export const WORKER_CAP = 1;
export const NUM_WORKERS = 3;
export const OUT_MAX_RATE = 2.4;
export const UDP_CAP = 16;

const MRU_DISPATCH_CAP = 0.8;
const IO_RATE = 1.2;
const CPU_RATE = 1.2;
const FILE_MAX = 400;
const FILE_WRITE_RATE = 0.55;
const UDP_ARRIVE_RATE = 1.8;

export function classifyState(fill: number, cap: number): StageState {
  const pct = fill / cap;
  if (pct >= 1.0) return 'blocked';
  if (pct > 0.5) return 'pressured';
  return 'ok';
}

export interface SimState {
  // Writable by the UI layer
  outputSpeed: number;
  sourceMode: SourceMode;
  // Pipeline fills (read by render layer)
  ioCpuFill: number;
  ioCpuFillSnap: number;
  pipelineFill: number;
  workerFills: number[];
  ioBlocked: boolean;
  cpuBlocked: boolean;
  // Counters
  totalProduced: number;
  totalDelivered: number;
  tickCount: number;
  // Source state
  fileWritten: number;
  fileRead: number;
  udpBuf: number;
  udpDrops: number;
  otlpBatchNum: number;
  otlpAccepted: number;
  otlpRejected: number;
  reset(): void;
  tick(): SimEvent[];
}

export function makeSim(): SimState {
  // Private transition-detection state (never exposed to render layer)
  let _prevIoState: StageState = 'ok';
  let _prevCpuState: StageState = 'ok';
  let _prevOutState: StageState = 'ok';

  const s: SimState = {
    outputSpeed: 100,
    sourceMode: 'file',
    ioCpuFill: 0, ioCpuFillSnap: 0,
    pipelineFill: 0, workerFills: [0, 0, 0],
    ioBlocked: false, cpuBlocked: false,
    totalProduced: 0, totalDelivered: 0,
    tickCount: 0,
    fileWritten: 0, fileRead: 0,
    udpBuf: 0, udpDrops: 0,
    otlpBatchNum: 0, otlpAccepted: 0, otlpRejected: 0,

    reset() {
      s.ioCpuFill = 0; s.ioCpuFillSnap = 0;
      s.pipelineFill = 0; s.workerFills = [0, 0, 0];
      s.ioBlocked = false; s.cpuBlocked = false;
      s.totalProduced = 0; s.totalDelivered = 0;
      s.tickCount = 0;
      s.fileWritten = 0; s.fileRead = 0;
      s.udpBuf = 0; s.udpDrops = 0;
      s.otlpBatchNum = 0; s.otlpAccepted = 0; s.otlpRejected = 0;
      _prevIoState = 'ok'; _prevCpuState = 'ok'; _prevOutState = 'ok';
    },

    tick(): SimEvent[] {
      s.tickCount++;
      const events: SimEvent[] = [];

      // Output workers drain
      const outDrain = (s.outputSpeed / 100) * OUT_MAX_RATE;
      const totalWorkerFill = s.workerFills.reduce((a, b) => a + b, 0);
      let remaining = Math.min(totalWorkerFill, outDrain);
      let tickDelivered = 0;
      for (let w = 0; w < NUM_WORKERS && remaining > 0; w++) {
        const take = Math.min(s.workerFills[w], remaining);
        s.workerFills[w] -= take; remaining -= take; tickDelivered += take;
      }
      s.totalDelivered += tickDelivered;

      // Pipeline drains into workers (MRU dispatch)
      for (let w2 = 0; w2 < NUM_WORKERS && s.pipelineFill > 0; w2++) {
        const space = WORKER_CAP - s.workerFills[w2];
        if (space > 0) {
          const send = Math.min(space, s.pipelineFill, MRU_DISPATCH_CAP);
          s.workerFills[w2] += send; s.pipelineFill -= send;
        }
      }

      // CPU worker drains I/O channel, fills pipeline
      s.cpuBlocked = s.pipelineFill >= PIPELINE_CAP;
      if (!s.cpuBlocked && s.ioCpuFill > 0) {
        const cpuDrain = Math.min(s.ioCpuFill, CPU_RATE, PIPELINE_CAP - s.pipelineFill);
        s.ioCpuFill -= cpuDrain; s.pipelineFill += cpuDrain; s.totalProduced += cpuDrain;
      }

      // I/O worker produces into I/O→CPU channel
      s.ioBlocked = s.ioCpuFill >= IO_CPU_CAP;
      s.ioCpuFillSnap = s.ioCpuFill; // pre-refill snapshot: used for stage coloring
      if (!s.ioBlocked) {
        s.ioCpuFill += Math.min(IO_RATE, IO_CPU_CAP - s.ioCpuFill);
      }

      // Clamp all fills
      s.ioCpuFill    = Math.max(0, Math.min(IO_CPU_CAP,   s.ioCpuFill));
      s.pipelineFill = Math.max(0, Math.min(PIPELINE_CAP, s.pipelineFill));
      for (let w3 = 0; w3 < NUM_WORKERS; w3++) {
        s.workerFills[w3] = Math.max(0, Math.min(WORKER_CAP, s.workerFills[w3]));
      }

      // Source side effects
      if (s.sourceMode === 'file') {
        s.fileWritten += FILE_WRITE_RATE;
        if (!s.ioBlocked) s.fileRead = Math.min(s.fileRead + IO_RATE * 0.85, s.fileWritten);
        if (s.fileWritten > FILE_MAX) {
          const fg = s.fileWritten - s.fileRead;
          s.fileWritten = fg + 50; s.fileRead = 50;
        }
      } else if (s.sourceMode === 'udp') {
        s.udpBuf += UDP_ARRIVE_RATE;
        if (!s.ioBlocked) s.udpBuf = Math.max(0, s.udpBuf - IO_RATE * 1.1);
        if (s.udpBuf > UDP_CAP) { s.udpDrops += (s.udpBuf - UDP_CAP); s.udpBuf = UDP_CAP; }
      } else {
        // OTLP: one batch every 6 ticks
        if (s.tickCount % 6 === 0) {
          s.otlpBatchNum++;
          if (s.ioBlocked) {
            s.otlpRejected++;
            events.push({ type: 'otlp', batch: s.otlpBatchNum, status: 429, note: 'retry' });
          } else {
            s.otlpAccepted++;
            events.push({ type: 'otlp', batch: s.otlpBatchNum, status: 200, note: null });
          }
        }
      }

      // Transition detection — emit events, never touch DOM
      const ioState = s.ioBlocked
        ? 'blocked'
        : (s.ioCpuFillSnap / IO_CPU_CAP > 0.5 ? 'pressured' : 'ok');
      const cpuState = classifyState(s.pipelineFill, PIPELINE_CAP);
      const workerBusy = s.workerFills.filter(f => f > 0.1).length;
      const allFull = s.workerFills.every(f => f >= WORKER_CAP * 0.9);
      const outState: StageState = allFull ? 'blocked' : (workerBusy >= 2 ? 'pressured' : 'ok');

      if (outState !== _prevOutState) {
        events.push({ type: 'transition', stage: 'out', from: _prevOutState, to: outState });
        _prevOutState = outState;
      }
      if (cpuState !== _prevCpuState) {
        events.push({ type: 'transition', stage: 'cpu', from: _prevCpuState, to: cpuState, pipelineFill: Math.ceil(s.pipelineFill) });
        _prevCpuState = cpuState;
      }
      if (ioState !== _prevIoState) {
        events.push({ type: 'transition', stage: 'io', from: _prevIoState, to: ioState, sourceMode: s.sourceMode });
        _prevIoState = ioState;
      }

      return events;
    },
  };

  return s;
}
