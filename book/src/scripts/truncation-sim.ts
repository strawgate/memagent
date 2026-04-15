import { genLine } from './log-line';

export interface TruncationTickResult {
  lineNum: number;
  lineText: string;
  newOffset: number;
}

export interface TruncationSim {
  readonly lineNum: number;
  readonly offset: number;
  readonly linesRead: number;
  readonly truncations: number;
  /** Always 12345 — truncation never changes the inode. */
  readonly fileInode: number;
  /** Append one line. Returns metadata. */
  tick(): TruncationTickResult;
  /** Simulate a truncation: offset resets to 0, lineNum resets to 1, truncations++. */
  truncate(): void;
}

export function makeTruncationSim(): TruncationSim {
  let lineNum = 1;
  let offset = 0;
  let linesRead = 0;
  let truncations = 0;
  const fileInode = 12345; // const: truncation never changes the inode

  function tick(): TruncationTickResult {
    const currentLineNum = lineNum;
    const lineText = genLine(currentLineNum);
    lineNum++;
    offset += lineText.length + 1;
    linesRead++;
    return { lineNum: currentLineNum, lineText, newOffset: offset };
  }

  function truncate(): void {
    truncations++;
    offset = 0;
    lineNum = 1;
  }

  return {
    get lineNum() { return lineNum; },
    get offset() { return offset; },
    get linesRead() { return linesRead; },
    get truncations() { return truncations; },
    get fileInode() { return fileInode; },
    tick,
    truncate,
  };
}
