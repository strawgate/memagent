import { genLine } from './log-line';

export const AUTO_THRESHOLD = 30;

export interface TickResult {
  /** Line number that was just appended (1-based). */
  lineNum: number;
  lineText: string;
  newOffset: number;
  /** True when autoLineCount hit AUTO_THRESHOLD this tick. Caller should call rotate(). */
  autoRotated: boolean;
}

export interface RotateResult {
  oldInode: number;
  newInode: number;
}

export interface TailingSim {
  readonly lineNum: number;
  readonly offset: number;
  readonly linesRead: number;
  readonly fileInode: number;
  readonly autoLineCount: number;
  /** Append one line. Returns metadata; may set autoRotated=true. */
  tick(): TickResult;
  /** Perform a log rotation: inode flips, offset/lineNum/autoLineCount reset. */
  rotate(): RotateResult;
}

export function makeTailingSim(): TailingSim {
  let lineNum = 1;
  let offset = 0;
  let linesRead = 0;
  let fileInode = 12345;
  let autoLineCount = 0;

  function tick(): TickResult {
    const currentLineNum = lineNum;
    const lineText = genLine(currentLineNum);
    offset += lineText.length + 1;
    lineNum++;
    linesRead++;
    autoLineCount++;
    const autoRotated = autoLineCount >= AUTO_THRESHOLD;
    return { lineNum: currentLineNum, lineText, newOffset: offset, autoRotated };
  }

  function rotate(): RotateResult {
    const oldInode = fileInode;
    fileInode = fileInode === 12345 ? 67890 : 12345;
    offset = 0;
    lineNum = 1;
    autoLineCount = 0;
    return { oldInode, newInode: fileInode };
  }

  return {
    get lineNum() { return lineNum; },
    get offset() { return offset; },
    get linesRead() { return linesRead; },
    get fileInode() { return fileInode; },
    get autoLineCount() { return autoLineCount; },
    tick,
    rotate,
  };
}
