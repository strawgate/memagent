const LEVELS = ['INFO', 'INFO', 'DEBUG', 'INFO', 'WARN', 'INFO', 'INFO', 'ERROR', 'INFO', 'INFO'] as const;
const MSGS = ['request handled', 'conn opened', 'query exec', 'cache hit', 'slow resp', 'health ok', 'batch done', 'conn refused', 'session new', 'auth ok'] as const;

/** Generate a synthetic JSON log line for a given sequence number. */
export function genLine(n: number): string {
  return `{"level":"${LEVELS[n % LEVELS.length]}","msg":"${MSGS[n % MSGS.length]}","n":${n}}`;
}
