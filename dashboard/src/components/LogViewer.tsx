import { useState, useEffect, useRef } from "preact/hooks";

interface LogsResponse {
  lines: string[];
  capturing: boolean;
}

export function LogViewer() {
  const [logs, setLogs] = useState<string[]>([]);
  const [capturing, setCapturing] = useState(false);
  const [open, setOpen] = useState(true);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout>;

    const poll = async () => {
      try {
        const res = await fetch("/api/logs");
        if (res.ok) {
          const data: LogsResponse = await res.json();
          setLogs(data.lines);
          setCapturing(data.capturing);
        }
      } catch {
        // ignore
      } finally {
        if (!cancelled) timer = setTimeout(poll, 2000);
      }
    };

    poll();
    return () => { cancelled = true; clearTimeout(timer); };
  }, [open]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [logs.length]);

  const icon = (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" style="flex-shrink:0">
      <rect x="1" y="2" width="14" height="12" rx="2" stroke="currentColor" stroke-width="1.2" />
      <line x1="4" y1="6" x2="12" y2="6" stroke="currentColor" stroke-width="1" opacity="0.5" />
      <line x1="4" y1="9" x2="10" y2="9" stroke="currentColor" stroke-width="1" opacity="0.5" />
    </svg>
  );

  return (
    <div class="log-box">
      <div class="log-header">
        <div class="log-header-left">
          {icon}
          <span>Process Logs</span>
          {open && capturing && <span class="log-live">● live</span>}
          {open && <span class="log-count">{logs.length} lines</span>}
        </div>
        <button class="log-close" onClick={() => setOpen(!open)} aria-label={open ? "Collapse log viewer" : "Expand log viewer"}>
          {open ? "−" : "+"}
        </button>
      </div>
      {open && (
        <div class="log-output">
          {logs.length === 0 ? (
            <div class="log-empty">
              {capturing ? "Waiting for output…" : "No logs yet."}
            </div>
          ) : (
            logs.map((line, i) => (
              <div key={i} class="log-line">{line}</div>
            ))
          )}
          <div ref={bottomRef} />
        </div>
      )}
    </div>
  );
}
