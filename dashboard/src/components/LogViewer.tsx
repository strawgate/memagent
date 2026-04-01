import { useState, useEffect, useRef } from "preact/hooks";

interface LogsResponse {
  lines: string[];
  capturing: boolean;
}

export function LogViewer() {
  const [logs, setLogs] = useState<string[]>([]);
  const [capturing, setCapturing] = useState(false);
  const [open, setOpen] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;

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
      }
    };

    poll();
    const id = setInterval(poll, 2000);
    return () => clearInterval(id);
  }, [open]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [logs.length]);

  if (!open) {
    return (
      <div
        class="log-prompt"
        onClick={() => setOpen(true)}
      >
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none" style="flex-shrink:0">
          <rect x="1" y="2" width="14" height="12" rx="2" stroke="currentColor" stroke-width="1.2" />
          <line x1="4" y1="6" x2="12" y2="6" stroke="currentColor" stroke-width="1" opacity="0.5" />
          <line x1="4" y1="9" x2="10" y2="9" stroke="currentColor" stroke-width="1" opacity="0.5" />
        </svg>
        <span>Stream process logs</span>
        <span class="log-prompt-hint">stderr output will appear here</span>
      </div>
    );
  }

  return (
    <div class="log-box">
      <div class="log-header">
        <div class="log-header-left">
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none" style="flex-shrink:0">
            <rect x="1" y="2" width="14" height="12" rx="2" stroke="currentColor" stroke-width="1.2" />
            <line x1="4" y1="6" x2="12" y2="6" stroke="currentColor" stroke-width="1" opacity="0.5" />
            <line x1="4" y1="9" x2="10" y2="9" stroke="currentColor" stroke-width="1" opacity="0.5" />
          </svg>
          <span>Process Logs</span>
          {capturing && <span class="log-live">● live</span>}
          <span class="log-count">{logs.length} lines</span>
        </div>
        <button class="log-close" onClick={() => setOpen(false)}>✕</button>
      </div>
      <div class="log-output">
        {logs.length === 0 ? (
          <div class="log-empty">
            {capturing
              ? "Capturing stderr… waiting for output."
              : "Activating capture…"}
          </div>
        ) : (
          logs.map((line, i) => (
            <div key={i} class="log-line">{line}</div>
          ))
        )}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
