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
    // Auto-scroll to bottom when new lines arrive.
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [logs.length]);

  return (
    <div class="section">
      <div
        class="heading"
        style="cursor:pointer;user-select:none"
        onClick={() => setOpen(!open)}
      >
        {open ? "▾" : "▸"} Process Logs
        {capturing && (
          <span style="margin-left:8px;font-size:9px;color:var(--ok);font-weight:normal;text-transform:none;letter-spacing:0">
            ● capturing
          </span>
        )}
        {!open && (
          <span style="margin-left:8px;font-size:9px;color:var(--t4);font-weight:normal;text-transform:none;letter-spacing:0">
            click to start capture
          </span>
        )}
      </div>
      {open && (
        <div
          style={{
            background: "var(--bg)",
            border: "1px solid var(--border)",
            borderRadius: "var(--r)",
            padding: "10px 14px",
            fontFamily: "var(--mono)",
            fontSize: "11px",
            lineHeight: "1.6",
            maxHeight: "300px",
            overflowY: "auto",
            color: "var(--t2)",
          }}
        >
          {logs.length === 0 ? (
            <div style="color:var(--t4)">
              {capturing
                ? "Capturing stderr… waiting for output."
                : "Activating capture…"}
            </div>
          ) : (
            logs.map((line, i) => (
              <div key={i} style="white-space:pre-wrap;word-break:break-all">
                {line}
              </div>
            ))
          )}
          <div ref={bottomRef} />
        </div>
      )}
    </div>
  );
}
