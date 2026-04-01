import { useState, useEffect } from "preact/hooks";
import { api } from "../api";
import type { ConfigResponse } from "../types";

function highlightYaml(yaml: string): string {
  const el = document.createElement("div");
  el.textContent = yaml;
  let s = el.innerHTML;
  // Comments first (before injecting HTML with special chars)
  s = s.replace(/(#.*)$/gm, '<span class="yc">$1</span>');
  // Keys
  s = s.replace(/^(\s*)([\w._-]+)(:)/gm, '$1<span class="yk">$2</span>$3');
  // Numbers
  s = s.replace(/:\s+(\d+(?:\.\d+)?)\s*$/gm, ': <span class="yn">$1</span>');
  // Booleans
  s = s.replace(/:\s+(true|false)\s*$/gm, ': <span class="yn">$1</span>');
  return s;
}

export function ConfigView() {
  const [config, setConfig] = useState<ConfigResponse | null>(null);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    if (!loaded) {
      setLoaded(true);
      api.config().then((data) => {
        if (data) setConfig(data);
      });
    }
  }, [loaded]);

  return (
    <div class="section">
      <div class="heading">Config</div>
      {config?.path && (
        <div class="cfg-src">
          Source: <b>{config.path}</b>
        </div>
      )}
      {config ? (
        <div
          class="yaml"
          dangerouslySetInnerHTML={{ __html: highlightYaml(config.raw_yaml || "(no config)") }}
        />
      ) : (
        <div class="yaml" style="color:var(--t4)">loading&hellip;</div>
      )}
    </div>
  );
}
