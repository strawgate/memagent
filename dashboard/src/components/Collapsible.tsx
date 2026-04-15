import type { ComponentChildren } from "preact";
import { useState } from "preact/hooks";

interface Props {
  title: string;
  /** Start open instead of collapsed. */
  defaultOpen?: boolean;
  /** Optional badge shown next to the title. */
  badge?: ComponentChildren;
  children: ComponentChildren;
}

/**
 * Collapsible section with a clickable header.
 * Content is unmounted when collapsed (saves DOM + canvas resources).
 */
export function Collapsible({ title, defaultOpen = false, badge, children }: Props) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div class={`collapsible ${open ? "open" : ""}`}>
      <button
        type="button"
        class="collapsible-header"
        onClick={() => setOpen(!open)}
        aria-expanded={open}
      >
        <span class="collapsible-arrow">{open ? "▾" : "▸"}</span>
        <span class="collapsible-title">{title}</span>
        {badge && <span class="collapsible-badge">{badge}</span>}
      </button>
      {open && <div class="collapsible-body">{children}</div>}
    </div>
  );
}
