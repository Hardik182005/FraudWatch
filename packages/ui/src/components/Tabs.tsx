import { useRef, type KeyboardEvent, type ReactNode } from 'react';

export interface TabItem {
  id: string;
  label: ReactNode;
}

/** WAI-ARIA tabs (automatic activation, arrow/Home/End keys). Render panels with <TabPanel>. */
export function Tabs({ items, value, onChange, label, idBase }: { items: TabItem[]; value: string; onChange: (id: string) => void; label: string; idBase: string }) {
  const refs = useRef<Array<HTMLButtonElement | null>>([]);
  const onKey = (e: KeyboardEvent, i: number) => {
    let next = -1;
    if (e.key === 'ArrowRight') next = (i + 1) % items.length;
    else if (e.key === 'ArrowLeft') next = (i - 1 + items.length) % items.length;
    else if (e.key === 'Home') next = 0;
    else if (e.key === 'End') next = items.length - 1;
    if (next >= 0) {
      e.preventDefault();
      onChange(items[next]!.id);
      refs.current[next]?.focus();
    }
  };
  return (
    <div className="fw-tabs" role="tablist" aria-label={label}>
      {items.map((t, i) => (
        <button
          key={t.id}
          ref={(el) => {
            refs.current[i] = el;
          }}
          role="tab"
          type="button"
          className="fw-tab"
          id={`${idBase}-tab-${t.id}`}
          aria-controls={`${idBase}-panel-${t.id}`}
          aria-selected={value === t.id}
          tabIndex={value === t.id ? 0 : -1}
          onClick={() => onChange(t.id)}
          onKeyDown={(e) => onKey(e, i)}
        >
          {t.label}
        </button>
      ))}
    </div>
  );
}

export function TabPanel({ idBase, id, children }: { idBase: string; id: string; children: ReactNode }) {
  return (
    <div className="fw-tabpanel" role="tabpanel" id={`${idBase}-panel-${id}`} aria-labelledby={`${idBase}-tab-${id}`} tabIndex={0}>
      {children}
    </div>
  );
}

export interface SegmentOption<V extends string> {
  value: V;
  label: ReactNode;
  disabled?: boolean;
  title?: string;
}

/** Radio-group style segmented control. */
export function Segmented<V extends string>({ options, value, onChange, label }: { options: SegmentOption<V>[]; value: V; onChange: (v: V) => void; label: string }) {
  const refs = useRef<Array<HTMLButtonElement | null>>([]);
  const enabled = options.map((o, i) => (o.disabled ? -1 : i)).filter((i) => i >= 0);
  const selectedIdx = options.findIndex((o) => o.value === value && !o.disabled);
  const tabStop = selectedIdx >= 0 ? selectedIdx : enabled[0];
  const onKey = (e: KeyboardEvent, i: number) => {
    const pos = enabled.indexOf(i);
    let next = -1;
    if (e.key === 'ArrowRight' || e.key === 'ArrowDown') next = enabled[(pos + 1) % enabled.length]!;
    else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') next = enabled[(pos - 1 + enabled.length) % enabled.length]!;
    if (next >= 0) {
      e.preventDefault();
      onChange(options[next]!.value);
      refs.current[next]?.focus();
    }
  };
  return (
    <div className="fw-segmented" role="radiogroup" aria-label={label}>
      {options.map((o, i) => (
        <button
          key={o.value}
          ref={(el) => {
            refs.current[i] = el;
          }}
          type="button"
          role="radio"
          aria-checked={value === o.value}
          tabIndex={i === tabStop ? 0 : -1}
          disabled={o.disabled}
          title={o.title}
          onClick={() => onChange(o.value)}
          onKeyDown={(e) => onKey(e, i)}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}
