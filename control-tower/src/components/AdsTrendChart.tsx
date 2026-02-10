"use client";

import { useMemo } from "react";

type TrendRow = {
  date?: string;
  day?: string;
  keys?: any[];
  // value source:
  value?: number;
};

function s(v: any) {
  return String(v ?? "").trim();
}
function num(v: any) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function fmtCompact(n: number) {
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(Math.round(n * 100) / 100);
}

function pickDate(r: TrendRow) {
  return s(r.date) || s(r.day) || s(Array.isArray(r.keys) ? r.keys[0] : "");
}

function toISOWeek(dateStr: string) {
  const d = new Date(dateStr + "T00:00:00Z");
  const dayNum = (d.getUTCDay() + 6) % 7; // Mon=0
  d.setUTCDate(d.getUTCDate() - dayNum + 3); // Thursday
  const firstThursday = new Date(Date.UTC(d.getUTCFullYear(), 0, 4));
  const firstDayNum = (firstThursday.getUTCDay() + 6) % 7;
  firstThursday.setUTCDate(firstThursday.getUTCDate() - firstDayNum + 3);
  const week =
    1 + Math.round((d.getTime() - firstThursday.getTime()) / (7 * 864e5));
  const year = d.getUTCFullYear();
  return `${year}-W${String(week).padStart(2, "0")}`;
}

function toMonth(dateStr: string) {
  return dateStr.slice(0, 7);
}

function groupTrend(
  rows: TrendRow[],
  mode: "day" | "week" | "month",
): Array<{ bucket: string; value: number }> {
  const m = new Map<string, { value: number }>();

  for (const r of rows) {
    const d = pickDate(r);
    if (!d) continue;

    const bucket =
      mode === "day" ? d : mode === "week" ? toISOWeek(d) : toMonth(d);

    const prev = m.get(bucket) || { value: 0 };
    prev.value += num(r.value);
    m.set(bucket, prev);
  }

  const out = Array.from(m.entries()).map(([bucket, v]) => ({ bucket, ...v }));
  out.sort((a, b) => String(a.bucket).localeCompare(String(b.bucket)));
  return out;
}

function getTicks(maxVal: number) {
  const t3 = Math.max(maxVal, 1);
  const t2 = Math.round(t3 * 0.66);
  const t1 = Math.round(t3 * 0.33);
  return [0, t1, t2, t3];
}

export default function AdsTrendChart({
  trend,
  mode,
  startDate,
  endDate,
  seriesLabel,
  unitHint,
}: {
  trend: TrendRow[];
  mode: "day" | "week" | "month";
  startDate?: string | null;
  endDate?: string | null;
  seriesLabel: string;
  unitHint?: string; // e.g. "$", "%", "count"
}) {
  const points = useMemo(() => groupTrend(trend || [], mode), [trend, mode]);

  const stats = useMemo(() => {
    const vals = points.map((p) => num(p.value));
    const maxVal = Math.max(...vals, 1);
    const total = vals.reduce((a, b) => a + b, 0);
    const left = points[0]?.bucket || "";
    const right = points[points.length - 1]?.bucket || "";
    return { maxVal, total, left, right };
  }, [points]);

  const chart = useMemo(() => {
    const w = 920;
    const h = 260;

    const padL = 56;
    const padR = 18;
    const padT = 18;
    const padB = 36;

    const vals = points.map((p) => num(p.value));
    const max = Math.max(...vals, 1);

    const x = (i: number) =>
      padL +
      (points.length <= 1 ? 0 : (i / (points.length - 1)) * (w - padL - padR));

    const y = (v: number) => h - padB - (v / max) * (h - padT - padB);

    let d = "";
    for (let i = 0; i < points.length; i++) {
      const xi = x(i);
      const yi = y(vals[i] || 0);
      d += i === 0 ? `M ${xi} ${yi}` : ` L ${xi} ${yi}`;
    }

    const idxs = vals
      .map((v, i) => ({ v, i }))
      .sort((a, b) => b.v - a.v)
      .slice(0, 3)
      .map((x) => x.i);

    const ticks = getTicks(max);

    const xLabelIdxs: number[] = [];
    if (points.length <= 5) {
      for (let i = 0; i < points.length; i++) xLabelIdxs.push(i);
    } else {
      xLabelIdxs.push(0);
      xLabelIdxs.push(Math.floor((points.length - 1) * 0.25));
      xLabelIdxs.push(Math.floor((points.length - 1) * 0.5));
      xLabelIdxs.push(Math.floor((points.length - 1) * 0.75));
      xLabelIdxs.push(points.length - 1);
    }

    return {
      w,
      h,
      padL,
      padR,
      padT,
      padB,
      d,
      x,
      y,
      vals,
      idxs,
      ticks,
      xLabelIdxs,
      max,
    };
  }, [points]);

  if (!points.length) {
    return (
      <div className="mapCard" style={{ padding: 14 }}>
        <div className="mini">No trend data.</div>
      </div>
    );
  }

  const hint = unitHint ? ` (${unitHint})` : "";

  return (
    <div className="mapCard" style={{ padding: 14 }}>
      <div className="mapCardTop" style={{ alignItems: "center" }}>
        <div>
          <div className="mapCardTitle">
            {seriesLabel} trend{hint} ({mode})
          </div>
          <div className="mini" style={{ marginTop: 6, opacity: 0.85 }}>
            Range: <b>{startDate || stats.left}</b> →{" "}
            <b>{endDate || stats.right}</b>
          </div>
        </div>

        <div style={{ display: "flex", gap: 10 }}>
          <div className="pill chartPill">
            <div className="mini" style={{ opacity: 0.8 }}>
              Max / bucket
            </div>
            <div style={{ fontWeight: 800 }}>{fmtCompact(stats.maxVal)}</div>
          </div>
          <div className="pill chartPill">
            <div className="mini" style={{ opacity: 0.8 }}>
              Total
            </div>
            <div style={{ fontWeight: 800 }}>{fmtCompact(stats.total)}</div>
          </div>
        </div>
      </div>

      <div className="mapFrame" style={{ marginTop: 12, padding: 10 }}>
        <svg
          viewBox={`0 0 ${chart.w} ${chart.h}`}
          width="100%"
          height={chart.h}
        >
          {chart.ticks.map((t, idx) => {
            const yy = chart.y(t);
            return (
              <g key={idx}>
                <line
                  x1={chart.padL}
                  y1={yy}
                  x2={chart.w - chart.padR}
                  y2={yy}
                  stroke="rgba(255,255,255,0.08)"
                />
                <text
                  x={chart.padL - 10}
                  y={yy + 4}
                  textAnchor="end"
                  fontSize="11"
                  fill="rgba(255,255,255,0.55)"
                >
                  {fmtCompact(t)}
                </text>
              </g>
            );
          })}

          <line
            x1={chart.padL}
            y1={chart.padT}
            x2={chart.padL}
            y2={chart.h - chart.padB}
            stroke="rgba(255,255,255,0.12)"
          />
          <line
            x1={chart.padL}
            y1={chart.h - chart.padB}
            x2={chart.w - chart.padR}
            y2={chart.h - chart.padB}
            stroke="rgba(255,255,255,0.12)"
          />

          <path
            d={chart.d}
            fill="none"
            stroke="currentColor"
            strokeWidth="2.6"
            opacity="0.95"
          />

          {chart.vals.map((v, i) => {
            const cx = chart.x(i);
            const cy = chart.y(v);
            const isPeak = chart.idxs.includes(i);
            return (
              <circle
                key={i}
                cx={cx}
                cy={cy}
                r={isPeak ? 3.6 : 2.1}
                fill="currentColor"
                opacity={isPeak ? 0.95 : 0.35}
              />
            );
          })}

          {chart.xLabelIdxs.map((i) => {
            const xx = chart.x(i);
            const label = points[i]?.bucket || "";
            return (
              <text
                key={i}
                x={xx}
                y={chart.h - 12}
                textAnchor="middle"
                fontSize="11"
                fill="rgba(255,255,255,0.55)"
              >
                {label}
              </text>
            );
          })}
        </svg>

        <div className="mini" style={{ marginTop: 8, opacity: 0.75 }}>
          Tip: usa picos para detectar escaladas, cambios de presupuesto o
          problemas de tracking.
        </div>
      </div>
    </div>
  );
}
