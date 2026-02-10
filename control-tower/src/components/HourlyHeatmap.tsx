"use client";

import { useMemo, useState } from "react";

type HeatCell = {
  total: number;
  missed: number;
  completed: number;
  avgDur: number; // seconds
};

type Props = {
  rows: Record<string, any>[];

  /** si quieres mostrar métricas por celda */
  mode?: "total" | "missedRate" | "avgDur";

  /** para heading */
  title?: string;
  subtitle?: string;
};

function s(v: any) {
  return String(v ?? "").trim();
}

function isMissed(statusRaw: any) {
  const st = s(statusRaw).toLowerCase();
  return st === "no-answer" || st === "voicemail";
}
function isCompleted(statusRaw: any) {
  return s(statusRaw).toLowerCase() === "completed";
}

function clamp01(x: number) {
  return Math.max(0, Math.min(1, x));
}

/**
 * 0=Sun..6=Sat → queremos Mon..Sun
 */
const DOW_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
function dowMon0(d: Date) {
  const js = d.getDay(); // 0 Sun..6 Sat
  return (js + 6) % 7; // 0 Mon..6 Sun
}

export default function HourlyHeatmap({
  rows,
  mode = "total",
  title = "Hourly distribution",
  subtitle = "Heatmap por día y hora. Útil para detectar picos, ventanas y “bottlenecks”.",
}: Props) {
  const [hover, setHover] = useState<{ d: number; h: number } | null>(null);

  const data = useMemo(() => {
    // 7x24
    const grid: HeatCell[][] = Array.from({ length: 7 }, () =>
      Array.from({ length: 24 }, () => ({
        total: 0,
        missed: 0,
        completed: 0,
        avgDur: 0,
      })),
    );

    // para avg duration por celda
    const durSum: number[][] = Array.from({ length: 7 }, () =>
      Array(24).fill(0),
    );
    const durCnt: number[][] = Array.from({ length: 7 }, () =>
      Array(24).fill(0),
    );

    let total = 0;
    let missed = 0;
    let completed = 0;

    for (const r of rows || []) {
      const iso =
        s((r as any).__startIso) ||
        s(r["Phone Call Start Time"]) ||
        s(r["Start Time"]);
      if (!iso) continue;

      const dt = new Date(iso);
      if (Number.isNaN(dt.getTime())) continue;

      const d = dowMon0(dt);
      const h = dt.getHours();

      const cell = grid[d][h];
      cell.total += 1;
      total += 1;

      const st = r["Phone Call Status"];
      if (isMissed(st)) {
        cell.missed += 1;
        missed += 1;
      }
      if (isCompleted(st)) {
        cell.completed += 1;
        completed += 1;
      }

      const dur = Number(r["Phone Call Duration"]);
      if (Number.isFinite(dur) && dur >= 0) {
        durSum[d][h] += dur;
        durCnt[d][h] += 1;
      }
    }

    // finalize avgDur
    for (let d = 0; d < 7; d++) {
      for (let h = 0; h < 24; h++) {
        const c = durCnt[d][h];
        grid[d][h].avgDur = c ? Math.round(durSum[d][h] / c) : 0;
      }
    }

    // intensity normalization
    const maxTotal = Math.max(1, ...grid.flat().map((c) => c.total));
    const maxAvgDur = Math.max(1, ...grid.flat().map((c) => c.avgDur));

    const valueFor = (c: HeatCell) => {
      if (mode === "missedRate") return c.total ? c.missed / c.total : 0;
      if (mode === "avgDur") return c.avgDur;
      return c.total;
    };

    const maxForMode =
      mode === "avgDur" ? maxAvgDur : mode === "missedRate" ? 1 : maxTotal;

    const intensityFor = (c: HeatCell) =>
      clamp01(valueFor(c) / (maxForMode || 1));

    const missRate = total ? Math.round((missed / total) * 100) : 0;

    return {
      grid,
      total,
      missed,
      completed,
      missRate,
      maxTotal,
      maxAvgDur,
      intensityFor,
      valueFor,
    };
  }, [rows, mode]);

  const hoverCell = useMemo(() => {
    if (!hover) return null;
    return data.grid[hover.d][hover.h];
  }, [hover, data.grid]);

  function fmtHour(h: number) {
    return String(h).padStart(2, "0");
  }

  const modeLabel =
    mode === "missedRate"
      ? "Missed rate"
      : mode === "avgDur"
        ? "Avg duration"
        : "Total";

  return (
    <div className="hmCard">
      <div className="hmTop">
        <div>
          <div className="hmTitle">{title}</div>
          <div className="mini" style={{ marginTop: 6, opacity: 0.85 }}>
            {subtitle}
          </div>
        </div>

        <div className="hmBadges">
          <div className="hmBadge">
            <div className="mini hmBadgeLabel">Total</div>
            <div className="hmBadgeValue">
              <b>{data.total}</b>
            </div>
          </div>

          <div className="hmBadge">
            <div className="mini hmBadgeLabel">Missed</div>
            <div className="hmBadgeValue">
              <b>{data.missed}</b>{" "}
              <span className="mini" style={{ opacity: 0.75 }}>
                ({data.missRate}%)
              </span>
            </div>
          </div>

          <div className="hmBadge">
            <div className="mini hmBadgeLabel">Mode</div>
            <div className="hmBadgeValue">
              <b>{modeLabel}</b>
            </div>
          </div>
        </div>
      </div>

      <div className="hmGridWrap">
        {/* header hours */}
        <div className="hmGrid" role="grid" aria-label="Hourly heatmap">
          <div className="hmCorner" />

          {Array.from({ length: 24 }).map((_, h) => (
            <div key={`h-${h}`} className="hmColHead">
              {fmtHour(h)}
            </div>
          ))}

          {/* rows */}
          {Array.from({ length: 7 }).map((_, d) => (
            <div key={`r-${d}`} className="hmRow">
              <div className="hmRowHead">{DOW_LABELS[d]}</div>

              {Array.from({ length: 24 }).map((__, h) => {
                const c = data.grid[d][h];
                const t = data.intensityFor(c);

                // alpha ramp suave (no rompe el theme)
                const a = 0.06 + t * 0.72;

                const isHotRisk =
                  c.total >= 3 &&
                  c.total > 0 &&
                  c.missed / (c.total || 1) >= 0.6;

                return (
                  <div
                    key={`c-${d}-${h}`}
                    className={`hmCell ${isHotRisk ? "hmCellRisk" : ""} ${
                      hover?.d === d && hover?.h === h ? "hmCellHover" : ""
                    }`}
                    style={{ ["--a" as any]: String(a) } as any}
                    onMouseEnter={() => setHover({ d, h })}
                    onMouseLeave={() => setHover(null)}
                    title={`${DOW_LABELS[d]} ${fmtHour(h)}:00 • total ${c.total} • missed ${c.missed} • completed ${c.completed} • avgDur ${c.avgDur}s`}
                  >
                    <span className="hmCellText">
                      {mode === "missedRate"
                        ? `${Math.round(c.total ? (c.missed / c.total) * 100 : 0)}%`
                        : mode === "avgDur"
                          ? c.avgDur
                          : c.total}
                    </span>
                  </div>
                );
              })}
            </div>
          ))}
        </div>

        {/* hover panel */}
        <div className="hmHover">
          {!hoverCell ? (
            <div className="mini" style={{ opacity: 0.75 }}>
              Hover una celda para ver detalle.
            </div>
          ) : (
            <div className="hmHoverBox">
              <div className="hmHoverTitle">
                {DOW_LABELS[hover!.d]} • {fmtHour(hover!.h)}:00
              </div>
              <div className="mini" style={{ marginTop: 8, opacity: 0.9 }}>
                <b>Total:</b> {hoverCell.total}
              </div>
              <div className="mini" style={{ marginTop: 6, opacity: 0.9 }}>
                <b>Missed:</b> {hoverCell.missed}{" "}
                <span style={{ opacity: 0.75 }}>
                  (
                  {hoverCell.total
                    ? Math.round((hoverCell.missed / hoverCell.total) * 100)
                    : 0}
                  %)
                </span>
              </div>
              <div className="mini" style={{ marginTop: 6, opacity: 0.9 }}>
                <b>Completed:</b> {hoverCell.completed}
              </div>
              <div className="mini" style={{ marginTop: 6, opacity: 0.9 }}>
                <b>Avg duration:</b> {hoverCell.avgDur}s
              </div>
            </div>
          )}
        </div>
      </div>

      <div className="mini" style={{ marginTop: 10, opacity: 0.75 }}>
        Tip: Si ves cuadros con alta intensidad + <b>hmCellRisk</b>, revisa
        staffing, SLA de respuesta, y routing en esas ventanas.
      </div>
    </div>
  );
}
