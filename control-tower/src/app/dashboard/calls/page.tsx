"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import UsaChoroplethProgressMap from "@/components/UsaChoroplethProgressMap";
import HourlyHeatmap from "@/components/HourlyHeatmap";
import AiAgentChatPanel from "@/components/AiAgentChatPanel";

type ApiRow = Record<string, any> & {
  __startIso?: string;
  __startMs?: number | null;
  __fromStateNorm?: string;
  __fromCityNorm?: string;
};

type CallsApiResponse = {
  ok: boolean;
  total: number;
  byState: Record<string, number>;
  rows: ApiRow[];
  error?: string;
};

type RangePreset = "1d" | "7d" | "28d" | "1m" | "3m" | "6m" | "1y" | "custom";
type TrendGrain = "day" | "week" | "month";
type StatusFilter = "all" | "missed" | "completed";
type DirFilter = "all" | "inbound" | "outbound";

function norm(v: any) {
  return String(v ?? "").trim();
}

function fmtDateLocal(iso?: string) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString();
}

/** ✅ SAFE ISO helpers (avoid "Invalid time value") */
function safeToIso(d: Date) {
  const t = d.getTime();
  if (!Number.isFinite(t)) return "";
  return d.toISOString();
}

function isoStartOfDay(d: Date) {
  const x = new Date(d);
  if (!Number.isFinite(x.getTime())) return "";
  x.setHours(0, 0, 0, 0);
  return safeToIso(x);
}
function isoEndOfDay(d: Date) {
  const x = new Date(d);
  if (!Number.isFinite(x.getTime())) return "";
  x.setHours(23, 59, 59, 999);
  return safeToIso(x);
}

function msToHuman(sec: number) {
  const s = Math.max(0, Math.floor(sec));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const r = s % 60;
  if (m < 60) return `${m}m ${r}s`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return `${h}h ${rm}m`;
}

const STATE_ABBR_TO_NAME: Record<string, string> = {
  AL: "Alabama",
  AK: "Alaska",
  AZ: "Arizona",
  AR: "Arkansas",
  CA: "California",
  CO: "Colorado",
  CT: "Connecticut",
  DE: "Delaware",
  DC: "District of Columbia",
  FL: "Florida",
  GA: "Georgia",
  HI: "Hawaii",
  ID: "Idaho",
  IL: "Illinois",
  IN: "Indiana",
  IA: "Iowa",
  KS: "Kansas",
  KY: "Kentucky",
  LA: "Louisiana",
  ME: "Maine",
  MD: "Maryland",
  MA: "Massachusetts",
  MI: "Michigan",
  MN: "Minnesota",
  MS: "Mississippi",
  MO: "Missouri",
  MT: "Montana",
  NE: "Nebraska",
  NV: "Nevada",
  NH: "New Hampshire",
  NJ: "New Jersey",
  NM: "New Mexico",
  NY: "New York",
  NC: "North Carolina",
  ND: "North Dakota",
  OH: "Ohio",
  OK: "Oklahoma",
  OR: "Oregon",
  PA: "Pennsylvania",
  RI: "Rhode Island",
  SC: "South Carolina",
  SD: "South Dakota",
  TN: "Tennessee",
  TX: "Texas",
  UT: "Utah",
  VT: "Vermont",
  VA: "Virginia",
  WA: "Washington",
  WV: "West Virginia",
  WI: "Wisconsin",
  WY: "Wyoming",
  PR: "Puerto Rico",
};

function normalizeStateName(raw: any) {
  const s = norm(raw);
  if (!s) return "";
  const up = s.toUpperCase();
  if (STATE_ABBR_TO_NAME[up]) return STATE_ABBR_TO_NAME[up];
  if (up === "PUERTO RICO") return "Puerto Rico";
  if (up === "PR") return "Puerto Rico";
  return s;
}

function normalizeCityName(raw: any) {
  const s = norm(raw);
  if (!s) return "";
  return s;
}

function isMissed(statusRaw: any) {
  const st = norm(statusRaw).toLowerCase();
  return st === "no-answer" || st === "voicemail";
}

function isCompleted(statusRaw: any) {
  const st = norm(statusRaw).toLowerCase();
  return st === "completed";
}

function dirType(dirRaw: any) {
  const d = norm(dirRaw).toLowerCase();
  if (d === "outbound") return "outbound";
  return "inbound";
}

/** ========= Trend utils ========= */
function startOfISOWeek(d: Date) {
  const x = new Date(d);
  const day = x.getDay(); // 0..6
  const diff = (day === 0 ? -6 : 1) - day;
  x.setDate(x.getDate() + diff);
  x.setHours(0, 0, 0, 0);
  return x;
}

function keyForGrain(ms: number, grain: TrendGrain) {
  const d = new Date(ms);
  if (grain === "day") {
    const x = new Date(d);
    x.setHours(0, 0, 0, 0);
    return x.toISOString().slice(0, 10);
  }
  if (grain === "week") {
    const w = startOfISOWeek(d);
    return w.toISOString().slice(0, 10);
  }
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  return `${y}-${m}`;
}

function labelForGrainKey(key: string, grain: TrendGrain) {
  if (grain === "month") return key;
  const d = new Date(`${key}T00:00:00.000Z`);
  if (Number.isNaN(d.getTime())) return key;
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${mm}/${dd}`;
}

type TrendPoint = { key: string; label: string; calls: number };

function buildTrend(rows: ApiRow[], grain: TrendGrain) {
  const m = new Map<string, number>();
  for (const r of rows) {
    const ms = Number(r.__startMs ?? NaN);
    if (!Number.isFinite(ms)) continue;
    const k = keyForGrain(ms, grain);
    m.set(k, (m.get(k) || 0) + 1);
  }
  const keys = Array.from(m.keys()).sort((a, b) => a.localeCompare(b));
  return keys.map((k) => ({
    key: k,
    label: labelForGrainKey(k, grain),
    calls: m.get(k) || 0,
  }));
}

function slicePrevPeriod(startIso: string, endIso: string) {
  const start = new Date(startIso).getTime();
  const end = new Date(endIso).getTime();
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
    return { prevStart: "", prevEnd: "" };
  }
  const len = end - start;
  const prevEnd = new Date(start - 1);
  const prevStart = new Date(start - 1 - len);
  return { prevStart: prevStart.toISOString(), prevEnd: prevEnd.toISOString() };
}

function percentChange(curr: number, prev: number) {
  if (!Number.isFinite(curr) || !Number.isFinite(prev)) return null;
  if (prev === 0) return curr === 0 ? 0 : 100;
  return ((curr - prev) / prev) * 100;
}

/** ========= Small SVG line chart ========= */
function LineTrend({
  points,
  height = 220,
  onHover,
}: {
  points: TrendPoint[];
  height?: number;
  onHover?: (p: TrendPoint | null) => void;
}) {
  const padL = 44;
  const padR = 16;
  const padT = 14;
  const padB = 34;

  const w = 1000;
  const h = height;

  const maxY = Math.max(...points.map((p) => p.calls), 1);
  const minY = 0;

  const plotW = w - padL - padR;
  const plotH = h - padT - padB;

  const xFor = (i: number) =>
    padL + (plotW * i) / Math.max(1, points.length - 1);
  const yFor = (v: number) =>
    padT + plotH * (1 - (v - minY) / (maxY - minY || 1));

  const d = points
    .map((p, i) => {
      const x = xFor(i);
      const y = yFor(p.calls);
      return `${i === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");

  const yTicks = [0, Math.round(maxY / 2), maxY];
  const first = points[0];
  const last = points[points.length - 1];

  return (
    <div className="chartWrap">
      <svg
        className="chartSvg"
        viewBox={`0 0 ${w} ${h}`}
        preserveAspectRatio="none"
        onMouseLeave={() => onHover?.(null)}
      >
        {yTicks.map((t, idx) => {
          const y = yFor(t);
          return (
            <g key={idx}>
              <line
                x1={padL}
                x2={w - padR}
                y1={y}
                y2={y}
                className="chartGrid"
              />
              <text
                x={padL - 10}
                y={y + 4}
                textAnchor="end"
                className="chartAxis"
              >
                {t}
              </text>
            </g>
          );
        })}

        {points.length >= 2 && (
          <>
            <text x={padL} y={h - 10} textAnchor="start" className="chartAxis">
              {first?.label ?? ""}
            </text>
            <text
              x={w - padR}
              y={h - 10}
              textAnchor="end"
              className="chartAxis"
            >
              {last?.label ?? ""}
            </text>
          </>
        )}

        <path
          d={`${d} L ${xFor(points.length - 1)} ${yFor(0)} L ${xFor(0)} ${yFor(0)} Z`}
          className="chartArea"
        />
        <path d={d} className="chartLine" />

        {points.map((p, i) => {
          const x = xFor(i);
          const y = yFor(p.calls);
          return (
            <circle
              key={p.key}
              cx={x}
              cy={y}
              r={3}
              className="chartPoint"
              onMouseEnter={() => onHover?.(p)}
            />
          );
        })}
      </svg>
    </div>
  );
}

/** ========= KPI helper from rows ========= */
function computeKpis(rows: ApiRow[]) {
  const total = rows.length;

  let inbound = 0;
  let outbound = 0;

  let completed = 0;
  let missed = 0;

  let durSum = 0;
  let durCount = 0;

  let lastCallMs = 0;

  for (const r of rows) {
    const ms = Number(r.__startMs ?? 0);
    if (Number.isFinite(ms) && ms > lastCallMs) lastCallMs = ms;

    const dir = dirType(r["Phone Call Direction"]);
    if (dir === "inbound") inbound++;
    else outbound++;

    const stRaw = r["Phone Call Status"];
    if (isCompleted(stRaw)) completed++;
    if (isMissed(stRaw)) missed++;

    const dur = Number(r["Phone Call Duration"]);
    if (Number.isFinite(dur) && dur >= 0) {
      durSum += dur;
      durCount++;
    }
  }

  const avgDur = durCount ? Math.round(durSum / durCount) : 0;
  const missRate = total ? Math.round((missed / total) * 100) : 0;
  const completedRate = total ? Math.round((completed / total) * 100) : 0;

  return {
    total,
    inbound,
    outbound,
    completed,
    missed,
    avgDur,
    totalTalk: durSum,
    missRate,
    completedRate,
    lastCallIso: lastCallMs > 0 ? new Date(lastCallMs).toISOString() : "",
  };
}

/** ========= By-state KPIs for deltas table ========= */
function computeByState(rows: ApiRow[]) {
  const m = new Map<string, { rows: ApiRow[] }>();
  for (const r of rows) {
    const st = r.__fromStateNorm || "";
    if (!st) continue;
    if (!m.has(st)) m.set(st, { rows: [] });
    m.get(st)!.rows.push(r);
  }
  const out = Array.from(m.entries()).map(([state, obj]) => {
    const k = computeKpis(obj.rows);
    return { state, kpis: k };
  });
  out.sort((a, b) => b.kpis.total - a.kpis.total);
  return out;
}

export default function CallsDashboardPage() {
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const [preset, setPreset] = useState<RangePreset>("7d");
  const [customStart, setCustomStart] = useState("");
  const [customEnd, setCustomEnd] = useState("");

  const [grain, setGrain] = useState<TrendGrain>("day");

  const [data, setData] = useState<CallsApiResponse | null>(null);
  const [prevData, setPrevData] = useState<CallsApiResponse | null>(null);

  const [mapSelected, setMapSelected] = useState<string>("");

  const [hoverPoint, setHoverPoint] = useState<TrendPoint | null>(null);

  // Advanced filters (affect EVERYTHING)
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [dirFilter, setDirFilter] = useState<DirFilter>("all");
  const [cityOnly, setCityOnly] = useState(false);

  // ✅ UI: show/hide advanced panel
  const [showMoreFilters, setShowMoreFilters] = useState(false);

  // AI
  const [aiLoading, setAiLoading] = useState(false);
  const [aiErr, setAiErr] = useState("");
  const [aiInsights, setAiInsights] = useState<any>(null);

  const computedRange = useMemo(() => {
    const now = new Date();
    const end = isoEndOfDay(now);

    const startFromDays = (days: number) => {
      const startD = new Date(now);
      startD.setDate(startD.getDate() - days);
      return { start: isoStartOfDay(startD), end };
    };

    if (preset === "1d") return startFromDays(1);
    if (preset === "7d") return startFromDays(7);
    if (preset === "28d") return startFromDays(28);

    if (preset === "1m") {
      const startD = new Date(now);
      startD.setMonth(startD.getMonth() - 1);
      return { start: isoStartOfDay(startD), end };
    }
    if (preset === "3m") {
      const startD = new Date(now);
      startD.setMonth(startD.getMonth() - 3);
      return { start: isoStartOfDay(startD), end };
    }
    if (preset === "6m") {
      const startD = new Date(now);
      startD.setMonth(startD.getMonth() - 6);
      return { start: isoStartOfDay(startD), end };
    }
    if (preset === "1y") {
      const startD = new Date(now);
      startD.setFullYear(startD.getFullYear() - 1);
      return { start: isoStartOfDay(startD), end };
    }

    if (preset === "custom") {
      // ✅ Safe parsing (avoid Invalid Date in some browsers)
      const startD = customStart ? new Date(`${customStart}T00:00:00`) : null;
      const endD = customEnd ? new Date(`${customEnd}T00:00:00`) : null;

      const start = startD ? isoStartOfDay(startD) : "";
      const end2 = endD ? isoEndOfDay(endD) : "";
      return { start, end: end2 };
    }

    return { start: "", end: "" };
  }, [preset, customStart, customEnd]);

  function clearSelection() {
    setMapSelected("");
  }

  function setPresetSafe(p: RangePreset) {
    setPreset(p);
  }

  async function fetchCalls(start: string, end: string) {
    const qs =
      start && end
        ? `?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`
        : "";
    const res = await fetch(`/api/dashboard/calls${qs}`, { cache: "no-store" });

    const ct = res.headers.get("content-type") || "";
    if (!ct.includes("application/json")) {
      const txt = await res.text();
      throw new Error(
        `Calls API returned non-JSON (content-type: ${ct}). First chars: ${txt.slice(0, 120)}`,
      );
    }

    const json = (await res.json()) as CallsApiResponse;
    if (!res.ok || !json?.ok)
      throw new Error(json?.error || `HTTP ${res.status}`);

    const rows = (json.rows || []).map((r) => {
      const s = r.__fromStateNorm || r["Phone Call From State"];
      const c = r.__fromCityNorm || r["Phone Call From City"];
      return {
        ...r,
        __fromStateNorm: normalizeStateName(s),
        __fromCityNorm: normalizeCityName(c),
      };
    });

    const byStateFallback: Record<string, number> = {};
    for (const r of rows) {
      const st = r.__fromStateNorm || "";
      if (!st) continue;
      byStateFallback[st] = (byStateFallback[st] || 0) + 1;
    }

    return {
      ...json,
      rows,
      byState: Object.keys(json.byState || {}).length
        ? json.byState
        : byStateFallback,
    } as CallsApiResponse;
  }

  async function load() {
    setErr("");
    setLoading(true);
    setAiInsights(null);
    setAiErr("");

    try {
      if (!computedRange.start || !computedRange.end) {
        throw new Error("Missing start/end range");
      }

      const current = await fetchCalls(computedRange.start, computedRange.end);
      setData(current);

      const { prevStart, prevEnd } = slicePrevPeriod(
        computedRange.start,
        computedRange.end,
      );
      if (prevStart && prevEnd) {
        const prev = await fetchCalls(prevStart, prevEnd);
        setPrevData(prev);
      } else {
        setPrevData(null);
      }
    } catch (e: any) {
      setData(null);
      setPrevData(null);
      setErr(e?.message || "Failed to load Calls Dashboard");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (preset !== "custom") load();
    else if (customStart && customEnd) load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [preset, customStart, customEnd]);

  /** ========= Apply advanced filters (status/direction/city) ========= */
  const filteredRows = useMemo(() => {
    let rows = data?.rows || [];

    // Map selection (state)
    if (mapSelected) {
      rows = rows.filter((r) => (r.__fromStateNorm || "") === mapSelected);
    }

    // Direction
    if (dirFilter !== "all") {
      rows = rows.filter(
        (r) => dirType(r["Phone Call Direction"]) === dirFilter,
      );
    }

    // Status
    if (statusFilter !== "all") {
      rows = rows.filter((r) => {
        const st = r["Phone Call Status"];
        return statusFilter === "missed" ? isMissed(st) : isCompleted(st);
      });
    }

    // City only
    if (cityOnly) {
      rows = rows.filter(
        (r) => !!norm(r.__fromCityNorm || r["Phone Call From City"]),
      );
    }

    return rows;
  }, [data, mapSelected, dirFilter, statusFilter, cityOnly]);

  const prevFilteredRows = useMemo(() => {
    let rows = prevData?.rows || [];

    if (mapSelected) {
      rows = rows.filter((r) => (r.__fromStateNorm || "") === mapSelected);
    }

    if (dirFilter !== "all") {
      rows = rows.filter(
        (r) => dirType(r["Phone Call Direction"]) === dirFilter,
      );
    }

    if (statusFilter !== "all") {
      rows = rows.filter((r) => {
        const st = r["Phone Call Status"];
        return statusFilter === "missed" ? isMissed(st) : isCompleted(st);
      });
    }

    if (cityOnly) {
      rows = rows.filter(
        (r) => !!norm(r.__fromCityNorm || r["Phone Call From City"]),
      );
    }

    return rows;
  }, [prevData, mapSelected, dirFilter, statusFilter, cityOnly]);

  /** ========= KPIs ========= */
  const kpis = useMemo(() => computeKpis(filteredRows), [filteredRows]);
  const prevKpis = useMemo(
    () => (prevData ? computeKpis(prevFilteredRows) : null),
    [prevData, prevFilteredRows],
  );

  /** ========= Deltas ========= */
  const deltas = useMemo(() => {
    if (!prevKpis) {
      return {
        callsPct: null as number | null,
        missRatePct: null as number | null,
        avgDurPct: null as number | null,
      };
    }
    return {
      callsPct: percentChange(kpis.total, prevKpis.total),
      missRatePct: percentChange(kpis.missRate, prevKpis.missRate),
      avgDurPct: percentChange(kpis.avgDur, prevKpis.avgDur),
    };
  }, [kpis, prevKpis]);

  /** ========= Table ========= */
  const tableRows = useMemo(() => {
    return [...filteredRows].sort(
      (a, b) => Number(b.__startMs || 0) - Number(a.__startMs || 0),
    );
  }, [filteredRows]);

  /** ========= Map rows (ALWAYS from FULL data.byState) ========= */
  const mapRows = useMemo(() => {
    const byState = data?.byState || {};
    const entries = Object.entries(byState);
    if (!entries.length) return [];

    const maxCalls = Math.max(...entries.map(([, v]) => Number(v) || 0), 1);

    return entries.map(([stateName, calls]) => ({
      state: stateName,
      counties: {
        total: maxCalls,
        ready: Number(calls) || 0,
        domainsActive: 0,
      },
      cities: { total: 0, ready: 0, domainsActive: 0 },
      __value: Number(calls) || 0,
    }));
  }, [data]);

  const selectedCalls = useMemo(() => {
    if (!mapSelected) return null;
    const n = data?.byState?.[mapSelected];
    return Number(n || 0);
  }, [mapSelected, data]);

  /** ========= Trend ========= */
  const trendPoints = useMemo(
    () => buildTrend(filteredRows, grain),
    [filteredRows, grain],
  );

  const trendSummary = useMemo(() => {
    const total = filteredRows.length;
    const points = trendPoints.length;
    const max = Math.max(...trendPoints.map((p) => p.calls), 0);
    const avg = points ? total / points : 0;
    return { total, points, max, avg };
  }, [trendPoints, filteredRows]);

  /** ========= Range label ========= */
  const rangeLabel = useMemo(() => {
    if (preset === "custom") return "Custom range";
    if (preset === "1d") return "Last 1 day";
    if (preset === "7d") return "Last 7 days";
    if (preset === "28d") return "Last 28 days";
    if (preset === "1m") return "Last month";
    if (preset === "3m") return "Last quarter";
    if (preset === "6m") return "Last 6 months";
    if (preset === "1y") return "Last year";
    return preset;
  }, [preset]);

  /** ========= State deltas (next phase #1) ========= */
  const byStateNow = useMemo(() => computeByState(data?.rows || []), [data]);
  const byStatePrev = useMemo(
    () => computeByState(prevData?.rows || []),
    [prevData],
  );

  const stateDeltas = useMemo(() => {
    if (!prevData) return [];

    const prevMap = new Map(byStatePrev.map((x) => [x.state, x.kpis]));
    const nowMap = new Map(byStateNow.map((x) => [x.state, x.kpis]));

    const states = Array.from(new Set([...prevMap.keys(), ...nowMap.keys()]));
    const out = states.map((st) => {
      const now =
        nowMap.get(st) ||
        ({
          total: 0,
          missRate: 0,
          avgDur: 0,
          completedRate: 0,
          completed: 0,
          missed: 0,
          inbound: 0,
          outbound: 0,
          totalTalk: 0,
          lastCallIso: "",
        } as any);
      const prev =
        prevMap.get(st) ||
        ({
          total: 0,
          missRate: 0,
          avgDur: 0,
          completedRate: 0,
          completed: 0,
          missed: 0,
          inbound: 0,
          outbound: 0,
          totalTalk: 0,
          lastCallIso: "",
        } as any);

      return {
        state: st,
        now,
        prev,
        dCalls: percentChange(now.total, prev.total),
        dMissRate: percentChange(now.missRate, prev.missRate),
        dAvgDur: percentChange(now.avgDur, prev.avgDur),
      };
    });

    out.sort((a, b) => (b.now.total || 0) - (a.now.total || 0));
    return out.slice(0, 20);
  }, [prevData, byStateNow, byStatePrev]);

  /** ========= Transcript intelligence (MVP scoring) ========= */
  const transcriptSignals = useMemo(() => {
    const total = filteredRows.length;
    if (!total) {
      return {
        qualityScore: 0,
        notes: ["No calls in current filtered scope."],
        nextBestActions: [],
      };
    }

    const missRate = kpis.missRate; // 0-100
    const avgDur = kpis.avgDur; // seconds

    let score = 100;
    score -= Math.min(70, missRate * 0.7);
    score += Math.min(15, Math.max(0, (avgDur - 10) * 0.3));
    score = Math.max(0, Math.min(100, Math.round(score)));

    const notes: string[] = [];
    if (missRate >= 50)
      notes.push(
        "Miss rate alto: riesgo directo de revenue (SLA/routing/staffing).",
      );
    if (avgDur <= 10)
      notes.push(
        "Avg duration muy bajo: posible contestador, llamadas cortadas o lead quality bajo.",
      );
    if (kpis.inbound > 0 && kpis.outbound === 0)
      notes.push(
        "Casi todo inbound: oportunidad de outbound follow-up para recuperar missed.",
      );

    const nextBestActions: string[] = [];
    if (missRate >= 40)
      nextBestActions.push(
        "Implementa callback automático 1-3 min (missed/no-answer) + 2 SMS en 15 min.",
      );
    if (avgDur <= 10)
      nextBestActions.push(
        "Revisa greeting/IVR y validación del lead (captura motivo + urgencia).",
      );
    nextBestActions.push(
      "Prioriza top states por volumen para optimizar staffing y scripts.",
    );

    return { qualityScore: score, notes, nextBestActions };
  }, [filteredRows, kpis]);

  async function generateInsights() {
    setAiErr("");
    setAiLoading(true);
    setAiInsights(null);

    try {
      const byState = data?.byState || {};
      const topStates = Object.entries(byState)
        .sort((a, b) => (Number(b[1]) || 0) - (Number(a[1]) || 0))
        .slice(0, 10)
        .map(([state, calls]) => ({ state, calls: Number(calls) || 0 }));

      const payload = {
        range: {
          start: computedRange.start,
          end: computedRange.end,
          label: rangeLabel,
        },
        scope: {
          state: mapSelected || null,
          filters: { statusFilter, dirFilter, cityOnly },
        },
        kpis,
        deltas,
        topStates,
        transcriptSignals,
        deltaSystem: {
          note: "Attach counties/cities/state catalog here to correlate call volume vs SEO rollout.",
        },
      };

      const res = await fetch("/api/dashboard/calls/insights", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const ct = res.headers.get("content-type") || "";
      if (!ct.includes("application/json")) {
        const txt = await res.text();
        throw new Error(
          `Insights API returned non-JSON (content-type: ${ct}). First chars: ${txt.slice(0, 200)}`,
        );
      }

      const json = await res.json();
      if (!res.ok || !json?.ok)
        throw new Error(json?.error || "Failed to generate AI insights");

      setAiInsights(json.insights);
    } catch (e: any) {
      setAiErr(e?.message || "Failed to generate AI insights");
    } finally {
      setAiLoading(false);
    }
  }

  return (
    <div className="shell callsDash">
      {/* Topbar */}
      <header className="topbar">
        <div className="brand">
          <div className="logo" />
          <div>
            <h1>My Drip Nurse — Calls Dashboard</h1>
          </div>
        </div>

        <div className="pills">
          <Link
            className="pill"
            href="/dashboard"
            style={{ textDecoration: "none" }}
          >
            ← Back
          </Link>

          <div className="pill">
            <span className="dot" />
            <span>Live</span>
          </div>

          <div className="pill">
            <span style={{ color: "var(--muted)" }}>Created by</span>
            <span style={{ opacity: 0.55 }}>•</span>
            <span>Axel Castro</span>
            <span style={{ opacity: 0.55 }}>•</span>
            <span>Devasks</span>
          </div>
        </div>
      </header>

      {/* Filters */}
      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Filters</h2>
            <div className="cardSubtitle">
              Este filtro afecta el mapa, KPIs, chart, heatmap y la tabla.
            </div>
          </div>

          <div className="cardHeaderActions">
            <button
              className="smallBtn"
              onClick={load}
              disabled={loading}
              type="button"
            >
              {loading ? "Refreshing..." : "Refresh"}
            </button>
          </div>
        </div>

        <div className="cardBody">
          <div className="filtersBar">
            <div className="filtersGroup">
              <div className="filtersLabel">Range</div>

              <div className="rangePills">
                <button
                  className={`smallBtn ${preset === "1d" ? "smallBtnOn" : ""}`}
                  onClick={() => setPresetSafe("1d")}
                  type="button"
                >
                  1 day
                </button>
                <button
                  className={`smallBtn ${preset === "7d" ? "smallBtnOn" : ""}`}
                  onClick={() => setPresetSafe("7d")}
                  type="button"
                >
                  7 days
                </button>
                <button
                  className={`smallBtn ${preset === "28d" ? "smallBtnOn" : ""}`}
                  onClick={() => setPresetSafe("28d")}
                  type="button"
                >
                  28 days
                </button>

                <span className="filtersDivider" />

                <button
                  className={`smallBtn ${preset === "1m" ? "smallBtnOn" : ""}`}
                  onClick={() => setPresetSafe("1m")}
                  type="button"
                >
                  Last month
                </button>
                <button
                  className={`smallBtn ${preset === "3m" ? "smallBtnOn" : ""}`}
                  onClick={() => setPresetSafe("3m")}
                  type="button"
                >
                  Last quarter
                </button>
                <button
                  className={`smallBtn ${preset === "6m" ? "smallBtnOn" : ""}`}
                  onClick={() => setPresetSafe("6m")}
                  type="button"
                >
                  Last 6 months
                </button>
                <button
                  className={`smallBtn ${preset === "1y" ? "smallBtnOn" : ""}`}
                  onClick={() => setPresetSafe("1y")}
                  type="button"
                >
                  Last year
                </button>

                <span className="filtersDivider" />

                <button
                  className={`smallBtn ${preset === "custom" ? "smallBtnOn" : ""}`}
                  onClick={() => setPresetSafe("custom")}
                  type="button"
                >
                  Custom
                </button>
              </div>
            </div>

            <div className="filtersGroup dateGroup">
              <div className="filtersLabel">Custom dates</div>

              <div className="dateInputs">
                <div className="dateField">
                  <label
                    className="mini"
                    style={{ marginBottom: 6, display: "block" }}
                  >
                    Start
                  </label>
                  <input
                    className="input"
                    type="date"
                    value={customStart}
                    onChange={(e) => setCustomStart(e.target.value)}
                    disabled={preset !== "custom"}
                  />
                </div>

                <div className="dateField">
                  <label
                    className="mini"
                    style={{ marginBottom: 6, display: "block" }}
                  >
                    End
                  </label>
                  <input
                    className="input"
                    type="date"
                    value={customEnd}
                    onChange={(e) => setCustomEnd(e.target.value)}
                    disabled={preset !== "custom"}
                  />
                </div>

                {/* ✅ Apply only in custom mode */}
                {preset === "custom" && (
                  <button
                    className="btn btnPrimary applyBtn"
                    onClick={load}
                    disabled={!customStart || !customEnd || loading}
                    type="button"
                  >
                    {loading ? "Applying..." : "Apply"}
                  </button>
                )}
              </div>

              <div className="mini" style={{ marginTop: 8, opacity: 0.85 }}>
                {preset !== "custom"
                  ? "Tip: Usa presets para velocidad. Custom es para rangos manuales."
                  : "Custom activo: selecciona Start/End y Apply."}
              </div>
            </div>
          </div>

          {/* ✅ Minimal compact filters + collapsible panel */}
          <div className="filtersRow2">
            <div className="filtersChips">
              <button
                className={`smallBtn ${statusFilter !== "all" ? "smallBtnOn" : ""}`}
                type="button"
                onClick={() =>
                  setStatusFilter((v) => (v === "all" ? "missed" : "all"))
                }
                title="Toggle Missed only"
              >
                {statusFilter === "missed" ? "Missed only" : "All statuses"}
              </button>

              <button
                className={`smallBtn ${dirFilter !== "all" ? "smallBtnOn" : ""}`}
                type="button"
                onClick={() =>
                  setDirFilter((v) => (v === "all" ? "inbound" : "all"))
                }
                title="Toggle Inbound only"
              >
                {dirFilter === "inbound" ? "Inbound only" : "All directions"}
              </button>

              <button
                className={`smallBtn ${cityOnly ? "smallBtnOn" : ""}`}
                type="button"
                onClick={() => setCityOnly((v) => !v)}
                title="Only rows that include a city"
              >
                {cityOnly ? "City: On" : "City: Any"}
              </button>

              <span className="filtersDivider" />

              <button
                className={`smallBtn ${showMoreFilters ? "smallBtnOn" : ""}`}
                type="button"
                onClick={() => setShowMoreFilters((v) => !v)}
              >
                {showMoreFilters ? "Hide filters" : "More filters"}
              </button>

              {(statusFilter !== "all" || dirFilter !== "all" || cityOnly) && (
                <button
                  className="smallBtn"
                  type="button"
                  onClick={() => {
                    setStatusFilter("all");
                    setDirFilter("all");
                    setCityOnly(false);
                  }}
                >
                  Reset
                </button>
              )}
            </div>

            {showMoreFilters && (
              <div className="filtersPanel">
                <div className="filtersPanelGrid">
                  <div>
                    <div
                      className="mini"
                      style={{ opacity: 0.75, marginBottom: 6 }}
                    >
                      Status
                    </div>
                    <div
                      className="segmented"
                      role="tablist"
                      aria-label="Status filter"
                    >
                      <button
                        className={`segBtn ${statusFilter === "all" ? "segBtnOn" : ""}`}
                        onClick={() => setStatusFilter("all")}
                        type="button"
                      >
                        All
                      </button>
                      <button
                        className={`segBtn ${statusFilter === "missed" ? "segBtnOn" : ""}`}
                        onClick={() => setStatusFilter("missed")}
                        type="button"
                      >
                        Missed
                      </button>
                      <button
                        className={`segBtn ${statusFilter === "completed" ? "segBtnOn" : ""}`}
                        onClick={() => setStatusFilter("completed")}
                        type="button"
                      >
                        Completed
                      </button>
                    </div>
                  </div>

                  <div>
                    <div
                      className="mini"
                      style={{ opacity: 0.75, marginBottom: 6 }}
                    >
                      Direction
                    </div>
                    <div
                      className="segmented"
                      role="tablist"
                      aria-label="Direction filter"
                    >
                      <button
                        className={`segBtn ${dirFilter === "all" ? "segBtnOn" : ""}`}
                        onClick={() => setDirFilter("all")}
                        type="button"
                      >
                        All
                      </button>
                      <button
                        className={`segBtn ${dirFilter === "inbound" ? "segBtnOn" : ""}`}
                        onClick={() => setDirFilter("inbound")}
                        type="button"
                      >
                        Inbound
                      </button>
                      <button
                        className={`segBtn ${dirFilter === "outbound" ? "segBtnOn" : ""}`}
                        onClick={() => setDirFilter("outbound")}
                        type="button"
                      >
                        Outbound
                      </button>
                    </div>
                  </div>

                  <label className="toggleRow">
                    <input
                      type="checkbox"
                      checked={cityOnly}
                      onChange={(e) => setCityOnly(e.target.checked)}
                    />
                    <span>
                      Only calls with City{" "}
                      <span className="mini" style={{ opacity: 0.75 }}>
                        (optional field)
                      </span>
                    </span>
                  </label>
                </div>
              </div>
            )}
          </div>

          {err ? (
            <div
              className="mini"
              style={{ color: "var(--danger)", marginTop: 10 }}
            >
              ❌ {err}
            </div>
          ) : (
            <div className="filtersFooter">
              

              <div className="deltaRow">
                <span className="deltaHint">
                  <div className="mini" style={{ opacity: 0.9 }}>
                Showing: <b>{rangeLabel}</b>
                {mapSelected ? (
                  <>
                    {" "}
                    • State: <b>{mapSelected}</b>
                  </>
                ) : null}{" "}
                • Total calls: <b>{kpis.total}</b> • Missed rate:{" "}
                <b>{kpis.missRate}%</b> • Completed rate:{" "}
                <b>{kpis.completedRate}%</b>
              </div>
                </span>

                <span className="deltaPills">
                  <span
                    className={`deltaPill ${
                      deltas.callsPct !== null && deltas.callsPct < 0
                        ? "deltaDown"
                        : deltas.callsPct !== null
                          ? "deltaUp"
                          : ""
                    }`}
                  >
                    Calls:{" "}
                    {deltas.callsPct === null
                      ? "—"
                      : `${Math.round(deltas.callsPct)}%`}
                  </span>
                  <span
                    className={`deltaPill ${
                      deltas.missRatePct !== null && deltas.missRatePct > 0
                        ? "deltaDown"
                        : deltas.missRatePct !== null
                          ? "deltaUp"
                          : ""
                    }`}
                  >
                    Miss rate:{" "}
                    {deltas.missRatePct === null
                      ? "—"
                      : `${Math.round(deltas.missRatePct)}%`}
                  </span>
                  <span
                    className={`deltaPill ${
                      deltas.avgDurPct !== null && deltas.avgDurPct < 0
                        ? "deltaDown"
                        : deltas.avgDurPct !== null
                          ? "deltaUp"
                          : ""
                    }`}
                  >
                    Avg dur:{" "}
                    {deltas.avgDurPct === null
                      ? "—"
                      : `${Math.round(deltas.avgDurPct)}%`}
                  </span>
                </span>
              </div>
            </div>
          )}
        </div>
      </section>

      {/* SUMMARY */}
      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Summary</h2>
            <div className="cardSubtitle">
              KPIs del rango seleccionado{" "}
              {mapSelected ? " (filtrado por estado)" : ""} + filtros avanzados.
            </div>
          </div>
          <div className="badge">{loading ? "loading…" : "ready"}</div>
        </div>

        <div className="cardBody">
          <div className="kpiRow kpiRowWide">
            <div className="kpi">
              <p className="n">{kpis.total}</p>
              <p className="l">Total calls</p>
            </div>

            <div className="kpi">
              <p className="n">{kpis.missed}</p>
              <p className="l">Missed calls (No-answer + Voicemail)</p>
            </div>

            <div className="kpi">
              <p className="n">{kpis.completed}</p>
              <p className="l">Completed</p>
            </div>

            <div className="kpi">
              <p className="n">{kpis.missRate}%</p>
              <p className="l">Miss rate (risk indicator)</p>
            </div>

            <div className="kpi">
              <p className="n">{kpis.completedRate}%</p>
              <p className="l">Completed rate (conversion proxy)</p>
            </div>

            <div className="kpi">
              <p className="n">{kpis.avgDur}s</p>
              <p className="l">Avg duration (talk quality proxy)</p>
            </div>

            <div className="kpi">
              <p className="n">{msToHuman(kpis.totalTalk)}</p>
              <p className="l">Total talk time (capacity + demand)</p>
            </div>

            <div className="kpi">
              <p className="n">
                {kpis.inbound} / {kpis.outbound}
              </p>
              <p className="l">Inbound / Outbound mix</p>
            </div>

            <div className="kpiDate">
              <p className="n">
                {kpis.lastCallIso ? fmtDateLocal(kpis.lastCallIso) : "—"}
              </p>
              <p className="l">Last call</p>
            </div>
          </div>

          <div className="mini" style={{ marginTop: 10 }}>
            Lectura estratégica: <b>Miss rate alto + volumen alto</b> = pérdida
            directa de revenue → revisar staffing, tiempos de respuesta y
            routing. <b>Avg duration muy bajo</b> puede indicar llamadas
            cortadas, contestador o lead quality bajo.
          </div>
        </div>
      </section>

      {/* Calls by state */}
      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Calls by state</h2>
            <div className="cardSubtitle">
              Mapa grande + drilldown real. El chart se ajusta al filtro y al
              estado seleccionado.
            </div>
          </div>

          <div className="cardHeaderActions">
            <div className="segmented" role="tablist" aria-label="Trend grain">
              <button
                className={`segBtn ${grain === "day" ? "segBtnOn" : ""}`}
                onClick={() => setGrain("day")}
                type="button"
              >
                Day
              </button>
              <button
                className={`segBtn ${grain === "week" ? "segBtnOn" : ""}`}
                onClick={() => setGrain("week")}
                type="button"
              >
                Week
              </button>
              <button
                className={`segBtn ${grain === "month" ? "segBtnOn" : ""}`}
                onClick={() => setGrain("month")}
                type="button"
              >
                Month
              </button>
            </div>

            <button className="smallBtn" onClick={clearSelection} type="button">
              Clear
            </button>
          </div>
        </div>

        <div className="cardBody">
          {!data?.rows?.length ? (
            <div className="mini">
              {loading ? "Loading..." : "No calls in this range."}
            </div>
          ) : (
            <div className="callsPlatform">
              {/* Trend */}
              <div className="trendCard">
                <div className="trendHeader">
                  <div>
                    <div className="trendTitle">Calls trend ({grain})</div>
                    <div className="mini" style={{ marginTop: 6 }}>
                      Útil para ver estacionalidad, picos, y si el “missed rate”
                      coincide con días/semanas.
                    </div>
                  </div>

                  <div className="trendMeta">
                    <div className="miniCard">
                      <div className="miniCardLabel">Max / point</div>
                      <div className="miniCardValue">
                        <b>{trendSummary.max}</b>
                      </div>
                    </div>

                    <div className="miniCard">
                      <div className="miniCardLabel">
                        {hoverPoint ? "Hover" : "Total"}
                      </div>
                      <div className="miniCardValue">
                        <b>
                          {hoverPoint ? hoverPoint.calls : trendSummary.total}
                        </b>
                        {hoverPoint ? (
                          <span
                            className="mini"
                            style={{ marginLeft: 8, opacity: 0.85 }}
                          >
                            • {hoverPoint.label}
                          </span>
                        ) : null}
                      </div>
                    </div>
                  </div>
                </div>

                {trendPoints.length ? (
                  <LineTrend
                    points={trendPoints}
                    height={240}
                    onHover={setHoverPoint}
                  />
                ) : (
                  <div className="mini" style={{ marginTop: 10 }}>
                    No data for chart.
                  </div>
                )}
              </div>

              {/* Map + panel */}
              <div className="mapGrid">
                <div className="mapCard">
                  <div className="mapCardTop">
                    <div>
                      <div className="mapCardTitle">US Calls Map</div>
                      <div className="mini" style={{ marginTop: 6 }}>
                        Tip: Click en un estado → filtra KPIs + chart + heatmap
                        + table. Hover muestra labels.
                      </div>
                    </div>

                    <div className="mapScale">
                      <span className="mini">Low</span>
                      <div className="scaleBar" aria-hidden="true">
                        <div className="scaleFill" />
                      </div>
                      <span className="mini">High</span>
                    </div>
                  </div>

                  <div className="mapFrame mapFrameXL">
                    <UsaChoroplethProgressMap
                      rows={mapRows as any}
                      metric={"calls" as any}
                      labelMode={"value" as any}
                      valueField={"__value" as any}
                      selectedState={mapSelected}
                      onPick={(name) =>
                        setMapSelected(normalizeStateName(name))
                      }
                    />
                  </div>
                </div>

                <aside className="statePanel">
                  <div className="statePanelTop">
                    <div className="mini" style={{ opacity: 0.85 }}>
                      State analytics
                    </div>

                    {mapSelected ? (
                      <div className="stateHead">
                        <div className="stateName">{mapSelected}</div>
                        <div className="statePill">
                          {selectedCalls ?? 0} calls
                        </div>
                      </div>
                    ) : (
                      <div className="mini" style={{ marginTop: 10 }}>
                        Click a state to drill down.
                      </div>
                    )}

                    {mapSelected && (
                      <div
                        className="mini"
                        style={{ marginTop: 6, opacity: 0.85 }}
                      >
                        Last call:{" "}
                        <b>
                          {kpis.lastCallIso
                            ? fmtDateLocal(kpis.lastCallIso)
                            : "—"}
                        </b>
                      </div>
                    )}
                  </div>

                  <div className="stateCards">
                    <div className="stateKpi">
                      <div className="mini">Missed</div>
                      <div className="stateKpiN">{kpis.missed}</div>
                      <div className="mini" style={{ opacity: 0.85 }}>
                        No-answer + Voicemail
                      </div>
                    </div>

                    <div className="stateKpi">
                      <div className="mini">Completed</div>
                      <div className="stateKpiN">{kpis.completed}</div>
                      <div className="mini" style={{ opacity: 0.85 }}>
                        Conversion proxy
                      </div>
                    </div>

                    <div className="stateKpi">
                      <div className="mini">Miss rate</div>
                      <div className="stateKpiN">{kpis.missRate}%</div>
                      <div className="mini" style={{ opacity: 0.85 }}>
                        Risk indicator
                      </div>
                    </div>

                    <div className="stateKpi">
                      <div className="mini">Avg duration</div>
                      <div className="stateKpiN">{kpis.avgDur}s</div>
                      <div className="mini" style={{ opacity: 0.85 }}>
                        Talk quality proxy
                      </div>
                    </div>

                    <div className="stateKpi">
                      <div className="mini">Total talk time</div>
                      <div className="stateKpiN">
                        {msToHuman(kpis.totalTalk)}
                      </div>
                      <div className="mini" style={{ opacity: 0.85 }}>
                        Capacity + demand
                      </div>
                    </div>

                    <div className="stateKpi">
                      <div className="mini">Inbound / Outbound</div>
                      <div className="stateKpiN">
                        {kpis.inbound} / {kpis.outbound}
                      </div>
                      <div className="mini" style={{ opacity: 0.85 }}>
                        Mix
                      </div>
                    </div>
                  </div>

                  {/* AI Strategist */}
                  <div className="aiCard">
                    <div className="aiCardTop">
                      <div>
                        <div className="aiTitle">AI Strategist</div>
                        <div
                          className="mini"
                          style={{ opacity: 0.85, marginTop: 4 }}
                        >
                          Insights accionables basados en KPIs, deltas, filtros
                          y top states.
                        </div>
                      </div>

                      <button
                        className="smallBtn aiBtn"
                        onClick={generateInsights}
                        disabled={aiLoading || loading || !data?.rows?.length}
                        type="button"
                      >
                        {aiLoading ? "Generating…" : "Generate insights"}
                      </button>
                    </div>

                    {aiErr ? (
                      <div
                        className="mini"
                        style={{ color: "var(--danger)", marginTop: 10 }}
                      >
                        ❌ {aiErr}
                      </div>
                    ) : null}

                    {aiInsights ? (
                      <div className="aiBody">
                        <div className="aiSummary">
                          <div className="aiSummaryTitle">
                            Executive summary
                          </div>
                          <div className="aiText">
                            {aiInsights.executive_summary}
                          </div>
                        </div>

                        <div className="aiScore">
                          <span
                            className={`aiBadge ${
                              aiInsights.scorecard?.health || ""
                            }`}
                          >
                            {String(
                              aiInsights.scorecard?.health || "mixed",
                            ).toUpperCase()}
                          </span>
                          <div
                            className="mini"
                            style={{ marginTop: 8, opacity: 0.9 }}
                          >
                            <b>Primary risk:</b>{" "}
                            {aiInsights.scorecard?.primary_risk}
                          </div>
                          <div
                            className="mini"
                            style={{ marginTop: 6, opacity: 0.9 }}
                          >
                            <b>Primary opportunity:</b>{" "}
                            {aiInsights.scorecard?.primary_opportunity}
                          </div>
                        </div>

                        {!!aiInsights.opportunities?.length && (
                          <div className="aiBlock">
                            <div className="aiBlockTitle">
                              Top opportunities
                            </div>
                            <div className="aiOps">
                              {aiInsights.opportunities
                                .slice(0, 3)
                                .map((o: any, idx: number) => (
                                  <div className="aiOp" key={idx}>
                                    <div className="aiOpHead">
                                      <div className="aiOpTitle">{o.title}</div>
                                      <span
                                        className={`aiImpact ${o.expected_impact}`}
                                      >
                                        {String(
                                          o.expected_impact || "medium",
                                        ).toUpperCase()}
                                      </span>
                                    </div>
                                    <div
                                      className="mini"
                                      style={{ opacity: 0.9, marginTop: 6 }}
                                    >
                                      <b>Why:</b> {o.why_it_matters}
                                    </div>
                                    <div
                                      className="mini"
                                      style={{ opacity: 0.85, marginTop: 6 }}
                                    >
                                      <b>Evidence:</b> {o.evidence}
                                    </div>
                                    {Array.isArray(o.recommended_actions) &&
                                    o.recommended_actions.length ? (
                                      <ul className="aiList">
                                        {o.recommended_actions
                                          .slice(0, 4)
                                          .map((a: string, i: number) => (
                                            <li key={i}>{a}</li>
                                          ))}
                                      </ul>
                                    ) : null}
                                  </div>
                                ))}
                            </div>
                          </div>
                        )}

                        {!!aiInsights.quick_wins_next_7_days?.length && (
                          <div className="aiBlock">
                            <div className="aiBlockTitle">
                              Quick wins (7 days)
                            </div>
                            <ul className="aiList">
                              {aiInsights.quick_wins_next_7_days
                                .slice(0, 6)
                                .map((x: string, i: number) => (
                                  <li key={i}>{x}</li>
                                ))}
                            </ul>
                          </div>
                        )}

                        {!!aiInsights.experiments_next_30_days?.length && (
                          <div className="aiBlock">
                            <div className="aiBlockTitle">
                              Experiments (30 days)
                            </div>
                            <ul className="aiList">
                              {aiInsights.experiments_next_30_days
                                .slice(0, 5)
                                .map((x: string, i: number) => (
                                  <li key={i}>{x}</li>
                                ))}
                            </ul>
                          </div>
                        )}
                      </div>
                    ) : (
                      <div className="aiPlaceholder">
                        <div className="mini" style={{ opacity: 0.85 }}>
                          Transcript intelligence (MVP)
                        </div>
                        <div className="nextPhaseTitle">
                          Quality score + next best action (sin transcript
                          todavía)
                        </div>

                        <div className="npScoreRow">
                          <div className="npScore">
                            <div className="mini" style={{ opacity: 0.8 }}>
                              Call Quality Score
                            </div>
                            <div className="npScoreN">
                              {transcriptSignals.qualityScore}/100
                            </div>
                          </div>
                        </div>

                        <div className="nextPhaseGrid">
                          <div className="nextPhaseItem">
                            <div className="npTitle">Signals</div>
                            <ul className="aiList">
                              {transcriptSignals.notes
                                .slice(0, 4)
                                .map((x, i) => (
                                  <li key={i}>{x}</li>
                                ))}
                            </ul>
                          </div>

                          <div className="nextPhaseItem">
                            <div className="npTitle">Next best actions</div>
                            <ul className="aiList">
                              {transcriptSignals.nextBestActions
                                .slice(0, 4)
                                .map((x, i) => (
                                  <li key={i}>{x}</li>
                                ))}
                            </ul>
                          </div>
                        </div>

                        <div
                          className="mini"
                          style={{ marginTop: 10, opacity: 0.7 }}
                        >
                          Próximo upgrade: enchufar transcripts → intent,
                          objeciones, lead score y follow-up recomendado.
                        </div>
                      </div>
                    )}
                  </div>

                  <div style={{ marginTop: 12 }}>
                    <AiAgentChatPanel
                      agent="calls"
                      title="Calls Agent Chat"
                      context={{
                        preset,
                        customStart,
                        customEnd,
                        selectedState: mapSelected || null,
                        statusFilter,
                        dirFilter,
                        trendGrain: grain,
                        kpis,
                      }}
                    />
                  </div>
                </aside>
              </div>

              {/* Heatmap (filtered scope) */}
              <div style={{ marginTop: 14 }}>
                <HourlyHeatmap rows={filteredRows as any} />
              </div>

              {/* Prev deltas by state */}
              <div className="card" style={{ marginTop: 14 }}>
                <div className="cardHeader">
                  <div>
                    <h2 className="cardTitle">Prev deltas by state</h2>
                    <div className="cardSubtitle">
                      Comparación real por estado (calls, miss rate, avg
                      duration). Top 20 por volumen.
                    </div>
                  </div>
                  <div className="badge">
                    {prevData ? "enabled" : "no prev"}
                  </div>
                </div>

                <div className="cardBody">
                  {!prevData ? (
                    <div className="mini">
                      No prev period available for this selection.
                    </div>
                  ) : (
                    <div className="tableWrap tableScrollX">
                      <table className="table">
                        <thead>
                          <tr>
                            <th className="th">State</th>
                            <th className="th">Calls</th>
                            <th className="th">Δ Calls</th>
                            <th className="th">Miss rate</th>
                            <th className="th">Δ Miss</th>
                            <th className="th">Avg dur</th>
                            <th className="th">Δ Dur</th>
                          </tr>
                        </thead>
                        <tbody>
                          {stateDeltas.map((r) => (
                            <tr key={r.state} className="tr">
                              <td className="td">
                                <b>{r.state}</b>
                              </td>
                              <td className="td">{r.now.total}</td>
                              <td className="td">
                                {r.dCalls === null ? (
                                  "—"
                                ) : (
                                  <span
                                    className={`deltaPill ${
                                      r.dCalls < 0 ? "deltaDown" : "deltaUp"
                                    }`}
                                  >
                                    {Math.round(r.dCalls)}%
                                  </span>
                                )}
                              </td>
                              <td className="td">{r.now.missRate}%</td>
                              <td className="td">
                                {r.dMissRate === null ? (
                                  "—"
                                ) : (
                                  <span
                                    className={`deltaPill ${
                                      r.dMissRate > 0 ? "deltaDown" : "deltaUp"
                                    }`}
                                  >
                                    {Math.round(r.dMissRate)}%
                                  </span>
                                )}
                              </td>
                              <td className="td">{r.now.avgDur}s</td>
                              <td className="td">
                                {r.dAvgDur === null ? (
                                  "—"
                                ) : (
                                  <span
                                    className={`deltaPill ${
                                      r.dAvgDur < 0 ? "deltaDown" : "deltaUp"
                                    }`}
                                  >
                                    {Math.round(r.dAvgDur)}%
                                  </span>
                                )}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}

                  <div
                    className="mini"
                    style={{ marginTop: 10, opacity: 0.75 }}
                  >
                    Interpretación: Δ Calls te dice demanda; Δ Miss rate te dice
                    fricción operacional; Δ Avg dur te ayuda a detectar
                    calidad/engagement.
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </section>

      {/* Table */}
      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Calls table</h2>
            <div className="cardSubtitle">
              Detalle de llamadas (ordenado por fecha desc){" "}
              {mapSelected ? " • filtrado por estado" : ""} + filtros avanzados.
            </div>
          </div>
          <div className="badge">{tableRows.length} rows</div>
        </div>

        <div className="cardBody">
          {!tableRows.length ? (
            <div className="mini">{loading ? "Loading..." : "No rows."}</div>
          ) : (
            <div className="tableWrap tableScrollX">
              <table className="table">
                <thead>
                  <tr>
                    <th className="th">Start Time</th>
                    <th className="th">From</th>
                    <th className="th">From State</th>
                    <th className="th">From City</th>
                    <th className="th">Direction</th>
                    <th className="th">Duration</th>
                    <th className="th">Status</th>
                    <th className="th">Client</th>
                    <th className="th">Email</th>
                  </tr>
                </thead>
                <tbody>
                  {tableRows.map((r, i) => {
                    const startIso = String(r.__startIso || "");
                    const from = String(r["Phone Call From"] || "—");
                    const st = String(
                      r.__fromStateNorm || r["Phone Call From State"] || "—",
                    );
                    const city = String(
                      r.__fromCityNorm || r["Phone Call From City"] || "—",
                    );
                    const dir = String(r["Phone Call Direction"] || "—");
                    const dur = r["Phone Call Duration"];
                    const status = String(r["Phone Call Status"] || "—");
                    const client = String(r["Client Name"] || "—");
                    const email = String(r["Email"] || "—");

                    return (
                      <tr key={i} className="tr">
                        <td className="td">
                          <span className="mini">{fmtDateLocal(startIso)}</span>
                        </td>
                        <td className="td">{from}</td>
                        <td className="td">{st}</td>
                        <td className="td">{city}</td>
                        <td className="td">{dir}</td>
                        <td className="td">
                          {dur === "" || dur === null || dur === undefined
                            ? "—"
                            : String(dur)}
                        </td>
                        <td className="td">{status}</td>
                        <td className="td">{client}</td>
                        <td className="td">{email}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          <div className="mini" style={{ marginTop: 10 }}>
            Próxima fase completada aquí: (1) prev deltas por estado, (2)
            filtros status/direction, (3) heatmap por hora, (4) transcript
            intelligence MVP.
          </div>
        </div>
      </section>
    </div>
  );
}
