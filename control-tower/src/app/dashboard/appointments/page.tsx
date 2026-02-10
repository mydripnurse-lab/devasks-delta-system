"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import dynamic from "next/dynamic";
import AiAgentChatPanel from "@/components/AiAgentChatPanel";

const UsaChoroplethProgressMap = dynamic(
  () => import("@/components/UsaChoroplethProgressMap"),
  { ssr: false },
);

type RangePreset = "1d" | "7d" | "28d" | "1m" | "3m" | "6m" | "1y" | "custom";
type TrendGrain = "day" | "week" | "month";

type ApptRow = {
  id: string;
  locationId: string;
  contactId: string;
  contactName: string;
  title: string;
  status: string;
  statusNormalized: "scheduled" | "confirmed" | "completed" | "cancelled" | "no_show" | "rescheduled" | "unknown";
  calendarId: string;
  startAt: string;
  endAt: string;
  __startMs: number | null;
  state: string;
  city: string;
  stateFrom: "appointment" | "contact.state" | "unknown";
};

type LostBookingRow = {
  id: string;
  locationId: string;
  contactId: string;
  contactName: string;
  pipelineId: string;
  pipelineName: string;
  stageId: string;
  stageName: string;
  source: string;
  state: string;
  county: string;
  city: string;
  accountName: string;
  value: number;
  currency: string;
  status: string;
  createdAt: string;
  updatedAt: string;
  __eventMs: number | null;
};

type AppointmentsApiResponse = {
  ok: boolean;
  range?: { start: string; end: string };
  total?: number;
  kpis?: {
    total: number;
    uniqueContacts: number;
    scheduled: number;
    confirmed: number;
    completed: number;
    cancelled: number;
    noShow: number;
    rescheduled: number;
    showRate: number;
    cancellationRate: number;
    noShowRate: number;
    withState: number;
    stateRate: number;
  };
  byState?: Record<string, number>;
  byStatus?: Record<string, number>;
  byLocation?: Record<string, number>;
  rows?: ApptRow[];
  lostBookings?: {
    total: number;
    uniqueContacts: number;
    valueTotal: number;
    withState: number;
    stateRate: number;
    byState: Record<string, number>;
    byCounty: Record<string, number>;
    byCity: Record<string, number>;
    rows: LostBookingRow[];
  };
  cache?: {
    source?: "memory" | "snapshot" | "ghl_refresh";
    snapshotUpdatedAt?: string;
    snapshotCoverage?: { newestStartAt: string; oldestStartAt: string };
    refreshedLocations?: number;
    totalLocations?: number;
    usedIncremental?: boolean;
    refreshReason?: string;
  };
  error?: string;
};

type AiOpportunity = {
  title: string;
  why_it_matters: string;
  evidence: string;
  expected_impact: "low" | "medium" | "high";
  recommended_actions: string[];
};

type AiInsights = {
  executive_summary: string;
  scorecard?: {
    health?: "good" | "mixed" | "bad";
    primary_risk?: string;
    primary_opportunity?: string;
  };
  opportunities?: AiOpportunity[];
  quick_wins_next_7_days?: string[];
  experiments_next_30_days?: string[];
};

type TrendPoint = { key: string; label: string; value: number };

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

function fmtDateLocal(iso?: string) {
  if (!iso) return "-";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "-";
  return d.toLocaleString();
}

function norm(v: unknown) {
  return String(v ?? "").trim();
}

function pctDelta(curr: number, prev: number) {
  if (!Number.isFinite(curr) || !Number.isFinite(prev)) return null;
  if (prev === 0) return curr === 0 ? 0 : 100;
  return ((curr - prev) / prev) * 100;
}

function fmtDelta(v: number | null) {
  if (v === null || !Number.isFinite(v)) return "-";
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(1)}%`;
}

function deltaClass(v: number | null) {
  if (v === null || !Number.isFinite(v)) return "";
  return v < 0 ? "deltaDown" : "deltaUp";
}

function fmtInt(v: unknown) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "0";
  return Math.round(n).toLocaleString();
}

function fmtMoney(v: unknown) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "$0.00";
  return n.toLocaleString(undefined, { style: "currency", currency: "USD" });
}

function startOfISOWeek(d: Date) {
  const x = new Date(d);
  const day = x.getDay();
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

function buildTrend(rows: ApptRow[], grain: TrendGrain) {
  const m = new Map<string, number>();
  for (const r of rows) {
    const ms = Number(r.__startMs ?? NaN);
    if (!Number.isFinite(ms)) continue;
    const k = keyForGrain(ms, grain);
    m.set(k, (m.get(k) || 0) + 1);
  }
  const keys = Array.from(m.keys()).sort((a, b) => a.localeCompare(b));
  return keys.map((k) => ({ key: k, label: labelForGrainKey(k, grain), value: m.get(k) || 0 }));
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
  const maxY = Math.max(...points.map((p) => p.value), 1);
  const plotW = w - padL - padR;
  const plotH = h - padT - padB;

  const xFor = (i: number) => padL + (plotW * i) / Math.max(1, points.length - 1);
  const yFor = (v: number) => padT + plotH * (1 - v / (maxY || 1));

  const d = points
    .map((p, i) => {
      const x = xFor(i);
      const y = yFor(p.value);
      return `${i === 0 ? "M" : "L"} ${x} ${y}`;
    })
    .join(" ");

  const areaD = points.length
    ? `${d} L ${xFor(points.length - 1)} ${h - padB} L ${xFor(0)} ${h - padB} Z`
    : "";

  return (
    <div style={{ width: "100%", overflowX: "auto" }}>
      <svg viewBox={`0 0 ${w} ${h}`} style={{ width: "100%", minWidth: 640, display: "block" }}>
        <defs>
          <linearGradient id="apptLineGrad" x1="0" x2="1" y1="0" y2="0">
            <stop offset="0%" stopColor="rgba(96,165,250,0.95)" />
            <stop offset="100%" stopColor="rgba(52,211,153,0.95)" />
          </linearGradient>
          <linearGradient id="apptAreaGrad" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="rgba(96,165,250,0.28)" />
            <stop offset="100%" stopColor="rgba(96,165,250,0.03)" />
          </linearGradient>
        </defs>

        {[0, 0.25, 0.5, 0.75, 1].map((t) => {
          const y = padT + plotH * t;
          return (
            <line
              key={t}
              x1={padL}
              y1={y}
              x2={w - padR}
              y2={y}
              stroke="rgba(255,255,255,0.08)"
              strokeWidth="1"
            />
          );
        })}

        {areaD ? <path d={areaD} fill="url(#apptAreaGrad)" /> : null}
        {d ? <path d={d} fill="none" stroke="url(#apptLineGrad)" strokeWidth="3" strokeLinecap="round" /> : null}

        {points.map((p, i) => {
          const x = xFor(i);
          const y = yFor(p.value);
          return (
            <g
              key={p.key}
              onMouseEnter={() => onHover?.(p)}
              onMouseLeave={() => onHover?.(null)}
              style={{ cursor: "pointer" }}
            >
              <circle cx={x} cy={y} r={4} fill="rgba(96,165,250,0.95)" />
            </g>
          );
        })}

        {points.map((p, i) => {
          if (i % Math.max(1, Math.floor(points.length / 8)) !== 0 && i !== points.length - 1) {
            return null;
          }
          const x = xFor(i);
          return (
            <text key={`${p.key}_lbl`} x={x} y={h - 12} textAnchor="middle" fill="rgba(255,255,255,0.68)" fontSize="11">
              {p.label}
            </text>
          );
        })}
      </svg>
    </div>
  );
}

export default function AppointmentsDashboardPage() {
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");

  const [preset, setPreset] = useState<RangePreset>("7d");
  const [customStart, setCustomStart] = useState("");
  const [customEnd, setCustomEnd] = useState("");
  const [grain, setGrain] = useState<TrendGrain>("day");

  const [data, setData] = useState<AppointmentsApiResponse | null>(null);
  const [prevData, setPrevData] = useState<AppointmentsApiResponse | null>(null);
  const [mapSelected, setMapSelected] = useState("");
  const [hoverPoint, setHoverPoint] = useState<TrendPoint | null>(null);

  const [aiLoading, setAiLoading] = useState(false);
  const [aiErr, setAiErr] = useState("");
  const [aiInsights, setAiInsights] = useState<AiInsights | null>(null);

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
      const startD = customStart ? new Date(`${customStart}T00:00:00`) : null;
      const endD = customEnd ? new Date(`${customEnd}T00:00:00`) : null;
      return {
        start: startD ? isoStartOfDay(startD) : "",
        end: endD ? isoEndOfDay(endD) : "",
      };
    }
    return { start: "", end: "" };
  }, [preset, customStart, customEnd]);

  async function load(force = false) {
    setErr("");
    setLoading(true);
    setAiErr("");
    setAiInsights(null);
    try {
      if (!computedRange.start || !computedRange.end) {
        throw new Error("Missing start/end range");
      }

      const qs = new URLSearchParams();
      qs.set("start", computedRange.start);
      qs.set("end", computedRange.end);
      if (force) qs.set("bust", "1");

      const currRes = await fetch(`/api/dashboard/appointments?${qs.toString()}`, { cache: "no-store" });
      const curr = (await currRes.json()) as AppointmentsApiResponse;
      if (!currRes.ok || !curr?.ok) throw new Error(curr?.error || `HTTP ${currRes.status}`);
      setData(curr);

      const prev = slicePrevPeriod(computedRange.start, computedRange.end);
      if (prev.prevStart && prev.prevEnd) {
        const pQs = new URLSearchParams();
        pQs.set("start", prev.prevStart);
        pQs.set("end", prev.prevEnd);
        if (force) pQs.set("bust", "1");
        const prevRes = await fetch(`/api/dashboard/appointments?${pQs.toString()}`, { cache: "no-store" });
        const prevJson = (await prevRes.json()) as AppointmentsApiResponse;
        if (prevRes.ok && prevJson?.ok) setPrevData(prevJson);
        else setPrevData(null);
      } else {
        setPrevData(null);
      }
    } catch (e: unknown) {
      setData(null);
      setPrevData(null);
      setErr(e instanceof Error ? e.message : "Failed to load appointments dashboard");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (preset !== "custom") load(false);
    else if (customStart && customEnd) load(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [preset, customStart, customEnd]);

  const rows = useMemo(() => (data?.rows || []) as ApptRow[], [data]);
  const filteredRows = useMemo(() => {
    if (!mapSelected) return rows;
    if (mapSelected === "__unknown") return rows.filter((r) => !norm(r.state));
    return rows.filter((r) => norm(r.state) === mapSelected);
  }, [rows, mapSelected]);

  const prevRows = useMemo(() => (prevData?.rows || []) as ApptRow[], [prevData]);
  const prevFilteredRows = useMemo(() => {
    if (!mapSelected) return prevRows;
    if (mapSelected === "__unknown") return prevRows.filter((r) => !norm(r.state));
    return prevRows.filter((r) => norm(r.state) === mapSelected);
  }, [prevRows, mapSelected]);

  const kpis = useMemo(() => {
    const total = filteredRows.length;
    const uniqueContacts = new Set(filteredRows.map((r) => r.contactId).filter(Boolean)).size;
    const counts = {
      scheduled: 0,
      confirmed: 0,
      completed: 0,
      cancelled: 0,
      no_show: 0,
      rescheduled: 0,
      unknown: 0,
    } as Record<string, number>;

    let withState = 0;
    for (const r of filteredRows) {
      counts[r.statusNormalized] = (counts[r.statusNormalized] || 0) + 1;
      if (norm(r.state)) withState++;
    }

    const showDen = counts.completed + counts.no_show;

    return {
      total,
      uniqueContacts,
      scheduled: counts.scheduled || 0,
      confirmed: counts.confirmed || 0,
      completed: counts.completed || 0,
      cancelled: counts.cancelled || 0,
      noShow: counts.no_show || 0,
      rescheduled: counts.rescheduled || 0,
      showRate: showDen ? Math.round((counts.completed / showDen) * 100) : 0,
      cancellationRate: total ? Math.round((counts.cancelled / total) * 100) : 0,
      noShowRate: showDen ? Math.round((counts.no_show / showDen) * 100) : 0,
      stateRate: total ? Math.round((withState / total) * 100) : 0,
    };
  }, [filteredRows]);

  const prevKpis = useMemo(() => {
    const total = prevFilteredRows.length;
    const uniqueContacts = new Set(prevFilteredRows.map((r) => r.contactId).filter(Boolean)).size;
    let completed = 0;
    let noShow = 0;
    let cancelled = 0;
    let withState = 0;
    for (const r of prevFilteredRows) {
      if (r.statusNormalized === "completed") completed++;
      else if (r.statusNormalized === "no_show") noShow++;
      else if (r.statusNormalized === "cancelled") cancelled++;
      if (norm(r.state)) withState++;
    }
    const showDen = completed + noShow;
    return {
      total,
      uniqueContacts,
      showRate: showDen ? Math.round((completed / showDen) * 100) : 0,
      cancellationRate: total ? Math.round((cancelled / total) * 100) : 0,
      noShowRate: showDen ? Math.round((noShow / showDen) * 100) : 0,
      stateRate: total ? Math.round((withState / total) * 100) : 0,
    };
  }, [prevFilteredRows]);

  const totalDelta = useMemo(() => (prevKpis ? pctDelta(kpis.total, prevKpis.total) : null), [kpis.total, prevKpis]);
  const contactDelta = useMemo(
    () => (prevKpis ? pctDelta(kpis.uniqueContacts, prevKpis.uniqueContacts) : null),
    [kpis.uniqueContacts, prevKpis],
  );
  const showRateDelta = useMemo(
    () => (prevKpis ? pctDelta(kpis.showRate, prevKpis.showRate) : null),
    [kpis.showRate, prevKpis],
  );
  const cancelRateDelta = useMemo(
    () => (prevKpis ? pctDelta(kpis.cancellationRate, prevKpis.cancellationRate) : null),
    [kpis.cancellationRate, prevKpis],
  );
  const noShowDelta = useMemo(
    () => (prevKpis ? pctDelta(kpis.noShowRate, prevKpis.noShowRate) : null),
    [kpis.noShowRate, prevKpis],
  );
  const stateRateDelta = useMemo(
    () => (prevKpis ? pctDelta(kpis.stateRate, prevKpis.stateRate) : null),
    [kpis.stateRate, prevKpis],
  );

  const trend = useMemo(() => buildTrend(filteredRows, grain), [filteredRows, grain]);

  const byState = useMemo(() => {
    const m: Record<string, number> = {};
    for (const r of filteredRows) {
      const st = norm(r.state);
      if (!st) m.__unknown = (m.__unknown || 0) + 1;
      else m[st] = (m[st] || 0) + 1;
    }
    return m;
  }, [filteredRows]);

  const byStatus = useMemo(() => {
    const m: Record<string, number> = {};
    for (const r of filteredRows) {
      const st = norm(r.statusNormalized || "unknown");
      m[st] = (m[st] || 0) + 1;
    }
    return m;
  }, [filteredRows]);

  const statusesSorted = useMemo(() => Object.entries(byStatus).sort((a, b) => b[1] - a[1]), [byStatus]);

  const lostRows = useMemo(() => (data?.lostBookings?.rows || []) as LostBookingRow[], [data]);
  const lostRowsFiltered = useMemo(() => {
    if (!mapSelected) return lostRows;
    if (mapSelected === "__unknown") return lostRows.filter((r) => !norm(r.state));
    return lostRows.filter((r) => norm(r.state) === mapSelected);
  }, [lostRows, mapSelected]);
  const prevLostRows = useMemo(() => (prevData?.lostBookings?.rows || []) as LostBookingRow[], [prevData]);
  const prevLostRowsFiltered = useMemo(() => {
    if (!mapSelected) return prevLostRows;
    if (mapSelected === "__unknown") return prevLostRows.filter((r) => !norm(r.state));
    return prevLostRows.filter((r) => norm(r.state) === mapSelected);
  }, [prevLostRows, mapSelected]);

  const lostKpis = useMemo(() => {
    const total = lostRowsFiltered.length;
    const uniqueContacts = new Set(lostRowsFiltered.map((r) => r.contactId).filter(Boolean)).size;
    const valueTotal = lostRowsFiltered.reduce((acc, r) => acc + Number(r.value || 0), 0);
    let withState = 0;
    for (const r of lostRowsFiltered) if (norm(r.state)) withState++;
    return {
      total,
      uniqueContacts,
      valueTotal,
      stateRate: total ? Math.round((withState / total) * 100) : 0,
      avgValue: total ? valueTotal / total : 0,
    };
  }, [lostRowsFiltered]);

  const prevLostKpis = useMemo(() => {
    const total = prevLostRowsFiltered.length;
    const uniqueContacts = new Set(prevLostRowsFiltered.map((r) => r.contactId).filter(Boolean)).size;
    const valueTotal = prevLostRowsFiltered.reduce((acc, r) => acc + Number(r.value || 0), 0);
    return {
      total,
      uniqueContacts,
      valueTotal,
      avgValue: total ? valueTotal / total : 0,
    };
  }, [prevLostRowsFiltered]);

  const lostTotalDelta = useMemo(
    () => (prevLostKpis ? pctDelta(lostKpis.total, prevLostKpis.total) : null),
    [lostKpis.total, prevLostKpis],
  );
  const lostValueDelta = useMemo(
    () => (prevLostKpis ? pctDelta(lostKpis.valueTotal, prevLostKpis.valueTotal) : null),
    [lostKpis.valueTotal, prevLostKpis],
  );
  const lostAvgDelta = useMemo(
    () => (prevLostKpis ? pctDelta(lostKpis.avgValue, prevLostKpis.avgValue) : null),
    [lostKpis.avgValue, prevLostKpis],
  );

  const lostByState = useMemo(() => {
    const m: Record<string, number> = {};
    for (const r of lostRowsFiltered) {
      const st = norm(r.state);
      if (!st) m.__unknown = (m.__unknown || 0) + 1;
      else m[st] = (m[st] || 0) + 1;
    }
    return m;
  }, [lostRowsFiltered]);

  const lostByCity = useMemo(() => {
    const m: Record<string, number> = {};
    for (const r of lostRowsFiltered) {
      const city = norm(r.city);
      if (!city) m.__unknown = (m.__unknown || 0) + 1;
      else m[city] = (m[city] || 0) + 1;
    }
    return m;
  }, [lostRowsFiltered]);

  const lostByCounty = useMemo(() => {
    const m: Record<string, number> = {};
    for (const r of lostRowsFiltered) {
      const county = norm(r.county);
      if (!county) m.__unknown = (m.__unknown || 0) + 1;
      else m[county] = (m[county] || 0) + 1;
    }
    return m;
  }, [lostRowsFiltered]);

  const lostTopCities = useMemo(
    () => Object.entries(lostByCity).sort((a, b) => b[1] - a[1]).slice(0, 8),
    [lostByCity],
  );
  const lostTopCounties = useMemo(
    () => Object.entries(lostByCounty).sort((a, b) => b[1] - a[1]).slice(0, 8),
    [lostByCounty],
  );

  const mapRows = useMemo(() => {
    const source = (data?.byState || {}) as Record<string, number>;
    return Object.entries(source)
      .filter(([state]) => state !== "__unknown")
      .map(([state, val]) => ({
        state,
        counties: { total: 1, ready: Number(val || 0), domainsActive: 0 },
        cities: { total: 0, ready: 0, domainsActive: 0 },
        __value: Number(val || 0),
      }));
  }, [data]);

  const lostMapRows = useMemo(() => {
    return Object.entries(lostByState)
      .filter(([state]) => state !== "__unknown")
      .map(([state, val]) => ({
        state,
        counties: { total: 1, ready: Number(val || 0), domainsActive: 0 },
        cities: { total: 0, ready: 0, domainsActive: 0 },
        __value: Number(val || 0),
      }));
  }, [lostByState]);

  const unknownStateCount = Number(byState.__unknown || 0);

  async function generateInsights() {
    setAiLoading(true);
    setAiErr("");
    setAiInsights(null);
    try {
      const payload = {
        range: computedRange,
        state: mapSelected || null,
        kpis,
        compare: {
          totalDelta,
          contactDelta,
          showRateDelta,
          cancelRateDelta,
          noShowDelta,
          stateRateDelta,
        },
        byStatus,
        byState,
        lostBookings: {
          kpis: lostKpis,
          byState: lostByState,
          byCounty: lostByCounty,
          byCity: lostByCity,
          rowsPreview: lostRowsFiltered.slice(0, 120).map((r) => ({
            id: r.id,
            locationId: r.locationId,
            contactName: r.contactName,
            source: r.source,
            state: r.state,
            county: r.county,
            city: r.city,
            value: r.value,
          })),
        },
        trend: trend.slice(-30),
        rowsPreview: filteredRows.slice(0, 120).map((r) => ({
          id: r.id,
          locationId: r.locationId,
          contactName: r.contactName,
          status: r.status,
          statusNormalized: r.statusNormalized,
          startAt: r.startAt,
          state: r.state,
          city: r.city,
        })),
      };

      const res = await fetch("/api/dashboard/appointments/insights", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      });
      const json = await res.json();
      if (!res.ok || !json?.ok) throw new Error(json?.error || `HTTP ${res.status}`);
      setAiInsights(json.insights || null);
    } catch (e: unknown) {
      setAiErr(e instanceof Error ? e.message : "Failed to generate AI insights");
    } finally {
      setAiLoading(false);
    }
  }

  return (
    <div className="shell callsDash contactsDash conversationsDash">
      {loading ? (
        <div className="dashLoadingOverlay" aria-live="polite" aria-busy="true">
          <div className="dashLoadingCard">
            <div className="dashSpinner" />
            <div className="dashLoadingText">Updating Appointments Dashboard...</div>
            <div className="mini" style={{ marginTop: 6 }}>
              Syncing calendars, show-rate KPIs and geo distribution.
            </div>
          </div>
        </div>
      ) : null}

      <header className="topbar">
        <div className="brand">
          <div className="logo" />
          <div>
            <h1>My Drip Nurse — Appointments Dashboard</h1>
          </div>
        </div>
        <div className="pills">
          <Link className="smallBtn" href="/dashboard">
            Back to Dashboard
          </Link>
          <div className="pill">
            <span className="dot" />
            <span>Live</span>
          </div>
        </div>
      </header>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Filters</h2>
            <div className="cardSubtitle">
              Rango afecta KPIs, mapa, trend, AI insights y tabla de appointments.
            </div>
          </div>
          <button className="smallBtn" onClick={() => load(true)} type="button" disabled={loading}>
            {loading ? "Refreshing..." : "Refresh"}
          </button>
        </div>
        <div className="cardBody">
          <div className="filtersBar">
            <div className="filtersGroup">
              <div className="filtersLabel">Range</div>
              <div className="rangePills">
                {([
                  ["1d", "1 day"],
                  ["7d", "7 days"],
                  ["28d", "28 days"],
                  ["1m", "Last month"],
                  ["3m", "Last quarter"],
                  ["6m", "Last 6 months"],
                  ["1y", "Last year"],
                  ["custom", "Custom"],
                ] as Array<[RangePreset, string]>).map(([p, label]) => (
                  <button
                    key={p}
                    className={`smallBtn ${preset === p ? "smallBtnOn" : ""}`}
                    onClick={() => setPreset(p)}
                    type="button"
                    disabled={loading}
                  >
                    {label}
                  </button>
                ))}
              </div>
            </div>
            <div className="filtersGroup dateGroup">
              <div className="filtersLabel">Custom dates</div>
              <div className="dateInputs">
                <div className="dateField">
                  <label className="mini" style={{ marginBottom: 6, display: "block" }}>Start</label>
                  <input
                    className="input"
                    type="date"
                    value={customStart}
                    onChange={(e) => setCustomStart(e.target.value)}
                    disabled={preset !== "custom" || loading}
                  />
                </div>
                <div className="dateField">
                  <label className="mini" style={{ marginBottom: 6, display: "block" }}>End</label>
                  <input
                    className="input"
                    type="date"
                    value={customEnd}
                    onChange={(e) => setCustomEnd(e.target.value)}
                    disabled={preset !== "custom" || loading}
                  />
                </div>
              </div>
            </div>
          </div>
          {err ? (
            <div className="mini" style={{ color: "var(--danger)", marginTop: 10 }}>
              X {err}
            </div>
          ) : null}
          {!err && data?.cache ? (
            <div className="mini" style={{ marginTop: 10, opacity: 0.9 }}>
              Cache: <b>{String(data.cache.source || "unknown")}</b>
              {data.cache.snapshotUpdatedAt ? ` • synced ${fmtDateLocal(data.cache.snapshotUpdatedAt)}` : ""}
              {data.cache.snapshotCoverage?.newestStartAt
                ? ` • newest appt ${fmtDateLocal(data.cache.snapshotCoverage.newestStartAt)}`
                : ""}
              {data.cache.snapshotCoverage?.oldestStartAt
                ? ` • oldest appt ${fmtDateLocal(data.cache.snapshotCoverage.oldestStartAt)}`
                : ""}
              {Number(data.cache.totalLocations || 0) > 0 ? ` • locations: ${data.cache.totalLocations}` : ""}
              {Number(data.cache.refreshedLocations || 0) > 0 ? ` • refreshed: ${data.cache.refreshedLocations}` : ""}
              {data.cache.usedIncremental ? " • incremental refresh" : ""}
            </div>
          ) : null}
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Lost bookings table (Qualified)</h2>
            <div className="cardSubtitle">Intentos de cita no completados por origen, contacto y valor potencial.</div>
          </div>
          <div className="badge">{lostRowsFiltered.length} rows</div>
        </div>
        <div className="cardBody">
          {!lostRowsFiltered.length ? (
            <div className="mini">{loading ? "Loading..." : "No lost booking rows in this range."}</div>
          ) : (
            <div className="tableWrap tableScrollX">
              <table className="table">
                <thead>
                  <tr>
                    <th className="th">Created</th>
                    <th className="th">Contact</th>
                    <th className="th">State</th>
                    <th className="th">County</th>
                    <th className="th">City</th>
                    <th className="th">Potential value</th>
                    <th className="th">Source</th>
                    <th className="th">Location ID</th>
                  </tr>
                </thead>
                <tbody>
                  {lostRowsFiltered
                    .slice()
                    .sort((a, b) => Number(b.__eventMs || 0) - Number(a.__eventMs || 0))
                    .map((r, idx) => (
                      <tr key={`${r.id || r.contactId || "lost"}_${idx}`} className="tr">
                        <td className="td"><span className="mini">{fmtDateLocal(r.createdAt || r.updatedAt)}</span></td>
                        <td className="td">{r.contactName || "-"}</td>
                        <td className="td">{r.state || "-"}</td>
                        <td className="td">{r.county || "-"}</td>
                        <td className="td">{r.city || "-"}</td>
                        <td className="td">{fmtMoney(r.value)}</td>
                        <td className="td"><span className="mini">{r.source || "-"}</span></td>
                        <td className="td"><span className="mini">{r.locationId || "-"}</span></td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Appointments KPIs ({mapSelected || "All states"})</h2>
            <div className="cardSubtitle">
              Rendimiento de citas: volumen, calidad operativa y resultados de show/no-show.
            </div>
          </div>
          <div className="badge">{kpis.total} appointments</div>
        </div>
        <div className="cardBody">
          <div className="kpiRow kpiRowWide">
            <div className="kpi">
              <p className="n">{fmtInt(kpis.total)}</p>
              <p className="l">Total appointments</p>
              <div className={`mini ${deltaClass(totalDelta)}`}>{fmtDelta(totalDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(kpis.uniqueContacts)}</p>
              <p className="l">Unique contacts</p>
              <div className={`mini ${deltaClass(contactDelta)}`}>{fmtDelta(contactDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{kpis.showRate}%</p>
              <p className="l">Show rate</p>
              <div className={`mini ${deltaClass(showRateDelta)}`}>{fmtDelta(showRateDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{kpis.cancellationRate}%</p>
              <p className="l">Cancellation rate</p>
              <div className={`mini ${deltaClass(cancelRateDelta)}`}>{fmtDelta(cancelRateDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{kpis.noShowRate}%</p>
              <p className="l">No-show rate</p>
              <div className={`mini ${deltaClass(noShowDelta)}`}>{fmtDelta(noShowDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{kpis.stateRate}%</p>
              <p className="l">State coverage</p>
              <div className={`mini ${deltaClass(stateRateDelta)}`}>{fmtDelta(stateRateDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(lostKpis.total)}</p>
              <p className="l">Lost qualified bookings</p>
              <div className={`mini ${deltaClass(lostTotalDelta)}`}>{fmtDelta(lostTotalDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtMoney(lostKpis.valueTotal)}</p>
              <p className="l">Potential value lost</p>
              <div className={`mini ${deltaClass(lostValueDelta)}`}>{fmtDelta(lostValueDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtMoney(lostKpis.avgValue)}</p>
              <p className="l">Avg lost value / attempt</p>
              <div className={`mini ${deltaClass(lostAvgDelta)}`}>{fmtDelta(lostAvgDelta)} vs prev period</div>
            </div>
          </div>
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Appointments trend</h2>
            <div className="cardSubtitle">Evolución por día, semana o mes según el filtro.</div>
          </div>
          <div className="cardHeaderActions">
            {(["day", "week", "month"] as TrendGrain[]).map((g) => (
              <button key={g} className={`smallBtn ${grain === g ? "smallBtnOn" : ""}`} onClick={() => setGrain(g)} type="button">
                {g}
              </button>
            ))}
          </div>
        </div>
        <div className="cardBody">
          {!trend.length ? (
            <div className="mini">{loading ? "Loading..." : "No trend data for this scope."}</div>
          ) : (
            <>
              <LineTrend points={trend} onHover={setHoverPoint} />
              <div className="mini" style={{ marginTop: 10 }}>
                {hoverPoint ? (
                  <>
                    {hoverPoint.label} • <b>{hoverPoint.value}</b> appointments
                  </>
                ) : (
                  <>Hover un punto para ver detalle.</>
                )}
              </div>
            </>
          )}
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Lost Booking Attempts (Qualified)</h2>
            <div className="cardSubtitle">
              Pipeline: Lead Generator Bookings • Stage: New Leads (Qualified). Origen de intentos no completados.
            </div>
          </div>
          <div className="badge">{fmtInt(lostKpis.total)} attempts</div>
        </div>
        <div className="cardBody">
          <div className="mapGrid">
            <div className="mapCard">
              <div className="mapCardTop">
                <div>
                  <div className="mapCardTitle">US Lost Bookings Map</div>
                  <div className="mini" style={{ marginTop: 6 }}>
                    Click en un estado para filtrar tabla y origen (county/city).
                  </div>
                </div>
              </div>
              <div className="mapFrame mapFrameXL">
                <UsaChoroplethProgressMap
                  rows={lostMapRows as any}
                  metric={"calls" as any}
                  labelMode={"value" as any}
                  valueField={"__value" as any}
                  selectedState={mapSelected === "__unknown" ? "" : mapSelected}
                  onPick={(name) => setMapSelected(norm(name))}
                />
              </div>
            </div>

            <aside className="statePanel">
              <div className="stateCards">
                <div className="stateKpi">
                  <div className="mini">Potential lost value</div>
                  <div className="stateKpiN">{fmtMoney(lostKpis.valueTotal)}</div>
                  <div className="mini" style={{ opacity: 0.85 }}>{lostKpis.stateRate}% state-mapped</div>
                </div>
                <div className="stateKpi">
                  <div className="mini">Top counties</div>
                  {lostTopCounties.length ? (
                    lostTopCounties.slice(0, 6).map(([name, count]) => (
                      <div key={`lost_county_${name}`} className="mini" style={{ marginTop: 6 }}>
                        {name === "__unknown" ? "Unknown county" : name}: <b>{fmtInt(count)}</b>
                      </div>
                    ))
                  ) : (
                    <div className="mini" style={{ marginTop: 6 }}>No county data.</div>
                  )}
                </div>
                <div className="stateKpi">
                  <div className="mini">Top cities / towns</div>
                  {lostTopCities.length ? (
                    lostTopCities.slice(0, 6).map(([name, count]) => (
                      <div key={`lost_city_${name}`} className="mini" style={{ marginTop: 6 }}>
                        {name === "__unknown" ? "Unknown city" : name}: <b>{fmtInt(count)}</b>
                      </div>
                    ))
                  ) : (
                    <div className="mini" style={{ marginTop: 6 }}>No city data.</div>
                  )}
                </div>
              </div>
            </aside>
          </div>
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardBody">
          <div className="mapGrid">
            <div className="mapCard">
              <div className="mapCardTop">
                <div>
                  <div className="mapCardTitle">US Appointments Map</div>
                  <div className="mini" style={{ marginTop: 6 }}>
                    Click en un estado para filtrar KPIs, trend, AI insights y tabla.
                  </div>
                </div>
              </div>
              <div className="mapFrame mapFrameXL">
                <UsaChoroplethProgressMap
                  rows={mapRows as any}
                  metric={"calls" as any}
                  labelMode={"value" as any}
                  valueField={"__value" as any}
                  selectedState={mapSelected === "__unknown" ? "" : mapSelected}
                  onPick={(name) => setMapSelected(norm(name))}
                />
              </div>
            </div>

            <aside className="statePanel">
              <div className="statePanelTop">
                <div className="mini" style={{ opacity: 0.85 }}>State analytics</div>
                {mapSelected ? (
                  <div className="stateHead">
                    <div className="stateName">{mapSelected === "__unknown" ? "Unknown" : mapSelected}</div>
                    <div className="statePill">{fmtInt((mapSelected === "__unknown" ? unknownStateCount : byState[mapSelected]) || 0)} appt</div>
                  </div>
                ) : (
                  <div className="stateHead">
                    <div className="stateName">All states</div>
                    <div className="statePill">{fmtInt(kpis.total)} appt</div>
                  </div>
                )}
              </div>

              <div className="stateCards">
                <div className="stateKpi">
                  <div className="mini">UNKNOWN STATE</div>
                  <div className="stateKpiN">{fmtInt(unknownStateCount)}</div>
                  <div className="mini" style={{ opacity: 0.85 }}>
                    Separate row for unresolved geo
                  </div>
                  <button
                    className={`smallBtn ${mapSelected === "__unknown" ? "smallBtnOn" : ""}`}
                    style={{ marginTop: 8 }}
                    onClick={() => setMapSelected(mapSelected === "__unknown" ? "" : "__unknown")}
                    type="button"
                  >
                    {mapSelected === "__unknown" ? "Show all" : "Filter unknown"}
                  </button>
                </div>

                {statusesSorted.slice(0, 6).map(([status, count]) => {
                  const share = kpis.total ? Math.round((count / kpis.total) * 100) : 0;
                  return (
                    <div className="stateKpi" key={status}>
                      <div className="mini">{status.toUpperCase()}</div>
                      <div className="stateKpiN">{fmtInt(count)}</div>
                      <div className="mini" style={{ opacity: 0.85 }}>{share}% of appointments</div>
                    </div>
                  );
                })}
              </div>

              <div className="aiCard">
                <div className="aiCardTop">
                  <div>
                    <div className="aiTitle">AI Strategist (Appointments Performance)</div>
                    <div className="mini" style={{ opacity: 0.85, marginTop: 4 }}>
                      Responde sobre show-rate, no-show, cancelaciones y plan operativo por estado.
                    </div>
                  </div>
                  <button
                    className="smallBtn aiBtn"
                    onClick={generateInsights}
                    disabled={aiLoading || loading || !rows.length}
                    type="button"
                  >
                    {aiLoading ? "Generating..." : "Generate insights"}
                  </button>
                </div>

                {aiErr ? (
                  <div className="mini" style={{ color: "var(--danger)", marginTop: 10 }}>X {aiErr}</div>
                ) : null}

                {aiInsights ? (
                  <div className="aiBody">
                    <div className="aiSummary">
                      <div className="aiSummaryTitle">Executive summary</div>
                      <div className="aiText">{aiInsights.executive_summary}</div>
                    </div>
                    <div className="aiScore">
                      <span className={`aiBadge ${aiInsights.scorecard?.health || ""}`}>
                        {String(aiInsights.scorecard?.health || "mixed").toUpperCase()}
                      </span>
                      <div className="mini" style={{ marginTop: 8, opacity: 0.9 }}>
                        <b>Primary risk:</b> {aiInsights.scorecard?.primary_risk}
                      </div>
                      <div className="mini" style={{ marginTop: 6, opacity: 0.9 }}>
                        <b>Primary opportunity:</b> {aiInsights.scorecard?.primary_opportunity}
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="aiPlaceholder mini">
                    Generate insights to get appointments strategy for this scope.
                  </div>
                )}
              </div>

              <div style={{ marginTop: 12 }}>
                <AiAgentChatPanel
                  agent="appointments"
                  title="Appointments Agent Chat"
                  context={{
                    preset,
                    customStart,
                    customEnd,
                    trendGrain: grain,
                    selectedState: mapSelected || null,
                    kpis,
                    lostBookings: {
                      total: lostKpis.total,
                      valueTotal: lostKpis.valueTotal,
                      byState: lostByState,
                      byCounty: lostByCounty,
                      byCity: lostByCity,
                    },
                    byStatus,
                    byState,
                  }}
                />
              </div>
            </aside>
          </div>
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Appointments table</h2>
            <div className="cardSubtitle">Detalle de citas para el alcance actual.</div>
          </div>
          <div className="badge">{filteredRows.length} rows</div>
        </div>
        <div className="cardBody">
          {!filteredRows.length ? (
            <div className="mini">{loading ? "Loading..." : "No rows."}</div>
          ) : (
            <div className="tableWrap tableScrollX">
              <table className="table">
                <thead>
                  <tr>
                    <th className="th">Start</th>
                    <th className="th">Contact</th>
                    <th className="th">Status</th>
                    <th className="th">Normalized</th>
                    <th className="th">State</th>
                    <th className="th">State From</th>
                    <th className="th">City</th>
                    <th className="th">Location ID</th>
                    <th className="th">Title</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows
                    .slice()
                    .sort((a, b) => Number(b.__startMs || 0) - Number(a.__startMs || 0))
                    .map((r, idx) => (
                      <tr key={`${r.id || r.contactId || "row"}_${idx}`} className="tr">
                        <td className="td"><span className="mini">{fmtDateLocal(r.startAt)}</span></td>
                        <td className="td">{r.contactName || "-"}</td>
                        <td className="td">{r.status || "-"}</td>
                        <td className="td">{r.statusNormalized || "-"}</td>
                        <td className="td">{r.state || "-"}</td>
                        <td className="td"><span className="mini">{r.stateFrom}</span></td>
                        <td className="td">{r.city || "-"}</td>
                        <td className="td"><span className="mini">{r.locationId || "-"}</span></td>
                        <td className="td">{r.title || "-"}</td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </section>
    </div>
  );
}
