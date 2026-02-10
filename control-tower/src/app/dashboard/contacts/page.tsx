// control-tower/src/app/dashboard/contacts/page.tsx
"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import UsaChoroplethProgressMap from "@/components/UsaChoroplethProgressMap";
import AiAgentChatPanel from "@/components/AiAgentChatPanel";

type RangePreset = "1d" | "7d" | "28d" | "1m" | "3m" | "6m" | "1y" | "custom";
type TrendGrain = "day" | "week" | "month";

type ContactsRow = {
  id: string;
  name: string;
  email: string;
  phone: string;
  state: string;
  city: string;
  source: string;
  tags: string[];
  dateAdded: string;
  __createdMs: number | null;
  leadType: "lead" | "guest_chat_like";
  stateFrom: "contact.state" | "opportunity.source" | "unknown";
  opportunityId?: string;
};

type ContactsApiResponse = {
  ok: boolean;
  range?: { start: string; end: string };
  total?: number;
  kpis?: {
    total: number;
    withEmail: number;
    withPhone: number;
    withState: number;
    emailRate: number;
    phoneRate: number;
    stateRate: number;
    guestChatLike: number;
    guestRate: number;
    inferredFromOpportunity: number;
  };
  byState?: Record<string, number>;
  rows?: ContactsRow[];
  error?: string;
};

type CallsApiResponse = {
  ok: boolean;
  total: number;
  byState: Record<string, number>;
  rows?: Record<string, unknown>[];
  error?: string;
};

type TrendPoint = { key: string; label: string; leads: number };
type MapMetricRow = {
  state: string;
  counties: { total: number; ready: number; domainsActive: number };
  cities: { total: number; ready: number; domainsActive: number };
  __value: number;
  [key: string]: unknown;
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

type LeadKpis = {
  total: number;
  withEmail: number;
  withPhone: number;
  withState: number;
  contactable: number;
  contactableRate: number;
  bothContactFields: number;
  bothContactRate: number;
  guestChatLike: number;
  guestRate: number;
  inferredFromOpportunity: number;
  inferredRate: number;
  lastLeadIso: string;
};

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

function buildTrend(rows: ContactsRow[], grain: TrendGrain) {
  const m = new Map<string, number>();
  for (const r of rows) {
    const ms = Number(r.__createdMs ?? NaN);
    if (!Number.isFinite(ms)) continue;
    const k = keyForGrain(ms, grain);
    m.set(k, (m.get(k) || 0) + 1);
  }
  const keys = Array.from(m.keys()).sort((a, b) => a.localeCompare(b));
  return keys.map((k) => ({
    key: k,
    label: labelForGrainKey(k, grain),
    leads: m.get(k) || 0,
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

function fmtDelta(v: number | null, suffix = "%") {
  if (v === null || !Number.isFinite(v)) return "-";
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(1)}${suffix}`;
}

function computeLeadKpis(rows: ContactsRow[]): LeadKpis {
  const total = rows.length;
  let withEmail = 0;
  let withPhone = 0;
  let withState = 0;
  let contactable = 0;
  let bothContactFields = 0;
  let guestChatLike = 0;
  let inferredFromOpportunity = 0;
  let lastLeadMs = 0;

  for (const r of rows) {
    if (r.email) withEmail++;
    if (r.phone) withPhone++;
    if (r.state) withState++;
    if (r.email || r.phone) contactable++;
    if (r.email && r.phone) bothContactFields++;
    if (r.leadType === "guest_chat_like") guestChatLike++;
    if (r.stateFrom === "opportunity.source") inferredFromOpportunity++;

    const ms = Number(r.__createdMs || 0);
    if (Number.isFinite(ms) && ms > lastLeadMs) lastLeadMs = ms;
  }

  const pct = (n: number, d: number) => (d ? Math.round((n / d) * 100) : 0);

  return {
    total,
    withEmail,
    withPhone,
    withState,
    contactable,
    contactableRate: pct(contactable, total),
    bothContactFields,
    bothContactRate: pct(bothContactFields, total),
    guestChatLike,
    guestRate: pct(guestChatLike, total),
    inferredFromOpportunity,
    inferredRate: pct(inferredFromOpportunity, total),
    lastLeadIso: lastLeadMs ? new Date(lastLeadMs).toISOString() : "",
  };
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

  const maxY = Math.max(...points.map((p) => p.leads), 1);
  const minY = 0;

  const plotW = w - padL - padR;
  const plotH = h - padT - padB;

  const xFor = (i: number) => padL + (plotW * i) / Math.max(1, points.length - 1);
  const yFor = (v: number) => padT + plotH * (1 - (v - minY) / (maxY - minY || 1));

  const d = points
    .map((p, i) => {
      const x = xFor(i);
      const y = yFor(p.leads);
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
              <line x1={padL} x2={w - padR} y1={y} y2={y} className="chartGrid" />
              <text x={padL - 10} y={y + 4} textAnchor="end" className="chartAxis">
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
            <text x={w - padR} y={h - 10} textAnchor="end" className="chartAxis">
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
          const y = yFor(p.leads);
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

export default function ContactsDashboardPage() {
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const [preset, setPreset] = useState<RangePreset>("7d");
  const [customStart, setCustomStart] = useState("");
  const [customEnd, setCustomEnd] = useState("");

  const [grain, setGrain] = useState<TrendGrain>("day");

  const [data, setData] = useState<ContactsApiResponse | null>(null);
  const [prevData, setPrevData] = useState<ContactsApiResponse | null>(null);

  const [callsData, setCallsData] = useState<CallsApiResponse | null>(null);
  const [prevCallsData, setPrevCallsData] = useState<CallsApiResponse | null>(null);

  const [mapSelected, setMapSelected] = useState<string>("");
  const [hoverPoint, setHoverPoint] = useState<TrendPoint | null>(null);

  const [aiLoading, setAiLoading] = useState(false);
  const [aiErr, setAiErr] = useState("");
  const [aiInsights, setAiInsights] = useState<AiInsights | null>(null);

  const loadSeqRef = useRef(0);

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
      const start = startD ? isoStartOfDay(startD) : "";
      const end2 = endD ? isoEndOfDay(endD) : "";
      return { start, end: end2 };
    }

    return { start: "", end: "" };
  }, [preset, customStart, customEnd]);

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

  async function fetchContacts(start: string, end: string, force = false) {
    const qs =
      start && end
        ? `?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}${
            force ? "&bust=1" : ""
          }`
        : "";

    const res = await fetch(`/api/dashboard/contacts${qs}`, {
      cache: "no-store",
    });

    const ct = res.headers.get("content-type") || "";
    if (!ct.includes("application/json")) {
      const txt = await res.text();
      throw new Error(`Contacts API non-JSON (${ct}): ${txt.slice(0, 120)}`);
    }

    const json = (await res.json()) as ContactsApiResponse;
    if (!res.ok || !json?.ok) {
      throw new Error(json?.error || `HTTP ${res.status}`);
    }
    return json;
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
      throw new Error(`Calls API non-JSON (${ct}): ${txt.slice(0, 120)}`);
    }

    const json = (await res.json()) as CallsApiResponse;
    if (!res.ok || !json?.ok) {
      throw new Error(json?.error || `HTTP ${res.status}`);
    }
    return json;
  }

  async function load(force = false) {
    const seq = ++loadSeqRef.current;
    setErr("");
    setLoading(true);
    setAiInsights(null);
    setAiErr("");

    try {
      if (!computedRange.start || !computedRange.end) {
        throw new Error("Missing start/end range");
      }

      const [currentContacts, currentCalls] = await Promise.all([
        fetchContacts(computedRange.start, computedRange.end, force),
        fetchCalls(computedRange.start, computedRange.end),
      ]);

      if (seq !== loadSeqRef.current) return;

      setData(currentContacts);
      setCallsData(currentCalls);

      const { prevStart, prevEnd } = slicePrevPeriod(
        computedRange.start,
        computedRange.end,
      );

      if (prevStart && prevEnd) {
        const [prevContacts, prevCalls] = await Promise.all([
          fetchContacts(prevStart, prevEnd, false),
          fetchCalls(prevStart, prevEnd),
        ]);

        if (seq !== loadSeqRef.current) return;

        setPrevData(prevContacts);
        setPrevCallsData(prevCalls);
      } else {
        setPrevData(null);
        setPrevCallsData(null);
      }
    } catch (e: unknown) {
      if (seq !== loadSeqRef.current) return;
      setData(null);
      setPrevData(null);
      setCallsData(null);
      setPrevCallsData(null);
      setErr(e instanceof Error ? e.message : "Failed to load Contacts Dashboard");
    } finally {
      if (seq === loadSeqRef.current) setLoading(false);
    }
  }

  useEffect(() => {
    if (preset !== "custom") load(false);
    else if (customStart && customEnd) load(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [preset, customStart, customEnd]);

  const filteredRows = useMemo(() => {
    const rows = data?.rows || [];
    if (!mapSelected) return rows;
    return rows.filter((r) => norm(r.state) === mapSelected);
  }, [data, mapSelected]);

  const prevFilteredRows = useMemo(() => {
    const rows = prevData?.rows || [];
    if (!mapSelected) return rows;
    return rows.filter((r) => norm(r.state) === mapSelected);
  }, [prevData, mapSelected]);

  const kpis = useMemo(() => computeLeadKpis(filteredRows), [filteredRows]);
  const prevKpis = useMemo(
    () => (prevData ? computeLeadKpis(prevFilteredRows) : null),
    [prevData, prevFilteredRows],
  );

  const mapRows = useMemo(() => {
    const byState = data?.byState || {};
    const entries = Object.entries(byState);
    if (!entries.length) return [];

    const max = Math.max(...entries.map(([, v]) => Number(v) || 0), 1);

    return entries.map(([stateName, leads]) => ({
      state: stateName,
      counties: { total: max, ready: Number(leads) || 0, domainsActive: 0 },
      cities: { total: 0, ready: 0, domainsActive: 0 },
      __value: Number(leads) || 0,
    }));
  }, [data]);

  const selectedLeads = useMemo(() => {
    if (!mapSelected) return Number(data?.total || 0);
    return Number(data?.byState?.[mapSelected] || 0);
  }, [mapSelected, data]);

  const trendPoints = useMemo(
    () => buildTrend(filteredRows, grain),
    [filteredRows, grain],
  );

  const trendSummary = useMemo(() => {
    const total = filteredRows.length;
    const points = trendPoints.length;
    const max = Math.max(...trendPoints.map((p) => p.leads), 0);
    const avg = points ? total / points : 0;
    return { total, points, max, avg };
  }, [trendPoints, filteredRows]);

  const leadsDeltaPct = useMemo(
    () => (prevKpis ? percentChange(kpis.total, prevKpis.total) : null),
    [kpis, prevKpis],
  );

  const leadsToCalls = useMemo(() => {
    const callsNow = mapSelected
      ? Number(callsData?.byState?.[mapSelected] || 0)
      : Number(callsData?.total || 0);

    if (!callsNow) return null;
    return kpis.total / callsNow;
  }, [kpis.total, callsData, mapSelected]);

  const callsNow = useMemo(
    () =>
      mapSelected
        ? Number(callsData?.byState?.[mapSelected] || 0)
        : Number(callsData?.total || 0),
    [callsData, mapSelected],
  );

  const callsPrev = useMemo(
    () =>
      mapSelected
        ? Number(prevCallsData?.byState?.[mapSelected] || 0)
        : Number(prevCallsData?.total || 0),
    [prevCallsData, mapSelected],
  );

  const callsDeltaPct = useMemo(() => {
    if (!prevCallsData) return null;
    return percentChange(callsNow, callsPrev);
  }, [callsNow, callsPrev, prevCallsData]);

  const leadsToCallsPrev = useMemo(() => {
    if (!prevKpis || !prevCallsData || !callsPrev) return null;
    return prevKpis.total / callsPrev;
  }, [prevKpis, prevCallsData, callsPrev]);

  const leadsToCallsDeltaPct = useMemo(() => {
    if (leadsToCalls === null || leadsToCallsPrev === null) return null;
    return percentChange(leadsToCalls, leadsToCallsPrev);
  }, [leadsToCalls, leadsToCallsPrev]);

  const topStates = useMemo(() => {
    return Object.entries(data?.byState || {})
      .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))
      .slice(0, 10)
      .map(([state, leads]) => ({ state, leads: Number(leads) || 0 }));
  }, [data]);

  const stateCoverageGap = Math.max(0, 100 - kpis.contactableRate);

  async function generateInsights() {
    setAiErr("");
    setAiLoading(true);
    setAiInsights(null);

    try {
      const payload = {
        range: {
          start: computedRange.start,
          end: computedRange.end,
          label: rangeLabel,
        },
        scope: {
          state: mapSelected || null,
        },
        leads_kpis: kpis,
        leads_deltas: {
          leadsPct: leadsDeltaPct,
        },
        leads_vs_calls: {
          callsNow,
          callsPrev,
          callsDeltaPct,
          leadsToCalls,
          leadsToCallsDeltaPct,
        },
        trend: {
          grain,
          points: trendPoints,
          summary: trendSummary,
        },
        top_states: topStates,
        quality_flags: {
          stateCoverageGap,
          guestRate: kpis.guestRate,
          inferredRate: kpis.inferredRate,
        },
      };

      const res = await fetch("/api/dashboard/contacts/insights", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const ct = res.headers.get("content-type") || "";
      if (!ct.includes("application/json")) {
        const txt = await res.text();
        throw new Error(`Insights API non-JSON: ${txt.slice(0, 120)}`);
      }

      const json = (await res.json()) as {
        ok?: boolean;
        error?: string;
        insights?: AiInsights;
      };
      if (!res.ok || !json?.ok) {
        throw new Error(json?.error || "Failed to generate AI insights");
      }

      setAiInsights(json.insights || null);
    } catch (e: unknown) {
      setAiErr(e instanceof Error ? e.message : "Failed to generate AI insights");
    } finally {
      setAiLoading(false);
    }
  }

  return (
    <div className="shell callsDash contactsDash">
      {loading && (
        <div className="dashLoadingOverlay" aria-live="polite" aria-busy="true">
          <div className="dashLoadingCard">
            <div className="dashSpinner" />
            <div className="dashLoadingText">Updating Leads Dashboard...</div>
            <div className="mini" style={{ marginTop: 6 }}>
              Applying filters and recalculating KPIs, map and chart.
            </div>
          </div>
        </div>
      )}

      <header className="topbar">
        <div className="brand">
          <div className="logo" />
          <div>
            <h1>My Drip Nurse - Contacts (Leads) Dashboard</h1>
          </div>
        </div>

        <div className="pills">
          <Link className="pill" href="/dashboard" style={{ textDecoration: "none" }}>
            ← Back
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
              El rango afecta KPIs, comparativas, trend, mapa y tabla.
            </div>
          </div>

          <div className="cardHeaderActions" style={{ gap: 10 }}>
            <button
              className="smallBtn"
              onClick={() => load(true)}
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
                  onClick={() => setPreset("1d")}
                  type="button"
                  disabled={loading}
                >
                  1 day
                </button>
                <button
                  className={`smallBtn ${preset === "7d" ? "smallBtnOn" : ""}`}
                  onClick={() => setPreset("7d")}
                  type="button"
                  disabled={loading}
                >
                  7 days
                </button>
                <button
                  className={`smallBtn ${preset === "28d" ? "smallBtnOn" : ""}`}
                  onClick={() => setPreset("28d")}
                  type="button"
                  disabled={loading}
                >
                  28 days
                </button>

                <span className="filtersDivider" />

                <button
                  className={`smallBtn ${preset === "1m" ? "smallBtnOn" : ""}`}
                  onClick={() => setPreset("1m")}
                  type="button"
                  disabled={loading}
                >
                  Last month
                </button>
                <button
                  className={`smallBtn ${preset === "3m" ? "smallBtnOn" : ""}`}
                  onClick={() => setPreset("3m")}
                  type="button"
                  disabled={loading}
                >
                  Last quarter
                </button>
                <button
                  className={`smallBtn ${preset === "6m" ? "smallBtnOn" : ""}`}
                  onClick={() => setPreset("6m")}
                  type="button"
                  disabled={loading}
                >
                  Last 6 months
                </button>
                <button
                  className={`smallBtn ${preset === "1y" ? "smallBtnOn" : ""}`}
                  onClick={() => setPreset("1y")}
                  type="button"
                  disabled={loading}
                >
                  Last year
                </button>

                <span className="filtersDivider" />

                <button
                  className={`smallBtn ${preset === "custom" ? "smallBtnOn" : ""}`}
                  onClick={() => setPreset("custom")}
                  type="button"
                  disabled={loading}
                >
                  Custom
                </button>
              </div>
            </div>

            <div className="filtersGroup dateGroup">
              <div className="filtersLabel">Custom dates</div>

              <div className="dateInputs">
                <div className="dateField">
                  <label className="mini" style={{ marginBottom: 6, display: "block" }}>
                    Start
                  </label>
                  <input
                    className="input"
                    type="date"
                    value={customStart}
                    onChange={(e) => setCustomStart(e.target.value)}
                    disabled={preset !== "custom" || loading}
                  />
                </div>

                <div className="dateField">
                  <label className="mini" style={{ marginBottom: 6, display: "block" }}>
                    End
                  </label>
                  <input
                    className="input"
                    type="date"
                    value={customEnd}
                    onChange={(e) => setCustomEnd(e.target.value)}
                    disabled={preset !== "custom" || loading}
                  />
                </div>

                {preset === "custom" && (
                  <button
                    className="btn btnPrimary applyBtn"
                    onClick={() => load(false)}
                    disabled={!customStart || !customEnd || loading}
                    type="button"
                  >
                    {loading ? "Applying..." : "Apply"}
                  </button>
                )}
              </div>
            </div>
          </div>

          {err ? (
            <div className="mini" style={{ color: "var(--danger)", marginTop: 10 }}>
              X {err}
            </div>
          ) : null}
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Lead KPIs ({mapSelected || "All states"})</h2>
            <div className="cardSubtitle">
              KPIs de generación de demanda, calidad de lead y completitud para el scope actual.
            </div>
          </div>
          <div className="badge">{loading ? "loading..." : "ready"}</div>
        </div>

        <div className="cardBody">
          <div className="kpiRow kpiRowWide">
            <div className="kpi">
              <p className="n">{kpis.total}</p>
              <p className="l">Total leads</p>
            </div>
            <div className="kpi">
              <p className="n">{kpis.contactable}</p>
              <p className="l">Contactable (email or phone)</p>
            </div>
            <div className="kpi">
              <p className="n">{kpis.contactableRate}%</p>
              <p className="l">Contactability rate</p>
            </div>
            <div className="kpi">
              <p className="n">{kpis.bothContactRate}%</p>
              <p className="l">Full contact profile</p>
            </div>
            <div className="kpi">
              <p className="n">{kpis.withState}</p>
              <p className="l">Leads with state</p>
            </div>
            <div className="kpi">
              <p className="n">{kpis.guestRate}%</p>
              <p className="l">Guest/chat-like rate</p>
            </div>
            <div className="kpi">
              <p className="n">{kpis.inferredFromOpportunity}</p>
              <p className="l">State inferred by opportunity</p>
            </div>
            <div className="kpi">
              <p className="n">{fmtDelta(leadsDeltaPct)}</p>
              <p className="l">Lead volume vs prev period</p>
            </div>
            <div className="kpi">
              <p className="n">{fmtDateLocal(kpis.lastLeadIso)}</p>
              <p className="l">Last lead</p>
            </div>
          </div>
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Leads vs Calls (same range/scope)</h2>
            <div className="cardSubtitle">
              Comparativa operativa para validar si el volumen de leads se transforma en llamadas.
            </div>
          </div>
        </div>

        <div className="cardBody">
          <div className="kpiRow kpiRowWide">
            <div className="kpi">
              <p className="n">{kpis.total}</p>
              <p className="l">Leads</p>
            </div>
            <div className="kpi">
              <p className="n">{callsNow}</p>
              <p className="l">Calls</p>
            </div>
            <div className="kpi">
              <p className="n">{leadsToCalls === null ? "-" : leadsToCalls.toFixed(2)}</p>
              <p className="l">Leads per call</p>
            </div>
            <div className="kpi">
              <p className="n">{fmtDelta(callsDeltaPct)}</p>
              <p className="l">Call volume vs prev period</p>
            </div>
            <div className="kpi">
              <p className="n">{fmtDelta(leadsToCallsDeltaPct)}</p>
              <p className="l">Leads/Call efficiency delta</p>
            </div>
          </div>
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Leads Trend and Map</h2>
            <div className="cardSubtitle">
              Click en estado para filtrar KPIs, chart, AI insights y tabla.
            </div>
          </div>

          <div className="cardHeaderActions">
            <div className="segmented" role="tablist" aria-label="Trend grain">
              {(["day", "week", "month"] as TrendGrain[]).map((g) => (
                <button
                  key={g}
                  className={`segBtn ${grain === g ? "segBtnOn" : ""}`}
                  onClick={() => setGrain(g)}
                  type="button"
                  disabled={loading}
                >
                  {g === "day" ? "Day" : g === "week" ? "Week" : "Month"}
                </button>
              ))}
            </div>

            <button
              className="smallBtn"
              onClick={() => setMapSelected("")}
              type="button"
              disabled={loading}
            >
              Clear state
            </button>
          </div>
        </div>

        <div className="cardBody">
          {!data?.rows?.length ? (
            <div className="mini">{loading ? "Loading..." : "No leads in this range."}</div>
          ) : (
            <div className="callsPlatform">
              <div className="trendCard" style={{ marginBottom: 12 }}>
                <div className="trendHeader">
                  <div>
                    <div className="trendTitle">Lead volume trend ({rangeLabel})</div>
                    <div className="mini" style={{ marginTop: 4 }}>
                      {mapSelected
                        ? `Filtered to ${mapSelected}`
                        : "All states"}
                    </div>
                  </div>
                  {hoverPoint ? (
                    <div className="mini">
                      <b>{hoverPoint.label}</b>: {hoverPoint.leads} leads
                    </div>
                  ) : (
                    <div className="mini">Hover a point</div>
                  )}
                </div>

                {!!trendPoints.length ? (
                  <LineTrend points={trendPoints} onHover={setHoverPoint} />
                ) : (
                  <div className="mini">No data for chart.</div>
                )}

                <div className="trendMeta">
                  <div className="miniCard">
                    <div className="miniCardLabel">Total leads</div>
                    <div className="miniCardValue">{trendSummary.total}</div>
                  </div>
                  <div className="miniCard">
                    <div className="miniCardLabel">Peak bucket</div>
                    <div className="miniCardValue">{trendSummary.max}</div>
                  </div>
                  <div className="miniCard">
                    <div className="miniCardLabel">Avg per bucket</div>
                    <div className="miniCardValue">{trendSummary.avg.toFixed(1)}</div>
                  </div>
                </div>
              </div>

              <div className="mapGrid">
                <div className="mapCard">
                  <div className="mapCardTop">
                    <div>
                      <div className="mapCardTitle">US Leads Map</div>
                      <div className="mini" style={{ marginTop: 6 }}>
                        Lead count by state in selected range.
                      </div>
                    </div>
                  </div>

                  <div className="mapFrame mapFrameXL">
                    <UsaChoroplethProgressMap
                      rows={mapRows as MapMetricRow[]}
                      metric={"calls"}
                      labelMode={"value"}
                      valueField={"__value"}
                      selectedState={mapSelected}
                      onPick={(name) => setMapSelected(norm(name))}
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
                        <div className="statePill">{selectedLeads} leads</div>
                      </div>
                    ) : (
                      <div className="stateHead">
                        <div className="stateName">All states</div>
                        <div className="statePill">{selectedLeads} leads</div>
                      </div>
                    )}
                  </div>

                  <div className="stateCards">
                    <div className="stateKpi">
                      <div className="mini">Contactability</div>
                      <div className="stateKpiN">{kpis.contactableRate}%</div>
                      <div className="mini" style={{ opacity: 0.85 }}>
                        Leads with email or phone
                      </div>
                    </div>

                    <div className="stateKpi">
                      <div className="mini">Full profile</div>
                      <div className="stateKpiN">{kpis.bothContactRate}%</div>
                      <div className="mini" style={{ opacity: 0.85 }}>
                        Leads with both email and phone
                      </div>
                    </div>

                    <div className="stateKpi">
                      <div className="mini">State inferred</div>
                      <div className="stateKpiN">{kpis.inferredRate}%</div>
                      <div className="mini" style={{ opacity: 0.85 }}>
                        Opportunity.source rescue rate
                      </div>
                    </div>

                    <div className="stateKpi">
                      <div className="mini">Guest/chat-like</div>
                      <div className="stateKpiN">{kpis.guestRate}%</div>
                      <div className="mini" style={{ opacity: 0.85 }}>
                        Intent with low profile data
                      </div>
                    </div>
                  </div>

                  <div className="aiCard">
                    <div className="aiCardTop">
                      <div>
                        <div className="aiTitle">AI Strategist</div>
                        <div className="mini" style={{ opacity: 0.85, marginTop: 4 }}>
                          Lead-gen recommendations based on quality, trend and calls comparison.
                        </div>
                      </div>

                      <button
                        className="smallBtn aiBtn"
                        onClick={generateInsights}
                        disabled={aiLoading || loading || !data?.rows?.length}
                        type="button"
                      >
                        {aiLoading ? "Generating..." : "Generate insights"}
                      </button>
                    </div>

                    {aiErr ? (
                      <div className="mini" style={{ color: "var(--danger)", marginTop: 10 }}>
                        X {aiErr}
                      </div>
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

                        {!!aiInsights.opportunities?.length && (
                          <div className="aiBlock">
                            <div className="aiBlockTitle">Top opportunities</div>
                            <div className="aiOps">
                              {aiInsights.opportunities.slice(0, 3).map((o: AiOpportunity, idx: number) => (
                                <div className="aiOp" key={idx}>
                                  <div className="aiOpHead">
                                    <div className="aiOpTitle">{o.title}</div>
                                    <span className={`aiImpact ${o.expected_impact}`}>
                                      {String(o.expected_impact || "medium").toUpperCase()}
                                    </span>
                                  </div>
                                  <div className="mini" style={{ opacity: 0.9, marginTop: 6 }}>
                                    <b>Why:</b> {o.why_it_matters}
                                  </div>
                                  <div className="mini" style={{ opacity: 0.85, marginTop: 6 }}>
                                    <b>Evidence:</b> {o.evidence}
                                  </div>
                                  {Array.isArray(o.recommended_actions) && o.recommended_actions.length ? (
                                    <ul className="aiList">
                                      {o.recommended_actions.slice(0, 4).map((a: string, i: number) => (
                                        <li key={i}>{a}</li>
                                      ))}
                                    </ul>
                                  ) : null}
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    ) : (
                      <div className="aiPlaceholder mini">
                        Generate insights to get a strategic read and action plan for this scope.
                      </div>
                    )}
                  </div>

                  <div style={{ marginTop: 12 }}>
                    <AiAgentChatPanel
                      agent="leads"
                      title="Leads Agent Chat"
                      context={{
                        preset,
                        customStart,
                        customEnd,
                        trendGrain: grain,
                        selectedState: mapSelected || null,
                        totalLeads: filteredRows.length,
                        kpis,
                        callsComparison: {
                          callsNow,
                          callsPrev,
                          leadsToCalls,
                          leadsToCallsPrev,
                          leadsToCallsDelta,
                        },
                      }}
                    />
                  </div>
                </aside>
              </div>
            </div>
          )}
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Leads table</h2>
            <div className="cardSubtitle">Detail table for current scope.</div>
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
                    <th className="th">Date Added</th>
                    <th className="th">Name</th>
                    <th className="th">Phone</th>
                    <th className="th">Email</th>
                    <th className="th">State</th>
                    <th className="th">State From</th>
                    <th className="th">Lead Type</th>
                    <th className="th">City</th>
                    <th className="th">Source</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows
                    .slice()
                    .sort((a, b) => Number(b.__createdMs || 0) - Number(a.__createdMs || 0))
                    .map((r) => (
                      <tr key={r.id} className="tr">
                        <td className="td">
                          <span className="mini">{fmtDateLocal(r.dateAdded)}</span>
                        </td>
                        <td className="td">{r.name || "-"}</td>
                        <td className="td">{r.phone || "-"}</td>
                        <td className="td">{r.email || "-"}</td>
                        <td className="td">{r.state || "-"}</td>
                        <td className="td">
                          <span className="mini">{r.stateFrom}</span>
                        </td>
                        <td className="td">
                          <span className="mini">{r.leadType}</span>
                        </td>
                        <td className="td">{r.city || "-"}</td>
                        <td className="td">{r.source || "-"}</td>
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
