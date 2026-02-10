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

type ConvRow = {
  id: string;
  contactId: string;
  contactName: string;
  channel: string;
  direction: "inbound" | "outbound" | "unknown";
  unreadCount: number;
  messageCount: number;
  snippet: string;
  lastMessageAt: string;
  __lastMs: number | null;
  state: string;
  city: string;
  stateFrom: "conversation" | "contact.state" | "opportunity.source" | "unknown";
};

type ConversationsApiResponse = {
  ok: boolean;
  range?: { start: string; end: string };
  total?: number;
  kpis?: {
    total: number;
    uniqueContacts: number;
    unreadConversations: number;
    avgMessagesPerConversation: number;
    withState: number;
    stateRate: number;
    inferredFromContact: number;
  };
  byState?: Record<string, number>;
  byChannel?: Record<string, number>;
  rows?: ConvRow[];
  cache?: {
    source?: "memory" | "snapshot" | "ghl_refresh";
    snapshotUpdatedAt?: string;
    snapshotCoverage?: { newestMessageAt: string; oldestMessageAt: string };
    fetchedPages?: number;
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

function buildTrend(rows: ConvRow[], grain: TrendGrain) {
  const m = new Map<string, number>();
  for (const r of rows) {
    const ms = Number(r.__lastMs ?? NaN);
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
          <linearGradient id="convLineGrad" x1="0" x2="1" y1="0" y2="0">
            <stop offset="0%" stopColor="rgba(96,165,250,0.95)" />
            <stop offset="100%" stopColor="rgba(52,211,153,0.95)" />
          </linearGradient>
          <linearGradient id="convAreaGrad" x1="0" x2="0" y1="0" y2="1">
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

        {areaD ? <path d={areaD} fill="url(#convAreaGrad)" /> : null}
        {d ? <path d={d} fill="none" stroke="url(#convLineGrad)" strokeWidth="3" strokeLinecap="round" /> : null}

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

export default function ConversationsDashboardPage() {
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");

  const [preset, setPreset] = useState<RangePreset>("7d");
  const [customStart, setCustomStart] = useState("");
  const [customEnd, setCustomEnd] = useState("");
  const [grain, setGrain] = useState<TrendGrain>("day");

  const [data, setData] = useState<ConversationsApiResponse | null>(null);
  const [prevData, setPrevData] = useState<ConversationsApiResponse | null>(null);
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

      const currRes = await fetch(`/api/dashboard/conversations?${qs.toString()}`, { cache: "no-store" });
      const curr = (await currRes.json()) as ConversationsApiResponse;
      if (!currRes.ok || !curr?.ok) {
        throw new Error(curr?.error || `HTTP ${currRes.status}`);
      }
      setData(curr);

      const prev = slicePrevPeriod(computedRange.start, computedRange.end);
      if (prev.prevStart && prev.prevEnd) {
        const pQs = new URLSearchParams();
        pQs.set("start", prev.prevStart);
        pQs.set("end", prev.prevEnd);
        if (force) pQs.set("bust", "1");
        const prevRes = await fetch(`/api/dashboard/conversations?${pQs.toString()}`, { cache: "no-store" });
        const prevJson = (await prevRes.json()) as ConversationsApiResponse;
        if (prevRes.ok && prevJson?.ok) setPrevData(prevJson);
        else setPrevData(null);
      } else {
        setPrevData(null);
      }
    } catch (e: unknown) {
      setData(null);
      setPrevData(null);
      setErr(e instanceof Error ? e.message : "Failed to load conversations dashboard");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (preset !== "custom") load(false);
    else if (customStart && customEnd) load(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [preset, customStart, customEnd]);

  const rows = useMemo(() => (data?.rows || []) as ConvRow[], [data]);
  const filteredRows = useMemo(() => {
    if (!mapSelected) return rows;
    return rows.filter((r) => norm(r.state) === mapSelected);
  }, [rows, mapSelected]);

  const prevRows = useMemo(() => (prevData?.rows || []) as ConvRow[], [prevData]);
  const prevFilteredRows = useMemo(() => {
    if (!mapSelected) return prevRows;
    return prevRows.filter((r) => norm(r.state) === mapSelected);
  }, [prevRows, mapSelected]);

  const kpis = useMemo(() => {
    const total = filteredRows.length;
    const contacts = new Set(filteredRows.map((r) => r.contactId).filter(Boolean)).size;
    const unread = filteredRows.filter((r) => Number(r.unreadCount || 0) > 0).length;
    const messageSum = filteredRows.reduce((a, r) => a + Number(r.messageCount || 0), 0);
    const withState = filteredRows.filter((r) => !!norm(r.state)).length;
    const inferred = filteredRows.filter((r) => r.stateFrom !== "conversation" && !!r.state).length;
    return {
      total,
      uniqueContacts: contacts,
      unreadConversations: unread,
      avgMessages: total ? Number((messageSum / total).toFixed(2)) : 0,
      stateRate: total ? Math.round((withState / total) * 100) : 0,
      inferred,
    };
  }, [filteredRows]);

  const prevKpis = useMemo(() => {
    const total = prevFilteredRows.length;
    const contacts = new Set(prevFilteredRows.map((r) => r.contactId).filter(Boolean)).size;
    const unread = prevFilteredRows.filter((r) => Number(r.unreadCount || 0) > 0).length;
    const messageSum = prevFilteredRows.reduce((a, r) => a + Number(r.messageCount || 0), 0);
    const withState = prevFilteredRows.filter((r) => !!norm(r.state)).length;
    return {
      total,
      uniqueContacts: contacts,
      unreadConversations: unread,
      avgMessages: total ? Number((messageSum / total).toFixed(2)) : 0,
      stateRate: total ? Math.round((withState / total) * 100) : 0,
    };
  }, [prevFilteredRows]);

  const totalDelta = useMemo(() => (prevKpis ? pctDelta(kpis.total, prevKpis.total) : null), [kpis.total, prevKpis]);
  const contactDelta = useMemo(
    () => (prevKpis ? pctDelta(kpis.uniqueContacts, prevKpis.uniqueContacts) : null),
    [kpis.uniqueContacts, prevKpis],
  );
  const unreadDelta = useMemo(
    () => (prevKpis ? pctDelta(kpis.unreadConversations, prevKpis.unreadConversations) : null),
    [kpis.unreadConversations, prevKpis],
  );
  const avgMsgDelta = useMemo(
    () => (prevKpis ? pctDelta(kpis.avgMessages, prevKpis.avgMessages) : null),
    [kpis.avgMessages, prevKpis],
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
      if (!st) {
        m.__unknown = (m.__unknown || 0) + 1;
      } else {
        m[st] = (m[st] || 0) + 1;
      }
    }
    return m;
  }, [filteredRows]);

  const byChannel = useMemo(() => {
    const m: Record<string, number> = {};
    for (const r of filteredRows) {
      const ch = norm(r.channel || "unknown").toLowerCase() || "unknown";
      m[ch] = (m[ch] || 0) + 1;
    }
    return m;
  }, [filteredRows]);

  const channelsSorted = useMemo(
    () => Object.entries(byChannel).sort((a, b) => b[1] - a[1]),
    [byChannel],
  );

  const mapRows = useMemo(() => {
    const allByState = (data?.byState || {}) as Record<string, number>;
    return Object.entries(allByState)
      .filter(([state]) => state !== "__unknown")
      .map(([state, val]) => ({
      state,
      counties: { total: 1, ready: Number(val || 0), domainsActive: 0 },
      cities: { total: 0, ready: 0, domainsActive: 0 },
      __value: Number(val || 0),
    }));
  }, [data]);

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
          unreadDelta,
          avgMsgDelta,
          stateRateDelta,
        },
        byChannel,
        byState,
        trend: trend.slice(-30),
        rowsPreview: filteredRows.slice(0, 120).map((r) => ({
          id: r.id,
          contactName: r.contactName,
          channel: r.channel,
          direction: r.direction,
          unreadCount: r.unreadCount,
          messageCount: r.messageCount,
          state: r.state,
          city: r.city,
          stateFrom: r.stateFrom,
          lastMessageAt: r.lastMessageAt,
        })),
      };

      const res = await fetch("/api/dashboard/conversations/insights", {
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
            <div className="dashLoadingText">Updating Conversations Dashboard...</div>
            <div className="mini" style={{ marginTop: 6 }}>
              Syncing channels, CRM mappings and geo distribution.
            </div>
          </div>
        </div>
      ) : null}

      <header className="topbar">
        <div className="brand">
          <div className="logo" />
          <div>
            <h1>My Drip Nurse — Conversations Dashboard</h1>
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
              Rango afecta KPIs, mapa, canales, AI insights y tabla.
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
              {data.cache.snapshotCoverage?.newestMessageAt
                ? ` • newest msg ${fmtDateLocal(data.cache.snapshotCoverage.newestMessageAt)}`
                : ""}
              {data.cache.snapshotCoverage?.oldestMessageAt
                ? ` • oldest msg ${fmtDateLocal(data.cache.snapshotCoverage.oldestMessageAt)}`
                : ""}
              {Number(data.cache.fetchedPages || 0) > 0 ? ` • pages fetched: ${data.cache.fetchedPages}` : ""}
              {data.cache.usedIncremental ? " • incremental refresh" : ""}
            </div>
          ) : null}
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Conversation KPIs ({mapSelected || "All states"})</h2>
            <div className="cardSubtitle">
              Conversaciones por canal, calidad operativa CRM y cobertura por estado.
            </div>
          </div>
          <div className="badge">{kpis.total} conversations</div>
        </div>
        <div className="cardBody">
          <div className="kpiRow kpiRowWide">
            <div className="kpi">
              <p className="n">{fmtInt(kpis.total)}</p>
              <p className="l">Total conversations</p>
              <div className={`mini ${deltaClass(totalDelta)}`}>{fmtDelta(totalDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(kpis.uniqueContacts)}</p>
              <p className="l">Unique contacts</p>
              <div className={`mini ${deltaClass(contactDelta)}`}>{fmtDelta(contactDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(kpis.unreadConversations)}</p>
              <p className="l">Unread conversations</p>
              <div className={`mini ${deltaClass(unreadDelta)}`}>{fmtDelta(unreadDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{kpis.avgMessages.toFixed(2)}</p>
              <p className="l">Avg messages / conv</p>
              <div className={`mini ${deltaClass(avgMsgDelta)}`}>{fmtDelta(avgMsgDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{kpis.stateRate}%</p>
              <p className="l">Mapped state coverage</p>
              <div className={`mini ${deltaClass(stateRateDelta)}`}>{fmtDelta(stateRateDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(kpis.inferred)}</p>
              <p className="l">State inferred (CRM)</p>
              <div className="mini">From contact/opportunity</div>
            </div>
          </div>
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Conversation trend</h2>
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
                    {hoverPoint.label} • <b>{hoverPoint.value}</b> conversations
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
        <div className="cardBody">
          <div className="mapGrid">
            <div className="mapCard">
              <div className="mapCardTop">
                <div>
                  <div className="mapCardTitle">US Conversations Map</div>
                  <div className="mini" style={{ marginTop: 6 }}>
                    Click en un estado para filtrar KPIs, chart, AI insights y tabla.
                  </div>
                </div>
              </div>
              <div className="mapFrame mapFrameXL">
                <UsaChoroplethProgressMap
                  rows={mapRows as any}
                  metric={"calls" as any}
                  labelMode={"value" as any}
                  valueField={"__value" as any}
                  selectedState={mapSelected}
                  onPick={(name) => setMapSelected(norm(name))}
                />
              </div>
            </div>

            <aside className="statePanel">
              <div className="statePanelTop">
                <div className="mini" style={{ opacity: 0.85 }}>State analytics</div>
                {mapSelected ? (
                  <div className="stateHead">
                    <div className="stateName">{mapSelected}</div>
                    <div className="statePill">{fmtInt(byState[mapSelected] || 0)} conversations</div>
                  </div>
                ) : (
                  <div className="stateHead">
                    <div className="stateName">All states</div>
                    <div className="statePill">{fmtInt(kpis.total)} conversations</div>
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
                </div>

                {channelsSorted.slice(0, 6).map(([channel, count]) => {
                  const share = kpis.total ? Math.round((count / kpis.total) * 100) : 0;
                  return (
                    <div className="stateKpi" key={channel}>
                      <div className="mini">{channel.toUpperCase()}</div>
                      <div className="stateKpiN">{fmtInt(count)}</div>
                      <div className="mini" style={{ opacity: 0.85 }}>{share}% of conversations</div>
                    </div>
                  );
                })}
              </div>

              <div className="aiCard">
                <div className="aiCardTop">
                  <div>
                    <div className="aiTitle">AI Strategist (Conversation & CRM)</div>
                    <div className="mini" style={{ opacity: 0.85, marginTop: 4 }}>
                      Responde sobre canales, backlog unread, performance por estado y plan CRM.
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
                    {!!aiInsights.opportunities?.length && (
                      <div className="aiBlock">
                        <div className="aiBlockTitle">Top opportunities</div>
                        <div className="aiOps">
                          {aiInsights.opportunities.slice(0, 3).map((o, idx) => (
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
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="aiPlaceholder mini">
                    Generate insights to get CRM + conversation strategy for this scope.
                  </div>
                )}
              </div>

              <div style={{ marginTop: 12 }}>
                <AiAgentChatPanel
                  agent="conversations"
                  title="Conversations Agent Chat"
                  context={{
                    preset,
                    customStart,
                    customEnd,
                    trendGrain: grain,
                    selectedState: mapSelected || null,
                    kpis,
                    byChannel,
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
            <h2 className="cardTitle">Conversations table</h2>
            <div className="cardSubtitle">Detalle de conversaciones para el alcance actual.</div>
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
                    <th className="th">Last message</th>
                    <th className="th">Contact</th>
                    <th className="th">Channel</th>
                    <th className="th">Direction</th>
                    <th className="th">Unread</th>
                    <th className="th">Messages</th>
                    <th className="th">State</th>
                    <th className="th">State From</th>
                    <th className="th">City</th>
                    <th className="th">Snippet</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows
                    .slice()
                    .sort((a, b) => Number(b.__lastMs || 0) - Number(a.__lastMs || 0))
                    .map((r, idx) => (
                      <tr key={`${r.id || r.contactId || "row"}_${idx}`} className="tr">
                        <td className="td"><span className="mini">{fmtDateLocal(r.lastMessageAt)}</span></td>
                        <td className="td">{r.contactName || "-"}</td>
                        <td className="td">{r.channel || "-"}</td>
                        <td className="td">{r.direction || "-"}</td>
                        <td className="td">{fmtInt(r.unreadCount)}</td>
                        <td className="td">{fmtInt(r.messageCount)}</td>
                        <td className="td">{r.state || "-"}</td>
                        <td className="td"><span className="mini">{r.stateFrom}</span></td>
                        <td className="td">{r.city || "-"}</td>
                        <td className="td"><span className="mini">{r.snippet || "-"}</span></td>
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
            <h2 className="cardTitle">State breakdown (including unknown)</h2>
            <div className="cardSubtitle">Unknown se muestra por separado para no contaminar el mapa.</div>
          </div>
          <div className="badge">geo</div>
        </div>
        <div className="cardBody">
          <div className="tableWrap tableScrollX">
            <table className="table">
              <thead>
                <tr>
                  <th className="th">State</th>
                  <th className="th">Conversations</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(byState)
                  .sort((a, b) => Number(b[1]) - Number(a[1]))
                  .map(([state, count]) => (
                    <tr key={state} className="tr">
                      <td className="td">{state === "__unknown" ? "Unknown" : state}</td>
                      <td className="td">{fmtInt(count)}</td>
                    </tr>
                  ))}
                {!Object.keys(byState).length ? (
                  <tr className="tr">
                    <td className="td" colSpan={2} style={{ opacity: 0.75 }}>
                      No state rows in this scope.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </div>
      </section>
    </div>
  );
}
