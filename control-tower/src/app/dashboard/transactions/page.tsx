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
type TrendMetric = "count" | "amount";

type TxRow = {
  id: string;
  contactId: string;
  customerName: string;
  amount: number;
  currency: string;
  status: string;
  paymentMethod: string;
  source: string;
  createdAt: string;
  __createdMs: number | null;
  state: string;
  city: string;
  stateFrom: "transaction" | "contact.state" | "unknown";
  contactLifetimeNet?: number;
  contactLifetimeOrders?: number;
};

type TransactionsApiResponse = {
  ok: boolean;
  range?: { start: string; end: string };
  total?: number;
  kpis?: {
    totalTransactions: number;
    successfulTransactions: number;
    nonRevenueTransactions: number;
    grossAmount: number;
    avgTicket: number;
    refundedTransactions: number;
    refundedAmount: number;
    netAmount: number;
    withState: number;
    stateRate: number;
    inferredFromContact: number;
    uniqueCustomers: number;
    avgOrdersPerCustomer: number;
    repeatCustomerRate: number;
    avgLifetimeOrderValue: number;
  };
  byStateCount?: Record<string, number>;
  byStateAmount?: Record<string, number>;
  rows?: TxRow[];
  cache?: {
    source?: "memory" | "snapshot" | "ghl_refresh";
    snapshotUpdatedAt?: string;
    snapshotCoverage?: { newestCreatedAt: string; oldestCreatedAt: string };
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

function isRefundLike(statusRaw: string) {
  const s = norm(statusRaw).toLowerCase();
  return s.includes("refund") || s.includes("chargeback") || s.includes("reversal") || s.includes("reversed");
}

function isSucceededRevenueStatus(statusRaw: string) {
  const s = norm(statusRaw).toLowerCase();
  if (!s) return false;
  if (isRefundLike(s)) return false;
  if (s.includes("failed") || s.includes("declined") || s.includes("canceled") || s.includes("void")) return false;
  if (s.includes("pending") || s.includes("processing") || s.includes("in_progress")) return false;
  return (
    s.includes("succeeded") ||
    s.includes("success") ||
    s.includes("paid") ||
    s.includes("completed") ||
    s.includes("captured") ||
    s.includes("settled")
  );
}

function safeMethodLabel(raw: unknown) {
  const s = norm(raw).toLowerCase();
  if (!s || s.includes("[object object]")) return "unknown";
  return s;
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

function buildTrend(rows: TxRow[], grain: TrendGrain, metric: TrendMetric) {
  const m = new Map<string, number>();
  for (const r of rows) {
    const ms = Number(r.__createdMs ?? NaN);
    if (!Number.isFinite(ms)) continue;
    const k = keyForGrain(ms, grain);
    const add =
      metric === "amount"
        ? isSucceededRevenueStatus(r.status)
          ? Number(r.amount || 0)
          : 0
        : 1;
    m.set(k, Number((m.get(k) || 0) + add));
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
  metric,
  height = 220,
  onHover,
}: {
  points: TrendPoint[];
  metric: TrendMetric;
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
          <linearGradient id="txLineGrad" x1="0" x2="1" y1="0" y2="0">
            <stop offset="0%" stopColor="rgba(96,165,250,0.95)" />
            <stop offset="100%" stopColor="rgba(52,211,153,0.95)" />
          </linearGradient>
          <linearGradient id="txAreaGrad" x1="0" x2="0" y1="0" y2="1">
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

        {areaD ? <path d={areaD} fill="url(#txAreaGrad)" /> : null}
        {d ? <path d={d} fill="none" stroke="url(#txLineGrad)" strokeWidth="3" strokeLinecap="round" /> : null}

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
      <div className="mini" style={{ marginTop: 4 }}>
        Metric: {metric === "amount" ? "Revenue (USD)" : "Transactions count"}
      </div>
    </div>
  );
}

export default function TransactionsDashboardPage() {
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");

  const [preset, setPreset] = useState<RangePreset>("7d");
  const [customStart, setCustomStart] = useState("");
  const [customEnd, setCustomEnd] = useState("");
  const [grain, setGrain] = useState<TrendGrain>("day");
  const [trendMetric, setTrendMetric] = useState<TrendMetric>("count");
  const [mapMetric, setMapMetric] = useState<TrendMetric>("count");

  const [data, setData] = useState<TransactionsApiResponse | null>(null);
  const [prevData, setPrevData] = useState<TransactionsApiResponse | null>(null);
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

      const currRes = await fetch(`/api/dashboard/transactions?${qs.toString()}`, { cache: "no-store" });
      const curr = (await currRes.json()) as TransactionsApiResponse;
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
        const prevRes = await fetch(`/api/dashboard/transactions?${pQs.toString()}`, { cache: "no-store" });
        const prevJson = (await prevRes.json()) as TransactionsApiResponse;
        if (prevRes.ok && prevJson?.ok) setPrevData(prevJson);
        else setPrevData(null);
      } else {
        setPrevData(null);
      }
    } catch (e: unknown) {
      setData(null);
      setPrevData(null);
      setErr(e instanceof Error ? e.message : "Failed to load transactions dashboard");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (preset !== "custom") load(false);
    else if (customStart && customEnd) load(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [preset, customStart, customEnd]);

  const rows = useMemo(() => (data?.rows || []) as TxRow[], [data]);
  const filteredRows = useMemo(() => {
    if (!mapSelected) return rows;
    if (mapSelected === "__unknown") return rows.filter((r) => !norm(r.state));
    return rows.filter((r) => norm(r.state) === mapSelected);
  }, [rows, mapSelected]);

  const prevRows = useMemo(() => (prevData?.rows || []) as TxRow[], [prevData]);
  const prevFilteredRows = useMemo(() => {
    if (!mapSelected) return prevRows;
    if (mapSelected === "__unknown") return prevRows.filter((r) => !norm(r.state));
    return prevRows.filter((r) => norm(r.state) === mapSelected);
  }, [prevRows, mapSelected]);

  const kpis = useMemo(() => {
    const total = filteredRows.length;
    const contactMap = new Map<string, { tx: number; lifetimeNet: number; lifetimeOrders: number }>();
    const successfulRows = filteredRows.filter((r) => isSucceededRevenueStatus(r.status));
    const gross = successfulRows.reduce((a, r) => a + Number(r.amount || 0), 0);
    const refundedRows = filteredRows.filter((r) => {
      return isRefundLike(r.status);
    });
    const refundedAmount = refundedRows.reduce((a, r) => a + Number(r.amount || 0), 0);
    const withState = filteredRows.filter((r) => !!norm(r.state)).length;
    const inferred = filteredRows.filter((r) => r.stateFrom === "contact.state").length;
    for (const r of filteredRows) {
      const cid = norm(r.contactId);
      if (!cid) continue;
      const prev = contactMap.get(cid) || { tx: 0, lifetimeNet: 0, lifetimeOrders: 0 };
      prev.tx += 1;
      prev.lifetimeNet = Number(r.contactLifetimeNet || prev.lifetimeNet || 0);
      prev.lifetimeOrders = Number(r.contactLifetimeOrders || prev.lifetimeOrders || 0);
      contactMap.set(cid, prev);
    }
    const uniqueCustomers = contactMap.size;
    const repeatCustomers = Array.from(contactMap.values()).filter((v) => v.tx > 1).length;
    const avgLifetimeOrderValue = uniqueCustomers
      ? Number(
          (
            Array.from(contactMap.values()).reduce((acc, v) => acc + Number(v.lifetimeNet || 0), 0) /
            uniqueCustomers
          ).toFixed(2),
        )
      : 0;
    return {
      total,
      successfulTransactions: successfulRows.length,
      nonRevenueTransactions: Math.max(0, total - successfulRows.length),
      grossAmount: Number(gross.toFixed(2)),
      netAmount: Number((gross - refundedAmount).toFixed(2)),
      avgTicket: successfulRows.length ? Number((gross / successfulRows.length).toFixed(2)) : 0,
      refundedTransactions: refundedRows.length,
      refundedAmount: Number(refundedAmount.toFixed(2)),
      stateRate: total ? Math.round((withState / total) * 100) : 0,
      inferred,
      uniqueCustomers,
      avgOrdersPerCustomer: uniqueCustomers ? Number((total / uniqueCustomers).toFixed(2)) : 0,
      repeatCustomerRate: uniqueCustomers ? Math.round((repeatCustomers / uniqueCustomers) * 100) : 0,
      avgLifetimeOrderValue,
    };
  }, [filteredRows]);

  const prevKpis = useMemo(() => {
    const total = prevFilteredRows.length;
    const contactMap = new Map<string, { tx: number; lifetimeNet: number }>();
    const successfulRows = prevFilteredRows.filter((r) => isSucceededRevenueStatus(r.status));
    const gross = successfulRows.reduce((a, r) => a + Number(r.amount || 0), 0);
    const refundedRows = prevFilteredRows.filter((r) => {
      return isRefundLike(r.status);
    });
    const withState = prevFilteredRows.filter((r) => !!norm(r.state)).length;
    for (const r of prevFilteredRows) {
      const cid = norm(r.contactId);
      if (!cid) continue;
      const prev = contactMap.get(cid) || { tx: 0, lifetimeNet: 0 };
      prev.tx += 1;
      prev.lifetimeNet = Number(r.contactLifetimeNet || prev.lifetimeNet || 0);
      contactMap.set(cid, prev);
    }
    const uniqueCustomers = contactMap.size;
    const repeatCustomers = Array.from(contactMap.values()).filter((v) => v.tx > 1).length;
    return {
      total,
      grossAmount: Number(gross.toFixed(2)),
      avgTicket: successfulRows.length ? Number((gross / successfulRows.length).toFixed(2)) : 0,
      refundedTransactions: refundedRows.length,
      stateRate: total ? Math.round((withState / total) * 100) : 0,
      avgLifetimeOrderValue: uniqueCustomers
        ? Number(
            (
              Array.from(contactMap.values()).reduce((acc, v) => acc + Number(v.lifetimeNet || 0), 0) /
              uniqueCustomers
            ).toFixed(2),
          )
        : 0,
    };
  }, [prevFilteredRows]);

  const totalDelta = useMemo(() => (prevKpis ? pctDelta(kpis.total, prevKpis.total) : null), [kpis.total, prevKpis]);
  const grossDelta = useMemo(() => (prevKpis ? pctDelta(kpis.grossAmount, prevKpis.grossAmount) : null), [kpis.grossAmount, prevKpis]);
  const avgTicketDelta = useMemo(() => (prevKpis ? pctDelta(kpis.avgTicket, prevKpis.avgTicket) : null), [kpis.avgTicket, prevKpis]);
  const refundDelta = useMemo(
    () => (prevKpis ? pctDelta(kpis.refundedTransactions, prevKpis.refundedTransactions) : null),
    [kpis.refundedTransactions, prevKpis],
  );
  const ltvDelta = useMemo(
    () => (prevKpis ? pctDelta(kpis.avgLifetimeOrderValue, prevKpis.avgLifetimeOrderValue) : null),
    [kpis.avgLifetimeOrderValue, prevKpis],
  );
  const stateRateDelta = useMemo(
    () => (prevKpis ? pctDelta(kpis.stateRate, prevKpis.stateRate) : null),
    [kpis.stateRate, prevKpis],
  );

  const trend = useMemo(() => buildTrend(filteredRows, grain, trendMetric), [filteredRows, grain, trendMetric]);

  const byStateCount = useMemo(() => {
    const m: Record<string, number> = {};
    for (const r of filteredRows) {
      const st = norm(r.state);
      if (!st) m.__unknown = (m.__unknown || 0) + 1;
      else m[st] = (m[st] || 0) + 1;
    }
    return m;
  }, [filteredRows]);

  const byStateAmount = useMemo(() => {
    const m: Record<string, number> = {};
    for (const r of filteredRows) {
      if (!isSucceededRevenueStatus(r.status)) continue;
      const st = norm(r.state);
      if (!st) m.__unknown = Number(((m.__unknown || 0) + Number(r.amount || 0)).toFixed(2));
      else m[st] = Number(((m[st] || 0) + Number(r.amount || 0)).toFixed(2));
    }
    return m;
  }, [filteredRows]);

  const byMethod = useMemo(() => {
    const m: Record<string, number> = {};
    for (const r of filteredRows) {
      const pm = safeMethodLabel(r.paymentMethod || "unknown");
      m[pm] = (m[pm] || 0) + 1;
    }
    return m;
  }, [filteredRows]);

  const methodsSorted = useMemo(() => Object.entries(byMethod).sort((a, b) => b[1] - a[1]), [byMethod]);

  const mapRows = useMemo(() => {
    const source = mapMetric === "amount" ? ((data?.byStateAmount || {}) as Record<string, number>) : ((data?.byStateCount || {}) as Record<string, number>);
    return Object.entries(source)
      .filter(([state]) => state !== "__unknown")
      .map(([state, val]) => ({
        state,
        counties: { total: 1, ready: Number(val || 0), domainsActive: 0 },
        cities: { total: 0, ready: 0, domainsActive: 0 },
        __value: Number(val || 0),
      }));
  }, [data, mapMetric]);

  const unknownStateCount = Number(byStateCount.__unknown || 0);
  const unknownStateAmount = Number(byStateAmount.__unknown || 0);

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
          grossDelta,
          avgTicketDelta,
          refundDelta,
          stateRateDelta,
        },
        byMethod,
        byStateCount,
        byStateAmount,
        trend: trend.slice(-30),
        rowsPreview: filteredRows.slice(0, 120).map((r) => ({
          id: r.id,
          customerName: r.customerName,
          amount: r.amount,
          status: r.status,
          paymentMethod: r.paymentMethod,
          state: r.state,
          city: r.city,
          stateFrom: r.stateFrom,
          createdAt: r.createdAt,
        })),
      };

      const res = await fetch("/api/dashboard/transactions/insights", {
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
            <div className="dashLoadingText">Updating Transactions Dashboard...</div>
            <div className="mini" style={{ marginTop: 6 }}>
              Syncing payments, geo attribution and finance KPIs.
            </div>
          </div>
        </div>
      ) : null}

      <header className="topbar">
        <div className="brand">
          <div className="logo" />
          <div>
            <h1>My Drip Nurse — Transactions Dashboard</h1>
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
              Rango afecta KPIs, revenue trend, mapa y tabla de transacciones.
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
              {data.cache.snapshotCoverage?.newestCreatedAt
                ? ` • newest tx ${fmtDateLocal(data.cache.snapshotCoverage.newestCreatedAt)}`
                : ""}
              {data.cache.snapshotCoverage?.oldestCreatedAt
                ? ` • oldest tx ${fmtDateLocal(data.cache.snapshotCoverage.oldestCreatedAt)}`
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
            <h2 className="cardTitle">Transaction KPIs ({mapSelected || "All states"})</h2>
            <div className="cardSubtitle">
              Volumen, revenue, refunds y cobertura geo por estado.
            </div>
          </div>
          <div className="badge">{kpis.total} transactions</div>
        </div>
        <div className="cardBody">
          <div className="kpiRow kpiRowWide">
            <div className="kpi">
              <p className="n">{fmtInt(kpis.total)}</p>
              <p className="l">Total transactions</p>
              <div className={`mini ${deltaClass(totalDelta)}`}>{fmtDelta(totalDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(kpis.successfulTransactions)}</p>
              <p className="l">Succeeded transactions</p>
              <div className="mini">Only these count as revenue</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(kpis.nonRevenueTransactions)}</p>
              <p className="l">Pending/failed/non-revenue</p>
              <div className="mini">Excluded from revenue metric</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtMoney(kpis.grossAmount)}</p>
              <p className="l">Revenue (succeeded only)</p>
              <div className={`mini ${deltaClass(grossDelta)}`}>{fmtDelta(grossDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtMoney(kpis.netAmount)}</p>
              <p className="l">Net collected (gross-refunds)</p>
              <div className="mini">Contabilidad operativa</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtMoney(kpis.avgTicket)}</p>
              <p className="l">Average ticket</p>
              <div className={`mini ${deltaClass(avgTicketDelta)}`}>{fmtDelta(avgTicketDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtMoney(kpis.avgLifetimeOrderValue)}</p>
              <p className="l">Avg Lifetime Order Value</p>
              <div className={`mini ${deltaClass(ltvDelta)}`}>{fmtDelta(ltvDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(kpis.refundedTransactions)}</p>
              <p className="l">Refund/chargeback tx</p>
              <div className={`mini ${deltaClass(refundDelta)}`}>{fmtDelta(refundDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtMoney(kpis.refundedAmount)}</p>
              <p className="l">Refunded amount</p>
              <div className="mini">Risk leakage</div>
            </div>
            <div className="kpi">
              <p className="n">{kpis.stateRate}%</p>
              <p className="l">Mapped state coverage</p>
              <div className={`mini ${deltaClass(stateRateDelta)}`}>{fmtDelta(stateRateDelta)} vs prev period</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(kpis.inferred)}</p>
              <p className="l">State inferred from contact</p>
              <div className="mini">Fallback attribution</div>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(kpis.uniqueCustomers)}</p>
              <p className="l">Unique customers</p>
              <div className="mini">{kpis.avgOrdersPerCustomer.toFixed(2)} orders/customer</div>
            </div>
            <div className="kpi">
              <p className="n">{kpis.repeatCustomerRate}%</p>
              <p className="l">Repeat customer rate</p>
              <div className="mini">Customers with 2+ orders</div>
            </div>
          </div>
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Transaction trend</h2>
            <div className="cardSubtitle">Evolución por día, semana o mes para count y revenue.</div>
          </div>
          <div className="cardHeaderActions">
            <div className="rangePills" style={{ marginBottom: 6 }}>
              {(["count", "amount"] as TrendMetric[]).map((m) => (
                <button key={m} className={`smallBtn ${trendMetric === m ? "smallBtnOn" : ""}`} onClick={() => setTrendMetric(m)} type="button">
                  {m === "count" ? "Transactions" : "Revenue"}
                </button>
              ))}
            </div>
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
              <LineTrend points={trend} metric={trendMetric} onHover={setHoverPoint} />
              <div className="mini" style={{ marginTop: 10 }}>
                {hoverPoint ? (
                  <>
                    {hoverPoint.label} • <b>{trendMetric === "amount" ? fmtMoney(hoverPoint.value) : fmtInt(hoverPoint.value)}</b>
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
                  <div className="mapCardTitle">US Transactions Map</div>
                  <div className="mini" style={{ marginTop: 6 }}>
                    Click en un estado para filtrar KPIs, chart, AI insights y tabla.
                  </div>
                </div>
                <div className="rangePills">
                  <button
                    className={`smallBtn ${mapMetric === "count" ? "smallBtnOn" : ""}`}
                    onClick={() => setMapMetric("count")}
                    type="button"
                  >
                    Transactions
                  </button>
                  <button
                    className={`smallBtn ${mapMetric === "amount" ? "smallBtnOn" : ""}`}
                    onClick={() => setMapMetric("amount")}
                    type="button"
                  >
                    Revenue
                  </button>
                </div>
              </div>
              <div className="mapFrame mapFrameXL">
                <UsaChoroplethProgressMap
                  rows={mapRows as any}
                  metric={"calls" as any}
                  labelMode={"value" as any}
                  valueField={"__value" as any}
                  valuePrefix={mapMetric === "amount" ? "$" : ""}
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
                    <div className="statePill">
                      {mapMetric === "amount"
                        ? fmtMoney((mapSelected === "__unknown" ? unknownStateAmount : byStateAmount[mapSelected]) || 0)
                        : `${fmtInt((mapSelected === "__unknown" ? unknownStateCount : byStateCount[mapSelected]) || 0)} tx`}
                    </div>
                  </div>
                ) : (
                  <div className="stateHead">
                    <div className="stateName">All states</div>
                    <div className="statePill">{fmtInt(kpis.total)} tx</div>
                  </div>
                )}
              </div>

              <div className="stateCards">
                <div className="stateKpi">
                  <div className="mini">UNKNOWN STATE</div>
                  <div className="stateKpiN">{fmtInt(unknownStateCount)}</div>
                  <div className="mini" style={{ opacity: 0.85 }}>
                    {fmtMoney(unknownStateAmount)} revenue not mapped
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

                {methodsSorted.slice(0, 6).map(([method, count]) => {
                  const share = kpis.total ? Math.round((count / kpis.total) * 100) : 0;
                  return (
                    <div className="stateKpi" key={method}>
                      <div className="mini">{method.toUpperCase()}</div>
                      <div className="stateKpiN">{fmtInt(count)}</div>
                      <div className="mini" style={{ opacity: 0.85 }}>{share}% of transactions</div>
                    </div>
                  );
                })}
              </div>

              <div className="aiCard">
                <div className="aiCardTop">
                  <div>
                    <div className="aiTitle">AI Strategist (Finance & Growth)</div>
                    <div className="mini" style={{ opacity: 0.85, marginTop: 4 }}>
                      Responde sobre cashflow, refunds, mezcla de pagos y foco por estado.
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
                    Generate insights to get finance + growth strategy for this scope.
                  </div>
                )}
              </div>

              <div style={{ marginTop: 12 }}>
                <AiAgentChatPanel
                  agent="transactions"
                  title="Transactions Agent Chat"
                  context={{
                    preset,
                    customStart,
                    customEnd,
                    trendGrain: grain,
                    trendMetric,
                    mapMetric,
                    selectedState: mapSelected || null,
                    kpis,
                    byMethod,
                    byStateCount,
                    byStateAmount,
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
            <h2 className="cardTitle">Transactions table</h2>
            <div className="cardSubtitle">Detalle de transacciones para el alcance actual.</div>
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
                    <th className="th">Created</th>
                    <th className="th">Customer</th>
                    <th className="th">Amount</th>
                    <th className="th">Status</th>
                    <th className="th">Method</th>
                    <th className="th">State</th>
                    <th className="th">State From</th>
                    <th className="th">City</th>
                    <th className="th">Source</th>
                    <th className="th">Transaction ID</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows
                    .slice()
                    .sort((a, b) => Number(b.__createdMs || 0) - Number(a.__createdMs || 0))
                    .map((r, idx) => (
                      <tr key={`${r.id || r.contactId || "row"}_${idx}`} className="tr">
                        <td className="td"><span className="mini">{fmtDateLocal(r.createdAt)}</span></td>
                        <td className="td">{r.customerName || "-"}</td>
                        <td className="td">{fmtMoney(r.amount)}</td>
                        <td className="td">{r.status || "-"}</td>
                        <td className="td">{r.paymentMethod || "-"}</td>
                        <td className="td">{r.state || "-"}</td>
                        <td className="td"><span className="mini">{r.stateFrom}</span></td>
                        <td className="td">{r.city || "-"}</td>
                        <td className="td"><span className="mini">{r.source || "-"}</span></td>
                        <td className="td"><span className="mini">{r.id || "-"}</span></td>
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
            <h2 className="cardTitle">State breakdown (transactions + revenue)</h2>
            <div className="cardSubtitle">Unknown se muestra separado para no contaminar el mapa.</div>
          </div>
          <div className="badge">geo</div>
        </div>
        <div className="cardBody">
          <div className="tableWrap tableScrollX">
            <table className="table">
              <thead>
                <tr>
                  <th className="th">State</th>
                  <th className="th">Transactions</th>
                  <th className="th">Revenue</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(byStateCount)
                  .sort((a, b) => Number(b[1]) - Number(a[1]))
                  .map(([state, count]) => (
                    <tr key={state} className="tr">
                      <td className="td">{state === "__unknown" ? "Unknown" : state}</td>
                      <td className="td">{fmtInt(count)}</td>
                      <td className="td">{fmtMoney(byStateAmount[state] || 0)}</td>
                    </tr>
                  ))}
                {!Object.keys(byStateCount).length ? (
                  <tr className="tr">
                    <td className="td" colSpan={3} style={{ opacity: 0.75 }}>
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
