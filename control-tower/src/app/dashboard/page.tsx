"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import AiAgentChatPanel from "@/components/AiAgentChatPanel";

type RangePreset = "1d" | "7d" | "28d" | "1m" | "3m" | "6m" | "1y" | "custom";

type OverviewResponse = {
  ok: boolean;
  error?: string;
  range?: {
    start: string;
    end: string;
    preset: string;
    adsRange: string;
  };
  prevRange?: {
    start: string;
    end: string;
  };
  executive?: {
    leadsNow: number;
    leadsBefore: number;
    leadsDeltaPct: number | null;
    callsNow: number;
    callsBefore: number;
    callsDeltaPct: number | null;
    conversationsNow: number;
    conversationsBefore: number;
    conversationsDeltaPct: number | null;
    transactionsNow: number;
    transactionsBefore: number;
    transactionsDeltaPct: number | null;
    transactionsRevenueNow: number;
    transactionsRevenueBefore: number;
    transactionsRevenueDeltaPct: number | null;
    transactionsAvgLtvNow: number;
    appointmentsNow: number;
    appointmentsBefore: number;
    appointmentsDeltaPct: number | null;
    appointmentsLostNow: number;
    appointmentsLostBefore: number;
    appointmentsLostDeltaPct: number | null;
    appointmentsLostValueNow: number;
    appointmentsLostValueBefore: number;
    appointmentsLostValueDeltaPct: number | null;
    leadToCall: number | null;
    leadToCallDeltaPct: number | null;
    gscClicks: number;
    gscImpressions: number;
    gaSessions: number;
    gaUsers: number;
    gaConversions: number;
    adsCost: number;
    adsConversions: number;
    adsConversionValue: number;
  };
  businessScore?: {
    current: number;
    previous: number;
    deltaPct: number | null;
    grade: "A" | "B" | "C" | "D" | "F";
    granularity: "day" | "week" | "month";
    components: {
      volume: number;
      revenue: number;
      appointmentQuality: number;
      coverage: number;
      lossHealth: number;
    };
    trend: Array<{
      key: string;
      label: string;
      score: number;
      leads: number;
      calls: number;
      conversations: number;
      appointments: number;
      successfulRevenue: number;
      lostCount: number;
      lostValue: number;
    }>;
  };
  modules?: {
    calls?: {
      ok: boolean;
      total: number;
      prevTotal: number;
      deltaPct: number | null;
      error: string | null;
    };
    contacts?: {
      ok: boolean;
      total: number;
      prevTotal: number;
      deltaPct: number | null;
      contactableRate: number;
      emailRate: number;
      inferredFromOpportunity: number;
      error: string | null;
    };
    conversations?: {
      ok: boolean;
      total: number;
      prevTotal: number;
      deltaPct: number | null;
      mappedStateRate: number;
      topChannel: string;
      error: string | null;
    };
    transactions?: {
      ok: boolean;
      total: number;
      prevTotal: number;
      deltaPct: number | null;
      grossAmount: number;
      prevGrossAmount: number;
      revenueDeltaPct: number | null;
      avgLifetimeOrderValue: number;
      mappedStateRate: number;
      error: string | null;
    };
    appointments?: {
      ok: boolean;
      total: number;
      prevTotal: number;
      deltaPct: number | null;
      showRate: number;
      noShowRate: number;
      cancellationRate: number;
      mappedStateRate: number;
      lostQualified: number;
      lostQualifiedPrev: number;
      lostQualifiedDeltaPct: number | null;
      potentialLostValue: number;
      potentialLostValuePrev: number;
      potentialLostValueDeltaPct: number | null;
      error: string | null;
    };
    gsc?: {
      ok: boolean;
      totals?: Record<string, unknown>;
      deltas?: Record<string, unknown>;
      error: string | null;
    };
    ga?: {
      ok: boolean;
      summaryOverall?: Record<string, unknown>;
      compare?: Record<string, unknown>;
      error: string | null;
    };
    ads?: {
      ok: boolean;
      summary?: Record<string, unknown>;
      error: string | null;
    };
  };
};

type CeoInsights = {
  ceo_summary: string;
  board_meeting_narrative?: string;
  board_scorecard?: {
    health?: "good" | "mixed" | "bad";
    biggest_risk?: string;
    biggest_opportunity?: string;
  };
  swarm_coordination?: Array<{
    owner_agent: string;
    mission: string;
    expected_business_impact: "low" | "medium" | "high";
    dependencies: string[];
  }>;
  decisions_next_7_days?: string[];
  decisions_next_30_days?: string[];
  execute_plan?: Array<{
    priority: "P1" | "P2" | "P3";
    action: string;
    dashboard: "calls" | "leads" | "conversations" | "transactions" | "appointments" | "gsc" | "ga" | "ads" | "facebook_ads";
    rationale: string;
    trigger_metric: string;
  }>;
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

function fmtInt(v: unknown) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "0";
  return Math.round(n).toLocaleString();
}

function fmtMoney(v: unknown) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "$0";
  return n.toLocaleString(undefined, { style: "currency", currency: "USD" });
}

function fmtPct(v: number | null, isFraction = false) {
  if (v === null || !Number.isFinite(v)) return "-";
  const val = isFraction ? v * 100 : v;
  const sign = val > 0 ? "+" : "";
  return `${sign}${val.toFixed(1)}%`;
}

function deltaClass(v: number | null) {
  if (v === null || !Number.isFinite(v)) return "";
  return v < 0 ? "deltaDown" : "deltaUp";
}

function adsRangeFromPreset(preset: RangePreset) {
  if (preset === "7d") return "last_7_days";
  if (preset === "28d") return "last_28_days";
  if (preset === "1m") return "last_month";
  if (preset === "3m") return "last_quarter";
  if (preset === "6m") return "last_6_months";
  if (preset === "1y") return "last_year";
  if (preset === "custom") return "last_28_days";
  return "last_7_days";
}

export default function DashboardHome() {
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const [preset, setPreset] = useState<RangePreset>("28d");
  const [customStart, setCustomStart] = useState("");
  const [customEnd, setCustomEnd] = useState("");

  const [data, setData] = useState<OverviewResponse | null>(null);

  const [aiLoading, setAiLoading] = useState(false);
  const [aiErr, setAiErr] = useState("");
  const [aiInsights, setAiInsights] = useState<CeoInsights | null>(null);
  const [boardMeetingMode, setBoardMeetingMode] = useState(false);
  const [hardRefreshing, setHardRefreshing] = useState(false);

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

  async function load(force = false) {
    setErr("");
    setLoading(true);
    setHardRefreshing(force);
    setAiInsights(null);
    setAiErr("");

    try {
      if (!computedRange.start || !computedRange.end) {
        throw new Error("Missing start/end range");
      }

      const qs = new URLSearchParams();
      qs.set("start", computedRange.start);
      qs.set("end", computedRange.end);
      qs.set("preset", preset);
      qs.set("adsRange", adsRangeFromPreset(preset));
      if (force) qs.set("force", "1");

      const res = await fetch(`/api/dashboard/overview?${qs.toString()}`, {
        cache: "no-store",
      });

      const ct = res.headers.get("content-type") || "";
      if (!ct.includes("application/json")) {
        const txt = await res.text();
        throw new Error(`Overview API non-JSON: ${txt.slice(0, 120)}`);
      }

      const json = (await res.json()) as OverviewResponse;
      if (!res.ok || !json?.ok) {
        throw new Error(json?.error || `HTTP ${res.status}`);
      }

      setData(json);
    } catch (e: unknown) {
      setData(null);
      setErr(e instanceof Error ? e.message : "Failed to load Executive Dashboard");
    } finally {
      setLoading(false);
      setHardRefreshing(false);
    }
  }

  useEffect(() => {
    if (preset !== "custom") load(false);
    else if (customStart && customEnd) load(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [preset, customStart, customEnd]);

  async function runCeoInsights() {
    setAiErr("");
    setAiLoading(true);
    setAiInsights(null);

    try {
      if (!data?.executive) {
        throw new Error("No executive data available.");
      }

      const payload = {
        range: data.range,
        prevRange: data.prevRange,
        executive: data.executive,
        modules: data.modules,
        swarm_agents: [
          "calls_strategist",
          "leads_strategist",
          "conversations_strategist",
          "transactions_strategist",
          "appointments_strategist",
          "gsc_strategist",
          "ga_strategist",
          "ads_strategist",
        ],
        objective:
          "Maximize growth efficiency with clear CEO-level decisions and cross-agent orchestration.",
        readiness: {
          gsc: {
            status: "test_mode_pending_approval",
            note: "GSC is pending approval to move out of test mode.",
          },
          facebook_ads: {
            status: "not_configured",
            note: "Facebook Ads setup is pending.",
          },
          keyword_planner: {
            status: "planned",
            note: "Google Ads Keyword Planner integration is planned for campaign recommendation automation.",
          },
        },
      };

      const res = await fetch("/api/dashboard/overview/insights", {
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
        insights?: CeoInsights;
      };

      if (!res.ok || !json?.ok) {
        throw new Error(json?.error || "Failed to generate CEO insights");
      }

      setAiInsights(json.insights || null);
      setBoardMeetingMode(true);
    } catch (e: unknown) {
      setAiErr(e instanceof Error ? e.message : "Failed to generate CEO insights");
    } finally {
      setAiLoading(false);
    }
  }

  const ex = data?.executive;
  const m = data?.modules;
  const bs = data?.businessScore;

  function dashboardHref(dashboard: string) {
    if (dashboard === "calls") return "/dashboard/calls";
    if (dashboard === "leads") return "/dashboard/contacts";
    if (dashboard === "conversations") return "/dashboard/conversations";
    if (dashboard === "transactions") return "/dashboard/transactions";
    if (dashboard === "appointments") return "/dashboard/appointments";
    if (dashboard === "gsc") return "/dashboard/gsc";
    if (dashboard === "ga") return "/dashboard/ga";
    if (dashboard === "ads") return "/dashboard/ads";
    return "";
  }

  return (
    <div className="shell callsDash ceoDash">
      {loading ? (
        <div className="dashLoadingOverlay" aria-live="polite" aria-busy="true">
          <div className="dashLoadingCard">
            <div className="dashSpinner" />
            <div className="dashLoadingText">Updating Executive Dashboard...</div>
            <div className="mini" style={{ marginTop: 6 }}>
              Syncing all module KPIs and comparisons.
            </div>
          </div>
        </div>
      ) : null}

      <header className="topbar">
        <div className="brand">
          <div className="logo" />
          <div>
            <h1>My Drip Nurse — Dashboard</h1>
          </div>
        </div>

        <div className="pills">
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

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Executive Filters</h2>
            <div className="cardSubtitle">
              Rango global para comparar negocio entre dashboards y período previo.
            </div>
          </div>
          <Link className="smallBtn" href="/">
            Back to Control Tower
          </Link>
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
                <button
                  className="btn btnPrimary applyBtn"
                  onClick={() => load(false)}
                  disabled={loading || (preset === "custom" && (!customStart || !customEnd))}
                  type="button"
                >
                  {loading && !hardRefreshing ? "Applying..." : "Refresh"}
                </button>
                <button
                  className="smallBtn"
                  onClick={() => load(true)}
                  disabled={loading || (preset === "custom" && (!customStart || !customEnd))}
                  type="button"
                  title="Force refresh all module dashboards and bypass snapshot cache"
                >
                  {loading && hardRefreshing ? "Hard Refreshing..." : "Hard Refresh All"}
                </button>
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
            <h2 className="cardTitle">CEO KPI Board</h2>
            <div className="cardSubtitle">
              Vista consolidada del negocio para decisiones ejecutivas.
            </div>
          </div>
          <div className="badge">{loading ? "loading..." : "ready"}</div>
        </div>

        <div className="cardBody">
          <div className="kpiRow kpiRowWide">
            <div className="kpi">
              <p className="n">{fmtInt(ex?.leadsNow)}</p>
              <p className="l">Total Leads</p>
              <div className={`mini ${deltaClass(ex?.leadsDeltaPct ?? null)}`}>
                {fmtPct(ex?.leadsDeltaPct ?? null)} vs prev period
              </div>
            </div>

            <div className="kpi">
              <p className="n">{fmtInt(ex?.callsNow)}</p>
              <p className="l">Total Calls</p>
              <div className={`mini ${deltaClass(ex?.callsDeltaPct ?? null)}`}>
                {fmtPct(ex?.callsDeltaPct ?? null)} vs prev period
              </div>
            </div>

            <div className="kpi">
              <p className="n">{fmtInt(ex?.conversationsNow)}</p>
              <p className="l">Conversations</p>
              <div className={`mini ${deltaClass(ex?.conversationsDeltaPct ?? null)}`}>
                {fmtPct(ex?.conversationsDeltaPct ?? null)} vs prev period
              </div>
            </div>

            <div className="kpi">
              <p className="n">{fmtMoney(ex?.transactionsRevenueNow)}</p>
              <p className="l">Transactions Revenue</p>
              <div className={`mini ${deltaClass(ex?.transactionsRevenueDeltaPct ?? null)}`}>
                {fmtPct(ex?.transactionsRevenueDeltaPct ?? null)} vs prev period
              </div>
            </div>

            <div className="kpi">
              <p className="n">{fmtMoney(ex?.transactionsAvgLtvNow)}</p>
              <p className="l">Avg Lifetime Order Value</p>
              <div className="mini">Average by transacting customer</div>
            </div>

            <div className="kpi">
              <p className="n">{fmtInt(ex?.appointmentsNow)}</p>
              <p className="l">Appointments</p>
              <div className={`mini ${deltaClass(ex?.appointmentsDeltaPct ?? null)}`}>
                {fmtPct(ex?.appointmentsDeltaPct ?? null)} vs prev period
              </div>
            </div>

            <div className="kpi">
              <p className="n">{fmtInt(ex?.appointmentsLostNow)}</p>
              <p className="l">Lost Qualified Bookings</p>
              <div className={`mini ${deltaClass(ex?.appointmentsLostDeltaPct ?? null)}`}>
                {fmtPct(ex?.appointmentsLostDeltaPct ?? null)} vs prev period
              </div>
            </div>

            <div className="kpi">
              <p className="n">{ex?.leadToCall == null ? "-" : ex.leadToCall.toFixed(2)}</p>
              <p className="l">Leads per Call</p>
              <div className={`mini ${deltaClass(ex?.leadToCallDeltaPct ?? null)}`}>
                {fmtPct(ex?.leadToCallDeltaPct ?? null)} efficiency
              </div>
            </div>

            <div className="kpi">
              <p className="n">{fmtInt(ex?.gaSessions)}</p>
              <p className="l">GA Sessions</p>
              <div className="mini">Users: {fmtInt(ex?.gaUsers)}</div>
            </div>

            <div className="kpi">
              <p className="n">{fmtInt(ex?.gscClicks)}</p>
              <p className="l">GSC Clicks</p>
              <div className="mini">Impr: {fmtInt(ex?.gscImpressions)}</div>
            </div>

            <div className="kpi">
              <p className="n">{fmtMoney(ex?.adsCost)}</p>
              <p className="l">Ads Spend</p>
              <div className="mini">Conv: {fmtInt(ex?.adsConversions)}</div>
            </div>
          </div>
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Business Health Score</h2>
            <div className="cardSubtitle">
              Score compuesto (0-100) y tendencia por {bs?.granularity || "period"}.
            </div>
          </div>
          <div className="badge">{bs?.grade ? `Grade ${bs.grade}` : "-"}</div>
        </div>
        <div className="cardBody">
          <div className="kpiRow kpiRowWide">
            <div className="kpi">
              <p className="n">{fmtInt(bs?.current)}</p>
              <p className="l">Current score</p>
              <div className={`mini ${deltaClass(bs?.deltaPct ?? null)}`}>
                {fmtPct(bs?.deltaPct ?? null)} vs prev period
              </div>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(bs?.components?.volume)}</p>
              <p className="l">Volume</p>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(bs?.components?.revenue)}</p>
              <p className="l">Revenue strength</p>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(bs?.components?.appointmentQuality)}</p>
              <p className="l">Appointment quality</p>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(bs?.components?.coverage)}</p>
              <p className="l">Lead coverage</p>
            </div>
            <div className="kpi">
              <p className="n">{fmtInt(bs?.components?.lossHealth)}</p>
              <p className="l">Loss control</p>
            </div>
          </div>

          <div style={{ marginTop: 14 }}>
            <div className="mini" style={{ marginBottom: 8 }}>
              Trend ({bs?.trend?.length || 0} points)
            </div>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: `repeat(${Math.max(1, bs?.trend?.length || 1)}, minmax(0, 1fr))`,
                gap: 6,
                alignItems: "end",
                minHeight: 130,
              }}
            >
              {(bs?.trend || []).map((p) => {
                const h = Math.max(6, Math.min(100, Number(p.score || 0)));
                return (
                  <div key={p.key} style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 6 }}>
                    <div className="mini" style={{ opacity: 0.9 }}>{fmtInt(p.score)}</div>
                    <div
                      title={`${p.label} | score ${p.score} | leads ${p.leads} | revenue ${fmtMoney(p.successfulRevenue)} | lost ${fmtMoney(p.lostValue)}`}
                      style={{
                        width: "100%",
                        maxWidth: 34,
                        height: `${h}%`,
                        minHeight: 8,
                        borderRadius: 8,
                        background: "linear-gradient(180deg, rgba(86,225,170,.95), rgba(62,130,246,.9))",
                        boxShadow: "0 0 0 1px rgba(255,255,255,.08) inset",
                      }}
                    />
                    <div className="mini" style={{ textAlign: "center", lineHeight: 1.1 }}>{p.label}</div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Module Dashboards</h2>
            <div className="cardSubtitle">
              KPIs críticos por módulo con acceso directo a cada dashboard.
            </div>
          </div>
          <div className="cardHeaderActions">
            <button
              className={`smallBtn ${boardMeetingMode ? "smallBtnOn" : ""}`}
              onClick={() => setBoardMeetingMode((x) => !x)}
              type="button"
            >
              {boardMeetingMode ? "Board Meeting: ON" : "Board Meeting: OFF"}
            </button>
          </div>
        </div>

        <div className="cardBody">
          <div className="moduleGrid">
            <div className="moduleCard">
              <div className="moduleTop">
                <p className="l moduleTitle">Calls</p>
                <span className={`mini moduleDelta ${deltaClass(m?.calls?.deltaPct ?? null)}`}>
                  {fmtPct(m?.calls?.deltaPct ?? null)}
                </span>
              </div>
              <div className="moduleStats">
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Total calls</div>
                  <div className="moduleStatValue">{fmtInt(m?.calls?.total)}</div>
                </div>
              </div>
              <div className="moduleActions">
                <Link className="btn btnPrimary moduleBtn" href="/dashboard/calls">Open Calls Dashboard</Link>
              </div>
            </div>

            <div className="moduleCard">
              <div className="moduleTop">
                <p className="l moduleTitle">Contacts / Leads</p>
                <span className={`mini moduleDelta ${deltaClass(m?.contacts?.deltaPct ?? null)}`}>
                  {fmtPct(m?.contacts?.deltaPct ?? null)}
                </span>
              </div>
              <div className="moduleStats">
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Total leads</div>
                  <div className="moduleStatValue">{fmtInt(m?.contacts?.total)}</div>
                </div>
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Inferred from opportunity</div>
                  <div className="moduleStatValue">{fmtInt(m?.contacts?.inferredFromOpportunity)}</div>
                </div>
              </div>
              <div className="moduleActions">
                <Link className="btn btnPrimary moduleBtn" href="/dashboard/contacts">Open Leads Dashboard</Link>
              </div>
            </div>

            <div className="moduleCard">
              <div className="moduleTop">
                <p className="l moduleTitle">Conversations / CRM</p>
                <span className={`mini moduleDelta ${deltaClass(m?.conversations?.deltaPct ?? null)}`}>
                  {fmtPct(m?.conversations?.deltaPct ?? null)}
                </span>
              </div>
              <div className="moduleStats">
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Total conversations</div>
                  <div className="moduleStatValue">{fmtInt(m?.conversations?.total)}</div>
                </div>
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">State mapping rate</div>
                  <div className="moduleStatValue">{fmtInt(m?.conversations?.mappedStateRate)}%</div>
                </div>
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Top channel</div>
                  <div className="moduleStatValue">{String(m?.conversations?.topChannel || "unknown")}</div>
                </div>
              </div>
              <div className="moduleActions">
                <Link className="btn btnPrimary moduleBtn" href="/dashboard/conversations">Open Conversations Dashboard</Link>
              </div>
              {m?.conversations?.error ? (
                <div className="mini" style={{ color: "var(--danger)", marginTop: 8 }}>
                  X {m.conversations.error}
                </div>
              ) : null}
            </div>

            <div className="moduleCard">
              <div className="moduleTop">
                <p className="l moduleTitle">Transactions / Revenue</p>
                <span className={`mini moduleDelta ${deltaClass(m?.transactions?.deltaPct ?? null)}`}>
                  {fmtPct(m?.transactions?.deltaPct ?? null)}
                </span>
              </div>
              <div className="moduleStats">
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Transactions</div>
                  <div className="moduleStatValue">{fmtInt(m?.transactions?.total)}</div>
                </div>
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Revenue (succeeded)</div>
                  <div className="moduleStatValue">{fmtMoney(m?.transactions?.grossAmount)}</div>
                </div>
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Avg Lifetime Order Value</div>
                  <div className="moduleStatValue">{fmtMoney(m?.transactions?.avgLifetimeOrderValue)}</div>
                </div>
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">State mapping rate</div>
                  <div className="moduleStatValue">{fmtInt(m?.transactions?.mappedStateRate)}%</div>
                </div>
              </div>
              <div className="moduleActions">
                <Link className="btn btnPrimary moduleBtn" href="/dashboard/transactions">Open Transactions Dashboard</Link>
              </div>
              {m?.transactions?.error ? (
                <div className="mini" style={{ color: "var(--danger)", marginTop: 8 }}>
                  X {m.transactions.error}
                </div>
              ) : null}
            </div>

            <div className="moduleCard">
              <div className="moduleTop">
                <p className="l moduleTitle">Appointments</p>
                <span className={`mini moduleDelta ${deltaClass(m?.appointments?.deltaPct ?? null)}`}>
                  {fmtPct(m?.appointments?.deltaPct ?? null)}
                </span>
              </div>
              <div className="moduleStats">
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Total appointments</div>
                  <div className="moduleStatValue">{fmtInt(m?.appointments?.total)}</div>
                </div>
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Show rate</div>
                  <div className="moduleStatValue">{fmtInt(m?.appointments?.showRate)}%</div>
                </div>
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Cancel rate</div>
                  <div className="moduleStatValue">{fmtInt(m?.appointments?.cancellationRate)}%</div>
                </div>
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">No-show rate</div>
                  <div className="moduleStatValue">{fmtInt(m?.appointments?.noShowRate)}%</div>
                </div>
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Lost qualified bookings</div>
                  <div className="moduleStatValue">{fmtInt(m?.appointments?.lostQualified)}</div>
                </div>
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Potential lost value</div>
                  <div className="moduleStatValue">{fmtMoney(m?.appointments?.potentialLostValue)}</div>
                </div>
              </div>
              <div className="moduleActions">
                <Link className="btn btnPrimary moduleBtn" href="/dashboard/appointments">Open Appointments Dashboard</Link>
              </div>
              {m?.appointments?.error ? (
                <div className="mini" style={{ color: "var(--danger)", marginTop: 8 }}>
                  X {m.appointments.error}
                </div>
              ) : null}
            </div>

            <div className="moduleCard">
              <div className="moduleTop">
                <p className="l moduleTitle">Google Search Console</p>
                <span className={`mini moduleDelta ${deltaClass((m?.gsc?.deltas?.clicksPct as number) || null)}`}>
                  {fmtPct((m?.gsc?.deltas?.clicksPct as number) || null)}
                </span>
              </div>
              <div className="moduleStats">
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Clicks</div>
                  <div className="moduleStatValue">{fmtInt((m?.gsc?.totals?.clicks as number) || 0)}</div>
                </div>
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Impressions</div>
                  <div className="moduleStatValue">{fmtInt((m?.gsc?.totals?.impressions as number) || 0)}</div>
                </div>
              </div>
              <div className="moduleActions">
                <Link className="btn btnPrimary moduleBtn" href="/dashboard/gsc">Open GSC Dashboard</Link>
              </div>
            </div>

            <div className="moduleCard">
              <div className="moduleTop">
                <p className="l moduleTitle">Google Analytics</p>
              </div>
              <div className="moduleStats">
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Sessions</div>
                  <div className="moduleStatValue">{fmtInt((m?.ga?.summaryOverall?.sessions as number) || 0)}</div>
                </div>
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Conversions</div>
                  <div className="moduleStatValue">{fmtInt((m?.ga?.summaryOverall?.conversions as number) || 0)}</div>
                </div>
              </div>
              <div className="moduleActions">
                <Link className="btn btnPrimary moduleBtn" href="/dashboard/ga">Open GA Dashboard</Link>
              </div>
            </div>

            <div className="moduleCard">
              <div className="moduleTop">
                <p className="l moduleTitle">Google Ads</p>
              </div>
              <div className="moduleStats">
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Cost</div>
                  <div className="moduleStatValue">{fmtMoney((m?.ads?.summary?.cost as number) || 0)}</div>
                </div>
                <div className="moduleStat">
                  <div className="mini moduleStatLabel">Conversions</div>
                  <div className="moduleStatValue">{fmtInt((m?.ads?.summary?.conversions as number) || 0)}</div>
                </div>
              </div>
              <div className="moduleActions">
                <Link className="btn btnPrimary moduleBtn" href="/dashboard/ads">Open Ads Dashboard</Link>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Growth Ops Readiness</h2>
            <div className="cardSubtitle">
              Estado operativo para escalar a Facebook Ads + recomendaciones automáticas con Keyword Planner.
            </div>
          </div>
        </div>
        <div className="cardBody">
          <div className="moduleGrid">
            <div className="moduleCard">
              <p className="l moduleTitle">Google Search Console Access</p>
              <p className="mini moduleLine">Current: waiting approval to leave test mode.</p>
              <p className="mini moduleLine">Impact: limits production-scale insights automation.</p>
            </div>
            <div className="moduleCard">
              <p className="l moduleTitle">Facebook Ads Integration</p>
              <p className="mini moduleLine">Current: setup pending.</p>
              <p className="mini moduleLine">Next: connect API, accounts, pixel, and conversion events.</p>
            </div>
            <div className="moduleCard">
              <p className="l moduleTitle">Google Ads Keyword Planner</p>
              <p className="mini moduleLine">Current: planned integration.</p>
              <p className="mini moduleLine">Next: automate geo-intent demand diagnosis by state/county/city.</p>
            </div>
          </div>
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">AI CEO Swarm</h2>
            <div className="cardSubtitle">
              Agente ejecutivo que coordina Calls, Leads, Conversations, Transactions, Appointments, GSC, GA y Ads para priorizar decisiones de negocio.
            </div>
          </div>
          <div className="cardHeaderActions">
            <button
              className="smallBtn"
              onClick={runCeoInsights}
              disabled={aiLoading || loading || !data?.executive}
              type="button"
            >
              {aiLoading ? "Generating..." : "Run CEO Analysis"}
            </button>
            <button
              className="smallBtn btnPrimary"
              onClick={runCeoInsights}
              disabled={aiLoading || loading || !data?.executive}
              type="button"
            >
              Execute Plan
            </button>
          </div>
        </div>

        <div className="cardBody">
          {aiErr ? (
            <div className="mini" style={{ color: "var(--danger)" }}>X {aiErr}</div>
          ) : null}

          {aiInsights ? (
            <div className="aiBody">
              <div className="aiSummary">
                <div className="aiSummaryTitle">CEO Summary</div>
                <div className="aiText">{aiInsights.ceo_summary}</div>
              </div>

              {aiInsights.board_meeting_narrative ? (
                <div className="aiBlock">
                  <div className="aiBlockTitle">Board Meeting Mode</div>
                  <div className="aiText">{aiInsights.board_meeting_narrative}</div>
                </div>
              ) : null}

              <div className="aiScore">
                <span className={`aiBadge ${aiInsights.board_scorecard?.health || "mixed"}`}>
                  {String(aiInsights.board_scorecard?.health || "mixed").toUpperCase()}
                </span>
                <div className="mini" style={{ marginTop: 8 }}>
                  <b>Biggest risk:</b> {aiInsights.board_scorecard?.biggest_risk}
                </div>
                <div className="mini" style={{ marginTop: 6 }}>
                  <b>Biggest opportunity:</b> {aiInsights.board_scorecard?.biggest_opportunity}
                </div>
              </div>

              {!!aiInsights.swarm_coordination?.length && (
                <div className="aiBlock">
                  <div className="aiBlockTitle">Swarm Coordination Plan</div>
                  <div className="aiOps">
                    {aiInsights.swarm_coordination.slice(0, 4).map((x, idx) => (
                      <div className="aiOp" key={idx}>
                        <div className="aiOpHead">
                          <div className="aiOpTitle">{x.owner_agent}</div>
                          <span className={`aiImpact ${x.expected_business_impact}`}>
                            {x.expected_business_impact.toUpperCase()}
                          </span>
                        </div>
                        <div className="mini" style={{ marginTop: 6 }}>
                          <b>Mission:</b> {x.mission}
                        </div>
                        {!!x.dependencies?.length && (
                          <ul className="aiList">
                            {x.dependencies.slice(0, 4).map((d, i) => (
                              <li key={i}>{d}</li>
                            ))}
                          </ul>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {!!aiInsights.execute_plan?.length && (
                <div className="aiBlock">
                  <div className="aiBlockTitle">Execute Plan</div>
                  <div className="aiOps">
                    {aiInsights.execute_plan.map((p, idx) => {
                      const href = dashboardHref(p.dashboard);
                      return (
                        <div className="aiOp" key={idx}>
                          <div className="aiOpHead">
                            <div className="aiOpTitle">
                              {p.priority} - {p.action}
                            </div>
                            <span className={`aiImpact ${p.priority === "P1" ? "high" : p.priority === "P2" ? "medium" : "low"}`}>
                              {p.dashboard}
                            </span>
                          </div>
                          <div className="mini" style={{ marginTop: 6 }}>
                            <b>Rationale:</b> {p.rationale}
                          </div>
                          <div className="mini" style={{ marginTop: 6 }}>
                            <b>Trigger:</b> {p.trigger_metric}
                          </div>
                          <div style={{ marginTop: 10 }}>
                            {href ? (
                              <Link className="btn btnPrimary moduleBtn" href={href}>
                                Execute in Dashboard
                              </Link>
                            ) : (
                              <button className="btn moduleBtn" disabled>
                                Pending setup
                              </button>
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
            </div>
          ) : (
            <div className="aiPlaceholder mini">
              Ejecuta el análisis CEO para orquestar decisiones entre todos los dashboards.
            </div>
          )}

          <div style={{ marginTop: 14 }}>
            <AiAgentChatPanel
              agent="overview"
              title="CEO Agent Chat"
              context={{
                board_meeting_mode: boardMeetingMode,
                range: data?.range || null,
                executive: data?.executive || null,
                modules: data?.modules || null,
              }}
            />
          </div>
        </div>
      </section>
    </div>
  );
}
