"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";

import AdsTrendChart from "@/components/AdsTrendChart";
import AiAgentChatPanel from "@/components/AiAgentChatPanel";

type RangePreset =
  | "last_7_days"
  | "last_28_days"
  | "last_month"
  | "last_quarter"
  | "last_6_months"
  | "last_year"
  | "custom";

type AdsMetric =
  | "impressions"
  | "clicks"
  | "conversions"
  | "cost"
  | "avgCpc"
  | "ctr";

function norm(v: any) {
  return String(v ?? "").trim();
}
function num(v: any) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function fmtInt(v: any) {
  const n = num(v);
  return n.toLocaleString();
}
function fmtPct01(v: any) {
  // expects 0..1
  const n = num(v);
  return `${(n * 100).toFixed(2)}%`;
}
function fmtPct100(v: any) {
  // expects 0..100
  const n = num(v);
  return `${n.toFixed(2)}%`;
}
function fmtMoney(v: any) {
  const n = num(v);
  return n.toLocaleString(undefined, { style: "currency", currency: "USD" });
}
function fmtDeltaPct(x: any) {
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  const sign = n > 0 ? "+" : "";
  return `${sign}${(n * 100).toFixed(1)}%`;
}
function deltaClass(pct: any, opts?: { invert?: boolean }) {
  const n = Number(pct);
  if (!Number.isFinite(n)) return "";
  const invert = !!opts?.invert;
  if (!invert) return n < 0 ? "deltaDown" : "deltaUp";
  return n > 0 ? "deltaDown" : "deltaUp";
}

function metricLabel(m: AdsMetric) {
  if (m === "impressions") return "Impressions";
  if (m === "clicks") return "Clicks";
  if (m === "conversions") return "Conversions";
  if (m === "cost") return "Cost";
  if (m === "avgCpc") return "Avg CPC";
  return "CTR";
}
function metricUnitHint(m: AdsMetric) {
  if (m === "cost" || m === "avgCpc") return "$";
  if (m === "ctr") return "%";
  return "count";
}

export default function AdsDashboardPage() {
  const [loading, setLoading] = useState(false);
  const [hardRefreshing, setHardRefreshing] = useState(false);
  const [err, setErr] = useState("");

  const [preset, setPreset] = useState<RangePreset>("last_28_days");
  const [customStart, setCustomStart] = useState("");
  const [customEnd, setCustomEnd] = useState("");

  const [compareOn, setCompareOn] = useState(true);

  const [trendMode, setTrendMode] = useState<"day" | "week" | "month">("day");
  const [metric, setMetric] = useState<AdsMetric>("cost");

  const [data, setData] = useState<any>(null);

  function buildParams(force: boolean) {
    const p = new URLSearchParams();
    p.set("range", preset);

    if (preset === "custom") {
      if (customStart) p.set("start", customStart);
      if (customEnd) p.set("end", customEnd);
    }

    if (compareOn) p.set("compare", "1");
    if (force) p.set("force", "1");
    p.set("v", String(Date.now()));
    return p.toString();
  }

  async function load(opts?: { force?: boolean }) {
    const force = !!opts?.force;
    setErr("");
    setLoading(true);

    try {
      const syncRes = await fetch(
        `/api/dashboard/ads/sync?${buildParams(force)}`,
        {
          cache: "no-store",
        },
      );
      const syncJson = await syncRes.json();
      if (!syncRes.ok || !syncJson?.ok) {
        throw new Error(syncJson?.error || `SYNC HTTP ${syncRes.status}`);
      }

      const joinRes = await fetch(
        `/api/dashboard/ads/join?${buildParams(false)}`,
        {
          cache: "no-store",
        },
      );
      const joinJson = await joinRes.json();
      if (!joinRes.ok || !joinJson?.ok) {
        throw new Error(joinJson?.error || `JOIN HTTP ${joinRes.status}`);
      }

      setData(joinJson);
    } catch (e: any) {
      setData(null);
      setErr(e?.message || "Failed to load Google Ads dashboard");
    } finally {
      setLoading(false);
      setHardRefreshing(false);
    }
  }

  useEffect(() => {
    if (preset !== "custom") load();
    else if (customStart && customEnd) load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [preset, customStart, customEnd, compareOn]);

  const summary = data?.summaryOverall || {
    impressions: 0,
    clicks: 0,
    cost: 0,
    conversions: 0,
    convValue: 0,
    ctr: 0, // could be 0..1 or 0..100 depending on your join
    avgCpc: 0,
    cpa: 0,
    roas: 0,
    generatedAt: null,
    startDate: null,
    endDate: null,
  };

  const compare = data?.compare || null;

  // Normalize CTR display:
  // If your backend returns ctr in 0..1 => use fmtPct01
  // If returns 0..100 => use fmtPct100
  // We'll auto-detect: if ctr > 1.5 assume it's 0..100.
  const ctrText = useMemo(() => {
    const c = num(summary.ctr);
    return c > 1.5 ? fmtPct100(c) : fmtPct01(c);
  }, [summary.ctr]);

  const startDate = summary?.startDate || data?.meta?.startDate || null;
  const endDate = summary?.endDate || data?.meta?.endDate || null;

  const trend = (data?.trend || []) as any[];
  const topCampaigns = (data?.topCampaigns || []) as any[];
  const topKeywords = (data?.topKeywords || []) as any[];
  const searchTerms = (data?.searchTerms || []) as any[];

  const opportunities = data?.opportunities || {
    winners: [],
    losers: [],
    kwLeaks: [],
    negativeIdeas: [],
    ctrProblems: [],
  };

  // -------------------------
  // Chart adapter (generic)
  // -------------------------
  const trendForChart = useMemo(() => {
    return (trend || []).map((r: any) => {
      const imps = num(r.impressions);
      const clicks = num(r.clicks);
      const cost = num(r.cost);
      const conv = num(r.conversions);
      const avgCpc = num(r.avgCpc);
      const ctr = num(r.ctr);

      const value =
        metric === "impressions"
          ? imps
          : metric === "clicks"
            ? clicks
            : metric === "conversions"
              ? conv
              : metric === "cost"
                ? cost
                : metric === "avgCpc"
                  ? avgCpc
                  : ctr;

      return {
        date: norm(r.date),
        value,
      };
    });
  }, [trend, metric]);

  // -------------------------
  // KPI helpers
  // -------------------------
  const kpi = useMemo(() => {
    const clicks = num(summary.clicks);
    const cost = num(summary.cost);
    const conv = num(summary.conversions);
    const convValue = num(summary.convValue);

    const avgCpc = clicks ? cost / clicks : 0;
    const cpa = conv ? cost / conv : 0;
    const roas = cost ? convValue / cost : 0;

    return { avgCpc, cpa, roas };
  }, [summary.clicks, summary.cost, summary.conversions, summary.convValue]);

  // Compare pills (expect backend compare.pct fields as deltas in 0..1)
  const comparePills = useMemo(() => {
    if (!compareOn || !compare?.pct) return null;
    const pct = compare.pct || {};
    return {
      impressions: pct.impressions ?? null,
      clicks: pct.clicks ?? null,
      cost: pct.cost ?? null,
      conversions: pct.conversions ?? null,
      avgCpc: pct.avgCpc ?? null,
      ctr: pct.ctr ?? null,
      roas: pct.roas ?? null,
    };
  }, [compareOn, compare]);

  return (
    <div className="shell callsDash gaDash">
      <header className="topbar">
        <div className="brand">
          <div className="logo" />
          <div>
            <h1>My Drip Nurse — Google Ads Dashboard</h1>
            <div className="mini" style={{ opacity: 0.8, marginTop: 4 }}>
              Performance + budget efficiency + keyword leakage + Delta
              opportunities. Customer:{" "}
              <b className="mono">{data?.meta?.customerId || "—"}</b>
            </div>
            {data?.meta?.warning ? (
              <div
                className="mini"
                style={{ color: "var(--warning)", marginTop: 6 }}
              >
                ⚠️ {String(data.meta.warning)}
              </div>
            ) : null}
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
              Este filtro afecta KPIs, trend y tablas. Cache se regenera
              automáticamente (stale ≤ 10 min).
            </div>
          </div>

          <div
            className="cardHeaderActions"
            style={{ display: "flex", gap: 10, alignItems: "center" }}
          >
            <button
              className={`smallBtn ${compareOn ? "smallBtnOn" : ""}`}
              onClick={() => setCompareOn((v) => !v)}
              disabled={loading}
              type="button"
              title="Compara contra la ventana previa"
            >
              Compare: {compareOn ? "On" : "Off"}
            </button>

            <button
              className="smallBtn"
              onClick={() => {
                setHardRefreshing(true);
                load({ force: true });
              }}
              disabled={loading}
              type="button"
              title="Forza refresh y recachea Ads"
            >
              {loading && hardRefreshing ? "Hard refresh..." : "Refresh"}
            </button>
          </div>
        </div>

        <div className="cardBody">
          <div className="filtersBar">
            <div className="filtersGroup">
              <div className="filtersLabel">Range</div>
              <div className="rangePills">
                <button
                  className={`smallBtn ${preset === "last_7_days" ? "smallBtnOn" : ""}`}
                  onClick={() => setPreset("last_7_days")}
                  type="button"
                >
                  7 days
                </button>
                <button
                  className={`smallBtn ${preset === "last_28_days" ? "smallBtnOn" : ""}`}
                  onClick={() => setPreset("last_28_days")}
                  type="button"
                >
                  28 days
                </button>

                <span className="filtersDivider" />

                <button
                  className={`smallBtn ${preset === "last_month" ? "smallBtnOn" : ""}`}
                  onClick={() => setPreset("last_month")}
                  type="button"
                >
                  Last month
                </button>
                <button
                  className={`smallBtn ${preset === "last_quarter" ? "smallBtnOn" : ""}`}
                  onClick={() => setPreset("last_quarter")}
                  type="button"
                >
                  Last quarter
                </button>
                <button
                  className={`smallBtn ${preset === "last_6_months" ? "smallBtnOn" : ""}`}
                  onClick={() => setPreset("last_6_months")}
                  type="button"
                >
                  Last 6 months
                </button>
                <button
                  className={`smallBtn ${preset === "last_year" ? "smallBtnOn" : ""}`}
                  onClick={() => setPreset("last_year")}
                  type="button"
                >
                  Last year
                </button>

                <span className="filtersDivider" />

                <button
                  className={`smallBtn ${preset === "custom" ? "smallBtnOn" : ""}`}
                  onClick={() => setPreset("custom")}
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

                {preset === "custom" && (
                  <button
                    className="btn btnPrimary applyBtn"
                    onClick={() => load({ force: true })}
                    disabled={!customStart || !customEnd || loading}
                    type="button"
                    title="Aplica el rango manual y forza refresh"
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

          <div className="filtersRow2">
            <div className="filtersChips">
              <button
                className={`smallBtn ${metric === "cost" ? "smallBtnOn" : ""}`}
                onClick={() => setMetric("cost")}
                type="button"
              >
                Cost
              </button>
              <button
                className={`smallBtn ${metric === "conversions" ? "smallBtnOn" : ""}`}
                onClick={() => setMetric("conversions")}
                type="button"
              >
                Conversions
              </button>
              <button
                className={`smallBtn ${metric === "avgCpc" ? "smallBtnOn" : ""}`}
                onClick={() => setMetric("avgCpc")}
                type="button"
              >
                Avg CPC
              </button>
              <button
                className={`smallBtn ${metric === "ctr" ? "smallBtnOn" : ""}`}
                onClick={() => setMetric("ctr")}
                type="button"
              >
                CTR
              </button>

              <span className="filtersDivider" />

              <button
                className={`smallBtn ${metric === "clicks" ? "smallBtnOn" : ""}`}
                onClick={() => setMetric("clicks")}
                type="button"
              >
                Clicks
              </button>
              <button
                className={`smallBtn ${metric === "impressions" ? "smallBtnOn" : ""}`}
                onClick={() => setMetric("impressions")}
                type="button"
              >
                Impressions
              </button>

              <span className="filtersDivider" />

              <div className="seg">
                <button
                  className={`segBtn ${trendMode === "day" ? "segOn" : ""}`}
                  onClick={() => setTrendMode("day")}
                  type="button"
                >
                  Day
                </button>
                <button
                  className={`segBtn ${trendMode === "week" ? "segOn" : ""}`}
                  onClick={() => setTrendMode("week")}
                  type="button"
                >
                  Week
                </button>
                <button
                  className={`segBtn ${trendMode === "month" ? "segOn" : ""}`}
                  onClick={() => setTrendMode("month")}
                  type="button"
                >
                  Month
                </button>
              </div>

              {summary.generatedAt && (
                <span className="mini" style={{ opacity: 0.8, marginLeft: 8 }}>
                  Last Update:{" "}
                  <b>{new Date(summary.generatedAt).toLocaleString()}</b>
                </span>
              )}
            </div>

            {err ? (
              <div className="mini" style={{ color: "var(--danger)" }}>
                ❌ {err}
              </div>
            ) : (
              <div className="filtersFooter">
                <div className="deltaRow">
                  <span className="deltaHint">
                    <div className="mini" style={{ opacity: 0.9 }}>
                      Range: <b>{summary.startDate || "—"}</b> →{" "}
                      <b>{summary.endDate || "—"}</b> • Impr:{" "}
                      <b>{fmtInt(summary.impressions)}</b> • Clicks:{" "}
                      <b>{fmtInt(summary.clicks)}</b> • Cost:{" "}
                      <b>{fmtMoney(summary.cost)}</b> • CTR: <b>{ctrText}</b> •
                      Conv: <b>{fmtInt(summary.conversions)}</b>
                    </div>
                  </span>

                  {compareOn && comparePills ? (
                    <span className="deltaPills">
                      <span
                        className={`deltaPill ${deltaClass(comparePills.impressions)}`}
                        title="Δ Impressions vs previous window"
                      >
                        Impr:{" "}
                        {comparePills.impressions == null
                          ? "—"
                          : fmtDeltaPct(comparePills.impressions)}
                      </span>
                      <span
                        className={`deltaPill ${deltaClass(comparePills.clicks)}`}
                        title="Δ Clicks vs previous window"
                      >
                        Clicks:{" "}
                        {comparePills.clicks == null
                          ? "—"
                          : fmtDeltaPct(comparePills.clicks)}
                      </span>
                      <span
                        className={`deltaPill ${deltaClass(comparePills.cost, { invert: true })}`}
                        title="Δ Cost vs previous window (lower is better)"
                      >
                        Cost:{" "}
                        {comparePills.cost == null
                          ? "—"
                          : fmtDeltaPct(comparePills.cost)}
                      </span>
                      <span
                        className={`deltaPill ${deltaClass(comparePills.conversions)}`}
                        title="Δ Conversions vs previous window"
                      >
                        Conv:{" "}
                        {comparePills.conversions == null
                          ? "—"
                          : fmtDeltaPct(comparePills.conversions)}
                      </span>
                      <span
                        className={`deltaPill ${deltaClass(comparePills.avgCpc, { invert: true })}`}
                        title="Δ Avg CPC vs previous window (lower is better)"
                      >
                        CPC:{" "}
                        {comparePills.avgCpc == null
                          ? "—"
                          : fmtDeltaPct(comparePills.avgCpc)}
                      </span>
                      <span
                        className={`deltaPill ${deltaClass(comparePills.ctr)}`}
                        title="Δ CTR vs previous window"
                      >
                        CTR:{" "}
                        {comparePills.ctr == null
                          ? "—"
                          : fmtDeltaPct(comparePills.ctr)}
                      </span>
                    </span>
                  ) : null}
                </div>
              </div>
            )}
          </div>
        </div>
      </section>

      {/* Summary */}
      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Summary</h2>
            <div className="cardSubtitle">
              Aquí medimos eficiencia. El objetivo no es “más clicks”, es “más
              conversiones por $”.
            </div>
          </div>
          <div className="badge">{loading ? "loading…" : "ready"}</div>
        </div>

        <div className="cardBody">
          <div className="kpiGrid32">
            <div className="kpi">
              <p className="n">{fmtMoney(summary.cost)}</p>
              <p className="l">Cost</p>
            </div>

            <div className="kpi">
              <p className="n">{fmtInt(summary.clicks)}</p>
              <p className="l">Clicks</p>
            </div>

            <div className="kpi">
              <p className="n">{ctrText}</p>
              <p className="l">CTR</p>
            </div>

            <div className="kpi">
              <p className="n">{fmtMoney(kpi.avgCpc)}</p>
              <p className="l">Avg CPC</p>
            </div>

            <div className="kpi">
              <p className="n">{fmtInt(summary.conversions)}</p>
              <p className="l">Conversions</p>
            </div>

            <div className="kpi">
              <p className="n">{fmtMoney(kpi.cpa)}</p>
              <p className="l">CPA</p>
            </div>

            <div className="kpi">
              <p className="n">{fmtMoney(summary.convValue)}</p>
              <p className="l">Conv. Value</p>
            </div>

            <div className="kpi">
              <p className="n">{kpi.roas.toFixed(2)}x</p>
              <p className="l">ROAS</p>
            </div>
          </div>

          {compareOn && compare ? (
            <div className="mini" style={{ marginTop: 10, opacity: 0.85 }}>
              Compare window: <b>{compare.previous.startDate}</b> →{" "}
              <b>{compare.previous.endDate}</b> vs current{" "}
              <b>{compare.current.startDate}</b> →{" "}
              <b>{compare.current.endDate}</b>
            </div>
          ) : null}

          <div className="mini" style={{ marginTop: 10 }}>
            Lectura Delta: si <b>ROAS</b> sube y <b>CPA</b> baja → escala. Si{" "}
            <b>Cost</b> sube con <b>Conv</b> flat → hay leakage
            (keywords/terms/ads/landing).
          </div>

          <div style={{ marginTop: 14 }}>
            <AdsTrendChart
              trend={trendForChart}
              mode={trendMode}
              startDate={startDate}
              endDate={endDate}
              seriesLabel={metricLabel(metric)}
              unitHint={metricUnitHint(metric)}
            />
          </div>
        </div>
      </section>

      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">AI Strategist (Ads)</h2>
            <div className="cardSubtitle">
              Conversa con el agente de Google Ads para optimizar gasto, CPA, ROAS y expansión por geo-intent.
            </div>
          </div>
          <div className="badge">shared memory</div>
        </div>
        <div className="cardBody">
          <AiAgentChatPanel
            agent="ads"
            title="Ads Agent Chat"
            context={{
              preset,
              customStart,
              customEnd,
              compareOn,
              trendMode,
              metric,
              summary,
              compare,
              opportunities,
              topCampaigns: (topCampaigns || []).slice(0, 20),
              topKeywords: (topKeywords || []).slice(0, 20),
              searchTerms: (searchTerms || []).slice(0, 20),
            }}
          />
        </div>
      </section>

      {/* Opportunities */}
      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Opportunities (Delta-Aware)</h2>
            <div className="cardSubtitle">
              Winners/Losers + leakage + negativos sugeridos. Esto es lo primero
              que arreglas antes de “subir presupuesto”.
            </div>
          </div>
          <div className="badge">ops</div>
        </div>

        <div className="cardBody">
          <div className="gscTopGrid">
            <div className="gscTopCard">
              <div className="gscTopHead">
                <div className="gscTopTitle">Winners (scale candidates)</div>
              </div>

              <div className="tableWrap tableScrollX">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="th">Campaign</th>
                      <th className="th">Cost</th>
                      <th className="th">Conv</th>
                      <th className="th">CPA</th>
                      <th className="th">ROAS</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(opportunities.winners || [])
                      .slice(0, 15)
                      .map((r: any, i: number) => (
                        <tr key={i} className="tr">
                          <td className="td">
                            <b className="mono">{r.campaign}</b>
                            <div className="mini" style={{ opacity: 0.7 }}>
                              clicks {fmtInt(r.clicks)} • impr{" "}
                              {fmtInt(r.impressions)}
                            </div>
                          </td>
                          <td className="td">{fmtMoney(r.cost)}</td>
                          <td className="td">{fmtInt(r.conversions)}</td>
                          <td className="td">
                            {r.cpa == null ? "—" : fmtMoney(r.cpa)}
                          </td>
                          <td className="td">
                            {r.roas == null
                              ? "—"
                              : `${Number(r.roas).toFixed(2)}x`}
                          </td>
                        </tr>
                      ))}
                    {!(opportunities.winners || []).length ? (
                      <tr className="tr">
                        <td
                          className="td"
                          colSpan={5}
                          style={{ opacity: 0.75 }}
                        >
                          No winners detectados en este rango.
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="gscTopCard">
              <div className="gscTopHead">
                <div className="gscTopTitle">
                  Losers (fix / pause / tighten)
                </div>
              </div>

              <div className="tableWrap tableScrollX">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="th">Campaign</th>
                      <th className="th">Cost</th>
                      <th className="th">Clicks</th>
                      <th className="th">Conv</th>
                      <th className="th">Hint</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(opportunities.losers || [])
                      .slice(0, 15)
                      .map((r: any, i: number) => (
                        <tr key={i} className="tr">
                          <td className="td">
                            <b className="mono">{r.campaign}</b>
                            <div className="mini" style={{ opacity: 0.7 }}>
                              impr {fmtInt(r.impressions)} • CTR{" "}
                              {num(r.ctr) > 1.5
                                ? fmtPct100(r.ctr)
                                : fmtPct01(r.ctr)}
                            </div>
                          </td>
                          <td className="td">{fmtMoney(r.cost)}</td>
                          <td className="td">{fmtInt(r.clicks)}</td>
                          <td className="td">{fmtInt(r.conversions)}</td>
                          <td className="td" style={{ opacity: 0.8 }}>
                            tighten terms • test landing • split adgroup
                          </td>
                        </tr>
                      ))}
                    {!(opportunities.losers || []).length ? (
                      <tr className="tr">
                        <td
                          className="td"
                          colSpan={5}
                          style={{ opacity: 0.75 }}
                        >
                          No losers detectados en este rango.
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div className="gscTopGrid" style={{ marginTop: 14 }}>
            <div className="gscTopCard">
              <div className="gscTopHead">
                <div className="gscTopTitle">Keyword leaks (spend no conv)</div>
              </div>

              <div className="tableWrap tableScrollX">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="th">Keyword</th>
                      <th className="th">Cost</th>
                      <th className="th">Clicks</th>
                      <th className="th">Conv</th>
                      <th className="th">Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(opportunities.kwLeaks || [])
                      .slice(0, 20)
                      .map((r: any, i: number) => (
                        <tr key={i} className="tr">
                          <td className="td mono">{r.keyword}</td>
                          <td className="td">{fmtMoney(r.cost)}</td>
                          <td className="td">{fmtInt(r.clicks)}</td>
                          <td className="td">{fmtInt(r.conversions)}</td>
                          <td className="td" style={{ opacity: 0.8 }}>
                            add negatives • change match • split intent
                          </td>
                        </tr>
                      ))}
                    {!(opportunities.kwLeaks || []).length ? (
                      <tr className="tr">
                        <td
                          className="td"
                          colSpan={5}
                          style={{ opacity: 0.75 }}
                        >
                          No keyword leaks detectados.
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="gscTopCard">
              <div className="gscTopHead">
                <div className="gscTopTitle">Negative ideas (search terms)</div>
              </div>

              <div className="tableWrap tableScrollX">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="th">Search term</th>
                      <th className="th">Cost</th>
                      <th className="th">Clicks</th>
                      <th className="th">Conv</th>
                      <th className="th">Why</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(opportunities.negativeIdeas || [])
                      .slice(0, 20)
                      .map((r: any, i: number) => (
                        <tr key={i} className="tr">
                          <td className="td mono">{r.term}</td>
                          <td className="td">{fmtMoney(r.cost)}</td>
                          <td className="td">{fmtInt(r.clicks)}</td>
                          <td className="td">{fmtInt(r.conversions)}</td>
                          <td className="td" style={{ opacity: 0.8 }}>
                            irrelevant / low-intent
                          </td>
                        </tr>
                      ))}
                    {!(opportunities.negativeIdeas || []).length ? (
                      <tr className="tr">
                        <td
                          className="td"
                          colSpan={5}
                          style={{ opacity: 0.75 }}
                        >
                          No negative ideas detectadas.
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div className="mini" style={{ marginTop: 10, opacity: 0.75 }}>
            Próximo nivel: combinar <b>GSC (queries por estado)</b> +{" "}
            <b>Keyword Planner (CPC/competition/volume)</b> para generar “Delta
            Expansion Plans” por estado/county/ciudad.
          </div>
        </div>
      </section>

      {/* Tables */}
      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Campaigns, Keywords, Search Terms</h2>
            <div className="cardSubtitle">
              Aquí ves el inventario real de performance. Lo usamos para crear
              “estructura Delta” (campaña → adgroups → intents → landing).
            </div>
          </div>
          <div className="badge">top</div>
        </div>

        <div className="cardBody">
          <div className="gscTopGrid">
            <div className="gscTopCard">
              <div className="gscTopHead">
                <div className="gscTopTitle">Top Campaigns</div>
              </div>

              <div className="tableWrap tableScrollX">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="th">Campaign</th>
                      <th className="th">Cost</th>
                      <th className="th">Impr</th>
                      <th className="th">Clicks</th>
                      <th className="th">CTR</th>
                      <th className="th">Conv</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topCampaigns.slice(0, 30).map((r: any, i: number) => (
                      <tr key={i} className="tr">
                        <td className="td mono">{r.campaign}</td>
                        <td className="td">{fmtMoney(r.cost)}</td>
                        <td className="td">{fmtInt(r.impressions)}</td>
                        <td className="td">{fmtInt(r.clicks)}</td>
                        <td className="td">
                          {num(r.ctr) > 1.5
                            ? fmtPct100(r.ctr)
                            : fmtPct01(r.ctr)}
                        </td>
                        <td className="td">{fmtInt(r.conversions)}</td>
                      </tr>
                    ))}
                    {!topCampaigns.length ? (
                      <tr className="tr">
                        <td
                          className="td"
                          colSpan={6}
                          style={{ opacity: 0.75 }}
                        >
                          No campaign data en este rango. Pulsa Refresh.
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="gscTopCard">
              <div className="gscTopHead">
                <div className="gscTopTitle">Top Keywords</div>
              </div>

              <div className="tableWrap tableScrollX">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="th">Keyword</th>
                      <th className="th">Cost</th>
                      <th className="th">Clicks</th>
                      <th className="th">Conv</th>
                      <th className="th">Avg CPC</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topKeywords.slice(0, 30).map((r: any, i: number) => (
                      <tr key={i} className="tr">
                        <td className="td mono">{r.keyword}</td>
                        <td className="td">{fmtMoney(r.cost)}</td>
                        <td className="td">{fmtInt(r.clicks)}</td>
                        <td className="td">{fmtInt(r.conversions)}</td>
                        <td className="td">{fmtMoney(r.avgCpc)}</td>
                      </tr>
                    ))}
                    {!topKeywords.length ? (
                      <tr className="tr">
                        <td
                          className="td"
                          colSpan={5}
                          style={{ opacity: 0.75 }}
                        >
                          No keyword data en este rango.
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div className="gscTopGrid" style={{ marginTop: 14 }}>
            <div className="gscTopCard">
              <div className="gscTopHead">
                <div className="gscTopTitle">Top Search Terms</div>
              </div>

              <div className="tableWrap tableScrollX">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="th">Term</th>
                      <th className="th">Cost</th>
                      <th className="th">Clicks</th>
                      <th className="th">Conv</th>
                      <th className="th">Hint</th>
                    </tr>
                  </thead>
                  <tbody>
                    {searchTerms.slice(0, 40).map((r: any, i: number) => (
                      <tr key={i} className="tr">
                        <td className="td mono">{r.term}</td>
                        <td className="td">{fmtMoney(r.cost)}</td>
                        <td className="td">{fmtInt(r.clicks)}</td>
                        <td className="td">{fmtInt(r.conversions)}</td>
                        <td className="td" style={{ opacity: 0.8 }}>
                          {num(r.conversions)
                            ? "keep / expand"
                            : "negative candidate"}
                        </td>
                      </tr>
                    ))}
                    {!searchTerms.length ? (
                      <tr className="tr">
                        <td
                          className="td"
                          colSpan={5}
                          style={{ opacity: 0.75 }}
                        >
                          No search term data en este rango.
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>

              <div className="mini" style={{ marginTop: 10, opacity: 0.75 }}>
                Nota: Search Terms es donde más rápido limpias el ROAS. Si esto
                no existe aún en tu sync, lo añadimos con GAQL.
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
