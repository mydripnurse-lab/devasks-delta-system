// control-tower/src/app/dashboard/gsc/page.tsx
"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import dynamic from "next/dynamic";
import AiAgentChatPanel from "@/components/AiAgentChatPanel";

const UsaChoroplethProgressMap = dynamic(
  () => import("@/components/UsaChoroplethProgressMap"),
  { ssr: false },
);

const GSCTrendChart = dynamic(() => import("@/components/GSCTrendChart"), {
  ssr: false,
});

type RangePreset =
  | "last_7_days"
  | "last_28_days"
  | "last_month"
  | "last_quarter"
  | "last_6_months"
  | "last_year"
  | "custom";
type SearchTab = "gsc" | "bing" | "all";

function fmtInt(x: any) {
  const n = Number(x);
  if (!Number.isFinite(n)) return "0";
  return n.toLocaleString();
}
function fmtPct(x: any) {
  const n = Number(x);
  if (!Number.isFinite(n)) return "0.00%";
  return `${(n * 100).toFixed(2)}%`;
}
function fmtPos(x: any) {
  const n = Number(x);
  if (!Number.isFinite(n) || n <= 0) return "—";
  return n.toFixed(2);
}
function fmtDeltaPct(x: any) {
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  const sign = n > 0 ? "+" : "";
  return `${sign}${(n * 100).toFixed(1)}%`;
}

function isoDateLocal(d: Date) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const da = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${da}`;
}

function addDaysLocal(base: Date, days: number) {
  const d = new Date(base);
  d.setDate(d.getDate() + days);
  return d;
}

function resolveRangeDates(
  preset: RangePreset,
  customStart: string,
  customEnd: string,
) {
  const now = new Date();
  const today = isoDateLocal(now);

  if (preset === "custom") {
    return {
      startDate: customStart || today,
      endDate: customEnd || today,
    };
  }

  if (preset === "last_year") {
    const prevYear = now.getFullYear() - 1;
    return {
      startDate: `${prevYear}-01-01`,
      endDate: `${prevYear}-12-31`,
    };
  }

  if (preset === "last_7_days") {
    return { startDate: isoDateLocal(addDaysLocal(now, -6)), endDate: today };
  }
  if (preset === "last_28_days") {
    return { startDate: isoDateLocal(addDaysLocal(now, -27)), endDate: today };
  }
  if (preset === "last_month") {
    const d = new Date(now);
    d.setMonth(d.getMonth() - 1);
    return { startDate: isoDateLocal(d), endDate: today };
  }
  if (preset === "last_quarter") {
    const d = new Date(now);
    d.setMonth(d.getMonth() - 3);
    return { startDate: isoDateLocal(d), endDate: today };
  }

  const d = new Date(now);
  d.setMonth(d.getMonth() - 6);
  return { startDate: isoDateLocal(d), endDate: today };
}

// For pills: allow "invert" semantics (lower is better: avg position)
function deltaClass(pct: any, opts?: { invert?: boolean }) {
  const n = Number(pct);
  if (!Number.isFinite(n)) return "";
  const invert = !!opts?.invert;

  if (!invert) return n < 0 ? "deltaDown" : "deltaUp";
  return n > 0 ? "deltaDown" : "deltaUp";
}

export default function GscDashboardPage() {
  const [loading, setLoading] = useState(false);
  const [hardRefreshing, setHardRefreshing] = useState(false);
  const [err, setErr] = useState("");
  const [warn, setWarn] = useState("");

  const [preset, setPreset] = useState<RangePreset>("last_28_days");
  const [customStart, setCustomStart] = useState("");
  const [customEnd, setCustomEnd] = useState("");

  const [metric, setMetric] = useState<"impressions" | "clicks">("impressions");
  const [mapSelected, setMapSelected] = useState<string>("");

  const [trendMode, setTrendMode] = useState<"day" | "week" | "month">("day");
  const [compareOn, setCompareOn] = useState(true);

  const [nfTab, setNfTab] = useState<"nationwide" | "funnels">("nationwide");

  const [aiLoading, setAiLoading] = useState(false);
  const [aiErr, setAiErr] = useState("");
  const [aiInsights, setAiInsights] = useState<any>(null);

  const [data, setData] = useState<any>(null);

  const [showTopKeywords, setShowTopKeywords] = useState(true);
  const [searchTab, setSearchTab] = useState<SearchTab>("all");

  function qs() {
    const p = new URLSearchParams();
    p.set("range", preset);
    const resolved = resolveRangeDates(preset, customStart, customEnd);
    p.set("start", resolved.startDate);
    p.set("end", resolved.endDate);
    if (mapSelected) p.set("state", mapSelected);
    if (compareOn) p.set("compare", "1");
    return p.toString();
  }

  function buildSyncParams(force: boolean) {
    const p = new URLSearchParams();
    p.set("range", preset);
    const resolved = resolveRangeDates(preset, customStart, customEnd);
    p.set("start", resolved.startDate);
    p.set("end", resolved.endDate);
    if (force) p.set("force", "1");

    // ✅ IMPORTANT: tell sync we need previous-window trend when compare is on
    if (compareOn) p.set("compare", "1");

    p.set("v", String(Date.now()));
    return p.toString();
  }

  function buildJoinUrl(forceCatalog: boolean) {
    const base = qs();
    const p = new URLSearchParams(base);
    if (forceCatalog) p.set("force", "1");
    p.set("v", String(Date.now()));
    if (searchTab === "bing") return `/api/dashboard/bing/join?${p.toString()}`;
    if (searchTab === "all")
      return `/api/dashboard/search-performance/join?${p.toString()}`;
    return `/api/dashboard/gsc/join?${p.toString()}`;
  }

  async function load(opts?: { force?: boolean }) {
    const force = !!opts?.force || preset === "custom";

    setErr("");
    setWarn("");
    setLoading(true);
    setAiErr("");
    setAiInsights(null);

    try {
      const syncWarnings: string[] = [];
      const syncTargets =
        searchTab === "all"
          ? ["/api/dashboard/gsc/sync", "/api/dashboard/bing/sync"]
          : [searchTab === "bing" ? "/api/dashboard/bing/sync" : "/api/dashboard/gsc/sync"];

      for (const syncTarget of syncTargets) {
        const syncRes = await fetch(
          `${syncTarget}?${buildSyncParams(force)}`,
          {
            cache: "no-store",
          },
        );
        const syncJson = await syncRes.json();
        if (!syncRes.ok || !syncJson?.ok) {
          syncWarnings.push(syncJson?.error || `SYNC HTTP ${syncRes.status}`);
        } else if (syncJson?.stale || syncJson?.warning) {
          syncWarnings.push(
            `${syncTarget.includes("/bing/") ? "Bing" : "GSC"} stale snapshot: ${syncJson?.warning || "provider unavailable"}`,
          );
        }
      }

      const res = await fetch(buildJoinUrl(force), { cache: "no-store" });
      const json = await res.json();
      if (!res.ok || !json?.ok)
        throw new Error(json?.error || `JOIN HTTP ${res.status}`);

      setData(json);
      if (syncWarnings.length) {
        console.warn("Search Performance sync warnings:", syncWarnings);
        setWarn(syncWarnings.join(" | "));
      }
    } catch (e: any) {
      setData(null);
      setErr(e?.message || "Failed to load GSC dashboard");
      setWarn("");
    } finally {
      setLoading(false);
      setHardRefreshing(false);
    }
  }

  useEffect(() => {
    if (preset !== "custom") load();
    else if (customStart && customEnd) load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [preset, customStart, customEnd, compareOn, searchTab]);

  useEffect(() => {
    if (data) load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mapSelected]);

  function clearSelection() {
    setMapSelected("");
  }

  const summaryOverall = data?.summaryOverall || {
    impressions: 0,
    clicks: 0,
    ctr: 0,
    position: 0,
    pagesCounted: 0,
    generatedAt: null,
    startDate: null,
    endDate: null,
  };

  const summaryFiltered = data?.summaryFiltered || summaryOverall;
  const summary = mapSelected ? summaryFiltered : summaryOverall;

  const compare = data?.compare || null;
  const stateRows = (data?.stateRows || []) as Array<any>;

  const mapRows = useMemo(() => {
    const rows = stateRows.filter((r) => r.state !== "__unknown");
    const values = rows.map((r) =>
      metric === "impressions"
        ? Number(r.impressions || 0)
        : Number(r.clicks || 0),
    );
    const max = Math.max(...values, 1);

    return rows.map((r) => {
      const v =
        metric === "impressions"
          ? Number(r.impressions || 0)
          : Number(r.clicks || 0);
      return {
        state: r.state,
        counties: { total: max, ready: v, domainsActive: 0 },
        cities: { total: 0, ready: 0, domainsActive: 0 },
        __value: v,
      };
    });
  }, [stateRows, metric]);

  const selectedStateRow = useMemo(() => {
    if (!mapSelected) return null;
    return stateRows.find((r) => String(r.state) === mapSelected) || null;
  }, [mapSelected, stateRows]);

  const topQueries = data?.top?.queries || [];
  const topPages = data?.top?.pages || [];

  const summaryNationwide = data?.summaryNationwide || {
    impressions: 0,
    clicks: 0,
    ctr: 0,
    position: 0,
    pagesCounted: 0,
    label: "Nationwide / Home Page",
    rootHost: "",
  };

  const funnelRows = (data?.funnels || []) as any[];
  const summaryFunnels = data?.summaryFunnels || {
    impressions: 0,
    clicks: 0,
    ctr: 0,
    position: 0,
    pagesCounted: 0,
    label: "Funnels (non-Delta subdomains)",
  };

  const trend = (data?.trend || []) as any[];
  const trendFiltered = (data?.trendFiltered || []) as any[];
  const startDate = summary?.startDate || data?.meta?.startDate || null;
  const endDate = summary?.endDate || data?.meta?.endDate || null;

  const keywordsCount = useMemo(() => {
    if (mapSelected) return selectedStateRow?.keywordsCount ?? 0;
    return data?.keywordsOverall ?? 0;
  }, [data, mapSelected, selectedStateRow]);

  const topKeywords = useMemo(() => {
    if (!data) return [];
    if (mapSelected) return (data?.topKeywordsFiltered || []).slice(0, 10);
    return (data?.topKeywordsOverall || []).slice(0, 10);
  }, [data, mapSelected]);

  const comparePills = useMemo(() => {
    if (!compareOn || !compare?.pct) return null;
    const pct = compare.pct || {};
    return {
      impressions: pct.impressions ?? null,
      clicks: pct.clicks ?? null,
      ctr: pct.ctr ?? null,
      position: pct.position ?? null,
    };
  }, [compareOn, compare]);

  async function generateInsights() {
    setAiErr("");
    setAiLoading(true);
    setAiInsights(null);

    try {
      const payload = {
        source: searchTab,
        range: {
          preset,
          start: summary.startDate,
          end: summary.endDate,
          generatedAt: summary.generatedAt,
        },
        scope: {
          state: mapSelected || null,
          metric,
        },
        summary,
        keywordsCount: keywordsCount ?? null,
        nationwide: summaryNationwide,
        funnels: {
          summary: summaryFunnels,
          rows: funnelRows.slice(0, 25),
        },
        compare: compareOn ? compare : null,
        trend: {
          mode: trendMode,
          note: "trendFiltered is range-aligned; comparison uses previous window based on trend only.",
        },
        top: {
          queries: topQueries.slice(0, 25),
          pages: topPages.slice(0, 25),
        },
        states: stateRows.slice(0, 20),
        searchPerformance: {
          note:
            "__unknown = páginas fuera del patrón Delta o sin match en catálogo scripts/out.",
        },
        debug: data?.debug || null,
      };

      const insightsEndpoint =
        searchTab === "bing"
          ? "/api/dashboard/bing/insights"
          : searchTab === "all"
            ? "/api/dashboard/search-performance/insights"
            : "/api/dashboard/gsc/insights";

      const res = await fetch(insightsEndpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const json = await res.json();
      if (!res.ok || !json?.ok)
        throw new Error(json?.error || "Failed to generate insights");
      setAiInsights(json.insights);
    } catch (e: any) {
      setAiErr(e?.message || "Failed to generate insights");
    } finally {
      setAiLoading(false);
    }
  }

  return (
    <div className="shell callsDash">
      <header className="topbar">
        <div className="brand">
          <div className="logo" />
          <div>
            <h1>My Drip Nurse — Search Performance Dashboard</h1>
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
              Este filtro afecta el mapa, KPIs, trend y tablas. Cache se
              regenera automáticamente (stale ≤ 10 min).
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
              title="Compara contra la ventana previa (trend-based)"
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
              title="Forza sync con Google + fuerza reload del catálogo scripts/out"
            >
              {loading && hardRefreshing ? "Hard refresh..." : "Refresh"}
            </button>
          </div>
        </div>

          <div className="cardBody">
          {warn && !err ? (
            <div style={{ color: "#f8c15c", marginBottom: 10 }}>
              ⚠ {warn}
            </div>
          ) : null}
          <div style={{ display: "flex", gap: 8, marginBottom: 12, flexWrap: "wrap" }}>
            <button
              className={`smallBtn ${searchTab === "gsc" ? "smallBtnOn" : ""}`}
              onClick={() => setSearchTab("gsc")}
              type="button"
            >
              Google Search Console
            </button>
            <button
              className={`smallBtn ${searchTab === "bing" ? "smallBtnOn" : ""}`}
              onClick={() => setSearchTab("bing")}
              type="button"
            >
              Bing Webmaster
            </button>
            <button
              className={`smallBtn ${searchTab === "all" ? "smallBtnOn" : ""}`}
              onClick={() => setSearchTab("all")}
              type="button"
            >
              All
            </button>
          </div>

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
                className={`smallBtn ${metric === "impressions" ? "smallBtnOn" : ""}`}
                type="button"
                onClick={() => setMetric("impressions")}
              >
                Impressions
              </button>
              <button
                className={`smallBtn ${metric === "clicks" ? "smallBtnOn" : ""}`}
                type="button"
                onClick={() => setMetric("clicks")}
              >
                Clicks
              </button>

              <span className="filtersDivider" />

              <button
                className="smallBtn"
                onClick={clearSelection}
                type="button"
              >
                Clear state
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
                      <b>{fmtInt(summary.clicks)}</b> • CTR:{" "}
                      <b>{fmtPct(summary.ctr)}</b> • Avg pos:{" "}
                      <b>{fmtPos(summary.position)}</b>
                      {mapSelected ? (
                        <>
                          {" "}
                          • State: <b>{mapSelected}</b>
                        </>
                      ) : null}
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
                        className={`deltaPill ${deltaClass(comparePills.ctr)}`}
                        title="Δ CTR vs previous window"
                      >
                        CTR:{" "}
                        {comparePills.ctr == null
                          ? "—"
                          : fmtDeltaPct(comparePills.ctr)}
                      </span>
                      <span
                        className={`deltaPill ${deltaClass(comparePills.position, { invert: true })}`}
                        title="Δ Avg position (lower is better)"
                      >
                        Pos:{" "}
                        {comparePills.position == null
                          ? "—"
                          : fmtDeltaPct(comparePills.position)}
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
              {searchTab === "all"
                ? "KPIs combinados de Google + Bing"
                : searchTab === "bing"
                  ? "KPIs de Bing Webmaster"
                  : "KPIs de Google Search Console"}{" "}
              del rango seleccionado{" "}
              {mapSelected ? "(filtrado por estado)" : ""}.
            </div>
          </div>
          <div className="badge">{loading ? "loading…" : "ready"}</div>
        </div>

        <div className="cardBody">
          <div className="kpiGrid32">
            <div className="kpi">
              <p className="n">{fmtInt(summary.impressions)}</p>
              <p className="l">
                Impressions{" "}
                {compareOn && compare?.pct?.impressions != null ? (
                  <span
                    className={`delta ${compare.pct.impressions >= 0 ? "deltaUp" : "deltaDown"}`}
                  >
                    {fmtDeltaPct(compare.pct.impressions)}
                  </span>
                ) : null}
              </p>
            </div>

            <div className="kpi">
              <p className="n">{fmtInt(summary.clicks)}</p>
              <p className="l">
                Clicks{" "}
                {compareOn && compare?.pct?.clicks != null ? (
                  <span
                    className={`delta ${compare.pct.clicks >= 0 ? "deltaUp" : "deltaDown"}`}
                  >
                    {fmtDeltaPct(compare.pct.clicks)}
                  </span>
                ) : null}
              </p>
            </div>

            <div className="kpi">
              <p className="n">{fmtPct(summary.ctr)}</p>
              <p className="l">
                CTR{" "}
                {compareOn && compare?.pct?.ctr != null ? (
                  <span
                    className={`delta ${compare.pct.ctr >= 0 ? "deltaUp" : "deltaDown"}`}
                  >
                    {fmtDeltaPct(compare.pct.ctr)}
                  </span>
                ) : null}
              </p>
            </div>

            <div className="kpi">
              <p className="n">{fmtPos(summary.position)}</p>
              <p className="l">
                Avg position {" "}
                {compareOn && compare?.pct?.position != null ? (
                  <span
                    className={`delta ${compare.pct.position > 0 ? "deltaDown" : "deltaUp"}`}
                  >
                    {fmtDeltaPct(compare.pct.position)}
                  </span>
                ) : null}
              </p>
            </div>

            <div className="kpi">
              <p className="n">{fmtInt(keywordsCount)}</p>
              <p className="l">Keywords</p>
            </div>

            <div className="kpi">
              <p className="n">{fmtInt(summary.pagesCounted)}</p>
              <p className="l">Pages counted</p>
            </div>
          </div>

          {compareOn && compare ? (
            <div className="mini" style={{ marginTop: 10, opacity: 0.85 }}>
              Compare window: <b>{compare.previous.startDate}</b> →{" "}
              <b>{compare.previous.endDate}</b> vs current{" "}
              <b>{compare.current.startDate}</b> →{" "}
              <b>{compare.current.endDate}</b> (trend-based)
            </div>
          ) : null}

          <div className="mini" style={{ marginTop: 10 }}>
            Lectura estratégica: <b>Impressions por estado</b> = dónde Google te
            está creyendo. <b>CTR bajo + pos 8–20</b> = quick wins
            (title/meta/snippet + enlaces internos).
          </div>

          <div style={{ marginTop: 14 }}>
            <GSCTrendChart
              trend={(trendFiltered.length ? trendFiltered : trend) as any}
              metric={metric}
              mode={trendMode}
              startDate={startDate}
              endDate={endDate}
            />
          </div>
        </div>
      </section>

      {/* Map + panel + AI */}
      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Performance by state</h2>
            <div className="cardSubtitle">
              Mapa + drilldown real. “__unknown” no sale en mapa pero sí en
              tabla.
            </div>
          </div>

          <div className="cardHeaderActions">
            <button className="smallBtn" onClick={clearSelection} type="button">
              Clear
            </button>
          </div>
        </div>

        <div className="cardBody">
          <div className="mapGrid">
            {/* <div className="mapCard"> */}
              {/* <div className="mapCardTop">
                <div>
                  <div className="mapCardTitle">
                    US {metric === "impressions" ? "Impressions" : "Clicks"} Map
                  </div>
                  <div className="mini" style={{ marginTop: 6 }}>
                    Tip: Click en un estado → filtra KPIs + Trend + Top Pages +
                    tabla.
                  </div>
                </div>
              </div> */}

              <div className="mapFrame mapFrameXL">
                <UsaChoroplethProgressMap
                  rows={mapRows as any}
                  metric={metric as any}
                  labelMode={"value" as any}
                  valueField={"__value" as any}
                  selectedState={mapSelected}
                  onPick={(name: string) => setMapSelected(String(name))}
                />
              </div>
            {/* </div> */}

            <aside className="statePanel">
              <div className="statePanelTop">
                <div className="mini" style={{ opacity: 0.85 }}>
                  State analytics
                </div>

                {mapSelected ? (
                  <div className="stateHead">
                    <div className="stateName">{mapSelected}</div>
                    <div className="statePill">
                      {metric === "impressions"
                        ? fmtInt(selectedStateRow?.impressions || 0)
                        : fmtInt(selectedStateRow?.clicks || 0)}{" "}
                      {metric}
                    </div>
                  </div>
                ) : (
                  <div className="mini" style={{ marginTop: 10 }}>
                    Click a state to drill down.
                  </div>
                )}
              </div>

              <div className="stateCards">
                <div className="stateKpi">
                  <div className="mini">Impressions</div>
                  <div className="stateKpiN">
                    {fmtInt(
                      selectedStateRow?.impressions ??
                        summaryOverall.impressions,
                    )}
                  </div>
                  <div className="mini" style={{ opacity: 0.85 }}>
                    Demand proxy
                  </div>
                </div>

                <div className="stateKpi">
                  <div className="mini">Clicks</div>
                  <div className="stateKpiN">
                    {fmtInt(selectedStateRow?.clicks ?? summaryOverall.clicks)}
                  </div>
                  <div className="mini" style={{ opacity: 0.85 }}>
                    Traffic delivered
                  </div>
                </div>

                <div className="stateKpi">
                  <div className="mini">CTR</div>
                  <div className="stateKpiN">
                    {fmtPct(selectedStateRow?.ctr ?? summaryOverall.ctr)}
                  </div>
                  <div className="mini" style={{ opacity: 0.85 }}>
                    Snippet quality
                  </div>
                </div>

                <div className="stateKpi">
                  <div className="mini">Avg position</div>
                  <div className="stateKpiN">
                    {fmtPos(
                      selectedStateRow?.position ?? summaryOverall.position,
                    )}
                  </div>
                  <div className="mini" style={{ opacity: 0.85 }}>
                    Ranking health
                  </div>
                </div>

                {/* ✅ Keywords fixed (real, state-aware) */}
                <div className="stateKpi">
                  <div className="mini">Keywords</div>
                  <div className="stateKpiN">{fmtInt(keywordsCount)}</div>
                  <div className="mini" style={{ opacity: 0.85 }}>
                    {mapSelected
                      ? "Queries in this state"
                      : "Queries overall (Delta pages)"}
                  </div>
                </div>
              </div>

              {/* ✅ Top keywords list (pro) */}
              <div className="aiCard" style={{ marginTop: 12 }}>
                <div className="aiCardTop">
                  <div>
                    <div className="aiTitle">
                      Top Keywords{" "}
                      <span className="mini" style={{ opacity: 0.85 }}>
                        {mapSelected ? `(${mapSelected})` : "(Overall)"}
                      </span>
                    </div>
                    <div
                      className="mini"
                      style={{ opacity: 0.85, marginTop: 4 }}
                    >
                      Basado en query+page (filtrable por estado). Top por
                      impressions.
                    </div>
                  </div>

                  <button
                    className="smallBtn aiBtn"
                    onClick={() => setShowTopKeywords((v) => !v)}
                    type="button"
                    disabled={!topKeywords?.length}
                  >
                    {showTopKeywords ? "Hide" : "Show"}
                  </button>
                </div>

                {showTopKeywords ? (
                  <div className="aiBody" style={{ paddingTop: 10 }}>
                    {topKeywords?.length ? (
                      <ul className="aiList" style={{ marginTop: 0 }}>
                        {topKeywords.slice(0, 8).map((k: any, i: number) => (
                          <li key={i}>
                            <span className="mono">{k.query}</span>{" "}
                            <span style={{ opacity: 0.7 }}>
                              • {fmtInt(k.impressions)} impr
                            </span>
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <div className="mini" style={{ opacity: 0.8 }}>
                        No keyword data (qp) en este rango. Pulsa Refresh.
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="aiPlaceholder">
                    <div className="mini" style={{ opacity: 0.85 }}>
                      Oculto. Toggle “Show” para ver el top.
                    </div>
                  </div>
                )}
              </div>

              {/* AI Strategist */}
              <div className="aiCard" id="ai-playbook">
                <div className="aiCardTop">
                  <div>
                    <div className="aiTitle">
                      {searchTab === "all"
                        ? "AI Playbook (Search Performance Expert)"
                        : searchTab === "bing"
                          ? "AI Playbook (Bing Webmaster Expert)"
                          : "AI Playbook (Google Search Console Expert)"}
                    </div>
                    <div
                      className="mini"
                      style={{ opacity: 0.85, marginTop: 4 }}
                    >
                      Insights accionables basados en KPIs + top queries/pages.
                    </div>
                  </div>

                  <button
                    className="smallBtn aiBtn"
                    onClick={generateInsights}
                    disabled={aiLoading || loading || !stateRows.length}
                    type="button"
                  >
                    {aiLoading ? "Generating…" : "Generate AI Playbook"}
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
                      <div className="aiSummaryTitle">Executive summary</div>
                      <div className="aiText">
                        {aiInsights.executive_summary}
                      </div>
                    </div>

                    <div className="aiScore">
                      <span
                        className={`aiBadge ${aiInsights.scorecard?.health || ""}`}
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
                        <div className="aiBlockTitle">Top opportunities</div>
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
                                      .slice(0, 5)
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
                        <div className="aiBlockTitle">Quick wins (7 days)</div>
                        <ul className="aiList">
                          {aiInsights.quick_wins_next_7_days
                            .slice(0, 7)
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
                      Tip: Selecciona un estado y luego “Generate insights” para
                      recomendaciones hiper-específicas por keywords + páginas.
                    </div>
                  </div>
                )}
              </div>

              <div style={{ marginTop: 12 }}>
                <AiAgentChatPanel
                  agent={
                    searchTab === "all"
                      ? "search_performance"
                      : searchTab === "bing"
                        ? "bing_webmaster"
                        : "gsc"
                  }
                  title={
                    searchTab === "all"
                      ? "Search Performance Agent Chat"
                      : searchTab === "bing"
                        ? "Bing Webmaster Agent Chat"
                        : "GSC Agent Chat"
                  }
                  context={{
                    source: searchTab,
                    preset,
                    customStart,
                    customEnd,
                    compareOn,
                    trendMode,
                    metric,
                    selectedState: mapSelected || null,
                    summary,
                    selectedStateRow,
                  }}
                />
              </div>
            </aside>
          </div>

          {/* Top tables */}
          <div className="card" style={{ marginTop: 14 }}>
            <div className="cardHeader">
              <div>
                <h2 className="cardTitle">Top Queries & Pages</h2>
                <div className="cardSubtitle">
                  Top 100 por impressions. (Pages se filtran por estado cuando
                  seleccionas uno.)
                </div>
              </div>
              <div className="badge">top 100</div>
            </div>

            <div className="cardBody">
              <div className="gscTopGrid">
                <div className="gscTopCard">
                  <div className="gscTopHead">
                    <div className="gscTopTitle">Top Queries</div>
                  </div>
                  <div className="tableWrap tableScrollX">
                    <table className="table">
                      <thead>
                        <tr>
                          <th className="th">Query</th>
                          <th className="th">Impr</th>
                          <th className="th">Clicks</th>
                          <th className="th">CTR</th>
                          <th className="th">Pos</th>
                        </tr>
                      </thead>
                      <tbody>
                        {topQueries.map((q: any, i: number) => (
                          <tr key={i} className="tr">
                            <td className="td mono">{q.query || "—"}</td>
                            <td className="td">{fmtInt(q.impressions)}</td>
                            <td className="td">{fmtInt(q.clicks)}</td>
                            <td className="td">{fmtPct(q.ctr)}</td>
                            <td className="td">{fmtPos(q.position)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                <div className="gscTopCard">
                  <div className="gscTopHead">
                    <div className="gscTopTitle">
                      Top Pages{" "}
                      {mapSelected ? (
                        <span className="mini">({mapSelected})</span>
                      ) : null}
                    </div>
                  </div>
                  <div className="tableWrap tableScrollX">
                    <table className="table">
                      <thead>
                        <tr>
                          <th className="th">Page</th>
                          <th className="th">Impr</th>
                          <th className="th">Clicks</th>
                          <th className="th">CTR</th>
                          <th className="th">Pos</th>
                        </tr>
                      </thead>
                      <tbody>
                        {topPages.map((p: any, i: number) => (
                          <tr key={i} className="tr">
                            <td className="td mono">{p.page || "—"}</td>
                            <td className="td">{fmtInt(p.impressions)}</td>
                            <td className="td">{fmtInt(p.clicks)}</td>
                            <td className="td">{fmtPct(p.ctr)}</td>
                            <td className="td">{fmtPos(p.position)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>

              <div className="mini" style={{ marginTop: 10, opacity: 0.75 }}>
                Roadmap: query+page join → cluster por estado/county/city + gap
                analysis contra scripts/out (Delta coverage).
              </div>
            </div>
          </div>

          {/* Nationwide + Funnels */}
          <div className="card" style={{ marginTop: 14 }}>
            <div className="cardHeader">
              <div>
                <h2 className="cardTitle">Nationwide & Funnels</h2>
                <div className="cardSubtitle">
                  Root domain se considera “Nationwide / Home Page”. Subdominios
                  fuera del patrón Delta → Funnels.
                </div>
              </div>

              <div
                className="cardHeaderActions"
                style={{ display: "flex", gap: 10, alignItems: "center" }}
              >
                <div
                  className="segmented"
                  role="tablist"
                  aria-label="Nationwide/Funnels tab"
                >
                  <button
                    className={`segBtn ${nfTab === "nationwide" ? "segBtnOn" : ""}`}
                    onClick={() => setNfTab("nationwide")}
                    type="button"
                  >
                    Nationwide
                  </button>
                  <button
                    className={`segBtn ${nfTab === "funnels" ? "segBtnOn" : ""}`}
                    onClick={() => setNfTab("funnels")}
                    type="button"
                  >
                    Funnels
                  </button>
                </div>

                <div className="badge">
                  {fmtInt(summaryNationwide.impressions)} impr •{" "}
                  {fmtInt(funnelRows.length)} funnels
                </div>
              </div>
            </div>

            <div className="cardBody">
              {nfTab === "nationwide" ? (
                <div className="gscTopCard">
                  <div className="gscTopHead">
                    <div className="gscTopTitle">
                      {summaryNationwide.label || "Nationwide / Home Page"}
                    </div>
                  </div>

                  <div className="kpiGrid32">
                    <div className="kpi">
                      <p className="n">
                        {fmtInt(summaryNationwide.impressions)}
                      </p>
                      <p className="l">Impressions</p>
                    </div>
                    <div className="kpi">
                      <p className="n">{fmtInt(summaryNationwide.clicks)}</p>
                      <p className="l">Clicks</p>
                    </div>
                    <div className="kpi">
                      <p className="n">{fmtPct(summaryNationwide.ctr)}</p>
                      <p className="l">CTR</p>
                    </div>
                    <div className="kpi">
                      <p className="n">{fmtPos(summaryNationwide.position)}</p>
                      <p className="l">Avg pos</p>
                    </div>
                    <div className="kpi">
                      <p className="n">
                        {fmtInt(summaryNationwide.pagesCounted)}
                      </p>
                      <p className="l">Pages counted</p>
                    </div>
                  </div>

                  <div className="mini" style={{ marginTop: 10, opacity: 0.8 }}>
                    Root host:{" "}
                    <b className="mono">{summaryNationwide.rootHost || "—"}</b>
                  </div>
                </div>
              ) : (
                <div className="gscTopCard">
                  <div className="gscTopHead">
                    <div className="gscTopTitle">
                      {summaryFunnels.label || "Funnels (non-Delta subdomains)"}
                    </div>
                  </div>

                  <div className="kpiGrid32" style={{ marginBottom: 12 }}>
                    <div className="kpi">
                      <p className="n">{fmtInt(summaryFunnels.impressions)}</p>
                      <p className="l">Impressions</p>
                    </div>
                    <div className="kpi">
                      <p className="n">{fmtInt(summaryFunnels.clicks)}</p>
                      <p className="l">Clicks</p>
                    </div>
                    <div className="kpi">
                      <p className="n">{fmtPct(summaryFunnels.ctr)}</p>
                      <p className="l">CTR</p>
                    </div>
                    <div className="kpi">
                      <p className="n">{fmtPos(summaryFunnels.position)}</p>
                      <p className="l">Avg pos</p>
                    </div>
                    <div className="kpi">
                      <p className="n">{fmtInt(summaryFunnels.pagesCounted)}</p>
                      <p className="l">Pages counted</p>
                    </div>
                  </div>

                  <div className="tableWrap tableScrollX">
                    <table className="table">
                      <thead>
                        <tr>
                          <th className="th">Funnel</th>
                          <th className="th">Host</th>
                          <th className="th">Impr</th>
                          <th className="th">Clicks</th>
                          <th className="th">CTR</th>
                          <th className="th">Pos</th>
                          <th className="th">Pages</th>
                        </tr>
                      </thead>
                      <tbody>
                        {funnelRows.slice(0, 30).map((r: any, i: number) => (
                          <tr key={i} className="tr">
                            <td className="td">
                              <b>{r.funnel || "Funnel"}</b>
                            </td>
                            <td className="td mono">{r.host || "—"}</td>
                            <td className="td">{fmtInt(r.impressions)}</td>
                            <td className="td">{fmtInt(r.clicks)}</td>
                            <td className="td">{fmtPct(r.ctr)}</td>
                            <td className="td">{fmtPos(r.position)}</td>
                            <td className="td">{fmtInt(r.pagesCounted)}</td>
                          </tr>
                        ))}
                        {!funnelRows.length ? (
                          <tr className="tr">
                            <td
                              className="td"
                              colSpan={7}
                              style={{ opacity: 0.75 }}
                            >
                              No funnels detectados en este rango.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>

                  {/* <div
                    className="mini"
                    style={{ marginTop: 10, opacity: 0.75 }}
                  >
                    Funnels = subdominios fuera de city/county/-abbr. Se agrupan
                    por host y se formatea el nombre a Title Case.
                  </div> */}
                </div>
              )}
            </div>
          </div>

          {/* States table */}
          <div className="card" style={{ marginTop: 14 }}>
            <div className="cardHeader">
              <div>
                <h2 className="cardTitle">States table</h2>
              </div>
              <div className="badge">{stateRows.length} rows</div>
            </div>

            <div className="cardBody">
              <div className="tableWrap tableScrollX">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="th">State</th>
                      <th className="th">Impressions</th>
                      <th className="th">Clicks</th>
                      <th className="th">CTR</th>
                      <th className="th">Avg pos</th>
                      <th className="th">Pages</th>
                      <th className="th">Keywords</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stateRows.map((r: any, i: number) => (
                      <tr
                        key={i}
                        className="tr"
                        style={{
                          cursor:
                            r.state === "__unknown" ? "default" : "pointer",
                        }}
                        onClick={() => {
                          if (r.state === "__unknown") return;
                          setMapSelected(String(r.state));
                        }}
                      >
                        <td className="td">
                          <b className="mono">{r.state}</b>
                        </td>
                        <td className="td">{fmtInt(r.impressions)}</td>
                        <td className="td">{fmtInt(r.clicks)}</td>
                        <td className="td">{fmtPct(r.ctr)}</td>
                        <td className="td">{fmtPos(r.position)}</td>
                        <td className="td">{fmtInt(r.pagesCounted)}</td>
                        <td className="td">{fmtInt(r.keywordsCount ?? 0)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* {data?.debug?.catalogFingerprint ? (
                <div className="mini" style={{ marginTop: 10, opacity: 0.65 }}>
                  Catalog fingerprint:{" "}
                  <b className="mono">
                    {String(data.debug.catalogFingerprint)}
                  </b>
                  {data?.debug?.forceCatalog ? (
                    <>
                      {" "}
                      • forceCatalog: <b className="mono">true</b>
                    </>
                  ) : null}
                </div>
              ) : null} */}
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
