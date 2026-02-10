"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import dynamic from "next/dynamic";

import UsaChoroplethGaMap from "@/components/UsaChoroplethGaMap";
import GaInsightsPanel from "@/components/GaInsightsPanel";

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

function norm(s: any) {
  return String(s ?? "").trim();
}

export default function GaDashboardPage() {
  const [loading, setLoading] = useState(false);
  const [hardRefreshing, setHardRefreshing] = useState(false);
  const [err, setErr] = useState("");

  const [preset, setPreset] = useState<RangePreset>("last_28_days");
  const [customStart, setCustomStart] = useState("");
  const [customEnd, setCustomEnd] = useState("");

  const [compareOn, setCompareOn] = useState(true);

  const [trendMode, setTrendMode] = useState<"day" | "week" | "month">("day");
  const [metric, setMetric] = useState<
    "sessions" | "users" | "views" | "conversions"
  >("sessions");

  // ✅ Map focus metric (default = main metric)
  const mapMetric = metric;

  // ✅ Selected state drill-down
  const [pickedState, setPickedState] = useState<string>("");

  const [data, setData] = useState<any>(null);

  function buildSyncParams(force: boolean) {
    const p = new URLSearchParams();
    p.set("range", preset);
    if (preset === "custom") {
      if (customStart) p.set("start", customStart);
      if (customEnd) p.set("end", customEnd);
    }
    if (force) p.set("force", "1");
    if (compareOn) p.set("compare", "1");
    p.set("v", String(Date.now()));
    return p.toString();
  }

  function buildJoinParams(force: boolean) {
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
        `/api/dashboard/ga/sync?${buildSyncParams(force)}`,
        { cache: "no-store" },
      );
      const syncJson = await syncRes.json();
      if (!syncRes.ok || !syncJson?.ok) {
        throw new Error(syncJson?.error || `SYNC HTTP ${syncRes.status}`);
      }

      const joinRes = await fetch(
        `/api/dashboard/ga/join?${buildJoinParams(force)}`,
        { cache: "no-store" },
      );
      const joinJson = await joinRes.json();
      if (!joinRes.ok || !joinJson?.ok) {
        throw new Error(joinJson?.error || `JOIN HTTP ${joinRes.status}`);
      }

      setData(joinJson);

      // if picked state no longer exists in range, clear it (we'll auto-pick again in effect)
      const exists =
        (joinJson?.stateRows || []).some(
          (r: any) =>
            norm(r.region).toLowerCase() === norm(pickedState).toLowerCase(),
        ) || norm(pickedState).toLowerCase() === "puerto rico";

      if (pickedState && !exists) setPickedState("");
    } catch (e: any) {
      setData(null);
      setErr(e?.message || "Failed to load GA dashboard");
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
    sessions: 0,
    users: 0,
    views: 0,
    engagementRate: 0,
    conversions: 0,
    generatedAt: null,
    startDate: null,
    endDate: null,
  };

  const compare = data?.compare || null;

  const comparePills = useMemo(() => {
    if (!compareOn || !compare?.pct) return null;
    const pct = compare.pct || {};
    return {
      sessions: pct.sessions ?? null,
      users: pct.users ?? null,
      views: pct.views ?? null,
      engagementRate: pct.engagementRate ?? null,
      conversions: pct.conversions ?? null,
    };
  }, [compareOn, compare]);

  const trend = (data?.trend || []) as any[];
  const trendFiltered = (data?.trendFiltered || []) as any[];

  const startDate = summary?.startDate || data?.meta?.startDate || null;
  const endDate = summary?.endDate || data?.meta?.endDate || null;

  const stateRows = (data?.stateRows || []) as any[];
  const topCities = (data?.topCities || []) as any[];
  const topLanding = (data?.topLanding || []) as any[];
  const topSourceMedium = (data?.topSourceMedium || []) as any[];

  // Chart rows -> reuse existing GSCTrendChart
  const chartRows = useMemo(() => {
    const rows = (trendFiltered.length ? trendFiltered : trend) || [];
    return rows.map((r: any) => ({
      date: r.date,
      impressions:
        metric === "sessions"
          ? Number(r.sessions || 0)
          : metric === "users"
            ? Number(r.users || 0)
            : metric === "views"
              ? Number(r.views || 0)
              : Number(r.conversions || 0),
      clicks: Number(r.users || 0),
      position: Number(r.conversions || 0),
      ctr: Number(r.engagementRate || 0),
    }));
  }, [trendFiltered, trend, metric]);

  // =========================
  // Auto-pick best state so panel is ALWAYS meaningful
  // =========================
  useEffect(() => {
    const rows = stateRows || [];
    if (!rows.length) return;

    const pickMetric =
      mapMetric === "sessions"
        ? "sessions"
        : mapMetric === "users"
          ? "users"
          : mapMetric === "views"
            ? "views"
            : "conversions";

    const exists =
      pickedState &&
      rows.some(
        (r: any) =>
          norm(r.region).toLowerCase() === norm(pickedState).toLowerCase(),
      );

    if (pickedState && exists) return;

    const best = [...rows].sort((a: any, b: any) => {
      const av = Number(a?.[pickMetric] || 0);
      const bv = Number(b?.[pickMetric] || 0);
      if (bv !== av) return bv - av;
      return Number(b?.sessions || 0) - Number(a?.sessions || 0);
    })[0];

    if (best?.region) setPickedState(String(best.region));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stateRows, mapMetric]);

  // =========================
  // Drilldown: picked state row + top cities for that state
  // =========================
  const pickedStateRow = useMemo(() => {
    const s = norm(pickedState).toLowerCase();
    if (!s) return null;

    const row = stateRows.find((r: any) => norm(r.region).toLowerCase() === s);
    return row || null;
  }, [pickedState, stateRows]);

  const pickedTopCities = useMemo(() => {
    const s = norm(pickedState).toLowerCase();
    if (!s) return [];

    return (topCities || [])
      .filter((c: any) => {
        const region = norm(c.region).toLowerCase();
        const country = norm(c.country).toLowerCase();

        if (region === s) return true;

        // PR fallback (si ciudades vienen con country Puerto Rico pero region distinto)
        if (s === "puerto rico" && country === "puerto rico") return true;

        return false;
      })
      .slice(0, 15);
  }, [pickedState, topCities]);

  // =========================
  // Map rows adapter
  // =========================
  const mapRows = useMemo(() => {
    return (stateRows || []).map((r: any) => ({
      state: r.region,
      stateCode: r.regionCode || r.stateCode || undefined,
      sessions: Number(r.sessions || 0),
      users: Number(r.users || 0),
      views: Number(r.views || 0),
      conversions: Number(r.conversions || 0),
      engagementRate: Number(r.engagementRate || 0),
    }));
  }, [stateRows]);

  function metricLabel(m: typeof metric) {
    if (m === "sessions") return "Sessions";
    if (m === "users") return "Users";
    if (m === "views") return "Pageviews";
    return "Conversions";
  }

  function resetPickedToTop() {
    const rows = stateRows || [];
    if (!rows.length) {
      setPickedState("");
      return;
    }

    const pickMetric =
      mapMetric === "sessions"
        ? "sessions"
        : mapMetric === "users"
          ? "users"
          : mapMetric === "views"
            ? "views"
            : "conversions";

    const best = [...rows].sort((a: any, b: any) => {
      const av = Number(a?.[pickMetric] || 0);
      const bv = Number(b?.[pickMetric] || 0);
      if (bv !== av) return bv - av;
      return Number(b?.sessions || 0) - Number(a?.sessions || 0);
    })[0];

    setPickedState(best?.region ? String(best.region) : "");
  }

  // =========================
  // ✅ Agent payload (compact + state-aware)
  // =========================
  const gaAgentPayload = useMemo(() => {
    if (!data) return null;

    const selected = norm(pickedState) || "";
    const selectedLower = selected.toLowerCase();

    const selectedStateRow =
      selectedLower
        ? (stateRows || []).find(
            (r: any) => norm(r.region).toLowerCase() === selectedLower,
          ) || null
        : null;

    const selectedCities = selectedLower
      ? (topCities || [])
          .filter((c: any) => {
            const region = norm(c.region).toLowerCase();
            const country = norm(c.country).toLowerCase();
            if (region === selectedLower) return true;
            if (selectedLower === "puerto rico" && country === "puerto rico")
              return true;
            return false;
          })
          .slice(0, 50)
      : [];

    // Mantén lo esencial para insights (sin mandar 500 filas)
    return {
      meta: data?.meta || null,

      context: {
        preset,
        range: data?.meta?.range || preset,
        startDate: data?.meta?.startDate || summary?.startDate || null,
        endDate: data?.meta?.endDate || summary?.endDate || null,
        compareOn,
        metric,
        mapMetric,
        selectedState: selected || null,
      },

      summaryOverall: data?.summaryOverall || null,
      compare: data?.compare || null,

      // Trend: suficiente para detectar spikes
      trendFiltered: (data?.trendFiltered || []).slice(0, 400),
      trend: (data?.trend || []).slice(0, 400),

      // Geo
      stateRows: (data?.stateRows || []).slice(0, 80),
      selectedStateRow,
      selectedTopCities: selectedCities,

      // Behavior
      topLanding: (data?.topLanding || []).slice(0, 60),

      // Acquisition
      topSourceMedium: (data?.topSourceMedium || []).slice(0, 60),

      // Debug light
      counts: data?.counts || null,
    };
  }, [
    data,
    preset,
    compareOn,
    metric,
    mapMetric,
    pickedState,
    stateRows,
    topCities,
    summary?.startDate,
    summary?.endDate,
  ]);

  return (
    <div className="shell callsDash gaDash">
      <header className="topbar">
        <div className="brand">
          <div className="logo" />
          <div>
            <h1>My Drip Nurse — Google Analytics (GA4) Dashboard</h1>
            <div className="mini" style={{ opacity: 0.8, marginTop: 4 }}>
              Behavior + traffic quality + geo insights (Delta-aware). Property:{" "}
              <b className="mono">{data?.meta?.propertyId || "—"}</b>
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
              title="Forza refresh y recachea GA4"
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
                className={`smallBtn ${metric === "sessions" ? "smallBtnOn" : ""}`}
                onClick={() => setMetric("sessions")}
                type="button"
              >
                Sessions
              </button>
              <button
                className={`smallBtn ${metric === "users" ? "smallBtnOn" : ""}`}
                onClick={() => setMetric("users")}
                type="button"
              >
                Users
              </button>
              <button
                className={`smallBtn ${metric === "views" ? "smallBtnOn" : ""}`}
                onClick={() => setMetric("views")}
                type="button"
              >
                Pageviews
              </button>
              <button
                className={`smallBtn ${metric === "conversions" ? "smallBtnOn" : ""}`}
                onClick={() => setMetric("conversions")}
                type="button"
              >
                Conversions
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
                      <b>{summary.endDate || "—"}</b> • Sessions:{" "}
                      <b>{fmtInt(summary.sessions)}</b> • Users:{" "}
                      <b>{fmtInt(summary.users)}</b> • Views:{" "}
                      <b>{fmtInt(summary.views)}</b> • Engagement:{" "}
                      <b>{fmtPct(summary.engagementRate)}</b> • Conversions:{" "}
                      <b>{fmtInt(summary.conversions)}</b>
                    </div>
                  </span>

                  {compareOn && comparePills ? (
                    <span className="deltaPills">
                      <span
                        className={`deltaPill ${deltaClass(comparePills.sessions)}`}
                        title="Δ Sessions vs previous window"
                      >
                        Sessions:{" "}
                        {comparePills.sessions == null
                          ? "—"
                          : fmtDeltaPct(comparePills.sessions)}
                      </span>
                      <span
                        className={`deltaPill ${deltaClass(comparePills.users)}`}
                        title="Δ Users vs previous window"
                      >
                        Users:{" "}
                        {comparePills.users == null
                          ? "—"
                          : fmtDeltaPct(comparePills.users)}
                      </span>
                      <span
                        className={`deltaPill ${deltaClass(comparePills.views)}`}
                        title="Δ Views vs previous window"
                      >
                        Views:{" "}
                        {comparePills.views == null
                          ? "—"
                          : fmtDeltaPct(comparePills.views)}
                      </span>
                      <span
                        className={`deltaPill ${deltaClass(comparePills.engagementRate)}`}
                        title="Δ Engagement Rate vs previous window"
                      >
                        Engage:{" "}
                        {comparePills.engagementRate == null
                          ? "—"
                          : fmtDeltaPct(comparePills.engagementRate)}
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
              KPIs del rango seleccionado. En GA, esto representa comportamiento
              real (no solo demanda).
            </div>
          </div>
          <div className="badge">{loading ? "loading…" : "ready"}</div>
        </div>

        <div className="cardBody">
          <div className="kpiGrid32">
            <div className="kpi">
              <p className="n">{fmtInt(summary.sessions)}</p>
              <p className="l">
                Sessions{" "}
                {compareOn && compare?.pct?.sessions != null ? (
                  <span className={`delta ${deltaClass(compare.pct.sessions)}`}>
                    {fmtDeltaPct(compare.pct.sessions)}
                  </span>
                ) : null}
              </p>
            </div>

            <div className="kpi">
              <p className="n">{fmtInt(summary.users)}</p>
              <p className="l">
                Users{" "}
                {compareOn && compare?.pct?.users != null ? (
                  <span className={`delta ${deltaClass(compare.pct.users)}`}>
                    {fmtDeltaPct(compare.pct.users)}
                  </span>
                ) : null}
              </p>
            </div>

            <div className="kpi">
              <p className="n">{fmtInt(summary.views)}</p>
              <p className="l">
                Pageviews{" "}
                {compareOn && compare?.pct?.views != null ? (
                  <span className={`delta ${deltaClass(compare.pct.views)}`}>
                    {fmtDeltaPct(compare.pct.views)}
                  </span>
                ) : null}
              </p>
            </div>

            <div className="kpi">
              <p className="n">{fmtPct(summary.engagementRate)}</p>
              <p className="l">
                Engagement rate{" "}
                {compareOn && compare?.pct?.engagementRate != null ? (
                  <span
                    className={`delta ${deltaClass(compare.pct.engagementRate)}`}
                  >
                    {fmtDeltaPct(compare.pct.engagementRate)}
                  </span>
                ) : null}
              </p>
            </div>

            <div className="kpi">
              <p className="n">{fmtInt(summary.conversions)}</p>
              <p className="l">
                Conversions{" "}
                {compareOn && compare?.pct?.conversions != null ? (
                  <span
                    className={`delta ${deltaClass(compare.pct.conversions)}`}
                  >
                    {fmtDeltaPct(compare.pct.conversions)}
                  </span>
                ) : null}
              </p>
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
            Lectura estratégica: <b>Sessions</b> = demanda + distribución;{" "}
            <b>Engagement</b> = calidad; <b>Conversions</b> = intención real. El
            Delta System debe convertir mejor en estados con engagement alto.
          </div>

          <div style={{ marginTop: 14 }}>
            <GSCTrendChart
              trend={chartRows as any}
              metric={"impressions" as any}
              mode={trendMode}
              startDate={startDate}
              endDate={endDate}
            />
          </div>
        </div>
      </section>

      {/* ✅ Map + State drill-down */}
      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">GA4 Map (State Drill-down)</h2>
            <div className="cardSubtitle">
              El mapa enfatiza <b>{metricLabel(mapMetric)}</b>. Click en un
              estado para ver KPIs y top cities del estado.
            </div>
          </div>
          <div className="badge">map</div>
        </div>

        <div className="cardBody">
          <div className="gaDashMapGrid">
            <div>
              <UsaChoroplethGaMap
                rows={mapRows}
                metric={
                  mapMetric === "sessions"
                    ? "sessions"
                    : mapMetric === "users"
                      ? "users"
                      : mapMetric === "views"
                        ? "views"
                        : "conversions"
                }
                selectedState={pickedState || undefined}
                onPick={({ stateName }) => {
                  setPickedState(stateName);
                }}
              />

              <div className="mini" style={{ marginTop: 10, opacity: 0.8 }}>
                Tip: usa <b>Engagement rate</b> (tabla) como calidad; el mapa
                está centrado en volumen ({metricLabel(mapMetric)}).
              </div>
            </div>

            {/* ✅ Right panel */}
            <div className="gscTopCard gaDashStatePanel" style={{ margin: 0 }}>
              <div
                className="gscTopHead"
                style={{ justifyContent: "space-between" }}
              >
                <div>
                  <div className="gscTopTitle">
                    State KPIs —{" "}
                    <span className="mono">
                      {pickedState ? pickedState : "—"}
                    </span>
                  </div>
                  <div className="mini" style={{ opacity: 0.75, marginTop: 4 }}>
                    Drill-down rápido para decisiones: volumen + calidad +
                    intención.
                  </div>
                </div>

                <button
                  className="smallBtn"
                  type="button"
                  onClick={resetPickedToTop}
                  title="Reset al estado #1 según el métrico actual del mapa"
                  disabled={!stateRows.length}
                >
                  Reset
                </button>
              </div>

              <div style={{ padding: 12 }}>
                {pickedStateRow ? (
                  <>
                    <div
                      className="kpiGrid32"
                      style={{ gridTemplateColumns: "1fr 1fr" }}
                    >
                      <div className="kpi">
                        <p className="n">{fmtInt(pickedStateRow.sessions)}</p>
                        <p className="l">Sessions</p>
                      </div>

                      <div className="kpi">
                        <p className="n">{fmtInt(pickedStateRow.users)}</p>
                        <p className="l">Users</p>
                      </div>

                      <div className="kpi">
                        <p className="n">{fmtInt(pickedStateRow.views)}</p>
                        <p className="l">Pageviews</p>
                      </div>

                      <div className="kpi">
                        <p className="n">
                          {fmtPct(pickedStateRow.engagementRate)}
                        </p>
                        <p className="l">Engagement</p>
                      </div>

                      <div className="kpi" style={{ gridColumn: "1 / span 2" }}>
                        <p className="n">
                          {fmtInt(pickedStateRow.conversions)}
                        </p>
                        <p className="l">Conversions</p>
                      </div>
                    </div>

                    <div
                      className="mini"
                      style={{ marginTop: 10, opacity: 0.75 }}
                    >
                      Lectura: si <b>Engagement</b> alto y <b>Conversions</b>{" "}
                      bajo → el problema suele ser oferta/CTA/flow (ideal para
                      resolver con GHL).
                    </div>

                    <div style={{ marginTop: 12 }}>
                      <div
                        className="mini"
                        style={{ opacity: 0.85, marginBottom: 8 }}
                      >
                        <b>Top Cities</b> en {pickedState}
                      </div>

                      <div className="tableWrap tableScrollX">
                        <table className="table">
                          <thead>
                            <tr>
                              <th className="th">City</th>
                              <th className="th">Sessions</th>
                              <th className="th">Users</th>
                              <th className="th">Engage</th>
                              <th className="th">Conv</th>
                            </tr>
                          </thead>
                          <tbody>
                            {pickedTopCities.map((r: any, i: number) => (
                              <tr key={i} className="tr">
                                <td className="td">
                                  <b className="mono">{r.city}</b>
                                  <div
                                    className="mini"
                                    style={{ opacity: 0.7 }}
                                  >
                                    {r.region} • {r.country}
                                  </div>
                                </td>
                                <td className="td">{fmtInt(r.sessions)}</td>
                                <td className="td">{fmtInt(r.users)}</td>
                                <td className="td">
                                  {fmtPct(r.engagementRate)}
                                </td>
                                <td className="td">{fmtInt(r.conversions)}</td>
                              </tr>
                            ))}
                            {!pickedTopCities.length ? (
                              <tr className="tr">
                                <td
                                  className="td"
                                  colSpan={5}
                                  style={{ opacity: 0.75 }}
                                >
                                  No city data para este estado en este rango.
                                </td>
                              </tr>
                            ) : null}
                          </tbody>
                        </table>
                      </div>
                    </div>

                    {/* ✅ AI Strategist (GA Agent) */}
                    <div style={{ marginTop: 14 }}>
                      <GaInsightsPanel data={gaAgentPayload} title="AI Strategist (GA4)" />
                    </div>
                  </>
                ) : (
                  <div className="mini" style={{ opacity: 0.8 }}>
                    No encontré data para <b>{pickedState || "—"}</b> en este
                    rango.
                    <div style={{ marginTop: 8 }}>
                      Sugerencia: cambia el rango o haz <b>Refresh</b>.
                    </div>
                    <div style={{ marginTop: 14 }}>
                      <GaInsightsPanel data={gaAgentPayload} title="AI Strategist (GA4)" />
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Tables */}
      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Geo & Acquisition</h2>
            <div className="cardSubtitle">
              Estados/ciudades por sesiones + landing pages + source/medium.
              (Próximo: merge con GSC para “Demand → Behavior → Conversion”.)
            </div>
          </div>
          <div className="badge">top</div>
        </div>

        <div className="cardBody">
          <div className="gscTopGrid">
            <div className="gscTopCard">
              <div className="gscTopHead">
                <div className="gscTopTitle">States (Region)</div>
              </div>

              <div className="tableWrap tableScrollX">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="th">Region</th>
                      <th className="th">Sessions</th>
                      <th className="th">Users</th>
                      <th className="th">Views</th>
                      <th className="th">Engage</th>
                      <th className="th">Conv</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stateRows.slice(0, 30).map((r: any, i: number) => (
                      <tr
                        key={i}
                        className="tr"
                        onClick={() => setPickedState(r.region)}
                        style={{ cursor: "pointer" }}
                        title="Click para drill-down en el panel del mapa"
                      >
                        <td className="td">
                          <b className="mono">{r.region}</b>
                          <div className="mini" style={{ opacity: 0.7 }}>
                            {r.country}
                          </div>
                        </td>
                        <td className="td">{fmtInt(r.sessions)}</td>
                        <td className="td">{fmtInt(r.users)}</td>
                        <td className="td">{fmtInt(r.views)}</td>
                        <td className="td">{fmtPct(r.engagementRate)}</td>
                        <td className="td">{fmtInt(r.conversions)}</td>
                      </tr>
                    ))}
                    {!stateRows.length ? (
                      <tr className="tr">
                        <td
                          className="td"
                          colSpan={6}
                          style={{ opacity: 0.75 }}
                        >
                          No data en este rango. Pulsa Refresh.
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="gscTopCard">
              <div className="gscTopHead">
                <div className="gscTopTitle">Top Cities</div>
              </div>

              <div className="tableWrap tableScrollX">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="th">City</th>
                      <th className="th">Sessions</th>
                      <th className="th">Users</th>
                      <th className="th">Engage</th>
                      <th className="th">Conv</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topCities.slice(0, 30).map((r: any, i: number) => (
                      <tr key={i} className="tr">
                        <td className="td">
                          <b className="mono">{r.city}</b>
                          <div className="mini" style={{ opacity: 0.7 }}>
                            {r.region} • {r.country}
                          </div>
                        </td>
                        <td className="td">{fmtInt(r.sessions)}</td>
                        <td className="td">{fmtInt(r.users)}</td>
                        <td className="td">{fmtPct(r.engagementRate)}</td>
                        <td className="td">{fmtInt(r.conversions)}</td>
                      </tr>
                    ))}
                    {!topCities.length ? (
                      <tr className="tr">
                        <td
                          className="td"
                          colSpan={5}
                          style={{ opacity: 0.75 }}
                        >
                          No city data en este rango.
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
                <div className="gscTopTitle">Top Landing Pages</div>
              </div>

              <div className="tableWrap tableScrollX">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="th">Landing page</th>
                      <th className="th">Sessions</th>
                      <th className="th">Users</th>
                      <th className="th">Engage</th>
                      <th className="th">Conv</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topLanding.slice(0, 30).map((r: any, i: number) => (
                      <tr key={i} className="tr">
                        <td className="td mono">{r.landingPage}</td>
                        <td className="td">{fmtInt(r.sessions)}</td>
                        <td className="td">{fmtInt(r.users)}</td>
                        <td className="td">{fmtPct(r.engagementRate)}</td>
                        <td className="td">{fmtInt(r.conversions)}</td>
                      </tr>
                    ))}
                    {!topLanding.length ? (
                      <tr className="tr">
                        <td
                          className="td"
                          colSpan={5}
                          style={{ opacity: 0.75 }}
                        >
                          No landing data en este rango.
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="gscTopCard">
              <div className="gscTopHead">
                <div className="gscTopTitle">Top Source / Medium</div>
              </div>

              <div className="tableWrap tableScrollX">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="th">Source</th>
                      <th className="th">Medium</th>
                      <th className="th">Sessions</th>
                      <th className="th">Engage</th>
                      <th className="th">Conv</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topSourceMedium.slice(0, 30).map((r: any, i: number) => (
                      <tr key={i} className="tr">
                        <td className="td mono">{r.source}</td>
                        <td className="td mono">{r.medium}</td>
                        <td className="td">{fmtInt(r.sessions)}</td>
                        <td className="td">{fmtPct(r.engagementRate)}</td>
                        <td className="td">{fmtInt(r.conversions)}</td>
                      </tr>
                    ))}
                    {!topSourceMedium.length ? (
                      <tr className="tr">
                        <td
                          className="td"
                          colSpan={5}
                          style={{ opacity: 0.75 }}
                        >
                          No acquisition data en este rango.
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div className="mini" style={{ marginTop: 10, opacity: 0.75 }}>
            Próximo paso: “Delta Score” = combinar GSC (impressions/clicks/pos)
            con GA4 (sessions/engagement/conversions) por estado/landing.
          </div>
        </div>
      </section>
    </div>
  );
}
