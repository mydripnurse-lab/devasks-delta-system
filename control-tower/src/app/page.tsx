// src/app/page.tsx
"use client";

import { useEffect, useMemo, useRef, useState } from "react";

const JOBS = [
  { key: "build-sheet-rows", label: "Create DB" },
  { key: "build-counties", label: "Create Subaccount Json" },
  { key: "run-delta-system", label: "Run Delta System" },
  { key: "build-state-sitemaps", label: "Create Sitemaps" },
  { key: "build-state-index", label: "Create Search Index" },
  { key: "update-custom-values", label: "Update Custom Values" },
];

type SheetStateRow = {
  state: string;
  counties: {
    total: number;
    statusTrue: number;
    hasLocId: number;
    ready: number;
    domainsActive?: number;
  };
  cities: {
    total: number;
    statusTrue: number;
    hasLocId: number;
    ready: number;
    domainsActive?: number;
  };
};

type OverviewResponse = {
  tabs?: { counties?: string; cities?: string };
  states: SheetStateRow[];
  error?: string;
};

type StateDetailResponse = {
  state: string;
  tabs: { counties: string; cities: string };
  counties: {
    rows: any[];
    stats: {
      total: number;
      statusTrue: number;
      hasLocId: number;
      eligible: number;
    };
    counties: string[];
  };
  cities: {
    rows: any[];
    stats: {
      total: number;
      statusTrue: number;
      hasLocId: number;
      eligible: number;
    };
    counties: string[];
  };
  error?: string;
};

function s(v: any) {
  return String(v ?? "").trim();
}
function isTrue(v: any) {
  const t = s(v).toLowerCase();
  return t === "true" || t === "1" || t === "yes" || t === "y";
}
function toUrlMaybe(domainOrUrl: string) {
  const d = s(domainOrUrl);
  if (!d) return "";
  if (d.startsWith("http://") || d.startsWith("https://")) return d;
  return `https://${d}`;
}

async function safeJson(res: Response) {
  const text = await res.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return {
      error: `Non-JSON response (${res.status})`,
      raw: text.slice(0, 400),
    };
  }
}

function tsLocal() {
  const d = new Date();
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${hh}:${mm}:${ss}`;
}

function buildRobotsTxt(sitemapUrl: string) {
  const sm = s(sitemapUrl);
  return [
    "User-agent: *",
    "Allow: /",
    "",
    "# Allow all AI crawlers",
    "User-agent: GPTBot",
    "Allow: /",
    "",
    "User-agent: ChatGPT-User",
    "Allow: /",
    "",
    "User-agent: Bingbot",
    "Allow: /",
    "",
    "User-agent: Applebot",
    "Allow: /",
    "",
    "User-agent: PerplexityBot",
    "Allow: /",
    "",
    "User-agent: ClaudeBot",
    "Allow: /",
    "",
    "User-agent: OAI-SearchBot",
    "Allow: /",
    "",
    "User-agent: Bytespider",
    "Allow: /",
    "",
    "User-agent: Amazonbot",
    "Allow: /",
    "",
    "User-agent: FacebookBot",
    "Allow: /",
    "",
    "User-agent: Twitterbot",
    "Allow: /",
    "",
    sm ? `Sitemap: ${sm}` : "",
  ]
    .filter(Boolean)
    .join("\n");
}

type ChecklistTabKey = "domain" | "sitemap" | "robots";

/** ---- Progress / Runner UX (client-only) ---- */
type RunnerTotals = {
  allTotal: number;
  countiesTotal: number;
  citiesTotal: number;
};

type RunnerProgress = {
  pct: number; // 0..1
  allDone: number;
  countiesDone: number;
  citiesDone: number;
  message: string;
  etaSec: number | null;
  status: "idle" | "running" | "stopping" | "done" | "error";
};

function clamp01(n: number) {
  return Math.max(0, Math.min(1, n));
}

function formatDuration(sec: number) {
  if (!Number.isFinite(sec) || sec < 0) return "—";
  const s0 = Math.round(sec);
  const hh = Math.floor(s0 / 3600);
  const mm = Math.floor((s0 % 3600) / 60);
  const ss = s0 % 60;
  if (hh > 0) return `${hh}h ${mm}m`;
  if (mm > 0) return `${mm}m ${ss}s`;
  return `${ss}s`;
}

function normalizePct(raw: any): number | null {
  const n = Number(raw);
  if (!Number.isFinite(n)) return null;
  // if 0..1
  if (n >= 0 && n <= 1) return clamp01(n);
  // if 0..100
  if (n >= 0 && n <= 100) return clamp01(n / 100);
  return null;
}

export default function Home() {
  const [statesOut, setStatesOut] = useState<string[]>([]);
  const [job, setJob] = useState(JOBS[0].key);
  const [stateOut, setStateOut] = useState<string>("all");
  const [mode, setMode] = useState<"dry" | "live">("dry");
  const [debug, setDebug] = useState(true);

  const [runId, setRunId] = useState<string>("");
  const [logs, setLogs] = useState<string[]>([]);
  const esRef = useRef<EventSource | null>(null);

  const runStartedAtRef = useRef<number | null>(null);

  const [sheet, setSheet] = useState<OverviewResponse | null>(null);
  const [sheetErr, setSheetErr] = useState<string>("");
  const [sheetLoading, setSheetLoading] = useState<boolean>(false);
  const [q, setQ] = useState("");

  const [openState, setOpenState] = useState<string>("");
  const [detail, setDetail] = useState<StateDetailResponse | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailErr, setDetailErr] = useState("");
  const [detailTab, setDetailTab] = useState<"counties" | "cities">("counties");
  const [countyFilter, setCountyFilter] = useState<string>("all");

  const [actOpen, setActOpen] = useState(false);
  const [actTitle, setActTitle] = useState("");
  const [actDomainToPaste, setActDomainToPaste] = useState("");
  const [actActivationUrl, setActActivationUrl] = useState("");
  const [actIsActive, setActIsActive] = useState<boolean>(false);
  const [actCopied, setActCopied] = useState<boolean>(false);

  // extra meta
  const [actAccountName, setActAccountName] = useState("");
  const [actTimezone, setActTimezone] = useState("");

  // sitemap + robots in modal
  const [actSitemapUrl, setActSitemapUrl] = useState("");
  const [actChecklistTab, setActChecklistTab] =
    useState<ChecklistTabKey>("domain");
  const [robotsCopied, setRobotsCopied] = useState(false);

  // ✅ Runner UX: running + progress
  const [isRunning, setIsRunning] = useState(false);
  const [progressTotals, setProgressTotals] = useState<RunnerTotals>({
    allTotal: 0,
    countiesTotal: 0,
    citiesTotal: 0,
  });

  const [progress, setProgress] = useState<RunnerProgress>({
    pct: 0,
    allDone: 0,
    countiesDone: 0,
    citiesDone: 0,
    message: "Idle",
    etaSec: null,
    status: "idle",
  });

  function pushLog(line: string) {
    const msg = `[${tsLocal()}] ${String(line ?? "")}`;
    setLogs((p) =>
      p.length > 4000 ? p.slice(-3500).concat(msg) : p.concat(msg)
    );
  }

  useEffect(() => {
    return () => {
      try {
        esRef.current?.close();
      } catch {}
      esRef.current = null;
    };
  }, []);

  useEffect(() => {
    fetch("/api/states")
      .then((r) => r.json())
      .then((d) => setStatesOut(d.states || []))
      .catch(() => setStatesOut([]));
  }, []);

  async function loadOverview() {
    setSheetErr("");
    setSheetLoading(true);
    try {
      const res = await fetch("/api/sheet/overview", { cache: "no-store" });
      const data = (await safeJson(res)) as OverviewResponse | any;
      if (!res.ok || data?.error)
        throw new Error(data?.error || `HTTP ${res.status}`);
      setSheet(data);
    } catch (e: any) {
      setSheet(null);
      setSheetErr(e?.message || "Failed to load sheet overview");
    } finally {
      setSheetLoading(false);
    }
  }

  useEffect(() => {
    loadOverview();
  }, []);

  const selectedJob = useMemo(() => JOBS.find((j) => j.key === job), [job]);

  const filteredSheetStates = useMemo(() => {
    const rows = sheet?.states || [];
    const term = q.trim().toLowerCase();
    if (!term) return rows;
    return rows.filter((r) => r.state.toLowerCase().includes(term));
  }, [sheet, q]);

  const totals = useMemo(() => {
    const rows = sheet?.states || [];
    let countiesTotal = 0,
      countiesReady = 0,
      countiesDomainsActive = 0,
      citiesTotal = 0,
      citiesReady = 0,
      citiesDomainsActive = 0;

    for (const r of rows) {
      countiesTotal += r.counties.total || 0;
      countiesReady += r.counties.ready || 0;
      countiesDomainsActive += r.counties.domainsActive || 0;

      citiesTotal += r.cities.total || 0;
      citiesReady += r.cities.ready || 0;
      citiesDomainsActive += r.cities.domainsActive || 0;
    }

    return {
      countiesTotal,
      countiesReady,
      countiesDomainsActive,
      citiesTotal,
      citiesReady,
      citiesDomainsActive,
    };
  }, [sheet]);

  // ✅ Jobs conceptualmente "por estado" (no por counties/cities)
  const isStateJob = useMemo(() => {
    return job === "build-state-sitemaps" || job === "build-state-index";
  }, [job]);

  // ✅ Scope totals (fallback when progress event doesn't provide totals yet)
  const runScopeTotals = useMemo<RunnerTotals>(() => {
    const rows = sheet?.states || [];
    if (!rows.length) {
      return { allTotal: 0, countiesTotal: 0, citiesTotal: 0 };
    }

    // For state jobs, totals are based on number of states selected
    if (isStateJob) {
      if (stateOut === "all") {
        const totalStates = rows.length;
        return { allTotal: totalStates, countiesTotal: 0, citiesTotal: 0 };
      }
      return { allTotal: 1, countiesTotal: 0, citiesTotal: 0 };
    }

    // Default: counties + cities
    if (stateOut === "all") {
      const allTotal = (totals.countiesTotal || 0) + (totals.citiesTotal || 0);
      return {
        allTotal,
        countiesTotal: totals.countiesTotal,
        citiesTotal: totals.citiesTotal,
      };
    }
    const row = rows.find((r) => r.state === stateOut);
    const c = row?.counties?.total || 0;
    const ci = row?.cities?.total || 0;
    return { allTotal: c + ci, countiesTotal: c, citiesTotal: ci };
  }, [sheet, stateOut, totals.countiesTotal, totals.citiesTotal, isStateJob]);

  async function run() {
    if (isRunning) return;

    setLogs([]);
    runStartedAtRef.current = Date.now();
    setIsRunning(true);

    setProgressTotals(runScopeTotals);
    setProgress({
      pct: 0,
      allDone: 0,
      countiesDone: 0,
      citiesDone: 0,
      message: "Starting…",
      etaSec: null,
      status: "running",
    });

    try {
      esRef.current?.close();
    } catch {}
    esRef.current = null;

    try {
      pushLog(
        `▶ Starting job="${job}" state="${stateOut}" mode="${mode}" debug="${
          debug ? "on" : "off"
        }"...`
      );

      const res = await fetch("/api/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ job, state: stateOut, mode, debug }),
      });

      const text = await res.text();
      let payload: any = null;
      try {
        payload = text ? JSON.parse(text) : null;
      } catch {}

      if (!res.ok) {
        const msg = payload?.error || text || `HTTP ${res.status}`;
        throw new Error(msg);
      }

      const id = payload?.runId as string;
      if (!id) throw new Error("Missing runId");

      setRunId(id);
      pushLog(`✅ runId=${id} (connecting SSE...)`);

      const es = new EventSource(`/api/stream/${id}`);
      esRef.current = es;

      const onHello = (ev: MessageEvent) => {
        pushLog(`🟢 SSE connected: ${ev.data}`);
        setProgress((p) => ({ ...p, message: "Running…", status: "running" }));
      };

      const onLine = (ev: MessageEvent) => {
        const raw = String(ev.data ?? "");
        if (!raw || raw === "__HB__" || raw === "__END__") return;

        // ✅ avoid duplicating machine progress logs into console
        if (
          raw.startsWith("__PROGRESS__ ") ||
          raw.startsWith("__PROGRESS_INIT__ ") ||
          raw.startsWith("__PROGRESS_END__ ")
        ) {
          return;
        }

        pushLog(raw);
      };

      // ✅ Parse dedicated progress payload
      const onProgress = (ev: MessageEvent) => {
        let data: any = null;
        try {
          data = JSON.parse(String(ev.data ?? ""));
        } catch {
          return;
        }

        const totalsAll = Number(data?.totals?.all ?? 0);
        const totalsCounties = Number(data?.totals?.counties ?? 0);
        const totalsCities = Number(data?.totals?.cities ?? 0);

        const doneAll = Number(data?.done?.all ?? 0);
        const doneCounties = Number(data?.done?.counties ?? 0);
        const doneCities = Number(data?.done?.cities ?? 0);

        // Use pct if present, else compute from done/total
        const pctFromPayload = normalizePct(data?.pct);
        const pctComputed = totalsAll > 0 ? clamp01(doneAll / totalsAll) : 0;
        const pctFinal =
          typeof pctFromPayload === "number" ? pctFromPayload : pctComputed;

        // Update totals from stream (best source of truth)
        setProgressTotals((prev) => ({
          allTotal: totalsAll || prev.allTotal || runScopeTotals.allTotal,
          countiesTotal:
            totalsCounties || prev.countiesTotal || runScopeTotals.countiesTotal,
          citiesTotal:
            totalsCities || prev.citiesTotal || runScopeTotals.citiesTotal,
        }));

        // ETA
        const startedAt = runStartedAtRef.current;
        let etaSec: number | null = null;
        if (startedAt && totalsAll > 0 && doneAll > 0) {
          const elapsedSec = (Date.now() - startedAt) / 1000;
          const rate = doneAll / Math.max(0.5, elapsedSec); // items/sec
          const remaining = Math.max(0, totalsAll - doneAll);
          etaSec = rate > 0 ? remaining / rate : null;
          if (etaSec !== null && !Number.isFinite(etaSec)) etaSec = null;
        }

        const last = data?.last;
        const msg =
          last?.kind === "state"
            ? `🗺️ ${s(last?.state)} • ${s(last?.action)}`
            : last?.kind === "city"
            ? `🏙️ ${s(last?.city)} • ${s(last?.action)}`
            : last?.kind === "county"
            ? `🧩 ${s(last?.county)} • ${s(last?.action)}`
            : "Running…";

        setProgress((p) => ({
          ...p,
          pct: pctFinal,
          allDone: Number.isFinite(doneAll) ? doneAll : p.allDone,
          countiesDone: Number.isFinite(doneCounties)
            ? doneCounties
            : p.countiesDone,
          citiesDone: Number.isFinite(doneCities) ? doneCities : p.citiesDone,
          message: msg,
          etaSec,
          status: "running",
        }));
      };

      const onEnd = (ev: MessageEvent) => {
        let data: any = ev.data;
        try {
          data = JSON.parse(String(ev.data ?? ""));
        } catch {}

        const ms = runStartedAtRef.current
          ? Date.now() - runStartedAtRef.current
          : null;
        const msTxt = ms === null ? "" : ` • duration=${(ms / 1000).toFixed(2)}s`;

        pushLog(
          `🏁 END ${
            typeof data === "object" ? JSON.stringify(data) : String(data)
          }${msTxt}`
        );

        try {
          es.close();
        } catch {}

        setIsRunning(false);
        setProgress((p) => ({
          ...p,
          pct: 1,
          etaSec: 0,
          message: "Done",
          status: data?.ok === false ? "error" : "done",
        }));

        setTimeout(() => {
          loadOverview();
          if (openState) openDetail(openState);
        }, 350);
      };

      es.addEventListener("hello", onHello as any);
      es.addEventListener("line", onLine as any);
      es.addEventListener("progress", onProgress as any);
      es.addEventListener("end", onEnd as any);

      es.onerror = () => {
        pushLog(
          "⚠ SSE error / disconnected. (If job still running, refresh or check server logs.)"
        );
        try {
          es.close();
        } catch {}
        setProgress((p) => ({
          ...p,
          message: "SSE disconnected",
          status: "error",
        }));
        setIsRunning(false);
      };
    } catch (e: any) {
      pushLog(`❌ /api/run failed: ${e?.message || e}`);
      setIsRunning(false);
      setProgress((p) => ({
        ...p,
        message: `Error: ${e?.message || e}`,
        status: "error",
      }));
    }
  }

  async function stop() {
    if (!runId) return;

    setProgress((p) => ({ ...p, message: "Stopping…", status: "stopping" }));

    try {
      await fetch(`/api/stop/${runId}`, { method: "POST" });
      pushLog("🛑 Stop requested");
    } catch {
      pushLog("❌ Stop failed (network)");
      setProgress((p) => ({
        ...p,
        message: "Stop failed (network)",
        status: "error",
      }));
    }
  }

  async function openDetail(stateName: string) {
    setOpenState(stateName);
    setDetail(null);
    setDetailErr("");
    setCountyFilter("all");
    setDetailTab("counties");
    setDetailLoading(true);

    try {
      const res = await fetch(
        `/api/sheet/state?name=${encodeURIComponent(stateName)}`,
        { cache: "no-store" }
      );
      const data = (await safeJson(res)) as StateDetailResponse | any;
      if (!res.ok || data?.error)
        throw new Error(data?.error || `HTTP ${res.status}`);
      setDetail(data);
    } catch (e: any) {
      setDetailErr(e?.message || "Failed to load state detail");
    } finally {
      setDetailLoading(false);
    }
  }

  function closeDetail() {
    setOpenState("");
    setDetail(null);
    setDetailErr("");
  }

  function openActivationHelper(opts: {
    title: string;
    domainToPaste: string;
    activationUrl: string;
    isActive: boolean;
    accountName?: string;
    timezone?: string;
    sitemapUrl?: string;
  }) {
    setActTitle(opts.title);
    setActDomainToPaste(opts.domainToPaste);
    setActActivationUrl(opts.activationUrl);
    setActIsActive(opts.isActive);

    setActAccountName(s(opts.accountName));
    setActTimezone(s(opts.timezone));

    setActSitemapUrl(s(opts.sitemapUrl));
    setActChecklistTab("domain");

    setActCopied(false);
    setRobotsCopied(false);
    setActOpen(true);
  }

  function closeActivationHelper() {
    setActOpen(false);
    setActCopied(false);
    setRobotsCopied(false);
  }

  async function copyDomain() {
    try {
      await navigator.clipboard.writeText(actDomainToPaste);
      setActCopied(true);
      setTimeout(() => setActCopied(false), 1300);
    } catch {}
  }

  async function copyRobots() {
    try {
      const txt = buildRobotsTxt(actSitemapUrl);
      await navigator.clipboard.writeText(txt);
      setRobotsCopied(true);
      setTimeout(() => setRobotsCopied(false), 1300);
    } catch {}
  }

  const robotsTxt = useMemo(
    () => buildRobotsTxt(actSitemapUrl),
    [actSitemapUrl]
  );

  const pctText = useMemo(() => {
    const pct = clamp01(progress.pct || 0);
    return `${Math.round(pct * 100)}%`;
  }, [progress.pct]);

  const runnerMeta = useMemo(() => {
    const allT = progressTotals.allTotal || 0;
    const allD = progress.allDone || 0;

    const cT = progressTotals.countiesTotal || 0;
    const ciT = progressTotals.citiesTotal || 0;

    const cD = progress.countiesDone || 0;
    const ciD = progress.citiesDone || 0;

    return {
      all: allT > 0 ? `${allD}/${allT}` : `${allD}`,
      counties: cT > 0 ? `${cD}/${cT}` : `${cD}`,
      cities: ciT > 0 ? `${ciD}/${ciT}` : `${ciD}`,
      eta: progress.etaSec === null ? "—" : formatDuration(progress.etaSec),
    };
  }, [
    progressTotals.allTotal,
    progressTotals.countiesTotal,
    progressTotals.citiesTotal,
    progress.allDone,
    progress.countiesDone,
    progress.citiesDone,
    progress.etaSec,
  ]);

  const runnerToneClass =
    progress.status === "running"
      ? "runnerToneRunning"
      : progress.status === "stopping"
      ? "runnerToneStopping"
      : progress.status === "done"
      ? "runnerToneDone"
      : progress.status === "error"
      ? "runnerToneError"
      : "runnerToneIdle";

  return (
    <div className="shell">
      {/* Header */}
      <header className="topbar">
        <div className="brand">
          <div className="logo" />
          <div>
            <h1>My Drip Nurse — Delta Control Tower</h1>
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

      {/* Top grid */}
      <div className="grid">
        {/* Runner */}
        <section className="card">
          <div className="cardHeader">
            <div>
              <h2 className="cardTitle">Runner</h2>
              <div className="cardSubtitle">
                Ejecuta scripts existentes y streamea logs en vivo (SSE).
              </div>
            </div>
            <div className="badge">{runId ? `runId: ${runId}` : "idle"}</div>
          </div>

          <div className="cardBody">
            <div className="row">
              <div className="field">
                <label>Job</label>
                <select
                  className="select"
                  value={job}
                  onChange={(e) => setJob(e.target.value)}
                  disabled={isRunning}
                >
                  {JOBS.map((j) => (
                    <option key={j.key} value={j.key}>
                      {j.label}
                    </option>
                  ))}
                </select>
              </div>

              <div className="field">
                <label>State</label>
                <select
                  className="select"
                  value={stateOut}
                  onChange={(e) => setStateOut(e.target.value)}
                  disabled={isRunning}
                >
                  <option value="all">ALL</option>
                  {statesOut.map((s0) => (
                    <option key={s0} value={s0}>
                      {s0}
                    </option>
                  ))}
                </select>
              </div>

              <div className="field">
                <label>Mode</label>
                <select
                  className="select"
                  value={mode}
                  onChange={(e) => setMode(e.target.value as any)}
                  disabled={isRunning}
                >
                  <option value="dry">Dry Run</option>
                  <option value="live">Live Run</option>
                </select>
              </div>

              <div className="field">
                <label>Debug</label>
                <select
                  className="select"
                  value={debug ? "on" : "off"}
                  onChange={(e) => setDebug(e.target.value === "on")}
                  disabled={isRunning}
                >
                  <option value="on">ON</option>
                  <option value="off">OFF</option>
                </select>
              </div>
            </div>

            <div className="actions">
              <button
                className="btn btnPrimary"
                onClick={run}
                disabled={isRunning}
                title={isRunning ? "Job is running" : "Run"}
              >
                {isRunning ? "Running…" : "Run"}
              </button>

              <button
                className="btn btnDanger"
                onClick={stop}
                disabled={!runId}
                title={!runId ? "No active runId" : "Stop"}
              >
                Stop
              </button>

              <div className="mini" style={{ alignSelf: "center" }}>
                Job: <b>{selectedJob?.label}</b> • State: <b>{stateOut}</b> •
                Mode: <b>{mode}</b>
              </div>
            </div>

            {/* ✅ Pro progress bar */}
            <div className={`runnerProgress ${runnerToneClass}`}>
              <div className="runnerProgressTop">
                <div className="runnerProgressTitle">
                  <span className="runnerDot" />
                  <span className="runnerText">{progress.message || "Idle"}</span>
                </div>

                <div className="runnerProgressMeta">
                  <span className="runnerChip">
                    <b>{pctText}</b>
                  </span>
                  <span className="runnerChip">
                    Done: <b>{runnerMeta.all}</b>
                  </span>

                  {/* ✅ Job-aware chips */}
                  {isStateJob ? (
                    <span className="runnerChip">
                      States: <b>{runnerMeta.all}</b>
                    </span>
                  ) : (
                    <>
                      <span className="runnerChip">
                        Counties: <b>{runnerMeta.counties}</b>
                      </span>
                      <span className="runnerChip">
                        Cities: <b>{runnerMeta.cities}</b>
                      </span>
                    </>
                  )}
                </div>
              </div>

              <div
                className="runnerBar"
                role="progressbar"
                aria-valuenow={Math.round(clamp01(progress.pct) * 100)}
                aria-valuemin={0}
                aria-valuemax={100}
              >
                <div
                  className="runnerBarFill"
                  style={{
                    width: `${Math.round(clamp01(progress.pct) * 100)}%`,
                  }}
                />
              </div>
            </div>
          </div>
        </section>

        {/* Overview KPI */}
        <aside className="card">
          <div className="cardHeader">
            <div>
              <h2 className="cardTitle">Sheet overview</h2>
              <div className="cardSubtitle">
                Live summary from Google Sheets ({sheet?.tabs?.counties || "Counties"}{" "}
                / {sheet?.tabs?.cities || "Cities"}).
              </div>
            </div>

            <button
              className="smallBtn"
              onClick={loadOverview}
              disabled={sheetLoading}
            >
              {sheetLoading ? "Refreshing..." : "Refresh"}
            </button>
          </div>

          <div className="cardBody">
            {sheetErr ? (
              <div className="mini" style={{ color: "var(--danger)" }}>
                ❌ {sheetErr}
              </div>
            ) : (
              <div className="kpiRow kpiRowCompact">
                <div className="kpi">
                  <p className="n">{sheet?.states?.length ?? 0}</p>
                  <p className="l">States in sheet</p>
                </div>

                <div className="kpi">
                  <p className="n">
                    {totals.countiesReady}/{totals.countiesTotal}
                  </p>
                  <p className="l">Counties ready</p>
                </div>

                <div className="kpi">
                  <p className="n">
                    {totals.citiesReady}/{totals.citiesTotal}
                  </p>
                  <p className="l">Cities ready</p>
                </div>

                <div className="kpi">
                  <p className="n">{totals.countiesDomainsActive}</p>
                  <p className="l">County domains active</p>
                </div>

                <div className="kpi">
                  <p className="n">{totals.citiesDomainsActive}</p>
                  <p className="l">City domains active</p>
                </div>
              </div>
            )}
          </div>
        </aside>
      </div>

      {/* Sheet Explorer */}
      <section className="card" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Sheet Explorer</h2>
            <div className="cardSubtitle">
              Estados + progreso de Counties/Cities desde Google Sheets.
            </div>
          </div>

          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            <input
              className="input"
              placeholder="Search state (e.g., Alabama, Florida...)"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              style={{ width: 340 }}
            />
            <div className="badge">{filteredSheetStates.length} shown</div>
          </div>
        </div>

        <div className="cardBody">
          {!sheet ? (
            <div className="mini">
              {sheetLoading ? "Loading sheet overview..." : "No data loaded."}
            </div>
          ) : (
            <div className="tableWrap">
              <table className="table">
                <thead>
                  <tr>
                    <th className="th">State</th>
                    <th className="th">Counties</th>
                    <th className="th">County Domains Activated</th>
                    <th className="th">Cities</th>
                    <th className="th">City Domains Activated</th>
                    <th className="th">Ready %</th>
                    <th className="th" style={{ width: 120 }} />
                  </tr>
                </thead>

                <tbody>
                  {filteredSheetStates.map((r) => {
                    const cTotal = r.counties.total || 0;
                    const ciTotal = r.cities.total || 0;

                    const totalRows = cTotal + ciTotal;

                    const readyDone =
                      (r.counties.ready || 0) + (r.cities.ready || 0);
                    const domainDone =
                      (r.counties.domainsActive || 0) +
                      (r.cities.domainsActive || 0);

                    const denom = totalRows > 0 ? totalRows * 2 : 0;
                    const overall = denom ? (readyDone + domainDone) / denom : 0;

                    const pillClass =
                      overall >= 0.9
                        ? "pillOk"
                        : overall >= 0.4
                        ? "pillWarn"
                        : "pillOff";

                    return (
                      <tr key={r.state} className="tr">
                        <td className="td">
                          <b>{r.state}</b>
                        </td>

                        <td className="td">
                          <span className={pillClass}>
                            {r.counties.ready}/{r.counties.total} ready
                          </span>
                        </td>

                        <td className="td">
                          <span className={pillClass}>
                            {r.counties.domainsActive || 0}/{r.counties.total} active
                          </span>
                        </td>

                        <td className="td">
                          <span className={pillClass}>
                            {r.cities.ready}/{r.cities.total} ready
                          </span>
                        </td>

                        <td className="td">
                          <span className={pillClass}>
                            {r.cities.domainsActive || 0}/{r.cities.total} active
                          </span>
                        </td>

                        <td className="td">
                          <span className={pillClass}>
                            {Math.round(overall * 100)}%
                          </span>
                        </td>

                        <td className="td" style={{ textAlign: "right" }}>
                          <button
                            className="smallBtn"
                            onClick={() => openDetail(r.state)}
                          >
                            View
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          <div className="mini" style={{ marginTop: 10 }}>
            Phase 4+: View → detalle del estado + Domain Activation helper.
          </div>
        </div>
      </section>

      {/* Console */}
      <section className="console">
        <div className="consoleHeader">
          <div>
            <b>Logs</b> <span className="mini">(live)</span>
          </div>
          <div className="badge">{logs.length} lines</div>
        </div>

        <div className="consoleBody">
          {logs.length === 0 ? (
            <div className="mini">Run a job to see live output here.</div>
          ) : (
            logs.map((l, i) => (
              <div key={i} className="logLine">
                {l}
              </div>
            ))
          )}
        </div>
      </section>

      {/* Drawer: State Detail */}
      {openState && (
        <>
          <div className="drawerBackdrop" onClick={closeDetail} />
          <div className="drawer">
            <div className="drawerHeader">
              <div>
                <div className="badge">STATE</div>
                <h2 style={{ marginTop: 6, marginBottom: 0 }}>{openState}</h2>

                <div className="mini" style={{ marginTop: 6 }}>
                  {detail?.tabs ? (
                    <>
                      Tabs: <b>{detail.tabs.counties}</b> /{" "}
                      <b>{detail.tabs.cities}</b>
                    </>
                  ) : (
                    <>Loading…</>
                  )}
                </div>

                <div className="tabs">
                  <button
                    className={`tabBtn ${
                      detailTab === "counties" ? "tabBtnActive" : ""
                    }`}
                    onClick={() => setDetailTab("counties")}
                  >
                    Counties
                  </button>
                  <button
                    className={`tabBtn ${
                      detailTab === "cities" ? "tabBtnActive" : ""
                    }`}
                    onClick={() => setDetailTab("cities")}
                  >
                    Cities
                  </button>
                </div>
              </div>

              <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
                <button
                  className="smallBtn"
                  onClick={() => openDetail(openState)}
                  disabled={detailLoading}
                >
                  {detailLoading ? "Refreshing..." : "Refresh"}
                </button>
                <button className="smallBtn" onClick={closeDetail}>
                  Close
                </button>
              </div>
            </div>

            <div className="drawerBody">
              {detailErr ? (
                <div className="mini" style={{ color: "var(--danger)" }}>
                  ❌ {detailErr}
                </div>
              ) : detailLoading && !detail ? (
                <div className="mini">Loading…</div>
              ) : !detail ? (
                <div className="mini">No detail loaded.</div>
              ) : (
                <>
                  <div className="kpiRow">
                    <div className="kpi">
                      <p className="n">{detail.counties.stats.eligible}</p>
                      <p className="l">Eligible counties</p>
                    </div>
                    <div className="kpi">
                      <p className="n">{detail.cities.stats.eligible}</p>
                      <p className="l">Eligible cities</p>
                    </div>
                  </div>

                  <div
                    style={{
                      display: "flex",
                      gap: 10,
                      alignItems: "center",
                      marginTop: 14,
                    }}
                  >
                    <div className="mini" style={{ minWidth: 110 }}>
                      Filter county
                    </div>

                    <select
                      className="select"
                      value={countyFilter}
                      onChange={(e) => setCountyFilter(e.target.value)}
                      style={{ maxWidth: 360 }}
                    >
                      <option value="all">ALL</option>
                      {(detailTab === "counties"
                        ? detail.counties.counties
                        : detail.cities.counties
                      ).map((c) => (
                        <option key={c} value={c}>
                          {c}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div className="tableWrap tableScrollX" style={{ marginTop: 12 }}>
                    <table
                      className={`table ${
                        detailTab === "cities" ? "tableWideCities" : ""
                      }`}
                    >
                      <thead>
                        <tr>
                          <th className="th">Eligible</th>
                          <th className="th">Active</th>
                          <th className="th">Location Id</th>
                          <th className="th">County</th>
                          {detailTab === "cities" && <th className="th">City</th>}
                          <th className="th">Domain</th>
                          <th className="th">Sitemap</th>
                          <th className="th">Activation</th>
                        </tr>
                      </thead>

                      <tbody>
                        {(detailTab === "counties"
                          ? detail.counties.rows
                          : detail.cities.rows
                        )
                          .filter((r) =>
                            countyFilter === "all"
                              ? true
                              : String(r["County"] || "").trim() === countyFilter
                          )
                          .map((r, i) => {
                            const eligible = !!r.__eligible;
                            const locId = s(r["Location Id"]);
                            const county = s(r["County"]);
                            const city = s(r["City"]);

                            const domainCreated = isTrue(r["Domain Created"]);
                            const activationUrl = s(r["Domain URL Activation"]);

                            const domainToPaste =
                              detailTab === "cities"
                                ? s(r["City Domain"]) || s(r["city domain"])
                                : s(r["Domain"]) || s(r["County Domain"]);

                            const domainForOpen =
                              detailTab === "counties"
                                ? s(r["Domain"]) || s(r["County Domain"]) || ""
                                : s(r["City Domain"]) || s(r["County Domain"]) || "";

                            const sitemap = s(r["Sitemap"]);
                            const domainUrl = toUrlMaybe(domainForOpen);

                            const title =
                              detailTab === "cities"
                                ? `${openState} • ${county || "County"} • ${
                                    city || "City"
                                  }`
                                : `${openState} • ${county || "County"}`;

                            const accountName = s(r["Account Name"]);
                            const timezone = s(r["Timezone"]);

                            return (
                              <tr
                                key={i}
                                className={`tr ${eligible ? "rowEligible" : ""}`}
                              >
                                <td className="td">{eligible ? "✅" : "—"}</td>

                                <td className="td">
                                  {domainCreated ? (
                                    <span className="pillOk">Active</span>
                                  ) : (
                                    <span className="pillOff">Pending</span>
                                  )}
                                </td>

                                <td className="td">
                                  <span className="mini">{locId || "—"}</span>
                                </td>

                                <td className="td">{county || "—"}</td>

                                {detailTab === "cities" && (
                                  <td className="td">{city || "—"}</td>
                                )}

                                <td className="td">
                                  {domainUrl ? (
                                    <a
                                      className="link"
                                      href={domainUrl}
                                      target="_blank"
                                      rel="noreferrer"
                                    >
                                      Open
                                    </a>
                                  ) : (
                                    <span className="mini">—</span>
                                  )}
                                </td>

                                <td className="td">
                                  {sitemap ? (
                                    <a
                                      className="link"
                                      href={sitemap}
                                      target="_blank"
                                      rel="noreferrer"
                                    >
                                      Open
                                    </a>
                                  ) : (
                                    <span className="mini">—</span>
                                  )}
                                </td>

                                <td className="td">
                                  <div className="rowActions">
                                    <button
                                      className="smallBtn"
                                      onClick={() =>
                                        openActivationHelper({
                                          title,
                                          domainToPaste,
                                          activationUrl,
                                          isActive: domainCreated,
                                          accountName,
                                          timezone,
                                          sitemapUrl: sitemap,
                                        })
                                      }
                                    >
                                      Activate
                                    </button>
                                  </div>
                                </td>
                              </tr>
                            );
                          })}
                      </tbody>
                    </table>
                  </div>

                  <div className="mini" style={{ marginTop: 10 }}>
                    Activation helper usa: <b>Domain URL Activation</b> + el domain a
                    pegar (<b>City Domain</b> o <b>Domain</b>).
                  </div>
                </>
              )}
            </div>
          </div>
        </>
      )}

      {/* Activation Modal */}
      {actOpen && (
        <>
          <div className="modalBackdrop" onClick={closeActivationHelper} />
          <div className="modal modalPro">
            <div className="modalHeader modalHeaderPro">
              <div className="modalHeadLeft">
                <div className="badge">DOMAIN ACTIVATION</div>

                <div className="modalTitleRow">
                  <h3 className="modalTitle">{actTitle}</h3>
                  <div className="modalStatus">
                    {actIsActive ? (
                      <span className="pillOk">Active</span>
                    ) : (
                      <span className="pillOff">Pending</span>
                    )}
                  </div>
                </div>

                <div className="modalMeta">
                  {actAccountName ? (
                    <div className="metaItem">
                      <div className="metaLabel">GHL Subaccount</div>
                      <div className="metaValue">{actAccountName}</div>
                    </div>
                  ) : null}

                  {actTimezone ? (
                    <div className="metaItem">
                      <div className="metaLabel">Timezone</div>
                      <div className="metaValue">{actTimezone}</div>
                    </div>
                  ) : null}
                </div>
              </div>

              <div className="modalHeaderActions">
                <button className="smallBtn" onClick={closeActivationHelper}>
                  Close
                </button>
              </div>
            </div>

            <div className="modalBody modalBodyPro">
              <div className="modalGrid">
                {/* Left */}
                <div className="modalLeft">
                  <div className="sectionTitle">Domain to paste</div>
                  <div className="sectionHint">
                    Click to copy (pégalo en GHL field{" "}
                    <span className="kbd">Domain</span>)
                  </div>

                  <button
                    className="copyField"
                    onClick={copyDomain}
                    disabled={!actDomainToPaste}
                    title="Click to copy"
                    style={{ opacity: actDomainToPaste ? 1 : 0.55 }}
                  >
                    <div className="copyFieldTop">
                      <code className="copyValue">{actDomainToPaste || "—"}</code>
                      <span
                        className={`copyBadge ${actCopied ? "copyBadgeOn" : ""}`}
                        style={{ color: "#FFFF" }}
                      >
                        {actCopied ? "Copied ✅" : "Copy"}
                      </span>
                    </div>
                    <div className="copyFieldSub">
                      Tip: si pega raro, haz click nuevamente (clipboard).
                    </div>
                  </button>

                  <div className="modalQuickActions">
                    <button
                      className="btn btnPrimary"
                      onClick={copyDomain}
                      disabled={!actDomainToPaste}
                    >
                      {actCopied ? "Copied ✅" : "Copy Domain"}
                    </button>

                    {actActivationUrl ? (
                      <a
                        className="btn"
                        href={actActivationUrl}
                        target="_blank"
                        rel="noreferrer"
                      >
                        Open Activation
                      </a>
                    ) : (
                      <button className="btn" disabled>
                        Open Activation
                      </button>
                    )}

                    {actDomainToPaste ? (
                      <a
                        className="btn"
                        href={toUrlMaybe(actDomainToPaste)}
                        target="_blank"
                        rel="noreferrer"
                      >
                        Open Website
                      </a>
                    ) : (
                      <button className="btn" disabled>
                        Open Website
                      </button>
                    )}
                  </div>
                </div>

                {/* Right */}
                <div className="modalRight">
                  <div className="stepCard">
                    <div className="stepCardHeader stepCardHeaderTabs">
                      <div className="stepPill">Checklist</div>

                      <div className="stepTabs">
                        <button
                          className={`stepTab ${
                            actChecklistTab === "domain" ? "stepTabOn" : ""
                          }`}
                          onClick={() => setActChecklistTab("domain")}
                          type="button"
                        >
                          Domain
                        </button>
                        <button
                          className={`stepTab ${
                            actChecklistTab === "sitemap" ? "stepTabOn" : ""
                          }`}
                          onClick={() => setActChecklistTab("sitemap")}
                          type="button"
                        >
                          Sitemap
                        </button>
                        <button
                          className={`stepTab ${
                            actChecklistTab === "robots" ? "stepTabOn" : ""
                          }`}
                          onClick={() => setActChecklistTab("robots")}
                          type="button"
                        >
                          Robots.txt
                        </button>
                      </div>
                    </div>

                    {actChecklistTab === "domain" && (
                      <>
                        <div className="stepName">Domain Activation Steps</div>
                        <ol className="stepsList" style={{ marginTop: 10 }}>
                          <li>Open the activation link.</li>
                          <li>
                            Paste the domain into{" "}
                            <span className="kbd">Domain</span>.
                          </li>
                          <li>
                            Click <span className="kbd">Continue</span>.
                          </li>
                          <li>
                            Select <span className="kbd">Add Record Manually</span>.
                          </li>
                          <li>
                            Select <span className="kbd">Verify Record</span>.
                          </li>
                          <li>
                            DNS updates could require up to{" "}
                            <span className="kbd">30 seconds</span>. If changes
                            aren’t acknowledged, <span className="kbd">retry</span>.
                          </li>
                          <li>
                            Select <span className="kbd">Website</span>.
                          </li>
                          <li>
                            In <span className="kbd">Link domain with website</span>,
                            choose <span className="kbd">County</span>.
                          </li>
                          <li>
                            In{" "}
                            <span className="kbd">
                              Select default step/page for Domain
                            </span>
                            , choose <span className="kbd">** Home Page</span>.
                          </li>
                          <li>
                            Select <span className="kbd">Proceed to finish</span>.
                          </li>
                        </ol>
                      </>
                    )}

                    {actChecklistTab === "sitemap" && (
                      <>
                        <div className="stepName">Sitemap Setup</div>
                        <div className="mini" style={{ marginTop: 8 }}>
                          Objetivo: asegurar que el subaccount tenga el sitemap
                          correcto y accesible públicamente.
                        </div>

                        <div className="miniCardGrid">
                          <div className="miniCard">
                            <div className="miniCardLabel">Sitemap URL (sheet)</div>
                            <div className="miniCardValue">
                              {actSitemapUrl ? (
                                <a
                                  className="link"
                                  href={actSitemapUrl}
                                  target="_blank"
                                  rel="noreferrer"
                                  title={actSitemapUrl}
                                >
                                  {actSitemapUrl}
                                </a>
                              ) : (
                                <span className="mini">—</span>
                              )}
                            </div>
                          </div>

                          <div className="miniCard miniCardAction">
                            <div className="miniCardLabel">Quick action</div>
                            {actSitemapUrl ? (
                              <a
                                className="qaBtn"
                                href={actSitemapUrl}
                                target="_blank"
                                rel="noreferrer"
                                style={{ fontSize: 13 }}
                              >
                                Open Sitemap
                              </a>
                            ) : (
                              <button className="qaBtn" disabled>
                                Open Sitemap
                              </button>
                            )}
                          </div>
                        </div>

                        <ol className="stepsList" style={{ marginTop: 12 }}>
                          <li>
                            Go to <span className="kbd">Domain & URL Redirects</span>{" "}
                            in GHL.
                          </li>
                          <li>
                            In External Domains, select{" "}
                            <span className="kbd">Manage</span>.
                          </li>
                          <li>
                            Then select <span className="kbd">⋮</span> on your right.
                          </li>
                          <li>
                            Then select{" "}
                            <span className="kbd">{`<> XML Sitemap`}</span>.
                          </li>
                          <li>
                            Open the <span className="kbd">County</span> option and
                            select only pages starting with{" "}
                            <span className="kbd">**</span>.
                          </li>
                          <li>
                            Then click <span className="kbd">Proceed</span>.
                          </li>
                          <li>
                            Then click <span className="kbd">Generate & Save</span>.
                          </li>
                          <li>
                            Then click <span className="kbd">Okay</span>.
                          </li>
                        </ol>
                      </>
                    )}

                    {actChecklistTab === "robots" && (
                      <>
                        <div className="robotsHeaderRow">
                          <div>
                            <div className="stepName">Robots.txt</div>
                            <div className="mini" style={{ marginTop: 6 }}>
                              Copia y pega este robots.txt en GHL (respetando el
                              formato). Ya incluye el sitemap al final.
                            </div>
                          </div>

                        <button
                          className={`smallBtn ${
                            robotsCopied ? "smallBtnOn" : ""
                          }`}
                          onClick={copyRobots}
                          type="button"
                          disabled={!robotsTxt}
                          title="Copy robots.txt"
                        >
                          {robotsCopied ? "Copied ✅" : "Copy Robots"}
                        </button>
                        </div>

                        <div className="robotsBox" role="region" aria-label="robots.txt">
                          <pre className="robotsPre">{robotsTxt}</pre>
                        </div>

                        <ol className="stepsList" style={{ marginTop: 12 }}>
                          <li>
                            En GHL, abre el área donde se edita{" "}
                            <span className="kbd">robots.txt</span>.
                          </li>
                          <li>Pega el contenido completo (sin modificar tabs/espacios).</li>
                          <li>
                            Verifica que la línea <span className="kbd">Sitemap:</span>{" "}
                            apunte al sitemap del subaccount.
                          </li>
                          <li>
                            Guarda y valida en el browser que{" "}
                            <span className="kbd">/robots.txt</span> responda 200 OK.
                          </li>
                        </ol>
                      </>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
