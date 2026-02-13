// src/app/page.tsx
"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import UsaChoroplethProgressMap from "@/components/UsaChoroplethProgressMap";
import Link from "next/link";

const JOBS = [
  { key: "build-sheet-rows", label: "Create DB" },
  { key: "build-counties", label: "Create Subaccount Json" },
  { key: "run-delta-system", label: "Run Delta System" },
  { key: "build-state-sitemaps", label: "Create Sitemaps" },
  { key: "build-state-index", label: "Create Search Index" },
  { key: "update-custom-values", label: "Update Custom Values (From Sheet)" },
  { key: "update-custom-values-one", label: "Update Custom Values (One)" },
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

function formatStateLabel(raw: string) {
  const cleaned = s(raw).replace(/[-_]+/g, " ").replace(/\s+/g, " ").trim();
  if (!cleaned) return "";
  return cleaned
    .split(" ")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(" ");
}

function fmtInt(value: number) {
  const n = Number(value || 0);
  return Number.isFinite(n) ? n.toLocaleString("en-US") : "0";
}

function fmtPair(a: number, b: number) {
  return `${fmtInt(a)}/${fmtInt(b)}`;
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

type ChecklistTabKey = "domain" | "sitemap" | "robots" | "headers";
type HeadersSubTabKey = "head" | "footer" | "favicon";

type HeadersPayload = {
  head: string;
  footer: string;
  favicon: string;
  source?: { row?: number | null; key?: string };
  cols?: {
    locationId?: string;
    head?: string;
    footer?: string;
    favicon?: string;
  };
};

type SitemapVerifyResponse = {
  ok: boolean;
  health?: "green" | "yellow" | "red";
  summary?: string;
  active?: boolean;
  matches?: boolean;
  expectedHost?: string;
  responseHost?: string;
  responseStatus?: number;
  requestedPath?: string;
  responsePath?: string;
  contentType?: string;
  pathMatchesSitemap?: boolean;
  xmlDetected?: boolean;
  blockedByProtection?: boolean;
  checks?: {
    statusOk?: boolean;
    pathIsSitemapXml?: boolean;
    xmlDetected?: boolean;
    hostMatches?: boolean;
    protectedByWaf?: boolean;
  };
  sampleHosts?: string[];
  checkedAt?: string;
  error?: string;
};

type IndexSubmitResponse = {
  ok: boolean;
  target: "google";
  domainUrl?: string;
  host?: string;
  google?: {
    ok: boolean;
    mode?: "inspect" | "discovery";
    status?: number;
    siteUrl?: string;
    siteProperty?: string;
    fetch?: {
      status?: number;
      finalUrl?: string;
      contentType?: string;
      error?: string;
    };
    inspection?: {
      verdict?: string;
      coverageState?: string;
      indexingState?: string;
      lastCrawlTime?: string;
      robotsTxtState?: string;
    };
    discovery?: {
      attempted: boolean;
      sitemapUrl: string;
      submitted: boolean;
      submittedBy?: string;
      submitError?: string;
    };
    bodyPreview?: string;
    error?: string;
  };
  error?: string;
};

type TabSitemapResultItem = {
  key: string;
  rowName: string;
  domainUrl: string;
  ok: boolean;
  error?: string;
};

type TabSitemapReport = {
  kind: "counties" | "cities";
  action: "inspect" | "discovery";
  total: number;
  success: number;
  failed: number;
  mode: "all" | "retry";
  items: TabSitemapResultItem[];
  updatedAt: string;
};

type TabSitemapRunItem = {
  key: string;
  rowName: string;
  domainUrl: string;
  status: "pending" | "running" | "done" | "failed";
  error?: string;
};

type TabGoogleAction = "inspect" | "discovery";

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
  if (n >= 0 && n <= 1) return clamp01(n);
  if (n >= 0 && n <= 100) return clamp01(n / 100);
  return null;
}

export default function Home() {
  type CelebrateParticle = {
    kind: "rocket" | "spark";
    originX: number;
    tx: number;
    ty: number;
    size: number;
    delay: number;
    duration: number;
    spin: number;
    hue: number;
    alpha: number;
  };

  const [statesOut, setStatesOut] = useState<string[]>([]);
  const [job, setJob] = useState(JOBS[0].key);
  const [stateOut, setStateOut] = useState<string>("all");
  const [mode, setMode] = useState<"dry" | "live">("live");
  const [debug, setDebug] = useState(true);

  // ✅ Runner params for single location jobs
  const [runLocId, setRunLocId] = useState("");
  const [runKind, setRunKind] = useState<"" | "counties" | "cities">("");

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
  const [detailSearch, setDetailSearch] = useState("");
  const [tabSitemapSubmitting, setTabSitemapSubmitting] = useState("");
  const [tabSitemapStatus, setTabSitemapStatus] = useState<{
    kind: "counties" | "cities";
    ok: boolean;
    message: string;
  } | null>(null);
  const [tabSitemapReports, setTabSitemapReports] = useState<
    Record<string, TabSitemapReport>
  >({});
  const [tabSitemapShowDetails, setTabSitemapShowDetails] = useState<
    Record<string, boolean>
  >({});
  const [tabSitemapRunOpen, setTabSitemapRunOpen] = useState(false);
  const [tabSitemapRunKind, setTabSitemapRunKind] = useState<
    "counties" | "cities"
  >("counties");
  const [tabSitemapRunAction, setTabSitemapRunAction] =
    useState<TabGoogleAction>("inspect");
  const [tabSitemapRunMode, setTabSitemapRunMode] = useState<"all" | "retry">(
    "all",
  );
  const [tabSitemapRunItems, setTabSitemapRunItems] = useState<
    TabSitemapRunItem[]
  >([]);
  const [tabSitemapRunDone, setTabSitemapRunDone] = useState(false);
  const [tabSitemapRunStartedAt, setTabSitemapRunStartedAt] = useState("");

  const [actOpen, setActOpen] = useState(false);
  const [actTitle, setActTitle] = useState("");
  const [actDomainToPaste, setActDomainToPaste] = useState("");
  const [actActivationUrl, setActActivationUrl] = useState("");
  const [actIsActive, setActIsActive] = useState<boolean>(false);
  const [actCopied, setActCopied] = useState<boolean>(false);

  // ✅ Website URL (Open Website button in modal)
  const [actWebsiteUrl, setActWebsiteUrl] = useState("");

  // extra meta
  const [actAccountName, setActAccountName] = useState("");
  const [actTimezone, setActTimezone] = useState("");

  // sitemap + robots in modal
  const [actSitemapUrl, setActSitemapUrl] = useState("");
  const [actSitemapChecking, setActSitemapChecking] = useState(false);
  const [actSitemapVerify, setActSitemapVerify] = useState<SitemapVerifyResponse | null>(null);
  const [actIndexing, setActIndexing] = useState<boolean>(false);
  const [actIndexResult, setActIndexResult] = useState<IndexSubmitResponse | null>(null);
  const [actChecklistTab, setActChecklistTab] =
    useState<ChecklistTabKey>("domain");
  const [robotsCopied, setRobotsCopied] = useState(false);

  // ✅ Headers tab states
  const [actHeaders, setActHeaders] = useState<HeadersPayload | null>(null);
  const [actHeadersLoading, setActHeadersLoading] = useState(false);
  const [actHeadersErr, setActHeadersErr] = useState("");
  const [actHeadersTab, setActHeadersTab] = useState<HeadersSubTabKey>("head");
  const [actHeadersCopied, setActHeadersCopied] = useState(false);

  // ✅ Complete states
  const [actLocId, setActLocId] = useState("");
  const [actMarking, setActMarking] = useState(false);
  const [actMarkErr, setActMarkErr] = useState("");
  const [actMarkDone, setActMarkDone] = useState(false);
  const [actKind, setActKind] = useState<"" | "counties" | "cities">("");
  const [actCelebrateOn, setActCelebrateOn] = useState(false);
  const [actCelebrateKey, setActCelebrateKey] = useState(0);

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

  // ✅ Map modal
  const [mapOpen, setMapOpen] = useState(false);

  type MapMetric = "ready" | "domains";
  const [mapMetric, setMapMetric] = useState<MapMetric>("ready");
  const [mapSelected, setMapSelected] = useState<string>("");
  const celebrateTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const celebrationParticles = useMemo<CelebrateParticle[]>(() => {
    const palette = [198, 160, 46, 278, 332, 118, 24, 214];

    const rockets = Array.from({ length: 58 }, (_, i) => {
      const lane = i % 9;
      const wave = Math.floor(i / 9);
      const hue = palette[i % palette.length];
      const originX = 6 + lane * 10.6 + (wave % 2 ? 1.9 : 0);
      const dir = lane % 2 === 0 ? -1 : 1;
      const tx = dir * (36 + (i % 6) * 13);
      const ty = -(360 + (wave % 5) * 105 + (i % 4) * 34);
      const size = 4 + (i % 4);
      const delay = wave * 0.055 + (i % 3) * 0.014;
      const duration = 1.15 + (i % 6) * 0.1;
      const spin = -34 + (i % 9) * 8;
      const alpha = 0.74 + (i % 4) * 0.06;
      return {
        kind: "rocket" as const,
        originX,
        tx,
        ty,
        size,
        delay,
        duration,
        spin,
        hue,
        alpha,
      };
    });

    const sparks = Array.from({ length: 86 }, (_, i) => {
      const lane = i % 11;
      const wave = Math.floor(i / 11);
      const hue = palette[(i + 3) % palette.length];
      const originX = 4 + lane * 9 + (wave % 2 ? 3.1 : 0.4);
      const dir = lane % 2 === 0 ? -1 : 1;
      const tx = dir * (65 + (i % 8) * 14);
      const ty = -(240 + (wave % 4) * 72 + (i % 5) * 20);
      const size = 2 + (i % 3);
      const delay = 0.08 + wave * 0.05 + (i % 4) * 0.012;
      const duration = 0.88 + (i % 5) * 0.08;
      const spin = -58 + (i % 12) * 10;
      const alpha = 0.62 + (i % 5) * 0.05;
      return {
        kind: "spark" as const,
        originX,
        tx,
        ty,
        size,
        delay,
        duration,
        spin,
        hue,
        alpha,
      };
    });

    return rockets.concat(sparks);
  }, []);

  function openMap() {
    setMapOpen(true);
  }
  function closeMap() {
    setMapOpen(false);
  }

  function pushLog(line: string) {
    const msg = `[${tsLocal()}] ${String(line ?? "")}`;
    setLogs((p) =>
      p.length > 4000 ? p.slice(-3500).concat(msg) : p.concat(msg),
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

  // ✅ States list source depends on Job:
  // - build-counties (Create Subaccount Json) => resources/statesFiles/*.json
  // - everything else => scripts/out/<slug>/<slug>.json (current behavior)
  useEffect(() => {
    let ignore = false;

    const source = job === "build-counties" ? "resources" : "out";
    const url =
      source === "resources" ? "/api/states?source=resources" : "/api/states";

    fetch(url)
      .then((r) => r.json())
      .then((d) => {
        if (ignore) return;
        const next = Array.isArray(d?.states) ? d.states : [];
        setStatesOut(next);

        // ✅ If current selected state is not available in new list, reset safely
        if (stateOut !== "all" && stateOut && !next.includes(stateOut)) {
          setStateOut("all");
        }
      })
      .catch(() => {
        if (!ignore) setStatesOut([]);
      });

    return () => {
      ignore = true;
    };
  }, [job]); // 👈 only depends on job (minimal change)

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

  const isStateJob = useMemo(() => {
    return job === "build-state-sitemaps" || job === "build-state-index";
  }, [job]);

  const isOneLocJob = useMemo(() => {
    return job === "update-custom-values-one";
  }, [job]);

  const runScopeTotals = useMemo<RunnerTotals>(() => {
    const rows = sheet?.states || [];
    if (!rows.length) return { allTotal: 0, countiesTotal: 0, citiesTotal: 0 };

    // ✅ single loc job is always 1 unit (for UX only)
    if (isOneLocJob) return { allTotal: 1, countiesTotal: 0, citiesTotal: 0 };

    if (isStateJob) {
      if (stateOut === "all")
        return { allTotal: rows.length, countiesTotal: 0, citiesTotal: 0 };
      return { allTotal: 1, countiesTotal: 0, citiesTotal: 0 };
    }

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
  }, [
    sheet,
    stateOut,
    totals.countiesTotal,
    totals.citiesTotal,
    isStateJob,
    isOneLocJob,
  ]);

  // ✅ DERIVED METRICS PER STATE (single source of truth)
  const stateMetrics = useMemo(() => {
    const rows = sheet?.states || [];
    const map: Record<
      string,
      {
        readyPct: number;
        domainsPct: number;
        countiesReady: number;
        countiesTotal: number;
        citiesReady: number;
        citiesTotal: number;
        countiesDomains: number;
        citiesDomains: number;
      }
    > = {};

    for (const r of rows) {
      const countiesTotal = r.counties.total || 0;
      const citiesTotal = r.cities.total || 0;

      const countiesReady = r.counties.ready || 0;
      const citiesReady = r.cities.ready || 0;

      const countiesDomains = r.counties.domainsActive || 0;
      const citiesDomains = r.cities.domainsActive || 0;

      const denom = countiesTotal + citiesTotal;

      map[r.state] = {
        readyPct: denom ? (countiesReady + citiesReady) / denom : 0,
        domainsPct: denom ? (countiesDomains + citiesDomains) / denom : 0,
        countiesReady,
        countiesTotal,
        citiesReady,
        citiesTotal,
        countiesDomains,
        citiesDomains,
      };
    }

    return map;
  }, [sheet]);

  const selectedStateMetrics = useMemo(() => {
    if (!mapSelected) return null;
    return stateMetrics[mapSelected] || null;
  }, [mapSelected, stateMetrics]);

  const tabRunKey = (
    kind: "counties" | "cities",
    action: TabGoogleAction,
  ) => `${kind}:${action}`;
  const currentTabRunKey = tabRunKey(detailTab, tabSitemapRunAction);

  const currentTabSitemapReport = useMemo(
    () => tabSitemapReports[currentTabRunKey],
    [tabSitemapReports, currentTabRunKey],
  );

  const tabSitemapRunCounts = useMemo(() => {
    let pending = 0;
    let running = 0;
    let done = 0;
    let failed = 0;
    for (const it of tabSitemapRunItems) {
      if (it.status === "pending") pending += 1;
      else if (it.status === "running") running += 1;
      else if (it.status === "done") done += 1;
      else if (it.status === "failed") failed += 1;
    }
    const total = tabSitemapRunItems.length;
    const completed = done + failed;
    const pct = total > 0 ? Math.round((completed / total) * 100) : 0;
    return { pending, running, done, failed, total, completed, pct };
  }, [tabSitemapRunItems]);

  // ✅ Puerto Rico metrics (separado)
  const prMetrics = useMemo(() => {
    const rows = sheet?.states || [];
    const prRow =
      rows.find((r) => r.state === "Puerto Rico") ||
      rows.find((r) => r.state === "PR");

    if (!prRow) return null;

    const countiesTotal = prRow.counties.total || 0;
    const citiesTotal = prRow.cities.total || 0;

    const countiesReady = prRow.counties.ready || 0;
    const citiesReady = prRow.cities.ready || 0;

    const countiesDomains = prRow.counties.domainsActive || 0;
    const citiesDomains = prRow.cities.domainsActive || 0;

    const denom = countiesTotal + citiesTotal;

    return {
      state: prRow.state,
      readyPct: denom ? (countiesReady + citiesReady) / denom : 0,
      domainsPct: denom ? (countiesDomains + citiesDomains) / denom : 0,
      countiesReady,
      countiesTotal,
      citiesReady,
      citiesTotal,
      countiesDomains,
      citiesDomains,
    };
  }, [sheet]);

  async function loadHeadersForLocation(locId: string) {
    const id = s(locId);
    if (!id) return;

    setActHeaders(null);
    setActHeadersErr("");
    setActHeadersLoading(true);

    try {
      const res = await fetch(
        `/api/sheet/headers?locId=${encodeURIComponent(id)}`,
        { cache: "no-store" },
      );
      const data = await safeJson(res);

      if (!res.ok || (data as any)?.error) {
        throw new Error((data as any)?.error || `HTTP ${res.status}`);
      }

      setActHeaders({
        head: s((data as any)?.head),
        footer: s((data as any)?.footer),
        favicon: s((data as any)?.favicon),
        source: (data as any)?.source,
        cols: (data as any)?.cols,
      });
    } catch (e: any) {
      setActHeadersErr(e?.message || "Failed to load Headers tab");
    } finally {
      setActHeadersLoading(false);
    }
  }

  // ✅ Mark Domain Created TRUE and refresh UI (CORRECT payload)
  async function markDomainCreatedTrue() {
    const locId = s(actLocId);
    if (!locId) return;

    if (actIsActive) {
      setActMarkDone(true);
      triggerActivationCelebrate();
      setTimeout(() => setActMarkDone(false), 900);
      return;
    }

    setActMarkErr("");
    setActMarkDone(false);
    setActMarking(true);

    try {
      const res = await fetch("/api/sheet/domain-created", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        cache: "no-store",
        body: JSON.stringify({ locId, value: true, kind: actKind }),
      });

      const data = await safeJson(res);

      if (!res.ok || (data as any)?.error) {
        throw new Error((data as any)?.error || `HTTP ${res.status}`);
      }

      setActIsActive(true);
      setActMarkDone(true);
      triggerActivationCelebrate();

      const keepTab = detailTab;
      await loadOverview();
      if (openState) {
        await openDetail(openState);
        setDetailTab(keepTab);
      }

      setTimeout(() => setActMarkDone(false), 1400);
    } catch (e: any) {
      setActMarkErr(e?.message || "Failed to mark Domain Created");
    } finally {
      setActMarking(false);
    }
  }

  function triggerActivationCelebrate() {
    if (celebrateTimerRef.current) {
      clearTimeout(celebrateTimerRef.current);
      celebrateTimerRef.current = null;
    }
    setActCelebrateKey((n) => n + 1);
    setActCelebrateOn(true);
    celebrateTimerRef.current = setTimeout(() => {
      setActCelebrateOn(false);
      celebrateTimerRef.current = null;
    }, 1550);
  }

  // ✅ Unified runner (supports optional locId/kind)
  async function run(opts?: { job?: string; locId?: string; kind?: string }) {
    if (isRunning) return;

    const jobKey = s(opts?.job || job);
    const locId = s(opts?.locId || (isOneLocJob ? runLocId : ""));
    const kind = s(opts?.kind || (isOneLocJob ? runKind : ""));
    const metaState =
      jobKey === "update-custom-values-one" ? s(openState) || "one" : stateOut;

    if (jobKey === "update-custom-values-one" && !locId) {
      pushLog("❌ Missing locId for update-custom-values-one");
      return;
    }

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
        `▶ Starting job="${jobKey}" state="${metaState}" mode="${mode}" debug="${debug ? "on" : "off"}"${
          locId ? ` extra={locId:"${locId}",kind:"${kind || "auto"}"}` : ""
        }...`,
      );

      const res = await fetch("/api/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          job: jobKey,
          state: metaState, // ✅ PRO
          mode,
          debug,
          locId: locId || "",
          kind: kind || "",
        }),
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
        if (
          raw.startsWith("__PROGRESS__ ") ||
          raw.startsWith("__PROGRESS_INIT__ ") ||
          raw.startsWith("__PROGRESS_END__ ")
        ) {
          return;
        }
        pushLog(raw);
      };

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

        const pctFromPayload = normalizePct(data?.pct);
        const pctComputed = totalsAll > 0 ? clamp01(doneAll / totalsAll) : 0;
        const pctFinal =
          typeof pctFromPayload === "number" ? pctFromPayload : pctComputed;

        setProgressTotals((prev) => ({
          allTotal: totalsAll || prev.allTotal || runScopeTotals.allTotal,
          countiesTotal:
            totalsCounties ||
            prev.countiesTotal ||
            runScopeTotals.countiesTotal,
          citiesTotal:
            totalsCities || prev.citiesTotal || runScopeTotals.citiesTotal,
        }));

        const startedAt = runStartedAtRef.current;
        let etaSec: number | null = null;
        if (startedAt && totalsAll > 0 && doneAll > 0) {
          const elapsedSec = (Date.now() - startedAt) / 1000;
          const rate = doneAll / Math.max(0.5, elapsedSec);
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
        const msTxt =
          ms === null ? "" : ` • duration=${(ms / 1000).toFixed(2)}s`;

        pushLog(
          `🏁 END ${
            typeof data === "object" ? JSON.stringify(data) : String(data)
          }${msTxt}`,
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
          "⚠ SSE error / disconnected. (If job still running, refresh or check server logs.)",
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
    setTabSitemapSubmitting("");
    setTabSitemapStatus(null);
    setTabSitemapReports({});
    setTabSitemapShowDetails({});
    setTabSitemapRunOpen(false);
    setTabSitemapRunAction("inspect");
    setTabSitemapRunItems([]);
    setTabSitemapRunDone(false);
    setCountyFilter("all");
    setDetailSearch("");
    setDetailTab("counties");
    setDetailLoading(true);

    try {
      const res = await fetch(
        `/api/sheet/state?name=${encodeURIComponent(stateName)}`,
        {
          cache: "no-store",
        },
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
    setTabSitemapSubmitting("");
    setTabSitemapStatus(null);
    setTabSitemapReports({});
    setTabSitemapShowDetails({});
    setTabSitemapRunOpen(false);
    setTabSitemapRunAction("inspect");
    setTabSitemapRunItems([]);
    setTabSitemapRunDone(false);
  }

  function openActivationHelper(opts: {
    title: string;
    domainToPaste: string;
    activationUrl: string;
    isActive: boolean;
    accountName?: string;
    timezone?: string;
    sitemapUrl?: string;
    locId?: string;
    kind?: "counties" | "cities";
  }) {
    setActTitle(opts.title);
    setActDomainToPaste(opts.domainToPaste);
    setActActivationUrl(opts.activationUrl);
    setActIsActive(opts.isActive);

    setActAccountName(s(opts.accountName));
    setActTimezone(s(opts.timezone));

    setActSitemapUrl(s(opts.sitemapUrl));
    setActSitemapVerify(null);
    setActSitemapChecking(false);
    setActIndexing(false);
    setActIndexResult(null);
    setActChecklistTab("domain");

    setActWebsiteUrl(toUrlMaybe(opts.domainToPaste));

    setActCopied(false);
    setRobotsCopied(false);

    setActHeaders(null);
    setActHeadersErr("");
    setActHeadersTab("favicon");
    setActHeadersCopied(false);

    const lid = s(opts.locId);
    setActLocId(lid);
    setActKind((opts.kind as any) || "");
    setActMarking(false);
    setActMarkErr("");
    setActMarkDone(false);

    if (lid) loadHeadersForLocation(lid);

    setActOpen(true);
  }

  async function verifySitemap() {
    const sitemapUrl = s(actSitemapUrl);
    if (!sitemapUrl) {
      setActSitemapVerify({
        ok: false,
        error: "Missing sitemap URL.",
      });
      return;
    }

    const expectedDomain = s(actDomainToPaste);
    setActSitemapChecking(true);
    setActSitemapVerify(null);

    try {
      const qs = new URLSearchParams();
      qs.set("url", sitemapUrl);
      if (expectedDomain) qs.set("expectedDomain", expectedDomain);

      const res = await fetch(`/api/tools/sitemap-verify?${qs.toString()}`, {
        cache: "no-store",
      });
      const data = (await safeJson(res)) as SitemapVerifyResponse | null;
      if (!data || !res.ok || !data.ok) {
        setActSitemapVerify({
          ok: false,
          error: data?.error || `HTTP ${res.status}`,
        });
        return;
      }
      setActSitemapVerify(data);
    } catch (e: any) {
      setActSitemapVerify({
        ok: false,
        error: e?.message || "Failed to verify sitemap.",
      });
    } finally {
      setActSitemapChecking(false);
    }
  }

  async function submitGoogleIndex() {
    const domainUrl = s(toUrlMaybe(s(actWebsiteUrl) || s(actDomainToPaste)));
    if (!domainUrl) {
      setActIndexResult({
        ok: false,
        target: "google",
        error: "Missing domain URL.",
      });
      return;
    }
    setActIndexing(true);
    setActIndexResult(null);
    try {
      const res = await fetch("/api/tools/index-submit", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          target: "google",
          domainUrl,
          mode: "inspect",
        }),
      });
      const data = (await safeJson(res)) as IndexSubmitResponse | null;
      if (!data || !res.ok || !data.ok) {
        setActIndexResult({
          ok: false,
          target: "google",
          error: data?.error || `HTTP ${res.status}`,
        });
        return;
      }
      setActIndexResult(data);
    } catch (e: any) {
      setActIndexResult({
        ok: false,
        target: "google",
        error: e?.message || "Index submit failed.",
      });
    } finally {
      setActIndexing(false);
    }
  }

  function getActiveRowsForTab(kind: "counties" | "cities"): any[] {
    if (!detail) return [];
    const rows =
      kind === "counties"
        ? (detail.counties.rows || [])
        : (detail.cities.rows || []);

    return rows.filter((r) => {
      const eligible = !!r.__eligible;
      const isActive = isTrue(r["Domain Created"]);
      const domainToPaste =
        kind === "cities"
          ? s(r["City Domain"]) || s(r["city domain"])
          : s(r["Domain"]) || s(r["County Domain"]);
      const locId = s(r["Location Id"]);
      return eligible && isActive && !!domainToPaste && !!locId;
    });
  }

  function getTabRowName(kind: "counties" | "cities", r: any) {
    return kind === "cities" ? s(r["City"]) || s(r["County"]) : s(r["County"]);
  }

  function getTabRowDomainUrl(kind: "counties" | "cities", r: any) {
    const domainToPaste =
      kind === "cities"
        ? s(r["City Domain"]) || s(r["city domain"])
        : s(r["Domain"]) || s(r["County Domain"]);
    return s(toUrlMaybe(domainToPaste));
  }

  async function runTabSitemaps(
    kind: "counties" | "cities",
    action: TabGoogleAction,
    rowsToRun: any[],
    mode: "all" | "retry",
  ) {
    const runKey = tabRunKey(kind, action);
    setTabSitemapSubmitting(runKey);
    setTabSitemapStatus(null);
    setTabSitemapRunKind(kind);
    setTabSitemapRunAction(action);
    setTabSitemapRunMode(mode);
    setTabSitemapRunDone(false);
    setTabSitemapRunStartedAt(new Date().toISOString());

    if (rowsToRun.length === 0) {
      setTabSitemapRunItems([]);
      setTabSitemapRunOpen(true);
      setTabSitemapRunDone(true);
      setTabSitemapStatus({
        kind,
        ok: false,
        message:
          mode === "retry"
            ? "No hay filas fallidas para reintentar."
            : "No hay filas activas con domain válido en este tab.",
      });
      setTabSitemapSubmitting("");
      return;
    }

    let okCount = 0;
    const items: TabSitemapResultItem[] = [];
    const runItemsSeed: TabSitemapRunItem[] = rowsToRun.map((r) => {
      const rowName = getTabRowName(kind, r) || "row";
      const domainUrl = getTabRowDomainUrl(kind, r);
      const key = `${kind}:${s(r["Location Id"])}:${rowName}:${domainUrl}`;
      return {
        key,
        rowName,
        domainUrl,
        status: "pending",
      };
    });
    setTabSitemapRunItems(runItemsSeed);
    setTabSitemapRunOpen(true);

    const updateRunItem = (
      key: string,
      status: TabSitemapRunItem["status"],
      error?: string,
    ) => {
      setTabSitemapRunItems((prev) =>
        prev.map((it) =>
          it.key === key ? { ...it, status, error: error || undefined } : it,
        ),
      );
    };

    for (const r of rowsToRun) {
      const domainUrl = getTabRowDomainUrl(kind, r);
      const rowName = getTabRowName(kind, r);
      const key = `${kind}:${s(r["Location Id"])}:${rowName}:${domainUrl}`;
      updateRunItem(key, "running");

      if (!domainUrl) {
        items.push({
          key,
          rowName: rowName || "row",
          domainUrl: "",
          ok: false,
          error: "missing domain URL",
        });
        updateRunItem(key, "failed", "missing domain URL");
        continue;
      }

      try {
        const res = await fetch("/api/tools/index-submit", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ target: "google", domainUrl, mode: action }),
        });
        const data = (await safeJson(res)) as IndexSubmitResponse | null;
        const submitted = !!data?.google?.discovery?.submitted;
        const indexable = !!data?.ok;
        const actionOk = action === "discovery" ? submitted : indexable;
        if (res.ok && data && actionOk) {
          okCount += 1;
          items.push({
            key,
            rowName: rowName || domainUrl,
            domainUrl,
            ok: true,
          });
          updateRunItem(key, "done");
        } else {
          const errMsg = data?.error || data?.google?.error || `HTTP ${res.status}`;
          items.push({
            key,
            rowName: rowName || domainUrl,
            domainUrl,
            ok: false,
            error: errMsg,
          });
          updateRunItem(key, "failed", errMsg);
        }
      } catch (e: any) {
        const errMsg = e?.message || "request failed";
        items.push({
          key,
          rowName: rowName || domainUrl,
          domainUrl,
          ok: false,
          error: errMsg,
        });
        updateRunItem(key, "failed", errMsg);
      }

      await new Promise((resolve) => setTimeout(resolve, 120));
    }

    const failCount = rowsToRun.length - okCount;
    setTabSitemapStatus({
      kind,
      ok: failCount === 0,
      message:
        failCount === 0
          ? action === "inspect"
            ? `URL inspection completado para ${okCount}/${rowsToRun.length} ${kind}.`
            : `Sitemap discovery enviado para ${okCount}/${rowsToRun.length} ${kind}.`
          : action === "inspect"
            ? `URL inspection completado ${okCount}/${rowsToRun.length}. Fallos: ${failCount}.`
            : `Sitemap discovery enviado ${okCount}/${rowsToRun.length}. Fallos: ${failCount}.`,
    });

    setTabSitemapReports((prev) => ({
      ...prev,
      [runKey]: {
        kind,
        action,
        total: rowsToRun.length,
        success: okCount,
        failed: failCount,
        mode,
        items,
        updatedAt: new Date().toISOString(),
      },
    }));

    setTabSitemapRunDone(true);
    setTabSitemapSubmitting("");
  }

  async function submitTabAction(
    kind: "counties" | "cities",
    action: TabGoogleAction,
  ) {
    await runTabSitemaps(kind, action, getActiveRowsForTab(kind), "all");
  }

  async function retryFailedTabSitemaps(
    kind: "counties" | "cities",
    action: TabGoogleAction,
  ) {
    const runKey = tabRunKey(kind, action);
    const last = tabSitemapReports[runKey];
    if (!last) {
      setTabSitemapStatus({
        kind,
        ok: false,
        message: "No hay ejecución previa para reintentar.",
      });
      return;
    }
    const failedSet = new Set(last.items.filter((it) => !it.ok).map((it) => it.key));
    if (failedSet.size === 0) {
      setTabSitemapStatus({
        kind,
        ok: true,
        message: "No hay fallos pendientes.",
      });
      return;
    }
    const rowsToRetry = getActiveRowsForTab(kind).filter((r) => {
      const rowName = getTabRowName(kind, r);
      const domainUrl = getTabRowDomainUrl(kind, r);
      const key = `${kind}:${s(r["Location Id"])}:${rowName}:${domainUrl}`;
      return failedSet.has(key);
    });
    await runTabSitemaps(kind, action, rowsToRetry, "retry");
  }

  function closeActivationHelper() {
    setActOpen(false);
    setActCopied(false);
    setRobotsCopied(false);
    setActHeadersCopied(false);
    setActCelebrateOn(false);
    if (celebrateTimerRef.current) {
      clearTimeout(celebrateTimerRef.current);
      celebrateTimerRef.current = null;
    }

    setActLocId("");
    setActKind("counties");
    setActMarking(false);
    setActMarkErr("");
    setActMarkDone(false);
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

  const headersCopyLabel = useMemo(() => {
    if (actHeadersTab === "head") return "Copy Head";
    if (actHeadersTab === "footer") return "Copy Footer";
    return "Copy Favicon";
  }, [actHeadersTab]);

  async function copyHeadersActive() {
    try {
      const txt =
        actHeadersTab === "head"
          ? s(actHeaders?.head)
          : actHeadersTab === "footer"
            ? s(actHeaders?.footer)
            : s(actHeaders?.favicon);

      await navigator.clipboard.writeText(txt);
      setActHeadersCopied(true);
      setTimeout(() => setActHeadersCopied(false), 1300);
    } catch {}
  }

  const robotsTxt = useMemo(
    () => buildRobotsTxt(actSitemapUrl),
    [actSitemapUrl],
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
      <header className="topbar">
        <div className="brand">
          <div className="logo" />
          <div>
            <h1>My Drip Nurse — Delta Control Tower</h1>
          </div>
        </div>

        <div className="topbarActions">
          <Link className="smallBtn" href="/dashboard">
            Dashboard - Reports
          </Link>
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
                  disabled={isRunning || isOneLocJob}
                  title={
                    isOneLocJob
                      ? "Single-location job does not require state"
                      : ""
                  }
                >
                  <option value="all">ALL</option>
                  {statesOut.map((s0) => (
                    <option key={s0} value={s0}>
                      {formatStateLabel(s0)}
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

            {/* ✅ Single Location args */}
            {isOneLocJob && (
              <div className="row" style={{ marginTop: 10 }}>
                <div className="field" style={{ flex: 2 }}>
                  <label>Location Id (locId)</label>
                  <input
                    className="input"
                    placeholder="e.g. 2rYTkmtMkwdUQLNCdCfB"
                    value={runLocId}
                    onChange={(e) => setRunLocId(e.target.value)}
                    disabled={isRunning}
                  />
                </div>

                <div className="field" style={{ maxWidth: 220 }}>
                  <label>Kind</label>
                  <select
                    className="select"
                    value={runKind}
                    onChange={(e) => setRunKind(e.target.value as any)}
                    disabled={isRunning}
                    title="Optional"
                  >
                    <option value="">auto</option>
                    <option value="counties">counties</option>
                    <option value="cities">cities</option>
                  </select>
                </div>
              </div>
            )}

            <div className="actions">
              <button
                className="btn btnPrimary"
                onClick={() => run()}
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
                Job: <b>{selectedJob?.label}</b>{" "}
                {isOneLocJob ? (
                  <>
                    • locId: <b>{runLocId || "—"}</b>
                  </>
                ) : (
                  <>
                    • State: <b>{stateOut === "all" ? "ALL" : formatStateLabel(stateOut)}</b>
                  </>
                )}{" "}
                • Mode: <b>{mode}</b>
              </div>
            </div>

            <div className={`runnerProgress ${runnerToneClass}`}>
              <div className="runnerProgressTop">
                <div className="runnerProgressTitle">
                  <span className="runnerDot" />
                  <span className="runnerText">
                    {progress.message || "Idle"}
                  </span>
                </div>

                <div className="runnerProgressMeta">
                  <span className="runnerChip">
                    <b>{pctText}</b>
                  </span>
                  <span className="runnerChip">
                    Done: <b>{runnerMeta.all}</b>
                  </span>

                  {isStateJob ? (
                    <span className="runnerChip">
                      States: <b>{runnerMeta.all}</b>
                    </span>
                  ) : isOneLocJob ? (
                    <span className="runnerChip">
                      Location: <b>{runLocId ? "1/1" : "—"}</b>
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

        {/* Sheet Overview */}
        <aside className="card sheetOverviewCard">
          <div className="cardHeader">
            <div>
              <h2 className="cardTitle">Sheet overview</h2>
              <div className="cardSubtitle">
                Live summary from Google Sheets
              </div>
            </div>

            <div className="cardHeaderActions">
              <button
                className="smallBtn smallBtnGhost"
                onClick={() => setMapOpen(true)}
                disabled={!sheet?.states?.length}
                title={
                  sheet?.states?.length
                    ? "Open progress map"
                    : "Load overview first"
                }
              >
                Map
              </button>

              <button
                className="smallBtn"
                onClick={loadOverview}
                disabled={sheetLoading}
              >
                {sheetLoading ? "Refreshing..." : "Refresh"}
              </button>
            </div>
          </div>

          <div className="cardBody">
            {sheetErr ? (
              <div className="mini" style={{ color: "var(--danger)" }}>
                ❌ {sheetErr}
              </div>
            ) : (
              <div className="kpiRow sheetOverviewGrid">
                <div className="kpi kpiHero">
                  <p className="n">
                    {fmtInt((totals.countiesTotal || 0) + (totals.citiesTotal || 0))}
                  </p>
                  <p className="l">Total counties + cities</p>
                </div>

                <div className="kpi">
                  <p className="n">{fmtInt(sheet?.states?.length ?? 0)}</p>
                  <p className="l">States in sheet</p>
                </div>

                <div className="kpi">
                  <p className="n nPair">{fmtPair(totals.countiesReady, totals.countiesTotal)}</p>
                  <p className="l">Counties ready</p>
                </div>

                <div className="kpi">
                  <p className="n nPair">{fmtPair(totals.citiesReady, totals.citiesTotal)}</p>
                  <p className="l">Cities ready</p>
                </div>

                <div className="kpi">
                  <p className="n">{fmtInt(totals.countiesDomainsActive)}</p>
                  <p className="l">County domains active</p>
                </div>

                <div className="kpi">
                  <p className="n">{fmtInt(totals.citiesDomainsActive)}</p>
                  <p className="l">City domains active</p>
                </div>
                <div className="kpi">
                  <p className="n">
                    {totals.countiesTotal
                      ? `${Math.round((totals.countiesDomainsActive / totals.countiesTotal) * 100)}%`
                      : "0%"}
                  </p>
                  <p className="l">County activation %</p>
                </div>
                <div className="kpi">
                  <p className="n">
                    {totals.citiesTotal
                      ? `${Math.round((totals.citiesDomainsActive / totals.citiesTotal) * 100)}%`
                      : "0%"}
                  </p>
                  <p className="l">City activation %</p>
                </div>
              </div>
            )}
          </div>
        </aside>
      </div>

      {/* Sheet Explorer */}
      <section className="card sheetExplorerCard" style={{ marginTop: 14 }}>
        <div className="cardHeader">
          <div>
            <h2 className="cardTitle">Sheet Explorer</h2>
            <div className="cardSubtitle">
              Estados + progreso de Counties/Cities desde Google Sheets.
            </div>
          </div>

          <div className="sheetExplorerHeadTools">
            <input
              className="input sheetExplorerSearch"
              placeholder="Search state (e.g., Alabama, Florida...)"
              value={q}
              onChange={(e) => setQ(e.target.value)}
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
            <div className="tableWrap tableWrapTall sheetExplorerTableWrap">
              <table className="table sheetExplorerTable">
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
                    const overall = denom
                      ? (readyDone + domainDone) / denom
                      : 0;

                    const pillClass =
                      domainDone === 0
                        ? "pillOff"
                        : overall >= 0.85
                          ? "pillOk"
                          : overall >= 0.55
                            ? "pillWarn"
                            : "pillOff";

                    const rowClass =
                      domainDone === 0
                        ? "stateRow stateRowPending"
                        : overall >= 0.9
                          ? "stateRow stateRowActive"
                          : overall >= 0.4
                            ? "stateRow stateRowProgress"
                            : "stateRow stateRowPending";

                    return (
                      <tr key={r.state} className={`tr ${rowClass}`}>
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
                            {r.counties.domainsActive || 0}/{r.counties.total}{" "}
                            active
                          </span>
                        </td>

                        <td className="td">
                          <span className={pillClass}>
                            {r.cities.ready}/{r.cities.total} ready
                          </span>
                        </td>

                        <td className="td">
                          <span className={pillClass}>
                            {r.cities.domainsActive || 0}/{r.cities.total}{" "}
                            active
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

      {/* Logs */}
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
                    className={`tabBtn ${detailTab === "counties" ? "tabBtnActive" : ""}`}
                    onClick={() => setDetailTab("counties")}
                  >
                    Counties
                  </button>
                  <button
                    className={`tabBtn ${detailTab === "cities" ? "tabBtnActive" : ""}`}
                    onClick={() => setDetailTab("cities")}
                  >
                    Cities
                  </button>
                </div>
                <div className="tabs" style={{ marginTop: 8 }}>
                  <button
                    className="smallBtn"
                    onClick={() => submitTabAction("counties", "inspect")}
                    disabled={tabSitemapSubmitting !== ""}
                    title="Run URL Inspection para todos los counties activos."
                  >
                    {tabSitemapSubmitting === tabRunKey("counties", "inspect")
                      ? "Inspect Counties..."
                      : "Inspect Counties"}
                  </button>
                  <button
                    className="smallBtn"
                    onClick={() => submitTabAction("cities", "inspect")}
                    disabled={tabSitemapSubmitting !== ""}
                    title="Run URL Inspection para todas las cities activas."
                  >
                    {tabSitemapSubmitting === tabRunKey("cities", "inspect")
                      ? "Inspect Cities..."
                      : "Inspect Cities"}
                  </button>
                  <button
                    className="smallBtn"
                    onClick={() => submitTabAction("counties", "discovery")}
                    disabled={tabSitemapSubmitting !== ""}
                    title="Enviar sitemap.xml a Google Search Console para todos los counties activos."
                  >
                    {tabSitemapSubmitting === tabRunKey("counties", "discovery")
                      ? "Sitemap Counties..."
                      : "Sitemap Counties"}
                  </button>
                  <button
                    className="smallBtn"
                    onClick={() => submitTabAction("cities", "discovery")}
                    disabled={tabSitemapSubmitting !== ""}
                    title="Enviar sitemap.xml a Google Search Console para todas las cities activas."
                  >
                    {tabSitemapSubmitting === tabRunKey("cities", "discovery")
                      ? "Sitemap Cities..."
                      : "Sitemap Cities"}
                  </button>
                  <button
                    className="smallBtn"
                    onClick={() =>
                      retryFailedTabSitemaps(detailTab, tabSitemapRunAction)
                    }
                    disabled={
                      tabSitemapSubmitting !== "" ||
                      !tabSitemapReports[currentTabRunKey] ||
                      (tabSitemapReports[currentTabRunKey]?.failed || 0) === 0
                    }
                    title="Reintenta solo los fallidos del tab actual."
                  >
                    {tabSitemapSubmitting === currentTabRunKey
                      ? "Retry failed..."
                      : `Retry failed (${tabSitemapReports[currentTabRunKey]?.failed || 0})`}
                  </button>
                  <button
                    className="smallBtn"
                    onClick={() =>
                      setTabSitemapShowDetails((p) => ({
                        ...p,
                        [currentTabRunKey]: !p[currentTabRunKey],
                      }))
                    }
                    disabled={!currentTabSitemapReport}
                    title="Ver detalle de resultados por fila."
                  >
                    {tabSitemapShowDetails[currentTabRunKey]
                      ? "Hide details"
                      : "View details"}
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
                    <div className="kpi">
                      <p className="n">
                        {(() => {
                          const eligible = detail.counties.stats.eligible || 0;
                          const active = (detail.counties.rows || []).filter(
                            (r) => !!r.__eligible && isTrue(r["Domain Created"]),
                          ).length;
                          if (!eligible) return "0%";
                          return `${Math.round((active / eligible) * 100)}%`;
                        })()}
                      </p>
                      <p className="l">County domains activated %</p>
                    </div>
                    <div className="kpi">
                      <p className="n">
                        {(() => {
                          const eligible = detail.cities.stats.eligible || 0;
                          const active = (detail.cities.rows || []).filter(
                            (r) => !!r.__eligible && isTrue(r["Domain Created"]),
                          ).length;
                          if (!eligible) return "0%";
                          return `${Math.round((active / eligible) * 100)}%`;
                        })()}
                      </p>
                      <p className="l">City domains activated %</p>
                    </div>
                  </div>

                  <div
                    className="detailFiltersRow"
                    style={{ marginTop: 14 }}
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
                    <input
                      className="input detailSearchInput"
                      placeholder={
                        detailTab === "cities"
                          ? "Search county, city, location id..."
                          : "Search county, location id..."
                      }
                      value={detailSearch}
                      onChange={(e) => setDetailSearch(e.target.value)}
                    />
                  </div>

                  {tabSitemapStatus && (
                    <div
                      className="mini"
                      style={{
                        marginTop: 10,
                        color: tabSitemapStatus.ok
                          ? "var(--ok)"
                          : "var(--danger)",
                      }}
                    >
                      {tabSitemapStatus.ok ? "✅ " : "❌ "}
                      {tabSitemapStatus.kind === "counties"
                        ? "Counties:"
                        : "Cities:"}{" "}
                      {tabSitemapStatus.message}
                    </div>
                  )}

                  {currentTabSitemapReport &&
                    tabSitemapShowDetails[currentTabRunKey] && (
                    <div
                      className="card"
                      style={{
                        marginTop: 10,
                        borderColor: "rgba(255,255,255,0.14)",
                      }}
                    >
                      <div className="cardBody" style={{ padding: 10 }}>
                        <div
                          className="mini"
                          style={{
                            display: "flex",
                            gap: 10,
                            alignItems: "center",
                            justifyContent: "space-between",
                            flexWrap: "wrap",
                          }}
                        >
                          <span>
                            <b>{detailTab === "counties" ? "Counties" : "Cities"}</b>{" "}
                            {currentTabSitemapReport.action === "inspect"
                              ? "inspect"
                              : "sitemap"}{" "}
                            run ({currentTabSitemapReport.mode}) •{" "}
                            {currentTabSitemapReport.success}/{currentTabSitemapReport.total} ok •{" "}
                            {currentTabSitemapReport.failed} failed
                          </span>
                          <span>
                            {new Date(currentTabSitemapReport.updatedAt).toLocaleString()}
                          </span>
                        </div>

                        <div
                          className="tableWrap tableScrollX"
                          style={{ marginTop: 8, maxHeight: 220 }}
                        >
                          <table className="table">
                            <thead>
                              <tr>
                                <th className="th">Status</th>
                                <th className="th">
                                  {detailTab === "counties" ? "County" : "City"}
                                </th>
                                <th className="th">Domain</th>
                                <th className="th">Error</th>
                              </tr>
                            </thead>
                            <tbody>
                              {currentTabSitemapReport.items.map((it) => (
                                <tr key={it.key} className="tr">
                                  <td
                                    className="td"
                                    style={{ color: it.ok ? "var(--ok)" : "var(--danger)" }}
                                  >
                                    {it.ok ? "OK" : "FAIL"}
                                  </td>
                                  <td className="td">{it.rowName || "—"}</td>
                                  <td className="td">
                                    <span className="mini">{it.domainUrl || "—"}</span>
                                  </td>
                                  <td className="td">
                                    <span className="mini">{it.error || "—"}</span>
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    </div>
                  )}

                  <div
                    className="tableWrap tableScrollX detailTableWrap"
                    style={{ marginTop: 12 }}
                  >
                    <table
                      className={`table detailDataTable ${detailTab === "cities" ? "tableWideCities" : ""}`}
                    >
                      <thead>
                        <tr>
                          <th className="th">Eligible</th>
                          <th className="th">Active</th>
                          <th className="th">Location Id</th>
                          <th className="th">County</th>
                          {detailTab === "cities" && (
                            <th className="th">City</th>
                          )}
                          <th className="th">Setup</th>
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
                              : String(r["County"] || "").trim() ===
                                countyFilter,
                          )
                          .filter((r) => {
                            const q0 = detailSearch.trim().toLowerCase();
                            if (!q0) return true;
                            const locId = s(r["Location Id"]).toLowerCase();
                            const county = s(r["County"]).toLowerCase();
                            const city = s(r["City"]).toLowerCase();
                            return (
                              locId.includes(q0) ||
                              county.includes(q0) ||
                              city.includes(q0)
                            );
                          })
                          .map((r, i) => {
                            const eligible = !!r.__eligible;
                            const locId = s(r["Location Id"]);
                            const hasLocId = !!locId;
                            const county = s(r["County"]);
                            const city = s(r["City"]);

                            const domainCreated = isTrue(r["Domain Created"]);
                            const activationUrl = s(r["Domain URL Activation"]);

                            const domainToPaste =
                              detailTab === "cities"
                                ? s(r["City Domain"]) || s(r["city domain"])
                                : s(r["Domain"]) || s(r["County Domain"]);

                            const sitemap = s(r["Sitemap"]);

                            const title =
                              detailTab === "cities"
                                ? `${openState} • ${county || "County"} • ${city || "City"}`
                                : `${openState} • ${county || "County"}`;

                            const accountName = s(r["Account Name"]);
                            const timezone = s(r["Timezone"]);

                            const rowTone = domainCreated
                              ? "rowDomainActive"
                              : eligible
                                ? "rowDomainPending"
                                : "rowDomainIdle";

                            return (
                              <tr
                                key={i}
                                className={`tr ${eligible ? "rowEligible" : ""} ${rowTone}`}
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
                                  {hasLocId ? (
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
                                            locId,
                                            kind: detailTab, // ✅ kind carried
                                          })
                                        }
                                      >
                                        View
                                      </button>
                                    </div>
                                  ) : (
                                    <span className="mini">—</span>
                                  )}
                                </td>
                              </tr>
                            );
                          })}
                      </tbody>
                    </table>
                  </div>

                  <div className="mini" style={{ marginTop: 10 }}>
                    Activation helper usa: <b>Domain URL Activation</b> + el
                    domain a pegar (<b>City Domain</b> o <b>Domain</b>).
                  </div>
                </>
              )}
            </div>
          </div>
        </>
      )}

      {tabSitemapRunOpen && (
        <>
          <div
            className="modalBackdrop"
            onClick={() => {
              if (!tabSitemapSubmitting) setTabSitemapRunOpen(false);
            }}
          />
          <div
            className="modal"
            role="dialog"
            aria-modal="true"
            style={{
              width: "min(980px, calc(100vw - 24px))",
              height: "min(620px, calc(100vh - 24px))",
            }}
          >
            <div className="modalHeader">
              <div>
                <div className="badge">
                  {tabSitemapRunAction === "inspect"
                    ? "GOOGLE URL INSPECTION RUN"
                    : "GOOGLE SITEMAP DISCOVERY RUN"}
                </div>
                <h3 className="modalTitle" style={{ marginTop: 8 }}>
                  {openState} •{" "}
                  {tabSitemapRunKind === "counties" ? "Counties" : "Cities"} •{" "}
                  {tabSitemapRunAction === "inspect" ? "URL Inspect" : "Sitemap Discovery"} •{" "}
                  {tabSitemapRunMode === "retry" ? "Retry Failed" : "Full Run"}
                </h3>
                <div className="mini" style={{ marginTop: 6 }}>
                  Started:{" "}
                  {tabSitemapRunStartedAt
                    ? new Date(tabSitemapRunStartedAt).toLocaleString()
                    : "—"}
                </div>
              </div>

              <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <span className="badge">{tabSitemapRunCounts.pct}%</span>
                <span className="badge">
                  Done {tabSitemapRunCounts.done}/{tabSitemapRunCounts.total}
                </span>
                <span className="badge" style={{ color: "var(--danger)" }}>
                  Failed {tabSitemapRunCounts.failed}
                </span>
                <button
                  className="smallBtn"
                  onClick={() => setTabSitemapRunOpen(false)}
                  disabled={!!tabSitemapSubmitting}
                >
                  {tabSitemapSubmitting ? "Running..." : "Close"}
                </button>
              </div>
            </div>

            <div className="modalBody" style={{ padding: 14 }}>
              <div className="card" style={{ marginBottom: 10 }}>
                <div className="cardBody" style={{ padding: 10 }}>
                  <div className="mini" style={{ marginBottom: 8 }}>
                    {tabSitemapRunDone ? "Run completed." : "Processing..."}
                  </div>
                  <div
                    className="progressWrap"
                    style={{
                      width: "100%",
                      height: 10,
                      borderRadius: 999,
                      border: "1px solid rgba(255,255,255,0.12)",
                      background: "rgba(255,255,255,0.05)",
                      overflow: "hidden",
                    }}
                  >
                    <div
                      className="progressBar"
                      style={{
                        width: `${tabSitemapRunCounts.pct}%`,
                        height: "100%",
                        background:
                          "linear-gradient(90deg, rgba(96,165,250,0.95), rgba(74,222,128,0.92))",
                        transition: "width 180ms ease",
                      }}
                    />
                  </div>
                  <div
                    className="chips"
                    style={{ marginTop: 8, display: "flex", gap: 8 }}
                  >
                    <span className="badge">Pending {tabSitemapRunCounts.pending}</span>
                    <span className="badge">Running {tabSitemapRunCounts.running}</span>
                    <span className="badge" style={{ color: "var(--ok)" }}>
                      Done {tabSitemapRunCounts.done}
                    </span>
                    <span className="badge" style={{ color: "var(--danger)" }}>
                      Failed {tabSitemapRunCounts.failed}
                    </span>
                  </div>
                </div>
              </div>

              <div className="tableWrap tableScrollX" style={{ maxHeight: 390 }}>
                <table className="table">
                  <thead>
                    <tr>
                      <th className="th">Status</th>
                      <th className="th">
                        {tabSitemapRunKind === "counties" ? "County" : "City"}
                      </th>
                      <th className="th">Domain</th>
                      <th className="th">Error</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tabSitemapRunItems.map((it) => (
                      <tr key={it.key} className="tr">
                        <td className="td">
                          {it.status === "done" && (
                            <span className="pillOk">Done</span>
                          )}
                          {it.status === "failed" && (
                            <span className="pillOff">Failed</span>
                          )}
                          {it.status === "running" && (
                            <span className="pillWarn">Running</span>
                          )}
                          {it.status === "pending" && (
                            <span className="badge">Pending</span>
                          )}
                        </td>
                        <td className="td">{it.rowName}</td>
                        <td className="td">
                          <span className="mini">{it.domainUrl || "—"}</span>
                        </td>
                        <td className="td">
                          <span className="mini">{it.error || "—"}</span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </>
      )}

      {/* ✅ Activation Modal */}
      {actOpen && (
        <>
          <div className="modalBackdrop" onClick={closeActivationHelper} />

          <div className="modal" role="dialog" aria-modal="true">
            {/* Header */}
            <div className="modalHeader">
              <div style={{ minWidth: 0 }}>
                <div className="badge">DOMAIN ACTIVATION</div>

                <div className="modalTitleRow" style={{ marginTop: 8 }}>
                  <h3 className="modalTitle" style={{ margin: 0 }}>
                    {actTitle || "Domain Activation"}
                  </h3>

                  <div className="modalStatus">
                    {actIsActive ? (
                      <span className="pillOk">Active</span>
                    ) : (
                      <span className="pillOff">Pending</span>
                    )}
                  </div>
                </div>

                <div className="modalMeta">
                  <div className="metaItem">
                    <div className="metaLabel">GHL Subaccount</div>
                    <div className="metaValue">{actAccountName || "—"}</div>
                  </div>

                  <div className="metaItem">
                    <div className="metaLabel">Timezone</div>
                    <div className="metaValue">{actTimezone || "—"}</div>
                  </div>
                </div>

                {actMarkErr ? (
                  <div
                    className="mini"
                    style={{ color: "var(--danger)", marginTop: 8 }}
                  >
                    ❌ {actMarkErr}
                  </div>
                ) : null}

              </div>

              <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
                <button
                  className={`smallBtn ${actMarkDone || actIsActive ? "smallBtnOn" : ""}`}
                  onClick={markDomainCreatedTrue}
                  disabled={!actLocId || actMarking || actIsActive}
                  title={
                    !actLocId
                      ? "Missing Location Id"
                      : actIsActive
                        ? "Already Active"
                        : `Set Domain Created = TRUE (${actKind})`
                  }
                  type="button"
                >
                  {actIsActive
                    ? "Complete ✅"
                    : actMarking
                      ? "Completing…"
                      : actMarkDone
                        ? "Completed ✅"
                        : "Complete"}
                </button>

                {/* ✅ NEW: Update Custom Values for this locId */}
                <button
                  className="smallBtn"
                  onClick={() => {
                    // lock runner config + run immediately
                    setJob("update-custom-values-one");
                    setRunLocId(actLocId);
                    setRunKind(actKind || "");
                    run({
                      job: "update-custom-values-one",
                      locId: actLocId,
                      kind: actKind || "",
                    });
                  }}
                  disabled={!actLocId || isRunning}
                  title={
                    !actLocId
                      ? "Missing Location Id"
                      : "Update Custom Values in this subaccount"
                  }
                  type="button"
                >
                  Update Custom Values
                </button>

                <button className="smallBtn" onClick={closeActivationHelper}>
                  Close
                </button>
              </div>
            </div>

            {/* Body */}
            <div className="modalBody">
              <div className="modalGrid">
                {/* LEFT */}
                <div style={{ minWidth: 0 }}>
                  <div className="sectionTitle">DOMAIN TO PASTE</div>
                  <div className="sectionHint">
                    Click to copy (pégalo en GHL field{" "}
                    <span className="kbd">Domain</span>)
                  </div>

                  <div
                    className="copyField"
                    onClick={copyDomain}
                    role="button"
                    tabIndex={0}
                  >
                    <div className="copyFieldTop">
                      <div className="copyValue">{actDomainToPaste || "—"}</div>
                      <div
                        className={`copyBadge ${actCopied ? "copyBadgeOn" : ""}`}
                      >
                        {actCopied ? "Copied" : "Copy"}
                      </div>
                    </div>
                    <div className="copyFieldSub">
                      Tip: si pega raro, haz click nuevamente (clipboard).
                    </div>
                  </div>

                  <div className="modalQuickActions">
                    <button className="btn btnPrimary" onClick={copyDomain}>
                      Copy Domain
                    </button>

                    <a
                      className="btn"
                      href={actActivationUrl || "#"}
                      target="_blank"
                      rel="noreferrer"
                      style={{
                        opacity: actActivationUrl ? 1 : 0.55,
                        pointerEvents: actActivationUrl ? "auto" : "none",
                      }}
                    >
                      Open Activation
                    </a>

                    <a
                      className="btn"
                      href={actWebsiteUrl || "#"}
                      target="_blank"
                      rel="noreferrer"
                      style={{
                        opacity: actWebsiteUrl ? 1 : 0.55,
                        pointerEvents: actWebsiteUrl ? "auto" : "none",
                      }}
                    >
                      Open Website
                    </a>
                  </div>

                  <div
                    className="mini"
                    style={{ marginTop: 12, lineHeight: 1.5 }}
                  >
                    Cuando completes el Checklist, presiona{" "}
                    <span className="kbd">Complete</span> (arriba) y luego{" "}
                    <span className="kbd">Update Custom Values</span>.
                  </div>
                </div>

                {/* RIGHT */}
                <div style={{ minWidth: 0 }}>
                  <div className="stepCard">
                    <div className="stepCardHeader stepCardHeaderTabs">
                      <div className="stepPill">Checklist</div>

                      <div className="stepTabs">
                        <button
                          className={`stepTab ${actChecklistTab === "domain" ? "stepTabOn" : ""}`}
                          onClick={() => setActChecklistTab("domain")}
                          type="button"
                        >
                          Domain
                        </button>

                        <button
                          className={`stepTab ${actChecklistTab === "sitemap" ? "stepTabOn" : ""}`}
                          onClick={() => setActChecklistTab("sitemap")}
                          type="button"
                        >
                          Sitemap
                        </button>

                        <button
                          className={`stepTab ${actChecklistTab === "robots" ? "stepTabOn" : ""}`}
                          onClick={() => setActChecklistTab("robots")}
                          type="button"
                        >
                          Robots.txt
                        </button>

                        <button
                          className={`stepTab ${actChecklistTab === "headers" ? "stepTabOn" : ""}`}
                          onClick={() => setActChecklistTab("headers")}
                          type="button"
                        >
                          Headers
                        </button>
                      </div>
                    </div>

                    {/* DOMAIN */}
                    {actChecklistTab === "domain" && (
                      <div style={{ padding: 12 }}>
                        <div className="sectionTitle">Domain</div>
                        <div className="sectionHint" style={{ marginTop: 6 }}>
                          Activa el dominio en GHL.
                        </div>

                        <ol className="stepsList">
                          <li>
                            Abre <span className="kbd">Open Activation</span>.
                          </li>
                          <li>
                            Pega el domain en el campo de{" "}
                            <span className="kbd">Domain</span>.
                          </li>
                          <li>
                            Haz click en <span className="kbd">Continue</span>.
                          </li>
                          <li>
                            Haz click en{" "}
                            <span className="kbd">Add record manually</span>.
                          </li>
                          <li>
                            Haz click en{" "}
                            <span className="kbd">Verify records</span> y espera
                            propagación.
                          </li>
                          <li>
                            Haz click en <span className="kbd">Website</span>.
                          </li>
                          <li>
                            En{" "}
                            <span className="kbd">
                              Link domain with website
                            </span>{" "}
                            selecciona <span className="kbd">County</span>.
                          </li>
                          <li>
                            En{" "}
                            <span className="kbd">
                              Select default step/page for Domain
                            </span>{" "}
                            selecciona <span className="kbd">** Home Page</span>
                            .
                          </li>
                          <li>
                            Haz click en{" "}
                            <span className="kbd">Proceed to finish</span>.
                          </li>
                          <li>Valida que el site responda.</li>
                        </ol>
                      </div>
                    )}

                    {/* SITEMAP */}
                    {actChecklistTab === "sitemap" && (
                      <div style={{ padding: 12 }}>
                        <div className="sectionTitle">Sitemap</div>
                        <div className="sectionHint" style={{ marginTop: 6 }}>
                          Genera el sitemap en GHL.
                        </div>

                        <div className="miniCardGrid" style={{ marginTop: 10 }}>
                          <div className="miniCard">
                            <div className="miniCardLabel">Sitemap URL</div>
                            <div className="miniCardValue">
                              {actSitemapUrl ? (
                                <a
                                  className="link"
                                  href={actSitemapUrl}
                                  target="_blank"
                                  rel="noreferrer"
                                >
                                  {actSitemapUrl}
                                </a>
                              ) : (
                                "—"
                              )}
                            </div>
                          </div>

                          <div className="miniCard miniCardAction">
                            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                              <a
                                className="smallBtn"
                                href={actSitemapUrl || "#"}
                                target="_blank"
                                rel="noreferrer"
                                style={{
                                  opacity: actSitemapUrl ? 1 : 0.55,
                                  pointerEvents: actSitemapUrl ? "auto" : "none",
                                  display: "inline-flex",
                                  alignItems: "center",
                                  justifyContent: "center",
                                }}
                              >
                                Open
                              </a>
                              <button
                                className="smallBtn"
                                type="button"
                                onClick={verifySitemap}
                                disabled={!actSitemapUrl || actSitemapChecking}
                                title="Verifica que el sitemap esté activo y que coincida con este dominio."
                              >
                                {actSitemapChecking ? "Checking..." : "Verify"}
                              </button>
                            </div>
                          </div>
                        </div>

                        {actSitemapVerify ? (
                          (() => {
                            const status = Number(actSitemapVerify.responseStatus || 0);
                            const verified =
                              !!actSitemapVerify.ok &&
                              (status === 200 || status === 403) &&
                              actSitemapVerify.pathMatchesSitemap !== false &&
                              !!actSitemapVerify.matches;
                            return (
                          <div
                            className="mini"
                            style={{
                              marginTop: 10,
                              color: verified ? "var(--ok)" : "var(--danger)",
                            }}
                          >
                            {actSitemapVerify.ok ? (
                              <>
                                {verified ? "Verificado" : "No existe o no se"}
                                {actSitemapVerify.responseStatus
                                  ? ` • status: ${actSitemapVerify.responseStatus}`
                                  : ""}
                                {actSitemapVerify.expectedHost
                                  ? ` • expected: ${actSitemapVerify.expectedHost}`
                                  : ""}
                                {actSitemapVerify.responseHost
                                  ? ` • response: ${actSitemapVerify.responseHost}`
                                  : ""}
                              </>
                            ) : (
                              <>X {actSitemapVerify.error || "Sitemap verify failed"}</>
                            )}
                          </div>
                            );
                          })()
                        ) : null}

                        <ol className="stepsList" style={{ marginTop: 10 }}>
                          <li>
                            Haz click en <span className="kbd">Manage</span>.
                          </li>
                          <li>
                            Haz click en <span className="kbd">⋮</span>.
                          </li>
                          <li>
                            Haz click en{" "}
                            <span className="kbd">&lt;&gt; XML Sitemap</span>.
                          </li>
                          <li>
                            Abre County y marca el checkbox solamente en las
                            paginas que contengan{" "}
                            <span className="kbd">**</span> al principio.
                          </li>
                          <li>
                            Haz click en <span className="kbd">Proceed</span>.
                          </li>
                          <li>
                            Haz click en{" "}
                            <span className="kbd">Generate & Save</span>.
                          </li>
                          <li>
                            Haz click en{" "}
                            <span className="kbd">Generate & Save</span>.
                          </li>
                          <li>
                            Haz click en <span className="kbd">Okay</span>.
                          </li>
                          <li>
                            Valida el sitemap a traves del boton que dice{" "}
                            <span className="kbd">Open</span> en esta ventana.
                          </li>
                        </ol>
                      </div>
                    )}

                    {/* ROBOTS */}
                    {actChecklistTab === "robots" && (
                      <div style={{ padding: 12 }}>
                        <div className="robotsHeaderRow" style={{ padding: 0 }}>
                          <div>
                            <div className="sectionTitle">Robots.txt</div>
                            <div
                              className="sectionHint"
                              style={{ marginTop: 6 }}
                            >
                              Genera el file robots.txt
                            </div>
                          </div>

                          <button className="smallBtn" onClick={copyRobots}>
                            {robotsCopied ? "Copied" : "Copy Robots"}
                          </button>
                        </div>

                        <div className="robotsBox" style={{ marginTop: 12 }}>
                          <pre className="robotsPre">{robotsTxt}</pre>
                        </div>

                        <ol className="stepsList" style={{ marginTop: 10 }}>
                          <li>
                            Haz click en <span className="kbd">⋮</span>.
                          </li>
                          <li>
                            Haz click en <span className="kbd">Edit</span>.
                          </li>
                          <li>
                            En <span className="kbd">Robots.txt code</span> haz
                            paste del codigo.
                          </li>
                          <li>
                            Haz click en <span className="kbd">Save</span>.
                          </li>
                          <li>
                            Valida en el browser que{" "}
                            <span className="kbd">/robots.txt</span> responda
                            200 OK.
                          </li>
                        </ol>
                      </div>
                    )}

                    {/* HEADERS */}
                    {actChecklistTab === "headers" && (
                      <div style={{ padding: 12 }}>
                        <div className="sectionTitle">Headers</div>
                        <div className="sectionHint" style={{ marginTop: 6 }}>
                          Head / Footer / Favicon (copiar y pegar en los
                          settings del website).
                        </div>

                        <div className="stepTabs" style={{ marginTop: 10 }}>
                          <button
                            className={`stepTab ${actHeadersTab === "favicon" ? "stepTabOn" : ""}`}
                            onClick={() => setActHeadersTab("favicon")}
                            type="button"
                          >
                            Favicon
                          </button>
                          <button
                            className={`stepTab ${actHeadersTab === "head" ? "stepTabOn" : ""}`}
                            onClick={() => setActHeadersTab("head")}
                            type="button"
                          >
                            Head
                          </button>
                          <button
                            className={`stepTab ${actHeadersTab === "footer" ? "stepTabOn" : ""}`}
                            onClick={() => setActHeadersTab("footer")}
                            type="button"
                          >
                            Body
                          </button>

                          <button
                            className={`smallBtn ${actHeadersCopied ? "smallBtnOn" : ""}`}
                            onClick={copyHeadersActive}
                            style={{ marginLeft: "auto" }}
                            type="button"
                            disabled={actHeadersLoading || !!actHeadersErr}
                            title={
                              actHeadersErr ? actHeadersErr : "Copy active tab"
                            }
                          >
                            {actHeadersCopied ? "Copied" : "Copy"}
                          </button>
                        </div>

                        {actHeadersLoading ? (
                          <div className="mini" style={{ marginTop: 12 }}>
                            Loading headers...
                          </div>
                        ) : actHeadersErr ? (
                          <div
                            className="mini"
                            style={{ marginTop: 12, color: "var(--danger)" }}
                          >
                            ❌ {actHeadersErr}
                          </div>
                        ) : (
                          <>
                            <div className="codeBox" style={{ marginTop: 12 }}>
                              <pre className="codePre">
                                {actHeadersTab === "head"
                                  ? s(actHeaders?.head)
                                  : actHeadersTab === "footer"
                                    ? s(actHeaders?.footer)
                                    : s(actHeaders?.favicon)}
                              </pre>
                            </div>
                            {/* <div
                              className="mini"
                              style={{ marginTop: 10, lineHeight: 1.5 }}
                            >
                              Fuente:{" "}
                              {actHeaders?.source?.row ? (
                                <>
                                  row <b>{actHeaders.source.row}</b>
                                </>
                              ) : (
                                "—"
                              )}
                            </div> */}
                          </>
                        )}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </>
      )}

      {/* ✅ MAP MODAL */}
      {mapOpen && (
        <>
          <div className="mapBackdrop" onClick={closeMap} />
          <div className="mapModal" role="dialog" aria-modal="true">
            <div className="mapModalHeader">
              <div>
                <div className="badge">VISUALIZATION</div>
                <h3 className="mapModalTitle">US Progress Map</h3>
                <div className="mini" style={{ marginTop: 6 }}>
                  Vista rápida por estado para priorizar producción y dominios
                  activos.
                </div>
              </div>

              <div className="mapModalActions">
                <div className="mapMetricTabs">
                  <button
                    className={`tabBtn ${mapMetric === "ready" ? "tabBtnActive" : ""}`}
                    onClick={() => setMapMetric("ready")}
                    type="button"
                  >
                    GHL Subaccounts Created
                  </button>
                  <button
                    className={`tabBtn ${mapMetric === "domains" ? "tabBtnActive" : ""}`}
                    onClick={() => setMapMetric("domains")}
                    type="button"
                  >
                    Domains Created
                  </button>
                </div>

                <button className="smallBtn" onClick={closeMap} type="button">
                  Close
                </button>
              </div>
            </div>

            <div className="mapModalBody">
              <div className="mapLayout">
                {/* Left */}
                <div className="mapPane">
                  <div className="mapFrame">
                    <UsaChoroplethProgressMap
                      rows={sheet?.states || []}
                      metric={mapMetric}
                      selectedState={mapSelected}
                      onPick={(name) =>
                        setMapSelected(String(name || "").trim())
                      }
                    />
                  </div>
                </div>

                {/* Right */}
                <aside className="mapSide">
                  <div className="mapSideCard">
                    <div className="mini" style={{ opacity: 0.8 }}>
                      Selection
                    </div>

                    {!selectedStateMetrics ? (
                      <div style={{ marginTop: 12 }} className="mini">
                        Click a state
                      </div>
                    ) : (
                      <>
                        <h4 style={{ marginTop: 8 }}>{mapSelected}</h4>

                        <div className="mapSideStats">
                          <div className="mapStat">
                            <div className="mapStatLabel">
                              GHL Subaccounts Created
                            </div>
                            <div className="mapStatValue">
                              {Math.round(selectedStateMetrics.readyPct * 100)}%
                            </div>
                            <div className="mini">
                              Counties {selectedStateMetrics.countiesReady}/
                              {selectedStateMetrics.countiesTotal} • Cities{" "}
                              {selectedStateMetrics.citiesReady}/
                              {selectedStateMetrics.citiesTotal}
                            </div>
                          </div>

                          <div className="mapStat">
                            <div className="mapStatLabel">Domains Created</div>
                            <div className="mapStatValue">
                              {Math.round(
                                selectedStateMetrics.domainsPct * 100,
                              )}
                              %
                            </div>
                            <div className="mini">
                              County domains{" "}
                              {selectedStateMetrics.countiesDomains} • City
                              domains {selectedStateMetrics.citiesDomains}
                            </div>
                          </div>
                        </div>

                        <div className="mapSideActions">
                          <button
                            className="smallBtn"
                            onClick={() => {
                              closeMap();
                              openDetail(mapSelected);
                            }}
                          >
                            Open State
                          </button>

                          <button
                            className="smallBtn"
                            onClick={() => setMapSelected("")}
                          >
                            Clear
                          </button>
                        </div>
                      </>
                    )}
                  </div>
                </aside>
              </div>
            </div>
          </div>
        </>
      )}

      <div
        key={actCelebrateKey}
        className={`modalCelebrate ${actCelebrateOn ? "isOn" : ""}`}
        aria-hidden="true"
      >
        <div className="modalCelebrateGlow" />
        <div className="modalCelebrateHeadlineWrap">
          <div className="modalCelebrateHeadline">
            <span className="modalCelebrateHeadlineTop">Yo soy de</span>
            <span className="modalCelebrateHeadlineMain">P FKN R</span>
          </div>
        </div>
        {celebrationParticles.map((p, idx) => (
          <span
            key={idx}
            className={`modalCelebrateParticle ${p.kind === "spark" ? "isSpark" : "isRocket"}`}
            style={
              {
                "--ox": `${p.originX}%`,
                "--tx": `${p.tx}px`,
                "--ty": `${p.ty}px`,
                "--sz": `${p.size}px`,
                "--delay": `${p.delay}s`,
                "--dur": `${p.duration}s`,
                "--h": `${p.hue}`,
                "--a": `${p.alpha}`,
                "--spin": `${p.spin}deg`,
              } as any
            }
          />
        ))}
      </div>
    </div>
  );
}
