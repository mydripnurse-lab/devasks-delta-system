// control-tower/src/app/api/dashboard/gsc/join/route.ts
import { NextResponse } from "next/server";
import fs from "fs/promises";
import path from "path";
import { loadGscCatalogIndex } from "@/lib/gscCatalogIndex";

export const runtime = "nodejs";

type AnyObj = Record<string, any>;

type GscRow = {
    page?: string;
    query?: string;
    impressions?: number;
    clicks?: number;
    ctr?: number; // 0..1
    position?: number; // avg position
};

type MetaFile = {
    ok?: boolean;
    siteUrl?: string;
    range?: string;
    startDate?: string;
    endDate?: string;
    fetchedAt?: string;
    error?: string;

    trendStart?: string;
    trendIncludesCompare?: boolean;

    qpIncluded?: boolean;
};

function s(v: any) {
    return String(v ?? "").trim();
}
function num(v: any) {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
}

/** ======= Fallback state helpers (ONLY if catalog misses) ======= */
const STATE_ABBR_TO_NAME: Record<string, string> = {
    AL: "Alabama",
    AK: "Alaska",
    AZ: "Arizona",
    AR: "Arkansas",
    CA: "California",
    CO: "Colorado",
    CT: "Connecticut",
    DE: "Delaware",
    DC: "District of Columbia",
    FL: "Florida",
    GA: "Georgia",
    HI: "Hawaii",
    ID: "Idaho",
    IL: "Illinois",
    IN: "Indiana",
    IA: "Iowa",
    KS: "Kansas",
    KY: "Kentucky",
    LA: "Louisiana",
    ME: "Maine",
    MD: "Maryland",
    MA: "Massachusetts",
    MI: "Michigan",
    MN: "Minnesota",
    MS: "Mississippi",
    MO: "Missouri",
    MT: "Montana",
    NE: "Nebraska",
    NV: "Nevada",
    NH: "New Hampshire",
    NJ: "New Jersey",
    NM: "New Mexico",
    NY: "New York",
    NC: "North Carolina",
    ND: "North Dakota",
    OH: "Ohio",
    OK: "Oklahoma",
    OR: "Oregon",
    PA: "Pennsylvania",
    RI: "Rhode Island",
    SC: "South Carolina",
    SD: "South Dakota",
    TN: "Tennessee",
    TX: "Texas",
    UT: "Utah",
    VT: "Vermont",
    VA: "Virginia",
    WA: "Washington",
    WV: "West Virginia",
    WI: "Wisconsin",
    WY: "Wyoming",
    PR: "Puerto Rico",
};

function isStateAbbr(x: string) {
    const up = x.toUpperCase();
    return !!STATE_ABBR_TO_NAME[up];
}

function normalizeState(raw: any) {
    const v = s(raw);
    if (!v) return "";
    if (v === "__unknown") return "__unknown";
    const up = v.toUpperCase();
    if (STATE_ABBR_TO_NAME[up]) return STATE_ABBR_TO_NAME[up];
    if (up === "PR" || up === "PUERTO RICO") return "Puerto Rico";
    return v;
}

/**
 * Fallback only:
 * Handles your real host pattern: <anything>-<abbr>.mydripnurse.com
 * e.g. autaugaville-city-al.mydripnurse.com => AL
 */
function fallbackExtractStateFromUrl(urlLike: any): string {
    const raw = s(urlLike);
    if (!raw) return "__unknown";

    try {
        const u = new URL(raw);

        const left = (u.hostname.split(".")[0] || "").toLowerCase();
        const mHost = left.match(/[-_](?<abbr>[a-z]{2})$/i);
        if (mHost?.groups?.abbr && isStateAbbr(mHost.groups.abbr)) {
            return normalizeState(mHost.groups.abbr);
        }

        const parts = left.split("-").filter(Boolean);
        const tail = parts.slice(-1)[0] || "";
        if (tail.length === 2 && isStateAbbr(tail)) return normalizeState(tail);

        const hostParts = u.hostname.split(".").filter(Boolean);
        if (hostParts.length >= 3) {
            const sub = hostParts[0];
            if (sub && sub.length === 2 && isStateAbbr(sub)) return normalizeState(sub);
        }

        return "__unknown";
    } catch {
        return "__unknown";
    }
}

/** ======= URL helpers ======= */

function safeUrl(urlLike: any): URL | null {
    const raw = s(urlLike);
    if (!raw) return null;
    try {
        return new URL(raw);
    } catch {
        return null;
    }
}

function safeHostname(urlLike: any): string {
    const u = safeUrl(urlLike);
    return (u?.hostname || "").toLowerCase();
}

function getLeftLabel(hostname: string) {
    const h = s(hostname).toLowerCase();
    return (h.split(".")[0] || "").trim();
}

/**
 * Best effort: infer root hostname from meta.siteUrl (sc-domain:mydripnurse.com, https://..., etc)
 */
function inferRootHost(metaSiteUrl?: string): string {
    const raw = s(metaSiteUrl);
    if (!raw) return "mydripnurse.com";

    const m = raw.match(/^sc-domain:(.+)$/i);
    if (m?.[1]) return s(m[1]).toLowerCase();

    try {
        const u = new URL(raw);
        return (u.hostname || "mydripnurse.com").toLowerCase();
    } catch {
        if (raw.includes(".")) return raw.toLowerCase();
        return "mydripnurse.com";
    }
}

function isRootDomainHost(hostname: string, rootHost: string) {
    const h = s(hostname).toLowerCase();
    const r = s(rootHost).toLowerCase();
    return h === r || h === `www.${r}`;
}

function looksLikeDeltaSubdomain(leftLabel: string) {
    const x = s(leftLabel).toLowerCase();
    if (!x) return false;

    if (x.includes("-city-")) return true;
    if (x.includes("-county-")) return true;

    const m = x.match(/[-_](?<abbr>[a-z]{2})$/i);
    if (m?.groups?.abbr && isStateAbbr(m.groups.abbr)) return true;

    if (x.length === 2 && isStateAbbr(x)) return true;

    return false;
}

function prettifySubdomainName(leftLabel: string) {
    const raw = s(leftLabel);
    if (!raw) return "Funnel";

    let parts = raw
        .replace(/[._]/g, " ")
        .replace(/-/g, " ")
        .trim()
        .split(/\s+/)
        .filter(Boolean);

    const expanded: string[] = [];
    for (const p of parts) {
        const tokens = p
            .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
            .replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2")
            .split(/\s+/)
            .filter(Boolean);
        expanded.push(...tokens);
    }

    return expanded
        .map((w) => {
            const x = s(w);
            if (!x) return "";
            return x.charAt(0).toUpperCase() + x.slice(1).toLowerCase();
        })
        .filter(Boolean)
        .join(" ");
}

type PageBucketKind = "delta" | "nationwide" | "funnel";

function classifyPageBucket(
    pageUrl: string,
    rootHost: string,
    catalogByHostname: Record<string, any>,
): { kind: PageBucketKind; host: string; left: string } {
    const host = safeHostname(pageUrl);
    const left = getLeftLabel(host);

    if (!host) return { kind: "delta", host: "", left: "" };

    if (isRootDomainHost(host, rootHost)) {
        return { kind: "nationwide", host, left };
    }

    if (catalogByHostname[host]) {
        return { kind: "delta", host, left };
    }

    if (looksLikeDeltaSubdomain(left)) {
        return { kind: "delta", host, left };
    }

    return { kind: "funnel", host, left };
}

/** ======= JSON reading + normalization ======= */

async function readJsonRaw(absPath: string): Promise<any> {
    try {
        const txt = await fs.readFile(absPath, "utf8");
        return JSON.parse(txt);
    } catch {
        return null;
    }
}

/**
 * Accepts:
 * - array
 * - { rows: array }
 */
function unwrapArray(maybe: any): any[] {
    if (Array.isArray(maybe)) return maybe;
    if (maybe && Array.isArray(maybe.rows)) return maybe.rows;
    return [];
}

function coerceGscRows(raw: any, kind: "pages" | "queries"): GscRow[] {
    const arr = unwrapArray(raw);

    return arr.map((r: AnyObj) => {
        const out: GscRow = {
            impressions: num(r.impressions),
            clicks: num(r.clicks),
            ctr: r.ctr !== undefined ? num(r.ctr) : undefined,
            position: r.position !== undefined ? num(r.position) : undefined,
        };

        const keys0 = Array.isArray(r.keys) ? s(r.keys[0]) : "";

        if (kind === "pages") out.page = s(r.page) || keys0 || "";
        else out.query = s(r.query) || keys0 || "";

        const imp = num(out.impressions);
        const clk = num(out.clicks);
        if (out.ctr === undefined) out.ctr = imp > 0 ? clk / imp : 0;

        return out;
    });
}

/**
 * qp rows: keys[0]=query, keys[1]=page
 */
function coerceQpRows(raw: any): GscRow[] {
    const arr = unwrapArray(raw);

    return arr.map((r: AnyObj) => {
        const keys = Array.isArray(r.keys) ? r.keys : [];
        const q = s(keys[0]);
        const p = s(keys[1]);

        const impressions = num(r.impressions);
        const clicks = num(r.clicks);
        const ctr = r.ctr !== undefined ? num(r.ctr) : impressions > 0 ? clicks / impressions : 0;
        const position = r.position !== undefined ? num(r.position) : 0;

        return {
            query: q,
            page: p,
            impressions,
            clicks,
            ctr,
            position,
        };
    });
}

function computeSummaryFromPages(pages: GscRow[]) {
    let impressions = 0;
    let clicks = 0;

    let posWeightedSum = 0;
    let posWeight = 0;

    const pageSet = new Set<string>();

    for (const r of pages) {
        const imp = num(r.impressions);
        const clk = num(r.clicks);
        const pos = num(r.position);

        impressions += imp;
        clicks += clk;

        if (imp > 0 && pos > 0) {
            posWeightedSum += pos * imp;
            posWeight += imp;
        }

        const p = s(r.page);
        if (p) pageSet.add(p);
    }

    const ctr = impressions > 0 ? clicks / impressions : 0;
    const position = posWeight > 0 ? posWeightedSum / posWeight : 0;

    return {
        impressions,
        clicks,
        ctr,
        position,
        pagesCounted: pageSet.size,
    };
}

/** ======= Catalog-based classification (state) ======= */

function classifyStateWithCatalog(
    pageUrl: string,
    catalogByHostname: Record<string, any>,
): { state: string; kind: "catalog" | "fallback" | "unknown" } {
    const h = safeHostname(pageUrl);
    if (h && catalogByHostname[h]?.state) {
        return { state: String(catalogByHostname[h].state), kind: "catalog" };
    }

    const st = fallbackExtractStateFromUrl(pageUrl);
    if (st && st !== "__unknown") return { state: st, kind: "fallback" };

    return { state: "__unknown", kind: "unknown" };
}

function groupByStateFromPagesWithCatalog(pages: GscRow[], catalogByHostname: Record<string, any>) {
    const m = new Map<
        string,
        {
            impressions: number;
            clicks: number;
            posWeightedSum: number;
            posWeight: number;
            pages: Set<string>;
            classifiedCatalog: number;
            classifiedFallback: number;
            classifiedUnknown: number;
        }
    >();

    for (const r of pages) {
        const page = s(r.page);
        const cls = classifyStateWithCatalog(page, catalogByHostname);
        const st = normalizeState(cls.state || "__unknown");

        if (!m.has(st)) {
            m.set(st, {
                impressions: 0,
                clicks: 0,
                posWeightedSum: 0,
                posWeight: 0,
                pages: new Set<string>(),
                classifiedCatalog: 0,
                classifiedFallback: 0,
                classifiedUnknown: 0,
            });
        }

        const b = m.get(st)!;
        const imp = num(r.impressions);
        const clk = num(r.clicks);
        const pos = num(r.position);

        b.impressions += imp;
        b.clicks += clk;

        if (imp > 0 && pos > 0) {
            b.posWeightedSum += pos * imp;
            b.posWeight += imp;
        }

        if (page) b.pages.add(page);

        if (cls.kind === "catalog") b.classifiedCatalog += 1;
        else if (cls.kind === "fallback") b.classifiedFallback += 1;
        else b.classifiedUnknown += 1;
    }

    const out = Array.from(m.entries()).map(([state, b]) => {
        const ctr = b.impressions > 0 ? b.clicks / b.impressions : 0;
        const position = b.posWeight > 0 ? b.posWeightedSum / b.posWeight : 0;
        return {
            state,
            impressions: b.impressions,
            clicks: b.clicks,
            ctr,
            position,
            pagesCounted: b.pages.size,
            classified: {
                catalog: b.classifiedCatalog,
                fallback: b.classifiedFallback,
                unknown: b.classifiedUnknown,
            },
            keywordsCount: 0, // injected later
        };
    });

    out.sort((a, b) => (b.impressions || 0) - (a.impressions || 0));
    return out;
}

function filterPagesByState(pages: GscRow[], stateName: string, catalogByHostname: Record<string, any>) {
    const target = normalizeState(stateName);
    if (!target) return pages;

    return pages.filter((r) => {
        const page = s(r.page);
        const cls = classifyStateWithCatalog(page, catalogByHostname);
        return normalizeState(cls.state) === target;
    });
}

/** ======= Trend filtering + comparison ======= */

function pickTrendDate(r: AnyObj): string {
    return s(r.date) || s(r.day) || s(Array.isArray(r.keys) ? r.keys[0] : "");
}

function toDate(dateStr: string): Date | null {
    if (!dateStr) return null;
    const d = new Date(dateStr + "T00:00:00Z");
    return Number.isFinite(d.getTime()) ? d : null;
}

function filterTrendByDate(trendRows: AnyObj[], startDate?: string | null, endDate?: string | null) {
    const s0 = s(startDate);
    const e0 = s(endDate);
    if (!s0 || !e0) return trendRows;

    const start = toDate(s0);
    const end = toDate(e0);
    if (!start || !end) return trendRows;

    return trendRows.filter((r) => {
        const dStr = pickTrendDate(r);
        const d = toDate(dStr);
        if (!d) return false;
        return d.getTime() >= start.getTime() && d.getTime() <= end.getTime();
    });
}

function summarizeTrend(trendRows: AnyObj[]) {
    let impressions = 0;
    let clicks = 0;

    let posWeightedSum = 0;
    let posWeight = 0;

    for (const r of trendRows) {
        const imp = num(r.impressions);
        const clk = num(r.clicks);
        const pos = num(r.position);

        impressions += imp;
        clicks += clk;

        if (imp > 0 && pos > 0) {
            posWeightedSum += pos * imp;
            posWeight += imp;
        }
    }

    const ctr = impressions > 0 ? clicks / impressions : 0;
    const position = posWeight > 0 ? posWeightedSum / posWeight : 0;

    return { impressions, clicks, ctr, position };
}

function daysBetweenInclusive(startDate: string, endDate: string) {
    const a = toDate(startDate);
    const b = toDate(endDate);
    if (!a || !b) return 0;
    const diff = Math.round((b.getTime() - a.getTime()) / 864e5);
    return diff + 1;
}

function addDays(dateStr: string, delta: number) {
    const d = toDate(dateStr);
    if (!d) return dateStr;
    d.setUTCDate(d.getUTCDate() + delta);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    const da = String(d.getUTCDate()).padStart(2, "0");
    return `${y}-${m}-${da}`;
}

/** ======= Funnels aggregation ======= */

function groupFunnelsBySubdomain(rows: GscRow[], rootHost: string) {
    const m = new Map<
        string,
        {
            host: string;
            left: string;
            name: string;
            impressions: number;
            clicks: number;
            posWeightedSum: number;
            posWeight: number;
            pages: Set<string>;
        }
    >();

    for (const r of rows) {
        const page = s(r.page);
        const host = safeHostname(page);
        const left = getLeftLabel(host);

        const key = host || left || "__unknown_funnel";
        if (!m.has(key)) {
            m.set(key, {
                host,
                left,
                name: prettifySubdomainName(left || host || "Funnel"),
                impressions: 0,
                clicks: 0,
                posWeightedSum: 0,
                posWeight: 0,
                pages: new Set<string>(),
            });
        }

        const b = m.get(key)!;
        const imp = num(r.impressions);
        const clk = num(r.clicks);
        const pos = num(r.position);

        b.impressions += imp;
        b.clicks += clk;

        if (imp > 0 && pos > 0) {
            b.posWeightedSum += pos * imp;
            b.posWeight += imp;
        }

        if (page) b.pages.add(page);
    }

    const out = Array.from(m.values()).map((b) => {
        const ctr = b.impressions > 0 ? b.clicks / b.impressions : 0;
        const position = b.posWeight > 0 ? b.posWeightedSum / b.posWeight : 0;
        return {
            funnel: b.name,
            host: b.host,
            subdomain: b.left,
            impressions: b.impressions,
            clicks: b.clicks,
            ctr,
            position,
            pagesCounted: b.pages.size,
        };
    });

    out.sort((a, b) => (b.impressions || 0) - (a.impressions || 0));
    return out;
}

/** ======= Keywords (query+page) ======= */

function buildKeywordAnalytics(args: {
    qpRows: GscRow[];
    rootHost: string;
    catalogByHostname: Record<string, any>;
    selectedState?: string | null;
}) {
    const targetState = args.selectedState ? normalizeState(args.selectedState) : "";

    const uniqOverall = new Set<string>();
    const uniqFiltered = new Set<string>();

    // state -> set(query)
    const byState = new Map<string, Set<string>>();

    // query -> impressions (for top keywords)
    const impByQueryOverall = new Map<string, number>();
    const impByQueryFiltered = new Map<string, number>();

    let considered = 0;
    let skippedNoState = 0;
    let skippedNoKeys = 0;

    for (const r of args.qpRows) {
        const q = s(r.query);
        const p = s(r.page);
        if (!q || !p) {
            skippedNoKeys += 1;
            continue;
        }

        const stRaw = classifyStateWithCatalog(p, args.catalogByHostname).state || "__unknown";
        const st = normalizeState(stRaw);

        if (!st || st === "__unknown") {
            skippedNoState += 1;
            continue;
        }

        considered += 1;

        uniqOverall.add(q);
        impByQueryOverall.set(q, (impByQueryOverall.get(q) || 0) + num(r.impressions));

        if (!byState.has(st)) byState.set(st, new Set<string>());
        byState.get(st)!.add(q);

        if (targetState && st === targetState) {
            uniqFiltered.add(q);
            impByQueryFiltered.set(q, (impByQueryFiltered.get(q) || 0) + num(r.impressions));
        }
    }

    const topKeywordsOverall = Array.from(impByQueryOverall.entries())
        .sort((a, b) => (b[1] || 0) - (a[1] || 0))
        .slice(0, 50)
        .map(([query, impressions]) => ({ query, impressions }));

    const topKeywordsFiltered = Array.from(impByQueryFiltered.entries())
        .sort((a, b) => (b[1] || 0) - (a[1] || 0))
        .slice(0, 50)
        .map(([query, impressions]) => ({ query, impressions }));

    return {
        keywordsOverall: uniqOverall.size,
        keywordsFiltered: targetState ? uniqFiltered.size : null,
        byState,
        topKeywordsOverall,
        topKeywordsFiltered,
        debug: {
            qpRows: args.qpRows.length,
            considered,
            skippedNoState,
            skippedNoKeys,
            selectedState: targetState || null,
        },
    };
}

export async function GET(req: Request) {
    try {
        const url = new URL(req.url);
        const range = url.searchParams.get("range") || "";
        const state = url.searchParams.get("state") || "";
        const compareEnabled = url.searchParams.get("compare") === "1";

        const forceCatalog =
            url.searchParams.get("forceCatalog") === "1" ||
            url.searchParams.get("force") === "1";

        const cacheDir = path.join(process.cwd(), "data", "cache", "gsc");

        const metaRaw = await readJsonRaw(path.join(cacheDir, "meta.json"));
        const pagesRaw = await readJsonRaw(path.join(cacheDir, "pages.json"));
        const queriesRaw = await readJsonRaw(path.join(cacheDir, "queries.json"));

        // ✅ FIX: read qp.json first; fallback to query_pages.json if needed
        const qpRawPrimary = await readJsonRaw(path.join(cacheDir, "qp.json"));
        const qpRawFallback = qpRawPrimary ? null : await readJsonRaw(path.join(cacheDir, "query_pages.json"));
        const qpRaw = qpRawPrimary || qpRawFallback;

        const trendRaw = await readJsonRaw(path.join(cacheDir, "trend.json"));

        const meta: MetaFile = (metaRaw || {}) as MetaFile;

        const pages = coerceGscRows(pagesRaw, "pages");
        const queries = coerceGscRows(queriesRaw, "queries");
        const qpRows = coerceQpRows(qpRaw);
        const trend = unwrapArray(trendRaw);

        const catalog = await loadGscCatalogIndex({ force: forceCatalog });
        const catalogByHostname = catalog.byHostname || {};

        const rootHost = inferRootHost(meta?.siteUrl);

        // ===== Bucket split =====
        const deltaPages: GscRow[] = [];
        const nationwidePages: GscRow[] = [];
        const funnelPages: GscRow[] = [];

        for (const r of pages) {
            const page = s(r.page);
            const bucket = classifyPageBucket(page, rootHost, catalogByHostname);
            if (bucket.kind === "nationwide") nationwidePages.push(r);
            else if (bucket.kind === "funnel") funnelPages.push(r);
            else deltaPages.push(r);
        }

        const summaryOverallCore = computeSummaryFromPages(pages);
        const summaryOverall = {
            ...summaryOverallCore,
            generatedAt: meta?.fetchedAt || null,
            startDate: meta?.startDate || null,
            endDate: meta?.endDate || null,
        };

        const summaryNationwideCore = computeSummaryFromPages(nationwidePages);
        const summaryNationwide = {
            ...summaryNationwideCore,
            label: "Nationwide / Home Page",
            rootHost,
        };

        const funnelRows = groupFunnelsBySubdomain(funnelPages, rootHost);
        const summaryFunnelsCore = computeSummaryFromPages(funnelPages);
        const summaryFunnels = {
            ...summaryFunnelsCore,
            label: "Funnels (non-Delta subdomains)",
        };

        // states from delta pages
        const stateRows = groupByStateFromPagesWithCatalog(deltaPages, catalogByHostname);

        // filter delta pages by state
        const deltaPagesFiltered = state ? filterPagesByState(deltaPages, state, catalogByHostname) : deltaPages;

        const summaryFilteredCore = computeSummaryFromPages(deltaPagesFiltered);
        const summaryFiltered = {
            ...summaryFilteredCore,
            generatedAt: meta?.fetchedAt || null,
            startDate: meta?.startDate || null,
            endDate: meta?.endDate || null,
            state: state ? normalizeState(state) : null,
        };

        const top = {
            queries: queries.slice(0, 100),
            pages: deltaPagesFiltered.slice(0, 100),
            nationwidePages: nationwidePages.slice(0, 50),
            funnelPages: funnelPages.slice(0, 50),
        };

        const trendFiltered = filterTrendByDate(trend, meta?.startDate || null, meta?.endDate || null);

        // ✅ keywords analytics from qpRows
        const kw = buildKeywordAnalytics({
            qpRows,
            rootHost,
            catalogByHostname,
            selectedState: state ? state : null,
        });

        // inject keywordsCount into stateRows
        for (const row of stateRows) {
            const st = normalizeState(row.state);
            row.keywordsCount = kw.byState.get(st)?.size || 0;
        }

        let compare: any = null;
        if (compareEnabled && meta?.startDate && meta?.endDate) {
            const windowDays = daysBetweenInclusive(meta.startDate, meta.endDate);
            if (windowDays > 0) {
                const prevEnd = addDays(meta.startDate, -1);
                const prevStart = addDays(meta.startDate, -windowDays);

                const prevTrend = filterTrendByDate(trend, prevStart, prevEnd);

                const curSum = summarizeTrend(trendFiltered);
                const prevSum = summarizeTrend(prevTrend);

                const pct = (cur: number, prev: number) => (prev > 0 ? (cur - prev) / prev : null);

                compare = {
                    windowDays,
                    current: { ...curSum, startDate: meta.startDate, endDate: meta.endDate },
                    previous: { ...prevSum, startDate: prevStart, endDate: prevEnd },
                    pct: {
                        impressions: pct(curSum.impressions, prevSum.impressions),
                        clicks: pct(curSum.clicks, prevSum.clicks),
                        ctr: pct(curSum.ctr, prevSum.ctr),
                        position: pct(curSum.position, prevSum.position),
                    },
                    delta: {
                        impressions: curSum.impressions - prevSum.impressions,
                        clicks: curSum.clicks - prevSum.clicks,
                        ctr: curSum.ctr - prevSum.ctr,
                        position: curSum.position - prevSum.position,
                    },
                    debug: {
                        trendHasPrevWindow: prevTrend.length > 0,
                        trendTotalRows: trend.length,
                        trendFilteredRows: trendFiltered.length,
                        metaTrendStart: meta?.trendStart || null,
                        metaTrendIncludesCompare: !!meta?.trendIncludesCompare,
                    },
                };
            }
        }

        const counts = {
            pages: pages.length,
            deltaPages: deltaPages.length,
            nationwidePages: nationwidePages.length,
            funnelPages: funnelPages.length,
            queries: queries.length,
            qp: qpRows.length,
            trend: Array.isArray(trend) ? trend.length : 0,
            trendFiltered: Array.isArray(trendFiltered) ? trendFiltered.length : 0,
            catalogHosts: Object.keys(catalogByHostname).length,
            catalogStates: catalog.statesPresent.size,
            funnels: funnelRows.length,
        };

        return NextResponse.json({
            ok: true,
            meta: {
                ok: meta?.ok ?? true,
                siteUrl: meta?.siteUrl || "",
                range: meta?.range || range,
                startDate: meta?.startDate || null,
                endDate: meta?.endDate || null,
                fetchedAt: meta?.fetchedAt || null,
                error: meta?.error || null,
                trendStart: meta?.trendStart || null,
                trendIncludesCompare: !!meta?.trendIncludesCompare,
                qpIncluded: meta?.qpIncluded ?? (qpRows.length > 0),
                keywordDebug: kw.debug,
            },

            counts,

            summaryOverall,
            summaryFiltered,
            summaryNationwide,
            summaryFunnels,

            // ✅ keywords outputs
            keywordsOverall: kw.keywordsOverall,
            keywordsFiltered: kw.keywordsFiltered,
            topKeywordsOverall: kw.topKeywordsOverall,
            topKeywordsFiltered: kw.topKeywordsFiltered,

            stateRows,
            funnels: funnelRows,

            top,
            trend,
            trendFiltered,

            compareEnabled,
            compare,

            debug: {
                cacheDir,
                catalogBaseDir: catalog.baseDir,
                catalogFingerprint: catalog.fingerprint,
                rootHost,
                range,
                state,
                forceCatalog,
                rawShapes: {
                    pagesRaw: Array.isArray(pagesRaw) ? "array" : typeof pagesRaw,
                    queriesRaw: Array.isArray(queriesRaw) ? "array" : typeof queriesRaw,
                    qpRaw: Array.isArray(qpRaw) ? "array" : typeof qpRaw,
                    trendRaw: Array.isArray(trendRaw) ? "array" : typeof trendRaw,
                },
            },
        });
    } catch (e: any) {
        return NextResponse.json({ ok: false, error: e?.message || "Failed to join GSC cache data" }, { status: 500 });
    }
}
