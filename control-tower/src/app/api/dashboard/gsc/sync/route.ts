// control-tower/src/app/api/dashboard/gsc/sync/route.ts
import { NextResponse } from "next/server";
import fs from "fs/promises";
import path from "path";

export const runtime = "nodejs";

type AnyObj = Record<string, any>;

function s(v: any) {
    return String(v ?? "").trim();
}
function num(v: any) {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
}

function daysAgoISO(days: number) {
    const d = new Date();
    d.setDate(d.getDate() - days);
    return d.toISOString().slice(0, 10);
}

function monthsAgoISO(months: number) {
    const d = new Date();
    d.setMonth(d.getMonth() - months);
    return d.toISOString().slice(0, 10);
}

function toDate(dateStr: string): Date | null {
    if (!dateStr) return null;
    const d = new Date(dateStr + "T00:00:00Z");
    return Number.isFinite(d.getTime()) ? d : null;
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

function parseRange(preset: string, start?: string, end?: string) {
    const today = new Date().toISOString().slice(0, 10);
    const now = new Date();
    const prevYear = now.getFullYear() - 1;
    const prevYearStart = `${prevYear}-01-01`;
    const prevYearEnd = `${prevYear}-12-31`;

    const explicitStart = s(start);
    const explicitEnd = s(end);
    if (explicitStart && explicitEnd) {
        return {
            startDate: explicitStart,
            endDate: explicitEnd,
            range: preset || "custom",
        };
    }

    if (preset === "custom") {
        return {
            startDate: s(start) || daysAgoISO(28),
            endDate: s(end) || today,
            range: "custom",
        };
    }

    switch (preset) {
        case "last_7_days":
            return { startDate: daysAgoISO(6), endDate: today, range: "last_7_days" };
        case "last_28_days":
            return { startDate: daysAgoISO(27), endDate: today, range: "last_28_days" };
        case "last_month":
            return { startDate: monthsAgoISO(1), endDate: today, range: "last_month" };
        case "last_quarter":
            return { startDate: monthsAgoISO(3), endDate: today, range: "last_quarter" };
        case "last_6_months":
            return { startDate: monthsAgoISO(6), endDate: today, range: "last_6_months" };
        case "last_year":
            return { startDate: prevYearStart, endDate: prevYearEnd, range: "last_year" };
        default:
            return { startDate: daysAgoISO(28), endDate: today, range: "last_28_days" };
    }
}

/**
 * Lee tu token JSON (OAuth tokens).
 */
async function loadTokens() {
    const envPath = s(process.env.GOOGLE_APPLICATION_CREDENTIALS);
    if (!envPath) throw new Error("Missing GOOGLE_APPLICATION_CREDENTIALS env var");

    const abs = path.isAbsolute(envPath) ? envPath : path.join(process.cwd(), envPath);
    const raw = await fs.readFile(abs, "utf8");
    const json = JSON.parse(raw);

    const access_token = json?.tokens?.access_token;
    const refresh_token = json?.tokens?.refresh_token;
    const expiry_date = num(json?.tokens?.expiry_date);

    const siteUrl = s(json?.siteUrl);
    if (!siteUrl) throw new Error("Token JSON missing siteUrl");
    if (!refresh_token) throw new Error("Token JSON missing refresh_token");

    return { siteUrl, access_token, refresh_token, expiry_date };
}

/**
 * Refresca access token usando refresh_token (OAuth).
 */
async function refreshAccessToken(refreshToken: string) {
    const clientId = s(process.env.GSC_CLIENT_ID);
    const clientSecret = s(process.env.GSC_CLIENT_SECRET);

    if (!clientId || !clientSecret) {
        throw new Error("Missing GSC_CLIENT_ID / GSC_CLIENT_SECRET in env. Required to refresh token.");
    }

    const body = new URLSearchParams();
    body.set("client_id", clientId);
    body.set("client_secret", clientSecret);
    body.set("refresh_token", refreshToken);
    body.set("grant_type", "refresh_token");

    const res = await fetch("https://oauth2.googleapis.com/token", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body,
    });

    const json = await res.json();
    if (!res.ok) throw new Error(json?.error_description || json?.error || `OAuth HTTP ${res.status}`);

    return {
        access_token: s(json.access_token),
        expires_in: num(json.expires_in),
    };
}

async function gscQueryAll(params: {
    siteUrl: string;
    accessToken: string;
    startDate: string;
    endDate: string;
    dimensions: string[];
    rowLimit?: number;
}) {
    const rowLimit = params.rowLimit ?? 25000;

    const allRows: AnyObj[] = [];
    let startRow = 0;

    while (true) {
        const payload: AnyObj = {
            startDate: params.startDate,
            endDate: params.endDate,
            dimensions: params.dimensions,
            rowLimit,
            startRow,
            dataState: "final",
        };

        const url =
            "https://www.googleapis.com/webmasters/v3/sites/" +
            encodeURIComponent(params.siteUrl) +
            "/searchAnalytics/query";

        const res = await fetch(url, {
            method: "POST",
            headers: {
                Authorization: `Bearer ${params.accessToken}`,
                "Content-Type": "application/json",
            },
            body: JSON.stringify(payload),
        });

        const json = await res.json();
        if (!res.ok) throw new Error(json?.error?.message || `GSC HTTP ${res.status}`);

        const rows = Array.isArray(json?.rows) ? json.rows : [];
        allRows.push(...rows);

        if (rows.length < rowLimit) break;

        startRow += rowLimit;
        if (startRow > 500000) break;
    }

    return allRows;
}

async function ensureDir(dir: string) {
    await fs.mkdir(dir, { recursive: true });
}

async function readMeta(metaPath: string) {
    try {
        const raw = await fs.readFile(metaPath, "utf8");
        return JSON.parse(raw);
    } catch {
        return null;
    }
}

function isStale(meta: any, staleMinutes: number) {
    const fetchedAt = meta?.fetchedAt ? new Date(meta.fetchedAt).getTime() : 0;
    if (!fetchedAt) return true;
    const ageMs = Date.now() - fetchedAt;
    return ageMs > staleMinutes * 60 * 1000;
}

export async function GET(req: Request) {
    const cacheDir = path.join(process.cwd(), "data", "cache", "gsc");
    const metaPath = path.join(cacheDir, "meta.json");
    const pagesPath = path.join(cacheDir, "pages.json");
    const queriesPath = path.join(cacheDir, "queries.json");
    const qpPath = path.join(cacheDir, "qp.json");
    const trendPath = path.join(cacheDir, "trend.json");
    try {
        const u = new URL(req.url);
        const preset = s(u.searchParams.get("range") || "last_28_days");
        const start = s(u.searchParams.get("start"));
        const end = s(u.searchParams.get("end"));
        const force = s(u.searchParams.get("force")) === "1";

        // ✅ compare=1 => trend trae también la ventana previa (para %)
        const compare = s(u.searchParams.get("compare")) === "1";

        const { startDate, endDate, range } = parseRange(preset, start, end);

        await ensureDir(cacheDir);

        const metaPrev = await readMeta(metaPath);
        const stale = isStale(metaPrev, 10);

        const wantTrendCompare = compare ? true : false;

        if (
            !force &&
            !stale &&
            metaPrev?.range === range &&
            metaPrev?.startDate === startDate &&
            metaPrev?.endDate === endDate &&
            !!metaPrev?.trendIncludesCompare === wantTrendCompare &&
            // ✅ if we already had qp included, keep cache
            !!metaPrev?.qpIncluded === true
        ) {
            return NextResponse.json({
                ok: true,
                meta: metaPrev,
                cache: { refreshed: false, reason: "fresh" },
            });
        }

        const tok = await loadTokens();

        const { access_token } = await refreshAccessToken(tok.refresh_token);
        if (!access_token) throw new Error("Could not refresh access token");

        // 1) pages (page)
        const pagesRows = await gscQueryAll({
            siteUrl: tok.siteUrl,
            accessToken: access_token,
            startDate,
            endDate,
            dimensions: ["page"],
            rowLimit: 25000,
        });

        // 2) queries (query)
        const queriesRows = await gscQueryAll({
            siteUrl: tok.siteUrl,
            accessToken: access_token,
            startDate,
            endDate,
            dimensions: ["query"],
            rowLimit: 25000,
        });

        // 3) ✅ query + page (para keywords por estado)
        const queryPageRows = await gscQueryAll({
            siteUrl: tok.siteUrl,
            accessToken: access_token,
            startDate,
            endDate,
            dimensions: ["query", "page"],
            rowLimit: 25000,
        });

        // trend por date (si compare=1, incluye ventana previa)
        const windowDays = daysBetweenInclusive(startDate, endDate);
        const trendStart = compare && windowDays > 0 ? addDays(startDate, -windowDays) : startDate;

        const trendRows = await gscQueryAll({
            siteUrl: tok.siteUrl,
            accessToken: access_token,
            startDate: trendStart,
            endDate,
            dimensions: ["date"],
            rowLimit: 25000,
        });

        const fetchedAt = new Date().toISOString();
        const meta = {
            ok: true,
            siteUrl: tok.siteUrl,
            range,
            startDate,
            endDate,
            fetchedAt,
            error: null,
            trendStart,
            trendIncludesCompare: wantTrendCompare,

            // ✅ IMPORTANT: join expects qp.json; we guarantee it's present
            qpIncluded: true,
        };

        await fs.writeFile(path.join(cacheDir, "meta.json"), JSON.stringify(meta, null, 2), "utf8");
        await fs.writeFile(path.join(cacheDir, "pages.json"), JSON.stringify({ rows: pagesRows }, null, 2), "utf8");
        await fs.writeFile(path.join(cacheDir, "queries.json"), JSON.stringify({ rows: queriesRows }, null, 2), "utf8");

        // ✅ FIX: write qp.json (the join reads qp.json)
        await fs.writeFile(path.join(cacheDir, "qp.json"), JSON.stringify({ rows: queryPageRows }, null, 2), "utf8");

        // ✅ optional: keep your previous filename too (backwards compatibility)
        await fs.writeFile(
            path.join(cacheDir, "query_pages.json"),
            JSON.stringify({ rows: queryPageRows }, null, 2),
            "utf8",
        );

        await fs.writeFile(path.join(cacheDir, "trend.json"), JSON.stringify({ rows: trendRows }, null, 2), "utf8");

        return NextResponse.json({
            ok: true,
            meta,
            counts: {
                pages: pagesRows.length,
                queries: queriesRows.length,
                qp: queryPageRows.length,
                trend: trendRows.length,
            },
            cache: { refreshed: true },
        });
    } catch (e: any) {
        const rootErr = e?.message || "GSC sync failed";
        try {
            await Promise.all([
                fs.access(metaPath),
                fs.access(pagesPath),
                fs.access(queriesPath),
                fs.access(qpPath),
                fs.access(trendPath),
            ]);
            let snapshotAgeMs: number | null = null;
            try {
                const st = await fs.stat(metaPath);
                snapshotAgeMs = Math.max(0, Date.now() - st.mtimeMs);
            } catch {
                snapshotAgeMs = null;
            }
            return NextResponse.json({
                ok: true,
                cached: true,
                stale: true,
                usedSnapshot: true,
                warning: rootErr,
                snapshotAgeMs,
                message: "GSC sync failed; using cached snapshot",
            });
        } catch {
            return NextResponse.json({ ok: false, error: rootErr }, { status: 500 });
        }
    }
}
