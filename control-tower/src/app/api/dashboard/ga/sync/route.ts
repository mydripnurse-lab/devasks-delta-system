// control-tower/src/app/api/dashboard/ga/sync/route.ts
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

function parseRange(preset: string, start?: string, end?: string) {
    const today = new Date().toISOString().slice(0, 10);

    if (preset === "custom") {
        return {
            startDate: s(start) || daysAgoISO(28),
            endDate: s(end) || today,
            range: "custom",
        };
    }

    switch (preset) {
        case "last_7_days":
            return { startDate: daysAgoISO(7), endDate: today, range: "last_7_days" };
        case "last_28_days":
            return { startDate: daysAgoISO(28), endDate: today, range: "last_28_days" };
        case "last_month":
            return { startDate: monthsAgoISO(1), endDate: today, range: "last_month" };
        case "last_quarter":
            return { startDate: monthsAgoISO(3), endDate: today, range: "last_quarter" };
        case "last_6_months":
            return { startDate: monthsAgoISO(6), endDate: today, range: "last_6_months" };
        case "last_year":
            return { startDate: monthsAgoISO(12), endDate: today, range: "last_year" };
        default:
            return { startDate: daysAgoISO(28), endDate: today, range: "last_28_days" };
    }
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

/** Reads refresh token from your existing gsc_tokens.json */
async function loadRefreshTokenFromSecrets() {
    const filePath = path.join(process.cwd(), "data", "secrets", "gsc_tokens.json");
    const raw = await fs.readFile(filePath, "utf8");
    const json = JSON.parse(raw);

    const refresh_token = s(json?.tokens?.refresh_token);
    const scopes = s(json?.tokens?.scope);

    if (!refresh_token) throw new Error("Missing refresh_token in data/secrets/gsc_tokens.json");
    return { refresh_token, scopes };
}

async function refreshAccessToken(refreshToken: string) {
    const clientId = s(process.env.GSC_CLIENT_ID);
    const clientSecret = s(process.env.GSC_CLIENT_SECRET);

    if (!clientId || !clientSecret) {
        throw new Error("Missing GSC_CLIENT_ID / GSC_CLIENT_SECRET in env.");
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

async function gaRunReport(params: {
    propertyId: string;
    accessToken: string;
    startDate: string;
    endDate: string;
    dimensions: string[];
    metrics: string[];
    limit?: number;
    orderBys?: AnyObj[];
}) {
    const url = `https://analyticsdata.googleapis.com/v1beta/properties/${encodeURIComponent(
        params.propertyId,
    )}:runReport`;

    const payload: AnyObj = {
        dateRanges: [{ startDate: params.startDate, endDate: params.endDate }],
        dimensions: params.dimensions.map((name) => ({ name })),
        metrics: params.metrics.map((name) => ({ name })),
        limit: params.limit ?? 25000,
    };

    if (Array.isArray(params.orderBys) && params.orderBys.length) {
        payload.orderBys = params.orderBys;
    }

    const res = await fetch(url, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${params.accessToken}`,
            "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
    });

    const json = await res.json();
    if (!res.ok) {
        throw new Error(json?.error?.message || `GA4 HTTP ${res.status}`);
    }

    return json;
}

export async function GET(req: Request) {
    try {
        const u = new URL(req.url);
        const preset = s(u.searchParams.get("range") || "last_28_days");
        const start = s(u.searchParams.get("start"));
        const end = s(u.searchParams.get("end"));
        const force = s(u.searchParams.get("force")) === "1";
        const compare = s(u.searchParams.get("compare")) === "1";

        const propertyId = s(process.env.GA4_PROPERTY_ID);
        if (!propertyId) throw new Error("Missing env: GA4_PROPERTY_ID");

        const { startDate, endDate, range } = parseRange(preset, start, end);

        const cacheDir = path.join(process.cwd(), "data", "cache", "ga");
        const metaPath = path.join(cacheDir, "meta.json");
        await ensureDir(cacheDir);

        const metaPrev = await readMeta(metaPath);
        const stale = isStale(metaPrev, 10);

        const windowDays = daysBetweenInclusive(startDate, endDate);
        const trendStart = compare && windowDays > 0 ? addDays(startDate, -windowDays) : startDate;

        if (
            !force &&
            !stale &&
            metaPrev?.range === range &&
            metaPrev?.startDate === startDate &&
            metaPrev?.endDate === endDate &&
            !!metaPrev?.trendIncludesCompare === !!compare
        ) {
            return NextResponse.json({ ok: true, meta: metaPrev, cache: { refreshed: false, reason: "fresh" } });
        }

        const { refresh_token, scopes } = await loadRefreshTokenFromSecrets();

        // sanity: if GA scope missing, you’ll get 403 later – make it obvious
        if (!scopes.includes("analytics")) {
            // still proceed, but warn in meta
        }

        const { access_token } = await refreshAccessToken(refresh_token);
        if (!access_token) throw new Error("Could not refresh access token");

        // 1) Trend (date)
        const trend = await gaRunReport({
            propertyId,
            accessToken: access_token,
            startDate: trendStart,
            endDate,
            dimensions: ["date"],
            metrics: ["sessions", "activeUsers", "screenPageViews", "engagementRate", "conversions"],
            limit: 25000,
        });

        // 2) By state/region
        const byRegion = await gaRunReport({
            propertyId,
            accessToken: access_token,
            startDate,
            endDate,
            dimensions: ["country", "region"],
            metrics: ["sessions", "activeUsers", "screenPageViews", "engagementRate", "conversions"],
            limit: 25000,
            orderBys: [{ metric: { metricName: "sessions" }, desc: true }],
        });

        // 3) By city
        const byCity = await gaRunReport({
            propertyId,
            accessToken: access_token,
            startDate,
            endDate,
            dimensions: ["country", "region", "city"],
            metrics: ["sessions", "activeUsers", "screenPageViews", "engagementRate", "conversions"],
            limit: 25000,
            orderBys: [{ metric: { metricName: "sessions" }, desc: true }],
        });

        // 4) Landing pages
        const landing = await gaRunReport({
            propertyId,
            accessToken: access_token,
            startDate,
            endDate,
            dimensions: ["landingPagePlusQueryString"],
            metrics: ["sessions", "activeUsers", "screenPageViews", "engagementRate", "conversions"],
            limit: 25000,
            orderBys: [{ metric: { metricName: "sessions" }, desc: true }],
        });

        // 5) Source / Medium
        const sourceMedium = await gaRunReport({
            propertyId,
            accessToken: access_token,
            startDate,
            endDate,
            dimensions: ["sessionSource", "sessionMedium"],
            metrics: ["sessions", "activeUsers", "screenPageViews", "engagementRate", "conversions"],
            limit: 25000,
            orderBys: [{ metric: { metricName: "sessions" }, desc: true }],
        });

        const fetchedAt = new Date().toISOString();
        const meta = {
            ok: true,
            propertyId,
            range,
            startDate,
            endDate,
            fetchedAt,
            trendStart,
            trendIncludesCompare: !!compare,
            warning: scopes.includes("analytics")
                ? null
                : "Refresh token scope does NOT include analytics.readonly. Re-run /api/auth/gsc/start and consent again.",
        };

        await fs.writeFile(path.join(cacheDir, "meta.json"), JSON.stringify(meta, null, 2), "utf8");
        await fs.writeFile(path.join(cacheDir, "trend.json"), JSON.stringify(trend, null, 2), "utf8");
        await fs.writeFile(path.join(cacheDir, "by_region.json"), JSON.stringify(byRegion, null, 2), "utf8");
        await fs.writeFile(path.join(cacheDir, "by_city.json"), JSON.stringify(byCity, null, 2), "utf8");
        await fs.writeFile(path.join(cacheDir, "landing.json"), JSON.stringify(landing, null, 2), "utf8");
        await fs.writeFile(path.join(cacheDir, "source_medium.json"), JSON.stringify(sourceMedium, null, 2), "utf8");

        return NextResponse.json({
            ok: true,
            meta,
            counts: {
                trend: trend?.rows?.length || 0,
                byRegion: byRegion?.rows?.length || 0,
                byCity: byCity?.rows?.length || 0,
                landing: landing?.rows?.length || 0,
                sourceMedium: sourceMedium?.rows?.length || 0,
            },
            cache: { refreshed: true },
        });
    } catch (e: any) {
        return NextResponse.json({ ok: false, error: e?.message || "GA sync failed" }, { status: 500 });
    }
}
