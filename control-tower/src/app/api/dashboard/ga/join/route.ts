// control-tower/src/app/api/dashboard/ga/join/route.ts
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

async function readJson(absPath: string) {
    try {
        const txt = await fs.readFile(absPath, "utf8");
        return JSON.parse(txt);
    } catch {
        return null;
    }
}

function pickDim(row: AnyObj, idx: number) {
    return s(row?.dimensionValues?.[idx]?.value);
}
function pickMet(row: AnyObj, idx: number) {
    return num(row?.metricValues?.[idx]?.value);
}

function parseDateYYYYMMDD(x: string) {
    // GA returns date as YYYYMMDD
    const raw = s(x);
    if (!raw || raw.length !== 8) return raw;
    return `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}`;
}

function summarizeRows(rows: AnyObj[]) {
    let sessions = 0;
    let users = 0;
    let views = 0;
    let conversions = 0;
    let engagementRateSum = 0;
    let engagementRateCount = 0;

    for (const r of rows) {
        sessions += pickMet(r, 0);
        users += pickMet(r, 1);
        views += pickMet(r, 2);

        const er = pickMet(r, 3);
        if (er > 0) {
            engagementRateSum += er;
            engagementRateCount += 1;
        }

        conversions += pickMet(r, 4);
    }

    const engagementRate = engagementRateCount ? engagementRateSum / engagementRateCount : 0;

    return { sessions, users, views, engagementRate, conversions };
}

function filterTrendByDate(trendRows: AnyObj[], startDate?: string | null, endDate?: string | null) {
    const s0 = s(startDate);
    const e0 = s(endDate);
    if (!s0 || !e0) return trendRows;

    return trendRows.filter((r) => {
        const d = parseDateYYYYMMDD(pickDim(r, 0));
        return d >= s0 && d <= e0;
    });
}

function summarizeTrend(trendRows: AnyObj[]) {
    // trend metrics order: sessions, activeUsers, screenPageViews, engagementRate, conversions
    return summarizeRows(trendRows);
}

// =====================
// Puerto Rico injection helpers
// =====================
function normKey(x: any) {
    return s(x).toLowerCase();
}

function weightedEngagementRate(rows: AnyObj[]) {
    // engagementRate is a ratio (0..1). Weighted by sessions.
    let wSum = 0;
    let w = 0;
    for (const r of rows) {
        const sessions = num(r.sessions);
        const er = num(r.engagementRate);
        if (sessions > 0 && er >= 0) {
            wSum += er * sessions;
            w += sessions;
        }
    }
    return w > 0 ? wSum / w : 0;
}

function sumField(rows: AnyObj[], k: string) {
    let t = 0;
    for (const r of rows) t += num(r[k]);
    return t;
}

export async function GET(req: Request) {
    try {
        const url = new URL(req.url);
        const range = s(url.searchParams.get("range"));
        const compareEnabled = url.searchParams.get("compare") === "1";

        const cacheDir = path.join(process.cwd(), "data", "cache", "ga");

        const meta = (await readJson(path.join(cacheDir, "meta.json"))) || {};
        const trendRaw = (await readJson(path.join(cacheDir, "trend.json"))) || {};
        const byRegionRaw = (await readJson(path.join(cacheDir, "by_region.json"))) || {};
        const byCityRaw = (await readJson(path.join(cacheDir, "by_city.json"))) || {};
        const landingRaw = (await readJson(path.join(cacheDir, "landing.json"))) || {};
        const sourceMediumRaw = (await readJson(path.join(cacheDir, "source_medium.json"))) || {};

        const trendRows = Array.isArray(trendRaw?.rows) ? trendRaw.rows : [];
        const byRegionRows = Array.isArray(byRegionRaw?.rows) ? byRegionRaw.rows : [];
        const byCityRows = Array.isArray(byCityRaw?.rows) ? byCityRaw.rows : [];
        const landingRows = Array.isArray(landingRaw?.rows) ? landingRaw.rows : [];
        const sourceMediumRows = Array.isArray(sourceMediumRaw?.rows) ? sourceMediumRaw.rows : [];

        const startDate = s(meta?.startDate) || null;
        const endDate = s(meta?.endDate) || null;

        const trendFiltered = filterTrendByDate(trendRows, startDate, endDate);

        const summaryOverall = {
            ...summarizeTrend(trendFiltered),
            generatedAt: meta?.fetchedAt || null,
            startDate,
            endDate,
        };

        // State/Region table (base from by_region)
        const stateRowsBase = byRegionRows.map((r: AnyObj) => {
            const country = pickDim(r, 0);
            const region = pickDim(r, 1);

            return {
                country,
                region: region || "__unknown",
                sessions: pickMet(r, 0),
                users: pickMet(r, 1),
                views: pickMet(r, 2),
                engagementRate: pickMet(r, 3),
                conversions: pickMet(r, 4),
            };
        });

        // Top cities
        const topCities = byCityRows.slice(0, 200).map((r: AnyObj) => {
            const country = pickDim(r, 0);
            const region = pickDim(r, 1);
            const city = pickDim(r, 2);
            return {
                country,
                region: region || "__unknown",
                city: city || "__unknown",
                sessions: pickMet(r, 0),
                users: pickMet(r, 1),
                views: pickMet(r, 2),
                engagementRate: pickMet(r, 3),
                conversions: pickMet(r, 4),
            };
        });

        // ✅ Inject Puerto Rico into stateRows if not present in by_region
        // GA4 often returns PR as country="Puerto Rico" and region="(not set)".
        // The map expects a "state-like" row with region="Puerto Rico".
        const hasPRAlready = stateRowsBase.some((x) => normKey(x.region) === "puerto rico");
        let stateRows = stateRowsBase;

        if (!hasPRAlready) {
            const prCityRows = topCities.filter((c) => normKey(c.country) === "puerto rico");

            if (prCityRows.length > 0) {
                const prRow = {
                    country: "Puerto Rico",
                    region: "Puerto Rico",
                    sessions: sumField(prCityRows, "sessions"),
                    users: sumField(prCityRows, "users"),
                    views: sumField(prCityRows, "views"),
                    engagementRate: weightedEngagementRate(prCityRows),
                    conversions: sumField(prCityRows, "conversions"),
                };

                stateRows = [...stateRowsBase, prRow];
            }
        }

        const topLanding = landingRows.slice(0, 200).map((r: AnyObj) => {
            const lp = pickDim(r, 0);
            return {
                landingPage: lp || "—",
                sessions: pickMet(r, 0),
                users: pickMet(r, 1),
                views: pickMet(r, 2),
                engagementRate: pickMet(r, 3),
                conversions: pickMet(r, 4),
            };
        });

        const topSourceMedium = sourceMediumRows.slice(0, 200).map((r: AnyObj) => {
            const src = pickDim(r, 0);
            const med = pickDim(r, 1);
            return {
                source: src || "(direct)",
                medium: med || "(none)",
                sessions: pickMet(r, 0),
                users: pickMet(r, 1),
                views: pickMet(r, 2),
                engagementRate: pickMet(r, 3),
                conversions: pickMet(r, 4),
            };
        });

        // Compare (trend-based) — same concept as GSC
        let compare: any = null;
        if (compareEnabled && startDate && endDate) {
            const windowDays = (() => {
                // inclusive count
                const a = new Date(startDate + "T00:00:00Z").getTime();
                const b = new Date(endDate + "T00:00:00Z").getTime();
                const diff = Math.round((b - a) / 864e5);
                return diff + 1;
            })();

            const prevEnd = new Date(startDate + "T00:00:00Z");
            prevEnd.setUTCDate(prevEnd.getUTCDate() - 1);
            const prevEndISO = prevEnd.toISOString().slice(0, 10);

            const prevStart = new Date(startDate + "T00:00:00Z");
            prevStart.setUTCDate(prevStart.getUTCDate() - windowDays);
            const prevStartISO = prevStart.toISOString().slice(0, 10);

            const prevTrend = filterTrendByDate(trendRows, prevStartISO, prevEndISO);

            const curSum = summarizeTrend(trendFiltered);
            const prevSum = summarizeTrend(prevTrend);

            const pct = (cur: number, prev: number) => (prev > 0 ? (cur - prev) / prev : null);

            compare = {
                windowDays,
                current: { ...curSum, startDate, endDate },
                previous: { ...prevSum, startDate: prevStartISO, endDate: prevEndISO },
                pct: {
                    sessions: pct(curSum.sessions, prevSum.sessions),
                    users: pct(curSum.users, prevSum.users),
                    views: pct(curSum.views, prevSum.views),
                    engagementRate: pct(curSum.engagementRate, prevSum.engagementRate),
                    conversions: pct(curSum.conversions, prevSum.conversions),
                },
                delta: {
                    sessions: curSum.sessions - prevSum.sessions,
                    users: curSum.users - prevSum.users,
                    views: curSum.views - prevSum.views,
                    engagementRate: curSum.engagementRate - prevSum.engagementRate,
                    conversions: curSum.conversions - prevSum.conversions,
                },
            };
        }

        return NextResponse.json({
            ok: true,
            meta: {
                ok: meta?.ok ?? true,
                range: meta?.range || range,
                startDate,
                endDate,
                fetchedAt: meta?.fetchedAt || null,
                propertyId: meta?.propertyId || null,
                warning: meta?.warning || null,
                trendStart: meta?.trendStart || null,
                trendIncludesCompare: !!meta?.trendIncludesCompare,
            },

            summaryOverall,
            trend: trendRows.map((r: AnyObj) => ({
                date: parseDateYYYYMMDD(pickDim(r, 0)),
                sessions: pickMet(r, 0),
                users: pickMet(r, 1),
                views: pickMet(r, 2),
                engagementRate: pickMet(r, 3),
                conversions: pickMet(r, 4),
            })),
            trendFiltered: trendFiltered.map((r: AnyObj) => ({
                date: parseDateYYYYMMDD(pickDim(r, 0)),
                sessions: pickMet(r, 0),
                users: pickMet(r, 1),
                views: pickMet(r, 2),
                engagementRate: pickMet(r, 3),
                conversions: pickMet(r, 4),
            })),

            stateRows,
            topCities,
            topLanding,
            topSourceMedium,

            compareEnabled,
            compare,

            counts: {
                trend: trendRows.length,
                trendFiltered: trendFiltered.length,
                byRegion: byRegionRows.length,
                byCity: byCityRows.length,
                landing: landingRows.length,
                sourceMedium: sourceMediumRows.length,
            },
        });
    } catch (e: any) {
        return NextResponse.json({ ok: false, error: e?.message || "GA join failed" }, { status: 500 });
    }
}
