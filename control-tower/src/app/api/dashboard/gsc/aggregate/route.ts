import fs from "fs/promises";
import path from "path";

export const runtime = "nodejs";

function s(v: any) {
    return String(v ?? "").trim();
}
function num(v: any) {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
}
function safePct(clicks: number, impressions: number) {
    if (!impressions) return 0;
    return clicks / impressions;
}
function weightedAvgPosition(rows: any[]) {
    const imp = rows.reduce((a, r) => a + num(r.impressions), 0);
    if (!imp) return 0;
    const w = rows.reduce((a, r) => a + num(r.position) * num(r.impressions), 0);
    return w / imp;
}

const CACHE_DIR = path.join(process.cwd(), "data", "cache", "gsc");

async function readJson(name: string) {
    const p = path.join(CACHE_DIR, name);
    const txt = await fs.readFile(p, "utf8");
    return JSON.parse(txt);
}

function sliceByDate(rows: any[], start: string, end: string) {
    if (!start || !end) return rows;
    const a = new Date(start).getTime();
    const b = new Date(end).getTime();
    if (!Number.isFinite(a) || !Number.isFinite(b)) return rows;
    return rows.filter((r) => {
        const d = new Date(String(r.date || r.keys?.[0] || "")).getTime();
        if (!Number.isFinite(d)) return true;
        return d >= a && d <= b;
    });
}

function prevPeriodRange(startIso: string, endIso: string) {
    const start = new Date(startIso).getTime();
    const end = new Date(endIso).getTime();
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
        return { prevStart: "", prevEnd: "" };
    }
    const len = end - start;
    const prevEnd = new Date(start - 1);
    const prevStart = new Date(start - 1 - len);
    return { prevStart: prevStart.toISOString(), prevEnd: prevEnd.toISOString() };
}

function percentChange(curr: number, prev: number) {
    if (!Number.isFinite(curr) || !Number.isFinite(prev)) return null;
    if (prev === 0) return curr === 0 ? 0 : 100;
    return ((curr - prev) / prev) * 100;
}

export async function GET(req: Request) {
    try {
        const { searchParams } = new URL(req.url);
        const start = s(searchParams.get("start"));
        const end = s(searchParams.get("end"));

        const meta = await readJson("meta.json");
        const pages = await readJson("pages.json");
        const trend = await readJson("trend.json");

        const trendRowsAll = Array.isArray(trend?.rows) ? trend.rows : [];
        const trendRows = sliceByDate(trendRowsAll, start, end);

        const pageRowsAll = Array.isArray(pages?.rows) ? pages.rows : [];
        const pageRows = pageRowsAll;

        const totalImpressions = trendRows.reduce((a, r) => a + num(r.impressions), 0);
        const totalClicks = trendRows.reduce((a, r) => a + num(r.clicks), 0);
        const totalCtr = safePct(totalClicks, totalImpressions);
        const totalPosition = weightedAvgPosition(
            trendRows.map((r) => ({ impressions: num(r.impressions), position: num(r.position) })),
        );

        const byStateMap = new Map<
            string,
            { state: string; clicks: number; impressions: number; positionW: number; pages: number }
        >();

        for (const r of pageRows) {
            const state = s(r.__state || r.state || "__unknown") || "__unknown";
            if (!byStateMap.has(state)) {
                byStateMap.set(state, { state, clicks: 0, impressions: 0, positionW: 0, pages: 0 });
            }
            const o = byStateMap.get(state)!;
            const imp = num(r.impressions);
            const clk = num(r.clicks);
            const pos = num(r.position);
            o.impressions += imp;
            o.clicks += clk;
            o.positionW += pos * imp;
            o.pages += 1;
        }

        const byState = Array.from(byStateMap.values()).map((x) => {
            const ctr = safePct(x.clicks, x.impressions);
            const position = x.impressions ? x.positionW / x.impressions : 0;
            return { state: x.state, clicks: x.clicks, impressions: x.impressions, ctr, position, pages: x.pages };
        });

        byState.sort((a, b) => (b.impressions || 0) - (a.impressions || 0));

        const { prevStart, prevEnd } = prevPeriodRange(start || meta?.range?.startDate, end || meta?.range?.endDate);
        const prevTrendRows = prevStart && prevEnd ? sliceByDate(trendRowsAll, prevStart, prevEnd) : [];

        const prevImpressions = prevTrendRows.reduce((a, r) => a + num(r.impressions), 0);
        const prevClicks = prevTrendRows.reduce((a, r) => a + num(r.clicks), 0);
        const prevCtr = safePct(prevClicks, prevImpressions);
        const prevPosition = weightedAvgPosition(
            prevTrendRows.map((r) => ({ impressions: num(r.impressions), position: num(r.position) })),
        );

        const totals = {
            clicks: totalClicks,
            impressions: totalImpressions,
            ctr: totalCtr,
            position: totalPosition,
            pagesCounted: pageRows.length,
        };

        const prevTotals = prevTrendRows.length
            ? { clicks: prevClicks, impressions: prevImpressions, ctr: prevCtr, position: prevPosition }
            : null;

        const deltas = prevTotals
            ? {
                clicksPct: percentChange(totals.clicks, prevTotals.clicks),
                impressionsPct: percentChange(totals.impressions, prevTotals.impressions),
                ctrPct: percentChange(totals.ctr, prevTotals.ctr),
                positionPct: percentChange(totals.position, prevTotals.position),
            }
            : { clicksPct: null, impressionsPct: null, ctrPct: null, positionPct: null };

        return Response.json({
            ok: true,
            meta: {
                ...meta,
                range: {
                    startDate: start || meta?.range?.startDate || "",
                    endDate: end || meta?.range?.endDate || "",
                    prevStart,
                    prevEnd,
                },
            },
            totals,
            prevTotals,
            deltas,
            byState,
        });
    } catch (e: any) {
        return Response.json({ ok: false, error: e?.message || "aggregate failed" }, { status: 500 });
    }
}
