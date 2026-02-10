import { NextResponse } from "next/server";
import { googleAdsSearch } from "@/lib/ads/adsRest";
import { readCache, writeCache, cacheFresh } from "@/lib/ads/adsCache";
import { qKpis, qTrendDaily, qCampaigns } from "@/lib/ads/adsQueries";

export const runtime = "nodejs";

function s(v: any) {
    return String(v ?? "").trim();
}

function todayISO() {
    return new Date().toISOString().slice(0, 10);
}
function addDays(dateIso: string, days: number) {
    const d = new Date(dateIso + "T00:00:00Z");
    d.setUTCDate(d.getUTCDate() + days);
    return d.toISOString().slice(0, 10);
}

function resolveRange(range: string) {
    const end = todayISO();
    if (range === "last_7_days") return { start: addDays(end, -6), end };
    if (range === "last_28_days") return { start: addDays(end, -27), end };
    if (range === "last_month") return { start: addDays(end, -30), end };
    if (range === "last_quarter") return { start: addDays(end, -90), end };
    if (range === "last_6_months") return { start: addDays(end, -180), end };
    if (range === "last_year") return { start: addDays(end, -365), end };
    return { start: addDays(end, -27), end };
}

export async function GET(req: Request) {
    try {
        const url = new URL(req.url);
        const range = s(url.searchParams.get("range")) || "last_28_days";
        const force = s(url.searchParams.get("force")) === "1";

        const ttl = Number(process.env.ADS_CACHE_TTL_SECONDS || 600);

        const { start, end } = resolveRange(range);

        const cacheKey = `ads_${range}`;
        const cached = await readCache(cacheKey);

        if (!force && cached && cacheFresh(cached, ttl)) {
            return NextResponse.json({ ok: true, cached: true, key: cacheKey, meta: cached.meta });
        }

        const generatedAt = new Date().toISOString();
        const meta = { range, startDate: start, endDate: end, generatedAt };

        // KPIs (customer)
        const kpis = await googleAdsSearch({ query: qKpis(start, end), pageSize: 1000, version: "v17" });

        // Trend daily
        const trend = await googleAdsSearch({
            query: qTrendDaily(start, end),
            pageSize: 10000,
            version: "v17",
        });

        // Campaigns top
        const campaigns = await googleAdsSearch({
            query: qCampaigns(start, end),
            pageSize: 2000,
            version: "v17",
        });

        const envelope = { meta, kpis, trend, campaigns, generatedAt };

        const savedPath = await writeCache(cacheKey, envelope);

        return NextResponse.json({ ok: true, cached: false, key: cacheKey, savedPath, meta });
    } catch (e: any) {
        return NextResponse.json(
            { ok: false, error: e?.message || String(e) },
            { status: 500 },
        );
    }
}
