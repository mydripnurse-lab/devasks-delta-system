import { NextResponse } from "next/server";
import { readCache } from "@/lib/ads/adsCache";
import { joinAds } from "@/lib/ads/adsJoin";

export const runtime = "nodejs";

function s(v: any) {
    return String(v ?? "").trim();
}

function pickResults(raw: any) {
    return Array.isArray(raw?.results) ? raw.results : [];
}
function num(v: any) {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
}
function microsToMoney(m: any) {
    return num(m) / 1_000_000;
}

export async function GET(req: Request) {
    try {
        const url = new URL(req.url);
        const range = s(url.searchParams.get("range")) || "last_28_days";
        const key = `ads_${range}`;

        const cached = await readCache(key);
        if (!cached) {
            return NextResponse.json(
                { ok: false, error: `No cache for ${key}. Run /api/dashboard/ads/sync first.` },
                { status: 404 },
            );
        }

        const meta = cached.meta || null;

        const summary = joinAds(cached.kpis, meta);

        const trendRows = pickResults(cached.trend).map((r: any) => {
            const d = r?.segments?.date || "";
            const m = r?.metrics || {};
            return {
                date: d,
                impressions: num(m.impressions),
                clicks: num(m.clicks),
                ctr: num(m.ctr),
                avgCpc: microsToMoney(m.averageCpc),
                cost: microsToMoney(m.costMicros),
                conversions: num(m.conversions),
                conversionValue: num(m.conversionsValue),
            };
        });

        const campaignRows = pickResults(cached.campaigns).map((r: any) => {
            const c = r?.campaign || {};
            const m = r?.metrics || {};
            return {
                id: String(c.id || ""),
                name: String(c.name || ""),
                status: String(c.status || ""),
                channel: String(c.advertisingChannelType || ""),
                impressions: num(m.impressions),
                clicks: num(m.clicks),
                ctr: num(m.ctr),
                avgCpc: microsToMoney(m.averageCpc),
                cost: microsToMoney(m.costMicros),
                conversions: num(m.conversions),
                conversionValue: num(m.conversionsValue),
            };
        });

        return NextResponse.json({
            ok: true,
            meta,
            summary: summary.summary,
            trend: trendRows,
            campaigns: campaignRows,
            generatedAt: cached.generatedAt || meta?.generatedAt || null,
        });
    } catch (e: any) {
        return NextResponse.json(
            { ok: false, error: e?.message || String(e) },
            { status: 500 },
        );
    }
}
