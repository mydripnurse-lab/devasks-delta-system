// control-tower/src/lib/gscCache.ts
import fs from "fs/promises";
import path from "path";
import { gscQuery, getGscSiteUrl } from "@/lib/gscClient";

type CacheMeta = {
    ok: boolean;
    siteUrl: string;
    startDate: string;
    endDate: string;
    generatedAt: string;
    pagesRows: number;
    queriesRows: number;
};

function n(v: any) {
    const x = Number(v);
    return Number.isFinite(x) ? x : 0;
}

function pct(x: any) {
    const v = Number(x);
    return Number.isFinite(v) ? v : 0;
}

function cacheDir(): string {
    // Default: ../control-tower-data-cache/gsc (desde control-tower/)
    return path.resolve(process.cwd(), process.env.GSC_CACHE_DIR || "../control-tower/data/cache/gsc");
}

async function ensureDir(p: string) {
    await fs.mkdir(p, { recursive: true });
}

function yyyyMmDd(d: Date) {
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${dd}`;
}

function computeRange(preset: string) {
    const now = new Date();
    const end = new Date(now);
    // GSC suele trabajar bien con end = "today-1" para evitar día incompleto,
    // pero tú puedes usar hoy. Yo lo dejo en hoy.
    const endDate = yyyyMmDd(end);

    const start = new Date(now);

    if (preset === "last_7_days") start.setDate(start.getDate() - 7);
    else if (preset === "last_28_days") start.setDate(start.getDate() - 28);
    else if (preset === "last_month") start.setMonth(start.getMonth() - 1);
    else if (preset === "last_quarter") start.setMonth(start.getMonth() - 3);
    else if (preset === "last_6_months") start.setMonth(start.getMonth() - 6);
    else if (preset === "last_year") start.setFullYear(start.getFullYear() - 1);
    else start.setDate(start.getDate() - 28);

    return { startDate: yyyyMmDd(start), endDate };
}

export async function refreshGscCache(params: {
    range: string;
    start?: string;
    end?: string;
}) {
    const dir = cacheDir();
    await ensureDir(dir);

    const siteUrl = getGscSiteUrl();

    const { startDate, endDate } =
        params.range === "custom" && params.start && params.end
            ? { startDate: params.start, endDate: params.end }
            : computeRange(params.range);

    // 1) Pages
    const pagesRaw = await gscQuery({
        siteUrl,
        startDate,
        endDate,
        dimensions: ["page"],
        rowLimit: 25000,
    });

    const pages = pagesRaw.map((r) => ({
        page: String(r.keys?.[0] || ""),
        clicks: n(r.clicks),
        impressions: n(r.impressions),
        ctr: pct(r.ctr),
        position: n(r.position),
    }));

    // 2) Queries
    const queriesRaw = await gscQuery({
        siteUrl,
        startDate,
        endDate,
        dimensions: ["query"],
        rowLimit: 25000,
    });

    const queries = queriesRaw.map((r) => ({
        query: String(r.keys?.[0] || ""),
        clicks: n(r.clicks),
        impressions: n(r.impressions),
        ctr: pct(r.ctr),
        position: n(r.position),
    }));

    // 3) Trend by date (para chart si lo activas luego)
    const trendRaw = await gscQuery({
        siteUrl,
        startDate,
        endDate,
        dimensions: ["date"],
        rowLimit: 5000,
    });

    const trend = trendRaw.map((r) => ({
        date: String(r.keys?.[0] || ""),
        clicks: n(r.clicks),
        impressions: n(r.impressions),
        ctr: pct(r.ctr),
        position: n(r.position),
    }));

    const meta: CacheMeta = {
        ok: true,
        siteUrl,
        startDate,
        endDate,
        generatedAt: new Date().toISOString(),
        pagesRows: pages.length,
        queriesRows: queries.length,
    };

    await fs.writeFile(path.join(dir, "pages.json"), JSON.stringify(pages, null, 2), "utf8");
    await fs.writeFile(path.join(dir, "queries.json"), JSON.stringify(queries, null, 2), "utf8");
    await fs.writeFile(path.join(dir, "trend.json"), JSON.stringify(trend, null, 2), "utf8");
    await fs.writeFile(path.join(dir, "meta.json"), JSON.stringify(meta, null, 2), "utf8");

    return { dir, meta };
}

export async function readGscCache() {
    const dir = cacheDir();

    const readJson = async (name: string) => {
        const p = path.join(dir, name);
        const raw = await fs.readFile(p, "utf8");
        return JSON.parse(raw);
    };

    const [pages, queries, trend, meta] = await Promise.all([
        readJson("pages.json").catch(() => []),
        readJson("queries.json").catch(() => []),
        readJson("trend.json").catch(() => []),
        readJson("meta.json").catch(() => null),
    ]);

    return { dir, pages, queries, trend, meta };
}

export async function isCacheFresh(maxAgeMs: number) {
    const { meta } = await readGscCache();
    const ts = meta?.generatedAt ? new Date(meta.generatedAt).getTime() : 0;
    if (!Number.isFinite(ts) || ts <= 0) return false;
    return Date.now() - ts <= maxAgeMs;
}
