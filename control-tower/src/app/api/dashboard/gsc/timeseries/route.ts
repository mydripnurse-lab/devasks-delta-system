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
        const d = new Date(String(r.date || "")).getTime();
        if (!Number.isFinite(d)) return true;
        return d >= a && d <= b;
    });
}

export async function GET(req: Request) {
    try {
        const { searchParams } = new URL(req.url);
        const start = s(searchParams.get("start"));
        const end = s(searchParams.get("end"));

        const trend = await readJson("trend.json");
        const rowsAll = Array.isArray(trend?.rows) ? trend.rows : [];
        const rows = sliceByDate(rowsAll, start, end)
            .map((r) => ({
                date: String(r.date || ""),
                clicks: num(r.clicks),
                impressions: num(r.impressions),
                ctr: Number.isFinite(Number(r.ctr)) ? Number(r.ctr) : (num(r.impressions) ? num(r.clicks) / num(r.impressions) : 0),
                position: num(r.position),
            }))
            .sort((a, b) => a.date.localeCompare(b.date));

        return Response.json({ ok: true, rows });
    } catch (e: any) {
        return Response.json({ ok: false, error: e?.message || "timeseries failed" }, { status: 500 });
    }
}
