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

export async function GET(req: Request) {
    try {
        const { searchParams } = new URL(req.url);
        const type = s(searchParams.get("type")) || "queries";
        const q = s(searchParams.get("q")).toLowerCase();
        const limit = Math.max(1, Math.min(500, Number(searchParams.get("limit") || 100)));

        const file = type === "pages" ? "pages.json" : "queries.json";
        const json = await readJson(file);
        const rowsAll = Array.isArray(json?.rows) ? json.rows : [];

        let rows = rowsAll.map((r) => ({
            key: String(r.query || r.page || r.keys?.[0] || ""),
            clicks: num(r.clicks),
            impressions: num(r.impressions),
            ctr: Number.isFinite(Number(r.ctr))
                ? Number(r.ctr)
                : num(r.impressions)
                    ? num(r.clicks) / num(r.impressions)
                    : 0,
            position: num(r.position),
            __state: s(r.__state || r.state || ""),
        }));

        if (q) rows = rows.filter((r) => r.key.toLowerCase().includes(q));

        rows.sort((a, b) => (b.impressions || 0) - (a.impressions || 0));
        rows = rows.slice(0, limit);

        return Response.json({ ok: true, type, total: rows.length, rows });
    } catch (e: any) {
        return Response.json({ ok: false, error: e?.message || "top failed" }, { status: 500 });
    }
}
