import fs from "fs/promises";
import path from "path";

export const runtime = "nodejs";

async function readJson(rel: string) {
    const p = path.join(process.cwd(), rel);
    const raw = await fs.readFile(p, "utf8");
    return JSON.parse(raw);
}

export async function GET() {
    try {
        const meta = await readJson("data/gsc/meta.json");
        const queries = await readJson("data/gsc/queries.json");
        const pages = await readJson("data/gsc/pages.json");
        const trend = await readJson("data/gsc/trend.json");

        return new Response(
            JSON.stringify({ ok: true, meta, queries, pages, trend }),
            { status: 200, headers: { "content-type": "application/json" } },
        );
    } catch (e: any) {
        return new Response(
            JSON.stringify({
                ok: false,
                error:
                    e?.message ||
                    "No cache found yet. Run POST /api/dashboard/gsc/sync first.",
            }),
            { status: 500, headers: { "content-type": "application/json" } },
        );
    }
}
