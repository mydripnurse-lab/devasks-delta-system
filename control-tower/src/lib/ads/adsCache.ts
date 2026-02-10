import fs from "fs/promises";
import path from "path";

function s(v: any) {
    return String(v ?? "").trim();
}

export function adsCachePath(key: string) {
    const root = s(process.env.DASH_CACHE_DIR) || "data/cache";
    return path.join(process.cwd(), root, "ads", `${key}.json`);
}

export async function readCache(key: string) {
    const p = adsCachePath(key);
    try {
        const raw = await fs.readFile(p, "utf8");
        return JSON.parse(raw);
    } catch {
        return null;
    }
}

export async function writeCache(key: string, data: any) {
    const p = adsCachePath(key);
    await fs.mkdir(path.dirname(p), { recursive: true });
    await fs.writeFile(p, JSON.stringify(data, null, 2), "utf8");
    return p;
}

export function cacheFresh(envelope: any, ttlSeconds: number) {
    const t = Number(envelope?.generatedAt ? Date.parse(envelope.generatedAt) : 0);
    if (!t) return false;
    return Date.now() - t < ttlSeconds * 1000;
}
