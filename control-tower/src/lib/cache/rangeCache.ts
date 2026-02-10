// src/lib/cache/rangeCache.ts
import fs from "fs/promises";
import path from "path";
import crypto from "crypto";

type CacheEntry<T> = { savedAt: number; data: T };

const mem = new Map<string, CacheEntry<any>>();

function ttlMs() {
    const s = Number(process.env.CONTACTS_CACHE_TTL_SEC || "300");
    return Math.max(5, s) * 1000;
}

function cacheDir() {
    return path.resolve(process.cwd(), process.env.CONTACTS_CACHE_DIR || "storage/cache");
}

function keyToFile(key: string) {
    const h = crypto.createHash("sha1").update(key).digest("hex");
    return path.join(cacheDir(), `${h}.json`);
}

export async function cacheGet<T>(key: string): Promise<T | null> {
    const now = Date.now();

    // 1) memory
    const m = mem.get(key);
    if (m && now - m.savedAt <= ttlMs()) return m.data as T;

    // 2) disk
    try {
        const fp = keyToFile(key);
        const raw = await fs.readFile(fp, "utf-8");
        const parsed = JSON.parse(raw) as CacheEntry<T>;
        if (!parsed?.savedAt) return null;

        if (now - parsed.savedAt > ttlMs()) return null;

        mem.set(key, parsed);
        return parsed.data;
    } catch {
        return null;
    }
}

export async function cacheSet<T>(key: string, data: T) {
    const entry: CacheEntry<T> = { savedAt: Date.now(), data };
    mem.set(key, entry);

    await fs.mkdir(cacheDir(), { recursive: true });
    await fs.writeFile(keyToFile(key), JSON.stringify(entry), "utf-8");
}
