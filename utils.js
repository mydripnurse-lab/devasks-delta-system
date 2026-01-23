import fs from "node:fs/promises";

export function nowISO() {
    return new Date().toISOString();
}

export async function ensureDir(dir) {
    await fs.mkdir(dir, { recursive: true });
}

export function titleCaseFromKey(key) {
    return String(key || "")
        .replace(/[-_]/g, " ")
        .split(" ")
        .filter(Boolean)
        .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
        .join(" ");
}

export function cleanUndefined(obj) {
    if (!obj || typeof obj !== "object") return obj;
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
        if (v === undefined) continue;
        if (v && typeof v === "object" && !Array.isArray(v)) {
            const nested = cleanUndefined(v);
            if (Object.keys(nested).length === 0) continue;
            out[k] = nested;
            continue;
        }
        out[k] = v;
    }
    return out;
}
