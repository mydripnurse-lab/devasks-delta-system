// scripts/src/utils.js
import fs from "node:fs/promises";

export function nowISO() {
    return new Date().toISOString();
}

export async function ensureDir(dir) {
    await fs.mkdir(dir, { recursive: true });
}

export function titleCaseFromKey(stateKey) {
    return String(stateKey)
        .replace(/-/g, " ")
        .replace(/\b\w/g, (m) => m.toUpperCase());
}

export function cleanUndefined(obj) {
    return Object.fromEntries(
        Object.entries(obj).filter(([_, v]) => v !== undefined && v !== "")
    );
}
