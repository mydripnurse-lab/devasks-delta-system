// src/lib/gscStorage.ts
import fs from "fs/promises";
import path from "path";

function rootDir() {
    // Ajusta si tu estructura difiere.
    // Asumo que este archivo vive en control-tower y data vive en control-tower/data
    return process.cwd();
}

export function gscDataDir() {
    return path.join(rootDir(), "data", "gsc");
}

export async function ensureGscDir() {
    await fs.mkdir(gscDataDir(), { recursive: true });
}

export async function writeJson(fileName: string, data: any) {
    await ensureGscDir();
    const p = path.join(gscDataDir(), fileName);
    await fs.writeFile(p, JSON.stringify(data, null, 2), "utf8");
    return p;
}

export async function readJson<T = any>(fileName: string): Promise<T | null> {
    try {
        const p = path.join(gscDataDir(), fileName);
        const raw = await fs.readFile(p, "utf8");
        return JSON.parse(raw) as T;
    } catch {
        return null;
    }
}
