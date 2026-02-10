// control-tower/src/lib/ghlTokens.ts
import fs from "fs/promises";
import path from "path";

export type StoredTokens = {
    access_token?: string;
    refresh_token?: string;
    expires_at?: number;
    scope?: string;
    userType?: string;
    companyId?: string;
    locationId?: string;
};

function tokensPathFromControlTower() {
    // process.cwd() en Next suele ser ".../control-tower"
    // storage está en el root del repo: "../storage/tokens.json"
    return path.resolve(process.cwd(), "..", "storage", "tokens.json");
}

export async function readTokens(): Promise<StoredTokens> {
    const p = tokensPathFromControlTower();
    const raw = await fs.readFile(p, "utf-8");
    const json = JSON.parse(raw || "{}");
    return json as StoredTokens;
}

export async function tokensDebugInfo() {
    const p = tokensPathFromControlTower();
    try {
        const st = await fs.stat(p);
        return { path: p, size: st.size, mtimeMs: st.mtimeMs };
    } catch {
        return { path: p, size: 0, mtimeMs: 0 };
    }
}
