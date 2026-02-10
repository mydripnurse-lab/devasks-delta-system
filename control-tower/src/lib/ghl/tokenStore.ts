// src/lib/ghl/tokenStore.ts
import fs from "fs/promises";
import path from "path";

export type TokenState = {
    access_token: string;
    refresh_token?: string;
    expires_at?: number;
    scope?: string;
    userType?: string;
    companyId?: string;
    locationId?: string;
    oauth_state?: string;
};

const TOKENS_PATH = path.resolve(
    process.cwd(),
    "..",              // ⬅️ SUBE FUERA DE control-tower
    "storage",
    "tokens.json"
);


let cached: { at: number; tokens: TokenState | null } = { at: 0, tokens: null };

// ✅ cache corto para no leer disco en cada request
const FILE_CACHE_MS = Number(process.env.GHL_TOKENS_FILE_CACHE_MS || "3000");

export function tokensPath() {
    return TOKENS_PATH;
}

export async function readTokensFile(): Promise<TokenState> {
    const now = Date.now();
    if (cached.tokens && now - cached.at < FILE_CACHE_MS) return cached.tokens;

    const raw = await fs.readFile(TOKENS_PATH, "utf-8").catch((e) => {
        const err = new Error(
            `tokens.json not found at ${TOKENS_PATH}. (Read-only mode).`
        ) as any;
        err.cause = e;
        throw err;
    });

    let parsed: any = {};
    try {
        parsed = JSON.parse(raw || "{}");
    } catch (e) {
        const err = new Error(
            `tokens.json is not valid JSON at ${TOKENS_PATH}.`
        ) as any;
        err.cause = e;
        throw err;
    }

    const t: TokenState = {
        access_token: String(parsed.access_token || "").trim(),
        refresh_token: parsed.refresh_token ? String(parsed.refresh_token) : undefined,
        expires_at:
            parsed.expires_at !== undefined && parsed.expires_at !== null
                ? Number(parsed.expires_at)
                : undefined,
        scope: parsed.scope ? String(parsed.scope) : undefined,
        userType: parsed.userType ? String(parsed.userType) : undefined,
        companyId: parsed.companyId ? String(parsed.companyId) : undefined,
        locationId: parsed.locationId ? String(parsed.locationId) : undefined,
        oauth_state: parsed.oauth_state ? String(parsed.oauth_state) : undefined,
    };

    cached = { at: now, tokens: t };
    return t;
}
