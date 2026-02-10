// src/lib/ghl/readTokensFile.ts
import fs from "fs/promises";
import path from "path";

export type TokenState = {
    access_token?: string;
    refresh_token?: string;
    expires_at?: number;
    scope?: string;
    userType?: string;
    companyId?: string;
    locationId?: string;
};

const TOKENS_PATH = path.resolve(process.cwd(), "..", "storage", "tokens.json");

// micro-cache para no leer el file 50 veces por request
let _cache: { at: number; tokens: TokenState | null } = { at: 0, tokens: null };
const FILE_CACHE_MS = 2000;

export function tokensPath() {
    return TOKENS_PATH;
}

export async function readTokensFile(): Promise<TokenState> {
    const now = Date.now();
    if (_cache.tokens && now - _cache.at < FILE_CACHE_MS) return _cache.tokens;

    const raw = await fs.readFile(TOKENS_PATH, "utf-8").catch((e) => {
        const err = new Error(
            `Cannot read tokens.json at ${TOKENS_PATH}. ` +
            `This dashboard is READ-ONLY. Run your Node OAuth/dev flow to generate tokens.json.`
        );
        (err as any).cause = e;
        throw err;
    });

    let parsed: any = {};
    try {
        parsed = JSON.parse(raw || "{}");
    } catch {
        throw new Error(`tokens.json is not valid JSON: ${TOKENS_PATH}`);
    }

    const tokens: TokenState = {
        access_token: String(parsed.access_token || "").trim(),
        refresh_token: parsed.refresh_token,
        expires_at: parsed.expires_at,
        scope: parsed.scope,
        userType: parsed.userType,
        companyId: parsed.companyId,
        locationId: parsed.locationId,
    };

    _cache = { at: now, tokens };
    return tokens;
}
