// services/tokenStore.js
import fs from "fs/promises";
import path from "path";

const TOKENS_PATH = path.resolve(process.cwd(), "storage", "tokens.json");

let tokens = {
    access_token: "",
    refresh_token: "",
    expires_at: 0,
    scope: "",
    userType: "",
    companyId: "",
    locationId: "",
    oauth_state: "",
};

export function tokensPath() {
    return TOKENS_PATH;
}

export async function loadTokens() {
    try {
        const raw = await fs.readFile(TOKENS_PATH, "utf-8");
        const parsed = JSON.parse(raw || "{}");
        tokens = { ...tokens, ...parsed };
        return tokens;
    } catch {
        // si no existe todavía, lo creamos
        await fs.mkdir(path.dirname(TOKENS_PATH), { recursive: true });
        await fs.writeFile(TOKENS_PATH, JSON.stringify(tokens, null, 2), "utf-8");
        return tokens;
    }
}

export function getTokens() {
    return tokens;
}

export async function saveTokens(next) {
    tokens = { ...tokens, ...next };
    await fs.mkdir(path.dirname(TOKENS_PATH), { recursive: true });
    await fs.writeFile(TOKENS_PATH, JSON.stringify(tokens, null, 2), "utf-8");
    return tokens;
}

export function isExpiredSoon(bufferSec = 120) {
    if (!tokens.expires_at) return true;
    return Date.now() > Number(tokens.expires_at) - bufferSec * 1000;
}
