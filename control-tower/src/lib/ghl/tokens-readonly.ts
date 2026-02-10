// control-tower/src/lib/ghl/tokens-readonly.ts
import fs from "fs/promises";
import path from "path";

export type AgencyTokens = {
    access_token?: string;
    refresh_token?: string;
    expires_at?: number;
    companyId?: string;
    locationId?: string;
};

function resolveTokensPath() {
    // control-tower/ -> .. -> storage/tokens.json
    return path.resolve(process.cwd(), "..", "storage", "tokens.json");
}

export async function readAgencyTokensReadOnly(): Promise<AgencyTokens> {
    const p = resolveTokensPath();
    const raw = await fs.readFile(p, "utf8");
    const json = JSON.parse(raw || "{}");
    return json || {};
}

export function tokensDebugSafe(t: AgencyTokens) {
    return {
        has_access_token: !!t?.access_token,
        has_refresh_token: !!t?.refresh_token,
        expires_at: t?.expires_at || 0,
        companyId: t?.companyId || "",
        locationId: t?.locationId || "",
    };
}
