// src/lib/ghl/getLocationTokenReadOnly.ts
import { readTokensFile } from "./readTokensFile";

const API_BASE = "https://services.leadconnectorhq.com";

export type LocationTokenResponse = {
    access_token?: string;
    token_type?: string;
    expires_in?: number;
    scope?: string;
    userType?: string;
};

export async function getLocationTokenReadOnly(locationId: string) {
    const tokens = await readTokensFile();

    const agencyAccessToken = String(tokens.access_token || "").trim();
    const companyId = String(tokens.companyId || "").trim();

    if (!agencyAccessToken) {
        throw new Error("No access_token in tokens.json (agency token). Run Node OAuth/dev flow.");
    }
    if (!companyId) {
        throw new Error("No companyId in tokens.json. Run Node OAuth/dev flow.");
    }
    if (!locationId) {
        throw new Error("locationId is required");
    }

    const r = await fetch(`${API_BASE}/oauth/locationToken`, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${agencyAccessToken}`,
            Version: "2021-07-28",
            "Content-Type": "application/json",
            Accept: "application/json",
        },
        body: JSON.stringify({ companyId, locationId }),
    });

    const ct = r.headers.get("content-type") || "";
    const text = await r.text();

    if (!ct.includes("application/json")) {
        // esto es CLAVE para diagnosticar Cloudflare / HTML / redirects
        throw new Error(
            `locationToken non-JSON (status ${r.status}, content-type ${ct}). head=${text.slice(0, 180)}`
        );
    }

    let json: any = {};
    try {
        json = JSON.parse(text);
    } catch {
        json = { raw: text };
    }

    if (!r.ok) {
        throw new Error(`locationToken HTTP ${r.status}: ${JSON.stringify(json)}`);
    }

    return json as LocationTokenResponse;
}
