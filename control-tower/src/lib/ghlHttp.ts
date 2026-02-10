// control-tower/src/lib/ghlHttp.ts
import { readTokens } from "./ghlTokens";

const API_BASE = "https://services.leadconnectorhq.com";
const VERSION = "2021-07-28";

type LocationTokenCache = {
    token: string;
    expiresAtMs: number;
};

let locationTokenCache: LocationTokenCache | null = null;

function mustEnv(name: string) {
    const v = process.env[name];
    if (!v) throw new Error(`Missing env var: ${name}`);
    return v;
}

function safeJsonParse(txt: string) {
    try {
        return JSON.parse(txt);
    } catch {
        return { raw: txt };
    }
}

export async function getAgencyAccessTokenOrThrow() {
    const t = await readTokens();
    const tok = String(t.access_token || "").trim();
    if (!tok) {
        throw new Error(
            "No access_token in root/storage/tokens.json. Run your Node dev OAuth flow (READ-ONLY dashboard).",
        );
    }
    return tok;
}

export async function getEffectiveLocationIdOrThrow() {
    const t = await readTokens();
    const fromFile = String(t.locationId || "").trim();
    const fromEnv = String(process.env.GHL_LOCATION_ID || "").trim();
    const id = fromEnv || fromFile;
    if (!id) throw new Error("Missing locationId (set GHL_LOCATION_ID or store it in tokens.json).");
    return id;
}

export async function getEffectiveCompanyIdOrThrow() {
    const t = await readTokens();
    const fromFile = String(t.companyId || "").trim();
    const fromEnv = String(process.env.GHL_COMPANY_ID || "").trim();
    const id = fromEnv || fromFile;
    if (!id) throw new Error("Missing companyId (set GHL_COMPANY_ID or store it in tokens.json).");
    return id;
}

export async function getLocationAccessTokenCached() {
    const now = Date.now();
    if (locationTokenCache && locationTokenCache.expiresAtMs - 30_000 > now) {
        return locationTokenCache.token;
    }

    const agencyToken = await getAgencyAccessTokenOrThrow();
    const locationId = await getEffectiveLocationIdOrThrow();
    const companyId = await getEffectiveCompanyIdOrThrow();

    // Location Token exchange (NO OAuth flow; just exchange)
    const url = `${API_BASE}/oauth/locationToken`;
    const r = await fetch(url, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${agencyToken}`,
            Version: VERSION,
            Accept: "application/json",
            "Content-Type": "application/json",
        },
        body: JSON.stringify({ companyId, locationId }),
    });

    const txt = await r.text();
    const data = safeJsonParse(txt);

    if (!r.ok) {
        throw new Error(`GHL locationToken error (${r.status}): ${JSON.stringify(data)}`);
    }

    const token = String(data?.access_token || "").trim();
    if (!token) throw new Error(`Location token missing in response: ${JSON.stringify(data)}`);

    const expiresInSec = Number(data?.expires_in || 0);
    const expiresAtMs = now + Math.max(60, expiresInSec) * 1000;

    locationTokenCache = { token, expiresAtMs };
    return token;
}

export async function ghlFetchJson(
    pathOrUrl: string,
    opts: {
        method?: string;
        headers?: Record<string, string>;
        body?: any;
        authToken?: string; // override bearer
    } = {},
) {
    const url =
        pathOrUrl.startsWith("http") ? pathOrUrl : `${API_BASE}${pathOrUrl.startsWith("/") ? "" : "/"}${pathOrUrl}`;

    const headers: Record<string, string> = {
        Version: VERSION,
        Accept: "application/json",
        ...(opts.headers || {}),
    };

    const token = opts.authToken || (await getLocationAccessTokenCached());
    headers.Authorization = `Bearer ${token}`;

    let body = opts.body;
    if (body && typeof body !== "string") {
        headers["Content-Type"] = headers["Content-Type"] || "application/json";
        body = JSON.stringify(body);
    }

    const r = await fetch(url, { method: opts.method || "GET", headers, body });
    const txt = await r.text();
    const ct = r.headers.get("content-type") || "";
    const data = ct.includes("application/json") ? safeJsonParse(txt) : { raw: txt, contentType: ct };

    if (!r.ok) {
        const e = new Error(`GHL API error (${r.status}) ${url}: ${JSON.stringify(data)}`);
        // @ts-ignore
        e.status = r.status;
        // @ts-ignore
        e.data = data;
        throw e;
    }

    return data;
}
