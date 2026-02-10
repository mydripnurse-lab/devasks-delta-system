// src/lib/ghl/ghlClient.ts
import { readTokensFile, tokensPath } from "./tokenStore";

const API_BASE = "https://services.leadconnectorhq.com";

export async function getReadOnlyAgencyAccessToken() {
    const t = await readTokensFile();
    if (!t.access_token) {
        throw new Error(
            `No access_token in ${tokensPath()}. This dashboard is READ-ONLY; run your Node OAuth/dev flow to generate tokens.json.`
        );
    }
    return t.access_token;
}

export async function ghlFetch(pathOrUrl: string, options: any = {}) {
    const accessToken = await getReadOnlyAgencyAccessToken();

    const url =
        pathOrUrl.startsWith("http")
            ? pathOrUrl
            : `${API_BASE}${pathOrUrl.startsWith("/") ? "" : "/"}${pathOrUrl}`;

    const headers = {
        Authorization: `Bearer ${accessToken}`,
        Version: "2021-07-28",
        Accept: "application/json",
        ...(options.headers || {}),
    };

    const r = await fetch(url, { ...options, headers });
    const text = await r.text();

    let json: any;
    try {
        json = JSON.parse(text);
    } catch {
        json = { raw: text };
    }

    if (!r.ok) {
        // 401 típico cuando el token expiró
        if (r.status === 401) {
            const err: any = new Error(
                `GHL 401 Unauthorized. Token in ${tokensPath()} is likely expired. This dashboard will NOT refresh tokens. Re-run your Node dev/OAuth flow to regenerate tokens.json.`
            );
            err.status = r.status;
            err.data = json;
            throw err;
        }

        const err: any = new Error(`GHL API error (${r.status}) ${url}`);
        err.status = r.status;
        err.data = json;
        throw err;
    }

    return json;
}
