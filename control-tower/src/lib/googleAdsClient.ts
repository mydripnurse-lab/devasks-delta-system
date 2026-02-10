import { OAuth2Client } from "google-auth-library";

function s(v: any) {
    return String(v ?? "").trim();
}

export type GoogleAdsRow = Record<string, any>;

export async function googleAdsSearch(opts: {
    oauth2: OAuth2Client;
    customerId: string;
    loginCustomerId?: string;
    developerToken: string;
    query: string;
}) {
    const customerId = s(opts.customerId).replaceAll("-", "");
    const loginCustomerId = s(opts.loginCustomerId || "").replaceAll("-", "");
    const developerToken = s(opts.developerToken);
    const query = s(opts.query);

    if (!customerId) throw new Error("Missing customerId");
    if (!developerToken) throw new Error("Missing developerToken");
    if (!query) throw new Error("Missing GAQL query");

    const accessToken = await opts.oauth2.getAccessToken();
    const token = s((accessToken as any)?.token || accessToken || "");
    if (!token) throw new Error("Failed to obtain access token for Google Ads");

    const url = `https://googleads.googleapis.com/v17/customers/${customerId}/googleAds:search`;

    const headers: Record<string, string> = {
        Authorization: `Bearer ${token}`,
        "developer-token": developerToken,
        "Content-Type": "application/json",
    };

    // login-customer-id requerido si entras vía MCC
    if (loginCustomerId) headers["login-customer-id"] = loginCustomerId;

    const res = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify({ query }),
    });

    const json: any = await res.json().catch(() => ({}));
    if (!res.ok) {
        const msg =
            json?.error?.message ||
            `Google Ads API error HTTP ${res.status}: ${JSON.stringify(json).slice(0, 4000)}`;
        throw new Error(msg);
    }

    const results = Array.isArray(json?.results) ? json.results : [];
    return results as GoogleAdsRow[];
}
