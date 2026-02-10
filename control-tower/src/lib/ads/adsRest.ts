import { getAdsOAuth2 } from "./adsAuth";

function s(v: any) {
    return String(v ?? "").trim();
}
function cleanCid(v: string) {
    return s(v).replace(/-/g, "");
}

async function getAccessToken() {
    const oauth2 = await getAdsOAuth2();
    const tok = await oauth2.getAccessToken();
    const accessToken = s((tok as any)?.token || tok);
    if (!accessToken) throw new Error("Failed to obtain Google OAuth access_token");
    return accessToken;
}

function headersBase(loginCustomerId?: string) {
    const developerToken = s(process.env.GOOGLE_ADS_DEVELOPER_TOKEN);
    if (!developerToken) throw new Error("Missing env GOOGLE_ADS_DEVELOPER_TOKEN");

    const loginCid = cleanCid(loginCustomerId || s(process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID));

    return {
        "developer-token": developerToken,
        ...(loginCid ? { "login-customer-id": loginCid } : {}),
        "content-type": "application/json",
    };
}

function buildSearchStreamUrl(opts: { version?: string; customerId: string }) {
    const version = s(opts.version) || "v16"; // ✅ v16 test (puedes subir a v17 después)
    const customerId = cleanCid(opts.customerId);

    const base = `https://googleads.googleapis.com/${version}`;
    const path = `customers/${customerId}/googleAds:searchStream`;
    return `${base}/${path}`; // ✅ slash correcto
}

/**
 * We normalize to `{ results: [...] }` so your joins stay consistent.
 */
export async function googleAdsSearch(opts: {
    query: string;
    customerId?: string;
    loginCustomerId?: string;
    version?: string;
}) {
    const customerId = cleanCid(opts.customerId || s(process.env.GOOGLE_ADS_CUSTOMER_ID));
    if (!customerId) throw new Error("Missing GOOGLE_ADS_CUSTOMER_ID");

    const endpoint = buildSearchStreamUrl({ version: opts.version, customerId });

    // ✅ Safe logging (endpoint exists)
    console.log("[ADS] POST", endpoint);

    const accessToken = await getAccessToken();

    const res = await fetch(endpoint, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${accessToken}`,
            ...headersBase(opts.loginCustomerId),
        },
        body: JSON.stringify({ query: opts.query }),
    });

    const text = await res.text();
    let json: any;
    try {
        json = text ? JSON.parse(text) : null;
    } catch {
        json = { raw: text };
    }

    if (!res.ok) {
        throw new Error(`Google Ads HTTP ${res.status}: ${JSON.stringify(json).slice(0, 3000)}`);
    }

    // searchStream returns: [{ results: [...] }, { results: [...] }, ...]
    const chunks = Array.isArray(json) ? json : [];
    const results = chunks.flatMap((c) => (Array.isArray(c?.results) ? c.results : []));

    return { results };
}
