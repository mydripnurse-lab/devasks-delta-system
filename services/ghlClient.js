// services/ghlClient.js
import { getTokens, isExpiredSoon, saveTokens } from "./tokenStore.js";

const TOKEN_URL = "https://services.leadconnectorhq.com/oauth/token";
const API_BASE = "https://services.leadconnectorhq.com";

function mustEnv(name) {
    const v = process.env[name];
    if (!v) throw new Error(`Missing env var: ${name}`);
    return v;
}

async function refreshAccessToken() {
    const { refresh_token } = getTokens();
    if (!refresh_token) throw new Error("No refresh_token available. Run OAuth again.");

    const client_id = mustEnv("GHL_CLIENT_ID");
    const client_secret = mustEnv("GHL_CLIENT_SECRET");

    // IMPORTANT: token endpoint requiere x-www-form-urlencoded
    const body = new URLSearchParams({
        client_id,
        client_secret,
        grant_type: "refresh_token",
        refresh_token,
    });

    const r = await fetch(TOKEN_URL, {
        method: "POST",
        headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            Accept: "application/json",
        },
        body,
    });

    const data = await r.json();
    if (!r.ok) {
        throw new Error(`Refresh failed (${r.status}): ${JSON.stringify(data)}`);
    }

    const expires_in = Number(data.expires_in || 0); // seconds
    const expires_at = Date.now() + expires_in * 1000;

    await saveTokens({
        access_token: data.access_token,
        refresh_token: data.refresh_token || refresh_token,
        expires_at,
        scope: data.scope || "",
        userType: data.userType || "",
        companyId: data.companyId || "",
        locationId: data.locationId || "",
    });

    return data.access_token;
}

export async function getValidAccessToken() {
    const t = getTokens();

    if (!t.access_token) throw new Error("No access_token yet. Run /connect/ghl first.");
    if (isExpiredSoon()) {
        return await refreshAccessToken();
    }
    return t.access_token;
}

export async function ghlFetch(pathOrUrl, options = {}) {
    const accessToken = await getValidAccessToken();

    const url = pathOrUrl.startsWith("http")
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

    let json;
    try {
        json = JSON.parse(text);
    } catch {
        json = { raw: text };
    }

    if (!r.ok) {
        const err = new Error(`GHL API error (${r.status}) ${url}`);
        err.status = r.status;
        err.data = json;
        throw err;
    }

    return json;
}

/* ===========================
   ✅ Helpers específicos GHL
   =========================== */

// 1) Crear subaccount (POST /locations/)
export async function createSubAccount(locationBody) {
    return await ghlFetch("/locations/", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(locationBody),
    });
}

// 2) Sacar Location Token (POST /oauth/locationToken)
// OJO: esta ruta es JSON (no x-www-form-urlencoded)
export async function getLocationToken(locationId) {
    if (!locationId) throw new Error("getLocationToken requires locationId");

    return await ghlFetch("/oauth/locationToken", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ locationId }),
    });
}

// 3) Get Custom Values de una location
export async function getCustomValues(locationId, locationAccessToken) {
    if (!locationId) throw new Error("getCustomValues requires locationId");
    if (!locationAccessToken) throw new Error("getCustomValues requires locationAccessToken");

    return await ghlFetch(`/locations/${locationId}/customValues`, {
        method: "GET",
        headers: {
            Authorization: `Bearer ${locationAccessToken}`, // 👈 location token
        },
    });
}

// 4) Update un Custom Value (PUT) por ID (lo típico en GHL)
export async function updateCustomValue(locationId, customValueId, payload, locationAccessToken) {
    if (!locationId) throw new Error("updateCustomValue requires locationId");
    if (!customValueId) throw new Error("updateCustomValue requires customValueId");
    if (!locationAccessToken) throw new Error("updateCustomValue requires locationAccessToken");

    // payload esperado por ti: { name, value }
    return await ghlFetch(`/locations/${locationId}/customValues/${customValueId}`, {
        method: "PUT",
        headers: {
            Authorization: `Bearer ${locationAccessToken}`, // 👈 location token
            "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
    });
}
