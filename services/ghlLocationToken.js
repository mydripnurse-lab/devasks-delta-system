// services/ghlLocationToken.js
import { getValidAccessToken } from "./ghlClient.js";

const LOCATION_TOKEN_URL = "https://services.leadconnectorhq.com/oauth/locationToken";

/**
 * Pide un Location Token (Sub-Account Token) usando el Agency/Company OAuth access_token.
 * @param {string} locationId
 * @returns {Promise<{ token: string, raw: any }>}
 */
export async function getLocationToken(locationId) {
    if (!locationId) throw new Error("getLocationToken: locationId is required");

    const accessToken = await getValidAccessToken();

    const r = await fetch(LOCATION_TOKEN_URL, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${accessToken}`,
            Version: "2021-07-28",
            Accept: "application/json",
            "Content-Type": "application/json",
        },
        body: JSON.stringify({ locationId }),
    });

    const raw = await r.json();

    if (!r.ok) {
        throw new Error(`GHL locationToken failed (${r.status}): ${JSON.stringify(raw)}`);
    }

    const token = raw?.access_token || raw?.accessToken || raw?.token;

    if (!token) {
        throw new Error(`GHL locationToken response missing token: ${JSON.stringify(raw)}`);
    }

    return { token, raw };
}
