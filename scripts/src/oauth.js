// scripts/src/oauth.js
import axios from "axios";
import "dotenv/config";

const BASE = "https://services.leadconnectorhq.com";

function must(name) {
    const v = process.env[name];
    if (!v) throw new Error(`Missing env var: ${name}`);
    return v;
}

/**
 * 1) Exchange Authorization Code -> Agency Token (Company)
 * Docs show user_type=Company for agency-level token. :contentReference[oaicite:4]{index=4}
 */
export async function exchangeCodeForAgencyToken({ code }) {
    const client_id = must("GHL_CLIENT_ID");
    const client_secret = must("GHL_CLIENT_SECRET");

    const form = new URLSearchParams();
    form.set("client_id", client_id);
    form.set("client_secret", client_secret);
    form.set("grant_type", "authorization_code");
    form.set("code", code);
    form.set("user_type", "Company");

    const { data } = await axios.post(`${BASE}/oauth/token`, form, {
        headers: {
            Accept: "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout: 60_000,
    });

    return data; // {access_token, refresh_token, companyId, userType, ...}
}

/**
 * 2) Exchange Agency Token -> Location Token
 * Endpoint: /oauth/locationToken with Version header. :contentReference[oaicite:5]{index=5}
 */
export async function exchangeAgencyTokenForLocationToken({
    agencyAccessToken,
    companyId,
    locationId,
}) {
    const form = new URLSearchParams();
    form.set("companyId", companyId);
    form.set("locationId", locationId);

    const { data } = await axios.post(`${BASE}/oauth/locationToken`, form, {
        headers: {
            Accept: "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            Version: "2021-07-28",
            Authorization: `Bearer ${agencyAccessToken}`,
        },
        timeout: 60_000,
    });

    return data; // {access_token, refresh_token, userType:"Location", locationId,...}
}
