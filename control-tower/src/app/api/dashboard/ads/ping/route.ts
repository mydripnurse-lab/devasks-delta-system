// control-tower/src/app/api/dashboard/ads/ping/route.ts
import { NextResponse } from "next/server";
import fs from "fs/promises";
import path from "path";
import { google } from "googleapis";

export const runtime = "nodejs";

function s(v: any) {
    return String(v ?? "").trim();
}
function absFromCwd(p: string) {
    return path.isAbsolute(p) ? p : path.join(process.cwd(), p);
}

async function readJson(filePath: string) {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw);
}

async function getAccessTokenFromRefreshToken() {
    const clientId = s(process.env.ADS_CLIENT_ID);
    const clientSecret = s(process.env.ADS_CLIENT_SECRET);
    const redirectUri = s(process.env.ADS_REDIRECT_URI);
    const tokensFile = s(process.env.ADS_TOKENS_FILE);

    if (!clientId || !clientSecret || !redirectUri) {
        throw new Error("Missing env: ADS_CLIENT_ID / ADS_CLIENT_SECRET / ADS_REDIRECT_URI");
    }
    if (!tokensFile) throw new Error("Missing env: ADS_TOKENS_FILE");

    const tokensPath = absFromCwd(tokensFile);
    const tokenJson = await readJson(tokensPath);

    // soporta formatos:
    // { tokens: {...} } o {...}
    const tokens = tokenJson?.tokens || tokenJson;
    const refreshToken = s(tokens?.refresh_token);

    if (!refreshToken) {
        throw new Error(
            `ads_tokens.json missing refresh_token. Re-run /api/auth/ads/start and confirm refresh_token saved.`,
        );
    }

    const oauth2 = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
    oauth2.setCredentials({ refresh_token: refreshToken });

    const r = await oauth2.getAccessToken();
    const accessToken = s((r as any)?.token || r);

    if (!accessToken) throw new Error("Failed to obtain Google OAuth access token (empty).");

    return accessToken;
}

async function googleAdsSearch(customerId: string, query: string) {
    const accessToken = await getAccessTokenFromRefreshToken();

    const developerToken = s(process.env.GOOGLE_ADS_DEVELOPER_TOKEN);
    const loginCustomerId = s(process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID);

    if (!developerToken) throw new Error("Missing env: GOOGLE_ADS_DEVELOPER_TOKEN");
    if (!customerId) throw new Error("Missing customerId");

    // ✅ Docs indican base URL con versión (hoy v23) y método search
    // https://googleads.googleapis.com/v23/customers/CUSTOMER_ID/googleAds:search
    // :contentReference[oaicite:2]{index=2}
    const url = `https://googleads.googleapis.com/v23/customers/${customerId}/googleAds:search`;

    const headers: Record<string, string> = {
        Authorization: `Bearer ${accessToken}`,
        "developer-token": developerToken,
        "Content-Type": "application/json",
    };

    // opcional pero recomendado cuando usas MCC
    if (loginCustomerId) headers["login-customer-id"] = loginCustomerId;

    const res = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify({ query }),
        cache: "no-store",
    });

    const text = await res.text();

    if (!res.ok) {
        // devuelve raw para debug
        throw new Error(`Google Ads HTTP ${res.status}: ${text}`);
    }

    try {
        return JSON.parse(text);
    } catch {
        return { raw: text };
    }
}

export async function GET() {
    try {
        const customerId = s(process.env.GOOGLE_ADS_CUSTOMER_ID).replace(/-/g, "");
        if (!customerId) throw new Error("Missing env: GOOGLE_ADS_CUSTOMER_ID");

        // query mínima para confirmar que responde
        const query = `
      SELECT
        customer.id,
        customer.descriptive_name,
        customer.currency_code,
        customer.time_zone
      FROM customer
      LIMIT 1
    `.trim();

        const out = await googleAdsSearch(customerId, query);

        return NextResponse.json({
            ok: true,
            customerId,
            out,
        });
    } catch (e: any) {
        return NextResponse.json(
            { ok: false, error: e?.message || String(e) },
            { status: 500 },
        );
    }
}
