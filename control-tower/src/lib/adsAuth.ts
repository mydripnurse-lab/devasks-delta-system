import fs from "fs/promises";
import path from "path";
import { google } from "googleapis";

type OAuthClientJson = { installed?: any; web?: any } | Record<string, any>;

function absFromCwd(p: string) {
    return path.isAbsolute(p) ? p : path.join(process.cwd(), p);
}

async function readJson(filePath: string) {
    const raw = await fs.readFile(filePath, "utf8");
    try {
        return JSON.parse(raw);
    } catch {
        throw new Error(`Invalid JSON at: ${filePath}`);
    }
}

function pickClientCreds(oauthJson: OAuthClientJson) {
    const block = (oauthJson as any).installed || (oauthJson as any).web;
    if (!block) {
        throw new Error(
            `OAuth client JSON must include "installed" or "web" with client_id/client_secret.`,
        );
    }

    const client_id = String(block.client_id || "").trim();
    const client_secret = String(block.client_secret || "").trim();
    const redirect_uris = Array.isArray(block.redirect_uris) ? block.redirect_uris : [];
    const redirect_uri = String(redirect_uris[0] || "http://localhost").trim();

    if (!client_id || !client_secret) {
        throw new Error(`OAuth client JSON missing client_id/client_secret.`);
    }
    return { client_id, client_secret, redirect_uri };
}

export async function getAdsOAuthClient() {
    const tokensFile = process.env.ADS_TOKENS_FILE;
    const oauthClientFile =
        process.env.ADS_OAUTH_CLIENT_FILE || process.env.GSC_OAUTH_CLIENT_FILE;

    if (!tokensFile) throw new Error(`Missing env ADS_TOKENS_FILE`);
    if (!oauthClientFile)
        throw new Error(`Missing env ADS_OAUTH_CLIENT_FILE or GSC_OAUTH_CLIENT_FILE`);

    const tokensPath = absFromCwd(tokensFile);
    const oauthPath = absFromCwd(oauthClientFile);

    const tokenJson = await readJson(tokensPath);
    const oauthJson = await readJson(oauthPath);

    const tokens = tokenJson?.tokens || tokenJson;
    const refresh_token = String(tokens?.refresh_token || "").trim();

    if (!refresh_token) {
        throw new Error(
            `ads_tokens.json missing refresh_token. Re-run /api/auth/ads/start and ensure prompt=consent.`,
        );
    }

    const { client_id, client_secret, redirect_uri } = pickClientCreds(oauthJson);

    const oauth2 = new google.auth.OAuth2(client_id, client_secret, redirect_uri);
    oauth2.setCredentials({
        refresh_token,
        access_token: tokens?.access_token,
        expiry_date: tokens?.expiry_date,
        scope: tokens?.scope,
        token_type: tokens?.token_type,
    });

    try {
        await oauth2.getAccessToken();
    } catch (e: any) {
        throw new Error(
            `Failed to refresh Ads access token. Check OAuth client + refresh_token. Details: ${e?.message || e}`,
        );
    }

    return { oauth2, tokenMeta: tokenJson };
}
