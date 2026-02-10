import { google } from "googleapis";
import fs from "fs/promises";
import path from "path";

function s(v: any) {
    return String(v ?? "").trim();
}

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

export async function getAdsOAuth2() {
    const clientId = s(process.env.ADS_CLIENT_ID);
    const clientSecret = s(process.env.ADS_CLIENT_SECRET);
    const redirectUri = s(process.env.ADS_REDIRECT_URI) || "http://localhost";

    if (!clientId || !clientSecret) {
        throw new Error("Missing env ADS_CLIENT_ID / ADS_CLIENT_SECRET");
    }

    const tokensFile = s(process.env.ADS_TOKENS_FILE);
    if (!tokensFile) throw new Error("Missing env ADS_TOKENS_FILE");

    const tokensPath = absFromCwd(tokensFile);
    const tokenJson = await readJson(tokensPath);
    const tokens = tokenJson?.tokens || tokenJson;

    const refresh_token = s(tokens?.refresh_token);
    if (!refresh_token) {
        throw new Error(
            `ads_tokens.json missing refresh_token. Re-run OAuth with access_type=offline & prompt=consent.`,
        );
    }

    const oauth2 = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
    oauth2.setCredentials({ refresh_token });

    // Fail-fast
    await oauth2.getAccessToken();

    return oauth2;
}
