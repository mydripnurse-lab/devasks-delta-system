import fs from "fs/promises";
import path from "path";
import { GoogleAdsApi } from "google-ads-api";

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

async function getRefreshTokenFromFile() {
    const tokensFile = s(process.env.ADS_TOKENS_FILE);
    if (!tokensFile) throw new Error("Missing env ADS_TOKENS_FILE");

    const tokensPath = absFromCwd(tokensFile);
    const tokenJson = await readJson(tokensPath);

    const tokens = tokenJson?.tokens || tokenJson;
    const refresh = s(tokens?.refresh_token);

    if (!refresh) {
        throw new Error(
            `ads_tokens.json missing refresh_token. Re-run /api/auth/ads/start with access_type=offline&prompt=consent.`,
        );
    }
    return refresh;
}

export async function getAdsCustomer() {
    const developerToken = s(process.env.GOOGLE_ADS_DEVELOPER_TOKEN);
    const loginCustomerId = s(process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID).replace(/-/g, "");
    const customerId = s(process.env.GOOGLE_ADS_CUSTOMER_ID).replace(/-/g, "");

    const clientId = s(process.env.ADS_CLIENT_ID);
    const clientSecret = s(process.env.ADS_CLIENT_SECRET);

    if (!developerToken) throw new Error("Missing env GOOGLE_ADS_DEVELOPER_TOKEN");
    if (!customerId) throw new Error("Missing env GOOGLE_ADS_CUSTOMER_ID");
    if (!clientId) throw new Error("Missing env ADS_CLIENT_ID");
    if (!clientSecret) throw new Error("Missing env ADS_CLIENT_SECRET");

    const refreshToken = await getRefreshTokenFromFile();

    const api = new GoogleAdsApi({
        client_id: clientId,
        client_secret: clientSecret,
        developer_token: developerToken,
    });

    if (!api) throw new Error("Failed to init GoogleAdsApi");

    // create customer
    const customer = api.Customer({
        customer_id: customerId,
        login_customer_id: loginCustomerId || undefined,
        refresh_token: refreshToken,
    });

    if (!customer) {
        throw new Error(
            `Failed to create Google Ads customer instance. Check customer_id/login_customer_id.`,
        );
    }

    return { api, customer, customerId, loginCustomerId };
}
