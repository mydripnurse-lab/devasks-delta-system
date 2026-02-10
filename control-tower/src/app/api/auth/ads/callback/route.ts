import { google } from "googleapis";
import fs from "fs/promises";
import path from "path";

export const runtime = "nodejs";

function s(v: any) {
    return String(v ?? "").trim();
}

async function writeJson(filePath: string, data: any) {
    await fs.mkdir(path.dirname(filePath), { recursive: true });
    await fs.writeFile(filePath, JSON.stringify(data, null, 2), "utf8");
}

export async function GET(req: Request) {
    const url = new URL(req.url);
    const code = s(url.searchParams.get("code"));
    const err = s(url.searchParams.get("error"));

    if (err) return new Response(`OAuth error: ${err}`, { status: 400 });
    if (!code) return new Response("Missing ?code=", { status: 400 });

    // ✅ Reusamos el mismo OAuth client (Cloud Project)
    const clientId = s(process.env.GSC_CLIENT_ID);
    const clientSecret = s(process.env.GSC_CLIENT_SECRET);

    // ✅ Redirect propio para Ads
    const redirectUri = s(process.env.ADS_REDIRECT_URI);

    if (!clientId || !clientSecret || !redirectUri) {
        return new Response(
            "Missing env: GSC_CLIENT_ID / GSC_CLIENT_SECRET / ADS_REDIRECT_URI",
            { status: 500 },
        );
    }

    const oauth2 = new google.auth.OAuth2(clientId, clientSecret, redirectUri);

    // Exchange code -> tokens
    const { tokens } = await oauth2.getToken(code);

    const out = {
        createdAt: new Date().toISOString(),
        tokens: {
            access_token: tokens.access_token || "",
            refresh_token: tokens.refresh_token || "",
            scope: tokens.scope || "",
            token_type: tokens.token_type || "",
            expiry_date: tokens.expiry_date || null,
        },
    };

    const filePath = path.join(process.cwd(), "data", "secrets", "ads_tokens.json");
    await writeJson(filePath, out);

    const hasRefresh = !!out.tokens.refresh_token;

    return new Response(
        [
            "✅ Google Ads OAuth connected.",
            `Saved: ${filePath}`,
            `Has refresh_token: ${hasRefresh ? "YES" : "NO"}`,
            "",
            hasRefresh
                ? "Next: build /api/dashboard/ads/summary to fetch GAQL metrics."
                : "If refresh_token is NO: revoke access in Google Account permissions, then retry /api/auth/ads/start.",
        ].join("\n"),
        { status: 200, headers: { "content-type": "text/plain; charset=utf-8" } },
    );
}
