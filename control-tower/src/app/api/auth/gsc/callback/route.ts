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

    if (err) {
        return new Response(`OAuth error: ${err}`, { status: 400 });
    }
    if (!code) {
        return new Response("Missing ?code=", { status: 400 });
    }

    const clientId = s(process.env.GSC_CLIENT_ID);
    const clientSecret = s(process.env.GSC_CLIENT_SECRET);
    const redirectUri = s(process.env.GSC_REDIRECT_URI);

    if (!clientId || !clientSecret || !redirectUri) {
        return new Response(
            "Missing env: GSC_CLIENT_ID / GSC_CLIENT_SECRET / GSC_REDIRECT_URI",
            { status: 500 },
        );
    }

    const oauth2 = new google.auth.OAuth2(clientId, clientSecret, redirectUri);

    // Exchange code -> tokens
    const { tokens } = await oauth2.getToken(code);

    // ⚠️ refresh_token solo suele venir la 1ra vez (por eso prompt=consent)
    const out = {
        createdAt: new Date().toISOString(),
        siteUrl: s(process.env.GSC_SITE_URL),
        tokens: {
            access_token: tokens.access_token || "",
            refresh_token: tokens.refresh_token || "",
            scope: tokens.scope || "",
            token_type: tokens.token_type || "",
            expiry_date: tokens.expiry_date || null,
        },
    };

    // Guarda local (gitignored)
    const filePath = path.join(process.cwd(), "data", "secrets", "gsc_tokens.json");
    await writeJson(filePath, out);

    // Respuesta corta y clara
    const hasRefresh = !!out.tokens.refresh_token;

    return new Response(
        [
            "✅ GSC OAuth connected.",
            `Saved: ${filePath}`,
            `Has refresh_token: ${hasRefresh ? "YES" : "NO"}`,
            "",
            hasRefresh
                ? "Next: create /api/dashboard/gsc/sync to fetch & cache JSON."
                : "If refresh_token is NO: revoke access in Google Account permissions, then retry /api/auth/gsc/start.",
        ].join("\n"),
        { status: 200, headers: { "content-type": "text/plain; charset=utf-8" } },
    );
}
