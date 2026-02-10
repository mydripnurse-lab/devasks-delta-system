// control-tower/src/app/api/auth/gsc/start/route.ts
import { NextResponse } from "next/server";

export const runtime = "nodejs";

function s(v: any) {
    return String(v ?? "").trim();
}

export async function GET() {
    const clientId = s(process.env.GSC_CLIENT_ID);
    const redirectUri = s(process.env.GSC_REDIRECT_URI);

    if (!clientId || !redirectUri) {
        return new Response("Missing env: GSC_CLIENT_ID / GSC_REDIRECT_URI", {
            status: 500,
        });
    }

    const scopes = [
        "https://www.googleapis.com/auth/webmasters.readonly",
        "https://www.googleapis.com/auth/analytics.readonly",
    ];

    const p = new URLSearchParams();
    p.set("client_id", clientId);
    p.set("redirect_uri", redirectUri);
    p.set("response_type", "code");

    // 🔥 Esto es lo que “garantiza” refresh_token nuevo
    p.set("access_type", "offline");
    p.set("prompt", "consent");

    p.set("scope", scopes.join(" "));
    p.set("include_granted_scopes", "true");

    const authUrl = `https://accounts.google.com/o/oauth2/v2/auth?${p.toString()}`;
    return NextResponse.redirect(authUrl);
}
