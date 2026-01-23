// routes/oauth.routes.js
import express from "express";
import crypto from "crypto";
import { loadTokens, saveTokens, getTokens, tokensPath } from "../services/tokenStore.js";

export const oauthRouter = express.Router();

const CHOOSE_LOCATION_URL = "https://marketplace.gohighlevel.com/oauth/chooselocation";
const TOKEN_URL = "https://services.leadconnectorhq.com/oauth/token";

function mustEnv(name) {
    const v = process.env[name];
    if (!v) throw new Error(`Missing env var: ${name}`);
    return v;
}

oauthRouter.get("/connect/ghl", async (_req, res) => {
    await loadTokens();

    const client_id = mustEnv("GHL_CLIENT_ID");
    const redirect_uri = mustEnv("GHL_REDIRECT_URI");
    const scope = mustEnv("GHL_SCOPES");
    const user_type = process.env.GHL_USER_TYPE || "Location";
    const state = crypto.randomBytes(16).toString("hex");

    // Guardamos state para validación simple
    await saveTokens({ oauth_state: state });

    const authUrl =
        `${CHOOSE_LOCATION_URL}` +
        `?response_type=code` +
        `&client_id=${encodeURIComponent(client_id)}` +
        `&redirect_uri=${encodeURIComponent(redirect_uri)}` +
        `&scope=${encodeURIComponent(scope)}` +
        `&state=${encodeURIComponent(state)}` +
        `&user_type=${encodeURIComponent(user_type)}`;

    res.redirect(authUrl);
});

oauthRouter.get("/oauth/callback", async (req, res) => {
    await loadTokens();

    const { code, state } = req.query;

    if (!code) return res.status(400).send("Missing ?code in callback");
    if (!state) return res.status(400).send("Missing ?state in callback");

    const t = getTokens();
    if (t.oauth_state && String(state) !== String(t.oauth_state)) {
        return res.status(400).send("Invalid state (CSRF). Try /connect/ghl again.");
    }

    const client_id = mustEnv("GHL_CLIENT_ID");
    const client_secret = mustEnv("GHL_CLIENT_SECRET");
    const redirect_uri = mustEnv("GHL_REDIRECT_URI");

    // IMPORTANT: token endpoint => x-www-form-urlencoded
    const body = new URLSearchParams({
        client_id,
        client_secret,
        grant_type: "authorization_code",
        code: String(code),
        redirect_uri,
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
    if (!r.ok) return res.status(r.status).json(data);

    const expires_in = Number(data.expires_in || 0);
    const expires_at = Date.now() + expires_in * 1000;

    await saveTokens({
        access_token: data.access_token,
        refresh_token: data.refresh_token,
        expires_at,
        scope: data.scope || "",
        userType: data.userType || "",
        companyId: data.companyId || "",
        locationId: data.locationId || "",
        oauth_state: "", // limpiamos
    });

    res.type("html").send(`
    <h2>✅ Tokens obtenidos</h2>
    <p><b>tokens.json</b>: ${tokensPath()}</p>
    <p><a href="/tokens">Ver tokens</a></p>
    <p><a href="/ghl/me">Probar /oauth/me</a></p>
    <p><a href="/">Back</a></p>
  `);
});

oauthRouter.get("/tokens", async (_req, res) => {
    await loadTokens();
    res.json(getTokens());
});
