import express from "express";
import { loadTokens } from "../services/tokenStore.js";
import { ghlFetch } from "../services/ghlClient.js";
import { getLocationToken } from "../services/ghlLocationToken.js";
import { getLocationCustomValues, updateLocationCustomValue } from "../services/ghlCustomValues.js";

export const ghlRouter = express.Router();

ghlRouter.get("/ghl/me", async (_req, res) => {
    try {
        await loadTokens();
        const data = await ghlFetch("/oauth/me", { method: "GET" });
        console.log("✅ /oauth/me =>", data);
        res.json(data);
    } catch (e) {
        res.status(e.status || 500).json({ error: String(e.message || e), data: e.data || null });
    }
});

ghlRouter.get("/ghl/tokens", async (_req, res) => {
    await loadTokens();
    const t = getTokens();
    res.json({
        has_access_token: !!t.access_token,
        has_refresh_token: !!t.refresh_token,
        expires_at: t.expires_at,
        now: Date.now(),
        isExpiredSoon: t.expires_at ? Date.now() > (t.expires_at - 120_000) : true,
    });
});

// Crear subaccount/location
ghlRouter.post("/ghl/locations", async (req, res) => {
    try {
        await loadTokens();
        const data = await ghlFetch("/locations/", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(req.body),
        });
        console.log("✅ Created location:", data);
        res.json(data);
    } catch (e) {
        console.error("❌ Create location error:", e.data || e);
        res.status(e.status || 500).json({ error: String(e.message || e), data: e.data || null });
    }
});

// ✅ 1) Obtener Location Token (Sub-account token)
ghlRouter.post("/ghl/locationToken/:locationId", async (req, res) => {
    try {
        await loadTokens();
        const { locationId } = req.params;
        const data = await getLocationToken(locationId);
        console.log("✅ locationToken =>", { locationId, ...data.raw });
        res.json(data);
    } catch (e) {
        res.status(e.status || 500).json({ error: String(e.message || e), data: e.data || null });
    }
});

// ✅ 2) GET Custom Values (usa Location token)
ghlRouter.get("/ghl/:locationId/customValues", async (req, res) => {
    try {
        await loadTokens();
        const { locationId } = req.params;
        const { token } = await getLocationToken(locationId);

        const data = await getLocationCustomValues(locationId, token);
        console.log("✅ customValues =>", data);
        res.json(data);
    } catch (e) {
        res.status(e.status || 500).json({ error: String(e.message || e), data: e.data || null });
    }
});

// ✅ 3) PUT update 1 custom value (usa Location token)
ghlRouter.put("/ghl/:locationId/customValues/:customValueId", async (req, res) => {
    try {
        await loadTokens();
        const { locationId, customValueId } = req.params;
        const { value } = req.body;

        const { token } = await getLocationToken(locationId);
        const data = await updateLocationCustomValue(locationId, customValueId, value, token);

        console.log("✅ updated customValue =>", { locationId, customValueId, value });
        res.json(data);
    } catch (e) {
        res.status(e.status || 500).json({ error: String(e.message || e), data: e.data || null });
    }
});
