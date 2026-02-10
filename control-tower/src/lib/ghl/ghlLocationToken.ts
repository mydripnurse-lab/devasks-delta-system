// src/lib/ghl/ghlLocationToken.ts
import { ghlFetch } from "./ghlClient";
import { readTokensFile } from "./tokenStore";

export async function getLocationAccessTokenReadOnly(locationId?: string) {
    const t = await readTokensFile();

    const companyId =
        (t.companyId && String(t.companyId).trim()) ||
        String(process.env.GHL_COMPANY_ID || "").trim();

    const locId =
        String(locationId || "").trim() ||
        (t.locationId && String(t.locationId).trim()) ||
        String(process.env.GHL_LOCATION_ID || "").trim();

    if (!companyId) throw new Error("Missing companyId (tokens.json.companyId or env GHL_COMPANY_ID)");
    if (!locId) throw new Error("Missing locationId (tokens.json.locationId or env GHL_LOCATION_ID)");

    // Agency token ya viene en ghlFetch()
    return await ghlFetch("https://services.leadconnectorhq.com/oauth/locationToken", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ companyId, locationId: locId }),
    });
}
