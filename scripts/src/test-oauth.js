// scripts/src/test-oauth.js
import "dotenv/config";
import { exchangeCodeForAgencyToken, exchangeAgencyTokenForLocationToken } from "./oauth.js";

function must(name) {
    const v = process.env[name];
    if (!v) throw new Error(`Missing env var: ${name}`);
    return v;
}

async function main() {
    // Paso A: pega el code aquí por env var (rápido) o lo pasas por argumento
    const code = must("GHL_AUTH_CODE");

    console.log("1) Exchanging auth code -> Agency token...");
    const agency = await exchangeCodeForAgencyToken({ code });

    console.log("✅ Agency token OK:", {
        userType: agency.userType,
        companyId: agency.companyId,
        scope: agency.scope,
        expires_in: agency.expires_in,
    });

    // Paso B: para obtener location token necesitas locationId
    // En tu caso: lo tendrás después de crear el location (subaccount),
    // o por el webhook de INSTALL, o por tu flujo actual.
    const locationId = must("GHL_TEST_LOCATION_ID");
    const companyId = agency.companyId || must("GHL_COMPANY_ID");

    console.log("2) Exchanging Agency token -> Location token...");
    const loc = await exchangeAgencyTokenForLocationToken({
        agencyAccessToken: agency.access_token,
        companyId,
        locationId,
    });

    console.log("✅ Location token OK:", {
        userType: loc.userType,
        locationId: loc.locationId,
        expires_in: loc.expires_in,
        scope: loc.scope,
    });
}

main().catch((err) => {
    const msg = err?.response?.data || err.message;
    console.error("❌ OAuth test failed:", msg);
    process.exit(1);
});
