// scripts/update-custom-values-one.js
import "dotenv/config";

import fs from "fs/promises";
import path from "path";

import { loadTokens, getTokens } from "../services/tokenStore.js";
import { ghlFetch } from "../services/ghlClient.js";
import { getLocationAccessToken } from "../services/ghlLocationToken.js";
import { loadSheetTabIndex } from "../services/sheetsClient.js";

// =====================
// CLI / ENV
// =====================
function s(v) {
    return String(v ?? "").trim();
}
function getArgValue(prefix) {
    const a = process.argv.find((x) => String(x).startsWith(prefix));
    if (!a) return "";
    const i = a.indexOf("=");
    return i >= 0 ? a.slice(i + 1).trim() : "";
}

const MODE_ARG = (getArgValue("--mode=") || s(process.env.MODE) || "dry").toLowerCase();
const DEBUG =
    getArgValue("--debug=") === "1" ||
    process.argv.includes("--debug") ||
    process.env.DEBUG === "1";

const isDryRun = MODE_ARG !== "live"; // ✅ ONLY live disables dry

const LOC_ID = s(getArgValue("--locId=") || process.env.LOC_ID);
const KIND = s(getArgValue("--kind=") || process.env.KIND); // "counties" | "cities" | ""

// Sheets
const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID;
const COUNTY_TAB = process.env.GOOGLE_SHEET_COUNTY_TAB || "Counties";
const CITY_TAB = process.env.GOOGLE_SHEET_CITY_TAB || "Cities";

// Paths
const CUSTOM_VALUES_SERVICES_FILE = path.join(
    process.cwd(),
    "resources",
    "customValues",
    "services",
    "mobile-iv-therapy.json"
);
const CUSTOM_VALUES_SOCIAL_FILE = path.join(
    process.cwd(),
    "resources",
    "customValues",
    "socialMedia",
    "meta.json"
);

// Rate limiting (GHL)
const GHL_RPM = Number(process.env.GHL_RPM || "80");
const MIN_MS_BETWEEN_GHL_CALLS = Math.ceil(60000 / Math.max(1, GHL_RPM));
const GHL_CV_MAX_RETRIES = Number(process.env.GHL_CV_MAX_RETRIES || "6");
const GHL_CV_INITIAL_DELAY_MS = Number(process.env.GHL_CV_INITIAL_DELAY_MS || "800");

// =====================
// Helpers
// =====================
function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}
let _lastGhlCallAt = 0;
async function ghlThrottle() {
    const now = Date.now();
    const wait = _lastGhlCallAt + MIN_MS_BETWEEN_GHL_CALLS - now;
    if (wait > 0) await sleep(wait);
    _lastGhlCallAt = Date.now();
}

function normalizeNameForMatch(x) {
    return String(x || "")
        .replace(/\u00A0/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .toLowerCase();
}

function ensureHttps(domainOrUrl) {
    const v = String(domainOrUrl || "").trim();
    if (!v) return "";
    if (v.startsWith("http://") || v.startsWith("https://")) return v;
    return `https://${v.replace(/^\/+/, "")}`;
}

function ensureSuffix(name, suffixLower) {
    const t = String(name || "").trim();
    if (!t) return "";
    if (t.toLowerCase().endsWith(suffixLower)) return t;
    return `${t} ${suffixLower[0].toUpperCase()}${suffixLower.slice(1)}`;
}

function getCountyLabelFrom(obj) {
    if (!obj) return "";
    if (obj?.countyName) return ensureSuffix(obj.countyName, "county");
    if (obj?.parishName) return ensureSuffix(obj.parishName, "parish");
    if (obj?.name) return String(obj.name).trim();
    return "";
}

async function readJson(filePath) {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw);
}

function toCustomValuesArray(json, labelForErrors) {
    const arr = json?.customValues;
    if (!Array.isArray(arr)) {
        const keys = Object.keys(json || {});
        throw new Error(
            `❌ ${labelForErrors} NO tiene estructura { customValues: [...] }.\n` +
            `Keys detectadas: ${keys.join(", ")}`
        );
    }
    return arr
        .filter((x) => x && String(x.name || "").trim() !== "")
        .map((x) => ({
            name: String(x.name).trim(),
            value: x.value === undefined || x.value === null ? "" : String(x.value),
        }));
}

function extractCustomValuesFromAny(res) {
    if (!res) return [];
    const candidates = [
        res?.customValues,
        res?.data?.customValues,
        res?.Data?.customValues,
        res?.data?.data?.customValues,
        res?.data?.Data?.customValues,
        res?.Data?.data?.customValues,
    ];
    for (const c of candidates) if (Array.isArray(c)) return c;
    return [];
}

function buildCustomValueIndex(ghlCustomValuesArr) {
    const byNorm = new Map();
    for (const cv of ghlCustomValuesArr) {
        const name = String(cv?.name || "").trim();
        const id = cv?.id;
        if (!name || !id) continue;
        const key = normalizeNameForMatch(name);
        if (!byNorm.has(key)) byNorm.set(key, { id, name, value: cv?.value ?? "" });
    }
    return byNorm;
}

function maskSecret(v) {
    const t = String(v || "");
    if (!t) return "";
    if (t.length <= 10) return "***";
    return `${t.slice(0, 6)}***${t.slice(-4)}`;
}

// =====================
// GHL API
// =====================
async function ghlGetCustomValues({ locationId, locationToken }) {
    await ghlThrottle();
    return ghlFetch(`/locations/${locationId}/customValues`, {
        method: "GET",
        headers: {
            Authorization: `Bearer ${locationToken}`,
            Version: "2021-07-28",
        },
    });
}

async function ghlUpdateCustomValue({
    locationId,
    locationToken,
    customValueId,
    value,
    customValueName,
}) {
    await ghlThrottle();
    const safeValue = value === undefined || value === null ? "" : String(value);

    return ghlFetch(`/locations/${locationId}/customValues/${customValueId}`, {
        method: "PUT",
        headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${locationToken}`,
            Version: "2021-07-28",
        },
        body: JSON.stringify({ name: customValueName, value: safeValue }),
    });
}

async function getCustomValuesWithRetry({
    locationId,
    locationToken,
    maxRetries = 6,
    initialDelayMs = 800,
}) {
    let attempt = 0;
    let delay = initialDelayMs;

    while (attempt < maxRetries) {
        attempt++;
        const res = await ghlGetCustomValues({ locationId, locationToken });
        const arr = extractCustomValuesFromAny(res);

        if (Array.isArray(arr) && arr.length > 0) {
            if (DEBUG) console.log(`✅ CV ready after attempt ${attempt} (count=${arr.length})`);
            return arr;
        }

        if (attempt < maxRetries) {
            if (DEBUG) console.log(`⏳ CV empty (attempt ${attempt}/${maxRetries}) → wait ${delay}ms`);
            await sleep(delay);
            delay = Math.min(Math.ceil(delay * 1.6), 5000);
        }
    }

    if (DEBUG) console.log(`⚠️ CV still empty after ${maxRetries} attempts (locationId=${locationId})`);
    return [];
}

// =====================
// Location token
// =====================
async function getLocToken(locationId) {
    const tokens = getTokens();
    const agencyAccessToken = tokens?.access_token;

    const companyId =
        tokens?.companyId ||
        process.env.GHL_COMPANY_ID ||
        process.env.COMPANY_ID ||
        process.env.COMPANYID;

    if (!agencyAccessToken) throw new Error("Missing agency access_token in tokenStore");
    if (!companyId) throw new Error("Missing companyId (tokens.companyId or env)");

    const locTok = await getLocationAccessToken({
        companyId,
        locationId,
        agencyAccessToken,
    });

    const locationToken = locTok?.access_token;
    if (!locationToken) throw new Error("Location token missing access_token");
    return locationToken;
}

// =====================
// Extras builder (same rules as from-sheet)
// =====================
function getEntityDomain(entity, parentCounty) {
    if (entity?.type === "county") return entity?.countyDomain || entity?.parishDomain || "";
    if (entity?.type === "city")
        return entity?.cityDomain || parentCounty?.countyDomain || parentCounty?.parishDomain || "";
    return "";
}

function buildExtraCustomValuesFromEntity({ entity, parentCounty, stateName }) {
    const isCity = entity?.type === "city";

    const countyName = getCountyLabelFrom(parentCounty) || getCountyLabelFrom(entity) || "";
    const countyDomain =
        parentCounty?.countyDomain ||
        parentCounty?.parishDomain ||
        entity?.countyDomain ||
        entity?.parishDomain ||
        "";

    const nameAndState = isCity
        ? `${entity?.cityName || ""} ${stateName || ""}`.trim()
        : `${getCountyLabelFrom(entity) || ""} ${stateName || ""}`.trim();

    const websiteDomain = getEntityDomain(entity, parentCounty);
    const websiteUrl = ensureHttps(websiteDomain);

    return [
        { name: "Business - County Domain", value: ensureHttps(countyDomain) },
        { name: "Business - County Name", value: String(countyName || "") },
        { name: "County Name And State", value: String(nameAndState || "") },
        { name: "Website Favicon", value: "https://sitemaps.mydripnurse.com/favicon.ico" },
        { name: "Website Url", value: websiteUrl },
        { name: "Pixel ID", value: String(process.env.FACEBOOK_PIXEL || "") },
        { name: "Access Token", value: String(process.env.FACEBOOK_ACCESS_TOKEN || "") },
        { name: "Business - Email", value: String(process.env.BUSINESS_EMAIL || "") },
        { name: "Business - Phone", value: String(process.env.DEFAULT_PHONE || "1 (833) 381-0071") },
    ];
}

// =====================
// Sheet lookup
// =====================
function cleanCell(v) {
    return String(v ?? "")
        .replace(/\u00A0/g, " ")
        .replace(/^["']|["']$/g, "")
        .trim();
}

function rowToObject(headers, row) {
    const out = {};
    for (let i = 0; i < headers.length; i++) out[headers[i]] = row?.[i];
    return out;
}

function findRowByLocId(tabIndex, locId) {
    const headers = tabIndex.headers || [];
    const rows = tabIndex.rows || [];
    const locIdx = tabIndex.headerMap?.get("Location Id");
    if (locIdx == null || locIdx < 0) return null;

    const target = cleanCell(locId);

    for (let r = 0; r < rows.length; r++) {
        const row = rows[r] || [];
        const cell = cleanCell(row?.[locIdx]);
        if (cell && cell === target) {
            return { rowIndex1: r + 2, row, obj: rowToObject(headers, row) };
        }
    }
    return null;
}

function buildEntityFromSheetRow({ foundKind, obj }) {
    const stateName = cleanCell(obj["State"]);
    const countyRaw = cleanCell(obj["County"]);
    const cityRaw = cleanCell(obj["City"]);

    const countyName = countyRaw ? ensureSuffix(countyRaw, "county") : "";

    const countyDomain = cleanCell(obj["County Domain"] || obj["Domain"] || "");
    const cityDomain = cleanCell(obj["City Domain"] || "");

    if (foundKind === "cities") {
        const parentCounty = { type: "county", countyName, countyDomain };
        const entity = { type: "city", cityName: cityRaw, cityDomain };
        return { stateName, entity, parentCounty };
    }

    const entity = { type: "county", countyName, countyDomain };
    return { stateName, entity, parentCounty: null };
}

// =====================
// Update
// =====================
async function updateLocationCustomValues({ locationId, label, extras, servicesArr, socialArr }) {
    console.log(`\n🧾 UPDATE CUSTOM VALUES (ONE) -> ${label}`);
    console.log(`📍 locationId=${locationId}`);
    console.log(`Mode: ${isDryRun ? "DRY" : "LIVE"} | Debug: ${DEBUG ? "ON" : "OFF"}`);

    const desired = [...servicesArr, ...socialArr, ...extras];

    if (DEBUG) {
        console.log(`🔧 desired custom values: ${desired.length}`);
        // ⚠️ mask tokens in logs
        for (const d of desired) {
            const name = String(d.name || "");
            const val = name.toLowerCase().includes("token") ? maskSecret(d.value) : String(d.value ?? "");
            console.log(` - ${name} = ${val}`);
        }
    }

    if (isDryRun) {
        console.log("🧪 DRY RUN (no API calls).");
        return { updated: 0, noMatch: 0, failed: 0, wouldUpdate: desired.length };
    }

    let locationToken;
    try {
        locationToken = await getLocToken(locationId);
    } catch (e) {
        console.log("❌ Location token failed -> skip:", e?.message || e);
        return { skipped: true, reason: "location_token_failed" };
    }

    const ghlCustomValuesArr = await getCustomValuesWithRetry({
        locationId,
        locationToken,
        maxRetries: GHL_CV_MAX_RETRIES,
        initialDelayMs: GHL_CV_INITIAL_DELAY_MS,
    });

    console.log(`📦 Custom Values extracted count=${ghlCustomValuesArr.length}`);
    const byNorm = buildCustomValueIndex(ghlCustomValuesArr);

    let updated = 0;
    let noMatch = 0;
    let failed = 0;

    for (const item of desired) {
        const wantName = String(item.name || "").trim();
        const wantKey = normalizeNameForMatch(wantName);
        const match = byNorm.get(wantKey);

        if (!match) {
            noMatch++;
            if (DEBUG) console.log(`🔎 NO MATCH: "${wantName}"`);
            continue;
        }

        try {
            await ghlUpdateCustomValue({
                locationId,
                locationToken,
                customValueId: match.id,
                value: item.value,
                customValueName: wantName,
            });
            updated++;
            if (DEBUG) console.log(`✅ updated: ${wantName}`);
        } catch (e) {
            failed++;
            console.log(`❌ Failed CV "${wantName}" ->`, e?.message || e);
        }
    }

    console.log(`✅ Done: updated=${updated} | no-match=${noMatch} | failed=${failed}`);
    return { updated, noMatch, failed };
}

// =====================
// MAIN
// =====================
async function main() {
    if (!SPREADSHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID in .env");
    if (!LOC_ID) throw new Error("Missing --locId=XXXXX");

    console.log(`Throttle: GHL_RPM=${GHL_RPM} => min ${MIN_MS_BETWEEN_GHL_CALLS}ms between calls`);

    await loadTokens();

    const servicesJson = await readJson(CUSTOM_VALUES_SERVICES_FILE);
    const socialJson = await readJson(CUSTOM_VALUES_SOCIAL_FILE);
    const servicesArr = toCustomValuesArray(servicesJson, "mobile-iv-therapy.json");
    const socialArr = toCustomValuesArray(socialJson, "meta.json");

    const countyTabIndex = await loadSheetTabIndex({
        spreadsheetId: SPREADSHEET_ID,
        sheetName: COUNTY_TAB,
        range: "A:ZZ",
        keyHeaders: ["State", "County"],
    });

    const cityTabIndex = await loadSheetTabIndex({
        spreadsheetId: SPREADSHEET_ID,
        sheetName: CITY_TAB,
        range: "A:ZZ",
        keyHeaders: ["State", "County", "City"],
    });

    let found = null;
    let foundKind = "";

    if (KIND === "counties") {
        found = findRowByLocId(countyTabIndex, LOC_ID);
        foundKind = "counties";
    } else if (KIND === "cities") {
        found = findRowByLocId(cityTabIndex, LOC_ID);
        foundKind = "cities";
    } else {
        found = findRowByLocId(countyTabIndex, LOC_ID);
        foundKind = found ? "counties" : "";
        if (!found) {
            found = findRowByLocId(cityTabIndex, LOC_ID);
            foundKind = found ? "cities" : "";
        }
    }

    if (!found) {
        throw new Error(`locId not found in sheet (Counties/Cities): ${LOC_ID}`);
    }

    const { stateName, entity, parentCounty } = buildEntityFromSheetRow({
        foundKind,
        obj: found.obj || {},
    });

    const label =
        foundKind === "cities"
            ? `${stateName} | ${cleanCell(found.obj?.County)} | ${cleanCell(found.obj?.City)}`
            : `${stateName} | ${cleanCell(found.obj?.County)}`;

    const extras = buildExtraCustomValuesFromEntity({ entity, parentCounty, stateName });

    const r = await updateLocationCustomValues({
        locationId: LOC_ID,
        label,
        extras,
        servicesArr,
        socialArr,
    });

    console.log("\n--------------------------------------------------");
    console.log("📌 SUMMARY");
    console.log(`kind:            ${foundKind}`);
    console.log(`locId:           ${LOC_ID}`);
    console.log(`updated CVs:     ${r.updated || 0}`);
    console.log(`no-match CVs:    ${r.noMatch || 0}`);
    console.log(`failed updates:  ${r.failed || 0}`);
}

main().catch((e) => {
    console.error("❌ Fatal:", e?.message || e);
    if (DEBUG) console.dir(e, { depth: 6 });
    process.exit(1);
});
