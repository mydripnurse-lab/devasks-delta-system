// scripts/update-custom-values-from-sheet.js
import "dotenv/config";

import fs from "fs/promises";
import path from "path";
import readline from "readline/promises";
import { stdin as input, stdout as output } from "process";

import { loadTokens, getTokens } from "../services/tokenStore.js";
import { ghlFetch } from "../services/ghlClient.js";
import { getLocationAccessToken } from "../services/ghlLocationToken.js";

import { loadSheetTabIndex, makeCompositeKey } from "../services/sheetsClient.js";

// =====================
// PATHS / CONFIG
// =====================
const OUT_ROOT = path.join(process.cwd(), "scripts", "out");

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

// Sheets
const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID;
const COUNTY_TAB = process.env.GOOGLE_SHEET_COUNTY_TAB || "Counties";
const CITY_TAB = process.env.GOOGLE_SHEET_CITY_TAB || "Cities";

// Rate limiting (GHL)
const GHL_RPM = Number(process.env.GHL_RPM || "80");
const MIN_MS_BETWEEN_GHL_CALLS = Math.ceil(60000 / Math.max(1, GHL_RPM));

// =====================
// FLAGS (UPDATED)
// =====================
function getArgValue(prefix) {
    const a = process.argv.find((x) => String(x).startsWith(prefix));
    if (!a) return "";
    const i = a.indexOf("=");
    return i >= 0 ? a.slice(i + 1).trim() : "";
}

const MODE_ARG = getArgValue("--mode=") || String(process.env.MODE || "").trim();
const STATE_ARG =
    getArgValue("--state=") ||
    String(process.env.STATE || process.env.DELTA_STATE || "").trim();

const DEBUG =
    getArgValue("--debug=") === "1" ||
    process.argv.includes("--debug") ||
    process.env.DEBUG === "1";

// ✅ supports both --dry-run and --mode=dry
const isDryRun =
    process.argv.includes("--dry-run") ||
    MODE_ARG.toLowerCase() === "dry" ||
    MODE_ARG.toLowerCase() === "dryrun" ||
    MODE_ARG.toLowerCase() === "dry-run";

// CV retry tuning
const GHL_CV_MAX_RETRIES = Number(process.env.GHL_CV_MAX_RETRIES || "6");
const GHL_CV_INITIAL_DELAY_MS = Number(process.env.GHL_CV_INITIAL_DELAY_MS || "800");

// =====================
// HELPERS
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

function normalizeNameForMatch(s) {
    return String(s || "")
        .replace(/\u00A0/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .toLowerCase();
}

function isStatusTrue(val) {
    const s = String(val ?? "").trim().toLowerCase();
    return s === "true" || s === "1" || s === "yes" || s === "y";
}

function nonEmpty(val) {
    return String(val ?? "").trim() !== "";
}

function ensureHttps(domainOrUrl) {
    const v = String(domainOrUrl || "").trim();
    if (!v) return "";
    if (v.startsWith("http://") || v.startsWith("https://")) return v;
    return `https://${v.replace(/^\/+/, "")}`;
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

// ---------- Consistencia County/Parish ----------
function ensureSuffix(name, suffixLower) {
    const s = String(name || "").trim();
    if (!s) return "";
    if (s.toLowerCase().endsWith(suffixLower)) return s;
    return `${s} ${suffixLower[0].toUpperCase()}${suffixLower.slice(1)}`;
}

function getCountyLabelFrom(obj) {
    if (!obj) return "";
    if (obj?.countyName) return ensureSuffix(obj.countyName, "county");
    if (obj?.parishName) return ensureSuffix(obj.parishName, "parish");
    if (obj?.name) return String(obj.name).trim();
    return "";
}

// =====================
// OUT folder state picker
// =====================
async function listOutStates() {
    const entries = await fs.readdir(OUT_ROOT, { withFileTypes: true }).catch(() => []);
    const dirs = entries.filter((e) => e.isDirectory()).map((e) => e.name);

    const states = [];
    for (const slug of dirs) {
        if (slug === "checkpoints") continue;
        const p = path.join(OUT_ROOT, slug, `${slug}.json`);
        try {
            await fs.access(p);
            states.push({ slug, jsonPath: p });
        } catch {
            // ignore
        }
    }
    states.sort((a, b) => a.slug.localeCompare(b.slug));
    return states;
}

async function promptStateChoice(states) {
    console.log("\nAvailable states (scripts/out/<state>/<state>.json):");
    states.forEach((s, i) => console.log(`  ${i + 1}) ${s.slug}`));

    const rl = readline.createInterface({ input, output });
    const answer = (await rl.question("\nType state number OR state slug (e.g. 1 or alabama): ")).trim();
    rl.close();

    if (!answer) return null;

    const asNum = Number(answer);
    if (!Number.isNaN(asNum) && asNum >= 1 && asNum <= states.length) {
        return { slug: states[asNum - 1].slug };
    }

    const exact = states.find((s) => s.slug === answer);
    if (exact) return { slug: exact.slug };

    return null;
}

// ✅ NEW: pick state from args (no prompt)
function pickStateFromArgs(states) {
    const slug = String(STATE_ARG || "").toLowerCase().trim();
    if (!slug) return null;

    if (slug === "all") return { mode: "all" };

    const found = states.find((s) => s.slug === slug);
    if (found) return { mode: "one", slug: found.slug };

    // allow passing the state slug as positional (last arg) for CLI convenience
    const last = String(process.argv[process.argv.length - 1] || "").trim().toLowerCase();
    if (last && last !== "0" && last !== "1" && last !== "true" && last !== "false") {
        const found2 = states.find((s) => s.slug === last);
        if (found2) return { mode: "one", slug: found2.slug };
        if (last === "all") return { mode: "all" };
    }

    return null;
}

// =====================
// Custom Values helpers
// =====================
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
    for (const c of candidates) {
        if (Array.isArray(c)) return c;
    }
    return [];
}

function buildCustomValueIndex(ghlCustomValuesArr) {
    const byNorm = new Map();
    for (const cv of ghlCustomValuesArr) {
        const name = String(cv?.name || "").trim();
        const id = cv?.id;
        if (!name || !id) continue;
        const key = normalizeNameForMatch(name);
        if (!byNorm.has(key)) {
            byNorm.set(key, {
                id,
                name,
                value: cv?.value ?? "",
            });
        }
    }
    return byNorm;
}

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

async function ghlUpdateCustomValue({ locationId, locationToken, customValueId, value, customValueName }) {
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

async function getCustomValuesWithRetry({ locationId, locationToken, maxRetries = 6, initialDelayMs = 800 }) {
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
    if (isDryRun) return "dry-location-token";

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
// buildExtraCustomValuesFromEntity (CON TODO LO QUE PEDISTE)
// =====================
function getEntityDomain(entity, parentCounty) {
    if (entity?.type === "county") return entity?.countyDomain || entity?.parishDomain || "";
    if (entity?.type === "city")
        return (
            entity?.cityDomain ||
            parentCounty?.countyDomain ||
            parentCounty?.parishDomain ||
            ""
        );
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
// Find rowInfo from Sheet (LIKE run-delta-system)
// =====================
function getRowInfoForEntity({ tabIndex, stateName, entity, parentCounty }) {
    const isCity = entity.type === "city";
    const keyHeaders = isCity ? ["State", "County", "City"] : ["State", "County"];

    const countyName = getCountyLabelFrom(parentCounty) || getCountyLabelFrom(entity) || "";
    const cityName = String(entity?.cityName || "").trim();

    const keyValuesMap = isCity
        ? { State: stateName, County: countyName, City: cityName }
        : { State: stateName, County: countyName };

    const sheetKey = makeCompositeKey(keyHeaders, keyValuesMap);
    const rowInfo = tabIndex.mapByKeyValue.get(sheetKey);

    return { rowInfo, sheetKey };
}

// =====================
// Update one location by locationId
// =====================
async function updateLocationCustomValues({ locationId, entityLabel, extras, servicesArr, socialArr }) {
    console.log(`\n🧾 UPDATE -> ${entityLabel}`);
    console.log(`📍 locationId=${locationId}`);

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

    const desired = [...servicesArr, ...socialArr, ...extras];

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
            if (!isDryRun) {
                await ghlUpdateCustomValue({
                    locationId,
                    locationToken,
                    customValueId: match.id,
                    value: item.value,
                    customValueName: wantName,
                });
            }
            updated++;
        } catch (e) {
            failed++;
            console.log(`❌ Failed CV "${wantName}" ->`, e?.message || e);
        }
    }

    console.log(`✅ Done: updated=${updated} | no-match=${noMatch} | failed=${failed}`);
    return { updated, noMatch, failed };
}

// =====================
// RUN ONE STATE (NEW)
// =====================
async function runOneState({ slug, jsonPath }) {
    const stateJson = await readJson(jsonPath);

    const stateSlug = stateJson.stateSlug || slug;
    const stateName = stateJson.stateName || stateJson.name || slug;

    console.log(`\n🏁 RUN STATE: ${stateSlug} (${stateName})`);
    console.log(`➡️ Rule: Status=TRUE + Location Id not empty -> update CVs (one-by-one)\n`);
    console.log(`Throttle: GHL_RPM=${GHL_RPM} => min ${MIN_MS_BETWEEN_GHL_CALLS}ms between calls`);
    console.log(`Mode: ${isDryRun ? "DRY" : "LIVE"} | Debug: ${DEBUG ? "ON" : "OFF"}\n`);

    // Load CV templates
    const servicesJson = await readJson(CUSTOM_VALUES_SERVICES_FILE);
    const socialJson = await readJson(CUSTOM_VALUES_SOCIAL_FILE);
    const servicesArr = toCustomValuesArray(servicesJson, "mobile-iv-therapy.json");
    const socialArr = toCustomValuesArray(socialJson, "meta.json");

    // Load sheet tab indexes
    const countyTabIndex = await loadSheetTabIndex({
        spreadsheetId: SPREADSHEET_ID,
        sheetName: COUNTY_TAB,
        range: "A:Z",
        keyHeaders: ["State", "County"],
    });

    const cityTabIndex = await loadSheetTabIndex({
        spreadsheetId: SPREADSHEET_ID,
        sheetName: CITY_TAB,
        range: "A:Z",
        keyHeaders: ["State", "County", "City"],
    });

    if (!countyTabIndex?.mapByKeyValue?.get || !cityTabIndex?.mapByKeyValue?.get) {
        throw new Error(
            "Sheet tabIndex mapByKeyValue not available. (loadSheetTabIndex must provide mapByKeyValue)"
        );
    }

    // The state JSON usually contains counties and their cities
    const counties = Array.isArray(stateJson?.counties) ? stateJson.counties : [];
    if (!counties.length) {
        console.log("⚠️ No counties found in state JSON -> nothing to do.");
        return {
            stateSlug,
            scanned: 0,
            foundRow: 0,
            eligible: 0,
            updatedTotal: 0,
            noMatchTotal: 0,
            failedTotal: 0,
            skippedTotal: 0,
        };
    }

    let scanned = 0;
    let foundRow = 0;
    let eligible = 0;
    let updatedTotal = 0;
    let noMatchTotal = 0;
    let failedTotal = 0;
    let skippedTotal = 0;

    // Iterate counties + cities from the JSON
    for (const c of counties) {
        // County entity wrapper (match run-delta-system style)
        const countyEntity = { ...c, type: "county" };
        const countyNameLabel = getCountyLabelFrom(countyEntity) || getCountyLabelFrom(c) || "";
        const countyLabel = `${stateName} | ${countyNameLabel}`;

        // ---- Counties tab row
        scanned++;
        {
            const { rowInfo, sheetKey } = getRowInfoForEntity({
                tabIndex: countyTabIndex,
                stateName,
                entity: countyEntity,
                parentCounty: null,
            });

            if (!rowInfo) {
                if (DEBUG) console.log(`⚠️ Counties row not found key="${sheetKey}" -> ${countyLabel}`);
            } else {
                foundRow++;
                const statusIdx = countyTabIndex.headerMap.get("Status");
                const locIdx = countyTabIndex.headerMap.get("Location Id");

                const statusVal = rowInfo.row?.[statusIdx];
                const locationId = rowInfo.row?.[locIdx];

                if (isStatusTrue(statusVal) && nonEmpty(locationId)) {
                    eligible++;

                    const extras = buildExtraCustomValuesFromEntity({
                        entity: countyEntity,
                        parentCounty: null,
                        stateName,
                    });

                    const r = await updateLocationCustomValues({
                        locationId: String(locationId).trim(),
                        entityLabel: countyLabel,
                        extras,
                        servicesArr,
                        socialArr,
                    });

                    if (r?.skipped) skippedTotal++;
                    else {
                        updatedTotal += r.updated || 0;
                        noMatchTotal += r.noMatch || 0;
                        failedTotal += r.failed || 0;
                    }
                } else {
                    skippedTotal++;
                    if (DEBUG) {
                        console.log(`⏭️ SKIP county (not eligible) -> ${countyLabel}`, {
                            Status: statusVal,
                            "Location Id": locationId,
                        });
                    }
                }
            }
        }

        // ---- Cities tab rows
        const cities = Array.isArray(c?.cities) ? c.cities : [];
        for (const city of cities) {
            scanned++;

            const cityEntity = {
                ...city,
                type: "city",
                cityName: city?.cityName || city?.name || city?.City || "",
            };

            const cityLabel = `${stateName} | ${countyNameLabel} | ${String(cityEntity.cityName || "").trim()}`;

            const { rowInfo, sheetKey } = getRowInfoForEntity({
                tabIndex: cityTabIndex,
                stateName,
                entity: cityEntity,
                parentCounty: countyEntity,
            });

            if (!rowInfo) {
                if (DEBUG) console.log(`⚠️ Cities row not found key="${sheetKey}" -> ${cityLabel}`);
                continue;
            }

            foundRow++;

            const statusIdx = cityTabIndex.headerMap.get("Status");
            const locIdx = cityTabIndex.headerMap.get("Location Id");

            const statusVal = rowInfo.row?.[statusIdx];
            const locationId = rowInfo.row?.[locIdx];

            if (isStatusTrue(statusVal) && nonEmpty(locationId)) {
                eligible++;

                const extras = buildExtraCustomValuesFromEntity({
                    entity: cityEntity,
                    parentCounty: countyEntity,
                    stateName,
                });

                const r = await updateLocationCustomValues({
                    locationId: String(locationId).trim(),
                    entityLabel: cityLabel,
                    extras,
                    servicesArr,
                    socialArr,
                });

                if (r?.skipped) skippedTotal++;
                else {
                    updatedTotal += r.updated || 0;
                    noMatchTotal += r.noMatch || 0;
                    failedTotal += r.failed || 0;
                }
            } else {
                skippedTotal++;
                if (DEBUG) {
                    console.log(`⏭️ SKIP city (not eligible) -> ${cityLabel}`, {
                        Status: statusVal,
                        "Location Id": locationId,
                    });
                }
            }
        }
    }

    console.log("\n--------------------------------------------------");
    console.log(`📌 SUMMARY (${stateSlug})`);
    console.log(`scanned entities: ${scanned}`);
    console.log(`sheet rows found: ${foundRow}`);
    console.log(`eligible rows:    ${eligible}`);
    console.log(`updated CVs:      ${updatedTotal}`);
    console.log(`no-match CVs:     ${noMatchTotal}`);
    console.log(`failed updates:   ${failedTotal}`);
    console.log(`skipped:          ${skippedTotal}`);

    return { stateSlug, scanned, foundRow, eligible, updatedTotal, noMatchTotal, failedTotal, skippedTotal };
}

// =====================
// MAIN (UPDATED: supports --state and --state=all without prompt)
// =====================
async function main() {
    if (!SPREADSHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID in .env");

    await loadTokens();

    const outStates = await listOutStates();
    if (!outStates.length) {
        throw new Error(`No states found in ${OUT_ROOT} (expected scripts/out/<slug>/<slug>.json)`);
    }

    // ✅ If we have --state=... -> skip prompt
    const argPick = pickStateFromArgs(outStates);

    if (argPick?.mode === "all") {
        console.log(`\n🏁 RUN ALL STATES (${outStates.length})`);
        console.log(`➡️ Rule: Status=TRUE + Location Id not empty -> update CVs (one-by-one)\n`);
        console.log(`Throttle: GHL_RPM=${GHL_RPM} => min ${MIN_MS_BETWEEN_GHL_CALLS}ms between calls`);
        console.log(`Mode: ${isDryRun ? "DRY" : "LIVE"} | Debug: ${DEBUG ? "ON" : "OFF"}\n`);

        // Optional: keep a grand summary
        let total = {
            scanned: 0,
            foundRow: 0,
            eligible: 0,
            updatedTotal: 0,
            noMatchTotal: 0,
            failedTotal: 0,
            skippedTotal: 0,
        };

        for (const st of outStates) {
            const r = await runOneState(st);
            total.scanned += r.scanned || 0;
            total.foundRow += r.foundRow || 0;
            total.eligible += r.eligible || 0;
            total.updatedTotal += r.updatedTotal || 0;
            total.noMatchTotal += r.noMatchTotal || 0;
            total.failedTotal += r.failedTotal || 0;
            total.skippedTotal += r.skippedTotal || 0;
        }

        console.log("\n==================================================");
        console.log("📌 GRAND SUMMARY (ALL STATES)");
        console.log(`scanned entities: ${total.scanned}`);
        console.log(`sheet rows found: ${total.foundRow}`);
        console.log(`eligible rows:    ${total.eligible}`);
        console.log(`updated CVs:      ${total.updatedTotal}`);
        console.log(`no-match CVs:     ${total.noMatchTotal}`);
        console.log(`failed updates:   ${total.failedTotal}`);
        console.log(`skipped:          ${total.skippedTotal}`);
        return;
    }

    if (argPick?.mode === "one") {
        const selected = outStates.find((s) => s.slug === argPick.slug);
        if (!selected) throw new Error(`State "${argPick.slug}" not found in scripts/out.`);
        await runOneState(selected);
        return;
    }

    // Fallback: interactive prompt (CLI usage)
    const choice = await promptStateChoice(outStates);
    if (!choice) throw new Error("Invalid state selection.");

    const selected = outStates.find((s) => s.slug === choice.slug);
    if (!selected) throw new Error("Selected state not found.");

    await runOneState(selected);
}

main().catch((e) => {
    console.error("❌ Fatal:", e?.message || e);
    if (DEBUG) console.dir(e, { depth: 6 });
    process.exit(1);
});
