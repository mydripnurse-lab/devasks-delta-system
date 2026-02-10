// scripts/run-delta-system.js
import "dotenv/config";

import fs from "fs/promises";
import path from "path";
import readline from "readline/promises";
import { stdin as input, stdout as output } from "process";

import { loadTokens, getTokens } from "../services/tokenStore.js";
import { ghlFetch } from "../services/ghlClient.js";

import {
    findTwilioAccountByFriendlyName,
    closeTwilioAccount,
} from "../services/twilioClient.js";

import { getLocationAccessToken } from "../services/ghlLocationToken.js";

import {
    loadSheetTabIndex,
    updateRowByHeaders,
    makeCompositeKey,
} from "../services/sheetsClient.js";

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

// Run meta
const RUN_ID = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
const RUN_STARTED_AT = Date.now();

// =====================
// CLI FLAGS (UI-friendly)
// =====================
const isDryRun = process.argv.includes("--dry-run");

// ✅ allow: --debug OR --debug=1
const DEBUG =
    process.argv.includes("--debug") ||
    String(process.env.DEBUG || "") === "1" ||
    process.argv.some((a) => String(a).startsWith("--debug=") && String(a).split("=")[1] === "1");

// ✅ NEW: --state=all | --state=alabama | --state="Alabama,Florida"
function argValue(name, fallback = null) {
    const argv = process.argv.slice(2);
    const direct = `--${name}=`;

    for (let i = 0; i < argv.length; i++) {
        const a = String(argv[i] ?? "");
        if (a === `--${name}`) {
            const next = argv[i + 1];
            if (next && !String(next).startsWith("--")) return String(next);
            return fallback;
        }
        if (a.startsWith(direct)) return a.slice(direct.length);
    }
    return fallback;
}

const STATE_ARG = argValue("state", ""); // if empty -> interactive

// =====================
// PROGRESS (SSE-friendly)
// =====================
function emitProgressInit({ totals, message }) {
    const payload = {
        totals: {
            all: Number(totals?.all ?? 0),
            counties: Number(totals?.counties ?? 0),
            cities: Number(totals?.cities ?? 0),
        },
        done: { all: 0, counties: 0, cities: 0 },
        pct: 0,
        last: { kind: "state", state: "", action: "init" },
        message: message || "init",
    };
    console.log(`__PROGRESS_INIT__ ${JSON.stringify(payload)}`);
}

function emitProgress({ totals, done, last, message }) {
    const totalAll = Number(totals?.all ?? 0);
    const doneAll = Number(done?.all ?? 0);
    const pct = totalAll > 0 ? Math.max(0, Math.min(1, doneAll / totalAll)) : 0;

    const payload = {
        totals: {
            all: totalAll,
            counties: Number(totals?.counties ?? 0),
            cities: Number(totals?.cities ?? 0),
        },
        done: {
            all: doneAll,
            counties: Number(done?.counties ?? 0),
            cities: Number(done?.cities ?? 0),
        },
        pct,
        last: last || null,
        message: message || "",
    };

    console.log(`__PROGRESS__ ${JSON.stringify(payload)}`);
}

function emitProgressEnd({ totals, done, ok, error }) {
    const totalAll = Number(totals?.all ?? 0);
    const doneAll = Number(done?.all ?? 0);
    const pct = totalAll > 0 ? Math.max(0, Math.min(1, doneAll / totalAll)) : 1;

    const payload = {
        totals: {
            all: totalAll,
            counties: Number(totals?.counties ?? 0),
            cities: Number(totals?.cities ?? 0),
        },
        done: {
            all: doneAll,
            counties: Number(done?.counties ?? 0),
            cities: Number(done?.cities ?? 0),
        },
        pct,
        ok: !!ok,
        error: error || null,
        last: { kind: "state", state: "", action: "end" },
    };

    console.log(`__PROGRESS_END__ ${JSON.stringify(payload)}`);
}

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

function isPR(stateSlug, stateName) {
    const s = String(stateSlug || "").toLowerCase();
    const n = String(stateName || "").toLowerCase();
    return s === "puerto-rico" || n.includes("puerto rico");
}

function ensureHttps(domainOrUrl) {
    const v = String(domainOrUrl || "").trim();
    if (!v) return "";
    if (v.startsWith("http://") || v.startsWith("https://")) return v;
    return `https://${v.replace(/^\/+/, "")}`;
}

function toBoolishTRUE() {
    return "TRUE";
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
// Custom Values helpers
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

function buildExtraCustomValues({ entity, parentCounty, stateName }) {
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
                fieldKey: cv?.fieldKey,
                value: cv?.value ?? "",
            });
        }
    }
    return byNorm;
}

// =====================
// GHL API wrappers
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

// ✅ Retry wrapper para race-condition de GHL (customValues provision async)
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
            if (DEBUG) {
                console.log(`✅ Custom Values ready after attempt ${attempt} (count=${arr.length})`);
            }
            return arr;
        }

        if (attempt < maxRetries) {
            if (DEBUG) {
                console.log(`⏳ Custom Values empty (attempt ${attempt}/${maxRetries}) → waiting ${delay}ms`);
            }
            await sleep(delay);
            delay = Math.min(Math.ceil(delay * 1.6), 5000);
        }
    }

    if (DEBUG) {
        console.log(`⚠️ Custom Values still empty after ${maxRetries} attempts (locationId=${locationId})`);
    }
    return [];
}

// =====================
// CORE: process one entity (county/city)
// =====================
async function processOneAccount({
    entity,
    parentCounty,
    stateSlug,
    stateName,
    countyTabIndex,
    cityTabIndex,
}) {
    const isCity = entity.type === "city";
    const tabIndex = isCity ? cityTabIndex : countyTabIndex;

    // ✅ Composite keys
    const countyName = getCountyLabelFrom(parentCounty) || getCountyLabelFrom(entity) || "";
    const cityName = String(entity?.cityName || "").trim();

    const keyHeaders = isCity ? ["State", "County", "City"] : ["State", "County"];
    const keyValuesMap = isCity
        ? { State: stateName, County: countyName, City: cityName }
        : { State: stateName, County: countyName };

    const sheetKey = makeCompositeKey(keyHeaders, keyValuesMap);
    const rowInfo = tabIndex.mapByKeyValue.get(sheetKey);

    if (!rowInfo) {
        console.log(`⚠️ Sheet row not found for key="${sheetKey}" -> SKIP (no update)`);
        return { skipped: true, reason: "sheet_row_missing" };
    }

    const statusIdx = tabIndex.headerMap.get("Status");
    const statusVal = rowInfo.row?.[statusIdx];

    if (isStatusTrue(statusVal)) {
        console.log(`⏭️ SKIP Status TRUE -> key="${sheetKey}"`);
        return { skipped: true, reason: "status_true" };
    }

    const body = entity?.body;
    if (!body?.name) {
        console.log(`⚠️ Missing body.name -> SKIP key="${sheetKey}"`);
        return { skipped: true, reason: "missing_body" };
    }

    // ===== 1) CREATE LOCATION
    console.log(
        `🚀 Creating ${isCity ? "CITY" : "COUNTY"} -> ${body.name} | key="${sheetKey}"`
    );

    let created = null;
    if (isDryRun) {
        created = { id: `dry-${Date.now()}`, name: body.name };
        console.log("🟡 DRY RUN: skipping GHL create");
    } else {
        await ghlThrottle();
        created = await ghlFetch("/locations/", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
    }

    const locationId = created?.id;
    if (!locationId) {
        console.log("❌ No locationId returned -> STOP this account");
        return { skipped: true, reason: "no_location_id" };
    }

    // ===== 2) TWILIO: match & close (64 chars safe)
    try {
        if (isDryRun) {
            console.log("🟡 DRY RUN: skipping Twilio close");
        } else {
            const twilioLookupName = String(created?.name || "").slice(0, 64);

            const twilioAcc = await findTwilioAccountByFriendlyName(twilioLookupName, {
                exact: true,
                limit: 200,
            });

            if (!twilioAcc) {
                console.log("⚠️ Twilio: no match found (first 64 chars):", twilioLookupName);
            } else {
                console.log("✅ Twilio match:", {
                    sid: twilioAcc.sid,
                    friendlyName: twilioAcc.friendlyName,
                    status: twilioAcc.status,
                });

                const closed = await closeTwilioAccount(twilioAcc.sid);
                console.log("🧨 Twilio CLOSED:", {
                    sid: closed?.sid || twilioAcc.sid,
                    status: closed?.status,
                });
            }
        }
    } catch (e) {
        console.log("⚠️ Twilio step failed (continuing):", e?.message || e);
    }

    // ===== 3) GET LOCATION TOKEN
    let locationToken = null;
    try {
        if (isDryRun) {
            locationToken = "dry-location-token";
            console.log("🟡 DRY RUN: skipping location token");
        } else {
            const tokens = getTokens();
            const agencyAccessToken = tokens?.access_token;

            const companyId =
                tokens?.companyId ||
                process.env.GHL_COMPANY_ID ||
                process.env.COMPANY_ID ||
                process.env.COMPANYID;

            if (!agencyAccessToken) throw new Error("Missing agency access_token in tokenStore");
            if (!companyId) throw new Error("Missing companyId (tokens.companyId or env)");

            console.log("🔐 Getting location access token...");
            const locTok = await getLocationAccessToken({
                companyId,
                locationId,
                agencyAccessToken,
            });

            locationToken = locTok?.access_token;
            if (!locationToken) {
                if (DEBUG) console.log("DEBUG locationToken response:", locTok);
                throw new Error("Location token missing access_token");
            }
            console.log("✅ Location token OK");
        }
    } catch (e) {
        console.log("❌ Location token failed -> cannot do custom values:", e?.message || e);
    }

    // ===== 4) CUSTOM VALUES
    if (locationToken) {
        try {
            console.log("📥 #5 GET Custom Values from GHL (with retry)...");
            const ghlCustomValuesArr = await getCustomValuesWithRetry({
                locationId,
                locationToken,
                maxRetries: Number(process.env.GHL_CV_MAX_RETRIES || "6"),
                initialDelayMs: Number(process.env.GHL_CV_INITIAL_DELAY_MS || "800"),
            });

            console.log(`📦 Custom Values extracted count=${ghlCustomValuesArr.length}`);

            const byNorm = buildCustomValueIndex(ghlCustomValuesArr);

            const servicesJson = await readJson(CUSTOM_VALUES_SERVICES_FILE);
            const socialJson = await readJson(CUSTOM_VALUES_SOCIAL_FILE);

            const desired = [
                ...toCustomValuesArray(servicesJson, "mobile-iv-therapy.json"),
                ...toCustomValuesArray(socialJson, "meta.json"),
                ...buildExtraCustomValues({ entity, parentCounty, stateName }),
            ];

            console.log(`✍️ #6 Updating Custom Values: desired=${desired.length}`);

            let updated = 0;
            let skippedNoMatch = 0;
            let failed = 0;

            for (const item of desired) {
                const wantName = String(item.name || "").trim();
                const wantKey = normalizeNameForMatch(wantName);

                const match = byNorm.get(wantKey);

                if (!match) {
                    skippedNoMatch++;
                    if (DEBUG) console.log(`🔎 NO MATCH: "${wantName}" (norm="${wantKey}")`);
                    continue;
                }

                if (DEBUG) console.log(`🔗 MATCH "${wantName}" -> "${match.name}" (id=${match.id})`);

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
                    console.error(
                        `❌ Failed custom value "${wantName}" (id=${match.id}) ->`,
                        e?.message || e
                    );
                }
            }

            console.log(`✅ Custom Values updated=${updated} | no-match=${skippedNoMatch} | failed=${failed}`);
        } catch (e) {
            console.log("⚠️ Custom values step failed (continuing):", e?.message || e);
            if (DEBUG) console.dir(e, { depth: 6 });
        }
    }

    // ===== 5) UPDATE GOOGLE SHEET
    try {
        const updates = {
            "Account Name": String(created?.name || body?.name || ""),
            "Location Id": String(locationId || ""),
            Status: toBoolishTRUE(),
        };

        if (!isDryRun) {
            await updateRowByHeaders({
                spreadsheetId: SPREADSHEET_ID,
                sheetName: tabIndex.sheetName,
                headers: tabIndex.headers,
                rowNumber: rowInfo.rowNumber,
                updatesByHeader: updates,
            });
        }

        // update in-memory row
        const accIdx = tabIndex.headerMap.get("Account Name");
        const locIdx = tabIndex.headerMap.get("Location Id");
        const stIdx = tabIndex.headerMap.get("Status");

        if (rowInfo.row && accIdx !== undefined) rowInfo.row[accIdx] = updates["Account Name"];
        if (rowInfo.row && locIdx !== undefined) rowInfo.row[locIdx] = updates["Location Id"];
        if (rowInfo.row && stIdx !== undefined) rowInfo.row[stIdx] = updates.Status;

        console.log(
            `🧾 Sheet updated (${tabIndex.sheetName}) row=${rowInfo.rowNumber}: Account Name + Location Id + Status TRUE`
        );
    } catch (e) {
        console.log("⚠️ Sheet update failed:", e?.message || e);
    }

    return { created: true, locationId };
}

// =====================
// STATE scope counting for progress totals
// =====================
function countEntitiesInStateJson(stateJson) {
    const stateSlug = stateJson?.stateSlug || "";
    const stateName = stateJson?.stateName || stateJson?.name || "";
    const pr = isPR(stateSlug, stateName);

    const counties = Array.isArray(stateJson?.counties) ? stateJson.counties : [];
    let countiesTotal = 0;
    let citiesTotal = 0;

    for (const county of counties) {
        const cities = Array.isArray(county?.cities) ? county.cities : [];
        citiesTotal += cities.length;
        if (!pr) countiesTotal += 1;
    }

    return { countiesTotal, citiesTotal, allTotal: countiesTotal + citiesTotal };
}

// =====================
// RUN STATE
// =====================
async function runState({
    slug,
    jsonPath,
    countyTabIndex,
    cityTabIndex,
    progressTotals,
    progressDone,
}) {
    const stateJson = await readJson(jsonPath);

    const stateSlug = stateJson.stateSlug || slug;
    const stateName = stateJson.stateName || stateJson.name || slug;

    const counties = Array.isArray(stateJson.counties) ? stateJson.counties : [];
    const pr = isPR(stateSlug, stateName);

    console.log(`\n🏁 RUN STATE: ${stateSlug} | counties=${counties.length} | RUN_ID=${RUN_ID}`);
    console.log(`Throttle: GHL_RPM=${GHL_RPM} => min ${MIN_MS_BETWEEN_GHL_CALLS}ms between calls`);
    console.log(`Mode: ${isDryRun ? "DRY" : "LIVE"} | Debug: ${DEBUG ? "ON" : "OFF"}\n`);

    let countyCreated = 0;
    let cityCreated = 0;
    let skipped = 0;

    for (let i = 0; i < counties.length; i++) {
        const county = counties[i];
        const countyName = getCountyLabelFrom(county) || "Unknown County";
        const countyLabel = `[${i + 1}/${counties.length}] ${countyName}`;

        // PR: no counties
        if (!pr) {
            console.log(`\n🧩 COUNTY ${countyLabel}`);

            // progress: we are about to process a county item
            emitProgress({
                totals: progressTotals,
                done: progressDone,
                last: { kind: "county", state: stateSlug, county: countyName, action: "start" },
                message: `🧩 ${countyName} • start`,
            });

            if (county?.body?.name) {
                const r = await processOneAccount({
                    entity: { ...county, countyName, type: "county" },
                    parentCounty: null,
                    stateSlug,
                    stateName,
                    countyTabIndex,
                    cityTabIndex,
                });
                if (r?.created) countyCreated++;
                else skipped++;
            } else {
                console.log(`⚠️ COUNTY missing body -> SKIP create county: ${countyLabel}`);
                skipped++;
            }

            // mark county done
            progressDone.counties += 1;
            progressDone.all += 1;

            emitProgress({
                totals: progressTotals,
                done: progressDone,
                last: { kind: "county", state: stateSlug, county: countyName, action: "done" },
                message: `🧩 ${countyName} • done`,
            });
        }

        const cities = Array.isArray(county?.cities) ? county.cities : [];
        if (!cities.length) continue;

        console.log(`\n🏙️  Cities for ${countyLabel}: ${cities.length}`);

        for (let c = 0; c < cities.length; c++) {
            const city = cities[c];
            const cityName = city?.cityName || city?.name || "Unknown City";

            emitProgress({
                totals: progressTotals,
                done: progressDone,
                last: { kind: "city", state: stateSlug, county: countyName, city: cityName, action: "start" },
                message: `🏙️ ${cityName} • start`,
            });

            if (!city?.body?.name) {
                console.log(`⚠️ CITY missing body -> SKIP: ${cityName}`);
                skipped++;

                // mark city done (even if skipped)
                progressDone.cities += 1;
                progressDone.all += 1;

                emitProgress({
                    totals: progressTotals,
                    done: progressDone,
                    last: { kind: "city", state: stateSlug, county: countyName, city: cityName, action: "skip(missing body)" },
                    message: `🏙️ ${cityName} • skip`,
                });
                continue;
            }

            const r = await processOneAccount({
                entity: { ...city, cityName, type: "city" },
                parentCounty: { ...county, countyName },
                stateSlug,
                stateName,
                countyTabIndex,
                cityTabIndex,
            });

            if (r?.created) cityCreated++;
            else skipped++;

            progressDone.cities += 1;
            progressDone.all += 1;

            emitProgress({
                totals: progressTotals,
                done: progressDone,
                last: { kind: "city", state: stateSlug, county: countyName, city: cityName, action: r?.created ? "created" : "done" },
                message: `🏙️ ${cityName} • ${r?.created ? "created" : "done"}`,
            });
        }
    }

    console.log(
        `\n✅ STATE DONE ${stateSlug} | countyCreated=${countyCreated} | cityCreated=${cityCreated} | skipped=${skipped}\n`
    );
    return { countyCreated, cityCreated, skipped };
}

// =====================
// Target selection (UI vs interactive)
// =====================
function parseStateArgIntoSlugs(v) {
    const s = String(v || "").trim();
    if (!s) return null;

    const low = s.toLowerCase();
    if (low === "all" || low === "*") return { mode: "all", slugs: [] };

    // allow: "Alabama,Florida" or "alabama,florida"
    const parts = s
        .split(",")
        .map((x) => String(x || "").trim())
        .filter(Boolean)
        .map((x) =>
            x
                .normalize("NFD")
                .replace(/[\u0300-\u036f]/g, "")
                .toLowerCase()
                .replace(/\s+/g, "-")
        );

    if (!parts.length) return null;
    return { mode: "list", slugs: parts };
}

async function promptStateChoice(states) {
    console.log("\nAvailable states (scripts/out/<state>/<state>.json):");
    states.forEach((s, i) => console.log(`  ${i + 1}) ${s.slug}`));
    console.log(`  all) Run ALL states`);

    const rl = readline.createInterface({ input, output });
    const answer = (
        await rl.question("\nType state number OR state slug (e.g. 1 or florida or all): ")
    ).trim();
    rl.close();

    if (!answer) return null;
    if (answer.toLowerCase() === "all") return { mode: "all" };

    const asNum = Number(answer);
    if (!Number.isNaN(asNum) && asNum >= 1 && asNum <= states.length) {
        return { mode: "one", slug: states[asNum - 1].slug };
    }

    const exact = states.find((s) => s.slug === answer);
    if (exact) return { mode: "one", slug: exact.slug };

    return null;
}

// =====================
// MAIN
// =====================
async function main() {
    if (!SPREADSHEET_ID) {
        throw new Error("Missing GOOGLE_SHEET_ID in .env");
    }

    await loadTokens();

    const states = await listOutStates();
    if (!states.length) {
        throw new Error(
            `No states found in ${OUT_ROOT} (expected scripts/out/<slug>/<slug>.json)`
        );
    }

    // ✅ Determine targets: UI arg takes precedence
    let targets = [];

    const parsedArg = parseStateArgIntoSlugs(STATE_ARG);
    if (parsedArg) {
        if (parsedArg.mode === "all") {
            targets = states;
        } else {
            const wanted = new Set(parsedArg.slugs);
            targets = states.filter((s) => wanted.has(s.slug));
            if (!targets.length) {
                throw new Error(
                    `No states matched --state="${STATE_ARG}". Available slugs example: ${states
                        .slice(0, 10)
                        .map((x) => x.slug)
                        .join(", ")}`
                );
            }
        }
    } else {
        // fallback interactive
        const choice = await promptStateChoice(states);
        if (!choice) throw new Error("State not found / invalid selection.");

        targets =
            choice.mode === "all"
                ? states
                : [states.find((s) => s.slug === choice.slug)].filter(Boolean);
    }

    console.log(`\n📄 Loading Google Sheet tab indexes...`);

    // ✅ IMPORTANT: composite keys
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

    // sanity required headers for update
    for (const tab of [countyTabIndex, cityTabIndex]) {
        for (const h of ["Status", "Location Id", "Account Name"]) {
            if (!tab.headerMap.has(h)) {
                throw new Error(`Sheet tab "${tab.sheetName}" missing required header "${h}"`);
            }
        }
    }

    console.log(`\n🚀 RUN START | mode=${isDryRun ? "DRY" : "LIVE"} | targets=${targets.length}`);
    console.log(`Tabs: Counties="${COUNTY_TAB}" | Cities="${CITY_TAB}"`);
    console.log(`RunId(local)=${RUN_ID} | Throttle min=${MIN_MS_BETWEEN_GHL_CALLS}ms\n`);

    // ✅ compute totals for progress across ALL targets
    let totals = { all: 0, counties: 0, cities: 0 };
    for (const t of targets) {
        const st = await readJson(t.jsonPath);
        const cnt = countEntitiesInStateJson(st);
        totals.all += cnt.allTotal;
        totals.counties += cnt.countiesTotal;
        totals.cities += cnt.citiesTotal;
    }

    const done = { all: 0, counties: 0, cities: 0 };

    emitProgressInit({
        totals,
        message: `Run Delta System (${targets.length} state(s))`,
    });

    let totalCounty = 0;
    let totalCity = 0;
    let totalSkipped = 0;

    for (let i = 0; i < targets.length; i++) {
        const t = targets[i];

        console.log(`\n⏳ [${i + 1}/${targets.length}] Processing: ${t.slug}`);

        emitProgress({
            totals,
            done,
            last: { kind: "state", state: t.slug, action: "start" },
            message: `State ${t.slug} • start`,
        });

        const summary = await runState({
            slug: t.slug,
            jsonPath: t.jsonPath,
            countyTabIndex,
            cityTabIndex,
            progressTotals: totals,
            progressDone: done,
        });

        totalCounty += summary.countyCreated;
        totalCity += summary.cityCreated;
        totalSkipped += summary.skipped;

        emitProgress({
            totals,
            done,
            last: { kind: "state", state: t.slug, action: "done" },
            message: `State ${t.slug} • done`,
        });
    }

    const elapsedMs = Date.now() - RUN_STARTED_AT;

    console.log("--------------------------------------------------");
    console.log(
        `🎉 DONE | counties=${totalCounty} | cities=${totalCity} | skipped=${totalSkipped} | time=${(
            elapsedMs / 1000
        ).toFixed(1)}s`
    );

    emitProgressEnd({ totals, done, ok: true });
}

main().catch((e) => {
    console.error("❌ Fatal:", e?.message || e);
    if (DEBUG) console.dir(e, { depth: 6 });

    try {
        emitProgressEnd({
            totals: { all: 1, counties: 0, cities: 0 },
            done: { all: 1, counties: 0, cities: 0 },
            ok: false,
            error: e?.message || String(e),
        });
    } catch { }

    process.exit(1);
});
