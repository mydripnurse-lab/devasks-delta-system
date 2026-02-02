// scripts/src/builds/build-sheets-counties-cities.js
import "dotenv/config";

import fs from "fs/promises";
import path from "path";
import readline from "readline/promises";
import { stdin as input, stdout as output } from "process";

import {
    loadSheetTabIndex,
    appendRow,
    buildRowFromHeaders,
    updateRowByHeaders,
    makeCompositeKey,
    norm,
    isFilled,
    getArgValue,
} from "../../../services/sheetsClient.js";

const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID;
const COUNTY_TAB = process.env.GOOGLE_SHEET_COUNTY_TAB || "Counties";
const CITY_TAB = process.env.GOOGLE_SHEET_CITY_TAB || "Cities";

const RESOURCES_DIR = path.join(process.cwd(), "resources", "statesFiles");

// Defaults
const DEFAULT_PHONE =
    process.env.MDN_PHONE || process.env.DEFAULT_PHONE || "1 (833) 381-0071";

// Headers esperados (incluye Robots txt)
const COUNTY_HEADERS_EXPECTED = [
    "Account Name",
    "Company Id",
    "County",
    "State",
    "Domain",
    "Phone",
    "Timezone",
    "Status",
    "Robots txt", // NEW
];

const CITY_HEADERS_EXPECTED = [
    "Account Name",
    "Company Id",
    "County",
    "State",
    "City",
    "County Domain",
    "City Domain",
    "Phone",
    "Timezone",
    "Status",
    "Robots txt", // NEW
];

function isStatusTrue(v) {
    const s = String(v ?? "").trim().toLowerCase();
    return s === "true" || s === "1" || s === "yes" || s === "y";
}

function pickTimezoneFrom(obj) {
    return (
        obj?.Timezone?.Zone ||
        obj?.timezone?.Zone ||
        obj?.Timezone?.zone ||
        obj?.timezone?.zone ||
        obj?.Timezone?.Name ||
        obj?.Timezone ||
        obj?.timezone ||
        obj?.tz ||
        ""
    );
}

function pickSitemapFrom(obj) {
    return (
        obj?.Sitemap ||
        obj?.sitemap ||
        obj?.sitemapUrl ||
        obj?.sitemap_url ||
        obj?.SitemapUrl ||
        ""
    );
}

function toUrlMaybe(domainOrUrl) {
    const d = String(domainOrUrl ?? "").trim();
    if (!d) return "";
    if (d.startsWith("http://") || d.startsWith("https://")) return d;
    return `https://${d}`;
}

function extractCounties(stateJson) {
    if (!stateJson) return [];
    if (Array.isArray(stateJson)) return stateJson;
    if (Array.isArray(stateJson.counties)) return stateJson.counties;
    if (Array.isArray(stateJson.items)) return stateJson.items;

    for (const k of Object.keys(stateJson)) {
        if (Array.isArray(stateJson[k])) return stateJson[k];
    }
    return [];
}

function ensureHeaders(actualHeaders, expectedHeaders, tabName) {
    const missing = expectedHeaders.filter((h) => !actualHeaders.includes(h));
    if (missing.length) {
        throw new Error(
            `Tab "${tabName}" missing headers: ${missing.join(", ")}\nFound: ${actualHeaders.join(", ")}`
        );
    }
}

async function listStateSlugs() {
    const files = await fs.readdir(RESOURCES_DIR);
    return files
        .filter((f) => f.toLowerCase().endsWith(".json"))
        .map((f) => f.replace(/\.json$/i, ""))
        .sort((a, b) => a.localeCompare(b));
}

// ----------------- Naming helpers -----------------
function cleanCountyOrParishName(raw) {
    let s = String(raw || "").trim();
    if (!s) return "";

    s = s.replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
    s = s.replace(/,+\s*$/, "").trim();

    s = s.replace(/\bParish\s+County\b/gi, "Parish").trim();
    s = s.replace(/\bCounty\s+Parish\b/gi, "Parish").trim();
    s = s.replace(/\bCounty\s+County\b/gi, "County").trim();

    return s;
}

function isLouisiana(stateName) {
    return String(stateName || "").trim().toLowerCase() === "louisiana";
}

function normalizeCountyLabel({ stateName, countyObj }) {
    const la = isLouisiana(stateName);

    const parishName = cleanCountyOrParishName(countyObj?.parishName);
    const countyName = cleanCountyOrParishName(countyObj?.countyName);

    let base = parishName || countyName || "";
    if (!base) return { label: "", type: "county" };

    const alreadyParish = /\bparish\b/i.test(base);

    if (la || alreadyParish || parishName) {
        if (!alreadyParish) base = `${base} Parish`;
        base = cleanCountyOrParishName(base);
        return { label: base, type: "parish" };
    }

    const alreadyCounty = /\bcounty\b/i.test(base);
    if (!alreadyCounty) base = `${base} County`;
    base = cleanCountyOrParishName(base);
    return { label: base, type: "county" };
}

// ----------------- State slugify -----------------
function slugifyFolderName(input) {
    return String(input || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .trim()
        .replace(/&/g, "and")
        .replace(/[^a-z0-9\s-]/g, "")
        .replace(/\s+/g, "-")
        .replace(/-+/g, "-");
}

// ----------------- UI / args -----------------
function getRequestedState() {
    const arg = getArgValue("state");
    const env = process.env.DELTA_STATE || process.env.STATE || "";
    const picked = String(arg || env || "").trim();
    return picked || "";
}

async function promptStateChoice(stateSlugs) {
    const rl = readline.createInterface({ input, output });

    console.log("\nAvailable states (resources/statesFiles):");
    stateSlugs.forEach((s, i) => console.log(`  ${i + 1}) ${s}`));
    console.log("  ALL) build all states");

    const answer = (await rl.question("\nType state number OR state slug OR ALL: ")).trim();
    rl.close();

    if (!answer) return null;
    if (answer.toLowerCase() === "all") return { mode: "all" };

    const asNum = Number(answer);
    if (!Number.isNaN(asNum) && asNum >= 1 && asNum <= stateSlugs.length) {
        return { mode: "one", slug: stateSlugs[asNum - 1] };
    }

    const found = stateSlugs.find((s) => s === answer);
    if (found) return { mode: "one", slug: found };

    return null;
}

async function resolveTargetsFromArgOrEnv({ requested, stateSlugs }) {
    const req = String(requested || "").trim();
    if (!req) return null;

    if (req.toLowerCase() === "all") {
        return { mode: "all", targets: stateSlugs };
    }

    const direct = stateSlugs.find((s) => s.toLowerCase() === req.toLowerCase());
    if (direct) return { mode: "one", targets: [direct] };

    const slugified = slugifyFolderName(req);
    const bySlugified = stateSlugs.find((s) => s.toLowerCase() === slugified);
    if (bySlugified) return { mode: "one", targets: [bySlugified] };

    for (const slug of stateSlugs) {
        try {
            const filePath = path.join(RESOURCES_DIR, `${slug}.json`);
            const raw = await fs.readFile(filePath, "utf8");
            const stateJson = JSON.parse(raw);
            const stateName = String(stateJson?.stateName || stateJson?.name || "").trim();
            if (stateName && stateName.toLowerCase() === req.toLowerCase()) {
                return { mode: "one", targets: [slug] };
            }
        } catch { }
    }

    return null;
}

// ----------------- Robots txt builder -----------------
function buildRobotsTxt(sitemapUrl) {
    const sm = String(sitemapUrl || "").trim();
    return [
        "User-agent: *",
        "Allow: /",
        "",
        "# Allow all AI crawlers",
        "User-agent: GPTBot",
        "Allow: /",
        "",
        "User-agent: ChatGPT-User",
        "Allow: /",
        "",
        "User-agent: Bingbot",
        "Allow: /",
        "",
        "User-agent: Applebot",
        "Allow: /",
        "",
        "User-agent: PerplexityBot",
        "Allow: /",
        "",
        "User-agent: ClaudeBot",
        "Allow: /",
        "",
        "User-agent: OAI-SearchBot",
        "Allow: /",
        "",
        "User-agent: Bytespider",
        "Allow: /",
        "",
        "User-agent: Amazonbot",
        "Allow: /",
        "",
        "User-agent: FacebookBot",
        "Allow: /",
        "",
        "User-agent: Twitterbot",
        "Allow: /",
        "",
        `Sitemap: ${sm || "<sitemap-url>"}`,
    ].join("\n");
}

// ----------------- Composite upsert -----------------
function getExistingByCompositeKey(tabIndex, keyHeaders, valuesByHeader) {
    const key = makeCompositeKey(keyHeaders, valuesByHeader);
    return tabIndex.mapByKeyValue.get(key) || null;
}

function setIndexAfterCreate(tabIndex, keyHeaders, valuesByHeader, rowNumber, rowArrayNoHeader) {
    const key = makeCompositeKey(keyHeaders, valuesByHeader);
    tabIndex.mapByKeyValue.set(key, { rowNumber, row: rowArrayNoHeader });
    tabIndex.rows.push(rowArrayNoHeader);
}

async function upsertByCompositeKey({
    spreadsheetId,
    tabName,
    tabIndex,
    keyHeaders,
    keyValuesMap,
    rowMap,
}) {
    const existing = getExistingByCompositeKey(tabIndex, keyHeaders, keyValuesMap);

    if (!existing) {
        if (!("Status" in rowMap)) rowMap.Status = "FALSE";

        const rowArray = buildRowFromHeaders(tabIndex.headers, rowMap);
        const created = await appendRow({
            spreadsheetId,
            sheetName: tabName,
            valuesArray: rowArray,
        });

        const newRowNumber = created?.rowNumber || tabIndex.rows.length + 2;
        setIndexAfterCreate(tabIndex, keyHeaders, keyValuesMap, newRowNumber, rowArray);

        return { action: "created" };
    }

    const statusCol = tabIndex.headerMap.get("Status");
    const currentStatus = statusCol !== undefined ? existing.row?.[statusCol] : "";
    if (isStatusTrue(currentStatus)) {
        return { action: "skipped_true" };
    }

    const updates = {};
    let changed = 0;

    for (const [header, desiredVal] of Object.entries(rowMap)) {
        const colIndex0 = tabIndex.headerMap.get(header);
        if (colIndex0 === undefined) continue;
        if (header === "Status") continue;

        const currentVal = existing.row?.[colIndex0];
        const cur = isFilled(currentVal) ? String(currentVal).trim() : "";
        const des = isFilled(desiredVal) ? String(desiredVal).trim() : "";

        if (cur !== des) {
            updates[header] = des;
            changed++;
        }
    }

    if (!changed) return { action: "skipped_same" };

    await updateRowByHeaders({
        spreadsheetId,
        sheetName: tabName,
        headers: tabIndex.headers,
        rowNumber: existing.rowNumber,
        updatesByHeader: updates,
    });

    for (const [h, v] of Object.entries(updates)) {
        const idx = tabIndex.headerMap.get(h);
        if (idx !== undefined && existing.row) existing.row[idx] = v;
    }

    return { action: "updated", changed };
}

// ----------------- Logging helpers -----------------
function ms(n) {
    return `${Math.round(n)}ms`;
}

function pct(done, total) {
    if (!total) return 0;
    return Math.round((done / total) * 100);
}

/**
 * ✅ Machine-readable progress log.
 * Your UI should parse lines that begin with "__PROGRESS__ ".
 */
function emitProgress(payload) {
    // single-line, parseable
    console.log(`__PROGRESS__ ${JSON.stringify(payload)}`);
}
function emitProgressInit(payload) {
    console.log(`__PROGRESS_INIT__ ${JSON.stringify(payload)}`);
}
function emitProgressEnd(payload) {
    console.log(`__PROGRESS_END__ ${JSON.stringify(payload)}`);
}

// ----------------- State processor -----------------
async function processState({ slug, spreadsheetId, countyTabIndex, cityTabIndex }) {
    const filePath = path.join(RESOURCES_DIR, `${slug}.json`);
    const raw = await fs.readFile(filePath, "utf8");
    const stateJson = JSON.parse(raw);

    const stateName = stateJson?.stateName || stateJson?.name || slug;
    const companyId = stateJson?.companyId || process.env.COMPANY_ID || "";
    const counties = extractCounties(stateJson);

    // Precompute totals for progress
    let totalCountyItems = 0;
    let totalCityItems = 0;

    for (const c of counties) {
        const { label } = normalizeCountyLabel({ stateName, countyObj: c });
        if (isFilled(label)) totalCountyItems++;

        const cities = Array.isArray(c?.cities) ? c.cities : [];
        for (const city of cities) {
            const cityName = String(city?.cityName || "").trim();
            if (isFilled(cityName)) totalCityItems++;
        }
    }

    const totalItems = totalCountyItems + totalCityItems;

    let doneItems = 0;
    let doneCounties = 0;
    let doneCities = 0;

    let createdCounties = 0;
    let updatedCounties = 0;
    let skippedCounties = 0;

    let createdCities = 0;
    let updatedCities = 0;
    let skippedCities = 0;

    const tStateStart = Date.now();

    // ✅ INIT event (UI can reset progress bar)
    emitProgressInit({
        job: "build-sheet-rows",
        script: "build-sheets-counties-cities",
        stateSlug: slug,
        stateName,
        totals: { all: totalItems, counties: totalCountyItems, cities: totalCityItems },
        done: { all: 0, counties: 0, cities: 0 },
        counts: {
            counties: { created: 0, updated: 0, skipped: 0 },
            cities: { created: 0, updated: 0, skipped: 0 },
        },
        pct: 0,
        ts: Date.now(),
    });

    for (const c of counties) {
        const countyDomainRaw = c?.countyDomain || c?.parishDomain || c?.domain || "";
        const countyDomain = String(countyDomainRaw || "").trim();
        const countyTimezone = pickTimezoneFrom(c);

        const { label: countyLabel } = normalizeCountyLabel({ stateName, countyObj: c });
        const cities = Array.isArray(c?.cities) ? c.cities : [];

        // ---- Counties tab ----
        if (isFilled(countyLabel)) {
            const countyAccountName = `My Drip Nurse ${countyLabel}, ${stateName}`;

            const countySitemap =
                String(pickSitemapFrom(c) || "").trim() ||
                (countyDomain ? `${toUrlMaybe(countyDomain)}/sitemap.xml` : "");

            const countyRow = {
                "Account Name": countyAccountName,
                "Company Id": companyId,
                County: countyLabel,
                State: stateName,
                Domain: countyDomain,
                Phone: DEFAULT_PHONE,
                Timezone: countyTimezone,
                "Robots txt": buildRobotsTxt(countySitemap),
            };

            const t0 = Date.now();
            const res = await upsertByCompositeKey({
                spreadsheetId,
                tabName: COUNTY_TAB,
                tabIndex: countyTabIndex,
                keyHeaders: ["State", "County"],
                keyValuesMap: { State: stateName, County: countyLabel },
                rowMap: countyRow,
            });
            const took = Date.now() - t0;

            doneItems++;
            doneCounties++;

            if (res.action === "created") createdCounties++;
            else if (res.action === "updated") updatedCounties++;
            else skippedCounties++;

            const progressPct = pct(doneItems, totalItems);

            // Human log (kept)
            console.log(
                `🧩 [${progressPct}% (${doneItems}/${totalItems})] COUNTY • ${countyLabel} • ${res.action} • ${ms(took)}`
            );

            // ✅ Machine log (UI reads this)
            emitProgress({
                job: "build-sheet-rows",
                script: "build-sheets-counties-cities",
                stateSlug: slug,
                stateName,
                pct: progressPct,
                totals: { all: totalItems, counties: totalCountyItems, cities: totalCityItems },
                done: { all: doneItems, counties: doneCounties, cities: doneCities },
                counts: {
                    counties: { created: createdCounties, updated: updatedCounties, skipped: skippedCounties },
                    cities: { created: createdCities, updated: updatedCities, skipped: skippedCities },
                },
                last: { kind: "county", county: countyLabel, action: res.action, tookMs: took },
                ts: Date.now(),
            });
        }

        // ---- Cities tab ----
        for (const city of cities) {
            const cityName = String(city?.cityName || "").trim();
            if (!isFilled(cityName)) continue;

            const cityDomainRaw = city?.cityDomain || city?.domain || "";
            const cityDomain = String(cityDomainRaw || "").trim();
            const cityTimezone = pickTimezoneFrom(city) || countyTimezone;

            const cityAccountName = `My Drip Nurse ${cityName}, ${countyLabel}, ${stateName}`;

            const citySitemap =
                String(pickSitemapFrom(city) || "").trim() ||
                (cityDomain ? `${toUrlMaybe(cityDomain)}/sitemap.xml` : "");

            const cityRow = {
                "Account Name": cityAccountName,
                "Company Id": companyId,
                County: countyLabel,
                State: stateName,
                City: cityName,
                "County Domain": countyDomain,
                "City Domain": cityDomain,
                Phone: DEFAULT_PHONE,
                Timezone: cityTimezone,
                "Robots txt": buildRobotsTxt(citySitemap),
            };

            const t0 = Date.now();
            const res = await upsertByCompositeKey({
                spreadsheetId,
                tabName: CITY_TAB,
                tabIndex: cityTabIndex,
                keyHeaders: ["State", "County", "City"],
                keyValuesMap: { State: stateName, County: countyLabel, City: cityName },
                rowMap: cityRow,
            });
            const took = Date.now() - t0;

            doneItems++;
            doneCities++;

            if (res.action === "created") createdCities++;
            else if (res.action === "updated") updatedCities++;
            else skippedCities++;

            const progressPct = pct(doneItems, totalItems);

            // Human log (kept)
            console.log(
                `🏙️ [${progressPct}% (${doneItems}/${totalItems})] CITY   • ${cityName}, ${countyLabel} • ${res.action} • ${ms(took)}`
            );

            // ✅ Machine log
            emitProgress({
                job: "build-sheet-rows",
                script: "build-sheets-counties-cities",
                stateSlug: slug,
                stateName,
                pct: progressPct,
                totals: { all: totalItems, counties: totalCountyItems, cities: totalCityItems },
                done: { all: doneItems, counties: doneCounties, cities: doneCities },
                counts: {
                    counties: { created: createdCounties, updated: updatedCounties, skipped: skippedCounties },
                    cities: { created: createdCities, updated: updatedCities, skipped: skippedCities },
                },
                last: { kind: "city", county: countyLabel, city: cityName, action: res.action, tookMs: took },
                ts: Date.now(),
            });
        }
    }

    const tState = Date.now() - tStateStart;

    // ✅ END event
    emitProgressEnd({
        job: "build-sheet-rows",
        script: "build-sheets-counties-cities",
        stateSlug: slug,
        stateName,
        pct: 100,
        totals: { all: totalItems, counties: totalCountyItems, cities: totalCityItems },
        done: { all: doneItems, counties: doneCounties, cities: doneCities },
        counts: {
            counties: { created: createdCounties, updated: updatedCounties, skipped: skippedCounties },
            cities: { created: createdCities, updated: updatedCities, skipped: skippedCities },
        },
        timeMs: tState,
        ts: Date.now(),
    });

    return {
        state: slug,
        stateName,
        timeMs: tState,
        totals: { counties: totalCountyItems, cities: totalCityItems, all: totalItems },
        counties: {
            created: createdCounties,
            updated: updatedCounties,
            skipped: skippedCounties,
        },
        cities: { created: createdCities, updated: updatedCities, skipped: skippedCities },
    };
}

async function main() {
    if (!SPREADSHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID in .env");

    const stateSlugs = await listStateSlugs();
    if (!stateSlugs.length) throw new Error(`No state JSON found in: ${RESOURCES_DIR}`);

    const requested = getRequestedState();
    const resolved = await resolveTargetsFromArgOrEnv({ requested, stateSlugs });

    let targetsInfo = resolved;
    if (!targetsInfo) {
        const choice = await promptStateChoice(stateSlugs);
        if (!choice) throw new Error("State not found / invalid selection.");
        targetsInfo = {
            mode: choice.mode,
            targets: choice.mode === "all" ? stateSlugs : [choice.slug],
        };
    }

    console.log(`\n📄 Loading Google Sheet tab indexes...`);

    const countyTabIndex = await loadSheetTabIndex({
        spreadsheetId: SPREADSHEET_ID,
        sheetName: COUNTY_TAB,
        range: "A:AZ",
        accountNameHeader: "Account Name",
        keyHeaders: ["State", "County"],
        logScope: "sheets",
    });

    const cityTabIndex = await loadSheetTabIndex({
        spreadsheetId: SPREADSHEET_ID,
        sheetName: CITY_TAB,
        range: "A:AZ",
        accountNameHeader: "Account Name",
        keyHeaders: ["State", "County", "City"],
        logScope: "sheets",
    });

    ensureHeaders(countyTabIndex.headers, COUNTY_HEADERS_EXPECTED, COUNTY_TAB);
    ensureHeaders(cityTabIndex.headers, CITY_HEADERS_EXPECTED, CITY_TAB);

    const targets = targetsInfo.targets;

    console.log(
        `\n✅ Starting build: ${targets.length} state(s)\nTabs: ${COUNTY_TAB} + ${CITY_TAB}\n`
    );

    for (let i = 0; i < targets.length; i++) {
        const slug = targets[i];
        console.log(`⏳ [${i + 1}/${targets.length}] Building state: ${slug}`);

        const summary = await processState({
            slug,
            spreadsheetId: SPREADSHEET_ID,
            countyTabIndex,
            cityTabIndex,
        });

        console.log(
            `✅ Done ${summary.state} (${summary.stateName}) • ${ms(summary.timeMs)}\n` +
            `   Counties (total ${summary.totals.counties}): +${summary.counties.created} upd:${summary.counties.updated} skip:${summary.counties.skipped}\n` +
            `   Cities   (total ${summary.totals.cities}): +${summary.cities.created} upd:${summary.cities.updated} skip:${summary.cities.skipped}\n`
        );

        if (targetsInfo.mode === "all") {
            const freshCounty = await loadSheetTabIndex({
                spreadsheetId: SPREADSHEET_ID,
                sheetName: COUNTY_TAB,
                range: "A:AZ",
                accountNameHeader: "Account Name",
                keyHeaders: ["State", "County"],
                logScope: "sheets",
            });

            const freshCity = await loadSheetTabIndex({
                spreadsheetId: SPREADSHEET_ID,
                sheetName: CITY_TAB,
                range: "A:AZ",
                accountNameHeader: "Account Name",
                keyHeaders: ["State", "County", "City"],
                logScope: "sheets",
            });

            countyTabIndex.headers = freshCounty.headers;
            countyTabIndex.headerMap = freshCounty.headerMap;
            countyTabIndex.rows = freshCounty.rows;
            countyTabIndex.mapByKeyValue = freshCounty.mapByKeyValue;

            cityTabIndex.headers = freshCity.headers;
            cityTabIndex.headerMap = freshCity.headerMap;
            cityTabIndex.rows = freshCity.rows;
            cityTabIndex.mapByKeyValue = freshCity.mapByKeyValue;
        }
    }

    console.log("🎉 Build completed.\n");
}

main().catch((e) => {
    console.error("❌ Fatal:", e?.message || e);
    process.exit(1);
});
