// scripts/src/builds/build-counties.js
import "dotenv/config";

import fs from "fs/promises";
import path from "path";

const STATES_DIR = path.join(process.cwd(), "resources", "statesFiles");
const OUT_ROOT = path.join(process.cwd(), "scripts", "out");

// ====== ENV (required) ======
const COMPANY_ID = process.env.COMPANY_ID || process.env.COMPANYID || "";
const SNAPSHOT_ID = process.env.SNAPSHOT_ID || process.env.SNAPSHOTID || "";

// ====== Helpers ======
function latinToAscii(str) {
    return String(str || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "");
}

function slugify(name) {
    return latinToAscii(name)
        .trim()
        .toLowerCase()
        .replace(/&/g, "and")
        .replace(/['"]/g, "")
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/-+/g, "-")
        .replace(/^-|-$/g, "");
}

function isPR(stateSlug, stateName) {
    const s = String(stateSlug || "").toLowerCase();
    const n = String(stateName || "").toLowerCase();
    return s === "puerto-rico" || n.includes("puerto rico");
}

function isLA(stateSlug, stateName) {
    const s = String(stateSlug || "").toLowerCase();
    const n = String(stateName || "").toLowerCase();
    return s === "louisiana" || n.includes("louisiana");
}

function safeStr(v) {
    return v === null || v === undefined ? "" : String(v);
}

async function ensureDir(dir) {
    await fs.mkdir(dir, { recursive: true });
}

async function readJson(filePath) {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw);
}

function extractCounties(stateJson) {
    if (!stateJson) return [];
    if (Array.isArray(stateJson)) return stateJson;
    if (Array.isArray(stateJson.items)) return stateJson.items;
    if (Array.isArray(stateJson.counties)) return stateJson.counties;

    for (const k of Object.keys(stateJson)) {
        if (Array.isArray(stateJson[k])) return stateJson[k];
    }
    return [];
}

function pickStateName(stateJson, fallbackSlug) {
    return (
        stateJson?.stateName ||
        stateJson?.name ||
        stateJson?.State ||
        (fallbackSlug ? fallbackSlug.replace(/-/g, " ") : "Unknown")
    );
}

/**
 * ✅ TIMEZONE FIX:
 * Lee Timezone.Zone del objeto county/parish/bucket.
 * Soporta:
 * - c.Timezone.Zone
 * - c.timezone.Zone
 * - c.Timezone (si viene string)
 * - c.timezone (si viene string)
 */
function pickTimezoneZoneFromCountyObj(c) {
    const z =
        c?.Timezone?.Zone ||
        c?.timezone?.Zone ||
        c?.Timezone?.zone ||
        c?.timezone?.zone ||
        c?.Timezone ||
        c?.timezone ||
        "";
    return safeStr(z).trim();
}

function getEnvOptionalDefaults() {
    // timezone NO se toma de env, se toma del JSON (Timezone.Zone)
    return {
        phone: process.env.DEFAULT_PHONE || "",
        prospectInfo: {
            firstName: process.env.OWNER_FIRST_NAME || "",
            lastName: process.env.OWNER_LAST_NAME || "",
            email: process.env.OWNER_EMAIL || "",
        },
        twilio: {
            sid: process.env.TWILIO_SID || "",
            authToken: process.env.TWILIO_AUTH_TOKEN || "",
        },
        mailgun: {
            apiKey: process.env.MAILGUN_API_KEY || "",
            domain: process.env.MAILGUN_DOMAIN || "",
        },
    };
}

/**
 * ✅ Construye el body para COUNTY (se crea cuenta para el county)
 * Reglas:
 * - PR:   "My Drip Nurse Puerto Rico" (o stateName)
 * - LA:   "My Drip Nurse <County> Parish, <State>"
 * - Else: "My Drip Nurse <County> County, <State>"
 */
function buildCountyBody({
    stateSlug,
    stateName,
    countyName,
    phone,
    timezone,
    website,
    twilio,
    mailgun,
    prospectInfo,
    settings,
    social,
}) {
    const pr = isPR(stateSlug, stateName);
    const la = isLA(stateSlug, stateName);

    let locationName = "";
    if (pr) {
        locationName = `My Drip Nurse ${countyName || stateName}`;
    } else if (la) {
        locationName = `My Drip Nurse ${countyName} Parish, ${stateName}`;
    } else {
        locationName = `My Drip Nurse ${countyName} County, ${stateName}`;
    }

    return {
        name: locationName,
        phone: phone || "",
        state: stateName || "",
        country: "US",
        prospectInfo: prospectInfo || { firstName: "", lastName: "", email: "" },
        settings: settings || {
            allowDuplicateContact: false,
            allowDuplicateOpportunity: false,
            allowFacebookNameMerge: false,
            disableContactTimezone: false,
        },
        social: social || {
            facebookUrl: "",
            googlePlus: "",
            linkedIn: "",
            foursquare: "",
            twitter: "",
            yelp: "",
            instagram: "",
            youtube: "",
            pinterest: "",
            blogRss: "",
            googlePlacesId: "",
        },
        twilio: twilio || { sid: "", authToken: "" },
        mailgun: mailgun || { apiKey: "", domain: "" },

        companyId: COMPANY_ID,
        snapshotId: SNAPSHOT_ID,

        timezone: timezone || "",
        website: website || "",
    };
}

/**
 * ✅ Construye el body para CITY (como ya venías haciendo)
 * Reglas:
 * - PR: "My Drip Nurse <City>, <State>"
 * - LA: "My Drip Nurse <City>, <County> Parish, <State>"
 * - Else:"My Drip Nurse <City>, <County> County, <State>"
 */
function buildCityBody({
    stateSlug,
    stateName,
    countyName,
    cityName,
    phone,
    timezone,
    website,
    twilio,
    mailgun,
    prospectInfo,
    settings,
    social,
}) {
    const pr = isPR(stateSlug, stateName);
    const la = isLA(stateSlug, stateName);

    let locationName = "";
    if (pr) {
        locationName = `My Drip Nurse ${cityName}, ${stateName}`;
    } else if (la) {
        locationName = `My Drip Nurse ${cityName}, ${countyName} Parish, ${stateName}`;
    } else {
        locationName = `My Drip Nurse ${cityName}, ${countyName} County, ${stateName}`;
    }

    return {
        name: locationName,
        phone: phone || "",
        state: stateName || "",
        country: "US",
        prospectInfo: prospectInfo || { firstName: "", lastName: "", email: "" },
        settings: settings || {
            allowDuplicateContact: false,
            allowDuplicateOpportunity: false,
            allowFacebookNameMerge: false,
            disableContactTimezone: false,
        },
        social: social || {
            facebookUrl: "",
            googlePlus: "",
            linkedIn: "",
            foursquare: "",
            twitter: "",
            yelp: "",
            instagram: "",
            youtube: "",
            pinterest: "",
            blogRss: "",
            googlePlacesId: "",
        },
        twilio: twilio || { sid: "", authToken: "" },
        mailgun: mailgun || { apiKey: "", domain: "" },

        companyId: COMPANY_ID,
        snapshotId: SNAPSHOT_ID,

        timezone: timezone || "",
        website: website || "",
    };
}

// ====== Main builder ======
async function listStateFiles() {
    const files = await fs.readdir(STATES_DIR);
    return files
        .filter((f) => f.toLowerCase().endsWith(".json"))
        .map((f) => ({
            file: f,
            slug: f.replace(/\.json$/i, ""),
            fullPath: path.join(STATES_DIR, f),
        }))
        .sort((a, b) => a.slug.localeCompare(b.slug));
}

async function buildOneState(stateFile) {
    if (!COMPANY_ID) {
        throw new Error("Missing companyId. Set COMPANY_ID in .env (COMPANY_ID=...)");
    }
    if (!SNAPSHOT_ID) {
        throw new Error("Missing snapshotId. Set SNAPSHOT_ID in .env (SNAPSHOT_ID=...)");
    }

    const stateJson = await readJson(stateFile.fullPath);
    const stateSlug = stateFile.slug;
    const stateName = pickStateName(stateJson, stateSlug);

    const countiesRaw = extractCounties(stateJson);
    const pr = isPR(stateSlug, stateName);
    const envDefaults = getEnvOptionalDefaults();

    let countiesNormalized = [];

    // ---------------------------
    // Puerto Rico (bucket)
    // ---------------------------
    if (pr) {
        const first = countiesRaw[0] || {};
        const cities = Array.isArray(first?.cities) ? first.cities : [];

        const bucketCountyName = "Puerto Rico";
        const bucketTimezone = pickTimezoneZoneFromCountyObj(first);

        // ✅ COUNTY BODY (bucket)
        const countyBody = buildCountyBody({
            stateSlug,
            stateName,
            countyName: bucketCountyName,
            phone: envDefaults.phone,
            timezone: bucketTimezone,
            website: "",
            twilio: envDefaults.twilio,
            mailgun: envDefaults.mailgun,
            prospectInfo: envDefaults.prospectInfo,
        });

        countiesNormalized = [
            {
                type: "county",
                countyName: bucketCountyName,
                countyDomain: "",
                countySitemap: "",
                cityCount: cities.length,

                body: countyBody,

                cities: cities.map((city) => {
                    const cityName = safeStr(city?.cityName);
                    const cityDomain = safeStr(city?.cityDomain) || safeStr(city?.domain);
                    const website = cityDomain || "";

                    const body = buildCityBody({
                        stateSlug,
                        stateName,
                        countyName: bucketCountyName,
                        cityName,
                        phone: envDefaults.phone,
                        timezone: bucketTimezone,
                        website,
                        twilio: envDefaults.twilio,
                        mailgun: envDefaults.mailgun,
                        prospectInfo: envDefaults.prospectInfo,
                    });

                    return {
                        type: "city",
                        cityName,
                        cityDomain,
                        citySitemap: safeStr(city?.citySitemap),
                        body,
                    };
                }),
            },
        ];
    } else {
        // ---------------------------
        // Normal states + Louisiana
        // ---------------------------
        countiesNormalized = countiesRaw
            .filter((c) => c && (c.countyName || c.parishName))
            .map((c) => {
                const countyName = safeStr(c.countyName || c.parishName);
                const countyDomain = safeStr(c.countyDomain || c.parishDomain);
                const cities = Array.isArray(c.cities) ? c.cities : [];

                const countyTimezone = pickTimezoneZoneFromCountyObj(c);

                // ✅ COUNTY BODY
                const countyBody = buildCountyBody({
                    stateSlug,
                    stateName,
                    countyName,
                    phone: envDefaults.phone,
                    timezone: countyTimezone,
                    website: countyDomain || "",
                    twilio: envDefaults.twilio,
                    mailgun: envDefaults.mailgun,
                    prospectInfo: envDefaults.prospectInfo,
                });

                const mappedCities = cities
                    .filter((city) => city?.cityName)
                    .map((city) => {
                        const cityName = safeStr(city?.cityName);
                        const cityDomain = safeStr(city?.cityDomain);

                        const website = cityDomain || countyDomain || "";

                        const body = buildCityBody({
                            stateSlug,
                            stateName,
                            countyName,
                            cityName,
                            phone: envDefaults.phone,
                            timezone: countyTimezone,
                            website,
                            twilio: envDefaults.twilio,
                            mailgun: envDefaults.mailgun,
                            prospectInfo: envDefaults.prospectInfo,
                        });

                        return {
                            type: "city",
                            cityName,
                            cityDomain,
                            citySitemap: safeStr(city?.citySitemap),
                            body,
                        };
                    });

                return {
                    type: "county",
                    countyName,
                    countyDomain,
                    countySitemap: safeStr(c?.countySitemap),
                    cityCount: mappedCities.length,

                    body: countyBody,

                    cities: mappedCities,
                };
            });
    }

    const countyCount = countiesNormalized.length;

    const out = {
        companyId: COMPANY_ID,
        snapshotId: SNAPSHOT_ID,

        countyCount,
        count: countyCount,

        stateName,
        stateSlug,
        counties: countiesNormalized,
    };

    const outDir = path.join(OUT_ROOT, stateSlug);
    await ensureDir(outDir);

    const outFile = path.join(outDir, `${stateSlug}.json`);

    // ✅ Overwrite ALWAYS
    await fs.writeFile(outFile, JSON.stringify(out, null, 2), "utf8");

    return { outFile, countyCount };
}

/**
 * ✅ New: parse --state= from argv (Next API uses this)
 * Supports:
 * - --state=florida
 * - --state florida
 * - env STATE / DELTA_STATE
 * Default: all
 */
function parseStateFromArgs() {
    const argv = process.argv.slice(2).map(String);

    const eq = argv.find((a) => a.startsWith("--state="));
    if (eq) return String(eq.split("=").slice(1).join("=") || "").trim() || "all";

    const idx = argv.findIndex((a) => a === "--state");
    if (idx >= 0) return String(argv[idx + 1] || "").trim() || "all";

    const env =
        process.env.DELTA_STATE ||
        process.env.STATE ||
        process.env.STATE_SLUG ||
        process.env.STATEOUT ||
        "";
    if (String(env).trim()) return String(env).trim();

    return "all";
}

async function main() {
    const stateFiles = await listStateFiles();
    if (!stateFiles.length) {
        console.error("❌ No state JSON files found in:", STATES_DIR);
        process.exit(1);
    }

    const stateArg = parseStateFromArgs().trim();
    const pick = stateArg.toLowerCase();

    if (!pick || pick === "all") {
        let ok = 0;
        let fail = 0;

        for (const s0 of stateFiles) {
            try {
                const res = await buildOneState(s0);
                console.log(`✅ ${s0.slug} -> ${res.outFile} (countyCount=${res.countyCount})`);
                ok++;
            } catch (e) {
                console.error(`❌ ${s0.slug} failed:`, e?.message || e);
                fail++;
            }
        }

        console.log(`\nDONE. ok=${ok} fail=${fail}\n`);
        return;
    }

    const chosen =
        stateFiles.find((s0) => s0.slug === pick) ||
        stateFiles.find((s0) => s0.slug === slugify(pick));

    if (!chosen) {
        console.error("❌ State not found. You typed:", stateArg);
        console.error("Available:", stateFiles.map((s0) => s0.slug).join(", "));
        process.exit(1);
    }

    const res = await buildOneState(chosen);
    console.log(`\n✅ DONE ${chosen.slug}`);
    console.log(`Output: ${res.outFile}`);
    console.log(`countyCount: ${res.countyCount}\n`);
}

main().catch((e) => {
    console.error("❌ Fatal:", e?.message || e);
    process.exit(1);
});
