import "dotenv/config";
import fs from "node:fs/promises";
import path from "node:path";
import readline from "node:readline/promises";
import { performance } from "node:perf_hooks";
import { nowISO, ensureDir, titleCaseFromKey, cleanUndefined } from "./utils.js";

const ROOT = process.cwd();

const STATES_DIR = path.join(ROOT, "resources", "statesFiles");
const CUSTOM_VALUES_PATH = path.join(
    ROOT,
    "resources",
    "customValues",
    "services",
    "mobile-iv-therapy.json"
);

const CONFIG_PATH = path.join(ROOT, "resources", "config", "ghl.json"); // opcional

// -------------------------
// Helpers (logging + timing)
// -------------------------
function formatDuration(msTotal) {
    const s = Math.max(0, Math.round(msTotal / 1000));
    const m = Math.floor(s / 60);
    const r = s % 60;
    if (m <= 0) return `${r}s`;
    return `${m}m ${r}s`;
}

function pct(done, total) {
    if (!total) return "0%";
    return `${Math.floor((done / total) * 100)}%`;
}

async function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}

// Simulación de “API call”
async function simulateStep(label, minMs = 120, maxMs = 450, failRate = 0) {
    const t0 = performance.now();
    const wait = minMs + Math.random() * (maxMs - minMs);
    await sleep(wait);

    if (failRate > 0 && Math.random() < failRate) {
        const err = new Error(`Simulated failure at step: ${label}`);
        err.step = label;
        throw err;
    }
    return { label, durationMs: performance.now() - t0 };
}

// -------------------------
// Simulation helpers (4 steps)
// -------------------------
function fakeId(prefix = "id") {
    const s = Math.random().toString(36).slice(2, 10);
    return `${prefix}_${s}`;
}

async function simulateCreateLocation(failRate) {
    const step = await simulateStep("create_location", 200, 650, failRate);
    return { ...step, locationId: fakeId("location") };
}

async function simulateGenerateSubaccountToken(locationId, failRate) {
    const step = await simulateStep("generate_subaccount_token", 140, 420, failRate);
    return {
        ...step,
        token: `sub_${locationId}_${Math.random().toString(36).slice(2, 8)}`,
    };
}

async function simulateGetRemoteCustomValues(failRate) {
    const step = await simulateStep("get_custom_values", 160, 520, failRate);

    const remote = [];
    for (let i = 0; i < 120; i++) {
        remote.push({ id: fakeId("cv"), name: `Remote Field ${i + 1}`, value: "" });
    }

    remote.push({ id: fakeId("cv"), name: "countyName", value: "" });
    remote.push({ id: fakeId("cv"), name: "countyDomain", value: "" });
    remote.push({ id: fakeId("cv"), name: "countySitemap", value: "" });
    remote.push({ id: fakeId("cv"), name: "timezoneZone", value: "" });
    remote.push({ id: fakeId("cv"), name: "stateName", value: "" });
    remote.push({ id: fakeId("cv"), name: "stateAbbr", value: "" });
    remote.push({ id: fakeId("cv"), name: "Price - Myers Cocktail", value: "" });

    return { ...step, remoteCustomValues: remote };
}

function mapCustomValuesForUpdate(localCustomValuesBody, remoteCustomValues) {
    const remoteByName = new Map(remoteCustomValues.map((x) => [String(x.name).trim(), x]));
    const local = Array.isArray(localCustomValuesBody?.customValues) ? localCustomValuesBody.customValues : [];

    const updates = [];
    const missing = [];

    for (const cv of local) {
        const name = String(cv?.name ?? "").trim();
        const value = cv?.value ?? "";

        if (!name) continue;

        const remote = remoteByName.get(name);
        if (!remote?.id) {
            missing.push(name);
            continue;
        }

        updates.push({ id: remote.id, name, value });
    }

    return {
        updates,
        summary: {
            localCount: local.length,
            remoteCount: remoteCustomValues.length,
            matched: updates.length,
            missing: missing.length,
            missingNamesSample: missing.slice(0, 25),
        },
    };
}

async function simulatePutCustomValuesOneByOne(updates, failRate) {
    const t0 = performance.now();
    let ok = 0;
    let failed = 0;

    for (let i = 0; i < updates.length; i++) {
        try {
            await simulateStep(`put_custom_value_${i + 1}/${updates.length}`, 60, 180, failRate);
            ok++;
        } catch {
            failed++;
        }
    }

    return { durationMs: performance.now() - t0, ok, failed, total: updates.length };
}

// -------------------------
// IO + parsing
// -------------------------
async function safeReadJson(filePath, fallback = null) {
    try {
        const raw = await fs.readFile(filePath, "utf8");
        return JSON.parse(raw);
    } catch {
        return fallback;
    }
}

async function listStateFiles() {
    const files = await fs.readdir(STATES_DIR);
    return files
        .filter((f) => f.endsWith(".json"))
        .map((f) => ({
            file: f,
            key: f.replace(".json", ""),
            fullPath: path.join(STATES_DIR, f),
        }))
        .sort((a, b) => a.key.localeCompare(b.key));
}

async function pickState(stateFiles) {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

    console.log("\nStates disponibles:\n");
    stateFiles.forEach((s, i) => console.log(`${String(i + 1).padStart(2, "0")}. ${s.key}`));

    const answer = await rl.question("\nEscribe el número del state: ");
    rl.close();

    const idx = Number(answer) - 1;
    if (!Number.isInteger(idx) || idx < 0 || idx >= stateFiles.length) {
        throw new Error("Selección inválida.");
    }
    return stateFiles[idx];
}

function extractCountiesArray(stateJson) {
    if (Array.isArray(stateJson)) return stateJson;
    if (Array.isArray(stateJson.counties)) return stateJson.counties;
    if (Array.isArray(stateJson.data)) return stateJson.data;

    for (const k of Object.keys(stateJson || {})) {
        if (Array.isArray(stateJson[k]) && typeof stateJson[k]?.[0] === "object") return stateJson[k];
    }
    return [];
}

function getTimezoneZone(countyObj) {
    return countyObj?.Timezone?.Zone || "";
}
function getCountyName(countyObj) {
    return String(countyObj?.countyName || "").trim();
}
function getCountyDomain(countyObj) {
    return String(countyObj?.countyDomain || "").trim();
}
function getCountySitemap(countyObj) {
    return String(countyObj?.countySitemap || "").trim();
}

function extractStateAbbrFromDomain(domainUrl) {
    try {
        const host = new URL(domainUrl).host;
        const suffix = ".mydripnurse.com";
        if (!host.endsWith(suffix)) return "";
        const left = host.slice(0, -suffix.length);
        const parts = left.split("-");
        return parts[parts.length - 1] || "";
    } catch {
        return "";
    }
}

// -------------------------
// Builders
// -------------------------
function buildAccountName(companyName, countyName, stateName) {
    return `${companyName} ${countyName} County, ${stateName}`;
}

function buildLocationCreateBody({
    accountName,
    phone,
    companyId,
    city,
    address,
    state,
    country,
    postalCode,
    prospectInfo,
    settings,
    social,
    twilio,
    mailgun,
    snapshotId,
    timezoneZone,
    website,
}) {
    return cleanUndefined({
        name: accountName,
        phone,
        address,
        city,
        state,
        country,
        postalCode,
        prospectInfo,
        settings,
        social,
        twilio,
        mailgun,
        companyId,
        snapshotId,
        timezone: timezoneZone,
        website,
    });
}

function mergeCustomValues(baseCustomValuesJson, dynamicPairs) {
    const base = Array.isArray(baseCustomValuesJson?.customValues) ? baseCustomValuesJson.customValues : [];
    const dynamic = Object.entries(dynamicPairs).map(([name, value]) => ({ name, value }));
    return { customValues: [...base, ...dynamic] };
}

// -------------------------
// Main
// -------------------------
async function main() {
    const stateFiles = await listStateFiles();
    const chosen = await pickState(stateFiles);

    const config = (await safeReadJson(CONFIG_PATH, {})) || {};

    const companyId = process.env.COMPANY_ID || config?.agency?.companyId || "";
    const snapshotId = process.env.SNAPSHOT_ID || config?.agency?.snapshotId || "";
    const phone = process.env.DEFAULT_PHONE || config?.agency?.phone || "";
    const companyName = process.env.COMPANY_NAME || config?.agency?.companyName || "My Drip Nurse";

    const address = process.env.ADDRESS || "";
    const city = process.env.CITY || "";
    const postalCode = process.env.POSTAL_CODE || "";
    const country = process.env.COUNTRY || "US";

    const prospectInfo = {
        firstName: process.env.OWNER_FIRST_NAME || "",
        lastName: process.env.OWNER_LAST_NAME || "",
        email: process.env.OWNER_EMAIL || ""
    };

    const settings = {
        allowDuplicateContact: false,
        allowDuplicateOpportunity: false,
        allowFacebookNameMerge: false,
        disableContactTimezone: false,
    };

    const mailgun = {
        apiKey: process.env.MAILGUN_API_KEY || "",
        domain: process.env.MAILGUN_DOMAIN || "",
    };

    // ✅ FIX: social.youtube estaba roto; aquí está bien
    const social = {
        facebookUrl: "",
        googlePlus: "",
        linkedIn: "",
        foursquare: "",
        twitter: "",
        yelp: "",
        instagram: "",
        youtube: "", // ✅
        pinterest: "",
        blogRss: "",
        googlePlacesId: "",
    };

    const twilio = {
        sid: process.env.TWILIO_SID || "",
        authToken: process.env.TWILIO_AUTH_TOKEN || "",
    };

    if (!companyId) console.log("\n⚠️ Missing COMPANY_ID (env) o agency.companyId (config)\n");
    if (!snapshotId) console.log("\n⚠️ Missing SNAPSHOT_ID (env) o agency.snapshotId (config)\n");

    const stateName = titleCaseFromKey(chosen.key);

    const stateJson = JSON.parse(await fs.readFile(chosen.fullPath, "utf8"));
    const counties = extractCountiesArray(stateJson);
    if (!counties.length) throw new Error("No encontré counties array en el state JSON.");

    const baseCustomValuesJson = await safeReadJson(CUSTOM_VALUES_PATH, { customValues: [] });

    const outDir = path.join(ROOT, "scripts", "out", chosen.key);
    await ensureDir(outDir);

    const stamp = nowISO().replace(/[:.]/g, "-");

    const preview = [];
    const ghlCreate = [];
    const sheetRows = [];

    const total = counties.length;
    const runStart = performance.now();

    console.log("\n===============================");
    console.log("BUILD COUNTIES (SIMULATION 1x1)");
    console.log("===============================");
    console.log(`State: ${chosen.key} (${stateName})`);
    console.log(`Total counties: ${total}`);
    console.log(`Output folder: ${outDir}`);
    console.log("-------------------------------\n");

    const SIMULATE = true;
    const FAIL_RATE = Number(process.env.SIM_FAIL_RATE || "0");

    let ok = 0;
    let failed = 0;

    for (let i = 0; i < total; i++) {
        const tCounty0 = performance.now();

        const c = counties[i];
        const countyName = getCountyName(c);
        const countyDomain = getCountyDomain(c);
        const countySitemap = getCountySitemap(c);
        const timezoneZone = getTimezoneZone(c);
        const stateAbbr = extractStateAbbrFromDomain(countyDomain);

        const accountName = buildAccountName(companyName, countyName, stateName);

        const locationCreateBody = buildLocationCreateBody({
            accountName,
            phone,
            companyId,
            address,
            city,
            country,
            state: stateName,
            postalCode,
            prospectInfo,
            settings,
            social,
            twilio,
            mailgun,
            snapshotId,
            timezoneZone,
            website: countyDomain,
        });

        const dynamicCV = { countyName, countyDomain, countySitemap, timezoneZone, stateName, stateAbbr };
        const customValuesBody = mergeCustomValues(baseCustomValuesJson, dynamicCV);

        const sheetRow = {
            "Account Name": accountName,
            "Location Id": "",
            "Company Id": companyId,
            "County": countyName,
            "State": stateName,
            "Domain": countyDomain,
            "Sitemap": countySitemap,
            "Phone": phone,
            "Domain URL Activation": "",
            "Timezone": timezoneZone,
            "Status": "",
            "Domain Created": "",
        };

        let status = "BUILT";
        let error = "";
        const steps = [];
        let simulationTrace = null;

        try {
            if (SIMULATE) {
                const s1 = await simulateCreateLocation(FAIL_RATE);
                steps.push({ label: s1.label, durationMs: s1.durationMs, locationId: s1.locationId });

                const s2 = await simulateGenerateSubaccountToken(s1.locationId, FAIL_RATE);
                steps.push({ label: s2.label, durationMs: s2.durationMs });

                const s3 = await simulateGetRemoteCustomValues(FAIL_RATE);
                steps.push({ label: s3.label, durationMs: s3.durationMs, remoteCount: s3.remoteCustomValues.length });

                const mapped = mapCustomValuesForUpdate(customValuesBody, s3.remoteCustomValues);

                const putRes = await simulatePutCustomValuesOneByOne(mapped.updates, FAIL_RATE);
                steps.push({
                    label: "put_custom_values_one_by_one",
                    durationMs: putRes.durationMs,
                    ok: putRes.ok,
                    failed: putRes.failed,
                    total: putRes.total,
                });

                const s4 = await simulateStep("append_sheet_row", 80, 220, FAIL_RATE);
                steps.push({ label: s4.label, durationMs: s4.durationMs });

                sheetRow["Location Id"] = s1.locationId;
                sheetRow["Domain URL Activation"] = `https://app.devasks.com/v2/location/${s1.locationId}/settings/domain`;

                ok++;
                status = "BUILT_OK";
                sheetRow["Status"] = status;

                simulationTrace = {
                    locationId: s1.locationId,
                    subaccountToken: s2.token,
                    remoteCustomValuesSample: s3.remoteCustomValues.slice(0, 5),
                    mapSummary: mapped.summary,
                    putSummary: putRes,
                };
            } else {
                ok++;
                status = "BUILT_OK";
                sheetRow["Status"] = status;
            }
        } catch (e) {
            failed++;
            status = "BUILT_FAILED";
            error = String(e?.message || e);
            sheetRow["Status"] = status;
        }

        const countyMs = performance.now() - tCounty0;

        sheetRows.push(sheetRow);

        ghlCreate.push({
            type: "county",
            stateKey: chosen.key,
            stateName,
            countyName,
            stateAbbr,
            body: locationCreateBody,
            customValuesBody,
        });

        preview.push({
            idx: i + 1,
            type: "county",
            stateKey: chosen.key,
            stateName,
            countyName,
            timezoneZone,
            stateAbbr,
            countyDomain,
            countySitemap,
            accountName,
            locationCreateBody,
            customValuesBody,
            sheetRow,
            simulation: {
                status,
                error,
                steps,
                durationMs: countyMs,
                trace: simulationTrace,
            },
        });

        const done = i + 1;
        const elapsed = performance.now() - runStart;
        const avg = elapsed / done;
        const remaining = (total - done) * avg;

        const line =
            `${String(done).padStart(String(total).length, " ")}/${total}` +
            ` | ${pct(done, total)}` +
            ` | ok:${ok} fail:${failed}` +
            ` | ${countyName}` +
            ` | ${status}` +
            ` | item:${formatDuration(countyMs)}` +
            ` | ETA:${formatDuration(remaining)}`;

        console.log(line);

        if (error) console.log(`   ↳ error: ${error}`);

        if (simulationTrace?.mapSummary) {
            console.log(`   ↳ map: matched=${simulationTrace.mapSummary.matched} missing=${simulationTrace.mapSummary.missing}`);
            if (simulationTrace.mapSummary.missing > 0) {
                console.log(`   ↳ missing sample: ${simulationTrace.mapSummary.missingNamesSample.join(", ")}`);
            }
        }
    }

    await fs.writeFile(
        path.join(outDir, `preview-counties-${stamp}.json`),
        JSON.stringify({ stateKey: chosen.key, stateName, count: preview.length, ok, failed, preview }, null, 2),
        "utf8"
    );

    await fs.writeFile(
        path.join(outDir, `ghl-create-counties-${stamp}.json`),
        JSON.stringify({ stateKey: chosen.key, stateName, companyId, snapshotId, count: ghlCreate.length, items: ghlCreate }, null, 2),
        "utf8"
    );

    await fs.writeFile(
        path.join(outDir, `sheets-rows-counties-${stamp}.json`),
        JSON.stringify({ stateKey: chosen.key, stateName, count: sheetRows.length, rows: sheetRows }, null, 2),
        "utf8"
    );

    const totalMs = performance.now() - runStart;

    console.log("\n✅ BUILD COUNTIES DONE");
    console.log(`State: ${chosen.key} (${stateName})`);
    console.log(`Total: ${total} | OK: ${ok} | Failed: ${failed}`);
    console.log(`Elapsed: ${formatDuration(totalMs)}`);
    console.log(`Output: ${outDir}\n`);
}

main().catch((e) => {
    console.error("❌", e?.message || e);
    process.exit(1);
});
