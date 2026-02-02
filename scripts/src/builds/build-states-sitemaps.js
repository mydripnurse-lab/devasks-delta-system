// scripts/build-state-sitemaps.js
import fs from "fs/promises";
import path from "path";
import { logHeader, logKV, emitProgressInit, emitProgress, emitProgressEnd } from "../../progress.js";

const DEFAULT_RESOURCES_DIR = path.join(process.cwd(), "resources", "statesFiles");
const DEFAULT_STATES_OUT_DIR = path.join(process.cwd(), "states");
const DEFAULT_SITEMAPS_HOST = "https://sitemaps.mydripnurse.com";

/** yyyy-mm-dd (local) */
function todayYMD() {
    const d = new Date();
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
}

/**
 * Normaliza acentos/diéresis/ñ:
 * "Añasco" => "anasco"
 * "Mayagüez" => "mayaguez"
 * "Peñuelas" => "penuelas"
 */
function latinToAscii(str) {
    return String(str || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "");
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

function renderSitemapIndex({ entries, lastmod }) {
    const parts = [];
    parts.push(`<?xml version="1.0" encoding="UTF-8"?>`);
    parts.push(`<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`);
    parts.push("");

    for (const e of entries) {
        if (!e?.loc) continue;
        if (e.comment) parts.push(`  <!-- ${e.comment} -->`);
        parts.push(`  <sitemap>`);
        parts.push(`    <loc>${e.loc}</loc>`);
        parts.push(`    <lastmod>${lastmod}</lastmod>`);
        parts.push(`  </sitemap>`);
        parts.push("");
    }

    parts.push(`</sitemapindex>`);
    parts.push("");
    return parts.join("\n");
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

function detectStateNameFromJson(stateJson, filenameSlug) {
    return (
        stateJson?.stateName ||
        stateJson?.name ||
        stateJson?.State ||
        (filenameSlug ? filenameSlug.replace(/-/g, " ") : "Unknown")
    );
}

function pickDivisionFolder(stateSlug) {
    if (stateSlug === "louisiana") return "parishes";
    if (stateSlug === "puerto-rico") return "cities";
    return "counties";
}

async function listStateFiles(resourcesDir) {
    const files = await fs.readdir(resourcesDir);
    return files
        .filter((f) => f.toLowerCase().endsWith(".json"))
        .map((f) => ({
            file: f,
            slug: f.replace(/\.json$/i, ""),
            fullPath: path.join(resourcesDir, f),
        }))
        .sort((a, b) => a.slug.localeCompare(b.slug));
}

async function ensureDir(p) {
    await fs.mkdir(p, { recursive: true });
}

async function writeFileEnsureDir(filePath, content) {
    await ensureDir(path.dirname(filePath));
    await fs.writeFile(filePath, content, "utf8");
}

/** Loc builders (host central) */
function locStateDivisionRoot(host, stateSlug, divisionFolder) {
    return `${host}/states/${stateSlug}/${divisionFolder}/sitemap.xml`;
}

function locDivisionIndexChild(host, stateSlug, divisionFolder, divisionSlug) {
    return `${host}/states/${stateSlug}/${divisionFolder}/${divisionSlug}/sitemap.xml`;
}

function locNestedCity(host, stateSlug, divisionFolder, countySlug, citySlug) {
    return `${host}/states/${stateSlug}/${divisionFolder}/${countySlug}/${citySlug}/sitemap.xml`;
}

/** CLI args */
function arg(name, fallback = "") {
    const idx = process.argv.indexOf(name);
    if (idx === -1) return fallback;
    return process.argv[idx + 1] ?? fallback;
}

function parseSelection(inputStr, stateFiles) {
    const v = String(inputStr || "").trim().toLowerCase();
    if (!v) return [];

    if (v === "all" || v === "*") return [...stateFiles];

    // soporta: "1" | "florida" | "1,5,puerto-rico"
    const parts = v
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean);

    const chosen = [];

    for (const p of parts) {
        const asNum = Number(p);
        if (!Number.isNaN(asNum) && asNum >= 1 && asNum <= stateFiles.length) {
            chosen.push(stateFiles[asNum - 1]);
            continue;
        }

        const bySlug =
            stateFiles.find((s) => s.slug === p) ||
            stateFiles.find((s) => s.slug === slugify(p));

        if (bySlug) chosen.push(bySlug);
    }

    // de-dup
    const seen = new Set();
    return chosen.filter((x) => {
        if (seen.has(x.slug)) return false;
        seen.add(x.slug);
        return true;
    });
}

/** Pre-calc totals for progress bar */
function calcTotalsForBatch(batch, jsonMap) {
    // "divisions" = counties/parishes/cities at root
    // "cities" = nested cities inside counties/parishes (PR counts as divisions only)
    let divisions = 0;
    let cities = 0;

    for (const chosen of batch) {
        const stateJson = jsonMap.get(chosen.slug);
        const counties = extractCounties(stateJson);

        const stateSlug = chosen.slug;
        const divisionFolder = pickDivisionFolder(stateSlug);

        if (stateSlug === "puerto-rico" && divisionFolder === "cities") {
            const pr = counties[0];
            const prCities = Array.isArray(pr?.cities) ? pr.cities : [];
            divisions += prCities.filter((c) => c?.cityName && c?.citySitemap).length;
            continue;
        }

        // normal/louisiana
        const filteredCounties = counties.filter((c) => c?.countyName);
        divisions += filteredCounties.length;

        for (const c of filteredCounties) {
            const list = Array.isArray(c?.cities) ? c.cities : [];
            cities += list.filter((x) => x?.cityName && x?.citySitemap).length;
        }
    }

    const all = divisions + cities;
    return { all, divisions, cities };
}

async function buildOneState({
    chosen,
    lastmod,
    host,
    outDir,
    stateJson,
    // progress bookkeeping:
    onDivisionDone,
    onCityDone,
}) {
    const stateSlug = chosen.slug;
    const stateName = detectStateNameFromJson(stateJson, stateSlug);
    const divisionFolder = pickDivisionFolder(stateSlug);

    const counties = extractCounties(stateJson);

    const outStateDir = path.join(outDir, stateSlug);
    const outDivisionRootDir = path.join(outStateDir, divisionFolder);

    console.log("\n===============================================");
    console.log("State:", stateName);
    console.log("State slug:", stateSlug);
    console.log("Input JSON:", chosen.fullPath);
    console.log("Output dir:", outStateDir);
    console.log("Folder type:", divisionFolder);
    console.log("Lastmod:", lastmod);
    console.log("Total county objects:", counties.length);
    console.log("===============================================\n");

    await ensureDir(outStateDir);
    await ensureDir(outDivisionRootDir);

    // 1) STATE sitemap.xml
    const stateSitemapXml = renderSitemapIndex({
        lastmod,
        entries: [
            {
                comment: `${stateName} Main Page`,
                loc: `https://${stateSlug}.mydripnurse.com/sitemap.xml`,
            },
            {
                comment:
                    divisionFolder === "parishes"
                        ? `${stateName} Parishes`
                        : divisionFolder === "cities"
                            ? `${stateName} Cities`
                            : `${stateName} Counties`,
                loc: locStateDivisionRoot(host, stateSlug, divisionFolder),
            },
        ],
    });

    await writeFileEnsureDir(path.join(outStateDir, "sitemap.xml"), stateSitemapXml);

    /**
     * 2) division root sitemap.xml
     * - PR: lista cities directas
     * - Normal: lista counties/parishes
     */
    let divisionRootEntries = [];

    if (stateSlug === "puerto-rico" && divisionFolder === "cities") {
        const pr = counties[0];
        const cities = Array.isArray(pr?.cities) ? pr.cities : [];

        divisionRootEntries = cities
            .filter((c) => c?.cityName)
            .map((c) => ({
                loc: locDivisionIndexChild(host, stateSlug, divisionFolder, slugify(c.cityName)),
            }));
    } else {
        divisionRootEntries = counties
            .filter((c) => c?.countyName)
            .map((c) => ({
                loc: locDivisionIndexChild(host, stateSlug, divisionFolder, slugify(c.countyName)),
            }));
    }

    const divisionRootXml = renderSitemapIndex({ lastmod, entries: divisionRootEntries });
    await writeFileEnsureDir(path.join(outDivisionRootDir, "sitemap.xml"), divisionRootXml);

    /**
     * 3) Build folders + sitemaps
     */
    let ok = 0;
    let failed = 0;

    // 3A) Puerto Rico direct cities
    if (stateSlug === "puerto-rico" && divisionFolder === "cities") {
        const pr = counties[0];
        const cities = Array.isArray(pr?.cities) ? pr.cities : [];

        for (const city of cities) {
            const cityName = city?.cityName;
            const citySitemapUrl = city?.citySitemap;
            if (!cityName || !citySitemapUrl) continue;

            try {
                const citySlug = slugify(cityName);
                const cityDir = path.join(outDivisionRootDir, citySlug);
                const cityFile = path.join(cityDir, "sitemap.xml");

                const xml = renderSitemapIndex({
                    lastmod,
                    entries: [{ comment: `${cityName} Main Sitemap`, loc: String(citySitemapUrl).trim() }],
                });

                await writeFileEnsureDir(cityFile, xml);
                ok++;

                onDivisionDone?.({
                    stateSlug,
                    divisionFolder,
                    divisionName: cityName,
                    divisionSlug: citySlug,
                    kind: "pr-city",
                });
            } catch (e) {
                failed++;
                console.error(`❌ Failed PR city "${cityName}":`, e?.message || e);
            }
        }

        console.log(`\n✅ DONE ${stateSlug} | cities ok:${ok} fail:${failed}\n`);
        return { ok, failed };
    }

    // 3B) Normal/Louisiana: county/parish folders with nested city folders
    for (const c of counties) {
        const countyName = c?.countyName;
        if (!countyName) continue;

        const countySlug = slugify(countyName);
        const countyDir = path.join(outDivisionRootDir, countySlug);
        const countyFile = path.join(countyDir, "sitemap.xml");

        try {
            const countySitemapUrl = String(c?.countySitemap || "").trim();
            const cities = Array.isArray(c?.cities) ? c.cities : [];

            // 1) Crear sitemap.xml de cada city dentro del county folder
            for (const city of cities) {
                const cityName = city?.cityName;
                const citySitemapUrl = city?.citySitemap;
                if (!cityName || !citySitemapUrl) continue;

                const citySlug = slugify(cityName);
                const cityDir = path.join(countyDir, citySlug);
                const cityFile = path.join(cityDir, "sitemap.xml");

                const cityHostedXml = renderSitemapIndex({
                    lastmod,
                    entries: [{ comment: `${cityName} Main Sitemap`, loc: String(citySitemapUrl).trim() }],
                });

                await writeFileEnsureDir(cityFile, cityHostedXml);

                onCityDone?.({
                    stateSlug,
                    countySlug,
                    countyName,
                    citySlug,
                    cityName,
                    divisionFolder,
                });
            }

            // 2) County sitemap.xml index
            const entries = [];

            if (countySitemapUrl) {
                entries.push({ comment: `${countyName} Main Sitemap`, loc: countySitemapUrl });
            }

            for (const city of cities) {
                if (!city?.cityName) continue;
                const citySlug = slugify(city.cityName);
                entries.push({
                    comment: `${city.cityName} Hosted Sitemap`,
                    loc: locNestedCity(host, stateSlug, divisionFolder, countySlug, citySlug),
                });
            }

            const countyHostedXml = renderSitemapIndex({ lastmod, entries });
            await writeFileEnsureDir(countyFile, countyHostedXml);

            ok++;

            onDivisionDone?.({
                stateSlug,
                divisionFolder,
                divisionName: countyName,
                divisionSlug: countySlug,
                kind: divisionFolder === "parishes" ? "parish" : "county",
            });
        } catch (e) {
            failed++;
            console.error(`❌ Failed county/parish "${countyName}":`, e?.message || e);
        }
    }

    console.log(`\n✅ DONE ${stateSlug} | divisions ok:${ok} fail:${failed}\n`);
    return { ok, failed };
}

async function main() {
    const job = "build-state-sitemaps";

    const stateArg = arg("--state", "all"); // all | slug | "1,5,puerto-rico"
    const resourcesDir = arg("--resourcesDir", DEFAULT_RESOURCES_DIR);
    const outDir = arg("--outDir", DEFAULT_STATES_OUT_DIR);
    const host = arg("--host", DEFAULT_SITEMAPS_HOST);
    const debug = arg("--debug", "off") === "on";

    const lastmod = todayYMD();

    logHeader(`BUILD STATE SITEMAPS • state="${stateArg}" • host="${host}"`);
    logKV({ resourcesDir, outDir, lastmod, debug });

    const stateFiles = await listStateFiles(resourcesDir);
    if (!stateFiles.length) {
        console.error("❌ No JSON files found in:", resourcesDir);
        process.exit(1);
    }

    const batch = parseSelection(stateArg, stateFiles);
    if (!batch.length) {
        console.error(`❌ No matches for --state="${stateArg}". Available: ${stateFiles.length} files.`);
        process.exit(1);
    }

    // Load JSONs upfront (so totals are accurate and no partial progress)
    const jsonMap = new Map();
    for (const chosen of batch) {
        const raw = await fs.readFile(chosen.fullPath, "utf8");
        jsonMap.set(chosen.slug, JSON.parse(raw));
    }

    const totals = calcTotalsForBatch(batch, jsonMap);
    emitProgressInit({
        totals: { all: totals.all, counties: totals.divisions, cities: totals.cities },
        job,
        state: stateArg,
    });

    let doneDivisions = 0;
    let doneCities = 0;
    let okStates = 0;
    let failedStates = 0;

    const onDivisionDone = (info) => {
        doneDivisions++;

        emitProgress({
            totals: { all: totals.all, counties: totals.divisions, cities: totals.cities },
            done: { all: doneDivisions + doneCities, counties: doneDivisions, cities: doneCities },
            last: {
                kind: info.kind === "pr-city" ? "city" : "county",
                state: info.stateSlug,
                action: `sitemap_written`,
                name: info.divisionName,
            },
        });

        if (debug) console.log(`✅ Division: ${info.divisionName} (${doneDivisions}/${totals.divisions})`);
    };

    const onCityDone = (info) => {
        doneCities++;

        // Opcional: no spamear tanto si hay miles de ciudades.
        // Puedes throttle aquí, pero por ahora lo dejamos.
        emitProgress({
            totals: { all: totals.all, counties: totals.divisions, cities: totals.cities },
            done: { all: doneDivisions + doneCities, counties: doneDivisions, cities: doneCities },
            last: {
                kind: "city",
                state: info.stateSlug,
                county: info.countyName,
                action: `city_sitemap_written`,
                name: info.cityName,
            },
        });

        if (debug) console.log(`🏙️ City: ${info.cityName} (${doneCities}/${totals.cities})`);
    };

    // Build sequentially (safe)
    for (const chosen of batch) {
        try {
            const stateJson = jsonMap.get(chosen.slug);
            await buildOneState({
                chosen,
                lastmod,
                host,
                outDir,
                stateJson,
                onDivisionDone,
                onCityDone,
            });
            okStates++;
        } catch (e) {
            failedStates++;
            console.error(`❌ Fatal building "${chosen.slug}":`, e?.message || e);
        }
    }

    const ok = failedStates === 0;
    emitProgressEnd({
        totals: { all: totals.all, counties: totals.divisions, cities: totals.cities },
        done: { all: doneDivisions + doneCities, counties: doneDivisions, cities: doneCities },
        ok,
    });

    console.log(`\n🏁 ${job} finished • states_ok=${okStates} • states_fail=${failedStates}\n`);
    process.exit(ok ? 0 : 1);
}

main().catch((e) => {
    console.error("❌ Fatal:", e?.message || e);
    try {
        emitProgressEnd({
            totals: { all: 0, counties: 0, cities: 0 },
            done: { all: 0, counties: 0, cities: 0 },
            ok: false,
        });
    } catch { }
    process.exit(1);
});
