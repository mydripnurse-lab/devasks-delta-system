// scripts/src/builds/build-states-sitemaps.js
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ---- paths
const RESOURCES_DIR = path.join(process.cwd(), "resources", "statesFiles");
const STATES_OUT_DIR = path.join(process.cwd(), "states");

// Host central donde se sirven estos sitemaps
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

// --------------------
// ✅ Robust argv parser: supports "--k v" AND "--k=v"
function argValue(key, fallback = null) {
    const argv = process.argv.slice(2);
    const direct = `--${key}=`;
    for (let i = 0; i < argv.length; i++) {
        const a = String(argv[i] ?? "");
        if (a === `--${key}`) {
            const next = argv[i + 1];
            if (next && !String(next).startsWith("--")) return String(next);
            return fallback;
        }
        if (a.startsWith(direct)) return a.slice(direct.length);
    }
    return fallback;
}

function argBool(key, fallback = false) {
    const v = argValue(key, null);
    if (v === null) return fallback;
    const t = String(v).trim().toLowerCase();
    return t === "1" || t === "true" || t === "yes" || t === "y" || t === "on";
}

function ts() {
    const d = new Date();
    const hh = String(d.getHours()).padStart(2, "0");
    const mm = String(d.getMinutes()).padStart(2, "0");
    const ss = String(d.getSeconds()).padStart(2, "0");
    return `[${hh}:${mm}:${ss}]`;
}

function log(line) {
    console.log(`${ts()} ${line}`);
}

function renderSitemapIndex({ entries, lastmod }) {
    const parts = [];
    parts.push(`<?xml version="1.0" encoding="UTF-8"?>`);
    parts.push(
        `<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`
    );
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

async function listStateFiles() {
    const files = await fs.readdir(RESOURCES_DIR);
    return files
        .filter((f) => f.toLowerCase().endsWith(".json"))
        .map((f) => ({
            file: f,
            slug: f.replace(/\.json$/i, ""),
            fullPath: path.join(RESOURCES_DIR, f),
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

async function buildOneState(chosen, lastmod, host, debug) {
    const raw = await fs.readFile(chosen.fullPath, "utf8");
    const stateJson = JSON.parse(raw);

    const stateSlug = chosen.slug;
    const stateName = detectStateNameFromJson(stateJson, stateSlug);
    const divisionFolder = pickDivisionFolder(stateSlug);

    const counties = extractCounties(stateJson);

    const outStateDir = path.join(STATES_OUT_DIR, stateSlug);
    const outDivisionRootDir = path.join(outStateDir, divisionFolder);

    log("================================================");
    log(`State: ${stateName}`);
    log(`State slug: ${stateSlug}`);
    log(`Input JSON: ${chosen.fullPath}`);
    log(`Output dir: ${outStateDir}`);
    log(`Folder type: ${divisionFolder}`);
    log(`Lastmod: ${lastmod}`);
    log(`Total county objects: ${counties.length}`);
    log("================================================");

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

    // 2) division root sitemap.xml
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

    const divisionRootXml = renderSitemapIndex({
        lastmod,
        entries: divisionRootEntries,
    });

    await writeFileEnsureDir(path.join(outDivisionRootDir, "sitemap.xml"), divisionRootXml);

    // 3) Build folders + sitemaps
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
            } catch (e) {
                failed++;
                log(`❌ Failed PR city "${cityName}": ${e?.message || e}`);
            }
        }

        log(`✅ DONE ${stateSlug} | cities ok:${ok} fail:${failed}`);
        return;
    }

    // 3B) Normal/Louisiana
    for (const c of counties) {
        const countyName = c?.countyName;
        if (!countyName) continue;

        const countySlug = slugify(countyName);
        const countyDir = path.join(outDivisionRootDir, countySlug);
        const countyFile = path.join(countyDir, "sitemap.xml");

        try {
            const countySitemapUrl = String(c?.countySitemap || "").trim();
            const cities = Array.isArray(c?.cities) ? c.cities : [];

            // 1) city sitemaps inside county folder
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
            }

            // 2) County sitemap index
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

            if (debug && ok % 25 === 0) {
                log(`…progress ${stateSlug}: ${ok} divisions built`);
            }
        } catch (e) {
            failed++;
            log(`❌ Failed county/parish "${countyName}": ${e?.message || e}`);
        }
    }

    log(`✅ DONE ${stateSlug} | divisions ok:${ok} fail:${failed}`);
}

async function main() {
    const stateArgRaw = argValue("state", "all");
    const state = slugify(stateArgRaw || "all");
    const host = String(argValue("host", DEFAULT_SITEMAPS_HOST) || DEFAULT_SITEMAPS_HOST).trim();
    const debug = argBool("debug", false);

    const lastmod = todayYMD();

    log("------------------------------------------------");
    log(`BUILD STATE SITEMAPS • state="${state}" • host="${host}"`);
    log(`• resourcesDir: ${RESOURCES_DIR}`);
    log(`• outDir: ${STATES_OUT_DIR}`);
    log(`• lastmod: ${lastmod}`);
    log(`• debug: ${debug}`);
    log("------------------------------------------------");

    const stateFiles = await listStateFiles();
    if (!stateFiles.length) {
        log(`❌ No JSON files found in: ${RESOURCES_DIR}`);
        process.exit(1);
    }

    let batch = [];

    if (state === "all" || state === "*") {
        batch = stateFiles;
    } else {
        const found =
            stateFiles.find((s) => s.slug === state) ||
            stateFiles.find((s) => s.slug === slugify(state));

        if (!found) {
            log(`❌ State not found: "${stateArgRaw}". Expected a slug like "alabama".`);
            process.exit(1);
        }

        batch = [found];
    }

    // sequential build
    for (const chosen of batch) {
        try {
            await buildOneState(chosen, lastmod, host, debug);
        } catch (e) {
            log(`❌ Fatal building "${chosen.slug}": ${e?.message || e}`);
        }
    }

    log(`🏁 DONE batch (${batch.length} state(s))`);
}

main().catch((e) => {
    console.error(`${ts()} ❌ Fatal:`, e?.message || e);
    process.exit(1);
});
