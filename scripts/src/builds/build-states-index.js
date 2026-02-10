// scripts/src/build-states-index.js
import fs from "fs/promises";
import path from "path";

// -------------------- Paths --------------------
const STATES_FILES_DIR = path.join(process.cwd(), "resources", "statesFiles");
const OUT_DIR = path.join(process.cwd(), "public", "json");
const OUT_FILE = path.join(OUT_DIR, "states-index.json");

// ✅ Folder que contiene los estados generados (alaska, puerto-rico, etc.)
const GENERATED_STATES_DIR = path.join(process.cwd(), "scripts", "out");

// Si lo vas a servir desde Netlify, pon el BASE URL público.
const BASE_URL = process.env.SITEMAPS_BASE_URL || "https://sitemaps.mydripnurse.com";

// -------------------- Helpers --------------------
function tsLocal() {
    const d = new Date();
    const hh = String(d.getHours()).padStart(2, "0");
    const mm = String(d.getMinutes()).padStart(2, "0");
    const ss = String(d.getSeconds()).padStart(2, "0");
    return `[${hh}:${mm}:${ss}]`;
}

function log(line) {
    console.log(`${tsLocal()} ${line}`);
}

function latinToAscii(str) {
    return String(str || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}

function slugifyFolderName(input) {
    return latinToAscii(input)
        .toLowerCase()
        .trim()
        .replace(/&/g, "and")
        .replace(/[^a-z0-9\s-]/g, "")
        .replace(/\s+/g, "-")
        .replace(/-+/g, "-")
        .replace(/^-|-$/g, "");
}

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

function normalizeListArg(v) {
    // "Alabama, Florida" -> ["Alabama","Florida"]
    return String(v || "")
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean);
}

// -------------------- Progress (SSE-friendly) --------------------
function emitProgressInit({ totals, message }) {
    const payload = {
        totals: {
            all: totals?.all ?? 0,
            counties: totals?.counties ?? 0,
            cities: totals?.cities ?? 0,
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
        pct: totalAll > 0 ? Math.max(0, Math.min(1, doneAll / totalAll)) : 1,
        ok: !!ok,
        error: error || null,
        last: { kind: "state", state: "", action: "end" },
    };

    console.log(`__PROGRESS_END__ ${JSON.stringify(payload)}`);
}

// -------------------- FS --------------------
async function listGeneratedStateSlugs(dirPath) {
    try {
        const entries = await fs.readdir(dirPath, { withFileTypes: true });
        return new Set(
            entries
                .filter((e) => e.isDirectory())
                .map((e) => e.name)
                .filter(Boolean)
        );
    } catch (e) {
        log(`⚠️ No pude leer ${dirPath}. Error: ${e?.message || e}`);
        return new Set();
    }
}

// -------------------- Main --------------------
async function main() {
    const debug = argBool("debug", false);

    // Args soportados (para UI / API):
    //  --state=all | "Alabama" | "alabama" | "Alabama,Florida"
    //  --slug=alabama | "puerto-rico" | "alabama,florida"
    //  --file=alabama.json | "alabama"
    //
    // Precedencia:
    //  file > slug > state
    const fileArgRaw = argValue("file", "");
    const slugArgRaw = argValue("slug", "");
    const stateArgRaw = argValue("state", "all");

    // Build targeting
    let targetFilesSet = null; // Set<string> of filenames
    let targetSlugsSet = null; // Set<string> of slugs

    if (fileArgRaw && String(fileArgRaw).trim()) {
        const f = String(fileArgRaw).trim();
        const f2 = f.toLowerCase().endsWith(".json") ? f : `${f}.json`;
        targetFilesSet = new Set([f2]);
    } else if (slugArgRaw && String(slugArgRaw).trim()) {
        const slugs = normalizeListArg(slugArgRaw).map(slugifyFolderName);
        targetSlugsSet = new Set(slugs);
    } else if (
        stateArgRaw &&
        String(stateArgRaw).trim() &&
        String(stateArgRaw).trim().toLowerCase() !== "all" &&
        String(stateArgRaw).trim() !== "*"
    ) {
        const items = normalizeListArg(stateArgRaw);
        const slugs = items.map((x) => slugifyFolderName(x));
        targetSlugsSet = new Set(slugs);
    }

    await fs.mkdir(OUT_DIR, { recursive: true });

    const generatedSlugs = await listGeneratedStateSlugs(GENERATED_STATES_DIR);

    // Load files list
    const files = await fs.readdir(STATES_FILES_DIR);
    const jsonFiles = files.filter((f) => f.toLowerCase().endsWith(".json"));

    // Compute scope for progress totals: number of candidate files after filters (soft)
    // We'll count a state as "done" after it is evaluated (even if skipped by generatedSlugs filter),
    // so progress matches actual work.
    let scopeFiles = jsonFiles;

    if (targetFilesSet) {
        scopeFiles = scopeFiles.filter((f) => targetFilesSet.has(f));
    } else if (targetSlugsSet) {
        scopeFiles = scopeFiles.filter((f) => {
            const slug = slugifyFolderName(f.replace(/\.json$/i, ""));
            return targetSlugsSet.has(slug);
        });
    } else {
        // state=all -> keep as is
        // state="*" -> same
        // no filters -> keep as is
    }

    // Init banner logs
    log("------------------------------------------------");
    log(`BUILD STATES INDEX • state="${stateArgRaw}" slug="${slugArgRaw}" file="${fileArgRaw}"`);
    log(`• resourcesDir: ${STATES_FILES_DIR}`);
    log(`• generatedDir: ${GENERATED_STATES_DIR}`);
    log(`• outFile: ${OUT_FILE}`);
    log(`• baseUrl: ${BASE_URL}`);
    log(`• debug: ${debug}`);
    log(`• files in scope: ${scopeFiles.length}`);
    log("------------------------------------------------");

    // Init progress
    const totals = { all: scopeFiles.length, counties: 0, cities: 0 };
    const done = { all: 0, counties: 0, cities: 0 };

    emitProgressInit({
        totals,
        message: `Building states-index (${scopeFiles.length} file(s))`,
    });

    const states = [];

    // Process sequentially
    for (const file of scopeFiles) {
        done.all += 1;

        // Optional filter by exact file name (already applied above, but safe)
        if (targetFilesSet && !targetFilesSet.has(file)) {
            emitProgress({
                totals,
                done,
                last: { kind: "state", state: "", action: `skip-file(${file})` },
                message: "Skipping (file filter)",
            });
            continue;
        }

        const full = path.join(STATES_FILES_DIR, file);

        try {
            const raw = await fs.readFile(full, "utf8");
            const parsed = JSON.parse(raw);

            const stateName = parsed.stateName || file.replace(/\.json$/i, "");
            const stateSlug = slugifyFolderName(stateName);

            // Optional filter by slug(s)
            if (targetSlugsSet && !targetSlugsSet.has(stateSlug)) {
                emitProgress({
                    totals,
                    done,
                    last: { kind: "state", state: stateSlug, action: "skip(slug filter)" },
                    message: `Skipping ${stateSlug} (slug filter)`,
                });
                continue;
            }

            // ✅ FILTRO CLAVE: solo si existe folder en scripts/out/<stateSlug>
            if (!generatedSlugs.has(stateSlug)) {
                emitProgress({
                    totals,
                    done,
                    last: { kind: "state", state: stateSlug, action: "skip(no generated folder)" },
                    message: `Skipping ${stateSlug} (no folder in scripts/out)`,
                });
                if (debug) log(`⚠ skip ${stateSlug}: no folder in scripts/out`);
                continue;
            }

            const stateJsonUrl = `${BASE_URL}/resources/statesFiles/${file}`;

            states.push({
                stateName,
                stateSlug,
                // ✅ compat con tu UI
                stateJsonUrl,
                stateFileUrl: stateJsonUrl,
                url: stateJsonUrl,
            });

            emitProgress({
                totals,
                done,
                last: { kind: "state", state: stateSlug, action: "included" },
                message: `Included ${stateSlug}`,
            });

            if (debug) log(`✅ included: ${stateSlug}`);
        } catch (e) {
            emitProgress({
                totals,
                done,
                last: { kind: "state", state: file, action: "error" },
                message: `Error parsing ${file}`,
            });
            log(`❌ Failed processing "${file}": ${e?.message || e}`);
        }
    }

    states.sort((a, b) => a.stateName.localeCompare(b.stateName));

    const out = {
        generatedAt: new Date().toISOString(),
        baseUrl: BASE_URL,
        states,
        includedOnlyIfFolderExistsIn: "scripts/out/<stateSlug>",
        filters: {
            state: stateArgRaw || "all",
            slug: slugArgRaw || "",
            file: fileArgRaw || "",
        },
    };

    // Warning claro si pidieron filtro específico y no salió nada
    const usedFilters = !!(targetFilesSet || targetSlugsSet);
    if (usedFilters && states.length === 0) {
        log(
            "⚠️ No se incluyó ningún estado con los filtros. " +
            "Verifica: (1) JSON en resources/statesFiles (2) folder en scripts/out/<stateSlug>."
        );
    }

    await fs.writeFile(OUT_FILE, JSON.stringify(out, null, 2), "utf8");

    log(`✅ Generated: ${OUT_FILE}`);
    log(`✅ States included: ${states.length} / scope=${scopeFiles.length}`);

    emitProgressEnd({ totals, done, ok: true });
}

main().catch((e) => {
    console.error(`${tsLocal()} ❌ build-states-index failed:`, e?.message || e);
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
