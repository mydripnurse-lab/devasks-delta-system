// scripts/src/build-states-index.js
import fs from "fs/promises";
import path from "path";

const STATES_FILES_DIR = path.join(process.cwd(), "resources", "statesFiles");
const OUT_DIR = path.join(process.cwd(), "public", "json");
const OUT_FILE = path.join(OUT_DIR, "states-index.json");

// ✅ Folder que contiene los estados generados (alaska, puerto-rico, etc.)
const GENERATED_STATES_DIR = path.join(process.cwd(), "scripts", "out");

// Si lo vas a servir desde Netlify, pon el BASE URL público.
const BASE_URL = process.env.SITEMAPS_BASE_URL || "https://sitemaps.mydripnurse.com";

/**
 * Args soportados (para UI / API):
 *  --state=all | "Alabama" | "alabama" | "Alabama,Florida"
 *  --slug=alabama | "puerto-rico"
 *  --file=alabama.json
 *
 * Precedencia:
 *  file > slug > state
 */
function getArgValue(name) {
    const prefix = `--${name}=`;
    const hit = process.argv.find((a) => a.startsWith(prefix));
    return hit ? hit.slice(prefix.length) : null;
}

function slugifyFolderName(input) {
    return String(input || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "") // diacríticos
        .toLowerCase()
        .trim()
        .replace(/&/g, "and")
        .replace(/[^a-z0-9\s-]/g, "")
        .replace(/\s+/g, "-")
        .replace(/-+/g, "-");
}

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
        console.warn(`⚠️ No pude leer ${dirPath}. Error:`, e?.message || e);
        return new Set();
    }
}

function normalizeListArg(v) {
    // "Alabama, Florida" -> ["alabama", "florida"] (como slugs)
    return String(v || "")
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean);
}

async function main() {
    await fs.mkdir(OUT_DIR, { recursive: true });

    const generatedSlugs = await listGeneratedStateSlugs(GENERATED_STATES_DIR);
    if (generatedSlugs.size === 0) {
        console.warn(`⚠️ No hay folders en ${GENERATED_STATES_DIR}. El index saldrá vacío.`);
    }

    const files = await fs.readdir(STATES_FILES_DIR);
    const jsonFiles = files.filter((f) => f.endsWith(".json"));

    // ----------------------------
    // ✅ Targeting (ALL vs ONE)
    // ----------------------------
    const fileArg = getArgValue("file"); // e.g. alabama.json
    const slugArg = getArgValue("slug"); // e.g. alabama
    const stateArg = getArgValue("state"); // e.g. Alabama | all | Alabama,Florida

    let targetFilesSet = null; // Set<string> of filenames
    let targetSlugsSet = null; // Set<string> of slugs

    if (fileArg && String(fileArg).trim()) {
        const f = String(fileArg).trim();
        targetFilesSet = new Set([f.endsWith(".json") ? f : `${f}.json`]);
    } else if (slugArg && String(slugArg).trim()) {
        // single or comma list
        const slugs = normalizeListArg(slugArg).map(slugifyFolderName);
        targetSlugsSet = new Set(slugs);
    } else if (stateArg && String(stateArg).trim() && String(stateArg).trim().toLowerCase() !== "all") {
        // could be a single state name OR comma list
        const items = normalizeListArg(stateArg);

        // We'll treat each item as either:
        // - already a slug (contains "-" or is lowercase) OR
        // - a stateName -> slugify
        const slugs = items.map((x) => slugifyFolderName(x));
        targetSlugsSet = new Set(slugs);
    }

    // ----------------------------
    // Build index
    // ----------------------------
    const states = [];

    for (const file of jsonFiles) {
        // Optional filter by file name
        if (targetFilesSet && !targetFilesSet.has(file)) continue;

        const full = path.join(STATES_FILES_DIR, file);
        const raw = await fs.readFile(full, "utf8");
        const parsed = JSON.parse(raw);

        const stateName = parsed.stateName || file.replace(".json", "");
        const stateSlug = slugifyFolderName(stateName);

        // Optional filter by slug(s)
        if (targetSlugsSet && !targetSlugsSet.has(stateSlug)) continue;

        // ✅ FILTRO CLAVE: solo si existe el folder en scripts/out/<stateSlug>
        if (!generatedSlugs.has(stateSlug)) continue;

        const stateJsonUrl = `${BASE_URL}/resources/statesFiles/${file}`;

        states.push({
            stateName,
            stateSlug,
            // ✅ compat con tu UI
            stateJsonUrl,
            stateFileUrl: stateJsonUrl,
            url: stateJsonUrl,
        });
    }

    states.sort((a, b) => a.stateName.localeCompare(b.stateName));

    const out = {
        generatedAt: new Date().toISOString(),
        baseUrl: BASE_URL,
        states,
        includedOnlyIfFolderExistsIn: "scripts/out/<stateSlug>",
        // 🔍 debug útil para UI / logs
        filters: {
            state: stateArg || "all",
            slug: slugArg || "",
            file: fileArg || "",
        },
    };

    // Si pidieron un estado específico y no salió ninguno, mejor dar warning claro (sin romper)
    if ((targetFilesSet || targetSlugsSet) && states.length === 0) {
        console.warn(
            "⚠️ No se incluyó ningún estado con los filtros. " +
            "Verifica que: (1) exista el JSON en resources/statesFiles y (2) exista el folder en scripts/out/<stateSlug>."
        );
    }

    await fs.writeFile(OUT_FILE, JSON.stringify(out, null, 2), "utf8");
    console.log("✅ Generated:", OUT_FILE);
    console.log("States included:", states.length);
}

main().catch((e) => {
    console.error("❌ build-states-index failed:", e);
    process.exit(1);
});
