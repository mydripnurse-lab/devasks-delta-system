import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";

import { createSubAccount } from "../../services/ghlClient.js";
// 👆 si tu create endpoint está en otro service (ej: ghlLocations.js), cámbialo

const OUT_FILE =
    process.env.COUNTIES_OUT_FILE ||
    path.join(process.cwd(), "scripts", "out", "counties.json");

const LOG_DIR = path.join(process.cwd(), "scripts", "logs");
const PROGRESS_FILE = path.join(LOG_DIR, "create-subaccounts-progress.json");

async function ensureDir(p) {
    await fs.mkdir(p, { recursive: true });
}

async function loadJson(filePath) {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw);
}

async function saveJson(filePath, data) {
    await ensureDir(path.dirname(filePath));
    await fs.writeFile(filePath, JSON.stringify(data, null, 2), "utf8");
}

async function loadProgress() {
    try {
        return await loadJson(PROGRESS_FILE);
    } catch {
        return { done: {}, errors: {} };
    }
}

function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}

async function main() {
    await ensureDir(LOG_DIR);

    const counties = await loadJson(OUT_FILE);
    const progress = await loadProgress();

    console.log(`✅ Loaded counties: ${counties.length}`);
    console.log(`📄 OUT_FILE: ${OUT_FILE}`);
    console.log(`🧾 PROGRESS_FILE: ${PROGRESS_FILE}`);

    for (let i = 0; i < counties.length; i++) {
        const county = counties[i];

        // Ajusta este key a tu estructura real:
        const key =
            county?.slug ||
            county?.countySlug ||
            county?.customValuesBody?.customValues?.find((x) => x.name === "countyDomain")
                ?.value ||
            `idx-${i}`;

        if (progress.done[key]) {
            console.log(`⏭️  Skip (already done): ${key}`);
            continue;
        }

        // Tu build genera esto: countyObj.body (según dijiste)
        const body = county.body || county.locationBody || county;

        try {
            console.log(`\n🚀 Creating subaccount ${i + 1}/${counties.length}: ${key}`);
            const created = await createSubAccount(body);

            // Guarda lo importante para pasos siguientes
            progress.done[key] = {
                createdAt: new Date().toISOString(),
                locationId: created?.id || created?.locationId || created?.data?.id,
                raw: created,
            };

            // Limpia errores previos si existían
            delete progress.errors[key];

            await saveJson(PROGRESS_FILE, progress);

            console.log(`✅ Created: ${key} -> locationId=${progress.done[key].locationId}`);

            // Pequeño delay para evitar rate limit
            await sleep(Number(process.env.CREATE_DELAY_MS || 250));
        } catch (e) {
            console.error(`❌ Error creating ${key}:`, e?.message || e);

            progress.errors[key] = {
                at: new Date().toISOString(),
                message: String(e?.message || e),
            };

            await saveJson(PROGRESS_FILE, progress);

            // Si quieres parar al primer error:
            if (process.env.STOP_ON_ERROR === "1") {
                throw e;
            }

            // si no, sigue
        }
    }

    console.log("\n🏁 Finished create-subaccounts run.");
}

main().catch((e) => {
    console.error("FATAL:", e);
    process.exit(1);
});
