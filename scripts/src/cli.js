import "dotenv/config";
import inquirer from "inquirer";
import path from "node:path";
import { ensureDirs, listStateJsonFiles, readJson, writeJson, PATHS } from "./io.js";
import { makeGhlClient, createSubaccount } from "./ghl.js";
import { appendRow } from "./sheets.js";

function nowISO() {
    return new Date().toISOString();
}

function pickCountiesArray(data) {
    // ✅ Ajustaremos al 100% cuando me confirmes la estructura exacta.
    // Por ahora: intenta varias llaves comunes.
    if (Array.isArray(data.counties)) return data.counties;
    if (Array.isArray(data.items)) return data.items;
    if (Array.isArray(data.data)) return data.data;

    // fallback: buscar el primer array grande de objetos dentro del JSON
    for (const k of Object.keys(data || {})) {
        if (Array.isArray(data[k]) && typeof data[k]?.[0] === "object") return data[k];
    }
    return [];
}

function countyNameFromItem(item, idx) {
    return String(item?.county ?? item?.name ?? item?.countyName ?? `county_${idx + 1}`).trim();
}

function buildSubaccountPayload(stateKey, countyItem, countyName) {
    // ⚠️ payload placeholder: lo ajustamos cuando confirmes endpoint y campos obligatorios de GHL
    return {
        name: `MDN - ${stateKey.toUpperCase()} - ${countyName}`,
        // aquí luego meteremos phone, address, timezone, etc si GHL lo requiere
        meta: {
            state: stateKey,
            county: countyName,
        },
    };
}

async function main() {
    ensureDirs();

    const files = listStateJsonFiles();

    const { statePick } = await inquirer.prompt([
        {
            type: "list",
            name: "statePick",
            message: "Selecciona el STATE (JSON) a procesar:",
            choices: files.map((f) => ({ name: f.file, value: f })),
            pageSize: 20,
        },
    ]);

    const stateKey = statePick.key;
    const data = readJson(statePick.fullPath);

    const countiesArr = pickCountiesArray(data);
    if (!countiesArr.length) {
        console.log("No pude detectar el array de counties en este JSON.");
        console.log("Pégame aquí un pedazo de scripts/out/sample.json para ajustar el detector.");
        return;
    }

    const dryRun = String(process.env.DRY_RUN ?? "true").toLowerCase() === "true";
    console.log(`\nSTATE: ${stateKey} | Counties detectados: ${countiesArr.length} | DRY_RUN=${dryRun}\n`);

    const client = makeGhlClient();

    const results = [];
    for (let i = 0; i < countiesArr.length; i++) {
        const countyItem = countiesArr[i];
        const countyName = countyNameFromItem(countyItem, i);
        const payload = buildSubaccountPayload(stateKey, countyItem, countyName);

        console.log(`[${i + 1}/${countiesArr.length}] Procesando: ${countyName}`);

        try {
            const res = dryRun
                ? { dryRun: true, payload }
                : await createSubaccount(client, payload);

            // ✅ Log a Google Sheets (si lo tienes configurado)
            // Si aún no tienes credenciales, comenta estas 2 líneas por ahora.
            await appendRow([
                nowISO(),
                stateKey,
                countyName,
                dryRun ? "DRY_RUN" : "CREATED",
                JSON.stringify(res).slice(0, 45000),
            ]);

            results.push({ ok: true, countyName, res });
            console.log(`  ✅ OK: ${countyName}`);
        } catch (e) {
            const msg = e?.message || String(e);

            // log error sheet
            await appendRow([nowISO(), stateKey, countyName, "ERROR", msg]);

            results.push({ ok: false, countyName, error: msg });
            console.log(`  ❌ ERROR: ${countyName}`);
            console.log(`     ${msg}`);
        }
    }

    const outPath = path.join(PATHS.outDir, `run-${stateKey}.json`);
    writeJson(outPath, { state: stateKey, at: nowISO(), dryRun, total: countiesArr.length, results });

    console.log(`\nListo. Reporte guardado en: ${outPath}`);
}

main().catch((e) => {
    console.error(e?.message || e);
    process.exit(1);
});
