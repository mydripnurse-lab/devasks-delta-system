// scripts/run-create-subaccounts.js
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

import { loadTokens } from "../services/tokenStore.js";
import { ghlFetch } from "../services/ghlClient.js";

import {
    findTwilioAccountByFriendlyName,
    closeTwilioAccount,
} from "../services/twilioClient.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/* =========================
   Helpers: Checkpoints
========================= */

function getCheckpointPath(stateKey) {
    return path.join(
        process.cwd(),
        "scripts/out/checkpoints",
        `${stateKey || "unknown"}.json`
    );
}

async function readCheckpoint(stateKey) {
    try {
        const raw = await fs.readFile(getCheckpointPath(stateKey), "utf8");
        return JSON.parse(raw);
    } catch {
        return { createdByCountyKey: {} };
    }
}

async function writeCheckpoint(stateKey, data) {
    const p = getCheckpointPath(stateKey);
    await fs.mkdir(path.dirname(p), { recursive: true });
    await fs.writeFile(p, JSON.stringify(data, null, 2), "utf8");
}

function countyKey(it) {
    const s = (it?.stateKey || it?.state || "").toLowerCase();
    const c = (it?.countyName || "").toLowerCase();
    const d = (it?.countyDomain || "").toLowerCase();
    return `${s}::${c}::${d}`;
}

/* =========================
   Main
========================= */

const RUN_ID = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
const RUN_STARTED_AT = Date.now();

async function main() {
    const inputPath = process.argv[2];
    const isDryRun = process.argv.includes("--dry-run");
    const resume = !process.argv.includes("--no-resume"); // default ON

    if (!inputPath) {
        console.error(
            "❌ Usage: node scripts/run-create-subaccounts.js <path-to-json> [--dry-run] [--no-resume]"
        );
        process.exit(1);
    }

    await loadTokens();

    const abs = path.isAbsolute(inputPath)
        ? inputPath
        : path.join(process.cwd(), inputPath);

    const raw = await fs.readFile(abs, "utf8");
    const json = JSON.parse(raw);
    const items = json?.items || [];

    console.log(`\n🚀 RUN ID: ${RUN_ID}`);
    console.log(`State: ${json.stateKey} (${json.stateName})`);
    console.log(`Items: ${items.length}`);
    console.log(`Input: ${abs}`);
    console.log(`Mode: ${isDryRun ? "DRY RUN" : "LIVE"}`);
    console.log(`Resume: ${resume ? "ON" : "OFF"}`);
    console.log("--------------------------------------------------\n");

    const checkpoint = await readCheckpoint(json.stateKey);

    for (let i = 0; i < items.length; i++) {
        const it = items[i];
        const label = `${i + 1}/${items.length} | ${it.countyName}`;
        const key = countyKey(it);
        const countyStart = Date.now();

        if (resume && checkpoint.createdByCountyKey[key]?.locationId) {
            console.log(
                `⏭️ ${label} SKIPPED (checkpoint) locationId:`,
                checkpoint.createdByCountyKey[key].locationId
            );
            continue;
        }

        try {
            /* =========================
               1) CREATE GHL LOCATION
            ========================= */

            console.log(`🚀 ${label} creating GHL location...`);

            const created = await ghlFetch("/locations/", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(it.body),
            });

            console.log(`✅ ${label} locationId: ${created?.id}`);
            console.log(`🧠 GHL.name => ${created?.name}`);

            checkpoint.createdByCountyKey[key] = {
                countyName: it.countyName,
                countyDomain: it.countyDomain,
                locationId: created?.id,
                createdName: created?.name,
                runId: RUN_ID,
                createdAt: new Date().toISOString(),
            };

            await writeCheckpoint(json.stateKey, checkpoint);

            /* =========================
               2) TWILIO LOOKUP
            ========================= */

            if (!created?.name) {
                console.log("⚠️ No GHL name, skipping Twilio");
                continue;
            }

            console.log("🔎 Twilio: searching by friendlyName...");

            const twilioAcc = await findTwilioAccountByFriendlyName(created.name, {
                exact: true,
                limit: 200,
            });

            if (!twilioAcc) {
                console.log("⚠️ Twilio: no match found");
                continue;
            }

            console.log("✅ Twilio match:", {
                sid: twilioAcc.sid,
                friendlyName: twilioAcc.friendlyName,
                status: twilioAcc.status,
            });

            /* =========================
               3) CLOSE TWILIO
            ========================= */

            if (isDryRun) {
                console.log("🟡 DRY RUN: Twilio NOT closed:", twilioAcc.sid);
            } else {
                const closed = await closeTwilioAccount(twilioAcc.sid);
                console.log("🧨 Twilio CLOSED:", {
                    sid: closed.sid,
                    status: closed.status,
                });
            }

            /* =========================
               TIMING
            ========================= */

            const countyElapsed = Date.now() - countyStart;
            console.log(
                `⏱️ ${label} completed in ${(countyElapsed / 1000).toFixed(2)}s\n`
            );

        } catch (e) {
            const countyElapsed = Date.now() - countyStart;
            console.error(
                `❌ ${label} FAILED after ${(countyElapsed / 1000).toFixed(2)}s`,
                e?.data || e?.message || e
            );
            continue;
        }
    }

    /* =========================
       TOTAL TIME
    ========================= */

    const elapsedMs = Date.now() - RUN_STARTED_AT;
    const elapsedSec = Math.round(elapsedMs / 1000);
    const elapsedMin = (elapsedSec / 60).toFixed(2);

    console.log("--------------------------------------------------");
    console.log(`⏱️ TOTAL TIME: ${elapsedSec}s (${elapsedMin} min)`);
    console.log("DONE ✅");
}

main().catch((e) => {
    console.error("❌ Fatal:", e);
    process.exit(1);
});
