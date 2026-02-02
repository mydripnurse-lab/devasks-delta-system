// scripts/run-job.js
import { spawn } from "child_process";

function arg(name, fallback = "") {
    const idx = process.argv.indexOf(name);
    if (idx === -1) return fallback;
    return process.argv[idx + 1] ?? fallback;
}

const job = arg("--job", "");
const state = arg("--state", "all");
const mode = arg("--mode", "dry"); // no lo usa el sitemap builder, pero lo aceptamos
const debug = arg("--debug", "off");

if (!job) {
    console.log("❌ Missing --job");
    process.exit(1);
}

const map = {
    "build-state-sitemaps": ["scripts/build-state-sitemaps.js"],
    // agrega los demás:
    // "build-counties": ["scripts/build-counties.js"],
    // "build-sheet-rows": ["scripts/build-sheets-counties-cities.js"],
};

const entry = map[job];
if (!entry) {
    console.log(`❌ Unknown job "${job}"`);
    process.exit(1);
}

const script = entry[0];

const child = spawn(
    "node",
    [script, "--state", state, "--debug", debug],
    { stdio: "inherit", env: process.env }
);

child.on("close", (code) => process.exit(code ?? 0));
