// scripts/src/io.js
import fs from "node:fs";
import path from "node:path";

const ROOT = process.cwd();

export const PATHS = {
    statesFilesDir: path.join(ROOT, "resources", "statesFiles"),
    outDir: path.join(ROOT, "scripts", "out"),
    logsDir: path.join(ROOT, "scripts", "logs"),
};

export function ensureDirs() {
    fs.mkdirSync(PATHS.outDir, { recursive: true });
    fs.mkdirSync(PATHS.logsDir, { recursive: true });
}

export function listStateJsonFiles() {
    const dir = PATHS.statesFilesDir;
    if (!fs.existsSync(dir)) throw new Error(`No existe: ${dir}`);

    return fs
        .readdirSync(dir)
        .filter((f) => f.endsWith(".json"))
        .map((f) => ({
            file: f,
            fullPath: path.join(dir, f),
            key: f.replace(".json", ""),
        }))
        .sort((a, b) => a.file.localeCompare(b.file));
}

export function readJson(fullPath) {
    return JSON.parse(fs.readFileSync(fullPath, "utf8"));
}

export function writeJson(fullPath, data) {
    fs.writeFileSync(fullPath, JSON.stringify(data, null, 2));
}
