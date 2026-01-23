import "dotenv/config";
import fs from "node:fs";
import path from "node:path";

function readJson(filePath) {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function listAllJsonFiles(dir) {
    const results = [];
    const stack = [dir];
    while (stack.length) {
        const current = stack.pop();
        const entries = fs.readdirSync(current, { withFileTypes: true });
        for (const e of entries) {
            const full = path.join(current, e.name);
            if (e.isDirectory()) stack.push(full);
            else if (e.isFile() && e.name.endsWith(".json")) results.push(full);
        }
    }
    return results;
}

function main() {
    const resourcesDir = path.resolve(process.cwd(), "resources/statesFiles");
    if (!fs.existsSync(resourcesDir)) {
        throw new Error("No existe /resources/statesFiles en la raíz del proyecto.");
    }

    const jsonFiles = listAllJsonFiles(resourcesDir);

    console.log("JSON encontrados en /resources/statesFiles:", jsonFiles.length);
    console.log("Ejemplos:", jsonFiles.slice(0, 5));

    // Aquí no asumimos estructura todavía:
    // solo dump del primer archivo para inspección.
    const first = jsonFiles[0];
    const sample = readJson(first);

    fs.mkdirSync(path.resolve(process.cwd(), "scripts/out"), { recursive: true });
    fs.writeFileSync(
        path.resolve(process.cwd(), "scripts/out/sample.json"),
        JSON.stringify({ file: first, sample }, null, 2)
    );

    console.log("Listo: scripts/out/sample.json creado (preview).");
}

main();
