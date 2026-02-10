import { NextResponse } from "next/server";
import fs from "fs/promises";
import path from "path";

const REPO_ROOT = path.resolve(process.cwd(), "..");
const OUT_ROOT = path.join(REPO_ROOT, "scripts", "out");

// ✅ NEW: resources/statesFiles/*.json
const RESOURCES_STATES_ROOT = path.join(REPO_ROOT, "resources", "statesFiles");

function s(v: any) {
    return String(v ?? "").trim();
}

async function listStatesFromOut(): Promise<string[]> {
    const entries = await fs.readdir(OUT_ROOT, { withFileTypes: true }).catch(() => []);
    const dirs = entries.filter((e) => e.isDirectory()).map((e) => e.name);

    const states: string[] = [];
    for (const slug of dirs) {
        if (slug === "checkpoints") continue;
        const p = path.join(OUT_ROOT, slug, `${slug}.json`);
        try {
            await fs.access(p);
            states.push(slug);
        } catch { }
    }

    states.sort();
    return states;
}

async function listStatesFromResources(): Promise<string[]> {
    const entries = await fs.readdir(RESOURCES_STATES_ROOT, { withFileTypes: true }).catch(() => []);
    const files = entries.filter((e) => e.isFile()).map((e) => e.name);

    const states = files
        .filter((f) => f.toLowerCase().endsWith(".json"))
        .map((f) => f.replace(/\.json$/i, ""))
        .filter(Boolean)
        .sort();

    return states;
}

export async function GET(req: Request) {
    try {
        const url = new URL(req.url);
        const source = s(url.searchParams.get("source")).toLowerCase(); // "resources" | "out" | ""

        // ✅ Only when explicitly requested: resources
        const states =
            source === "resources"
                ? await listStatesFromResources()
                : await listStatesFromOut();

        return NextResponse.json({ states, source: source === "resources" ? "resources" : "out" });
    } catch (e: any) {
        return NextResponse.json(
            { states: [], error: e?.message || "Failed to list states" },
            { status: 500 },
        );
    }
}
