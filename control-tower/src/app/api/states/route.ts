import { NextResponse } from "next/server";
import fs from "fs/promises";
import path from "path";

const REPO_ROOT = path.resolve(process.cwd(), "..");
const OUT_ROOT = path.join(REPO_ROOT, "scripts", "out");

export async function GET() {
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
    return NextResponse.json({ states });
}
