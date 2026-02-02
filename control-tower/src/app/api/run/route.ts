// src/app/api/run/route.ts
import { NextResponse } from "next/server";
import path from "path";
import fs from "fs";
import { spawn } from "child_process";
import readline from "readline";

import {
    createRun,
    appendLine,
    attachProcess,
    endRun,
    errorRun,
    setRunMetaCmd,
} from "@/lib/runStore";

export const runtime = "nodejs";

function exists(p: string) {
    try {
        return fs.existsSync(p);
    } catch {
        return false;
    }
}

function findRepoRoot(startDir: string) {
    let dir = startDir;
    for (let i = 0; i < 10; i++) {
        const hasResources = exists(path.join(dir, "resources"));
        const hasBuilds = exists(path.join(dir, "scripts", "src", "builds"));
        const hasPkg = exists(path.join(dir, "package.json"));
        const score = (hasResources ? 1 : 0) + (hasBuilds ? 1 : 0) + (hasPkg ? 1 : 0);
        if (score >= 2) return dir;

        const parent = path.dirname(dir);
        if (parent === dir) break;
        dir = parent;
    }
    return startDir;
}

function resolveScriptPath(repoRoot: string, jobKey: string) {
    const buildsDir = path.join(repoRoot, "scripts", "src", "builds");

    const candidatesByJob: Record<string, string[]> = {
        "run-delta-system": [
            path.join(repoRoot, "scripts", "src", "run-delta-system.js"),
            path.join(repoRoot, "scripts", "src", "builds", "run-delta-system.js"),
        ],
        "update-custom-values": [
            path.join(repoRoot, "scripts", "src", "update-custom-values.js"),
            path.join(repoRoot, "scripts", "src", "builds", "update-custom-values.js"),
        ],
        "build-sheet-rows": [
            path.join(buildsDir, "build-sheets-counties-cities.js"),
            path.join(buildsDir, "build-sheet-rows.js"),
        ],
        "build-state-index": [
            path.join(buildsDir, "build-states-index.js"),
            path.join(buildsDir, "build-state-index.js"),
        ],
        "build-state-sitemaps": [
            path.join(buildsDir, "build-states-sitemaps.js"),
            path.join(buildsDir, "build-state-sitemaps.js"),
        ],
        "build-counties": [
            path.join(buildsDir, "build-counties.js"),
            path.join(buildsDir, "build-counties.js"),
        ],
    };

    const candidates = candidatesByJob[jobKey] || [];
    for (const c of candidates) if (exists(c)) return c;

    const direct = path.join(buildsDir, `${jobKey}.js`);
    if (exists(direct)) return direct;

    return null;
}

function safeStateArg(state: string) {
    const s = String(state || "").trim();
    return s ? s : "all";
}

// --- tiny env loader (no dependency)
function parseEnvFile(contents: string) {
    const out: Record<string, string> = {};
    const lines = contents.split(/\r?\n/);
    for (const line of lines) {
        const t = line.trim();
        if (!t || t.startsWith("#")) continue;
        const eq = t.indexOf("=");
        if (eq <= 0) continue;
        const k = t.slice(0, eq).trim();
        let v = t.slice(eq + 1).trim();
        if (
            (v.startsWith('"') && v.endsWith('"')) ||
            (v.startsWith("'") && v.endsWith("'"))
        ) {
            v = v.slice(1, -1);
        }
        out[k] = v;
    }
    return out;
}

function loadRepoEnv(repoRoot: string) {
    const envPaths = [path.join(repoRoot, ".env"), path.join(repoRoot, ".env.local")];
    const merged: Record<string, string> = {};
    for (const p of envPaths) {
        if (!exists(p)) continue;
        try {
            const raw = fs.readFileSync(p, "utf8");
            Object.assign(merged, parseEnvFile(raw));
        } catch {
            // ignore
        }
    }
    return merged;
}

export async function POST(req: Request) {
    const body = await req.json().catch(() => null);
    const job = body?.job as string;
    const state = safeStateArg(body?.state);
    const mode = (body?.mode as string) || "dry";
    const debug = !!body?.debug;

    if (!job) return NextResponse.json({ error: "Missing job" }, { status: 400 });

    const repoRoot = findRepoRoot(process.cwd());
    const scriptPath = resolveScriptPath(repoRoot, job);

    if (!scriptPath) {
        return NextResponse.json(
            { error: `Script not found for job="${job}". (repoRoot=${repoRoot})` },
            { status: 400 }
        );
    }

    const run = createRun({ job, state, mode, debug });

    let closed = false;

    try {
        appendLine(run.id, `🟢 created runId=${run.id}`);
        appendLine(run.id, `job=${job} state=${state} mode=${mode} debug=${debug}`);

        const args = [
            scriptPath,
            `--state=${state}`,
            `--mode=${mode}`,
            `--debug=${debug ? "1" : "0"}`,
            state, // ✅ optional positional fallback (safe)
        ];


        const cmd = `node ${args.map((a) => JSON.stringify(a)).join(" ")}`;
        setRunMetaCmd(run.id, cmd);

        // appendLine(run.id, `▶ cmd: ${cmd}`);
        // appendLine(run.id, `ℹ cwd: ${repoRoot}`);

        // Load repo .env so the child always gets GOOGLE_SERVICE_ACCOUNT_KEYFILE, etc.
        const repoEnv = loadRepoEnv(repoRoot);

        const child = spawn(process.execPath, args, {
            cwd: repoRoot,
            env: {
                ...process.env,
                ...repoEnv, // ✅ inject repo env

                // compat
                DELTA_STATE: state,
                STATE: state,
                MODE: mode,
                DEBUG: debug ? "1" : "0",
            },
            stdio: ["pipe", "pipe", "pipe"],
        });

        attachProcess(run.id, child);

        const rlOut = readline.createInterface({ input: child.stdout });
        const rlErr = readline.createInterface({ input: child.stderr });

        rlOut.on("line", (line) => appendLine(run.id, line));
        rlErr.on("line", (line) => appendLine(run.id, line));

        child.on("error", (err) => {
            // marca error y termina run (si no termina, UI queda "running")
            errorRun(run.id, err);

            if (!closed) {
                closed = true;
                try {
                    rlOut.close();
                    rlErr.close();
                } catch { }
                endRun(run.id, 1);
            }
        });

        child.on("close", (code) => {
            if (closed) return;
            closed = true;

            try {
                rlOut.close();
                rlErr.close();
            } catch { }

            endRun(run.id, code ?? 0);
        });

        return NextResponse.json({ runId: run.id });
    } catch (err) {
        errorRun(run.id, err);
        if (!closed) {
            closed = true;
            endRun(run.id, 1);
        }
        return NextResponse.json(
            { error: err instanceof Error ? err.message : String(err), runId: run.id },
            { status: 500 }
        );
    }
}
