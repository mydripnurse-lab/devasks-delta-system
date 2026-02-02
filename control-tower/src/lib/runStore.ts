// src/lib/runStore.ts
import { EventEmitter } from "events";
import type { ChildProcess } from "child_process";

export type RunMeta = {
    job?: string;
    state?: string;
    mode?: string;
    debug?: boolean;
    cmd?: string;
};

export type RunRecord = {
    id: string;
    createdAt: number;
    meta: RunMeta;
    emitter: EventEmitter;
    lines: string[];
    stopped: boolean;
    finished: boolean;
    exitCode: number | null;
    error?: string;
    proc?: ChildProcess;
};

// ✅ IMPORTANT: keep store in globalThis so /api/run and /api/stream share it
type GlobalRunStore = {
    runs: Map<string, RunRecord>;
};

declare global {
    // eslint-disable-next-line no-var
    var __RUN_STORE__: GlobalRunStore | undefined;
}

const g = globalThis as any;

if (!g.__RUN_STORE__) {
    g.__RUN_STORE__ = {
        runs: new Map<string, RunRecord>(),
    } satisfies GlobalRunStore;
}

const runs: Map<string, RunRecord> = g.__RUN_STORE__.runs;

function now() {
    return Date.now();
}

function cleanupOldRuns() {
    const TTL_MS = 1000 * 60 * 30; // 30 min
    const t = now();
    for (const [id, r] of runs.entries()) {
        if (t - r.createdAt > TTL_MS) runs.delete(id);
    }
}

export function createRun(meta: RunMeta = {}) {
    cleanupOldRuns();
    const id = `${Date.now()}-${Math.floor(Math.random() * 1e12)}`;

    const rec: RunRecord = {
        id,
        createdAt: now(),
        meta,
        emitter: new EventEmitter(),
        lines: [],
        stopped: false,
        finished: false,
        exitCode: null,
    };

    runs.set(id, rec);
    return rec;
}

export function getRun(id: string) {
    return runs.get(id) || null;
}

export function setRunMetaCmd(id: string, cmd: string) {
    const r = runs.get(id);
    if (!r) return;
    r.meta.cmd = cmd;
}

export function appendLine(id: string, line: string) {
    const r = runs.get(id);
    if (!r) return;

    const msg = String(line ?? "");
    r.lines.push(msg);

    if (r.lines.length > 5000) r.lines = r.lines.slice(-4000);

    r.emitter.emit("line", msg);
}

/**
 * ✅ Optional helper: emit progress in the exact format that stream parser expects.
 * Your scripts can call this by importing from "@/lib/runStore" IF you ever execute scripts in-process.
 * (Not required for your current spawn-based setup.)
 */
export function appendProgressLine(
    id: string,
    payload: unknown,
    kind: "__PROGRESS_INIT__" | "__PROGRESS__" | "__PROGRESS_END__" = "__PROGRESS__"
) {
    appendLine(id, `${kind} ${JSON.stringify(payload)}`);
}

export function attachProcess(id: string, proc: ChildProcess) {
    const r = runs.get(id);
    if (!r) return;
    r.proc = proc;
}

export function endRun(id: string, exitCode: number | null) {
    const r = runs.get(id);
    if (!r) return;

    r.finished = true;
    r.exitCode = exitCode ?? null;

    // ✅ HARDEN: if exitCode != 0 and no error set, mark a generic one
    if ((r.exitCode ?? 0) !== 0 && !r.error) {
        r.error = `Process exited with code ${r.exitCode}`;
        appendLine(id, `❌ ${r.error}`);
    }

    r.emitter.emit("end", r.exitCode ?? 0);
}

export function errorRun(id: string, err: unknown) {
    const r = runs.get(id);
    if (!r) return;

    const msg = err instanceof Error ? err.message : String(err);
    r.error = msg;
    appendLine(id, `❌ ${msg}`);

    // ✅ HARDEN: if already finished, don't double-close
    if (r.finished) return;

    // If no attached process, safest is to close the run.
    if (!r.proc) {
        endRun(id, 1);
        return;
    }
}

export function stopRun(id: string) {
    const r = runs.get(id);
    if (!r) return false;

    r.stopped = true;

    try {
        if (r.proc && !r.proc.killed) {
            r.proc.kill("SIGTERM");
            setTimeout(() => {
                try {
                    if (r.proc && !r.proc.killed) r.proc.kill("SIGKILL");
                } catch { }
            }, 1200);
        }
    } catch { }

    appendLine(id, "🛑 Stop requested");
    return true;
}
