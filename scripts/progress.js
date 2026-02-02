// scripts/progress.js
export function logHeader(title) {
    const line = "─".repeat(62);
    console.log(`\n${line}\n${title}\n${line}`);
}

export function logKV(obj) {
    for (const [k, v] of Object.entries(obj || {})) {
        console.log(`• ${k}: ${v}`);
    }
}

export function emitProgressInit({ totals, job, state }) {
    console.log(
        `__PROGRESS_INIT__ ${JSON.stringify({
            pct: 0,
            totals: totals || { all: 0, counties: 0, cities: 0 },
            done: { all: 0, counties: 0, cities: 0 },
            meta: { job, state },
            last: { kind: "init", action: "start" },
        })}`
    );
}

export function emitProgress({ totals, done, last }) {
    const all = Number(totals?.all ?? 0);
    const dAll = Number(done?.all ?? 0);
    const pct = all > 0 ? dAll / all : 0;

    console.log(
        `__PROGRESS__ ${JSON.stringify({
            pct,
            totals,
            done,
            last,
        })}`
    );
}

export function emitProgressEnd({ totals, done, ok = true }) {
    console.log(
        `__PROGRESS_END__ ${JSON.stringify({
            pct: ok ? 1 : 0,
            totals,
            done,
            ok,
            last: { kind: "end", action: ok ? "done" : "error" },
        })}`
    );
}
