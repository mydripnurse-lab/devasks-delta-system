// control-tower/src/lib/gscCatalogIndex.ts
import fs from "fs/promises";
import path from "path";

export type GscCatalogIndex = {
    baseDir: string;
    fingerprint: string;
    byHostname: Record<string, { state: string }>;
    statesPresent: Set<string>;
};

type LoadOpts = { force?: boolean };

function s(v: any) {
    return String(v ?? "").trim();
}

function isHostname(x: string) {
    const v = s(x).toLowerCase();
    return !!v && v.includes(".") && !v.includes(" ") && !v.startsWith("http");
}

function hostnameFromAnyString(x: string): string | null {
    const v = s(x);
    if (!v) return null;

    // If it's a URL, parse hostname
    try {
        if (v.startsWith("http://") || v.startsWith("https://")) {
            const u = new URL(v);
            return (u.hostname || "").toLowerCase() || null;
        }
    } catch { }

    // If it looks like a hostname already
    if (isHostname(v)) return v.toLowerCase();

    return null;
}

async function fileExists(p: string) {
    try {
        await fs.access(p);
        return true;
    } catch {
        return false;
    }
}

/**
 * Recursively walk JSON and collect any hostname-like strings.
 * We only need host -> state for join classification.
 */
function collectHostnamesDeep(node: any, out: Set<string>) {
    if (!node) return;

    if (typeof node === "string") {
        const host = hostnameFromAnyString(node);
        if (host) out.add(host);
        return;
    }

    if (Array.isArray(node)) {
        for (const x of node) collectHostnamesDeep(x, out);
        return;
    }

    if (typeof node === "object") {
        for (const k of Object.keys(node)) {
            collectHostnamesDeep((node as any)[k], out);
        }
    }
}

function titleCaseStateFromSlug(slug: string) {
    // slug may be "texas", "new-york", etc.
    const parts = s(slug)
        .replace(/[_]/g, "-")
        .split("-")
        .filter(Boolean)
        .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase());
    return parts.join(" ") || slug;
}

async function computeOutFingerprint(outRoot: string) {
    const entries = await fs.readdir(outRoot, { withFileTypes: true }).catch(() => []);
    const dirs = entries.filter((e) => e.isDirectory()).map((e) => e.name);

    let count = 0;
    let latest = 0;

    for (const slug of dirs) {
        if (slug === "checkpoints") continue;

        const p = path.join(outRoot, slug, `${slug}.json`);
        if (!(await fileExists(p))) continue;

        count += 1;

        try {
            const st = await fs.stat(p);
            if (st.mtimeMs > latest) latest = st.mtimeMs;
        } catch { }
    }

    return `count=${count};latest=${Math.floor(latest)}`;
}

async function buildIndex(outRoot: string): Promise<GscCatalogIndex> {
    const entries = await fs.readdir(outRoot, { withFileTypes: true }).catch(() => []);
    const dirs = entries.filter((e) => e.isDirectory()).map((e) => e.name);

    const byHostname: Record<string, { state: string }> = {};
    const statesPresent = new Set<string>();

    for (const slug of dirs) {
        if (slug === "checkpoints") continue;

        const p = path.join(outRoot, slug, `${slug}.json`);
        if (!(await fileExists(p))) continue;

        const stateName = titleCaseStateFromSlug(slug);
        statesPresent.add(stateName);

        let json: any = null;
        try {
            const raw = await fs.readFile(p, "utf8");
            json = JSON.parse(raw);
        } catch {
            continue;
        }

        // Collect all hostnames found in this state file
        const hosts = new Set<string>();
        collectHostnamesDeep(json, hosts);

        for (const h of hosts) {
            // ensure lower
            const host = s(h).toLowerCase();
            if (!host) continue;

            // Store mapping
            byHostname[host] = { state: stateName };
        }
    }

    const fingerprint = await computeOutFingerprint(outRoot);

    return {
        baseDir: outRoot,
        fingerprint,
        byHostname,
        statesPresent,
    };
}

/**
 * Loads catalog from scripts/out.
 * Caches in globalThis but invalidates if fingerprint changes.
 */
export async function loadGscCatalogIndex(opts?: LoadOpts): Promise<GscCatalogIndex> {
    const force = !!opts?.force;

    // Your repo layout:
    // process.cwd() = control-tower
    // scripts/out is at ../scripts/out
    const repoRoot = path.resolve(process.cwd(), "..");
    const outRoot = path.join(repoRoot, "scripts", "out");

    const g: any = globalThis as any;
    const cacheKey = "__gscCatalogIndexCache";

    const fingerprint = await computeOutFingerprint(outRoot);

    const cached: GscCatalogIndex | undefined = g[cacheKey];

    if (!force && cached && cached.baseDir === outRoot && cached.fingerprint === fingerprint) {
        return cached;
    }

    const built = await buildIndex(outRoot);

    // If buildIndex computed fingerprint again, keep the one we know is current
    built.fingerprint = fingerprint;

    g[cacheKey] = built;
    return built;
}
