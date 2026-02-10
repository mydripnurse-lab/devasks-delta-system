// src/lib/stateCatalog.ts
import fs from "fs/promises";
import path from "path";

type StateFile = {
    stateName?: string;
    stateSlug?: string;
    counties?: Array<{
        countyDomain?: string;
        cities?: Array<{ cityDomain?: string }>;
    }>;
};

function safeHost(urlStr: string) {
    try {
        const u = new URL(urlStr);
        return (u.hostname || "").toLowerCase();
    } catch {
        return "";
    }
}

export type StateCatalog = {
    states: Array<{ slug: string; name: string }>;
    hostToState: Record<string, { slug: string; name: string }>;
};

export async function loadStateCatalog(): Promise<StateCatalog> {
    // scripts está en el root fuera de control-tower (según tú)
    // Si este Next project está en control-tower/, entonces scripts está en ../scripts
    const scriptsOut = path.join(process.cwd(), "..", "scripts", "out");

    const states: Array<{ slug: string; name: string }> = [];
    const hostToState: Record<string, { slug: string; name: string }> = {};

    let dirs: string[] = [];
    try {
        dirs = await fs.readdir(scriptsOut);
    } catch {
        // no existe / no accesible
        return { states, hostToState };
    }

    for (const dir of dirs) {
        const slug = String(dir || "").trim();
        if (!slug) continue;

        const jsonPath = path.join(scriptsOut, slug, `${slug}.json`);
        let raw = "";
        try {
            raw = await fs.readFile(jsonPath, "utf8");
        } catch {
            continue;
        }

        let state: StateFile | null = null;
        try {
            state = JSON.parse(raw);
        } catch {
            continue;
        }

        const name = String(state?.stateName || slug);
        states.push({ slug, name });

        const counties = state?.counties || [];
        for (const c of counties) {
            const ch = safeHost(String(c?.countyDomain || ""));
            if (ch) hostToState[ch] = { slug, name };

            for (const city of c?.cities || []) {
                const h = safeHost(String(city?.cityDomain || ""));
                if (h) hostToState[h] = { slug, name };
            }
        }
    }

    return { states, hostToState };
}
