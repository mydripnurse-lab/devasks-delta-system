// control-tower/src/lib/ghl/http.ts
export const API_BASE = "https://services.leadconnectorhq.com";
export const VERSION = "2021-07-28";

export async function ghlFetchJson(
    pathOrUrl: string,
    opts: {
        method: string;
        bearer: string;
        body?: any;
        headers?: Record<string, string>;
    },
) {
    const url = pathOrUrl.startsWith("http")
        ? pathOrUrl
        : `${API_BASE}${pathOrUrl.startsWith("/") ? "" : "/"}${pathOrUrl}`;

    const headers: Record<string, string> = {
        Authorization: `Bearer ${opts.bearer}`,
        Version: VERSION,
        Accept: "application/json",
        ...(opts.headers || {}),
    };

    let body: any = undefined;
    if (opts.body !== undefined) {
        headers["Content-Type"] = headers["Content-Type"] || "application/json";
        body = typeof opts.body === "string" ? opts.body : JSON.stringify(opts.body);
    }

    const r = await fetch(url, { method: opts.method, headers, body });
    const text = await r.text();

    let json: any;
    try {
        json = JSON.parse(text);
    } catch {
        json = { raw: text };
    }

    if (!r.ok) {
        const err: any = new Error(`GHL API error (${r.status}) ${url}`);
        err.status = r.status;
        err.data = json;
        throw err;
    }

    return json;
}
