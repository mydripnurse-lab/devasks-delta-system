// control-tower/src/lib/gscClient.ts
import { google } from "googleapis";

export type GscApiRow = {
    keys?: string[];
    clicks?: number;
    impressions?: number;
    ctr?: number;
    position?: number;
};

export type GscQueryOpts = {
    siteUrl: string;
    startDate: string; // YYYY-MM-DD
    endDate: string;   // YYYY-MM-DD
    dimensions: Array<"query" | "page" | "country" | "device" | "date">;
    rowLimit?: number;
};

function must(v: string | undefined, name: string) {
    if (!v) throw new Error(`Missing env: ${name}`);
    return v;
}

export async function gscQuery(opts: GscQueryOpts): Promise<GscApiRow[]> {
    const auth = new google.auth.GoogleAuth({
        scopes: ["https://www.googleapis.com/auth/webmasters.readonly"],
    });

    const client = await auth.getClient();
    const webmasters = google.webmasters({ version: "v3", auth: client as any });

    const res = await webmasters.searchanalytics.query({
        siteUrl: opts.siteUrl,
        requestBody: {
            startDate: opts.startDate,
            endDate: opts.endDate,
            dimensions: opts.dimensions,
            rowLimit: opts.rowLimit ?? 25000,
        },
    });

    return (res.data.rows || []) as GscApiRow[];
}

export function getGscSiteUrl(): string {
    return must(process.env.GSC_SITE_URL, "GSC_SITE_URL");
}
