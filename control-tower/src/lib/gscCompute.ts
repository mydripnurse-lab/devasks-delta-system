// control-tower/src/lib/gscCompute.ts
export type GscAgg = {
    clicks: number;
    impressions: number;
    ctr: number;
    position: number;
};

export function n(v: any): number {
    const x = Number(v);
    return Number.isFinite(x) ? x : 0;
}

export function emptyAgg(): GscAgg {
    return { clicks: 0, impressions: 0, ctr: 0, position: 0 };
}

export function addAgg(a: GscAgg, b: Partial<GscAgg> & { impressions?: number; position?: number }): GscAgg {
    const clicks = a.clicks + n(b.clicks);
    const impressions = a.impressions + n(b.impressions);
    const ctr = impressions > 0 ? clicks / impressions : 0;

    const aW = a.position * (a.impressions || 0);
    const bW = n(b.position) * n(b.impressions);
    const pos = impressions > 0 ? (aW + bW) / impressions : 0;

    return { clicks, impressions, ctr, position: pos };
}
