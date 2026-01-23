// scripts/src/normalize.js
export function extractCounties(stateKey, data) {
    const countiesArr =
        (Array.isArray(data.counties) && data.counties) ||
        (Array.isArray(data.items) && data.items) ||
        (Array.isArray(data.data) && data.data) ||
        [];

    return countiesArr.map((c, idx) => ({
        stateKey,
        countyIndex: idx,
        countyName: String(c?.county ?? c?.name ?? c?.countyName ?? `county_${idx + 1}`).trim(),
        raw: c,
    }));
}
