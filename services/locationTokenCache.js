// services/locationTokenCache.js
import { getLocationToken } from "./ghlLocationToken.js";

// cache en memoria
// key: locationId => { token, expiresAtMs }
const cache = new Map();

// TTL default si GHL no devuelve expires_in
const DEFAULT_TTL_SECONDS = 55 * 60; // 55 min

export async function getCachedLocationToken(locationId, { forceRefresh = false } = {}) {
    if (!locationId) throw new Error("getCachedLocationToken: locationId is required");

    const now = Date.now();

    if (!forceRefresh) {
        const hit = cache.get(locationId);
        if (hit?.token && hit?.expiresAtMs && now < hit.expiresAtMs) {
            return hit.token;
        }
    }

    const { token, raw } = await getLocationToken(locationId);

    const expiresInSec =
        Number(raw?.expires_in) ||
        Number(raw?.expiresIn) ||
        DEFAULT_TTL_SECONDS;

    // buffer 60s
    const expiresAtMs = now + expiresInSec * 1000 - 60_000;

    cache.set(locationId, { token, expiresAtMs });
    return token;
}

export function clearLocationToken(locationId) {
    cache.delete(locationId);
}

export function clearAllLocationTokens() {
    cache.clear();
}
