// services/ghlCustomValues.js
import { ghlFetch } from "./ghlClient.js";

/**
 * GET custom values for a location using a LOCATION (sub-account) token
 * Docs: GET /locations/:locationId/customValues
 */
export async function getLocationCustomValues(locationId, locationToken) {
    if (!locationId) throw new Error("getLocationCustomValues: locationId is required");
    if (!locationToken) throw new Error("getLocationCustomValues: locationToken is required");

    // Usamos ghlFetch pero SOBREESCRIBIMOS Authorization con el location token
    return await ghlFetch(`/locations/${locationId}/customValues`, {
        method: "GET",
        headers: {
            Authorization: `Bearer ${locationToken}`,
        },
    });
}

/**
 * PUT update a single custom value by id using a LOCATION (sub-account) token
 * Docs: PUT /locations/:locationId/customValues/:customValueId
 *
 * Body expected by GHL:
 * { "name": "...", "value": "..." }
 *
 * NOTE: si no tienes el "name", GHL a veces lo requiere.
 * Por eso esta función acepta value y opcionalmente name.
 */
export async function updateLocationCustomValue(locationId, customValueId, value, locationToken, name = undefined) {
    if (!locationId) throw new Error("updateLocationCustomValue: locationId is required");
    if (!customValueId) throw new Error("updateLocationCustomValue: customValueId is required");
    if (value === undefined) throw new Error("updateLocationCustomValue: value is required");
    if (!locationToken) throw new Error("updateLocationCustomValue: locationToken is required");

    const body = name ? { name, value } : { value };

    return await ghlFetch(`/locations/${locationId}/customValues/${customValueId}`, {
        method: "PUT",
        headers: {
            Authorization: `Bearer ${locationToken}`,
            "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
    });
}
