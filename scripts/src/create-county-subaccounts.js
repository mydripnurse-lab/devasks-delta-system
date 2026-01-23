import { syncCustomValuesByName } from "../services/ghlCustomValues.js";

async function handleCounty(locationId, countyObj) {
    // countyObj.customValuesBody.customValues = [{name,value},...]
    const desired = countyObj.customValuesBody?.customValues || [];

    const res = await syncCustomValuesByName(locationId, desired);

    console.log("=== CustomValues Sync Summary ===");
    console.log("Updated:", res.updated.length);
    console.log("Skipped same:", res.skipped_same.length);
    console.log("Missing in GHL:", res.missing_in_ghl.length);
    console.log("Errors:", res.errors.length);

    // Si quieres, puedes parar el pipeline si hay errors:
    if (res.errors.length) {
        throw new Error(`CustomValues sync failed for locationId=${locationId} errors=${res.errors.length}`);
    }

    return res;
}
