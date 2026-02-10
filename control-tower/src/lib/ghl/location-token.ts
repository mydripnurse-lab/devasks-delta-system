// control-tower/src/lib/ghl/location-token.ts
import { ghlFetchJson } from "./http";

export async function getLocationAccessToken({
    agencyAccessToken,
    companyId,
    locationId,
}: {
    agencyAccessToken: string;
    companyId: string;
    locationId: string;
}) {
    if (!agencyAccessToken) throw new Error("Missing agencyAccessToken");
    if (!companyId) throw new Error("Missing companyId");
    if (!locationId) throw new Error("Missing locationId");

    const res = await ghlFetchJson("/oauth/locationToken", {
        method: "POST",
        bearer: agencyAccessToken,
        body: { companyId, locationId },
    });

    const tok = res?.access_token || res?.token?.access_token || "";
    if (!tok) throw new Error("Location token missing access_token");
    return tok as string;
}
