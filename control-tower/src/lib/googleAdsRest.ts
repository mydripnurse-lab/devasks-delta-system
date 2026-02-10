export async function googleAdsSearch(opts: {
    customerId?: string;
    loginCustomerId?: string;
    query: string;
    pageSize?: number;
    version?: string; // "v17"
}) {
    const developerToken = s(process.env.GOOGLE_ADS_DEVELOPER_TOKEN);
    if (!developerToken) throw new Error("Missing env GOOGLE_ADS_DEVELOPER_TOKEN");

    const customerId = cleanCid(opts.customerId || s(process.env.GOOGLE_ADS_CUSTOMER_ID));
    if (!customerId) throw new Error("Missing GOOGLE_ADS_CUSTOMER_ID");

    const loginCustomerId = cleanCid(
        opts.loginCustomerId || s(process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID),
    );

    const version = s(opts.version) || "v17";
    const pageSize = Number(opts.pageSize || 1000);

    const oauth2 = await getAdsOAuth2();
    const token = await oauth2.getAccessToken();
    const accessToken = s((token as any)?.token || token);
    if (!accessToken) throw new Error("Failed to obtain Google OAuth access_token");

    // ✅ NO trailing slash
    const url = `https://googleads.googleapis.com/${version}/customers/${customerId}/googleAds:search`;

    const res = await fetch(url, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${accessToken}`,
            "developer-token": developerToken,
            ...(loginCustomerId ? { "login-customer-id": loginCustomerId } : {}),
            "content-type": "application/json",
        },
        body: JSON.stringify({
            query: opts.query,
            pageSize,
        }),
    });

    const text = await res.text();
    let json: any = null;
    try {
        json = text ? JSON.parse(text) : null;
    } catch {
        json = { raw: text };
    }

    if (!res.ok) {
        throw new Error(
            `Google Ads HTTP ${res.status}: ${JSON.stringify(json)?.slice(0, 2000)}`,
        );
    }

    return json;
}
