import twilio from "twilio";
import "dotenv/config";

function mustEnvAny(names) {
    for (const n of names) {
        const v = process.env[n];
        if (v) return v;
    }
    throw new Error(`Missing env var. Expected one of: ${names.join(", ")}`);
}

function getTwilioClient() {
    const accountSid = mustEnvAny(["TWILIO_ACCOUNT_SID", "TWILIO_SID"]);
    const authToken = mustEnvAny(["TWILIO_AUTH_TOKEN"]);
    return twilio(accountSid, authToken);
}

/**
 * List subaccounts (status: active/suspended/closed)
 */
export async function listSubaccounts({ limit = 50 } = {}) {
    const client = getTwilioClient();
    // Twilio returns both master and subaccounts in Accounts list.
    // We'll filter out the master account by checking if sid != client.username?
    const accounts = await client.api.accounts.list({ limit });
    const masterSid = client.username; // this should be the Account SID used to auth
    return accounts.filter((a) => a.sid !== masterSid);
}

/**
 * Find subaccount by FriendlyName (exact match by default)
 */
export async function findTwilioAccountByFriendlyName(friendlyName, { exact = true, limit = 200 } = {}) {
    if (!friendlyName || !friendlyName.trim()) {
        throw new Error("friendlyName is required");
    }

    const client = getTwilioClient();
    const accounts = await client.api.accounts.list({ limit });

    const masterSid = client.username;
    const subs = accounts.filter((a) => a.sid !== masterSid);

    const target = exact
        ? subs.find((a) => (a.friendlyName || "").trim() === friendlyName.trim())
        : subs.find((a) => (a.friendlyName || "").toLowerCase().includes(friendlyName.trim().toLowerCase()));

    return target || null;
}

/**
 * Create subaccount
 */
export async function createSubaccount(friendlyName) {
    if (!friendlyName || !friendlyName.trim()) {
        throw new Error("friendlyName is required");
    }
    const client = getTwilioClient();
    const account = await client.api.accounts.create({ friendlyName: friendlyName.trim() });
    return account;
}

/**
 * Get or create subaccount by FriendlyName
 */
export async function getOrCreateSubaccountByFriendlyName(friendlyName) {
    const existing = await findTwilioAccountByFriendlyName(friendlyName, { exact: true });
    if (existing) return { account: existing, created: false };

    const created = await createSubaccount(friendlyName);
    return { account: created, created: true };
}

/**
 * Close subaccount (WARNING: irreversible)
 */
export async function closeTwilioAccount(accountSid) {
    if (!accountSid || !accountSid.trim()) {
        throw new Error("accountSid is required");
    }
    const client = getTwilioClient();

    // Twilio expects status: "closed"
    const updated = await client.api.accounts(accountSid.trim()).update({ status: "closed" });
    return updated;
}
