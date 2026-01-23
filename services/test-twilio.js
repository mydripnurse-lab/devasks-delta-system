import "dotenv/config";
import {
    listSubaccounts,
    findTwilioAccountByFriendlyName,
    getOrCreateSubaccountByFriendlyName,
    closeTwilioAccount,
} from "./services/twilioClient.js";

async function main() {
    const friendly = process.argv.slice(2).join(" ").trim() || "My Drip Nurse Test Subaccount";

    console.log("🔎 Searching subaccount by friendly name (exact):", friendly);
    const found = await findTwilioAccountByFriendlyName(friendly, { exact: true });

    if (found) {
        console.log("✅ Found:", { sid: found.sid, friendlyName: found.friendlyName, status: found.status });
    } else {
        console.log("ℹ️ Not found. Creating...");
        const { account, created } = await getOrCreateSubaccountByFriendlyName(friendly);
        console.log(created ? "✅ Created" : "✅ Reused", { sid: account.sid, friendlyName: account.friendlyName, status: account.status });
    }

    console.log("\n📋 Listing subaccounts (first 20):");
    const subs = await listSubaccounts({ limit: 50 });
    console.log(subs.slice(0, 20).map((a) => ({ sid: a.sid, friendlyName: a.friendlyName, status: a.status })));

    // ⚠️ Close example (COMMENTED)
    // const toClose = subs.find(a => a.friendlyName === friendly);
    // if (toClose) {
    //   console.log("\n⚠️ Closing subaccount:", toClose.sid);
    //   const closed = await closeTwilioAccount(toClose.sid);
    //   console.log("✅ Closed:", { sid: closed.sid, status: closed.status });
    // }
}

main().catch((err) => {
    console.error("❌ Test failed:");
    console.error(err?.response?.data || err?.message || err);
    process.exit(1);
});
