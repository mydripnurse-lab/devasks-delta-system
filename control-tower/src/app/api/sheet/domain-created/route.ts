import { NextResponse } from "next/server";
import { google } from "googleapis";
import fs from "fs";
import path from "path";

export const runtime = "nodejs";

const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID!;
const COUNTY_TAB = process.env.GOOGLE_SHEET_COUNTY_TAB || "Counties";
const CITY_TAB = process.env.GOOGLE_SHEET_CITY_TAB || "Cities";

function s(v: any) {
    return String(v ?? "").trim();
}

function colToLetter(colIndex0: number) {
    let n = colIndex0 + 1;
    let out = "";
    while (n > 0) {
        const r = (n - 1) % 26;
        out = String.fromCharCode(65 + r) + out;
        n = Math.floor((n - 1) / 26);
    }
    return out;
}

/**
 * Resolve a relative keyfile path robustly, regardless of where Next.js is running from.
 * - Accepts absolute paths directly.
 * - For relative paths, tries multiple candidate bases.
 */
function resolveKeyfilePathSmart(relOrAbs: string) {
    const raw = s(relOrAbs);
    if (!raw) return "";

    // If absolute, return as-is.
    if (path.isAbsolute(raw)) return raw;

    const cwd = process.cwd();

    // Candidate bases we will try:
    const bases: string[] = [
        cwd,
        path.join(cwd, ".."),
        path.join(cwd, "..", ".."),
    ];

    // If you're inside ".../control-tower", also try repo-root heuristics
    // e.g. /mydripnurse-sitemaps/control-tower -> /mydripnurse-sitemaps
    const parts = cwd.split(path.sep);
    const idx = parts.lastIndexOf("control-tower");
    if (idx >= 0) {
        const repoRoot = parts.slice(0, idx).join(path.sep);
        if (repoRoot) {
            bases.unshift(repoRoot);
            bases.unshift(path.join(repoRoot, "control-tower")); // just in case
        }
    }

    for (const base of bases) {
        const candidate = path.normalize(path.join(base, raw));
        if (fs.existsSync(candidate)) return candidate;
    }

    // Fallback: default join with cwd (keeps error message predictable)
    return path.normalize(path.join(cwd, raw));
}

async function getSheetsClient() {
    // 1) GOOGLE_SERVICE_ACCOUNT_JSON (stringified json)
    const jsonRaw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
    if (jsonRaw) {
        const creds = JSON.parse(jsonRaw);
        const auth = new google.auth.GoogleAuth({
            credentials: creds,
            scopes: ["https://www.googleapis.com/auth/spreadsheets"],
        });
        return google.sheets({ version: "v4", auth });
    }

    // 2) EMAIL + PRIVATE_KEY (env)
    const clientEmail = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
    const privateKeyRaw = process.env.GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY;
    if (clientEmail && privateKeyRaw) {
        const privateKey = privateKeyRaw.replace(/\\n/g, "\n");
        const auth = new google.auth.JWT({
            email: clientEmail,
            key: privateKey,
            scopes: ["https://www.googleapis.com/auth/spreadsheets"],
        });
        return google.sheets({ version: "v4", auth });
    }

    // 3) KEYFILE path (your .env.local uses this)
    const keyfileEnv = process.env.GOOGLE_SERVICE_ACCOUNT_KEYFILE;
    if (keyfileEnv) {
        const keyFile = resolveKeyfilePathSmart(keyfileEnv);

        // Make the error explicit if missing
        if (!fs.existsSync(keyFile)) {
            throw new Error(
                `GOOGLE_SERVICE_ACCOUNT_KEYFILE not found. Tried: "${keyFile}". ` +
                `Tip: Use an absolute path, or adjust the relative path based on where you run "npm run dev".`,
            );
        }

        const auth = new google.auth.GoogleAuth({
            keyFile,
            scopes: ["https://www.googleapis.com/auth/spreadsheets"],
        });
        return google.sheets({ version: "v4", auth });
    }

    // 4) Fallback ADC (only if you did gcloud auth application-default login)
    const auth = new google.auth.GoogleAuth({
        scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });
    return google.sheets({ version: "v4", auth });
}

async function findAndUpdateDomainCreated(opts: {
    sheets: any;
    spreadsheetId: string;
    sheetName: string;
    locId: string;
    value: string; // "TRUE" | "FALSE"
}) {
    const { sheets, spreadsheetId, sheetName, locId, value } = opts;

    const getRes = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: `${sheetName}!A:AZ`,
        majorDimension: "ROWS",
    });

    const values: any[][] = getRes.data.values || [];
    if (!values.length) throw new Error(`Sheet "${sheetName}" has no data`);

    const headers = (values[0] || []).map((h) => s(h));
    const idxLoc = headers.findIndex((h) => h === "Location Id");
    if (idxLoc < 0) throw new Error(`Missing header "Location Id" in ${sheetName}`);

    const idxDomainCreated = headers.findIndex((h) => h === "Domain Created");
    if (idxDomainCreated < 0) throw new Error(`Missing header "Domain Created" in ${sheetName}`);

    let foundRowNumber = -1; // 1-based
    for (let i = 1; i < values.length; i++) {
        const row = values[i] || [];
        const rowLoc = s(row[idxLoc]);
        if (rowLoc && rowLoc === locId) {
            foundRowNumber = i + 1;
            break;
        }
    }

    if (foundRowNumber < 0) return null;

    const colLetter = colToLetter(idxDomainCreated);
    const a1 = `${sheetName}!${colLetter}${foundRowNumber}`;

    await sheets.spreadsheets.values.update({
        spreadsheetId,
        range: a1,
        valueInputOption: "RAW",
        requestBody: { values: [[value]] },
    });

    return { sheetName, rowNumber: foundRowNumber, a1 };
}

export async function POST(req: Request) {
    try {
        if (!SPREADSHEET_ID) {
            return NextResponse.json({ error: "Missing GOOGLE_SHEET_ID" }, { status: 500 });
        }

        const body = await req.json().catch(() => ({} as any));
        const locId = s(body?.locId);
        const kind = s(body?.kind); // "counties" | "cities" | ""
        const valueBool = body?.value;

        if (!locId) {
            return NextResponse.json({ error: "Missing locId" }, { status: 400 });
        }

        const value =
            typeof valueBool === "boolean" ? (valueBool ? "TRUE" : "FALSE") : "TRUE";

        const sheets = await getSheetsClient();

        const targets =
            kind === "counties"
                ? [COUNTY_TAB]
                : kind === "cities"
                    ? [CITY_TAB]
                    : [COUNTY_TAB, CITY_TAB];

        for (const sheetName of targets) {
            const updated = await findAndUpdateDomainCreated({
                sheets,
                spreadsheetId: SPREADSHEET_ID,
                sheetName,
                locId,
                value,
            });

            if (updated) {
                return NextResponse.json({ ok: true, ...updated, locId, value });
            }
        }

        return NextResponse.json(
            { error: `locId not found in ${targets.join(" or ")}: ${locId}` },
            { status: 404 },
        );
    } catch (e: any) {
        console.error("POST /api/sheet/domain-created failed:", e);
        return NextResponse.json(
            { error: s(e?.message) || "Unknown error" },
            { status: 500 },
        );
    }
}
