// services/sheetsClient.js
import fs from "fs/promises";
import fsSync from "fs";
import path from "path";
import { google } from "googleapis";

// =====================
// CLI Args
// =====================
export function getArgValue(name) {
    const prefix = `--${name}=`;
    const hit = process.argv.find((a) => a.startsWith(prefix));
    return hit ? hit.slice(prefix.length) : null;
}

// =====================
// Utils
// =====================
export function norm(str) {
    return String(str || "")
        .trim()
        .replace(/\u00A0/g, " ") // NBSP
        .replace(/\s+/g, " ")
        .toLowerCase();
}

export function isFilled(v) {
    return v !== null && v !== undefined && String(v).trim() !== "";
}

function colToLetter(colIndex0) {
    let n = colIndex0 + 1;
    let s = "";
    while (n > 0) {
        const r = (n - 1) % 26;
        s = String.fromCharCode(65 + r) + s;
        n = Math.floor((n - 1) / 26);
    }
    return s;
}

/**
 * arma un key compuesto usando headers + valores.
 * => "colorado | morgan | brush"
 */
export function makeCompositeKey(keyHeaders, valuesByHeader = {}) {
    const headers = Array.isArray(keyHeaders) ? keyHeaders : [keyHeaders].filter(Boolean);
    return headers.map((h) => norm(valuesByHeader?.[h])).join(" | ");
}

/**
 * arma el key compuesto desde una fila real del sheet.
 */
export function makeCompositeKeyFromRow({ row, headerMap, keyHeaders }) {
    const parts = [];
    for (const h of keyHeaders) {
        const idx = headerMap.get(h);
        const val = idx === undefined ? "" : row?.[idx];
        parts.push(norm(val));
    }
    return parts.join(" | ");
}

// =====================
// Logging (optional)
// =====================
const SHEETS_LOG = String(process.env.SHEETS_LOG || "").trim() === "1";
const SHEETS_LOG_SCOPE = String(process.env.SHEETS_LOG_SCOPE || "").trim(); // optional

function ts() {
    return new Date().toISOString().replace("T", " ").replace("Z", "");
}

function scopeAllowed(scope) {
    if (!SHEETS_LOG_SCOPE) return true;
    const wanted = SHEETS_LOG_SCOPE.split(",").map((s) => s.trim()).filter(Boolean);
    if (!wanted.length) return true;
    return wanted.includes(scope);
}

function log(scope, ...args) {
    if (!SHEETS_LOG) return;
    if (!scopeAllowed(scope)) return;
    console.log(`[sheets ${ts()}]`, ...args);
}

// =====================
// Repo root resolution
// =====================
function existsSyncSafe(p) {
    try {
        return fsSync.existsSync(p);
    } catch {
        return false;
    }
}

function findRepoRoot(startDir) {
    let dir = startDir;
    for (let i = 0; i < 12; i++) {
        const hasResourcesConfig = existsSyncSafe(path.join(dir, "resources", "config"));
        const hasStatesFiles = existsSyncSafe(path.join(dir, "resources", "statesFiles"));
        const hasBuilds = existsSyncSafe(path.join(dir, "scripts", "src", "builds"));

        if (hasResourcesConfig || hasStatesFiles || hasBuilds) return dir;

        const parent = path.dirname(dir);
        if (parent === dir) break;
        dir = parent;
    }
    return startDir;
}

async function resolveKeyFileAbsolute(keyFileRaw) {
    const cwd = process.cwd();
    const repoRoot = findRepoRoot(cwd);

    const candidates = [];

    if (path.isAbsolute(keyFileRaw)) {
        candidates.push(keyFileRaw);
    } else {
        candidates.push(path.join(cwd, keyFileRaw));
        candidates.push(path.join(repoRoot, keyFileRaw));
        candidates.push(path.join(cwd, "..", keyFileRaw));
        candidates.push(path.normalize(path.join(repoRoot, keyFileRaw)));
    }

    for (const abs of candidates) {
        try {
            await fs.access(abs);
            return { absKeyFile: abs, cwd, repoRoot, tried: candidates };
        } catch {
            // continue
        }
    }

    const triedPretty = candidates.map((x) => ` - ${x}`).join("\n");
    throw new Error(
        `Google Cloud keyfile not found.\n` +
        `cwd=${cwd}\n` +
        `repoRoot=${repoRoot}\n` +
        `keyFile(raw)=${keyFileRaw}\n` +
        `Tried:\n${triedPretty}\n\n` +
        `Fix: set GOOGLE_SERVICE_ACCOUNT_KEYFILE to one of these (recommended):\n` +
        `- resources/config/google-cloud.json   (if running with cwd=repoRoot)\n` +
        `- ../resources/config/google-cloud.json (if running from control-tower)\n` +
        `Or provide an absolute path.`
    );
}

// =====================
// Safe number parsing
// =====================
function toIntSafe(v, fallback) {
    const n = Number(v);
    if (!Number.isFinite(n)) return fallback;
    const i = Math.floor(n);
    return i > 0 ? i : fallback;
}

function toMsSafe(v, fallback) {
    const n = Number(v);
    if (!Number.isFinite(n)) return fallback;
    return n >= 0 ? n : fallback;
}

// =====================
// Throttle / Delay (writes)
// =====================
const FIXED_DELAY_MS = toMsSafe(process.env.SHEETS_WRITE_DELAY_MS || "0", 0);
const SHEETS_RPM = toIntSafe(process.env.GOOGLE_SHEETS_RPM || "50", 50);

const MIN_MS_BETWEEN_SHEETS_WRITES =
    FIXED_DELAY_MS > 0 ? FIXED_DELAY_MS : Math.ceil(60000 / Math.max(1, SHEETS_RPM));

let _lastSheetsWriteAt = 0;

async function sleep(ms) {
    await new Promise((r) => setTimeout(r, ms));
}

async function sheetsWriteThrottle() {
    const now = Date.now();
    const wait = _lastSheetsWriteAt + MIN_MS_BETWEEN_SHEETS_WRITES - now;
    if (wait > 0) {
        log("throttle", `wait ${wait}ms`);
        await sleep(wait);
    }
    _lastSheetsWriteAt = Date.now();
}

// =====================
// Error wrapper (usable logs)
// =====================
export class SheetsError extends Error {
    constructor(message, { originalMessage = null, code = null, details = null } = {}) {
        super(message);
        this.name = "SheetsError";
        this.code = code;
        this.originalMessage = originalMessage;
        this.details = details;
    }
}

function extractErrDetails(err) {
    return (
        err?.response?.data ||
        err?.errors ||
        err?.cause ||
        (err?.stack ? String(err.stack) : null) ||
        null
    );
}

function extractErrCode(err) {
    return err?.code || err?.response?.status || null;
}

function toError(label, err) {
    const originalMessage = err?.message ? String(err.message) : err ? String(err) : "unknown error";
    const code = extractErrCode(err);
    const details = extractErrDetails(err);

    // 👇 IMPORTANTE: el message ahora incluye algo útil
    const msg = code ? `${label} failed (code=${code})` : `${label} failed`;

    return new SheetsError(msg, {
        originalMessage,
        code,
        details,
    });
}

// =====================
// Retry wrapper (ROBUST to NaN)
// =====================
const SHEETS_MAX_RETRIES = toIntSafe(process.env.SHEETS_MAX_RETRIES || "5", 5);

async function withRetries(fn, { label = "sheets-op", scope = "sheets" } = {}) {
    let lastErr = null;

    const max = Math.max(1, SHEETS_MAX_RETRIES);

    for (let attempt = 1; attempt <= max; attempt++) {
        try {
            return await fn();
        } catch (err) {
            lastErr = err;
            const msg = err?.message ? String(err.message) : String(err);
            log(scope, `❌ ${label} attempt ${attempt}/${max} failed: ${msg}`);

            if (attempt >= max) break;

            const backoff = Math.min(2500, 300 * attempt);
            await sleep(backoff);
        }
    }

    throw toError(label, lastErr);
}

// =====================
// Google Client
// =====================
async function getSheetsClient({ scope = "sheets" } = {}) {
    const keyFile =
        process.env.GOOGLE_SERVICE_ACCOUNT_KEYFILE ||
        process.env.GOOGLE_APPLICATION_CREDENTIALS ||
        "./google-cloud.json";

    const resolved = await resolveKeyFileAbsolute(keyFile);

    log(scope, `keyFile(raw)=${keyFile}`);
    log(scope, `keyFile(abs)=${resolved.absKeyFile}`);
    log(scope, `cwd=${resolved.cwd}`);
    log(scope, `repoRoot=${resolved.repoRoot}`);

    const auth = new google.auth.GoogleAuth({
        keyFile: resolved.absKeyFile,
        scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });

    return google.sheets({ version: "v4", auth });
}

// =====================
// Load sheet index (extended)
// =====================
export async function loadSheetIndex({
    spreadsheetId,
    sheetName,
    range = "A:Z",

    accountNameHeader = "Account Name",
    locationIdHeader = "Location Id",

    keyHeader,
    keyHeaders,

    logScope = "sheets",
} = {}) {
    if (!spreadsheetId) throw new Error("Missing spreadsheetId");
    if (!sheetName) throw new Error("Missing sheetName");

    const sheets = await getSheetsClient({ scope: logScope });
    const a1 = `${sheetName}!${range}`;

    log(logScope, `GET values: ${a1}`);

    const res = await withRetries(
        () =>
            sheets.spreadsheets.values.get({
                spreadsheetId,
                range: a1,
                valueRenderOption: "UNFORMATTED_VALUE",
            }),
        { label: `values.get ${sheetName}`, scope: logScope }
    );

    const values = res?.data?.values || [];
    const headers = (values[0] || []).map((h) => String(h || "").trim());
    const headerMap = new Map(headers.map((h, i) => [h, i]));
    const rows = values.slice(1);

    const accountNameCol = headerMap.get(accountNameHeader);
    const locationIdCol = headerMap.get(locationIdHeader);

    const mapByAccountName = new Map();

    if (accountNameCol !== undefined) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i] || [];
            const rowNumber = i + 2;
            const accountName = row[accountNameCol];
            if (!isFilled(accountName)) continue;

            const key = norm(accountName);
            const locationId = locationIdCol === undefined ? "" : row[locationIdCol] ?? "";

            const existing = mapByAccountName.get(key);
            if (!existing) {
                mapByAccountName.set(key, {
                    rowNumber,
                    accountName,
                    locationId: isFilled(locationId) ? String(locationId).trim() : "",
                    row,
                });
            } else {
                if (!isFilled(existing.locationId) && isFilled(locationId)) {
                    mapByAccountName.set(key, {
                        rowNumber,
                        accountName,
                        locationId: String(locationId).trim(),
                        row,
                    });
                }
            }
        }
    }

    const resolvedKeyHeaders =
        Array.isArray(keyHeaders) && keyHeaders.length ? keyHeaders : keyHeader ? [keyHeader] : [];

    const mapByKeyValue = new Map();

    if (resolvedKeyHeaders.length) {
        for (const h of resolvedKeyHeaders) {
            if (!headerMap.has(h)) {
                throw new Error(
                    `Sheet "${sheetName}" missing key header "${h}". Found: ${headers.join(", ")}`
                );
            }
        }

        const emptyKey = resolvedKeyHeaders.map(() => "").join(" | ");

        for (let i = 0; i < rows.length; i++) {
            const row = rows[i] || [];
            const rowNumber = i + 2;

            const key = makeCompositeKeyFromRow({ row, headerMap, keyHeaders: resolvedKeyHeaders });
            if (!key || key === emptyKey) continue;

            if (!mapByKeyValue.has(key)) {
                mapByKeyValue.set(key, { rowNumber, row });
            }
        }
    }

    log(logScope, `Loaded tab="${sheetName}" rows=${rows.length} headers=${headers.length}`);

    return {
        sheetName,
        range: a1,
        headers,
        headerMap,
        rows,

        mapByAccountName,
        accountNameCol: accountNameCol ?? -1,
        locationIdCol: locationIdCol ?? -1,

        keyHeaders: resolvedKeyHeaders,
        mapByKeyValue,
    };
}

// Alias (tu código importa esto)
export async function loadSheetTabIndex(opts = {}) {
    return loadSheetIndex(opts);
}

// =====================
// Row builders
// =====================
export function buildRowFromHeaders(headers, dataMap) {
    if (!Array.isArray(headers) || headers.length === 0) throw new Error("Missing headers array");
    const map = dataMap || {};
    return headers.map((h) => {
        const v = map[h];
        return v === undefined || v === null ? "" : v;
    });
}

// =====================
// Appends
// =====================
export async function appendRow({ spreadsheetId, sheetName, valuesArray, logScope = "sheets" } = {}) {
    if (!spreadsheetId) throw new Error("Missing spreadsheetId");
    if (!sheetName) throw new Error("Missing sheetName");
    if (!Array.isArray(valuesArray)) throw new Error("Missing valuesArray (array)");

    await sheetsWriteThrottle();
    const sheets = await getSheetsClient({ scope: logScope });

    log(logScope, `APPEND row -> ${sheetName}`);

    const res = await withRetries(
        () =>
            sheets.spreadsheets.values.append({
                spreadsheetId,
                range: `${sheetName}!A:Z`,
                valueInputOption: "RAW",
                insertDataOption: "INSERT_ROWS",
                requestBody: { values: [valuesArray] },
            }),
        { label: `values.append ${sheetName}`, scope: logScope }
    );

    const updatedRange = res?.data?.updates?.updatedRange || "";
    let rowNumber = null;
    const m = updatedRange.match(/![A-Z]+(\d+):/);
    if (m) rowNumber = Number(m[1]);

    return { updatedRange, rowNumber };
}

// =====================
// Update helpers (batch)
// =====================
export async function updateRowByHeaders(args = {}) {
    const {
        spreadsheetId,
        sheetName,

        headers,
        updatesByHeader,

        headerMap,
        updatesMap,

        dataMap,

        rowNumber,
        lookupHeader,
        lookupValue,

        logScope = "sheets",
    } = args;

    if (!spreadsheetId) throw new Error("Missing spreadsheetId");
    if (!sheetName) throw new Error("Missing sheetName");

    let resolvedHeaders = headers;
    if ((!Array.isArray(resolvedHeaders) || !resolvedHeaders.length) && headerMap) {
        resolvedHeaders = Array.from(headerMap.keys());
    }
    if (!Array.isArray(resolvedHeaders) || resolvedHeaders.length === 0) {
        throw new Error("Missing headers (loadSheetTabIndex first)");
    }

    const resolvedUpdates = updatesByHeader || updatesMap || dataMap || null;
    if (!resolvedUpdates || typeof resolvedUpdates !== "object") {
        throw new Error("Missing updatesByHeader object");
    }

    let targetRow = rowNumber;

    if (!targetRow) {
        if (!isFilled(lookupValue)) throw new Error("Missing rowNumber or lookupValue");
        const lookupH = lookupHeader || "Account Name";
        const lookupCol = resolvedHeaders.findIndex((h) => h === lookupH);
        if (lookupCol < 0) throw new Error(`Header "${lookupH}" not found in sheet headers`);

        const sheets = await getSheetsClient({ scope: logScope });
        const colLetter = colToLetter(lookupCol);
        const rangeA1 = `${sheetName}!${colLetter}:${colLetter}`;

        const res = await withRetries(
            () =>
                sheets.spreadsheets.values.get({
                    spreadsheetId,
                    range: rangeA1,
                    valueRenderOption: "UNFORMATTED_VALUE",
                }),
            { label: `values.get lookup ${sheetName}`, scope: logScope }
        );

        const colVals = res?.data?.values || [];
        const targetKey = norm(String(lookupValue));
        for (let i = 0; i < colVals.length; i++) {
            const cell = colVals[i]?.[0];
            if (!isFilled(cell)) continue;
            if (norm(String(cell)) === targetKey) {
                targetRow = i + 1;
                break;
            }
        }

        if (!targetRow || targetRow <= 1) {
            throw new Error(`Row not found by ${lookupH}="${lookupValue}"`);
        }
    }

    const data = [];
    for (const [h, v] of Object.entries(resolvedUpdates)) {
        const colIndex0 = headerMap?.get?.(h) ?? resolvedHeaders.findIndex((x) => x === h);
        if (colIndex0 === undefined || colIndex0 < 0) continue;

        const colLetter = colToLetter(colIndex0);
        const a1 = `${sheetName}!${colLetter}${targetRow}`;
        data.push({ range: a1, values: [[v === undefined || v === null ? "" : v]] });
    }

    if (!data.length) return { updated: 0, rowNumber: targetRow };

    await sheetsWriteThrottle();

    const sheets = await getSheetsClient({ scope: logScope });
    log(logScope, `BATCH UPDATE cells=${data.length} -> ${sheetName} row=${targetRow}`);

    await withRetries(
        () =>
            sheets.spreadsheets.values.batchUpdate({
                spreadsheetId,
                requestBody: { valueInputOption: "RAW", data },
            }),
        { label: `values.batchUpdate ${sheetName}`, scope: logScope }
    );

    return { updated: data.length, rowNumber: targetRow };
}
