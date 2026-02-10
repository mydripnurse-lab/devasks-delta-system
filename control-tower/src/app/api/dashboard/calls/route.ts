import { NextResponse } from "next/server";
import { loadSheetTabIndex } from "../../../../../../services/sheetsClient.js";

export const runtime = "nodejs";

function s(v: any) {
    return String(v ?? "").trim();
}

// Google Sheets serial date -> JS Date (days since 1899-12-30)
function sheetSerialToDate(serial: number): Date {
    const epoch = Date.UTC(1899, 11, 30);
    const ms = epoch + serial * 24 * 60 * 60 * 1000;
    return new Date(ms);
}

function parseDateLoose(v: any): Date | null {
    if (v === null || v === undefined || v === "") return null;

    if (typeof v === "number" && Number.isFinite(v)) {
        const d = sheetSerialToDate(v);
        return Number.isNaN(d.getTime()) ? null : d;
    }

    const t = s(v);
    if (!t) return null;

    const d1 = new Date(t);
    if (!Number.isNaN(d1.getTime())) return d1;

    const m = t.match(
        /^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?)?/i,
    );
    if (m) {
        const mm = Number(m[1]);
        const dd = Number(m[2]);
        let yy = Number(m[3]);
        if (yy < 100) yy += 2000;

        let hh = Number(m[4] ?? "0");
        const mi = Number(m[5] ?? "0");
        const ss = Number(m[6] ?? "0");
        const ap = (m[7] ?? "").toUpperCase();

        if (ap === "PM" && hh < 12) hh += 12;
        if (ap === "AM" && hh === 12) hh = 0;

        const d2 = new Date(yy, mm - 1, dd, hh, mi, ss);
        return Number.isNaN(d2.getTime()) ? null : d2;
    }

    return null;
}

function rowsToObjects(headers: string[], rows: any[][]) {
    return rows.map((arr) => {
        const obj: Record<string, any> = {};
        for (let i = 0; i < headers.length; i++) {
            const key = headers[i] || `col_${i}`;
            obj[key] = arr?.[i] ?? "";
        }
        return obj;
    });
}

// ===== State normalization =====
const STATE_CODE_TO_NAME: Record<string, string> = {
    AL: "Alabama",
    AK: "Alaska",
    AZ: "Arizona",
    AR: "Arkansas",
    CA: "California",
    CO: "Colorado",
    CT: "Connecticut",
    DE: "Delaware",
    FL: "Florida",
    GA: "Georgia",
    HI: "Hawaii",
    ID: "Idaho",
    IL: "Illinois",
    IN: "Indiana",
    IA: "Iowa",
    KS: "Kansas",
    KY: "Kentucky",
    LA: "Louisiana",
    ME: "Maine",
    MD: "Maryland",
    MA: "Massachusetts",
    MI: "Michigan",
    MN: "Minnesota",
    MS: "Mississippi",
    MO: "Missouri",
    MT: "Montana",
    NE: "Nebraska",
    NV: "Nevada",
    NH: "New Hampshire",
    NJ: "New Jersey",
    NM: "New Mexico",
    NY: "New York",
    NC: "North Carolina",
    ND: "North Dakota",
    OH: "Ohio",
    OK: "Oklahoma",
    OR: "Oregon",
    PA: "Pennsylvania",
    RI: "Rhode Island",
    SC: "South Carolina",
    SD: "South Dakota",
    TN: "Tennessee",
    TX: "Texas",
    UT: "Utah",
    VT: "Vermont",
    VA: "Virginia",
    WA: "Washington",
    WV: "West Virginia",
    WI: "Wisconsin",
    WY: "Wyoming",
    DC: "District of Columbia",
    PR: "Puerto Rico",
};

function normalizeState(raw: any) {
    const t = s(raw);
    if (!t) return "";

    const upper = t.toUpperCase();

    if (upper === "PR" || upper === "PUERTO RICO") return "Puerto Rico";
    if (upper.length === 2 && STATE_CODE_TO_NAME[upper]) return STATE_CODE_TO_NAME[upper];

    const wanted = t.toLowerCase();
    for (const name of Object.values(STATE_CODE_TO_NAME)) {
        if (name.toLowerCase() === wanted) return name;
    }

    return t;
}

export async function GET(req: Request) {
    try {
        const url = new URL(req.url);
        const start = s(url.searchParams.get("start"));
        const end = s(url.searchParams.get("end"));

        const startMs = start ? new Date(start).getTime() : null;
        const endMs = end ? new Date(end).getTime() : null;

        const spreadsheetId =
            process.env.GOOGLE_SHEETS_SPREADSHEET_ID ||
            process.env.GOOGLE_SHEET_ID ||
            process.env.SPREADSHEET_ID ||
            "";

        if (!spreadsheetId) {
            return NextResponse.json(
                { ok: false, error: "Missing spreadsheetId env (GOOGLE_SHEETS_SPREADSHEET_ID / GOOGLE_SHEET_ID)" },
                { status: 500 },
            );
        }

        // ✅ Tab correcto: "Call Report"
        const idx = await loadSheetTabIndex({
            spreadsheetId,
            sheetName: "Call Report",
            range: "A:ZZ",
            logScope: "calls-dashboard",
        });

        const headers = (idx.headers || []).map((h: any) => String(h || "").trim());
        const rows = idx.rows || [];

        let objects = rowsToObjects(headers, rows).map((r: any) => {
            const d = parseDateLoose(r["Phone Call Start Time"]);
            return {
                ...r,
                __startIso: d ? d.toISOString() : "",
                __startMs: d ? d.getTime() : null,
                __fromStateNorm: normalizeState(r["Phone Call From State"]),
            };
        });

        if (startMs !== null && endMs !== null) {
            objects = objects.filter((r: any) => {
                if (!r.__startMs) return false;
                return r.__startMs >= startMs && r.__startMs <= endMs;
            });
        }

        const byState: Record<string, number> = {};
        for (const r of objects) {
            const st = s(r.__fromStateNorm);
            if (!st) continue;
            byState[st] = (byState[st] || 0) + 1;
        }

        return NextResponse.json({
            ok: true,
            total: objects.length,
            byState,
            rows: objects,
        });
    } catch (e: any) {
        return NextResponse.json(
            { ok: false, error: e?.message || "Failed to load Calls Dashboard" },
            { status: 500 },
        );
    }
}
