// src/app/api/sheet/headers/route.ts
import { NextResponse } from "next/server";

export const runtime = "nodejs";

function s(v: any) {
    return String(v ?? "").trim();
}

function getSpreadsheetId() {
    // soporta ambos nombres para que no te vuelva a romper
    return (
        s(process.env.GOOGLE_SHEETS_SPREADSHEET_ID) ||
        s(process.env.GOOGLE_SHEET_ID) ||
        s(process.env.SPREADSHEET_ID) ||
        s(process.env.SHEET_ID)
    );
}

function maskId(id: string) {
    const x = s(id);
    if (x.length <= 8) return "***";
    return `${x.slice(0, 4)}…${x.slice(-4)}`;
}

function pickHeader(headers: string[], candidates: string[], envOverride?: string) {
    const normalized = headers.map((h) => s(h));
    const set = new Set(normalized);

    const override = s(envOverride);
    if (override && set.has(override)) return override;

    for (const c of candidates) {
        if (set.has(c)) return c;
    }
    return "";
}

async function getSheetsClient() {
    // usa TU sheetsClient.js (mismo que ya funciona en /api/sheet/state)
    const mod = await import("../../../../../../services/sheetsClient.js");
    return mod as any;
}

export async function GET(req: Request) {
    const url = new URL(req.url);
    const locId = s(url.searchParams.get("locId")); // opcional ahora

    const spreadsheetId = getSpreadsheetId();
    if (!spreadsheetId) {
        return NextResponse.json(
            {
                error:
                    "Missing spreadsheetId env. Set GOOGLE_SHEET_ID (recommended) or GOOGLE_SHEETS_SPREADSHEET_ID.",
            },
            { status: 500 }
        );
    }

    // defaults
    const sheetName = s(process.env.GOOGLE_SHEET_HEADERS_TAB) || "Headers";
    const range = s(process.env.GOOGLE_SHEET_HEADERS_RANGE) || "A:ZZ";

    try {
        const sc = await getSheetsClient();
        const { loadSheetTabIndex } = sc;

        if (typeof loadSheetTabIndex !== "function") {
            return NextResponse.json(
                {
                    error: "sheetsClient.loadSheetTabIndex is not a function",
                    debug: {
                        spreadsheetId: maskId(spreadsheetId),
                        sheetName,
                        range,
                        exports: Object.keys(sc || {}),
                    },
                },
                { status: 500 }
            );
        }

        const idx = await loadSheetTabIndex({
            spreadsheetId,
            sheetName,
            range,
            logScope: "headers",
        });

        const headers: string[] = Array.isArray(idx?.headers) ? idx.headers : [];
        const rows: any[][] = Array.isArray(idx?.rows) ? idx.rows : [];
        const headerMap: Map<string, number> = idx?.headerMap || new Map();

        // detect columns
        const COL_LOCID = pickHeader(
            headers,
            ["Location Id", "LocationID", "location id", "locationId", "Loc Id", "locId", "Location"],
            process.env.HEADERS_COL_LOCATION_ID
        );

        const COL_HEAD = pickHeader(
            headers,
            ["Head", "HEAD", "Head HTML", "Global Head"],
            process.env.HEADERS_COL_HEAD
        );

        const COL_FOOTER = pickHeader(
            headers,
            ["Footer", "FOOTER", "Footer HTML", "Global Footer"],
            process.env.HEADERS_COL_FOOTER
        );

        const COL_FAVICON = pickHeader(
            headers,
            ["Favicon", "FAVICON", "Favicon URL", "Favicon Link", "Icon", "Icon URL"],
            process.env.HEADERS_COL_FAVICON
        );

        const headCol = COL_HEAD ? headerMap.get(COL_HEAD) : undefined;
        const footerCol = COL_FOOTER ? headerMap.get(COL_FOOTER) : undefined;
        const faviconCol = COL_FAVICON ? headerMap.get(COL_FAVICON) : undefined;

        // --- MODE A: per-location (si existe Location Id) ---
        if (COL_LOCID) {
            const locCol = headerMap.get(COL_LOCID);

            if (locCol === undefined) {
                return NextResponse.json(
                    {
                        error: "Headers tab: Location Id column exists but headerMap didn’t map it.",
                        debug: {
                            spreadsheetId: maskId(spreadsheetId),
                            sheetName,
                            range,
                            COL_LOCID,
                            headerMapKeys: Array.from(headerMap.keys()),
                        },
                    },
                    { status: 500 }
                );
            }

            if (!locId) {
                return NextResponse.json(
                    {
                        error: "Missing locId (Headers tab is per-location because it has Location Id column).",
                        debug: { sheetName, spreadsheetId: maskId(spreadsheetId) },
                    },
                    { status: 400 }
                );
            }

            let foundIndex = -1;
            for (let i = 0; i < rows.length; i++) {
                const r = rows[i] || [];
                if (s(r[locCol]) === locId) {
                    foundIndex = i;
                    break;
                }
            }

            if (foundIndex < 0) {
                return NextResponse.json(
                    {
                        head: "",
                        footer: "",
                        favicon: "",
                        source: { mode: "per-location", match: "none", key: locId },
                    },
                    { status: 200 }
                );
            }

            const r = rows[foundIndex] || [];

            return NextResponse.json(
                {
                    head: headCol === undefined ? "" : s(r[headCol]),
                    footer: footerCol === undefined ? "" : s(r[footerCol]),
                    favicon: faviconCol === undefined ? "" : s(r[faviconCol]),
                    source: { mode: "per-location", key: locId, row: foundIndex + 2 },
                    cols: {
                        locationId: COL_LOCID,
                        head: COL_HEAD || "",
                        footer: COL_FOOTER || "",
                        favicon: COL_FAVICON || "",
                    },
                },
                { status: 200 }
            );
        }

        // --- MODE B: global (tu caso: NO existe Location Id) ---
        // Tomamos la primera fila de data (row 2 del sheet) como "global config"
        const first = rows[0] || [];

        return NextResponse.json(
            {
                head: headCol === undefined ? "" : s(first[headCol]),
                footer: footerCol === undefined ? "" : s(first[footerCol]),
                favicon: faviconCol === undefined ? "" : s(first[faviconCol]),
                source: { mode: "global", row: rows.length ? 2 : null },
                cols: {
                    head: COL_HEAD || "",
                    footer: COL_FOOTER || "",
                    favicon: COL_FAVICON || "",
                },
                debug: {
                    note:
                        "Global headers mode: tab has no Location Id column. Using first data row as global config.",
                    sheetName,
                    spreadsheetId: maskId(spreadsheetId),
                },
            },
            { status: 200 }
        );
    } catch (e: any) {
        return NextResponse.json(
            {
                error: e?.message || "Failed to read Headers tab",
                debug: {
                    spreadsheetId: maskId(spreadsheetId),
                    sheetName,
                    range,
                    locId,
                    stack: process.env.NODE_ENV !== "production" ? String(e?.stack || "") : undefined,
                },
            },
            { status: 500 }
        );
    }
}
