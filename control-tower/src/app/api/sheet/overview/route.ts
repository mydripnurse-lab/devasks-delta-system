import { NextResponse } from "next/server";
import { loadSheetTabIndex } from "../../../../../../services/sheetsClient.js";

export const runtime = "nodejs";

const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID;
const COUNTY_TAB = process.env.GOOGLE_SHEET_COUNTY_TAB || "Counties";
const CITY_TAB = process.env.GOOGLE_SHEET_CITY_TAB || "Cities";

function norm(v: any) {
    return String(v ?? "").trim();
}
function isTrue(v: any) {
    const s = norm(v).toLowerCase();
    return s === "true" || s === "1" || s === "yes" || s === "y";
}
function nonEmpty(v: any) {
    return norm(v) !== "";
}

function getCell(row: any[], headerMap: Map<string, number>, header: string) {
    const idx = headerMap.get(header);
    if (idx === undefined) return "";
    return row?.[idx] ?? "";
}

function ensureStateAgg(agg: any, state: string) {
    if (!agg[state]) {
        agg[state] = {
            state,
            counties: { total: 0, statusTrue: 0, hasLocId: 0, ready: 0, domainsActive: 0 },
            cities: { total: 0, statusTrue: 0, hasLocId: 0, ready: 0, domainsActive: 0 },
        };
    }
    return agg[state];
}

export async function GET() {
    try {
        const debugEnv = {
            GOOGLE_SHEET_ID: SPREADSHEET_ID ? `${String(SPREADSHEET_ID).slice(0, 4)}***${String(SPREADSHEET_ID).slice(-4)}` : null,
            GOOGLE_SHEET_COUNTY_TAB: COUNTY_TAB,
            GOOGLE_SHEET_CITY_TAB: CITY_TAB,
            hasLoadSheetTabIndex: typeof loadSheetTabIndex === "function",
            SHEETS_LOG: process.env.SHEETS_LOG || null,
            SHEETS_LOG_SCOPE: process.env.SHEETS_LOG_SCOPE || null,
            cwd: process.cwd(),
        };

        if (!SPREADSHEET_ID) {
            return NextResponse.json({ error: "Missing GOOGLE_SHEET_ID", debugEnv }, { status: 400 });
        }
        if (typeof loadSheetTabIndex !== "function") {
            return NextResponse.json(
                { error: "loadSheetTabIndex import is not a function", debugEnv },
                { status: 500 }
            );
        }

        const counties = await loadSheetTabIndex({
            spreadsheetId: SPREADSHEET_ID,
            sheetName: COUNTY_TAB,
            range: "A:Z",
            logScope: "overview",
        });

        const cities = await loadSheetTabIndex({
            spreadsheetId: SPREADSHEET_ID,
            sheetName: CITY_TAB,
            range: "A:Z",
            logScope: "overview",
        });

        const agg: Record<string, any> = {};

        for (const row of counties.rows || []) {
            const state = norm(getCell(row, counties.headerMap, "State"));
            if (!state) continue;

            const status = getCell(row, counties.headerMap, "Status");
            const locId = getCell(row, counties.headerMap, "Location Id");
            const domainCreated = getCell(row, counties.headerMap, "Domain Created");

            const s = ensureStateAgg(agg, state);
            s.counties.total += 1;
            if (isTrue(status)) s.counties.statusTrue += 1;
            if (nonEmpty(locId)) s.counties.hasLocId += 1;
            if (isTrue(status) && nonEmpty(locId)) s.counties.ready += 1;
            if (isTrue(domainCreated)) s.counties.domainsActive += 1;
        }

        for (const row of cities.rows || []) {
            const state = norm(getCell(row, cities.headerMap, "State"));
            if (!state) continue;

            const status = getCell(row, cities.headerMap, "Status");
            const locId = getCell(row, cities.headerMap, "Location Id");
            const domainCreated = getCell(row, cities.headerMap, "Domain Created");

            const s = ensureStateAgg(agg, state);
            s.cities.total += 1;
            if (isTrue(status)) s.cities.statusTrue += 1;
            if (nonEmpty(locId)) s.cities.hasLocId += 1;
            if (isTrue(status) && nonEmpty(locId)) s.cities.ready += 1;
            if (isTrue(domainCreated)) s.cities.domainsActive += 1;
        }

        const states = Object.values(agg).sort((a: any, b: any) =>
            String(a.state).localeCompare(String(b.state))
        );

        return NextResponse.json({
            tabs: { counties: COUNTY_TAB, cities: CITY_TAB },
            states,
            debugEnv,
        });
    } catch (err: any) {
        return NextResponse.json(
            {
                error: err?.message || "Unknown error",
                debug: {
                    GOOGLE_SHEET_ID: SPREADSHEET_ID ? `${String(SPREADSHEET_ID).slice(0, 4)}***${String(SPREADSHEET_ID).slice(-4)}` : null,
                    COUNTY_TAB,
                    CITY_TAB,
                    cwd: process.cwd(),
                },
            },
            { status: 500 }
        );
    }
}
