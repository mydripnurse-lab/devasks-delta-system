import { NextResponse } from "next/server";
import { loadSheetTabIndex } from "../../../../../../services/sheetsClient.js";

export const runtime = "nodejs";

const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID!;
const COUNTY_TAB = process.env.GOOGLE_SHEET_COUNTY_TAB || "Counties";
const CITY_TAB = process.env.GOOGLE_SHEET_CITY_TAB || "Cities";

function s(v: any) {
    return String(v ?? "").trim();
}
function isTrue(v: any) {
    const t = s(v).toLowerCase();
    return t === "true" || t === "1" || t === "yes" || t === "y";
}
function nonEmpty(v: any) {
    return s(v) !== "";
}

function rowToObj(headers: string[], row: any[]) {
    const o: Record<string, any> = {};
    for (let i = 0; i < headers.length; i++) o[headers[i]] = row[i];
    return o;
}

function buildStatePayload(tab: any, stateName: string) {
    const idxState = tab.headerMap.get("State");
    const idxStatus = tab.headerMap.get("Status");
    const idxLocId = tab.headerMap.get("Location Id");

    if (idxState === undefined) throw new Error(`Missing header "State" in ${tab.sheetName}`);
    if (idxStatus === undefined) throw new Error(`Missing header "Status" in ${tab.sheetName}`);
    if (idxLocId === undefined) throw new Error(`Missing header "Location Id" in ${tab.sheetName}`);

    const wanted = stateName.toLowerCase();

    const rows: any[] = [];
    let stats = {
        total: 0,
        statusTrue: 0,
        hasLocId: 0,
        eligible: 0,
    };

    for (const r of tab.rows) {
        stats.total++;
        const state = s(r[idxState]).toLowerCase();
        if (!state || state !== wanted) continue;

        const statusOK = isTrue(r[idxStatus]);
        const locOK = nonEmpty(r[idxLocId]);

        if (statusOK) stats.statusTrue++;
        if (locOK) stats.hasLocId++;
        if (statusOK && locOK) stats.eligible++;

        const obj = rowToObj(tab.headers, r);
        obj.__eligible = statusOK && locOK;
        rows.push(obj);
    }

    const idxCounty = tab.headerMap.get("County");
    const counties = new Set<string>();
    if (idxCounty !== undefined) {
        for (const row of rows) {
            const c = s(row["County"]);
            if (c) counties.add(c);
        }
    }

    return {
        headers: tab.headers,
        rows,
        stats,
        counties: Array.from(counties).sort((a, b) => a.localeCompare(b)),
    };
}

export async function GET(req: Request) {
    try {
        if (!SPREADSHEET_ID) {
            return NextResponse.json({ error: "Missing GOOGLE_SHEET_ID" }, { status: 500 });
        }

        const { searchParams } = new URL(req.url);
        const state = s(searchParams.get("name"));
        if (!state) return NextResponse.json({ error: 'Missing query param "?name=StateName"' }, { status: 400 });

        const [countiesTab, citiesTab] = await Promise.all([
            loadSheetTabIndex({
                spreadsheetId: SPREADSHEET_ID,
                sheetName: COUNTY_TAB,
                range: "A:AZ",
                keyHeaders: ["State", "County"],
            }),
            loadSheetTabIndex({
                spreadsheetId: SPREADSHEET_ID,
                sheetName: CITY_TAB,
                range: "A:AZ",
                keyHeaders: ["State", "County", "City"],
            }),
        ]);

        const counties = buildStatePayload(countiesTab, state);
        const cities = buildStatePayload(citiesTab, state);

        return NextResponse.json({
            state,
            tabs: { counties: COUNTY_TAB, cities: CITY_TAB },
            counties,
            cities,
        });
    } catch (e: any) {
        return NextResponse.json({ error: e?.message || "Unknown error" }, { status: 500 });
    }
}
