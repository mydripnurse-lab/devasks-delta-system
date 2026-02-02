import { NextResponse } from "next/server";
export const runtime = "nodejs";

export async function GET() {
  return NextResponse.json({
    GOOGLE_SHEET_ID: process.env.GOOGLE_SHEET_ID || null,
    GOOGLE_SHEET_COUNTY_TAB: process.env.GOOGLE_SHEET_COUNTY_TAB || null,
    GOOGLE_SHEET_CITY_TAB: process.env.GOOGLE_SHEET_CITY_TAB || null,
    GOOGLE_SERVICE_ACCOUNT_KEYFILE: process.env.GOOGLE_SERVICE_ACCOUNT_KEYFILE || null,
  });
}
