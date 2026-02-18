import { NextResponse } from "next/server";
import { pingDb } from "@/lib/db";

export const runtime = "nodejs";

export async function GET() {
  try {
    const row = await pingDb();
    return NextResponse.json({
      ok: true,
      status: "connected",
      db: row?.db_name || null,
      user: row?.db_user || null,
      dbTimeUtc: row?.now_utc || null,
      checkedAt: new Date().toISOString(),
    });
  } catch (error: unknown) {
    const message =
      error instanceof Error ? error.message : "Database health check failed";
    return NextResponse.json(
      {
        ok: false,
        status: "error",
        error: message,
        checkedAt: new Date().toISOString(),
      },
      { status: 500 },
    );
  }
}
