// control-tower/src/app/api/dashboard/gsc/refresh/route.ts
import { NextResponse } from "next/server";
import { refreshGscCache } from "@/lib/gscCache";

export const runtime = "nodejs";

export async function GET(req: Request) {
    try {
        const url = new URL(req.url);
        const range = url.searchParams.get("range") || "last_28_days";
        const start = url.searchParams.get("start") || "";
        const end = url.searchParams.get("end") || "";

        const out = await refreshGscCache({
            range,
            start: start || undefined,
            end: end || undefined,
        });

        return NextResponse.json({ ok: true, ...out }, { status: 200 });
    } catch (e: any) {
        return NextResponse.json(
            { ok: false, error: e?.message || "Failed to refresh GSC cache" },
            { status: 500 }
        );
    }
}
