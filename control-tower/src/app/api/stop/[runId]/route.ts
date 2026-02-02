// src/app/api/stop/[runId]/route.ts
import { NextResponse } from "next/server";
import { stopRun, getRun } from "@/lib/runStore";

export const runtime = "nodejs";

export async function POST(_req: Request, ctx: { params: Promise<{ runId: string }> }) {
    const { runId } = await ctx.params;

    if (!runId) return NextResponse.json({ ok: false, error: "Missing runId" }, { status: 400 });

    const run = getRun(runId);
    if (!run) {
        return NextResponse.json({ ok: false, error: "Run not found" }, { status: 404 });
    }

    const ok = stopRun(runId);
    return NextResponse.json({ ok: !!ok, runId });
}
