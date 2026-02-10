import { NextResponse } from "next/server";
import { getConversation, getRecentEvents } from "@/lib/aiMemory";

export const runtime = "nodejs";

function s(v: unknown) {
    return String(v ?? "").trim();
}

export async function GET(req: Request) {
    try {
        const { searchParams } = new URL(req.url);
        const agent = s(searchParams.get("agent") || "overview");

        const [history, events] = await Promise.all([
            getConversation(agent, 80),
            getRecentEvents(120),
        ]);

        return NextResponse.json({
            ok: true,
            agent,
            history,
            events,
        });
    } catch (e: unknown) {
        return NextResponse.json(
            { ok: false, error: e instanceof Error ? e.message : "history failed" },
            { status: 500 },
        );
    }
}
