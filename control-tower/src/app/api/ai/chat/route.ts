import { NextResponse } from "next/server";
import OpenAI from "openai";
import {
    appendAiEvent,
    appendConversationMessage,
    getConversation,
    getRecentEvents,
} from "@/lib/aiMemory";

export const runtime = "nodejs";

const client = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,
});

type ChatRequest = {
    agent: string;
    message: string;
    context?: Record<string, unknown>;
};

function s(v: unknown) {
    return String(v ?? "").trim();
}

function clip(v: string, n = 1400) {
    return v.length > n ? `${v.slice(0, n)}...` : v;
}

export async function POST(req: Request) {
    try {
        if (!process.env.OPENAI_API_KEY) {
            return NextResponse.json(
                { ok: false, error: "Missing OPENAI_API_KEY in environment." },
                { status: 500 },
            );
        }

        const body = (await req.json()) as ChatRequest;
        const agent = s(body?.agent || "overview");
        const userMsg = s(body?.message);
        const context = body?.context || {};

        if (!userMsg) {
            return NextResponse.json({ ok: false, error: "Missing message." }, { status: 400 });
        }

        await appendConversationMessage(agent, { role: "user", content: userMsg });
        await appendAiEvent({
            agent,
            kind: "chat_turn",
            summary: `User asked: ${clip(userMsg, 180)}`,
            metadata: { role: "user" },
        });

        const convo = await getConversation(agent, 30);
        const events = await getRecentEvents(80);
        const eventsCompact = events.map((e) => ({
            ts: e.ts,
            agent: e.agent,
            kind: e.kind,
            summary: e.summary,
        }));

        const response = await client.responses.create({
            model: "gpt-5.2",
            reasoning: { effort: "none" },
            input: [
                {
                    role: "system",
                    content:
                        "You are a multi-dashboard business copilot with CEO reasoning. " +
                        "You can collaborate across Calls, Leads, GSC, GA, Ads and Overview agents. " +
                        "Use conversation history, recent AI events, and provided context. " +
                        "Be concrete, action-oriented, and cite numeric evidence from context when available. " +
                        "If data/setup is missing, clearly call it out and propose next best steps.",
                },
                {
                    role: "user",
                    content: JSON.stringify({
                        agent,
                        context,
                        recent_events: eventsCompact,
                        conversation: convo.map((m) => ({ role: m.role, content: m.content })),
                        current_question: userMsg,
                    }),
                },
            ],
            temperature: 0.3,
        });

        const outText = s((response as any)?.output_text || "");
        if (!outText) {
            return NextResponse.json(
                { ok: false, error: "Empty model output." },
                { status: 502 },
            );
        }

        await appendConversationMessage(agent, { role: "assistant", content: outText });
        await appendAiEvent({
            agent,
            kind: "chat_turn",
            summary: `Assistant replied: ${clip(outText, 220)}`,
            metadata: { role: "assistant" },
        });

        const history = await getConversation(agent, 50);
        return NextResponse.json({ ok: true, reply: outText, history });
    } catch (e: unknown) {
        return NextResponse.json(
            { ok: false, error: e instanceof Error ? e.message : "chat failed" },
            { status: 500 },
        );
    }
}
