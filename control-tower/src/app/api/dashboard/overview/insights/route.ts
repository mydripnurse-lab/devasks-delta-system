import { NextResponse } from "next/server";
import OpenAI from "openai";
import { appendAiEvent } from "@/lib/aiMemory";

export const runtime = "nodejs";

const client = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,
});

type ResponseOutputText = {
    output_text?: string;
    output?: Array<{
        content?: Array<{
            type?: string;
            text?: string;
        }>;
    }>;
};

export async function POST(req: Request) {
    try {
        if (!process.env.OPENAI_API_KEY) {
            return NextResponse.json(
                { ok: false, error: "Missing OPENAI_API_KEY in environment." },
                { status: 500 },
            );
        }

        const payload = await req.json();

        const schema = {
            type: "object",
            additionalProperties: false,
            properties: {
                ceo_summary: { type: "string" },
                board_meeting_narrative: { type: "string" },
                board_scorecard: {
                    type: "object",
                    additionalProperties: false,
                    properties: {
                        health: { type: "string", enum: ["good", "mixed", "bad"] },
                        biggest_risk: { type: "string" },
                        biggest_opportunity: { type: "string" },
                    },
                    required: ["health", "biggest_risk", "biggest_opportunity"],
                },
                swarm_coordination: {
                    type: "array",
                    maxItems: 6,
                    items: {
                        type: "object",
                        additionalProperties: false,
                        properties: {
                            owner_agent: { type: "string" },
                            mission: { type: "string" },
                            expected_business_impact: { type: "string", enum: ["low", "medium", "high"] },
                            dependencies: {
                                type: "array",
                                items: { type: "string" },
                                maxItems: 6,
                            },
                        },
                        required: ["owner_agent", "mission", "expected_business_impact", "dependencies"],
                    },
                },
                decisions_next_7_days: {
                    type: "array",
                    items: { type: "string" },
                    maxItems: 8,
                },
                decisions_next_30_days: {
                    type: "array",
                    items: { type: "string" },
                    maxItems: 8,
                },
                execute_plan: {
                    type: "array",
                    maxItems: 8,
                    items: {
                        type: "object",
                        additionalProperties: false,
                        properties: {
                            priority: { type: "string", enum: ["P1", "P2", "P3"] },
                            action: { type: "string" },
                            dashboard: {
                                type: "string",
                                enum: ["calls", "leads", "conversations", "transactions", "appointments", "gsc", "ga", "ads", "facebook_ads"],
                            },
                            rationale: { type: "string" },
                            trigger_metric: { type: "string" },
                        },
                        required: ["priority", "action", "dashboard", "rationale", "trigger_metric"],
                    },
                },
            },
            required: [
                "ceo_summary",
                "board_meeting_narrative",
                "board_scorecard",
                "swarm_coordination",
                "decisions_next_7_days",
                "decisions_next_30_days",
                "execute_plan",
            ],
        };

        const resp = await client.responses.create({
            model: "gpt-5.2",
            reasoning: { effort: "none" },
            input: [
                {
                    role: "system",
                    content:
                        "You are the CEO and board strategist for a multi-dashboard growth stack. " +
                        "Act as a swarm coordinator across specialist agents: Calls, Leads, Conversations, Transactions, Appointments, GSC, GA, Ads. " +
                        "Make executive decisions with direct business impact. " +
                        "Return an execution-first board meeting narrative and concrete plan items with priorities. " +
                        "Use only provided data. Never invent metrics.",
                },
                {
                    role: "user",
                    content: JSON.stringify(payload),
                },
            ],
            text: {
                format: {
                    type: "json_schema",
                    name: "overview_ceo_insights",
                    schema,
                },
            },
            temperature: 0.3,
        });

        const out = resp as ResponseOutputText;
        let outText = out.output_text;
        if (!outText) {
            outText =
                out.output
                    ?.flatMap((o) => o.content || [])
                    ?.find((c) => c.type === "output_text")?.text || "";
        }

        if (!outText) {
            return NextResponse.json(
                { ok: false, error: "Empty model output." },
                { status: 502 },
            );
        }

        let insights: unknown = null;
        try {
            insights = JSON.parse(outText);
        } catch {
            return NextResponse.json(
                { ok: false, error: "Model did not return valid JSON.", raw: outText.slice(0, 800) },
                { status: 502 },
            );
        }

        const parsed = insights as Record<string, any>;
        await appendAiEvent({
            agent: "overview",
            kind: "insight_run",
            summary: `CEO insights generated (${String(parsed?.board_scorecard?.health || "mixed")})`,
            metadata: {
                health: parsed?.board_scorecard?.health || null,
                risk: parsed?.board_scorecard?.biggest_risk || null,
                opportunity: parsed?.board_scorecard?.biggest_opportunity || null,
            },
        });

        return NextResponse.json({ ok: true, insights });
    } catch (e: unknown) {
        return NextResponse.json(
            { ok: false, error: e instanceof Error ? e.message : "Failed to generate insights" },
            { status: 500 },
        );
    }
}
