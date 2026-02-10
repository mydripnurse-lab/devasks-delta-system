import { NextResponse } from "next/server";
import OpenAI from "openai";
import { appendAiEvent } from "@/lib/aiMemory";

export const runtime = "nodejs";

const client = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,
});

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
                executive_summary: { type: "string" },
                scorecard: {
                    type: "object",
                    additionalProperties: false,
                    properties: {
                        health: { type: "string", enum: ["good", "mixed", "bad"] },
                        primary_risk: { type: "string" },
                        primary_opportunity: { type: "string" },
                    },
                    required: ["health", "primary_risk", "primary_opportunity"],
                },
                opportunities: {
                    type: "array",
                    maxItems: 6,
                    items: {
                        type: "object",
                        additionalProperties: false,
                        properties: {
                            title: { type: "string" },
                            why_it_matters: { type: "string" },
                            evidence: { type: "string" },
                            expected_impact: { type: "string", enum: ["low", "medium", "high"] },
                            recommended_actions: {
                                type: "array",
                                items: { type: "string" },
                                maxItems: 6,
                            },
                        },
                        required: [
                            "title",
                            "why_it_matters",
                            "evidence",
                            "expected_impact",
                            "recommended_actions",
                        ],
                    },
                },
                quick_wins_next_7_days: {
                    type: "array",
                    items: { type: "string" },
                    maxItems: 8,
                },
                experiments_next_30_days: {
                    type: "array",
                    items: { type: "string" },
                    maxItems: 8,
                },
            },
            required: [
                "executive_summary",
                "scorecard",
                "opportunities",
                "quick_wins_next_7_days",
                "experiments_next_30_days",
            ],
        };

        const resp = await client.responses.create({
            model: "gpt-5.2",
            reasoning: { effort: "none" }, // ✅ habilita temperatura/top_p en GPT-5.2 :contentReference[oaicite:1]{index=1}
            input: [
                {
                    role: "system",
                    content:
                        "You are an elite performance analyst for a calls dashboard. " +
                        "Return concise, specific, action-oriented insights. " +
                        "Focus on revenue impact, operational bottlenecks, and next steps. " +
                        "Use ONLY the provided JSON data; do not invent metrics.",
                },
                {
                    role: "user",
                    content: JSON.stringify(payload),
                },
            ],
            text: {
                format: {
                    type: "json_schema",
                    name: "calls_dashboard_insights",
                    schema,
                },
            },
            temperature: 0.3,
        });

        // ✅ Node SDK suele exponer esto directamente:
        let outText = (resp as any).output_text as string | undefined;

        // Fallback si no viniera:
        if (!outText) {
            outText =
                resp.output
                    ?.flatMap((o: any) => o.content || [])
                    ?.find((c: any) => c.type === "output_text")?.text || "";
        }

        if (!outText) {
            return NextResponse.json(
                { ok: false, error: "Empty model output." },
                { status: 502 },
            );
        }

        let insights: any = null;
        try {
            insights = JSON.parse(outText);
        } catch {
            return NextResponse.json(
                {
                    ok: false,
                    error:
                        "Model did not return valid JSON. Try again or reduce payload size.",
                    raw: outText.slice(0, 800),
                },
                { status: 502 },
            );
        }

        await appendAiEvent({
            agent: "calls",
            kind: "insight_run",
            summary: `Calls insights generated (${String(insights?.scorecard?.health || "mixed")})`,
            metadata: {
                health: insights?.scorecard?.health || null,
                risk: insights?.scorecard?.primary_risk || null,
                opportunity: insights?.scorecard?.primary_opportunity || null,
            },
        });

        return NextResponse.json({ ok: true, insights });
    } catch (e: any) {
        return NextResponse.json(
            { ok: false, error: e?.message || "Failed to generate insights" },
            { status: 500 },
        );
    }
}
