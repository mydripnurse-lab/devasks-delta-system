import { NextResponse } from "next/server";
import OpenAI from "openai";
import { appendAiEvent } from "@/lib/aiMemory";

export const runtime = "nodejs";

const client = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,
});

function safeJsonStringify(x: any, maxChars = 160_000) {
    let s = "";
    try {
        s = JSON.stringify(x);
    } catch {
        s = String(x ?? "");
    }
    if (s.length <= maxChars) return s;
    return s.slice(0, maxChars) + `\n\n[TRUNCATED ${s.length - maxChars} chars]`;
}

export async function POST(req: Request) {
    try {
        if (!process.env.OPENAI_API_KEY) {
            return NextResponse.json(
                { ok: false, error: "Missing OPENAI_API_KEY in environment." },
                { status: 500 },
            );
        }

        const payload = await req.json();

        // ✅ Schema estricto y consistente con el agente GSC
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
                            expected_impact: { type: "string", enum: ["low", "medium", "high"] },
                            why_it_matters: { type: "string" },
                            evidence: { type: "string" },
                            recommended_actions: {
                                type: "array",
                                items: { type: "string" },
                                maxItems: 7,
                            },
                        },
                        required: ["title", "expected_impact", "why_it_matters", "evidence", "recommended_actions"],
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

        // ✅ System prompt ultra-especializado para GA4 + State drill-down
        const systemPrompt =
            "You are an elite Google Analytics 4 (GA4) data analyst and conversion strategist.\n" +
            "Your job: produce concise, action-oriented insights that improve revenue outcomes.\n\n" +
            "Hard rules:\n" +
            "1) Use ONLY the provided JSON data. Do not invent metrics or claim access to GA.\n" +
            "2) If a limitation exists (e.g., missing conversion tracking, (not set) regions/cities), mention it explicitly.\n" +
            "3) Prioritize measurable outcomes: more qualified sessions, higher engagement rate, more conversions, better landing performance.\n" +
            "4) Reference evidence directly from the dataset fields (summaryOverall, compare, trendFiltered, stateRows, topCities, topLanding, topSourceMedium, meta).\n" +
            "5) Be decisive: pick the main risk and main opportunity.\n\n" +
            "What to analyze (must consider all if available):\n" +
            "- Summary overall: sessions, users, views, engagementRate, conversions; note timeframe (startDate/endDate).\n" +
            "- Compare: identify meaningful deltas and % changes, and interpret whether growth is quality or noise.\n" +
            "- TrendFiltered: detect spikes/drops, relate to acquisition or landing issues.\n" +
            "- State/Region rows: concentration risk, winners/laggards, presence of Puerto Rico, impact of __unknown/(not set).\n" +
            "- Top Cities: spot cities with high sessions but weak engagement/conversions; detect PR cities signal.\n" +
            "- Top Landing Pages: identify pages with traffic but low conversions/engagement; call out what to improve (CTA, speed, copy, form friction).\n" +
            "- Source/Medium: identify channels driving volume vs quality; recommend reallocations or fixes.\n\n" +
            "Output must be VALID JSON per the given schema.";

        const inputData = safeJsonStringify(payload, 170_000);

        const resp = await client.responses.create({
            model: "gpt-5.2",
            reasoning: { effort: "none" }, // rápido + consistente
            input: [
                { role: "system", content: systemPrompt },
                { role: "user", content: inputData },
            ],
            text: {
                format: {
                    type: "json_schema",
                    name: "ga_dashboard_insights",
                    schema,
                },
            },
            temperature: 0.25,
        });

        let outText = (resp as any).output_text as string | undefined;

        if (!outText) {
            outText =
                resp.output
                    ?.flatMap((o: any) => o.content || [])
                    ?.find((c: any) => c.type === "output_text")?.text || "";
        }

        if (!outText) {
            return NextResponse.json({ ok: false, error: "Empty model output." }, { status: 502 });
        }

        let insights: any = null;
        try {
            insights = JSON.parse(outText);
        } catch {
            return NextResponse.json(
                {
                    ok: false,
                    error: "Model did not return valid JSON. Try again or reduce payload size.",
                    raw: outText.slice(0, 1200),
                },
                { status: 502 },
            );
        }

        await appendAiEvent({
            agent: "ga",
            kind: "insight_run",
            summary: `GA insights generated (${String(insights?.scorecard?.health || "mixed")})`,
            metadata: {
                health: insights?.scorecard?.health || null,
                risk: insights?.scorecard?.primary_risk || null,
                opportunity: insights?.scorecard?.primary_opportunity || null,
            },
        });

        return NextResponse.json({ ok: true, insights }, { status: 200 });
    } catch (e: any) {
        return NextResponse.json(
            { ok: false, error: e?.message || "Failed to generate GA insights" },
            { status: 500 },
        );
    }
}
