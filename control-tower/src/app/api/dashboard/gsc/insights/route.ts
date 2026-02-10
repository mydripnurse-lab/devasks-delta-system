// control-tower/src/app/api/dashboard/gsc/insights/route.ts
import { NextResponse } from "next/server";
import OpenAI from "openai";
import { appendAiEvent } from "@/lib/aiMemory";

export const runtime = "nodejs";

const client = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,
});

function safeJsonStringify(x: any, maxChars = 140_000) {
    // evita payloads gigantes (GSC top tables pueden ser grandes)
    let s = "";
    try {
        s = JSON.stringify(x);
    } catch {
        // fallback: stringify “suave”
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

        // ✅ JSON schema estricto (igual patrón que Calls)
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
            required: ["executive_summary", "scorecard", "opportunities", "quick_wins_next_7_days", "experiments_next_30_days"],
        };

        // ✅ System prompt especializado para GSC + Delta
        // Importante: no inventar métricas; usar SOLO lo que viene en payload.
        const systemPrompt =
            "You are an elite Google Search Console (GSC) data analyst and SEO strategist for the Delta System.\n" +
            "Your job: produce concise, action-oriented insights that improve organic performance.\n\n" +
            "Hard rules:\n" +
            "1) Use ONLY the provided JSON data. Do not invent metrics or claim access to GSC.\n" +
            "2) If a limitation exists (e.g., queries not state-filterable), mention it.\n" +
            "3) Prioritize measurable outcomes: CTR lift, position improvement, impressions expansion, Delta coverage alignment.\n" +
            "4) Reference evidence directly from the dataset fields (summary, compare, trend, top, states, debug).\n\n" +
            "What to analyze (must consider all):\n" +
            "- Summary: impressions, clicks, CTR, avg position, pagesCounted.\n" +
            "- Compare (if present): previous window deltas and % changes.\n" +
            "- Trend: detect spikes, drops, seasonality; connect to actions (indexing, internal links, titles).\n" +
            "- Top Queries: opportunities where impressions high but CTR low; and where position is 8–20.\n" +
            "- Top Pages: pages with high impressions but low CTR; pages with position in striking distance.\n" +
            "- States table: concentration risk, winners/laggards, __unknown implications.\n" +
            "- Delta System coverage: explain what __unknown likely means and how to reduce it using catalog/URL patterns.\n\n" +
            "Output must be VALID JSON per the given schema.";

        // ✅ Reduce payload size prudently (pero aún usa “toda la data” del dashboard)
        // Recomendación: mandar top 25 ya lo estás haciendo; igual aquí protegemos por si llega grande.
        const inputData = safeJsonStringify(payload, 160_000);

        const resp = await client.responses.create({
            model: "gpt-5.2",
            reasoning: { effort: "none" }, // rápido + consistente (como Calls)
            input: [
                { role: "system", content: systemPrompt },
                { role: "user", content: inputData },
            ],
            text: {
                format: {
                    type: "json_schema",
                    name: "gsc_dashboard_insights",
                    schema,
                },
            },
            temperature: 0.25,
        });

        // ✅ output_text (SDK) + fallback robusto
        let outText = (resp as any).output_text as string | undefined;

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
                    error: "Model did not return valid JSON. Try again or reduce payload size.",
                    raw: outText.slice(0, 1200),
                },
                { status: 502 },
            );
        }

        await appendAiEvent({
            agent: "gsc",
            kind: "insight_run",
            summary: `GSC insights generated (${String(insights?.scorecard?.health || "mixed")})`,
            metadata: {
                health: insights?.scorecard?.health || null,
                risk: insights?.scorecard?.primary_risk || null,
                opportunity: insights?.scorecard?.primary_opportunity || null,
            },
        });

        return NextResponse.json({ ok: true, insights }, { status: 200 });
    } catch (e: any) {
        return NextResponse.json(
            { ok: false, error: e?.message || "Failed to generate GSC insights" },
            { status: 500 },
        );
    }
}
