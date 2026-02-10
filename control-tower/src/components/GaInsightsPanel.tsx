"use client";

import { useMemo, useState } from "react";
import AiAgentChatPanel from "@/components/AiAgentChatPanel";

type Props = {
  data: any; // payload completo del dashboard GA (join response + selections)
  title?: string;
};

function cls(...xs: Array<string | false | undefined | null>) {
  return xs.filter(Boolean).join(" ");
}

function healthLabel(h: string) {
  if (h === "good") return "Good";
  if (h === "bad") return "Bad";
  return "Mixed";
}

export default function GaInsightsPanel({
  data,
  title = "AI Strategist",
}: Props) {
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [insights, setInsights] = useState<any>(null);

  const canRun = useMemo(() => !!data && typeof data === "object", [data]);

  async function run() {
    if (!canRun || loading) return;
    setLoading(true);
    setErr("");

    try {
      const res = await fetch("/api/dashboard/ga/insights", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(data),
      });

      const j = await res.json().catch(() => null);
      if (!res.ok || !j?.ok) {
        throw new Error(j?.error || `Request failed (${res.status})`);
      }

      setInsights(j.insights || null);
    } catch (e: any) {
      setErr(e?.message || "Failed to generate insights");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="aiBox">
      <div className="aiBoxTop">
        <div>
          <div className="aiTitle">{title}</div>
          <div className="mini" style={{ opacity: 0.85 }}>
            Insights accionables basados en KPIs + trend + landings + sources +
            geo.
          </div>
        </div>

        <button
          className={cls("btn", "btnGhost", loading && "btnDisabled")}
          onClick={run}
          disabled={!canRun || loading}
          aria-busy={loading ? "true" : "false"}
        >
          {loading ? "Generating..." : "Generate insights"}
        </button>
      </div>

      {!insights && !err && (
        <div className="aiHint">
          Tip: Selecciona un estado (si aplica) y luego “Generate insights” para
          recomendaciones hiper-específicas.
        </div>
      )}

      {err ? <div className="aiErr mini">{err}</div> : null}

      {insights ? (
        <div className="aiBody">
          <div className="aiExecutive">{insights.executive_summary}</div>

          <div className="aiScoreRow">
            <div className="aiScore">
              <div className="mini" style={{ opacity: 0.75 }}>
                Health
              </div>
              <div
                className={cls(
                  "aiBadge",
                  `aiBadge-${insights?.scorecard?.health || "mixed"}`,
                )}
              >
                {healthLabel(insights?.scorecard?.health || "mixed")}
              </div>
            </div>

            <div className="aiScore">
              <div className="mini" style={{ opacity: 0.75 }}>
                Primary risk
              </div>
              <div className="aiScoreText">
                {insights?.scorecard?.primary_risk}
              </div>
            </div>

            <div className="aiScore">
              <div className="mini" style={{ opacity: 0.75 }}>
                Primary opportunity
              </div>
              <div className="aiScoreText">
                {insights?.scorecard?.primary_opportunity}
              </div>
            </div>
          </div>

          {Array.isArray(insights.opportunities) &&
          insights.opportunities.length ? (
            <div className="aiSection">
              <div className="aiSectionTitle">Opportunities</div>
              <div className="aiCards">
                {insights.opportunities.map((o: any, i: number) => (
                  <div className="aiCard" key={`opp-${i}`}>
                    <div className="aiCardTop">
                      <div className="aiCardTitle">{o.title}</div>
                      <div
                        className={cls(
                          "aiImpact",
                          `aiImpact-${o.expected_impact || "medium"}`,
                        )}
                      >
                        {String(o.expected_impact || "medium").toUpperCase()}
                      </div>
                    </div>

                    <div className="aiCardBody">
                      <div className="mini" style={{ opacity: 0.75 }}>
                        Why it matters
                      </div>
                      <div className="aiP">{o.why_it_matters}</div>

                      <div
                        className="mini"
                        style={{ opacity: 0.75, marginTop: 10 }}
                      >
                        Evidence
                      </div>
                      <div className="aiP">{o.evidence}</div>

                      {Array.isArray(o.recommended_actions) &&
                      o.recommended_actions.length ? (
                        <>
                          <div
                            className="mini"
                            style={{ opacity: 0.75, marginTop: 10 }}
                          >
                            Recommended actions
                          </div>
                          <ul className="aiList">
                            {o.recommended_actions.map(
                              (a: string, j: number) => (
                                <li key={`act-${i}-${j}`}>{a}</li>
                              ),
                            )}
                          </ul>
                        </>
                      ) : null}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : null}

          <div className="aiGrid2">
            <div className="aiSection">
              <div className="aiSectionTitle">Quick wins (next 7 days)</div>
              <ul className="aiList">
                {(insights.quick_wins_next_7_days || []).map(
                  (x: string, i: number) => (
                    <li key={`qw-${i}`}>{x}</li>
                  ),
                )}
              </ul>
            </div>

            <div className="aiSection">
              <div className="aiSectionTitle">Experiments (next 30 days)</div>
              <ul className="aiList">
                {(insights.experiments_next_30_days || []).map(
                  (x: string, i: number) => (
                    <li key={`ex-${i}`}>{x}</li>
                  ),
                )}
              </ul>
            </div>
          </div>
        </div>
      ) : null}

      <div style={{ marginTop: 12 }}>
        <AiAgentChatPanel
          agent="ga"
          title="GA Agent Chat"
          context={{
            ga_context: data || null,
            latest_ai_insights: insights || null,
          }}
        />
      </div>
    </div>
  );
}
