"use client";

import { useMemo, useState } from "react";

function norm(v: any) {
  return String(v ?? "").trim();
}

export default function AdsInsightsPanel({
  data,
  title = "AI Strategist (Google Ads)",
}: {
  data: any;
  title?: string;
}) {
  const [open, setOpen] = useState(true);

  const prompt = useMemo(() => {
    if (!data) return "";
    const ctx = data?.context || {};
    const meta = data?.meta || {};
    const summary = data?.summaryOverall || {};
    const opp = data?.opportunities || {};

    // Compact, delta-aware briefing
    return `
You are a senior Google Ads strategist + CRO expert.
Goal: maximize conversions and profitability using the Delta System approach (Demand → Behavior → Conversion).
Return actionable recommendations only. Use bullets. Include “Quick wins”, “Fix leaks”, “Scale winners”, and “Next experiments”.

Context:
- Range: ${norm(ctx.startDate)} → ${norm(ctx.endDate)} (preset=${norm(ctx.preset || ctx.range)})
- CustomerId: ${norm(meta.customerId || "")}
- Summary: impressions=${summary.impressions}, clicks=${summary.clicks}, ctr=${summary.ctr}, cost=${summary.cost}, avgCpc=${summary.avgCpc}, conversions=${summary.conversions}, convValue=${summary.convValue}

Opportunities signals:
- Winners campaigns: ${Array.isArray(opp.winners) ? opp.winners.length : 0}
- Losers campaigns: ${Array.isArray(opp.losers) ? opp.losers.length : 0}
- Keyword leaks: ${Array.isArray(opp.kwLeaks) ? opp.kwLeaks.length : 0}
- Negative ideas: ${Array.isArray(opp.negativeIdeas) ? opp.negativeIdeas.length : 0}
- CTR problems: ${Array.isArray(opp.ctrProblems) ? opp.ctrProblems.length : 0}

Use the numbers, and propose:
- which campaigns to pause or restructure
- what negatives to add
- what ad copy/landing page angles to test
- how to allocate budget across winners
- what keyword themes to expand (based on top keywords/search terms)
`.trim();
  }, [data]);

  if (!data) {
    return (
      <div className="gscTopCard" style={{ margin: 0 }}>
        <div className="gscTopHead">
          <div className="gscTopTitle">{title}</div>
        </div>
        <div style={{ padding: 12 }} className="mini">
          No payload yet.
        </div>
      </div>
    );
  }

  return (
    <div className="gscTopCard" style={{ margin: 0 }}>
      <div className="gscTopHead" style={{ justifyContent: "space-between" }}>
        <div className="gscTopTitle">{title}</div>
        <button
          className="smallBtn"
          type="button"
          onClick={() => setOpen((v) => !v)}
        >
          {open ? "Hide" : "Show"}
        </button>
      </div>
      {open ? (
        <div style={{ padding: 12 }}>
          <div className="mini" style={{ opacity: 0.8, marginBottom: 10 }}>
            Copy this prompt into your agent runner (or wire it to your internal
            LLM endpoint).
          </div>
          <textarea
            className="input"
            style={{
              width: "100%",
              minHeight: 220,
              fontFamily:
                "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
            }}
            value={prompt}
            readOnly
          />
        </div>
      ) : null}
    </div>
  );
}
