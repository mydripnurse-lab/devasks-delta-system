function num(v: any) {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
}

function microsToMoney(micros: any) {
    return num(micros) / 1_000_000;
}

export function joinAds(raw: any, meta: any) {
    // raw.search returns { results: [...] }
    const results = Array.isArray(raw?.results) ? raw.results : [];

    // Para KPIs customer query suele venir 1 row (pero puede venir multiples)
    let impressions = 0,
        clicks = 0,
        cost = 0,
        conv = 0,
        convValue = 0,
        avgCpcMicrosSum = 0,
        avgCpcCount = 0,
        ctrSum = 0,
        ctrCount = 0;

    for (const r of results) {
        const m = r?.metrics || {};
        impressions += num(m.impressions);
        clicks += num(m.clicks);
        cost += microsToMoney(m.costMicros);
        conv += num(m.conversions);
        convValue += num(m.conversionsValue);

        if (m.averageCpc != null) {
            avgCpcMicrosSum += num(m.averageCpc);
            avgCpcCount += 1;
        }
        if (m.ctr != null) {
            ctrSum += num(m.ctr);
            ctrCount += 1;
        }
    }

    const avgCpc = avgCpcCount ? microsToMoney(avgCpcMicrosSum / avgCpcCount) : 0;
    const ctr = ctrCount ? ctrSum / ctrCount : 0;

    const cpc = clicks ? cost / clicks : 0;
    const cpa = conv ? cost / conv : 0;
    const roas = cost ? convValue / cost : 0;

    return {
        ok: true,
        meta,
        summary: {
            impressions,
            clicks,
            ctr,
            cost,
            avgCpc, // platform avg_cpc
            cpc, // computed cost/click
            conversions: conv,
            conversionValue: convValue,
            cpa,
            roas,
            generatedAt: meta?.generatedAt || null,
            startDate: meta?.startDate || null,
            endDate: meta?.endDate || null,
        },
        rawCount: results.length,
    };
}
