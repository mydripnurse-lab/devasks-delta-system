import { NextResponse } from "next/server";

export const runtime = "nodejs";

type JsonObject = Record<string, unknown>;

function s(v: unknown) {
    return String(v ?? "").trim();
}

function n(v: unknown) {
    const x = Number(v);
    return Number.isFinite(x) ? x : 0;
}

function percentChange(curr: number, prev: number) {
    if (!Number.isFinite(curr) || !Number.isFinite(prev)) return null;
    if (prev === 0) return curr === 0 ? 0 : 100;
    return ((curr - prev) / prev) * 100;
}

function clamp(n: number, lo: number, hi: number) {
    return Math.min(hi, Math.max(lo, n));
}

function toMsAny(v: unknown) {
    const n = Number(v);
    if (Number.isFinite(n) && n > 0) return n;
    const d = new Date(s(v));
    const t = d.getTime();
    return Number.isFinite(t) ? t : NaN;
}

type BucketAgg = {
    key: string;
    label: string;
    leads: number;
    calls: number;
    conversations: number;
    appointments: number;
    cancelledAppointments: number;
    successfulRevenue: number;
    lostCount: number;
    lostValue: number;
};

function chooseGranularity(preset: string, startIso: string, endIso: string) {
    if (preset === "1d" || preset === "7d" || preset === "28d") return "day" as const;
    if (preset === "1m" || preset === "3m") return "week" as const;
    if (preset === "6m" || preset === "1y") return "month" as const;
    const start = new Date(startIso).getTime();
    const end = new Date(endIso).getTime();
    const days = Number.isFinite(start) && Number.isFinite(end) ? (end - start) / (24 * 60 * 60 * 1000) : 30;
    if (days <= 45) return "day" as const;
    if (days <= 180) return "week" as const;
    return "month" as const;
}

function startOfBucket(ms: number, granularity: "day" | "week" | "month") {
    const d = new Date(ms);
    if (granularity === "day") {
        d.setHours(0, 0, 0, 0);
        return d.getTime();
    }
    if (granularity === "week") {
        const day = d.getDay();
        const diff = (day + 6) % 7;
        d.setDate(d.getDate() - diff);
        d.setHours(0, 0, 0, 0);
        return d.getTime();
    }
    d.setDate(1);
    d.setHours(0, 0, 0, 0);
    return d.getTime();
}

function bucketLabel(ms: number, granularity: "day" | "week" | "month") {
    const d = new Date(ms);
    if (granularity === "month") {
        return d.toLocaleDateString("en-US", { month: "short", year: "2-digit" });
    }
    if (granularity === "week") {
        const end = new Date(ms);
        end.setDate(end.getDate() + 6);
        return `${d.toLocaleDateString("en-US", { month: "2-digit", day: "2-digit" })} - ${end.toLocaleDateString("en-US", {
            month: "2-digit",
            day: "2-digit",
        })}`;
    }
    return d.toLocaleDateString("en-US", { month: "2-digit", day: "2-digit" });
}

function isSuccessfulTxStatus(statusRaw: unknown) {
    const st = s(statusRaw).toLowerCase();
    return st.includes("succeed") || st.includes("paid") || st.includes("complete") || st.includes("approved");
}

function computeBucketScore(
    b: BucketAgg,
    baselines: { maxActivity: number; maxRevenue: number },
) {
    const activity = b.leads + b.calls * 0.6 + b.conversations * 0.4;
    const volumeScore = clamp((activity / Math.max(1, baselines.maxActivity)) * 100, 0, 100);
    const revenueScore = clamp((b.successfulRevenue / Math.max(1, baselines.maxRevenue)) * 100, 0, 100);
    const cancellationRate = b.appointments > 0 ? b.cancelledAppointments / b.appointments : 0;
    const appointmentQuality = clamp((1 - cancellationRate) * 100, 0, 100);
    const leadBase = Math.max(1, b.leads);
    const coverage = clamp((b.calls / leadBase) * 45 + (b.appointments / leadBase) * 55, 0, 100);
    const lossHealth = 100 - clamp(
        ((b.lostValue / Math.max(1, b.successfulRevenue + b.lostValue)) * 100) * 0.7 +
        ((b.lostCount / Math.max(1, b.appointments + b.lostCount)) * 100) * 0.3,
        0,
        100,
    );
    const score = Math.round(
        volumeScore * 0.2 +
        revenueScore * 0.25 +
        appointmentQuality * 0.2 +
        coverage * 0.2 +
        lossHealth * 0.15,
    );
    return {
        score: clamp(score, 0, 100),
        components: {
            volume: Math.round(volumeScore),
            revenue: Math.round(revenueScore),
            appointmentQuality: Math.round(appointmentQuality),
            coverage: Math.round(coverage),
            lossHealth: Math.round(lossHealth),
        },
    };
}

function prevPeriodRange(startIso: string, endIso: string) {
    const start = new Date(startIso).getTime();
    const end = new Date(endIso).getTime();
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
        return { prevStart: "", prevEnd: "" };
    }
    const len = end - start;
    const prevEnd = new Date(start - 1);
    const prevStart = new Date(start - 1 - len);
    return { prevStart: prevStart.toISOString(), prevEnd: prevEnd.toISOString() };
}

async function fetchJson(url: string) {
    const r = await fetch(url, { cache: "no-store" });
    const txt = await r.text();
    let data: JsonObject = {};
    try {
        data = JSON.parse(txt) as JsonObject;
    } catch {
        data = { raw: txt };
    }
    return { ok: r.ok, status: r.status, data };
}

function adsRangeFromPreset(preset: string) {
    if (preset === "7d") return "last_7_days";
    if (preset === "28d") return "last_28_days";
    if (preset === "1m") return "last_month";
    if (preset === "3m") return "last_quarter";
    if (preset === "6m") return "last_6_months";
    if (preset === "1y") return "last_year";
    if (preset === "custom") return "last_28_days";
    return "last_7_days";
}

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function GET(req: Request) {
    try {
        const url = new URL(req.url);
        const start = s(url.searchParams.get("start"));
        const end = s(url.searchParams.get("end"));
        const preset = s(url.searchParams.get("preset")) || "28d";
        const adsRange = s(url.searchParams.get("adsRange")) || adsRangeFromPreset(preset);
        const force = s(url.searchParams.get("force")) === "1";

        if (!start || !end) {
            return NextResponse.json(
                { ok: false, error: "Missing start/end query params." },
                { status: 400 },
            );
        }

        const origin = `${url.protocol}//${url.host}`;
        const { prevStart, prevEnd } = prevPeriodRange(start, end);

        const convBust = force ? "&bust=1" : "";
        const contactsBust = force ? "&bust=1" : "";
        const forceQ = force ? "&force=1" : "";

        const [
            callsCur,
            callsPrev,
            contactsCur,
            contactsPrev,
            gscAgg,
            gaJoin,
            adsJoin,
        ] = await Promise.all([
            fetchJson(`${origin}/api/dashboard/calls?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`),
            prevStart && prevEnd
                ? fetchJson(
                    `${origin}/api/dashboard/calls?start=${encodeURIComponent(prevStart)}&end=${encodeURIComponent(prevEnd)}`,
                )
                : Promise.resolve({ ok: false, status: 0, data: {} as JsonObject }),
            fetchJson(
                `${origin}/api/dashboard/contacts?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}${contactsBust}`,
            ),
            prevStart && prevEnd
                ? fetchJson(
                    `${origin}/api/dashboard/contacts?start=${encodeURIComponent(prevStart)}&end=${encodeURIComponent(prevEnd)}${contactsBust}`,
                )
                : Promise.resolve({ ok: false, status: 0, data: {} as JsonObject }),
            fetchJson(`${origin}/api/dashboard/gsc/aggregate?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}${forceQ}`),
            fetchJson(`${origin}/api/dashboard/ga/join?compare=1${forceQ}`),
            fetchJson(`${origin}/api/dashboard/ads/join?range=${encodeURIComponent(adsRange)}${forceQ}`),
        ]);

        // Conversations are fetched sequentially to reduce GHL rate-limit pressure (429).
        const conversationsCur = await fetchJson(
            `${origin}/api/dashboard/conversations?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}${convBust}`,
        );
        await sleep(500);
        const conversationsPrev =
            prevStart && prevEnd
                ? await fetchJson(
                    `${origin}/api/dashboard/conversations?start=${encodeURIComponent(prevStart)}&end=${encodeURIComponent(prevEnd)}${convBust}`,
                )
                : { ok: false, status: 0, data: {} as JsonObject };
        await sleep(500);
        const transactionsCur = await fetchJson(
            `${origin}/api/dashboard/transactions?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}${convBust}`,
        );
        await sleep(500);
        const transactionsPrev =
            prevStart && prevEnd
                ? await fetchJson(
                    `${origin}/api/dashboard/transactions?start=${encodeURIComponent(prevStart)}&end=${encodeURIComponent(prevEnd)}${convBust}`,
                )
                : { ok: false, status: 0, data: {} as JsonObject };
        await sleep(500);
        const appointmentsCur = await fetchJson(
            `${origin}/api/dashboard/appointments?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}${convBust}`,
        );
        await sleep(500);
        const appointmentsPrev =
            prevStart && prevEnd
                ? await fetchJson(
                    `${origin}/api/dashboard/appointments?start=${encodeURIComponent(prevStart)}&end=${encodeURIComponent(prevEnd)}${convBust}`,
                )
                : { ok: false, status: 0, data: {} as JsonObject };

        const callsNow = callsCur.ok ? n(callsCur.data.total) : 0;
        const callsBefore = callsPrev.ok ? n(callsPrev.data.total) : 0;

        const leadsNow = contactsCur.ok ? n(contactsCur.data.total) : 0;
        const leadsBefore = contactsPrev.ok ? n(contactsPrev.data.total) : 0;

        const convNow = conversationsCur.ok ? n(conversationsCur.data.total) : 0;
        const convBefore = conversationsPrev.ok ? n(conversationsPrev.data.total) : 0;
        const txNow = transactionsCur.ok ? n(transactionsCur.data.total) : 0;
        const txBefore = transactionsPrev.ok ? n(transactionsPrev.data.total) : 0;
        const txGrossNow = transactionsCur.ok
            ? n((transactionsCur.data.kpis as JsonObject)?.grossAmount)
            : 0;
        const txGrossBefore = transactionsPrev.ok
            ? n((transactionsPrev.data.kpis as JsonObject)?.grossAmount)
            : 0;
        const txLtvNow = transactionsCur.ok
            ? n((transactionsCur.data.kpis as JsonObject)?.avgLifetimeOrderValue)
            : 0;
        const apptNow = appointmentsCur.ok ? n(appointmentsCur.data.total) : 0;
        const apptBefore = appointmentsPrev.ok ? n(appointmentsPrev.data.total) : 0;
        const apptLostNow = appointmentsCur.ok
            ? n((appointmentsCur.data.lostBookings as JsonObject)?.total)
            : 0;
        const apptLostBefore = appointmentsPrev.ok
            ? n((appointmentsPrev.data.lostBookings as JsonObject)?.total)
            : 0;
        const apptLostValueNow = appointmentsCur.ok
            ? n((appointmentsCur.data.lostBookings as JsonObject)?.valueTotal)
            : 0;
        const apptLostValueBefore = appointmentsPrev.ok
            ? n((appointmentsPrev.data.lostBookings as JsonObject)?.valueTotal)
            : 0;

        const gscTotals = (gscAgg.ok ? (gscAgg.data.totals as JsonObject) : {}) || {};
        const gscDeltas = (gscAgg.ok ? (gscAgg.data.deltas as JsonObject) : {}) || {};

        const gaSummary = (gaJoin.ok ? (gaJoin.data.summaryOverall as JsonObject) : {}) || {};
        const gaCompare = (gaJoin.ok ? (gaJoin.data.compare as JsonObject) : {}) || {};

        const adsSummary = (() => {
            if (!adsJoin.ok) return {};
            const fromOverall = (adsJoin.data.summaryOverall as JsonObject) || null;
            if (fromOverall && Object.keys(fromOverall).length) return fromOverall;
            return (adsJoin.data.summary as JsonObject) || {};
        })();

        const leadToCall = callsNow > 0 ? leadsNow / callsNow : null;
        const leadToCallPrev = callsBefore > 0 ? leadsBefore / callsBefore : null;

        const granularity = chooseGranularity(preset, start, end);
        const bucketMap = new Map<string, BucketAgg>();
        const ensureBucket = (ms: number) => {
            const bMs = startOfBucket(ms, granularity);
            const key = new Date(bMs).toISOString();
            let found = bucketMap.get(key);
            if (!found) {
                found = {
                    key,
                    label: bucketLabel(bMs, granularity),
                    leads: 0,
                    calls: 0,
                    conversations: 0,
                    appointments: 0,
                    cancelledAppointments: 0,
                    successfulRevenue: 0,
                    lostCount: 0,
                    lostValue: 0,
                };
                bucketMap.set(key, found);
            }
            return found;
        };

        const callsRows = ((callsCur.data.rows as unknown[]) || []) as Array<Record<string, unknown>>;
        for (const row of callsRows) {
            const ms = toMsAny(row.__startMs ?? row["Phone Call Start Time"] ?? row.__startIso);
            if (!Number.isFinite(ms)) continue;
            ensureBucket(ms).calls += 1;
        }

        const contactsRows = ((contactsCur.data.rows as unknown[]) || []) as Array<Record<string, unknown>>;
        for (const row of contactsRows) {
            const ms = toMsAny(row.__createdMs ?? row.dateAdded);
            if (!Number.isFinite(ms)) continue;
            ensureBucket(ms).leads += 1;
        }

        const convRows = ((conversationsCur.data.rows as unknown[]) || []) as Array<Record<string, unknown>>;
        for (const row of convRows) {
            const ms = toMsAny(row.__lastMs ?? row.lastMessageAt);
            if (!Number.isFinite(ms)) continue;
            ensureBucket(ms).conversations += 1;
        }

        const txRows = ((transactionsCur.data.rows as unknown[]) || []) as Array<Record<string, unknown>>;
        for (const row of txRows) {
            const ms = toMsAny(row.__createdMs ?? row.createdAt);
            if (!Number.isFinite(ms)) continue;
            const b = ensureBucket(ms);
            if (isSuccessfulTxStatus(row.status)) {
                b.successfulRevenue += n(row.amount);
            }
        }

        const apptRows = ((appointmentsCur.data.rows as unknown[]) || []) as Array<Record<string, unknown>>;
        for (const row of apptRows) {
            const ms = toMsAny(row.__startMs ?? row.startAt);
            if (!Number.isFinite(ms)) continue;
            const b = ensureBucket(ms);
            b.appointments += 1;
            const st = s(row.statusNormalized || row.status).toLowerCase();
            if (st.includes("cancel")) b.cancelledAppointments += 1;
        }

        const lostRows =
            ((((appointmentsCur.data.lostBookings as JsonObject)?.rows as unknown[]) || []) as Array<Record<string, unknown>>);
        for (const row of lostRows) {
            const ms = toMsAny(row.__eventMs ?? row.createdAt ?? row.updatedAt);
            if (!Number.isFinite(ms)) continue;
            const b = ensureBucket(ms);
            b.lostCount += 1;
            b.lostValue += n(row.value);
        }

        const buckets = Array.from(bucketMap.values()).sort((a, b) => (a.key < b.key ? -1 : 1));
        const maxActivity = buckets.reduce((mx, b) => Math.max(mx, b.leads + b.calls * 0.6 + b.conversations * 0.4), 1);
        const maxRevenue = buckets.reduce((mx, b) => Math.max(mx, b.successfulRevenue), 1);
        const trend = buckets.map((b) => {
            const calc = computeBucketScore(b, { maxActivity, maxRevenue });
            return {
                key: b.key,
                label: b.label,
                score: calc.score,
                ...calc.components,
                leads: b.leads,
                calls: b.calls,
                conversations: b.conversations,
                appointments: b.appointments,
                successfulRevenue: Math.round(b.successfulRevenue),
                lostCount: b.lostCount,
                lostValue: Math.round(b.lostValue),
            };
        });

        const currentBusinessScore =
            trend.length > 0
                ? Math.round(trend.reduce((acc, x) => acc + n(x.score), 0) / Math.max(1, trend.length))
                : 0;
        const prevBusinessScore = (() => {
            const callsPrevTotal = callsBefore;
            const leadsPrevTotal = leadsBefore;
            const convPrevTotal = convBefore;
            const apptPrevTotal = apptBefore;
            const apptPrevCancelled = n((appointmentsPrev.data.kpis as JsonObject)?.cancelled);
            const txPrevRevenue = txGrossBefore;
            const lossPrevCount = apptLostBefore;
            const lossPrevValue = apptLostValueBefore;
            const synthetic: BucketAgg = {
                key: "prev",
                label: "prev",
                leads: leadsPrevTotal,
                calls: callsPrevTotal,
                conversations: convPrevTotal,
                appointments: apptPrevTotal,
                cancelledAppointments: apptPrevCancelled,
                successfulRevenue: txPrevRevenue,
                lostCount: lossPrevCount,
                lostValue: lossPrevValue,
            };
            return computeBucketScore(synthetic, {
                maxActivity: Math.max(1, leadsPrevTotal + callsPrevTotal * 0.6 + convPrevTotal * 0.4),
                maxRevenue: Math.max(1, txPrevRevenue),
            }).score;
        })();
        const currentComponents =
            trend.length > 0
                ? {
                    volume: Math.round(trend.reduce((a, x) => a + n(x.volume), 0) / trend.length),
                    revenue: Math.round(trend.reduce((a, x) => a + n(x.revenue), 0) / trend.length),
                    appointmentQuality: Math.round(trend.reduce((a, x) => a + n(x.appointmentQuality), 0) / trend.length),
                    coverage: Math.round(trend.reduce((a, x) => a + n(x.coverage), 0) / trend.length),
                    lossHealth: Math.round(trend.reduce((a, x) => a + n(x.lossHealth), 0) / trend.length),
                }
                : { volume: 0, revenue: 0, appointmentQuality: 0, coverage: 0, lossHealth: 0 };

        const out = {
            ok: true,
            range: { start, end, preset, adsRange },
            prevRange: { start: prevStart, end: prevEnd },
            executive: {
                leadsNow,
                leadsBefore,
                leadsDeltaPct: percentChange(leadsNow, leadsBefore),
                callsNow,
                callsBefore,
                callsDeltaPct: percentChange(callsNow, callsBefore),
                conversationsNow: convNow,
                conversationsBefore: convBefore,
                conversationsDeltaPct: percentChange(convNow, convBefore),
                transactionsNow: txNow,
                transactionsBefore: txBefore,
                transactionsDeltaPct: percentChange(txNow, txBefore),
                transactionsRevenueNow: txGrossNow,
                transactionsRevenueBefore: txGrossBefore,
                transactionsRevenueDeltaPct: percentChange(txGrossNow, txGrossBefore),
                transactionsAvgLtvNow: txLtvNow,
                appointmentsNow: apptNow,
                appointmentsBefore: apptBefore,
                appointmentsDeltaPct: percentChange(apptNow, apptBefore),
                appointmentsLostNow: apptLostNow,
                appointmentsLostBefore: apptLostBefore,
                appointmentsLostDeltaPct: percentChange(apptLostNow, apptLostBefore),
                appointmentsLostValueNow: apptLostValueNow,
                appointmentsLostValueBefore: apptLostValueBefore,
                appointmentsLostValueDeltaPct: percentChange(apptLostValueNow, apptLostValueBefore),
                leadToCall,
                leadToCallDeltaPct:
                    leadToCall !== null && leadToCallPrev !== null
                        ? percentChange(leadToCall, leadToCallPrev)
                        : null,
                gscClicks: n(gscTotals.clicks),
                gscImpressions: n(gscTotals.impressions),
                gaSessions: n(gaSummary.sessions),
                gaUsers: n(gaSummary.users),
                gaConversions: n(gaSummary.conversions),
                adsCost: n(adsSummary.cost),
                adsConversions: n(adsSummary.conversions),
                adsConversionValue: n(adsSummary.conversionValue),
            },
            businessScore: {
                current: currentBusinessScore,
                previous: prevBusinessScore,
                deltaPct: percentChange(currentBusinessScore, prevBusinessScore),
                grade:
                    currentBusinessScore >= 80
                        ? "A"
                        : currentBusinessScore >= 70
                            ? "B"
                            : currentBusinessScore >= 60
                                ? "C"
                                : currentBusinessScore >= 50
                                    ? "D"
                                    : "F",
                granularity,
                components: currentComponents,
                trend,
            },
            modules: {
                calls: {
                    ok: callsCur.ok,
                    total: callsNow,
                    prevTotal: callsBefore,
                    deltaPct: percentChange(callsNow, callsBefore),
                    error: callsCur.ok ? null : s(callsCur.data.error || `HTTP ${callsCur.status}`),
                },
                contacts: {
                    ok: contactsCur.ok,
                    total: leadsNow,
                    prevTotal: leadsBefore,
                    deltaPct: percentChange(leadsNow, leadsBefore),
                    contactableRate: n((contactsCur.data.kpis as JsonObject)?.phoneRate) || 0,
                    emailRate: n((contactsCur.data.kpis as JsonObject)?.emailRate) || 0,
                    inferredFromOpportunity: n((contactsCur.data.kpis as JsonObject)?.inferredFromOpportunity) || 0,
                    error: contactsCur.ok ? null : s(contactsCur.data.error || `HTTP ${contactsCur.status}`),
                },
                conversations: {
                    ok: conversationsCur.ok,
                    total: convNow,
                    prevTotal: convBefore,
                    deltaPct: percentChange(convNow, convBefore),
                    mappedStateRate: n((conversationsCur.data.kpis as JsonObject)?.stateRate) || 0,
                    topChannel:
                        Object.entries(
                            ((conversationsCur.data.byChannel as JsonObject) || {}) as Record<string, unknown>,
                        ).sort((a, b) => n(b[1]) - n(a[1]))[0]?.[0] || "unknown",
                    error: conversationsCur.ok
                        ? null
                        : s(conversationsCur.data.error || `HTTP ${conversationsCur.status}`),
                },
                transactions: {
                    ok: transactionsCur.ok,
                    total: txNow,
                    prevTotal: txBefore,
                    deltaPct: percentChange(txNow, txBefore),
                    grossAmount: txGrossNow,
                    prevGrossAmount: txGrossBefore,
                    revenueDeltaPct: percentChange(txGrossNow, txGrossBefore),
                    avgLifetimeOrderValue: txLtvNow,
                    mappedStateRate: n((transactionsCur.data.kpis as JsonObject)?.stateRate) || 0,
                    error: transactionsCur.ok
                        ? null
                        : s(transactionsCur.data.error || `HTTP ${transactionsCur.status}`),
                },
                appointments: {
                    ok: appointmentsCur.ok,
                    total: apptNow,
                    prevTotal: apptBefore,
                    deltaPct: percentChange(apptNow, apptBefore),
                    showRate: n((appointmentsCur.data.kpis as JsonObject)?.showRate) || 0,
                    noShowRate: n((appointmentsCur.data.kpis as JsonObject)?.noShowRate) || 0,
                    cancellationRate: n((appointmentsCur.data.kpis as JsonObject)?.cancellationRate) || 0,
                    mappedStateRate: n((appointmentsCur.data.kpis as JsonObject)?.stateRate) || 0,
                    lostQualified: apptLostNow,
                    lostQualifiedPrev: apptLostBefore,
                    lostQualifiedDeltaPct: percentChange(apptLostNow, apptLostBefore),
                    potentialLostValue: apptLostValueNow,
                    potentialLostValuePrev: apptLostValueBefore,
                    potentialLostValueDeltaPct: percentChange(apptLostValueNow, apptLostValueBefore),
                    error: appointmentsCur.ok
                        ? null
                        : s(appointmentsCur.data.error || `HTTP ${appointmentsCur.status}`),
                },
                gsc: {
                    ok: gscAgg.ok,
                    totals: gscTotals,
                    deltas: gscDeltas,
                    error: gscAgg.ok ? null : s(gscAgg.data.error || `HTTP ${gscAgg.status}`),
                },
                ga: {
                    ok: gaJoin.ok,
                    summaryOverall: gaSummary,
                    compare: gaCompare,
                    error: gaJoin.ok ? null : s(gaJoin.data.error || `HTTP ${gaJoin.status}`),
                },
                ads: {
                    ok: adsJoin.ok,
                    summary: adsSummary,
                    error: adsJoin.ok ? null : s(adsJoin.data.error || `HTTP ${adsJoin.status}`),
                },
            },
        };

        return NextResponse.json(out);
    } catch (e: unknown) {
        return NextResponse.json(
            { ok: false, error: e instanceof Error ? e.message : "overview failed" },
            { status: 500 },
        );
    }
}
