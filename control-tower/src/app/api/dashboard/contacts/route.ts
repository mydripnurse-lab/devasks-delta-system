// control-tower/src/app/api/dashboard/contacts/route.ts
import { NextResponse } from "next/server";
import { ghlFetchJson, getEffectiveLocationIdOrThrow } from "@/lib/ghlHttp";
import { inferStateFromText, normalizeStateName, norm } from "@/lib/ghlState";
import { tokensDebugInfo } from "@/lib/ghlTokens";

export const runtime = "nodejs";

type Contact = Record<string, any>;

type ContactsRow = {
    id: string;
    name: string;
    email: string;
    phone: string;
    state: string;
    city: string;
    source: string;
    tags: string[];
    dateAdded: string;
    __createdMs: number | null;

    leadType: "lead" | "guest_chat_like";
    stateFrom: "contact.state" | "opportunity.source" | "unknown";
    opportunityId?: string;
};

type ApiResponse = {
    ok: boolean;
    range?: { start: string; end: string };
    total?: number;
    kpis?: {
        total: number;
        withEmail: number;
        withPhone: number;
        withState: number;
        emailRate: number;
        phoneRate: number;
        stateRate: number;

        guestChatLike: number;
        guestRate: number;
        inferredFromOpportunity: number;
    };
    byState?: Record<string, number>;
    rows?: ContactsRow[];
    debug?: any;
    error?: string;
};

function toMs(iso: string) {
    const d = new Date(iso);
    const t = d.getTime();
    return Number.isFinite(t) ? t : NaN;
}

function pct(n: number, d: number) {
    if (!d) return 0;
    return Math.round((n / d) * 100);
}

type CacheEntry = {
    atMs: number;
    ttlMs: number;
    value: ApiResponse;
};

const RANGE_CACHE = new Map<string, CacheEntry>();
const CACHE_TTL_MS = 60_000; // 60s
const MAX_PAGES = 50;
const PAGE_LIMIT = 200;

function cacheKey(start: string, end: string) {
    return `${start}__${end}`;
}

function getCache(start: string, end: string) {
    const k = cacheKey(start, end);
    const hit = RANGE_CACHE.get(k);
    if (!hit) return null;
    if (Date.now() - hit.atMs > hit.ttlMs) {
        RANGE_CACHE.delete(k);
        return null;
    }
    return hit.value;
}

function setCache(start: string, end: string, value: ApiResponse) {
    const k = cacheKey(start, end);
    RANGE_CACHE.set(k, { atMs: Date.now(), ttlMs: CACHE_TTL_MS, value });
}

async function fetchContactsSearch(
    locationId: string,
    startIso: string,
    endIso: string,
    debug = false,
) {
    const startMs = toMs(startIso);
    const endMs = toMs(endIso);

    if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) {
        throw new Error(`Invalid start/end ISO. start=${startIso} end=${endIso}`);
    }
    if (endMs <= startMs) {
        throw new Error(`Invalid range: end must be > start. start=${startIso} end=${endIso}`);
    }

    const sort = [{ field: "dateAdded", direction: "desc" }];
    const inRange = (c: Contact) => {
        const t = new Date(String(c?.dateAdded || "")).getTime();
        return Number.isFinite(t) && t >= startMs && t <= endMs;
    };

    async function runSearch(filters?: any[]) {
        const all: Contact[] = [];
        let page = 0;
        let searchAfter: any[] | null = null;

        while (page < MAX_PAGES) {
            page++;

            const body: any = {
                locationId,
                pageLimit: PAGE_LIMIT,
                sort,
            };
            if (filters?.length) body.filters = filters;

            // prefer cursor pagination if available
            if (searchAfter && Array.isArray(searchAfter) && searchAfter.length) {
                body.searchAfter = searchAfter;
            } else {
                body.page = page;
            }

            if (debug && page === 1) {
                console.log("[contacts/search] body:", body);
                console.log("[contacts/search] range (ms):", { startMs, endMs });
            }

            const res = await ghlFetchJson("/contacts/search", {
                method: "POST",
                body,
            });

            const contacts = Array.isArray(res?.contacts) ? res.contacts : [];
            all.push(...contacts);

            if (debug && page === 1) {
                console.log(
                    "[contacts/search] sample contact keys:",
                    contacts?.[0] ? Object.keys(contacts[0]) : [],
                );
                console.log("[contacts/search] sample contact:", contacts?.[0] || null);
            }

            const nextSearchAfter = Array.isArray(res?.searchAfter) ? res.searchAfter : null;
            if (nextSearchAfter && nextSearchAfter.length) {
                searchAfter = nextSearchAfter;
            } else if (contacts.length < PAGE_LIMIT) {
                break;
            }

            if (!contacts.length) break;
            if (all.length >= 10_000) break;

            // When no API filters are used, stop once oldest row in page is older than range start.
            if (!filters?.length) {
                const last = contacts[contacts.length - 1];
                const lastMs = new Date(String(last?.dateAdded || "")).getTime();
                if (Number.isFinite(lastMs) && lastMs < startMs) break;
            }
        }

        return all;
    }

    const filterAttempts: Array<{ name: string; filters?: any[] }> = [
        {
            name: "range-ms",
            filters: [
                {
                    group: "AND",
                    filters: [{ field: "dateAdded", operator: "range", value: [startMs, endMs] }],
                },
            ],
        },
        {
            name: "range-iso",
            filters: [
                {
                    group: "AND",
                    filters: [{ field: "dateAdded", operator: "range", value: [startIso, endIso] }],
                },
            ],
        },
        {
            name: "gte-lte-ms",
            filters: [
                {
                    group: "AND",
                    filters: [
                        { field: "dateAdded", operator: "gte", value: startMs },
                        { field: "dateAdded", operator: "lte", value: endMs },
                    ],
                },
            ],
        },
        { name: "no-filter-fallback" },
    ];

    let lastErr: any = null;
    for (const attempt of filterAttempts) {
        try {
            if (debug) console.log(`[contacts/search] attempt=${attempt.name}`);
            const rows = await runSearch(attempt.filters);
            return rows.filter(inRange);
        } catch (e: any) {
            lastErr = e;
            const status = Number(e?.status || 0);
            if (debug) console.log(`[contacts/search] attempt failed=${attempt.name}`, e?.message || e);
            if (status === 422) continue;
            throw e;
        }
    }

    throw lastErr || new Error("contacts/search failed");
}

async function fetchOpportunity(opportunityId: string) {
    return await ghlFetchJson(`/opportunities/${encodeURIComponent(opportunityId)}`, {
        method: "GET",
    });
}

function pickBestOpportunityIds(contact: any) {
    const opps = Array.isArray(contact?.opportunities) ? contact.opportunities : [];
    if (!opps.length) return [];

    const ids: string[] = [];
    for (const opp of opps) {
        if (typeof opp === "string") {
            const id = String(opp || "").trim();
            if (id) ids.push(id);
            continue;
        }
        if (opp && typeof opp === "object") {
            const id = String(opp.id || opp.opportunityId || "").trim();
            if (id) ids.push(id);
        }
    }
    return [...new Set(ids)];
}

function toRow(c: any): ContactsRow {
    const id = norm(c?.id);
    const name = norm(
        c?.contactName ||
        c?.name ||
        `${c?.firstName || ""} ${c?.lastName || ""}`.trim(),
    );
    const email = norm(c?.email);
    const phone = norm(c?.phone);
    const stateRaw = norm(c?.state);
    const city = norm(c?.city);
    const source = norm(c?.source);
    const tags = Array.isArray(c?.tags) ? c.tags.map((x: any) => norm(x)).filter(Boolean) : [];
    const dateAdded = norm(c?.dateAdded);

    const createdMs = Number.isFinite(new Date(dateAdded).getTime()) ? new Date(dateAdded).getTime() : null;

    const isGuest = /^guest\b/i.test(name);
    const chatLike = isGuest && !email && !phone;

    const state = normalizeStateName(stateRaw);

    return {
        id,
        name,
        email,
        phone,
        state,
        city,
        source,
        tags,
        dateAdded,
        __createdMs: createdMs,
        leadType: chatLike ? "guest_chat_like" : "lead",
        stateFrom: state ? "contact.state" : "unknown",
    };
}

export async function GET(req: Request) {
    const url = new URL(req.url);
    const start = url.searchParams.get("start") || "";
    const end = url.searchParams.get("end") || "";
    const bust = url.searchParams.get("bust") === "1";
    const debug = url.searchParams.get("debug") === "1";

    try {
        if (!start || !end) {
            return NextResponse.json({ ok: false, error: "Missing start/end" } satisfies ApiResponse, { status: 400 });
        }

        if (!bust) {
            const cached = getCache(start, end);
            if (cached) return NextResponse.json(cached);
        }

        const locationId = await getEffectiveLocationIdOrThrow();

        const contacts = await fetchContactsSearch(locationId, start, end, debug);

        const rows: ContactsRow[] = contacts.map(toRow);

        let inferredFromOpportunity = 0;
        const opportunityCache = new Map<string, any | null>();

        for (const r of rows) {
            if (r.state) continue;

            const c = contacts.find((x) => norm(x?.id) === r.id);
            const oppIds = pickBestOpportunityIds(c);
            if (!oppIds.length) continue;

            r.opportunityId = oppIds[0];

            for (const oppId of oppIds) {
                try {
                    let opp = opportunityCache.get(oppId);
                    if (opp === undefined) {
                        opp = await fetchOpportunity(oppId);
                        opportunityCache.set(oppId, opp);
                    }
                    if (!opp) continue;

                    const src = norm(opp?.source || opp?.opportunity?.source || "");
                    const inferred = inferStateFromText(src);
                    if (inferred) {
                        r.state = normalizeStateName(inferred);
                        r.stateFrom = "opportunity.source";
                        r.opportunityId = oppId;
                        inferredFromOpportunity++;
                        break;
                    }
                } catch (e: any) {
                    opportunityCache.set(oppId, null);
                    if (debug) console.log("[opportunity] failed", oppId, e?.message || e);
                }
            }
        }

        const byState: Record<string, number> = {};
        for (const r of rows) {
            const st = normalizeStateName(r.state);
            if (!st) continue;
            r.state = st;
            byState[st] = (byState[st] || 0) + 1;
        }

        const total = rows.length;
        const withEmail = rows.filter((r) => !!r.email).length;
        const withPhone = rows.filter((r) => !!r.phone).length;
        const withState = rows.filter((r) => !!r.state).length;

        const guestChatLike = rows.filter((r) => r.leadType === "guest_chat_like").length;

        const resp: ApiResponse = {
            ok: true,
            range: { start, end },
            total,
            kpis: {
                total,
                withEmail,
                withPhone,
                withState,
                emailRate: pct(withEmail, total),
                phoneRate: pct(withPhone, total),
                stateRate: pct(withState, total),

                guestChatLike,
                guestRate: pct(guestChatLike, total),
                inferredFromOpportunity,
            },
            byState,
            rows,
            ...(debug
                ? {
                    debug: {
                        tokens: await tokensDebugInfo(),
                        contactsCount: contacts.length,
                        sampleContact: contacts[0] || null,
                        sampleRow: rows[0] || null,
                        byStateTop: Object.entries(byState).slice(0, 15),
                    },
                }
                : {}),
        };

        setCache(start, end, resp);
        return NextResponse.json(resp);
    } catch (e: any) {
        const msg = e?.message || "Unknown error";
        return NextResponse.json({ ok: false, error: msg } satisfies ApiResponse, { status: 500 });
    }
}
