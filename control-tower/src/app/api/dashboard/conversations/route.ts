import { NextResponse } from "next/server";
import { getEffectiveLocationIdOrThrow, ghlFetchJson } from "@/lib/ghlHttp";
import { normalizeStateName, norm } from "@/lib/ghlState";
import fs from "fs/promises";
import path from "path";

export const runtime = "nodejs";

type ConvRow = {
    id: string;
    contactId: string;
    contactName: string;
    channel: string;
    direction: "inbound" | "outbound" | "unknown";
    unreadCount: number;
    messageCount: number;
    snippet: string;
    lastMessageAt: string;
    __lastMs: number | null;
    state: string;
    city: string;
    stateFrom: "conversation" | "contact.state" | "opportunity.source" | "unknown";
};

type ApiResponse = {
    ok: boolean;
    range?: { start: string; end: string };
    total?: number;
    kpis?: {
        total: number;
        uniqueContacts: number;
        unreadConversations: number;
        avgMessagesPerConversation: number;
        withState: number;
        stateRate: number;
        inferredFromContact: number;
    };
    byState?: Record<string, number>;
    byChannel?: Record<string, number>;
    rows?: ConvRow[];
    cache?: {
        source: "memory" | "snapshot" | "ghl_refresh";
        snapshotUpdatedAt?: string;
        snapshotCoverage?: { newestMessageAt: string; oldestMessageAt: string };
        fetchedPages?: number;
        usedIncremental?: boolean;
        refreshReason?: string;
    };
    debug?: Record<string, unknown>;
    error?: string;
};

type ConvSnapshot = {
    version: 1;
    locationId: string;
    updatedAtMs: number;
    newestMessageAt: string;
    oldestMessageAt: string;
    rows: ConvRow[];
};

type CacheEntry = {
    atMs: number;
    ttlMs: number;
    value: ApiResponse;
};

const RANGE_CACHE = new Map<string, CacheEntry>();
const CACHE_TTL_MS = 45_000;
const MAX_PAGES = 40;
const PAGE_LIMIT = 100;
const CONTACTS_MAX_PAGES = 25;
const CONTACTS_PAGE_LIMIT = 100;
const CONTACTS_PAGE_DELAY_MS = 420;
const CONTACTS_RETRY_BASE_MS = 1400;
const CONTACTS_MAX_429_RETRIES = 5;
const SNAPSHOT_TTL_MS = Number(process.env.CONVERSATIONS_SNAPSHOT_TTL_SEC || 900) * 1000;
const SNAPSHOT_MAX_NEW_PAGES = Math.max(3, Number(process.env.CONVERSATIONS_INCREMENTAL_MAX_PAGES || 12));
const SNAPSHOT_OVERLAP_MS = Number(process.env.CONVERSATIONS_INCREMENTAL_OVERLAP_MIN || 15) * 60 * 1000;

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function convCacheDir() {
    const root = process.env.DASH_CACHE_DIR || path.join("data", "cache");
    return path.join(process.cwd(), root, "conversations");
}

function convSnapshotPath(locationId: string) {
    const safeId = String(locationId || "unknown").replace(/[^a-zA-Z0-9_-]/g, "_");
    return path.join(convCacheDir(), `${safeId}.json`);
}

async function readConvSnapshot(locationId: string): Promise<ConvSnapshot | null> {
    try {
        const raw = await fs.readFile(convSnapshotPath(locationId), "utf8");
        const parsed = JSON.parse(raw) as ConvSnapshot;
        if (!parsed || !Array.isArray(parsed.rows)) return null;
        if (String(parsed.locationId || "") !== String(locationId || "")) return null;
        return parsed;
    } catch {
        return null;
    }
}

async function writeConvSnapshot(snapshot: ConvSnapshot) {
    await fs.mkdir(convCacheDir(), { recursive: true });
    await fs.writeFile(convSnapshotPath(snapshot.locationId), JSON.stringify(snapshot, null, 2), "utf8");
}

function convRowsCoverage(rows: ConvRow[]) {
    let newest = 0;
    let oldest = Number.POSITIVE_INFINITY;
    for (const r of rows) {
        const ms = Number(r.__lastMs ?? NaN);
        if (!Number.isFinite(ms)) continue;
        if (ms > newest) newest = ms;
        if (ms < oldest) oldest = ms;
    }
    return {
        newestMs: newest || 0,
        oldestMs: Number.isFinite(oldest) ? oldest : 0,
        newestIso: newest ? new Date(newest).toISOString() : "",
        oldestIso: Number.isFinite(oldest) && oldest > 0 ? new Date(oldest).toISOString() : "",
    };
}

function is429(err: any) {
    return Number(err?.status || 0) === 429 || String(err?.message || "").includes("(429)");
}

function retryAfterMs(err: any) {
    const retryAfterRaw =
        err?.data?.headers?.["retry-after"] ||
        err?.data?.headers?.["Retry-After"] ||
        err?.data?.retryAfter ||
        err?.data?.retry_after;
    const asNum = Number(retryAfterRaw);
    if (Number.isFinite(asNum) && asNum > 0) return asNum * 1000;
    return null;
}

async function with429Retry<T>(fn: () => Promise<T>) {
    let lastErr: unknown = null;
    for (let attempt = 0; attempt <= CONTACTS_MAX_429_RETRIES; attempt++) {
        try {
            return await fn();
        } catch (e: any) {
            lastErr = e;
            if (!is429(e) || attempt === CONTACTS_MAX_429_RETRIES) break;
            const hinted = retryAfterMs(e);
            const exp = Math.round(CONTACTS_RETRY_BASE_MS * Math.pow(1.6, attempt));
            const jitter = Math.floor(Math.random() * 450);
            await sleep(Math.max(hinted || 0, exp + jitter));
        }
    }
    throw lastErr;
}

function inferStateFromOppSourceSafe(source: unknown) {
    const txt = norm(source);
    if (!txt) return "";

    // Safer policy: only full state names (avoid abbreviations that can be noisy).
    const names = Object.values({
        AL: "Alabama",
        AK: "Alaska",
        AZ: "Arizona",
        AR: "Arkansas",
        CA: "California",
        CO: "Colorado",
        CT: "Connecticut",
        DE: "Delaware",
        DC: "District of Columbia",
        FL: "Florida",
        GA: "Georgia",
        HI: "Hawaii",
        ID: "Idaho",
        IL: "Illinois",
        IN: "Indiana",
        IA: "Iowa",
        KS: "Kansas",
        KY: "Kentucky",
        LA: "Louisiana",
        ME: "Maine",
        MD: "Maryland",
        MA: "Massachusetts",
        MI: "Michigan",
        MN: "Minnesota",
        MS: "Mississippi",
        MO: "Missouri",
        MT: "Montana",
        NE: "Nebraska",
        NV: "Nevada",
        NH: "New Hampshire",
        NJ: "New Jersey",
        NM: "New Mexico",
        NY: "New York",
        NC: "North Carolina",
        ND: "North Dakota",
        OH: "Ohio",
        OK: "Oklahoma",
        OR: "Oregon",
        PA: "Pennsylvania",
        RI: "Rhode Island",
        SC: "South Carolina",
        SD: "South Dakota",
        TN: "Tennessee",
        TX: "Texas",
        UT: "Utah",
        VT: "Vermont",
        VA: "Virginia",
        WA: "Washington",
        WV: "West Virginia",
        WI: "Wisconsin",
        WY: "Wyoming",
        PR: "Puerto Rico",
    });

    for (const name of names) {
        const re = new RegExp(`\\b${name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i");
        if (re.test(txt)) return name;
    }
    return "";
}

function toMs(iso: string) {
    const d = new Date(iso);
    const t = d.getTime();
    return Number.isFinite(t) ? t : NaN;
}

function dateMsFromUnknown(v: unknown) {
    if (typeof v === "number" && Number.isFinite(v)) {
        // 10 digits ~= epoch seconds, 13 digits ~= epoch ms
        if (v > 1_000_000_000_000) return v;
        if (v > 1_000_000_000) return v * 1000;
        return NaN;
    }

    const s = norm(v);
    if (!s) return NaN;

    const asNum = Number(s);
    if (Number.isFinite(asNum)) {
        if (asNum > 1_000_000_000_000) return asNum;
        if (asNum > 1_000_000_000) return asNum * 1000;
    }

    const d = new Date(s);
    const t = d.getTime();
    return Number.isFinite(t) ? t : NaN;
}

function pct(n: number, d: number) {
    if (!d) return 0;
    return Math.round((n / d) * 100);
}

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

function pickDateIso(c: any) {
    const candidates = [
        c.lastMessageDate,
        c.lastMessageAt,
        c.last_message_date,
        c.last_message_at,
        c.dateUpdated,
        c.dateAdded,
        c.updatedAt,
        c.createdAt,
        c.lastMessage?.date,
        c.lastMessage?.createdAt,
        c.lastMessage?.updatedAt,
        c.last_message?.date,
        c.last_message?.created_at,
        c.last_message?.updated_at,
    ];
    for (const x of candidates) {
        const ms = dateMsFromUnknown(x);
        if (Number.isFinite(ms)) return new Date(ms).toISOString();
    }
    return "";
}

function toNum(v: unknown) {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
}

function dateIsoFromCandidates(...vals: unknown[]) {
    for (const v of vals) {
        const ms = dateMsFromUnknown(v);
        if (Number.isFinite(ms)) return new Date(ms).toISOString();
    }
    return "";
}

function detectDirection(c: any): "inbound" | "outbound" | "unknown" {
    const raw = [
        c.direction,
        c.lastMessageDirection,
        c.last_message_direction,
        c.messageDirection,
        c.message_direction,
        c.type,
        c.lastMessage?.direction,
        c.last_message?.direction,
    ]
        .map((x) => norm(x).toLowerCase())
        .join(" ");

    if (!raw) return "unknown";
    if (raw.includes("inbound") || raw.includes("incoming")) return "inbound";
    if (raw.includes("outbound") || raw.includes("outgoing")) return "outbound";
    return "unknown";
}

function detectChannel(c: any) {
    const raw = [
        c.channel,
        c.channelType,
        c.channel_type,
        c.type,
        c.medium,
        c.provider,
        c.conversationProvider,
        c.integrationType,
        c.lastMessageType,
        c.last_message_type,
        c.lastMessage?.channel,
        c.lastMessage?.type,
        c.last_message?.channel,
        c.last_message?.type,
    ]
        .map((x) => norm(x).toLowerCase())
        .join(" ");

    if (raw.includes("whatsapp")) return "whatsapp";
    if (raw.includes("instagram")) return "instagram";
    if (raw.includes("facebook") || raw.includes("messenger")) return "facebook";
    if (raw.includes("email")) return "email";
    if (raw.includes("sms") || raw.includes("text")) return "sms";
    if (raw.includes("gmb") || raw.includes("google_business") || raw.includes("google business")) return "gmb";
    if (raw.includes("webchat") || raw.includes("livechat") || raw.includes("chat widget")) return "webchat";
    if (raw.includes("call") || raw.includes("phone")) return "call";
    return "unknown";
}

function pickContactId(c: any) {
    return norm(
        c.contactId ||
            c.contact_id ||
            c.contact?.id ||
            c.idContact ||
            c.participantId ||
            c.participants?.[0]?.contactId ||
            c.participants?.[0]?.id,
    );
}

function pickConversationId(c: any) {
    return norm(
        c.id ||
            c.conversationId ||
            c.conversation_id ||
            c.threadId ||
            c.thread_id ||
            c.chatId ||
            c.chat_id,
    );
}

function pickContactName(c: any) {
    return norm(
        c.fullName ||
            c.contactName ||
            c.name ||
            c.contact?.name ||
            `${norm(c.contact?.firstName)} ${norm(c.contact?.lastName)}`.trim(),
    );
}

function pickSnippet(c: any) {
    return norm(
        c.lastMessageBody ||
            c.snippet ||
            c.preview ||
            c.lastMessage ||
            c.lastMessage?.body ||
            c.last_message?.body,
    );
}

function pickConversationState(c: any) {
    const s0 = norm(c.state || c.contactState || c.contact?.state || c.address?.state);
    const normalized = normalizeStateName(s0);
    if (normalized) return normalized;
    return "";
}

function toRow(c: any): ConvRow {
    const lastIso = pickDateIso(c);
    const ms = Number.isFinite(new Date(lastIso).getTime()) ? new Date(lastIso).getTime() : null;
    const state = pickConversationState(c);

    return {
        id: pickConversationId(c),
        contactId: pickContactId(c),
        contactName: pickContactName(c),
        channel: detectChannel(c),
        direction: detectDirection(c),
        unreadCount: Math.max(0, toNum(c.unreadCount || c.unread_count)),
        messageCount: Math.max(
            0,
            toNum(
                c.messageCount ||
                    c.message_count ||
                    c.totalMessages ||
                    c.total_messages ||
                    c.messagesCount ||
                    c.messages_count,
            ),
        ),
        snippet: pickSnippet(c),
        lastMessageAt: lastIso,
        __lastMs: ms,
        state: state || "",
        city: norm(c.city || c.contactCity || c.contact?.city),
        stateFrom: state ? "conversation" : "unknown",
    };
}

function extractConversationArray(res: unknown) {
    const x = res as Record<string, any>;
    if (Array.isArray(x?.conversations)) return x.conversations;
    if (Array.isArray(x?.data?.conversations)) return x.data.conversations;
    if (Array.isArray(x?.items)) return x.items;
    if (Array.isArray(x?.data?.items)) return x.data.items;
    if (Array.isArray(x?.data)) return x.data;
    return [];
}

async function fetchConversations(
    locationId: string,
    startIso: string,
    endIso: string,
    debug = false,
    opts?: { stopWhenOlderThanMs?: number; maxPages?: number },
) {
    const startMs = toMs(startIso);
    const endMs = toMs(endIso);
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) {
        throw new Error(`Invalid start/end ISO. start=${startIso} end=${endIso}`);
    }
    if (endMs <= startMs) {
        throw new Error(`Invalid range: end must be > start. start=${startIso} end=${endIso}`);
    }

    const stopWhenOlderThanMs = Number(opts?.stopWhenOlderThanMs || 0);
    const maxPages = Math.max(1, Number(opts?.maxPages || MAX_PAGES));
    const shouldStopByOldest = (rows: any[]) => {
        if (!stopWhenOlderThanMs) return false;
        let oldest = Number.POSITIVE_INFINITY;
        for (const x of rows || []) {
            const iso = pickDateIso(x);
            const ms = dateMsFromUnknown(iso);
            if (Number.isFinite(ms) && ms < oldest) oldest = ms;
        }
        return Number.isFinite(oldest) && oldest <= stopWhenOlderThanMs;
    };

    const attempts = [
        async () => {
            const all: any[] = [];
            for (let page = 1; page <= maxPages; page++) {
                const body: any = {
                    locationId,
                    page,
                    pageLimit: PAGE_LIMIT,
                    limit: PAGE_LIMIT,
                };

                const res = await ghlFetchJson("/conversations/search", {
                    method: "POST",
                    body,
                });
                const rows = extractConversationArray(res);
                all.push(...rows);
                if (rows.length < PAGE_LIMIT) break;
                if (shouldStopByOldest(rows)) break;
            }
            return all;
        },
        async () => {
            const all: any[] = [];
            for (let page = 1; page <= maxPages; page++) {
                const body: any = {
                    page,
                    pageLimit: PAGE_LIMIT,
                    limit: PAGE_LIMIT,
                };
                const res = await ghlFetchJson(`/locations/${encodeURIComponent(locationId)}/conversations/search`, {
                    method: "POST",
                    body,
                });
                const rows = extractConversationArray(res);
                all.push(...rows);
                if (rows.length < PAGE_LIMIT) break;
                if (shouldStopByOldest(rows)) break;
            }
            return all;
        },
        async () => {
            const all: any[] = [];
            for (let page = 1; page <= maxPages; page++) {
                const body: any = {
                    page,
                    pageLimit: PAGE_LIMIT,
                    limit: PAGE_LIMIT,
                };
                const res = await ghlFetchJson("/conversations/search", {
                    method: "POST",
                    body,
                });
                const rows = extractConversationArray(res);
                all.push(...rows);
                if (rows.length < PAGE_LIMIT) break;
                if (shouldStopByOldest(rows)) break;
            }
            return all;
        },
        async () => {
            const all: any[] = [];
            for (let page = 1; page <= maxPages; page++) {
                const qs = new URLSearchParams();
                qs.set("locationId", locationId);
                qs.set("page", String(page));
                qs.set("limit", String(PAGE_LIMIT));
                const res = await ghlFetchJson(`/locations/${encodeURIComponent(locationId)}/conversations/search?${qs.toString()}`, {
                    method: "GET",
                });
                const rows = extractConversationArray(res);
                all.push(...rows);
                if (rows.length < PAGE_LIMIT) break;
                if (shouldStopByOldest(rows)) break;
            }
            return all;
        },
        async () => {
            const all: any[] = [];
            for (let page = 1; page <= maxPages; page++) {
                const qs = new URLSearchParams();
                qs.set("locationId", locationId);
                qs.set("page", String(page));
                qs.set("limit", String(PAGE_LIMIT));
                const res = await ghlFetchJson(`/conversations/search?${qs.toString()}`, { method: "GET" });
                const rows = extractConversationArray(res);
                all.push(...rows);
                if (rows.length < PAGE_LIMIT) break;
                if (shouldStopByOldest(rows)) break;
            }
            return all;
        },
        async () => {
            const all: any[] = [];
            for (let page = 1; page <= maxPages; page++) {
                const qs = new URLSearchParams();
                qs.set("page", String(page));
                qs.set("limit", String(PAGE_LIMIT));
                const res = await ghlFetchJson(`/conversations/search?${qs.toString()}`, { method: "GET" });
                const rows = extractConversationArray(res);
                all.push(...rows);
                if (rows.length < PAGE_LIMIT) break;
                if (shouldStopByOldest(rows)) break;
            }
            return all;
        },
        async () => {
            const all: any[] = [];
            for (let page = 1; page <= maxPages; page++) {
                const qs = new URLSearchParams();
                qs.set("locationId", locationId);
                qs.set("page", String(page));
                qs.set("limit", String(PAGE_LIMIT));
                const res = await ghlFetchJson(`/conversations/?${qs.toString()}`, { method: "GET" });
                const rows = extractConversationArray(res);
                all.push(...rows);
                if (rows.length < PAGE_LIMIT) break;
                if (shouldStopByOldest(rows)) break;
            }
            return all;
        },
        async () => {
            const all: any[] = [];
            for (let page = 1; page <= maxPages; page++) {
                const qs = new URLSearchParams();
                qs.set("page", String(page));
                qs.set("limit", String(PAGE_LIMIT));
                const res = await ghlFetchJson(`/conversations?${qs.toString()}`, { method: "GET" });
                const rows = extractConversationArray(res);
                all.push(...rows);
                if (rows.length < PAGE_LIMIT) break;
                if (shouldStopByOldest(rows)) break;
            }
            return all;
        },
        async () => {
            const all: any[] = [];
            for (let page = 1; page <= maxPages; page++) {
                const qs = new URLSearchParams();
                qs.set("locationId", locationId);
                qs.set("page", String(page));
                qs.set("limit", String(PAGE_LIMIT));
                const res = await ghlFetchJson(`/locations/${encodeURIComponent(locationId)}/conversations?${qs.toString()}`, {
                    method: "GET",
                });
                const rows = extractConversationArray(res);
                all.push(...rows);
                if (rows.length < PAGE_LIMIT) break;
                if (shouldStopByOldest(rows)) break;
            }
            return all;
        },
    ];

    let lastErr: unknown = null;
    for (let i = 0; i < attempts.length; i++) {
        try {
            const raw = await attempts[i]();
            const mapped = raw.map(toRow);
            return { rawCount: raw.length, rows: mapped, mappedCount: mapped.length };
        } catch (e: unknown) {
            lastErr = e;
            if (debug) {
                console.log("[conversations] attempt failed", i + 1, e);
            }
        }
    }

    throw lastErr instanceof Error
        ? new Error(`Unable to fetch conversations from GHL after ${attempts.length} endpoint variants. ${lastErr.message}`)
        : new Error("Unable to fetch conversations from GHL.");
}

async function fetchContactsAsConversationFallback(locationId: string, startIso: string, endIso: string) {
    const startMs = toMs(startIso);
    const endMs = toMs(endIso);

    const sort = [{ field: "dateAdded", direction: "desc" }];
    const all: any[] = [];
    let page = 0;
    let searchAfter: any[] | null = null;

    while (page < CONTACTS_MAX_PAGES) {
        page++;
        const body: any = {
            locationId,
            pageLimit: CONTACTS_PAGE_LIMIT,
            sort,
        };

        if (searchAfter && Array.isArray(searchAfter) && searchAfter.length) {
            body.searchAfter = searchAfter;
        } else {
            body.page = page;
        }

        let res: any = null;
        try {
            res = await with429Retry(() =>
                ghlFetchJson("/contacts/search", {
                    method: "POST",
                    body,
                }),
            );
        } catch (e: any) {
            // If we already have data, stop gracefully instead of failing whole dashboard.
            if (all.length > 0 && is429(e)) break;
            throw e;
        }

        const contacts = Array.isArray(res?.contacts) ? res.contacts : [];
        all.push(...contacts);

        const nextSearchAfter = Array.isArray(res?.searchAfter) ? res.searchAfter : null;
        if (nextSearchAfter && nextSearchAfter.length) {
            searchAfter = nextSearchAfter;
        } else if (contacts.length < CONTACTS_PAGE_LIMIT) {
            break;
        }

        if (!contacts.length) break;
        if (all.length >= 12_000) break;
        await sleep(CONTACTS_PAGE_DELAY_MS);
    }

    const rows: ConvRow[] = [];
    for (const c of all) {
        const lastIso = dateIsoFromCandidates(
            c?.lastMessageDate,
            c?.last_message_date,
            c?.lastActivity,
            c?.last_activity,
            c?.dateUpdated,
            c?.updatedAt,
            c?.dateAdded,
        );
        const ms = Number.isFinite(new Date(lastIso).getTime()) ? new Date(lastIso).getTime() : null;

        // Keep only rows in range when possible. If unknown date, keep as low-confidence row.
        if (ms !== null && (ms < startMs || ms > endMs)) continue;

        const sourceText = [
            norm(c?.source),
            norm(c?.dndSettings?.Call?.status),
            norm(c?.dndSettings?.SMS?.status),
            Array.isArray(c?.tags) ? c.tags.join(" ") : "",
        ]
            .filter(Boolean)
            .join(" ")
            .toLowerCase();

        let channel = "unknown";
        if (sourceText.includes("whatsapp")) channel = "whatsapp";
        else if (sourceText.includes("instagram")) channel = "instagram";
        else if (sourceText.includes("facebook") || sourceText.includes("messenger")) channel = "facebook";
        else if (sourceText.includes("email")) channel = "email";
        else if (sourceText.includes("sms") || sourceText.includes("text")) channel = "sms";
        else if (sourceText.includes("call") || sourceText.includes("phone")) channel = "call";

        const unread = toNum(
            c?.unreadCount ||
                c?.unread_count ||
                c?.unreadMessages ||
                c?.unread_messages,
        );
        const msgCount = Math.max(
            1,
            toNum(c?.messageCount || c?.messagesCount || c?.totalMessages || c?.total_messages || 1),
        );

        rows.push({
            id: norm(c?.id),
            contactId: norm(c?.id),
            contactName: norm(c?.contactName || c?.name || `${norm(c?.firstName)} ${norm(c?.lastName)}`.trim()),
            channel,
            direction: "unknown",
            unreadCount: Math.max(0, unread),
            messageCount: msgCount,
            snippet: norm(c?.source),
            lastMessageAt: lastIso || norm(c?.dateAdded),
            __lastMs: ms,
            state: normalizeStateName(norm(c?.state || c?.address?.state)),
            city: norm(c?.city || c?.address?.city),
            stateFrom: normalizeStateName(norm(c?.state || c?.address?.state)) ? "contact.state" : "unknown",
        });
    }

    return rows;
}

function pickOpportunityIds(contact: any) {
    const opps = Array.isArray(contact?.opportunities) ? contact.opportunities : [];
    const ids: string[] = [];
    for (const opp of opps) {
        if (typeof opp === "string") {
            const id = norm(opp);
            if (id) ids.push(id);
            continue;
        }
        if (opp && typeof opp === "object") {
            const id = norm(opp?.id || opp?.opportunityId);
            if (id) ids.push(id);
        }
    }
    return [...new Set(ids)];
}

async function resolveContactState(contactId: string, oppCache: Map<string, string | null>) {
    if (!contactId) return { state: "", city: "", from: "unknown" as const };

    const contact = (await ghlFetchJson(`/contacts/${encodeURIComponent(contactId)}`, {
        method: "GET",
    })) as Record<string, any>;

    const contactState = normalizeStateName(
        norm(contact?.state || contact?.address?.state || contact?.contact?.state),
    );
    const city = norm(contact?.city || contact?.address?.city || contact?.contact?.city);
    if (contactState) {
        return { state: contactState, city, from: "contact.state" as const };
    }

    const oppIds = pickOpportunityIds(contact);
    for (const oppId of oppIds) {
        if (oppCache.has(oppId)) {
            const cached = oppCache.get(oppId) || "";
            if (cached) {
                return { state: cached, city, from: "opportunity.source" as const };
            }
            continue;
        }

        try {
            const opp = (await ghlFetchJson(`/opportunities/${encodeURIComponent(oppId)}`, {
                method: "GET",
            })) as Record<string, any>;
            const src = norm(opp?.source || opp?.opportunity?.source);
            const inferred = normalizeStateName(inferStateFromOppSourceSafe(src));
            oppCache.set(oppId, inferred || null);
            if (inferred) return { state: inferred, city, from: "opportunity.source" as const };
        } catch {
            oppCache.set(oppId, null);
        }
    }

    return { state: "", city, from: "unknown" as const };
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
            if (cached) {
                const cachedOut: ApiResponse = {
                    ...cached,
                    cache: {
                        ...(cached.cache || {}),
                        source: "memory",
                    },
                };
                return NextResponse.json(cachedOut);
            }
        }

        const locationId = await getEffectiveLocationIdOrThrow();
        const snapshot = await readConvSnapshot(locationId);
        const snapshotFresh = !!snapshot && Date.now() - Number(snapshot.updatedAtMs || 0) <= SNAPSHOT_TTL_MS;

        let rowsSource = "ghl_conversations_api";
        const warnings: string[] = [];
        let fetchedMeta: { rawCount: number; mappedCount: number } = { rawCount: 0, mappedCount: 0 };
        let rawRows: ConvRow[] = [];
        let cacheSource: "memory" | "snapshot" | "ghl_refresh" = "ghl_refresh";
        let fetchedPages = 0;
        let usedIncremental = false;
        let refreshReason = "";
        let snapshotUpdatedAtIso = snapshot?.updatedAtMs ? new Date(snapshot.updatedAtMs).toISOString() : "";

        if (snapshotFresh && snapshot && !bust) {
            rawRows = snapshot.rows || [];
            rowsSource = "snapshot";
            cacheSource = "snapshot";
            refreshReason = "snapshot_fresh";
        } else {
            try {
                refreshReason = snapshot ? "snapshot_stale_refresh" : "snapshot_missing_full_fetch";
                const newestMs = snapshot?.newestMessageAt ? toMs(snapshot.newestMessageAt) : 0;
                const stopWhenOlderThanMs = newestMs ? Math.max(0, newestMs - SNAPSHOT_OVERLAP_MS) : 0;
                const fetched = await fetchConversations(
                    locationId,
                    start,
                    end,
                    debug,
                    snapshot
                        ? { stopWhenOlderThanMs, maxPages: SNAPSHOT_MAX_NEW_PAGES }
                        : { maxPages: MAX_PAGES },
                );
                fetchedMeta = {
                    rawCount: fetched.rawCount,
                    mappedCount: (fetched as any).mappedCount || fetched.rows.length,
                };
                fetchedPages = Math.ceil((fetched.rawCount || 0) / PAGE_LIMIT);
                usedIncremental = !!snapshot;
                cacheSource = "ghl_refresh";

                const mergedRows = [...fetched.rows, ...(snapshot?.rows || [])];
                const mergedMap = new Map<string, ConvRow>();
                for (const row of mergedRows) {
                    const id = norm(row.id);
                    const fallbackKey = `${norm(row.contactId)}|${norm(row.channel)}|${norm(row.lastMessageAt)}|${norm(row.snippet).slice(0, 80)}`;
                    const key = id || fallbackKey;
                    if (!key) continue;
                    const prev = mergedMap.get(key);
                    if (!prev) {
                        mergedMap.set(key, row);
                        continue;
                    }
                    const prevMs = Number(prev.__lastMs || 0);
                    const nextMs = Number(row.__lastMs || 0);
                    if (nextMs >= prevMs) mergedMap.set(key, row);
                }
                rawRows = Array.from(mergedMap.values());

                const cov = convRowsCoverage(rawRows);
                await writeConvSnapshot({
                    version: 1,
                    locationId,
                    updatedAtMs: Date.now(),
                    newestMessageAt: cov.newestIso,
                    oldestMessageAt: cov.oldestIso,
                    rows: rawRows,
                });
                snapshotUpdatedAtIso = new Date().toISOString();
            } catch (e: any) {
                const msg = String(e?.message || "");
                const isNotFound = msg.includes("(404)");
                warnings.push(`conversations_api_failed: ${msg}`);
                if (!isNotFound && !is429(e) && !snapshot) throw e;
                if (snapshot?.rows?.length) {
                    rowsSource = "snapshot_stale_on_error";
                    cacheSource = "snapshot";
                    refreshReason = "snapshot_stale_used_after_api_error";
                    rawRows = snapshot.rows;
                } else {
                    rowsSource = "contacts_search_fallback";
                    try {
                        rawRows = await fetchContactsAsConversationFallback(locationId, start, end);
                        fetchedMeta = {
                            rawCount: rawRows.length,
                            mappedCount: rawRows.length,
                        };
                    } catch (fallbackErr: any) {
                        warnings.push(`contacts_fallback_failed: ${String(fallbackErr?.message || fallbackErr)}`);
                        rowsSource = "empty_resilient";
                        rawRows = [];
                        fetchedMeta = { rawCount: 0, mappedCount: 0 };
                    }
                }
            }
        }

        const dedupedMap = new Map<string, ConvRow>();
        for (const row of rawRows) {
            const id = norm(row.id);
            const fallbackKey = `${norm(row.contactId)}|${norm(row.channel)}|${norm(row.lastMessageAt)}|${norm(row.snippet).slice(0, 80)}`;
            const key = id || fallbackKey;
            if (!key) continue;

            const prev = dedupedMap.get(key);
            if (!prev) {
                dedupedMap.set(key, row);
                continue;
            }
            const prevMs = Number(prev.__lastMs || 0);
            const nextMs = Number(row.__lastMs || 0);
            // Keep the newest snapshot if same conversation id appears more than once.
            if (nextMs >= prevMs) {
                dedupedMap.set(key, row);
            }
        }
        const allRows = Array.from(dedupedMap.values());
        const startMs = toMs(start);
        const endMs = toMs(end);
        const rows = allRows.filter((r) => {
            const ms = Number(r.__lastMs ?? NaN);
            if (!Number.isFinite(ms)) return true;
            return ms >= startMs && ms <= endMs;
        });
        const withMissing = rows.filter((r) => !r.state && !!r.contactId);

        const contactStateCache = new Map<string, { state: string; city: string; from: ConvRow["stateFrom"] }>();
        const oppCache = new Map<string, string | null>();

        for (const r of withMissing) {
            if (contactStateCache.has(r.contactId)) continue;
            try {
                const resolved = await resolveContactState(r.contactId, oppCache);
                contactStateCache.set(r.contactId, resolved);
            } catch {
                contactStateCache.set(r.contactId, { state: "", city: "", from: "unknown" });
            }
        }

        let inferredFromContact = 0;
        for (const r of rows) {
            if (r.state) continue;
            const resolved = contactStateCache.get(r.contactId);
            if (!resolved?.state) continue;
            r.state = normalizeStateName(resolved.state);
            if (!r.city && resolved.city) r.city = resolved.city;
            r.stateFrom = resolved.from;
            inferredFromContact++;
        }

        const byState: Record<string, number> = {};
        const byChannel: Record<string, number> = {};
        const contactSet = new Set<string>();
        let unreadConversations = 0;
        let messageCountSum = 0;
        let withState = 0;

        for (const r of rows) {
            if (r.contactId) contactSet.add(r.contactId);
            byChannel[r.channel] = (byChannel[r.channel] || 0) + 1;
            if (r.unreadCount > 0) unreadConversations++;
            messageCountSum += r.messageCount;

            const st = normalizeStateName(r.state);
            if (st) {
                r.state = st;
                byState[st] = (byState[st] || 0) + 1;
                withState++;
            } else {
                byState.__unknown = (byState.__unknown || 0) + 1;
            }
        }

        const total = rows.length;
        const allCoverage = convRowsCoverage(allRows);
        const kpis = {
            total,
            uniqueContacts: contactSet.size,
            unreadConversations,
            avgMessagesPerConversation: total ? Number((messageCountSum / total).toFixed(2)) : 0,
            withState,
            stateRate: pct(withState, total),
            inferredFromContact,
        };

        const resp: ApiResponse = {
            ok: true,
            range: { start, end },
            total,
            kpis,
            byState,
            byChannel,
            rows,
            cache: {
                source: cacheSource,
                snapshotUpdatedAt: snapshotUpdatedAtIso || undefined,
                snapshotCoverage:
                    allCoverage.newestIso || allCoverage.oldestIso
                        ? {
                            newestMessageAt: allCoverage.newestIso,
                            oldestMessageAt: allCoverage.oldestIso,
                        }
                        : undefined,
                fetchedPages: fetchedPages || undefined,
                usedIncremental: usedIncremental || undefined,
                refreshReason: refreshReason || undefined,
            },
            ...(debug
                ? {
                    debug: {
                        source: rowsSource,
                        warnings,
                        locationId,
                        rawFetchedConversations: fetchedMeta.rawCount,
                        mappedConversations: fetchedMeta.mappedCount || rows.length,
                        dedupedConversations: rows.length,
                        rowsInRange: rows.length,
                        sampleRow: rows[0] || null,
                        channels: Object.entries(byChannel).sort((a, b) => b[1] - a[1]).slice(0, 8),
                    },
                }
                : {}),
        };

        setCache(start, end, resp);
        return NextResponse.json(resp);
    } catch (e: any) {
        return NextResponse.json(
            { ok: false, error: e?.message || "Failed to load conversations dashboard." } satisfies ApiResponse,
            { status: 500 },
        );
    }
}
