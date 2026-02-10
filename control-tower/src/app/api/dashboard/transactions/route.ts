import { NextResponse } from "next/server";
import { getAgencyAccessTokenOrThrow, getEffectiveLocationIdOrThrow, ghlFetchJson } from "@/lib/ghlHttp";
import { normalizeStateName, norm } from "@/lib/ghlState";
import fs from "fs/promises";
import path from "path";

export const runtime = "nodejs";

type TxRow = {
    id: string;
    contactId: string;
    customerName: string;
    amount: number;
    currency: string;
    status: string;
    paymentMethod: string;
    source: string;
    createdAt: string;
    __createdMs: number | null;
    state: string;
    city: string;
    stateFrom: "transaction" | "contact.state" | "unknown";
    contactLifetimeNet?: number;
    contactLifetimeOrders?: number;
};

type ApiResponse = {
    ok: boolean;
    range?: { start: string; end: string };
    total?: number;
    kpis?: {
        totalTransactions: number;
        successfulTransactions: number;
        nonRevenueTransactions: number;
        grossAmount: number;
        avgTicket: number;
        refundedTransactions: number;
        refundedAmount: number;
        netAmount: number;
        withState: number;
        stateRate: number;
        inferredFromContact: number;
        uniqueCustomers: number;
        avgOrdersPerCustomer: number;
        repeatCustomerRate: number;
        avgLifetimeOrderValue: number;
    };
    byStateCount?: Record<string, number>;
    byStateAmount?: Record<string, number>;
    rows?: TxRow[];
    cache?: {
        source: "memory" | "snapshot" | "ghl_refresh";
        snapshotUpdatedAt?: string;
        snapshotCoverage?: { newestCreatedAt: string; oldestCreatedAt: string };
        fetchedPages?: number;
        usedIncremental?: boolean;
        refreshReason?: string;
    };
    debug?: Record<string, unknown>;
    error?: string;
};

type TxSnapshot = {
    version: 1;
    locationId: string;
    updatedAtMs: number;
    newestCreatedAt: string;
    oldestCreatedAt: string;
    rows: TxRow[];
};

type CacheEntry = {
    atMs: number;
    ttlMs: number;
    value: ApiResponse;
};

const RANGE_CACHE = new Map<string, CacheEntry>();
const CACHE_TTL_MS = 45_000;
const MAX_PAGES = 60;
const PAGE_LIMIT = 100;
const PAGE_DELAY_MS = 250;
const RETRY_BASE_MS = 1200;
const MAX_RETRIES_429 = 5;
const SNAPSHOT_TTL_MS = Number(process.env.TRANSACTIONS_SNAPSHOT_TTL_SEC || 900) * 1000;
const SNAPSHOT_MAX_NEW_PAGES = Math.max(3, Number(process.env.TRANSACTIONS_INCREMENTAL_MAX_PAGES || 12));
const SNAPSHOT_OVERLAP_MS = Number(process.env.TRANSACTIONS_INCREMENTAL_OVERLAP_MIN || 15) * 60 * 1000;

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function txCacheDir() {
    const root = process.env.DASH_CACHE_DIR || path.join("data", "cache");
    return path.join(process.cwd(), root, "transactions");
}

function txSnapshotPath(locationId: string) {
    const safeId = String(locationId || "unknown").replace(/[^a-zA-Z0-9_-]/g, "_");
    return path.join(txCacheDir(), `${safeId}.json`);
}

async function readTxSnapshot(locationId: string): Promise<TxSnapshot | null> {
    try {
        const p = txSnapshotPath(locationId);
        const raw = await fs.readFile(p, "utf8");
        const parsed = JSON.parse(raw) as TxSnapshot;
        if (!parsed || !Array.isArray(parsed.rows)) return null;
        if (String(parsed.locationId || "") !== String(locationId || "")) return null;
        return parsed;
    } catch {
        return null;
    }
}

async function writeTxSnapshot(snapshot: TxSnapshot) {
    const dir = txCacheDir();
    await fs.mkdir(dir, { recursive: true });
    await fs.writeFile(txSnapshotPath(snapshot.locationId), JSON.stringify(snapshot, null, 2), "utf8");
}

function rowsCoverage(rows: TxRow[]) {
    let newest = 0;
    let oldest = Number.POSITIVE_INFINITY;
    for (const r of rows) {
        const ms = Number(r.__createdMs ?? NaN);
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
    for (let attempt = 0; attempt <= MAX_RETRIES_429; attempt++) {
        try {
            return await fn();
        } catch (e: any) {
            lastErr = e;
            if (!is429(e) || attempt === MAX_RETRIES_429) break;
            const hinted = retryAfterMs(e);
            const exp = Math.round(RETRY_BASE_MS * Math.pow(1.6, attempt));
            const jitter = Math.floor(Math.random() * 450);
            await sleep(Math.max(hinted || 0, exp + jitter));
        }
    }
    throw lastErr;
}

function toMs(iso: string) {
    const d = new Date(iso);
    const t = d.getTime();
    return Number.isFinite(t) ? t : NaN;
}

function dateMsFromUnknown(v: unknown) {
    if (typeof v === "number" && Number.isFinite(v)) {
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

function pickCreatedIso(x: any) {
    const cands = [
        x.createdAt,
        x.created_at,
        x.dateAdded,
        x.date_added,
        x.updatedAt,
        x.updated_at,
        x.transactionDate,
        x.transaction_date,
        x.timestamp,
        x.time,
    ];
    for (const c of cands) {
        const ms = dateMsFromUnknown(c);
        if (Number.isFinite(ms)) return new Date(ms).toISOString();
    }
    return "";
}

function toNum(v: unknown) {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
}

function scalarFromUnknown(v: any): string {
    if (v === null || v === undefined) return "";
    if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") return String(v);
    if (typeof v !== "object") return "";
    const cands = [
        v.method,
        v.type,
        v.name,
        v.value,
        v.displayName,
        v.provider,
        v.channel,
        v.status,
        v.code,
    ];
    for (const c of cands) {
        const s = scalarFromUnknown(c);
        if (s) return s;
    }
    return "";
}

function pickAmount(x: any) {
    const raw = toNum(
        x.amount ??
            x.total ??
            x.totalAmount ??
            x.total_amount ??
            x.value ??
            x.transactionAmount ??
            x.transaction_amount ??
            0,
    );
    // Keep as-is to avoid wrong assumptions; just clamp not-a-number.
    return Number(raw.toFixed(2));
}

function pickStateFromTx(x: any) {
    const s0 = normalizeStateName(
        norm(
            x.state ||
                x.billingState ||
                x.billing_state ||
                x.billingAddress?.state ||
                x.address?.state ||
                x.customer?.state ||
                x.customer?.address?.state,
        ),
    );
    return s0 || "";
}

function pickCityFromTx(x: any) {
    return norm(
        x.city ||
            x.billingCity ||
            x.billing_city ||
            x.billingAddress?.city ||
            x.address?.city ||
            x.customer?.city ||
            x.customer?.address?.city,
    );
}

function pickContactId(x: any) {
    return norm(
        x.contactId ||
            x.contact_id ||
            x.customerId ||
            x.customer_id ||
            x.customer?.contactId ||
            x.customer?.id,
    );
}

function pickTxId(x: any) {
    return norm(
        x.id ||
            x.transactionId ||
            x.transaction_id ||
            x.paymentId ||
            x.payment_id ||
            x.referenceId ||
            x.reference_id,
    );
}

function pickStatus(x: any) {
    return norm(
        scalarFromUnknown(x.status) ||
            scalarFromUnknown(x.paymentStatus) ||
            scalarFromUnknown(x.payment_status) ||
            "unknown",
    ).toLowerCase();
}

function pickMethod(x: any) {
    const raw = norm(
        scalarFromUnknown(x.paymentMethod) ||
            scalarFromUnknown(x.payment_method) ||
            scalarFromUnknown(x.method) ||
            scalarFromUnknown(x.type) ||
            scalarFromUnknown(x.source) ||
            scalarFromUnknown(x.provider) ||
            scalarFromUnknown(x.gateway),
    ).toLowerCase();
    if (!raw) return "unknown";
    if (raw.includes("card")) return "card";
    if (raw.includes("ach") || raw.includes("bank")) return "bank";
    if (raw.includes("cash")) return "cash";
    if (raw.includes("apple")) return "apple_pay";
    if (raw.includes("google")) return "google_pay";
    return raw;
}

function isRefundLike(statusRaw: string) {
    const s = norm(statusRaw).toLowerCase();
    return s.includes("refund") || s.includes("chargeback") || s.includes("reversal") || s.includes("reversed");
}

function isSucceededRevenueStatus(statusRaw: string) {
    const s = norm(statusRaw).toLowerCase();
    if (!s) return false;
    if (isRefundLike(s)) return false;
    if (s.includes("failed") || s.includes("declined") || s.includes("canceled") || s.includes("void")) return false;
    if (s.includes("pending") || s.includes("processing") || s.includes("in_progress")) return false;
    return (
        s.includes("succeeded") ||
        s.includes("success") ||
        s.includes("paid") ||
        s.includes("completed") ||
        s.includes("captured") ||
        s.includes("settled")
    );
}

function extractTransactionsArray(res: unknown) {
    const x = res as any;
    if (Array.isArray(x?.transactions)) return x.transactions;
    if (Array.isArray(x?.data?.transactions)) return x.data.transactions;
    if (Array.isArray(x?.items)) return x.items;
    if (Array.isArray(x?.data?.items)) return x.data.items;
    if (Array.isArray(x?.data)) return x.data;
    if (Array.isArray(x)) return x;
    return [];
}

function toRow(x: any): TxRow {
    const createdAt = pickCreatedIso(x);
    const createdMs = Number.isFinite(new Date(createdAt).getTime()) ? new Date(createdAt).getTime() : null;
    const state = pickStateFromTx(x);
    return {
        id: pickTxId(x),
        contactId: pickContactId(x),
        customerName: norm(
            x.customerName ||
                x.customer?.name ||
                x.name ||
                `${norm(x.customer?.firstName)} ${norm(x.customer?.lastName)}`.trim(),
        ),
        amount: pickAmount(x),
        currency: norm(x.currency || x.currencyCode || "USD").toUpperCase() || "USD",
        status: pickStatus(x),
        paymentMethod: pickMethod(x),
        source: norm(x.source || x.provider || x.gateway || ""),
        createdAt,
        __createdMs: createdMs,
        state: state || "",
        city: pickCityFromTx(x),
        stateFrom: state ? "transaction" : "unknown",
    };
}

async function fetchTransactions(
    locationId: string,
    startIso: string,
    endIso: string,
    debug = false,
    opts?: { stopWhenOlderThanMs?: number; maxPages?: number },
) {
    const startMs = toMs(startIso);
    const endMs = toMs(endIso);
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
        throw new Error(`Invalid range. start=${startIso} end=${endIso}`);
    }

    const agencyToken = await getAgencyAccessTokenOrThrow().catch(() => "");
    const stopWhenOlderThanMs = Number(opts?.stopWhenOlderThanMs || 0);
    const maxPages = Math.max(1, Number(opts?.maxPages || MAX_PAGES));

    const shouldStopByOldest = (rows: any[]) => {
        if (!stopWhenOlderThanMs) return false;
        let oldest = Number.POSITIVE_INFINITY;
        for (const x of rows || []) {
            const iso = pickCreatedIso(x);
            const ms = dateMsFromUnknown(iso);
            if (Number.isFinite(ms) && ms < oldest) oldest = ms;
        }
        return Number.isFinite(oldest) && oldest <= stopWhenOlderThanMs;
    };

    const attempts = [
        // Documented endpoint (location token + explicit locationId)
        async () => {
            const all: any[] = [];
            for (let page = 1; page <= maxPages; page++) {
                const qs = new URLSearchParams();
                qs.set("locationId", locationId);
                qs.set("page", String(page));
                qs.set("limit", String(PAGE_LIMIT));
                const res = await with429Retry(() =>
                    ghlFetchJson(`/payments/transactions?${qs.toString()}`, { method: "GET" }),
                );
                const rows = extractTransactionsArray(res);
                all.push(...rows);
                if (rows.length < PAGE_LIMIT) break;
                if (shouldStopByOldest(rows)) break;
                await sleep(PAGE_DELAY_MS);
            }
            return all;
        },
        // Same endpoint without locationId; some accounts infer from token context.
        async () => {
            const all: any[] = [];
            for (let page = 1; page <= maxPages; page++) {
                const qs = new URLSearchParams();
                qs.set("page", String(page));
                qs.set("limit", String(PAGE_LIMIT));
                const res = await with429Retry(() =>
                    ghlFetchJson(`/payments/transactions?${qs.toString()}`, { method: "GET" }),
                );
                const rows = extractTransactionsArray(res);
                all.push(...rows);
                if (rows.length < PAGE_LIMIT) break;
                if (shouldStopByOldest(rows)) break;
                await sleep(PAGE_DELAY_MS);
            }
            return all;
        },
        // Agency token fallback + explicit locationId.
        async () => {
            if (!agencyToken) throw new Error("Agency token unavailable");
            const all: any[] = [];
            for (let page = 1; page <= maxPages; page++) {
                const qs = new URLSearchParams();
                qs.set("locationId", locationId);
                qs.set("page", String(page));
                qs.set("limit", String(PAGE_LIMIT));
                const res = await with429Retry(() =>
                    ghlFetchJson(`/payments/transactions?${qs.toString()}`, {
                        method: "GET",
                        authToken: agencyToken,
                    }),
                );
                const rows = extractTransactionsArray(res);
                all.push(...rows);
                if (rows.length < PAGE_LIMIT) break;
                if (shouldStopByOldest(rows)) break;
                await sleep(PAGE_DELAY_MS);
            }
            return all;
        },
        // Agency token + altId/altType fallback.
        async () => {
            if (!agencyToken) throw new Error("Agency token unavailable");
            const all: any[] = [];
            for (let page = 1; page <= maxPages; page++) {
                const qs = new URLSearchParams();
                qs.set("altType", "location");
                qs.set("altId", locationId);
                qs.set("page", String(page));
                qs.set("limit", String(PAGE_LIMIT));
                const res = await with429Retry(() =>
                    ghlFetchJson(`/payments/transactions?${qs.toString()}`, {
                        method: "GET",
                        authToken: agencyToken,
                    }),
                );
                const rows = extractTransactionsArray(res);
                all.push(...rows);
                if (rows.length < PAGE_LIMIT) break;
                if (shouldStopByOldest(rows)) break;
                await sleep(PAGE_DELAY_MS);
            }
            return all;
        },
    ];

    let lastErr: unknown = null;
    const attemptErrs: string[] = [];
    for (let i = 0; i < attempts.length; i++) {
        try {
            const raw = await attempts[i]();
            const mapped = raw.map(toRow);
            return {
                rawCount: raw.length,
                mappedCount: mapped.length,
                rows: mapped,
                startMs,
                endMs,
                usedMaxPages: maxPages,
                stopWhenOlderThanMs,
            };
        } catch (e: unknown) {
            lastErr = e;
            attemptErrs.push(`attempt ${i + 1}: ${e instanceof Error ? e.message : String(e)}`);
            if (debug) console.log("[transactions] attempt failed", i + 1, e);
        }
    }

    throw lastErr instanceof Error
        ? new Error(
            `Unable to fetch transactions after ${attempts.length} endpoint variants for locationId=${locationId}. ${lastErr.message}. Details: ${attemptErrs.join(" | ")}`,
        )
        : new Error("Unable to fetch transactions.");
}

async function resolveContactState(contactId: string) {
    if (!contactId) return { state: "", city: "", from: "unknown" as const };
    try {
        const c = (await with429Retry(() =>
            ghlFetchJson(`/contacts/${encodeURIComponent(contactId)}`, { method: "GET" }),
        )) as any;
        const state = normalizeStateName(norm(c?.state || c?.address?.state || c?.contact?.state));
        const city = norm(c?.city || c?.address?.city || c?.contact?.city);
        if (state) return { state, city, from: "contact.state" as const };
        return { state: "", city, from: "unknown" as const };
    } catch {
        return { state: "", city: "", from: "unknown" as const };
    }
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
        const snapshot = await readTxSnapshot(locationId);
        const snapshotFresh = !!snapshot && Date.now() - Number(snapshot.updatedAtMs || 0) <= SNAPSHOT_TTL_MS;

        let allRowsSource: TxRow[] = [];
        let cacheSource: "memory" | "snapshot" | "ghl_refresh" = "ghl_refresh";
        let fetchedPages = 0;
        let usedIncremental = false;
        let refreshReason = "";
        let snapshotUpdatedAtIso = snapshot?.updatedAtMs ? new Date(snapshot.updatedAtMs).toISOString() : "";

        if (snapshotFresh && snapshot && !bust) {
            allRowsSource = snapshot.rows || [];
            cacheSource = "snapshot";
            refreshReason = "snapshot_fresh";
        } else {
            refreshReason = snapshot ? "snapshot_stale_refresh" : "snapshot_missing_full_fetch";
            const snapshotNewestMs = snapshot?.newestCreatedAt ? toMs(snapshot.newestCreatedAt) : 0;
            const stopWhenOlderThanMs = snapshotNewestMs
                ? Math.max(0, snapshotNewestMs - SNAPSHOT_OVERLAP_MS)
                : 0;

            const fetched = await fetchTransactions(
                locationId,
                start,
                end,
                debug,
                snapshot
                    ? {
                        stopWhenOlderThanMs,
                        maxPages: SNAPSHOT_MAX_NEW_PAGES,
                    }
                    : {
                        maxPages: MAX_PAGES,
                    },
            );
            fetchedPages = Math.ceil((fetched.rawCount || 0) / PAGE_LIMIT);
            usedIncremental = !!snapshot;
            cacheSource = "ghl_refresh";

            const baseRows = snapshot?.rows || [];
            const merged = [...fetched.rows, ...baseRows];

            // Deduplicate merged rows by tx id/fallback key.
            const dedupeAll = new Map<string, TxRow>();
            for (const row of merged) {
                const key = norm(row.id) || `${norm(row.contactId)}|${norm(row.createdAt)}|${row.amount}|${norm(row.status)}`;
                if (!key) continue;
                const prev = dedupeAll.get(key);
                if (!prev) {
                    dedupeAll.set(key, row);
                    continue;
                }
                const p = Number(prev.__createdMs || 0);
                const n = Number(row.__createdMs || 0);
                if (n >= p) dedupeAll.set(key, row);
            }
            allRowsSource = Array.from(dedupeAll.values());

            const cov = rowsCoverage(allRowsSource);
            await writeTxSnapshot({
                version: 1,
                locationId,
                updatedAtMs: Date.now(),
                newestCreatedAt: cov.newestIso,
                oldestCreatedAt: cov.oldestIso,
                rows: allRowsSource,
            });
            snapshotUpdatedAtIso = new Date().toISOString();
        }

        // Deduplicate source rows again defensively (handles old snapshot format/duplicates).
        const dedupe = new Map<string, TxRow>();
        for (const row of allRowsSource) {
            const key = norm(row.id) || `${norm(row.contactId)}|${norm(row.createdAt)}|${row.amount}|${norm(row.status)}`;
            if (!key) continue;
            const prev = dedupe.get(key);
            if (!prev) {
                dedupe.set(key, row);
                continue;
            }
            const p = Number(prev.__createdMs || 0);
            const n = Number(row.__createdMs || 0);
            if (n >= p) dedupe.set(key, row);
        }
        const allRows = Array.from(dedupe.values());
        const covNow = rowsCoverage(allRows);
        const startMs = toMs(start);
        const endMs = toMs(end);
        const rows = allRows.filter((r) => {
            const ms = Number(r.__createdMs ?? NaN);
            if (!Number.isFinite(ms)) return true;
            return ms >= startMs && ms <= endMs;
        });

        const stateCache = new Map<string, { state: string; city: string; from: TxRow["stateFrom"] }>();
        const missing = rows.filter((r) => !r.state && !!r.contactId);
        for (const r of missing) {
            if (stateCache.has(r.contactId)) continue;
            const resolved = await resolveContactState(r.contactId);
            stateCache.set(r.contactId, resolved);
        }

        let inferredFromContact = 0;
        for (const r of rows) {
            if (r.state) continue;
            const resolved = stateCache.get(r.contactId);
            if (!resolved?.state) continue;
            r.state = normalizeStateName(resolved.state);
            if (!r.city && resolved.city) r.city = resolved.city;
            r.stateFrom = resolved.from;
            inferredFromContact++;
        }

        const byStateCount: Record<string, number> = {};
        const byStateAmount: Record<string, number> = {};
        let grossAmount = 0;
        let refundedAmount = 0;
        let refundedTransactions = 0;
        let successfulTransactions = 0;
        let nonRevenueTransactions = 0;
        let withState = 0;
        const byContactInScope = new Map<string, { count: number; gross: number; refunded: number }>();

        for (const r of rows) {
            const status = norm(r.status).toLowerCase();
            const refundLike = isRefundLike(status);
            const succeededRevenue = isSucceededRevenueStatus(status);
            if (refundLike) {
                refundedTransactions++;
                refundedAmount += r.amount;
            }
            if (succeededRevenue) {
                successfulTransactions++;
                grossAmount += r.amount;
            } else {
                nonRevenueTransactions++;
            }

            const cid = norm(r.contactId);
            if (cid) {
                const prev = byContactInScope.get(cid) || { count: 0, gross: 0, refunded: 0 };
                prev.count += 1;
                if (succeededRevenue) prev.gross += r.amount;
                if (refundLike) prev.refunded += r.amount;
                byContactInScope.set(cid, prev);
            }

            const st = normalizeStateName(r.state);
            if (!st) {
                byStateCount.__unknown = (byStateCount.__unknown || 0) + 1;
                if (succeededRevenue) {
                    byStateAmount.__unknown = Number(((byStateAmount.__unknown || 0) + r.amount).toFixed(2));
                }
                continue;
            }

            withState++;
            byStateCount[st] = (byStateCount[st] || 0) + 1;
            if (succeededRevenue) {
                byStateAmount[st] = Number(((byStateAmount[st] || 0) + r.amount).toFixed(2));
            }
        }

        const lifetimeByContact = new Map<string, { gross: number; net: number; orders: number }>();
        for (const r of allRows) {
            const cid = norm(r.contactId);
            if (!cid) continue;
            const status = norm(r.status).toLowerCase();
            const isRefund = isRefundLike(status);
            const isSucceeded = isSucceededRevenueStatus(status);
            const prev = lifetimeByContact.get(cid) || { gross: 0, net: 0, orders: 0 };
            if (isSucceeded) prev.gross += r.amount;
            if (isRefund) prev.net += -Math.abs(r.amount);
            else if (isSucceeded) prev.net += r.amount;
            prev.orders += 1;
            lifetimeByContact.set(cid, prev);
        }

        const scopedContactIds = Array.from(byContactInScope.keys());
        const avgLifetimeOrderValue = scopedContactIds.length
            ? Number(
                (
                    scopedContactIds.reduce((acc, cid) => acc + Number(lifetimeByContact.get(cid)?.net || 0), 0) /
                    scopedContactIds.length
                ).toFixed(2),
            )
            : 0;

        const uniqueCustomers = scopedContactIds.length;
        const repeatCustomers = scopedContactIds.filter((cid) => Number(byContactInScope.get(cid)?.count || 0) > 1).length;

        for (const r of rows) {
            const cid = norm(r.contactId);
            if (!cid) continue;
            const life = lifetimeByContact.get(cid);
            if (!life) continue;
            r.contactLifetimeNet = Number((life.net || 0).toFixed(2));
            r.contactLifetimeOrders = Number(life.orders || 0);
        }

        const total = rows.length;
        const netAmount = Number((grossAmount - refundedAmount).toFixed(2));
        const kpis = {
            totalTransactions: total,
            successfulTransactions,
            nonRevenueTransactions,
            grossAmount: Number(grossAmount.toFixed(2)),
            avgTicket: successfulTransactions ? Number((grossAmount / successfulTransactions).toFixed(2)) : 0,
            refundedTransactions,
            refundedAmount: Number(refundedAmount.toFixed(2)),
            netAmount,
            withState,
            stateRate: pct(withState, total),
            inferredFromContact,
            uniqueCustomers,
            avgOrdersPerCustomer: uniqueCustomers ? Number((total / uniqueCustomers).toFixed(2)) : 0,
            repeatCustomerRate: uniqueCustomers ? pct(repeatCustomers, uniqueCustomers) : 0,
            avgLifetimeOrderValue,
        };

        const resp: ApiResponse = {
            ok: true,
            range: { start, end },
            total,
            kpis,
            byStateCount,
            byStateAmount,
            rows,
            cache: {
                source: cacheSource,
                snapshotUpdatedAt: snapshotUpdatedAtIso || undefined,
                snapshotCoverage:
                    covNow.newestIso || covNow.oldestIso
                        ? { newestCreatedAt: covNow.newestIso, oldestCreatedAt: covNow.oldestIso }
                        : undefined,
                fetchedPages: fetchedPages || undefined,
                usedIncremental: usedIncremental || undefined,
                refreshReason: refreshReason || undefined,
            },
            ...(debug
                ? {
                    debug: {
                        locationId,
                        dedupedSnapshotTransactions: allRows.length,
                        scopedTransactions: rows.length,
                        sampleRow: rows[0] || null,
                        scopedUniqueCustomers: uniqueCustomers,
                        scopedRepeatCustomers: repeatCustomers,
                    },
                }
                : {}),
        };

        setCache(start, end, resp);
        return NextResponse.json(resp);
    } catch (e: any) {
        return NextResponse.json(
            { ok: false, error: e?.message || "Failed to load transactions dashboard." } satisfies ApiResponse,
            { status: 500 },
        );
    }
}
