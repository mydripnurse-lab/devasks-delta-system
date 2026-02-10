import { NextResponse } from "next/server";
import { loadSheetTabIndex } from "../../../../../../services/sheetsClient.js";
import {
  getAgencyAccessTokenOrThrow,
  getEffectiveCompanyIdOrThrow,
  getEffectiveLocationIdOrThrow,
  ghlFetchJson,
} from "@/lib/ghlHttp";
import { inferStateFromText, normalizeStateName, norm } from "@/lib/ghlState";
import fs from "fs/promises";
import path from "path";

export const runtime = "nodejs";

type ApptRow = {
  id: string;
  locationId: string;
  contactId: string;
  contactName: string;
  title: string;
  status: string;
  statusNormalized:
    | "scheduled"
    | "confirmed"
    | "completed"
    | "cancelled"
    | "no_show"
    | "rescheduled"
    | "unknown";
  calendarId: string;
  startAt: string;
  endAt: string;
  __startMs: number | null;
  state: string;
  city: string;
  stateFrom: "appointment" | "contact.state" | "unknown";
};

type LostBookingRow = {
  id: string;
  locationId: string;
  contactId: string;
  contactName: string;
  pipelineId: string;
  pipelineName: string;
  stageId: string;
  stageName: string;
  source: string;
  state: string;
  county: string;
  city: string;
  accountName: string;
  value: number;
  currency: string;
  status: string;
  createdAt: string;
  updatedAt: string;
  __eventMs: number | null;
};

type LostBookingsBlock = {
  total: number;
  uniqueContacts: number;
  valueTotal: number;
  opportunityValueTotal: number;
  stageValueTotal: number;
  withState: number;
  stateRate: number;
  byState: Record<string, number>;
  byCounty: Record<string, number>;
  byCity: Record<string, number>;
  rows: LostBookingRow[];
};

type ApiResponse = {
  ok: boolean;
  range?: { start: string; end: string };
  total?: number;
  kpis?: {
    total: number;
    uniqueContacts: number;
    scheduled: number;
    confirmed: number;
    completed: number;
    cancelled: number;
    noShow: number;
    rescheduled: number;
    showRate: number;
    cancellationRate: number;
    noShowRate: number;
    withState: number;
    stateRate: number;
  };
  byState?: Record<string, number>;
  byStatus?: Record<string, number>;
  byLocation?: Record<string, number>;
  rows?: ApptRow[];
  lostBookings?: LostBookingsBlock;
  cache?: {
    source: "memory" | "snapshot" | "ghl_refresh";
    snapshotUpdatedAt?: string;
    snapshotCoverage?: { newestStartAt: string; oldestStartAt: string };
    refreshedLocations?: number;
    totalLocations?: number;
    usedIncremental?: boolean;
    refreshReason?: string;
  };
  debug?: Record<string, unknown>;
  error?: string;
};

type LocationSnapshot = {
  version: 1;
  locationId: string;
  updatedAtMs: number;
  newestStartAt: string;
  oldestStartAt: string;
  rows: ApptRow[];
  lostRows: LostBookingRow[];
  lostDiscovery?: {
    pipelineIds: string[];
    stageIds: string[];
    discoveredAt: string;
    stageApiRows?: number;
  };
};

type RangeCacheEntry = {
  atMs: number;
  ttlMs: number;
  value: ApiResponse;
};

type LocationIdsCache = {
  atMs: number;
  ids: string[];
};

type LocationDirectoryItem = {
  locationId: string;
  state: string;
  county: string;
  city: string;
  accountName: string;
};

type LocationDirectoryCache = {
  atMs: number;
  ids: string[];
  byLocationId: Map<string, LocationDirectoryItem>;
  countiesByState: Map<string, Set<string>>;
  citiesByState: Map<string, Set<string>>;
  accountNamesByLocation: Map<string, string>;
  countiesByAccountToken: Map<string, { county: string; state: string; city: string; accountName: string }>;
};

const RANGE_CACHE = new Map<string, RangeCacheEntry>();
const RANGE_CACHE_TTL_MS = 45_000;

let LOCATION_IDS_CACHE: LocationIdsCache | null = null;
const LOCATION_IDS_CACHE_TTL_MS = 5 * 60 * 1000;
let LOCATION_DIRECTORY_CACHE: LocationDirectoryCache | null = null;

const API_BASE = "https://services.leadconnectorhq.com";
const API_VERSION = "2021-07-28";

const SNAPSHOT_TTL_MS = Number(process.env.APPOINTMENTS_SNAPSHOT_TTL_SEC || 1800) * 1000;
const SNAPSHOT_MAX_CONTACT_PAGES = Math.max(
  3,
  Number(process.env.APPOINTMENTS_INCREMENTAL_MAX_CONTACT_PAGES || 10),
);
const SNAPSHOT_MAX_CONTACTS = Math.max(50, Number(process.env.APPOINTMENTS_INCREMENTAL_MAX_CONTACTS || 400));
const FULL_SYNC_MAX_CONTACT_PAGES = Math.max(5, Number(process.env.APPOINTMENTS_FULL_MAX_CONTACT_PAGES || 40));
const FULL_SYNC_MAX_CONTACTS = Math.max(200, Number(process.env.APPOINTMENTS_FULL_MAX_CONTACTS || 2500));
const SNAPSHOT_CONTACT_OVERLAP_MS =
  Number(process.env.APPOINTMENTS_INCREMENTAL_OVERLAP_HOURS || 48) * 60 * 60 * 1000;

const LOCATION_REFRESH_BUDGET = Math.max(1, Number(process.env.APPOINTMENTS_REFRESH_LOCATIONS_PER_REQUEST || 5));
const MAX_LOCATIONS = Math.max(1, Number(process.env.APPOINTMENTS_MAX_LOCATIONS || 80));

const CONTACTS_PAGE_LIMIT = 100;
const CONTACTS_PAGE_DELAY_MS = 260;
const APPT_CONTACT_CONCURRENCY = Math.max(1, Number(process.env.APPOINTMENTS_CONTACT_CONCURRENCY || 3));
const APPT_CALL_DELAY_MS = 130;
const RETRY_BASE_MS = 1200;
const MAX_RETRIES_429 = 5;
const LOST_OPP_STAGE_MAX_PAGES = Math.max(2, Number(process.env.APPOINTMENTS_LOST_STAGE_MAX_PAGES || 20));
const LOST_OPP_STAGE_PAGE_LIMIT = Math.max(20, Number(process.env.APPOINTMENTS_LOST_STAGE_PAGE_LIMIT || 100));
const LOST_BOOKINGS_PIPELINE = String(process.env.APPOINTMENTS_LOST_PIPELINE_NAME || "Lead Generator Bookings").trim();
const LOST_BOOKINGS_STAGE = String(process.env.APPOINTMENTS_LOST_STAGE_NAME || "New Leads (Qualified)").trim();
const LOST_BOOKINGS_PIPELINE_ID = String(process.env.APPOINTMENTS_LOST_PIPELINE_ID || "").trim();
const LOST_BOOKINGS_STAGE_ID = String(process.env.APPOINTMENTS_LOST_STAGE_ID || "").trim();
const INCLUDE_SHEET_LOCATIONS = String(process.env.APPOINTMENTS_INCLUDE_SHEET_LOCATIONS || "").trim() === "1";
const REQUEST_MAX_MS = Math.max(10_000, Number(process.env.APPOINTMENTS_REQUEST_MAX_MS || 45_000));
const PLACE_NAME_CORRECTIONS: Record<string, string> = {
  rincon: "Rincón",
  mayaguez: "Mayagüez",
  manati: "Manatí",
  canovanas: "Canóvanas",
  loiza: "Loíza",
  anasco: "Añasco",
};

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isPastDeadline(deadlineAtMs?: number) {
  return Number.isFinite(deadlineAtMs) && Date.now() >= Number(deadlineAtMs);
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
      const jitter = Math.floor(Math.random() * 400);
      await sleep(Math.max(hinted || 0, exp + jitter));
    }
  }
  throw lastErr;
}

function rangeCacheKey(start: string, end: string) {
  return `${start}__${end}`;
}

function getRangeCache(start: string, end: string) {
  const hit = RANGE_CACHE.get(rangeCacheKey(start, end));
  if (!hit) return null;
  if (Date.now() - hit.atMs > hit.ttlMs) {
    RANGE_CACHE.delete(rangeCacheKey(start, end));
    return null;
  }
  return hit.value;
}

function setRangeCache(start: string, end: string, value: ApiResponse) {
  RANGE_CACHE.set(rangeCacheKey(start, end), {
    atMs: Date.now(),
    ttlMs: RANGE_CACHE_TTL_MS,
    value,
  });
}

function apptCacheDir() {
  const root = process.env.DASH_CACHE_DIR || path.join("data", "cache");
  return path.join(process.cwd(), root, "appointments");
}

function apptSnapshotPath(locationId: string) {
  const safeId = String(locationId || "unknown").replace(/[^a-zA-Z0-9_-]/g, "_");
  return path.join(apptCacheDir(), `${safeId}.json`);
}

async function readLocationSnapshot(locationId: string): Promise<LocationSnapshot | null> {
  try {
    const raw = await fs.readFile(apptSnapshotPath(locationId), "utf8");
    const parsed = JSON.parse(raw) as LocationSnapshot;
    if (!parsed || !Array.isArray(parsed.rows)) return null;
    if (String(parsed.locationId || "") !== String(locationId || "")) return null;
    if (!Array.isArray(parsed.lostRows)) parsed.lostRows = [];
    return parsed;
  } catch {
    return null;
  }
}

async function writeLocationSnapshot(snapshot: LocationSnapshot) {
  await fs.mkdir(apptCacheDir(), { recursive: true });
  await fs.writeFile(apptSnapshotPath(snapshot.locationId), JSON.stringify(snapshot, null, 2), "utf8");
}

function rowsCoverage(rows: ApptRow[]) {
  let newest = 0;
  let oldest = Number.POSITIVE_INFINITY;
  for (const r of rows) {
    const ms = Number(r.__startMs ?? NaN);
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

function normalizeApptStatus(raw: unknown): ApptRow["statusNormalized"] {
  const s = norm(raw).toLowerCase();
  if (!s) return "unknown";
  if (s.includes("no_show") || s.includes("noshow")) return "no_show";
  if (s.includes("cancel")) return "cancelled";
  if (s.includes("resched")) return "rescheduled";
  if (s.includes("complete") || s.includes("show") || s.includes("attended")) return "completed";
  if (s.includes("confirm")) return "confirmed";
  if (s.includes("new") || s.includes("book") || s.includes("sched")) return "scheduled";
  return "unknown";
}

function pickStartIso(a: any) {
  const cands = [
    a.startTime,
    a.start_time,
    a.appointmentStartTime,
    a.appointment_start_time,
    a.dateAdded,
    a.createdAt,
  ];
  for (const c of cands) {
    const ms = dateMsFromUnknown(c);
    if (Number.isFinite(ms)) return new Date(ms).toISOString();
  }
  return "";
}

function pickEndIso(a: any) {
  const cands = [a.endTime, a.end_time, a.appointmentEndTime, a.appointment_end_time];
  for (const c of cands) {
    const ms = dateMsFromUnknown(c);
    if (Number.isFinite(ms)) return new Date(ms).toISOString();
  }
  return "";
}

function extractArray(res: unknown, keys: string[]) {
  const x = res as any;
  for (const k of keys) {
    if (Array.isArray(x?.[k])) return x[k];
    if (Array.isArray(x?.data?.[k])) return x.data[k];
  }
  if (Array.isArray(x?.items)) return x.items;
  if (Array.isArray(x?.data?.items)) return x.data.items;
  if (Array.isArray(x?.data)) return x.data;
  if (Array.isArray(x)) return x;
  return [];
}

function inferStateFromContactOpportunities(opportunities: any[]) {
  for (const o of opportunities || []) {
    const fromSource = normalizeStateName(inferStateFromText(getOpportunitySource(o)));
    if (fromSource) return fromSource;
    const fromRaw = normalizeStateName(norm(o?.state || o?.contact?.state || o?.address?.state));
    if (fromRaw) return fromRaw;
  }
  return "";
}

function extractOpportunityIdFromUnknown(v: unknown) {
  if (!v) return "";
  if (typeof v === "string" || typeof v === "number") return norm(v);
  if (typeof v === "object") {
    const x = v as any;
    return norm(x?.id || x?.opportunityId || x?._id);
  }
  return "";
}

function extractStageIdFromOpportunity(o: any) {
  return norm(
    o?.stageId ||
      o?.pipelineStageId ||
      o?.pipelineStageID ||
      o?.pipeline_stage_id ||
      o?.pipeline_stage_ids?.[0] ||
      o?.stage_id ||
      o?.stage?.id ||
      o?.pipelineStage?.id ||
      o?.pipeline_stage?.id ||
      o?.opportunity?.stageId ||
      o?.opportunity?.stage?.id,
  );
}

function extractPipelineIdFromOpportunity(o: any) {
  return norm(
    o?.pipelineId ||
      o?.pipeline_id ||
      o?.pipeline?.id ||
      o?.pipeline?.pipelineId ||
      o?.opportunity?.pipelineId ||
      o?.opportunity?.pipeline?.id,
  );
}

function extractOpportunityIdsFromContact(raw: any) {
  const ids = new Set<string>();
  const list: any[] = [];
  if (Array.isArray(raw?.opportunities)) list.push(...raw.opportunities);
  if (Array.isArray(raw?.contact?.opportunities)) list.push(...raw.contact.opportunities);
  if (Array.isArray(raw?.opportunityIds)) list.push(...raw.opportunityIds);
  if (Array.isArray(raw?.contact?.opportunityIds)) list.push(...raw.contact.opportunityIds);
  if (raw?.opportunityId) list.push(raw.opportunityId);
  if (raw?.contact?.opportunityId) list.push(raw.contact.opportunityId);
  for (const item of list) {
    const id = extractOpportunityIdFromUnknown(item);
    if (id) ids.add(id);
  }
  return Array.from(ids);
}

type ContactLite = {
  id: string;
  name: string;
  state: string;
  city: string;
  updatedMs: number;
  opportunityIds: string[];
  opportunities: any[];
};

type LostDiscovery = {
  pipelineIds: string[];
  stageIds: string[];
};

type LostStageRefreshResult = {
  rows: LostBookingRow[];
  discovery: LostDiscovery;
  stageApiRows: number;
  errors?: string[];
};

const locationTokenCache = new Map<string, { token: string; expiresAtMs: number }>();

async function getLocationTokenFor(locationId: string) {
  const now = Date.now();
  const hit = locationTokenCache.get(locationId);
  if (hit && hit.expiresAtMs - 30_000 > now) return hit.token;

  const agencyToken = await getAgencyAccessTokenOrThrow();
  const companyId = await getEffectiveCompanyIdOrThrow();

  const res = await fetch(`${API_BASE}/oauth/locationToken`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${agencyToken}`,
      Version: API_VERSION,
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ companyId, locationId }),
  });

  const txt = await res.text();
  let data: any = {};
  try {
    data = JSON.parse(txt);
  } catch {
    data = { raw: txt };
  }

  if (!res.ok) {
    throw new Error(`locationToken failed (${res.status}) loc=${locationId}: ${JSON.stringify(data)}`);
  }

  const token = norm(data?.access_token);
  if (!token) throw new Error(`locationToken missing access_token for loc=${locationId}`);

  const expiresIn = Number(data?.expires_in || 3600);
  const expiresAtMs = now + Math.max(60, expiresIn) * 1000;
  locationTokenCache.set(locationId, { token, expiresAtMs });
  return token;
}

function s(v: unknown) {
  return String(v ?? "").trim();
}

function asObj(v: unknown): Record<string, unknown> {
  return v && typeof v === "object" ? (v as Record<string, unknown>) : {};
}

function safeSearchToken(v: unknown) {
  return s(v)
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[^\w\s-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function textIncludesAllWords(haystack: string, needle: string) {
  const h = safeSearchToken(haystack);
  const words = safeSearchToken(needle)
    .split(" ")
    .map((x) => x.trim())
    .filter(Boolean);
  if (!words.length) return false;
  return words.every((w) => h.includes(w));
}

function isTrue(v: unknown) {
  const t = s(v).toLowerCase();
  return t === "true" || t === "1" || t === "yes" || t === "y";
}

function addToSetMap(map: Map<string, Set<string>>, key: string, value: string) {
  const k = safeSearchToken(key);
  const v = safeSearchToken(value);
  if (!k || !v) return;
  if (!map.has(k)) map.set(k, new Set<string>());
  map.get(k)!.add(v);
}

function inferCityFromAccountName(accountName: string) {
  const raw = norm(accountName);
  if (!raw) return "";
  const left = raw.split("-")[0]?.trim() || raw;
  const noPrefix = left.replace(/^my\s+drip\s+nurse\s+/i, "").trim();
  if (!noPrefix) return "";
  const parts = noPrefix.split(",").map((x) => norm(x)).filter(Boolean);
  if (!parts.length) return "";
  const city = parts[0];
  if (/county$/i.test(city)) return "";
  return city;
}

function toDisplayPlaceName(v: unknown) {
  const raw = norm(v).replace(/\s+/g, " ").trim();
  if (!raw) return "";
  const fixed = PLACE_NAME_CORRECTIONS[safeSearchToken(raw)] || raw;
  return fixed
    .toLocaleLowerCase("es-US")
    .replace(/(^|\s|-)\p{L}/gu, (m) => m.toLocaleUpperCase("es-US"));
}

function pickContactState(raw: any) {
  return normalizeStateName(
    norm(
      raw?.state ||
        raw?.address?.state ||
        raw?.contact?.state ||
        raw?.contact?.address?.state ||
        raw?.addresses?.[0]?.state ||
        raw?.dndSettings?.state,
    ),
  );
}

function pickContactCity(raw: any) {
  return norm(
    raw?.city ||
      raw?.address?.city ||
      raw?.contact?.city ||
      raw?.contact?.address?.city ||
      raw?.addresses?.[0]?.city,
  );
}

async function getLocationDirectory() {
  const now = Date.now();
  if (
    LOCATION_DIRECTORY_CACHE &&
    LOCATION_IDS_CACHE &&
    now - LOCATION_DIRECTORY_CACHE.atMs <= LOCATION_IDS_CACHE_TTL_MS &&
    now - LOCATION_IDS_CACHE.atMs <= LOCATION_IDS_CACHE_TTL_MS
  ) {
    return LOCATION_DIRECTORY_CACHE;
  }

  const spreadsheetId =
    process.env.GOOGLE_SHEETS_SPREADSHEET_ID ||
    process.env.GOOGLE_SHEET_ID ||
    process.env.SPREADSHEET_ID ||
    "";
  const countyTab = process.env.GOOGLE_SHEET_COUNTY_TAB || "Counties";
  const cityTab = process.env.GOOGLE_SHEET_CITY_TAB || "Cities";

  const ids = new Set<string>();
  const byLocationId = new Map<string, LocationDirectoryItem>();
  const countiesByState = new Map<string, Set<string>>();
  const citiesByState = new Map<string, Set<string>>();
  const accountNamesByLocation = new Map<string, string>();
  const countiesByAccountToken = new Map<string, { county: string; state: string; city: string; accountName: string }>();

  const envLoc = await getEffectiveLocationIdOrThrow().catch(() => "");
  if (envLoc) ids.add(envLoc);

  if (spreadsheetId) {
    const countiesIdx = await loadSheetTabIndex({
      spreadsheetId,
      sheetName: countyTab,
      range: "A:AZ",
      keyHeaders: ["State", "County"],
      logScope: "appointments-dashboard",
    }).catch(() => null);

    if (countiesIdx) {
      const iStatus = countiesIdx.headerMap.get("Status");
      const iLoc = countiesIdx.headerMap.get("Location Id");
      const iState = countiesIdx.headerMap.get("State");
      const iCounty = countiesIdx.headerMap.get("County");
      const iAccount = countiesIdx.headerMap.get("Account Name");

      if (iStatus !== undefined) {
        for (const row of countiesIdx.rows || []) {
          const statusOk = isTrue(row?.[iStatus]);
          const locId = iLoc !== undefined ? norm(row?.[iLoc]) : "";
          if (statusOk && locId && INCLUDE_SHEET_LOCATIONS) ids.add(locId);
          if (!statusOk) continue;

          const state = normalizeStateName(norm(row?.[iState ?? -1]));
          const county = norm(row?.[iCounty ?? -1]);
          const accountName = norm(row?.[iAccount ?? -1]);
          const city = inferCityFromAccountName(accountName);

          if (locId) {
            byLocationId.set(locId, {
              locationId: locId,
              state,
              county,
              city,
              accountName,
            });
          }
          const accountToken = safeSearchToken(accountName);
          if (locId) accountNamesByLocation.set(locId, accountToken);
          if (accountToken && county) {
            countiesByAccountToken.set(accountToken, { county, state, city, accountName });
          }
          if (state && county) addToSetMap(countiesByState, state, county);
          if (state && city) addToSetMap(citiesByState, state, city);
        }
      }
    }

    const citiesIdx = await loadSheetTabIndex({
      spreadsheetId,
      sheetName: cityTab,
      range: "A:AZ",
      keyHeaders: ["State", "County", "City"],
      logScope: "appointments-dashboard",
    }).catch(() => null);

    if (citiesIdx) {
      const cStatus = citiesIdx.headerMap.get("Status");
      const cLoc = citiesIdx.headerMap.get("Location Id");
      const cState = citiesIdx.headerMap.get("State");
      const cCounty = citiesIdx.headerMap.get("County");
      const cCity = citiesIdx.headerMap.get("City");

      if (cStatus !== undefined) {
        for (const row of citiesIdx.rows || []) {
          const statusOk = isTrue(row?.[cStatus]);
          const locId = cLoc !== undefined ? norm(row?.[cLoc]) : "";
          if (!statusOk) continue;
          const state = normalizeStateName(norm(row?.[cState ?? -1]));
          const county = norm(row?.[cCounty ?? -1]);
          const city = norm(row?.[cCity ?? -1]);
          if (state && county) addToSetMap(countiesByState, state, county);
          if (state && city) addToSetMap(citiesByState, state, city);
          if (locId && byLocationId.has(locId)) {
            const prev = byLocationId.get(locId)!;
            byLocationId.set(locId, { ...prev, city: prev.city || city });
          }
        }
      }
    }
  }

  const out = Array.from(ids).slice(0, MAX_LOCATIONS);
  LOCATION_IDS_CACHE = { atMs: now, ids: out };
  LOCATION_DIRECTORY_CACHE = {
    atMs: now,
    ids: out,
    byLocationId,
    countiesByState,
    citiesByState,
    accountNamesByLocation,
    countiesByAccountToken,
  };
  return LOCATION_DIRECTORY_CACHE;
}

async function fetchContactsForLocation(
  locationId: string,
  authToken: string,
  opts: { maxPages: number; maxContacts: number; stopUpdatedBeforeMs?: number; deadlineAtMs?: number },
) {
  const all: ContactLite[] = [];
  let searchAfter: any[] | null = null;

  for (let page = 1; page <= opts.maxPages; page++) {
    if (isPastDeadline(opts.deadlineAtMs)) {
      console.warn(`[appointments] contacts timeout loc=${locationId} page=${page} fetched=${all.length}`);
      break;
    }
    const basePageBody: Record<string, unknown> = {
      locationId,
      sort: [{ field: "dateUpdated", direction: "desc" }],
      page,
    };
    const baseSearchAfterBody: Record<string, unknown> = {
      locationId,
      sort: [{ field: "dateUpdated", direction: "desc" }],
      searchAfter: searchAfter && searchAfter.length ? searchAfter : undefined,
    };
    const variants: Array<Record<string, unknown>> = searchAfter && searchAfter.length
      ? [
          baseSearchAfterBody,
          { ...baseSearchAfterBody, pageLimit: CONTACTS_PAGE_LIMIT },
          { ...baseSearchAfterBody, page_size: CONTACTS_PAGE_LIMIT },
          { ...baseSearchAfterBody, pageSize: CONTACTS_PAGE_LIMIT },
          { ...baseSearchAfterBody, location_id: locationId },
          { ...baseSearchAfterBody, location_id: locationId, pageLimit: CONTACTS_PAGE_LIMIT },
        ]
      : [
          basePageBody,
          { ...basePageBody, pageLimit: CONTACTS_PAGE_LIMIT },
          { ...basePageBody, page_size: CONTACTS_PAGE_LIMIT },
          { ...basePageBody, pageSize: CONTACTS_PAGE_LIMIT },
          { ...basePageBody, location_id: locationId },
          { ...basePageBody, location_id: locationId, pageLimit: CONTACTS_PAGE_LIMIT },
        ];

    let res: any = null;
    let lastErr: unknown = null;
    for (const body of variants) {
      try {
        res = (await with429Retry(() =>
          ghlFetchJson("/contacts/search", {
            method: "POST",
            body,
            authToken,
          }),
        )) as any;
        break;
      } catch (e) {
        lastErr = e;
      }
    }
    if (!res) throw lastErr;

    const contacts = Array.isArray(res?.contacts) ? res.contacts : [];
    console.info(
      `[appointments] contacts page loc=${locationId} page=${page} rows=${contacts.length} total=${all.length}`,
    );

    let oldestUpdatedOnPage = Number.POSITIVE_INFINITY;
    for (const c of contacts) {
      const id = norm(c?.id);
      if (!id) continue;
      const updatedMs = dateMsFromUnknown(c?.dateUpdated || c?.updatedAt || c?.dateAdded);
      if (Number.isFinite(updatedMs) && updatedMs < oldestUpdatedOnPage) oldestUpdatedOnPage = updatedMs;

      const opportunityIds = extractOpportunityIdsFromContact(c);
      all.push({
        id,
        name: norm(c?.contactName || c?.name || `${norm(c?.firstName)} ${norm(c?.lastName)}`.trim()),
        state: pickContactState(c),
        city: pickContactCity(c),
        updatedMs: Number.isFinite(updatedMs) ? updatedMs : 0,
        opportunityIds,
        opportunities: Array.isArray(c?.opportunities)
          ? c.opportunities
          : Array.isArray(c?.contact?.opportunities)
            ? c.contact.opportunities
            : [],
      });

      if (all.length >= opts.maxContacts) break;
    }

    if (all.length >= opts.maxContacts) break;

    if (opts.stopUpdatedBeforeMs && Number.isFinite(oldestUpdatedOnPage) && oldestUpdatedOnPage <= opts.stopUpdatedBeforeMs) {
      break;
    }

    const nextSearchAfter = Array.isArray(res?.searchAfter) ? res.searchAfter : null;
    if (nextSearchAfter && nextSearchAfter.length) {
      searchAfter = nextSearchAfter;
    } else if (contacts.length < CONTACTS_PAGE_LIMIT) {
      break;
    }

    if (!contacts.length) break;
    await sleep(CONTACTS_PAGE_DELAY_MS);
  }

  const dedupe = new Map<string, ContactLite>();
  for (const c of all) {
    const prev = dedupe.get(c.id);
    if (!prev || c.updatedMs >= prev.updatedMs) dedupe.set(c.id, c);
  }

  return Array.from(dedupe.values());
}

async function fetchAppointmentsForContacts(
  locationId: string,
  authToken: string,
  contacts: ContactLite[],
  deadlineAtMs?: number,
) {
  const out: ApptRow[] = [];

  for (let i = 0; i < contacts.length; i += APPT_CONTACT_CONCURRENCY) {
    if (isPastDeadline(deadlineAtMs)) {
      console.warn(`[appointments] appointments timeout loc=${locationId} processed=${i}/${contacts.length}`);
      break;
    }
    const batch = contacts.slice(i, i + APPT_CONTACT_CONCURRENCY);

    const batchRows = await Promise.all(
      batch.map(async (c) => {
        try {
          const res = await with429Retry(() =>
            ghlFetchJson(`/contacts/${encodeURIComponent(c.id)}/appointments`, {
              method: "GET",
              authToken,
            }),
          );

          const arr = extractArray(res, ["appointments", "events", "data"]);
          const rows: ApptRow[] = [];
          const contactOppState = inferStateFromContactOpportunities(c.opportunities || []);
          for (const a of arr) {
            const startAt = pickStartIso(a);
            const endAt = pickEndIso(a);
            const startMs = Number.isFinite(new Date(startAt).getTime()) ? new Date(startAt).getTime() : null;
            const apptState = normalizeStateName(norm(a?.state || a?.address?.state));
            const state = apptState || c.state || contactOppState || "";

            rows.push({
              id: norm(a?.id || a?.appointmentId || a?._id),
              locationId,
              contactId: c.id,
              contactName: c.name,
              title: norm(a?.title || a?.appointmentTitle || a?.name),
              status: norm(a?.appointmentStatus || a?.status || a?.currentStatus || "unknown"),
              statusNormalized: normalizeApptStatus(a?.appointmentStatus || a?.status || a?.currentStatus),
              calendarId: norm(a?.calendarId || a?.calendar?.id),
              startAt,
              endAt,
              __startMs: startMs,
              state,
              city: norm(a?.city || a?.address?.city || c.city),
              stateFrom: apptState ? "appointment" : c.state ? "contact.state" : "unknown",
            });
          }
          return rows;
        } catch {
          return [];
        }
      }),
    );

    for (const rows of batchRows) out.push(...rows);
    if (i % Math.max(APPT_CONTACT_CONCURRENCY * 10, 10) === 0) {
      console.info(`[appointments] appointments progress loc=${locationId} processed=${Math.min(i + batch.length, contacts.length)}/${contacts.length} rows=${out.length}`);
    }
    await sleep(APPT_CALL_DELAY_MS);
  }

  const dedupe = new Map<string, ApptRow>();
  for (const r of out) {
    const key = norm(r.id) || `${r.locationId}|${r.contactId}|${norm(r.startAt)}|${norm(r.title)}`;
    if (!key) continue;
    const prev = dedupe.get(key);
    if (!prev) {
      dedupe.set(key, r);
      continue;
    }
    const p = Number(prev.__startMs || 0);
    const n = Number(r.__startMs || 0);
    if (n >= p) dedupe.set(key, r);
  }

  return Array.from(dedupe.values());
}

async function fetchOpportunitiesForContact(authToken: string, locationId: string, contactId: string) {
  const variants: Array<{ method: "POST" | "GET"; path: string; body?: Record<string, unknown> }> = [
    {
      method: "POST",
      path: "/opportunities/search",
      body: { locationId, contactId, page: 1, limit: 50, pageLimit: 50 },
    },
    {
      method: "POST",
      path: "/opportunities/search",
      body: { location_id: locationId, contact_id: contactId, page: 1, limit: 50, pageLimit: 50 },
    },
    {
      method: "GET",
      path: `/opportunities/search?locationId=${encodeURIComponent(locationId)}&contactId=${encodeURIComponent(contactId)}&page=1&limit=50`,
    },
    {
      method: "GET",
      path: `/opportunities?locationId=${encodeURIComponent(locationId)}&contactId=${encodeURIComponent(contactId)}&page=1&limit=50`,
    },
  ];
  for (const v of variants) {
    try {
      const res = (await with429Retry(() =>
        ghlFetchJson(v.path, {
          method: v.method,
          authToken,
          body: v.body,
        }),
      )) as unknown;
      const rows = extractArray(res, ["opportunities", "data", "items"]);
      if (rows.length) return rows;
    } catch {
      // try next variant
    }
  }
  return [];
}

async function fetchOpportunityById(authToken: string, opportunityId: string) {
  const id = norm(opportunityId);
  if (!id) return null;
  const variants: Array<{ method: "GET"; path: string }> = [
    { method: "GET", path: `/opportunities/${encodeURIComponent(id)}` },
    { method: "GET", path: `/opportunities/${encodeURIComponent(id)}?id=${encodeURIComponent(id)}` },
  ];
  for (const v of variants) {
    try {
      const res = (await with429Retry(() =>
        ghlFetchJson(v.path, {
          method: v.method,
          authToken,
        }),
      )) as any;
      const opp = res?.opportunity || res?.data || res;
      if (opp && typeof opp === "object") return opp;
    } catch {
      // try next variant
    }
  }
  return null;
}

async function hydrateOpportunityDetailsByIds(authToken: string, opportunityIds: string[]) {
  const out = new Map<string, any>();
  const uniqueIds = Array.from(new Set(opportunityIds.map((x) => norm(x)).filter(Boolean)));
  const batchSize = Math.max(1, APPT_CONTACT_CONCURRENCY);
  for (let i = 0; i < uniqueIds.length; i += batchSize) {
    const batch = uniqueIds.slice(i, i + batchSize);
    const rows = await Promise.all(
      batch.map(async (id) => {
        const opp = await fetchOpportunityById(authToken, id);
        return { id, opp };
      }),
    );
    for (const r of rows) {
      if (r.opp) out.set(r.id, r.opp);
    }
    await sleep(APPT_CALL_DELAY_MS);
  }
  return out;
}

async function fetchPipelineCatalog(authToken: string, locationId: string) {
  const variants = [
    `/opportunities/pipelines?locationId=${encodeURIComponent(locationId)}`,
    `/opportunities/pipelines?location_id=${encodeURIComponent(locationId)}`,
    `/pipelines?locationId=${encodeURIComponent(locationId)}`,
    `/pipelines?location_id=${encodeURIComponent(locationId)}`,
    `/opportunities/pipeline?locationId=${encodeURIComponent(locationId)}`,
  ];
  for (const path of variants) {
    try {
      const res = (await with429Retry(() =>
        ghlFetchJson(path, {
          method: "GET",
          authToken,
        }),
      )) as unknown;
      const pipelines = extractArray(res, ["pipelines", "data", "items"]);
      if (pipelines.length) return pipelines;
    } catch {
      // keep trying variants
    }
  }
  return [];
}

async function discoverLostIdsFromPipelineCatalog(authToken: string, locationId: string): Promise<LostDiscovery> {
  const pipelines = await fetchPipelineCatalog(authToken, locationId);
  if (!pipelines.length) return { pipelineIds: [], stageIds: [] };

  const out: LostDiscovery = { pipelineIds: [], stageIds: [] };
  for (const p of pipelines) {
    const pName = norm((p as any)?.name || (p as any)?.pipelineName || (p as any)?.title);
    const pId = norm((p as any)?.id || (p as any)?.pipelineId || (p as any)?._id);
    const pipelineMatch =
      textIncludesAllWords(pName, LOST_BOOKINGS_PIPELINE) ||
      safeSearchToken(pName).includes(safeSearchToken(LOST_BOOKINGS_PIPELINE));
    if (!pipelineMatch) continue;
    if (pId) out.pipelineIds.push(pId);

    const stages = Array.isArray((p as any)?.stages)
      ? (p as any).stages
      : Array.isArray((p as any)?.pipelineStages)
        ? (p as any).pipelineStages
        : [];
    for (const s0 of stages) {
      const sName = norm((s0 as any)?.name || (s0 as any)?.stageName || (s0 as any)?.title);
      const sId = norm((s0 as any)?.id || (s0 as any)?.stageId || (s0 as any)?.pipelineStageId || (s0 as any)?._id);
      const stageMatch =
        textIncludesAllWords(sName, LOST_BOOKINGS_STAGE) ||
        safeSearchToken(sName).includes(safeSearchToken(LOST_BOOKINGS_STAGE));
      if (stageMatch && sId) out.stageIds.push(sId);
    }
  }
  return {
    pipelineIds: Array.from(new Set(out.pipelineIds)),
    stageIds: Array.from(new Set(out.stageIds)),
  };
}

async function fetchStageOpportunitiesByIds(
  authToken: string,
  locationId: string,
  discovered: LostDiscovery,
  startIso: string,
  endIso: string,
  debugErrors?: string[],
  deadlineAtMs?: number,
) {
  const pipelineIds = discovered.pipelineIds.filter(Boolean);
  const stageIds = discovered.stageIds.filter(Boolean);
  if (!pipelineIds.length && !stageIds.length) return [];

  const rows: any[] = [];
  const seenIds = new Set<string>();
  const startMs = toMs(startIso);
  const endMs = toMs(endIso);
  let searchAfter: any[] | null = null;

  for (let page = 1; page <= LOST_OPP_STAGE_MAX_PAGES; page++) {
    if (isPastDeadline(deadlineAtMs)) {
      if (debugErrors) debugErrors.push(`stage_search_timeout:loc=${locationId}:page=${page}`);
      break;
    }
    const variants: Array<{ method: "POST" | "GET"; path: string; body?: Record<string, unknown> }> = [
      {
        method: "POST",
        path: "/opportunities/search",
        body: {
          locationId,
          page,
          limit: LOST_OPP_STAGE_PAGE_LIMIT,
          pageLimit: LOST_OPP_STAGE_PAGE_LIMIT,
          pipelineIds,
          pipelineId: pipelineIds[0] || undefined,
          pipeline_id: pipelineIds[0] || undefined,
          pipelineStageIds: stageIds,
          pipelineStageId: stageIds[0] || undefined,
          pipeline_stage_id: stageIds[0] || undefined,
          pipeline_stage_ids: stageIds,
          dateAddedFrom: startIso,
          dateAddedTo: endIso,
          startDate: startIso,
          endDate: endIso,
          searchAfter: searchAfter && searchAfter.length ? searchAfter : undefined,
          sort: [{ field: "dateUpdated", direction: "desc" }],
        },
      },
      {
        method: "POST",
        path: "/opportunities/search",
        body: {
          location_id: locationId,
          page,
          limit: LOST_OPP_STAGE_PAGE_LIMIT,
          pageLimit: LOST_OPP_STAGE_PAGE_LIMIT,
          pipeline_id: pipelineIds[0] || undefined,
          pipeline_stage_id: stageIds[0] || undefined,
          startDate: startIso,
          endDate: endIso,
        },
      },
      {
        method: "GET",
        path:
          `/opportunities/search?locationId=${encodeURIComponent(locationId)}` +
          `&page=${page}&limit=${LOST_OPP_STAGE_PAGE_LIMIT}` +
          (pipelineIds[0] ? `&pipelineId=${encodeURIComponent(pipelineIds[0])}` : "") +
          (stageIds[0] ? `&pipelineStageId=${encodeURIComponent(stageIds[0])}` : "") +
          `&startDate=${encodeURIComponent(startIso)}&endDate=${encodeURIComponent(endIso)}`,
      },
      {
        method: "GET",
        path:
          `/opportunities/search?location_id=${encodeURIComponent(locationId)}` +
          `&page=${page}&limit=${LOST_OPP_STAGE_PAGE_LIMIT}` +
          (pipelineIds[0] ? `&pipeline_id=${encodeURIComponent(pipelineIds[0])}` : "") +
          (stageIds[0] ? `&pipeline_stage_id=${encodeURIComponent(stageIds[0])}` : "") +
          `&startDate=${encodeURIComponent(startIso)}&endDate=${encodeURIComponent(endIso)}`,
      },
      {
        method: "POST",
        path: "/opportunities/search",
        body: {
          locationId,
          page,
          limit: LOST_OPP_STAGE_PAGE_LIMIT,
          pageLimit: LOST_OPP_STAGE_PAGE_LIMIT,
          pipelineId: pipelineIds[0] || undefined,
          sort: [{ field: "dateUpdated", direction: "desc" }],
        },
      },
      {
        method: "GET",
        path:
          `/opportunities?locationId=${encodeURIComponent(locationId)}` +
          `&page=${page}&limit=${LOST_OPP_STAGE_PAGE_LIMIT}` +
          (pipelineIds[0] ? `&pipelineId=${encodeURIComponent(pipelineIds[0])}` : ""),
      },
      {
        method: "GET",
        path:
          `/opportunities?location_id=${encodeURIComponent(locationId)}` +
          `&page=${page}&limit=${LOST_OPP_STAGE_PAGE_LIMIT}` +
          (pipelineIds[0] ? `&pipeline_id=${encodeURIComponent(pipelineIds[0])}` : "") +
          (stageIds[0] ? `&pipeline_stage_id=${encodeURIComponent(stageIds[0])}` : ""),
      },
    ];

    let pageItems: any[] = [];
    let nextSearchAfter: any[] | null = null;
    let succeeded = false;
    for (const v of variants) {
      try {
        const res = (await with429Retry(() =>
          ghlFetchJson(v.path, {
            method: v.method,
            authToken,
            body: v.body,
          }),
        )) as unknown;
        pageItems = extractArray(res, ["opportunities", "data", "items"]) as any[];
        const obj = asObj(res);
        const sa = obj.searchAfter;
        const metaSa = asObj(obj.meta).searchAfter;
        nextSearchAfter = Array.isArray(sa) ? sa : Array.isArray(metaSa) ? metaSa : null;
        succeeded = true;
        break;
      } catch {
        if (debugErrors) debugErrors.push(`stage_search_fail:${v.method} ${v.path}`);
        // try next payload
      }
    }

    if (!succeeded || !pageItems.length) break;
    console.info(`[appointments] lost-stage page loc=${locationId} page=${page} rows=${pageItems.length}`);
    for (const it of pageItems) {
      const o = it as any;
      const oid = norm(o?.id || o?.opportunityId || o?._id);
      if (oid && seenIds.has(oid)) continue;
      if (oid) seenIds.add(oid);
      rows.push(o);
    }
    if (nextSearchAfter && nextSearchAfter.length) searchAfter = nextSearchAfter;
    else if (pageItems.length < LOST_OPP_STAGE_PAGE_LIMIT) break;
    await sleep(APPT_CALL_DELAY_MS);
  }

  const stageNameTarget = safeSearchToken(LOST_BOOKINGS_STAGE);
  const pipelineNameTarget = safeSearchToken(LOST_BOOKINGS_PIPELINE);

  const filtered = rows.filter((o: any) => {
    const pid = extractPipelineIdFromOpportunity(o);
    const sid = extractStageIdFromOpportunity(o);
    const pName = safeSearchToken(getOpportunityPipelineName(o));
    const sName = safeSearchToken(getOpportunityStageName(o));
    const inPipeline =
      (pipelineIds.length ? pipelineIds.includes(pid) : false) ||
      (!!pName && pName.includes(pipelineNameTarget));
    const inStage =
      (stageIds.length ? stageIds.includes(sid) : false) ||
      (!!sName && sName.includes(stageNameTarget));
    if (!inPipeline || !inStage) return false;

    const dMs = parseLostOpportunityEventMs(o, 0);
    if (!Number.isFinite(dMs)) return true;
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return true;
    return dMs >= startMs && dMs <= endMs;
  });

  return filtered;
}

async function fetchPipelineOpportunitiesWide(
  authToken: string,
  locationId: string,
  discovered: LostDiscovery,
  startIso: string,
  endIso: string,
  debugErrors?: string[],
  deadlineAtMs?: number,
) {
  const pipelineIds = discovered.pipelineIds.filter(Boolean);
  if (!pipelineIds.length) return [];
  const rows: any[] = [];
  const seen = new Set<string>();
  const startMs = toMs(startIso);
  const endMs = toMs(endIso);

  for (let page = 1; page <= LOST_OPP_STAGE_MAX_PAGES; page++) {
    if (isPastDeadline(deadlineAtMs)) {
      if (debugErrors) debugErrors.push(`pipeline_wide_timeout:loc=${locationId}:page=${page}`);
      break;
    }
    const variants: Array<{ method: "GET" | "POST"; path: string; body?: Record<string, unknown> }> = [
      {
        method: "POST",
        path: "/opportunities/search",
        body: {
          locationId,
          page,
          limit: LOST_OPP_STAGE_PAGE_LIMIT,
          pageLimit: LOST_OPP_STAGE_PAGE_LIMIT,
          pipelineId: pipelineIds[0],
          pipelineIds,
          startDate: startIso,
          endDate: endIso,
          sort: [{ field: "dateUpdated", direction: "desc" }],
        },
      },
      {
        method: "GET",
        path:
          `/opportunities/search?locationId=${encodeURIComponent(locationId)}` +
          `&page=${page}&limit=${LOST_OPP_STAGE_PAGE_LIMIT}` +
          `&pipelineId=${encodeURIComponent(pipelineIds[0])}` +
          `&startDate=${encodeURIComponent(startIso)}&endDate=${encodeURIComponent(endIso)}`,
      },
      {
        method: "GET",
        path:
          `/opportunities?locationId=${encodeURIComponent(locationId)}` +
          `&page=${page}&limit=${LOST_OPP_STAGE_PAGE_LIMIT}` +
          `&pipelineId=${encodeURIComponent(pipelineIds[0])}` +
          `&startDate=${encodeURIComponent(startIso)}&endDate=${encodeURIComponent(endIso)}`,
      },
    ];
    let pageItems: any[] = [];
    let ok = false;
    for (const v of variants) {
      try {
        const res = (await with429Retry(() =>
          ghlFetchJson(v.path, {
            method: v.method,
            authToken,
            body: v.body,
          }),
        )) as unknown;
        pageItems = extractArray(res, ["opportunities", "data", "items"]) as any[];
        ok = true;
        break;
      } catch {
        if (debugErrors) debugErrors.push(`pipeline_wide_fail:${v.method} ${v.path}`);
      }
    }
    if (!ok || !pageItems.length) break;
    console.info(`[appointments] lost-pipeline page loc=${locationId} page=${page} rows=${pageItems.length}`);
    for (const it of pageItems) {
      const id = norm((it as any)?.id || (it as any)?.opportunityId || (it as any)?._id);
      if (id && seen.has(id)) continue;
      if (id) seen.add(id);
      rows.push(it);
    }
    if (pageItems.length < LOST_OPP_STAGE_PAGE_LIMIT) break;
    await sleep(APPT_CALL_DELAY_MS);
  }

  return rows.filter((o) => {
    const ms = parseLostOpportunityEventMs(o, 0);
    if (!Number.isFinite(ms)) return true;
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return true;
    return ms >= startMs && ms <= endMs;
  });
}

async function hydrateContactsForOpportunities(authToken: string, locationId: string, contacts: ContactLite[]) {
  const out: ContactLite[] = [];
  const batchSize = Math.max(1, APPT_CONTACT_CONCURRENCY);
  for (let i = 0; i < contacts.length; i += batchSize) {
    const batch = contacts.slice(i, i + batchSize);
    const next = await Promise.all(
      batch.map(async (c) => {
        if ((c.opportunities || []).length) return c;
        try {
          const detail = (await with429Retry(() =>
            ghlFetchJson(`/contacts/${encodeURIComponent(c.id)}`, {
              method: "GET",
              authToken,
            }),
          )) as any;
          const src = (detail?.contact || detail || {}) as any;
          let opportunities = Array.isArray(src?.opportunities) ? src.opportunities : [];
          if (!opportunities.length) {
            opportunities = await fetchOpportunitiesForContact(authToken, locationId, c.id);
          }
          return {
            ...c,
            state: c.state || pickContactState(src),
            city: c.city || pickContactCity(src),
            opportunityIds: c.opportunityIds?.length ? c.opportunityIds : extractOpportunityIdsFromContact(src),
            opportunities,
          };
        } catch {
          return c;
        }
      }),
    );
    out.push(...next);
    await sleep(APPT_CALL_DELAY_MS);
  }
  return out;
}

async function refreshLocationRows(
  locationId: string,
  snapshot: LocationSnapshot | null,
  forceFull: boolean,
  startIso: string,
  endIso: string,
  deadlineAtMs?: number,
) {
  const authToken = await getLocationTokenFor(locationId);
  const incremental = !!snapshot && !forceFull;
  const stopBeforeMs = snapshot?.updatedAtMs
    ? Math.max(0, Number(snapshot.updatedAtMs) - SNAPSHOT_CONTACT_OVERLAP_MS)
    : 0;

  const contacts = await fetchContactsForLocation(locationId, authToken, {
    maxPages: incremental ? SNAPSHOT_MAX_CONTACT_PAGES : FULL_SYNC_MAX_CONTACT_PAGES,
    maxContacts: incremental ? SNAPSHOT_MAX_CONTACTS : FULL_SYNC_MAX_CONTACTS,
    stopUpdatedBeforeMs: incremental ? stopBeforeMs : 0,
    deadlineAtMs,
  });

  let contactsForLost = contacts;
  const contactsWithState = contactsForLost.filter((c) => !!norm(c.state)).length;
  if (!contactsWithState && contactsForLost.length) {
    contactsForLost = await hydrateContactsForOpportunities(authToken, locationId, contactsForLost);
  }
  contactsForLost = contactsForLost.map((c) => {
    const ids = new Set<string>(c.opportunityIds || []);
    for (const o of c.opportunities || []) {
      const id = extractOpportunityIdFromUnknown(o);
      if (id) ids.add(id);
    }
    return { ...c, opportunityIds: Array.from(ids) };
  });
  const allOpportunityIds = contactsForLost.flatMap((c) => c.opportunityIds || []);
  const opportunityDetails = await hydrateOpportunityDetailsByIds(authToken, allOpportunityIds);
  if (opportunityDetails.size) {
    contactsForLost = contactsForLost.map((c) => {
      const detailed = (c.opportunityIds || [])
        .map((id) => opportunityDetails.get(norm(id)))
        .filter(Boolean);
      const mergedOpps = [...detailed, ...(c.opportunities || [])];
      const seen = new Set<string>();
      const uniqueOpps = mergedOpps.filter((o) => {
        const oid = extractOpportunityIdFromUnknown(o);
        if (!oid) return true;
        if (seen.has(oid)) return false;
        seen.add(oid);
        return true;
      });
      return {
        ...c,
        opportunities: uniqueOpps,
      };
    });
  }
  const newRows = await fetchAppointmentsForContacts(locationId, authToken, contactsForLost, deadlineAtMs);
  let discovered = discoverLostPipelineStageIdsFromContacts(contactsForLost);
  if (!discovered.pipelineIds.length || !discovered.stageIds.length) {
    const fromCatalog = await discoverLostIdsFromPipelineCatalog(authToken, locationId);
    discovered = {
      pipelineIds: Array.from(new Set([...discovered.pipelineIds, ...fromCatalog.pipelineIds])),
      stageIds: Array.from(new Set([...discovered.stageIds, ...fromCatalog.stageIds])),
    };
  }
  const fromStageApi = await fetchStageOpportunitiesByIds(
    authToken,
    locationId,
    discovered,
    startIso,
    endIso,
    undefined,
    deadlineAtMs,
  );
  let stageApiRows = fromStageApi.length;
  let newLostRows =
    fromStageApi.length > 0
      ? deriveLostRowsFromContacts(
          locationId,
          [
            {
              id: "__stage_api__",
              name: "",
              state: "",
              city: "",
              updatedMs: Date.now(),
              opportunityIds: [],
              opportunities: fromStageApi,
            },
          ],
          snapshot?.lostRows || [],
          discovered,
        )
      : deriveLostRowsFromContacts(locationId, contactsForLost, snapshot?.lostRows || [], discovered);
  if (!newLostRows.length && contactsForLost.length) {
    contactsForLost = await hydrateContactsForOpportunities(authToken, locationId, contactsForLost);
    discovered = discoverLostPipelineStageIdsFromContacts(contactsForLost);
    if (!discovered.pipelineIds.length || !discovered.stageIds.length) {
      const fromCatalog = await discoverLostIdsFromPipelineCatalog(authToken, locationId);
      discovered = {
        pipelineIds: Array.from(new Set([...discovered.pipelineIds, ...fromCatalog.pipelineIds])),
        stageIds: Array.from(new Set([...discovered.stageIds, ...fromCatalog.stageIds])),
      };
    }
    const retryFromStage = await fetchStageOpportunitiesByIds(
      authToken,
      locationId,
      discovered,
      startIso,
      endIso,
      undefined,
      deadlineAtMs,
    );
    stageApiRows = Math.max(stageApiRows, retryFromStage.length);
    newLostRows =
      retryFromStage.length > 0
        ? deriveLostRowsFromContacts(
            locationId,
            [
              {
                id: "__stage_api_retry__",
                name: "",
                state: "",
                city: "",
                updatedMs: Date.now(),
                opportunityIds: [],
                opportunities: retryFromStage,
              },
            ],
            snapshot?.lostRows || [],
            discovered,
          )
        : deriveLostRowsFromContacts(locationId, contactsForLost, snapshot?.lostRows || [], discovered);
  }
  const merged = [...newRows, ...(snapshot?.rows || [])];

  const dedupe = new Map<string, ApptRow>();
  for (const r of merged) {
    const key = norm(r.id) || `${r.locationId}|${r.contactId}|${norm(r.startAt)}|${norm(r.title)}`;
    if (!key) continue;
    const prev = dedupe.get(key);
    if (!prev) {
      dedupe.set(key, r);
      continue;
    }
    const p = Number(prev.__startMs || 0);
    const n = Number(r.__startMs || 0);
    if (n >= p) dedupe.set(key, r);
  }

  const rows = Array.from(dedupe.values());
  const cov = rowsCoverage(rows);

  const next: LocationSnapshot = {
    version: 1,
    locationId,
    updatedAtMs: Date.now(),
    newestStartAt: cov.newestIso,
    oldestStartAt: cov.oldestIso,
    rows,
    lostRows: newLostRows,
    lostDiscovery: {
      pipelineIds: discovered.pipelineIds,
      stageIds: discovered.stageIds,
      discoveredAt: new Date().toISOString(),
      stageApiRows,
    },
  };

  await writeLocationSnapshot(next);
  return next;
}

function asMoney(v: unknown) {
  const n = Number(v);
  if (!Number.isFinite(n)) return 0;
  return n;
}

function extractOpportunityValue(o: any) {
  return asMoney(
    o?.monetaryValue ??
      o?.opportunityValue ??
      o?.value ??
      o?.amount ??
      o?.price ??
      o?.opportunity?.monetaryValue ??
      o?.opportunity?.value,
  );
}

function getOpportunitySource(o: any) {
  return norm(
    o?.source ??
      o?.opportunity?.source ??
      o?.contact?.source ??
      o?.meta?.source ??
      o?.leadSource ??
      o?.additionalInfo?.source,
  );
}

function firstNamedValue(obj: any, keys: string[]) {
  for (const k of keys) {
    const v = norm(obj?.[k]);
    if (v) return v;
  }
  return "";
}

function getOpportunityPipelineName(o: any) {
  const direct = firstNamedValue(o, ["pipelineName", "pipelineTitle", "pipeline_title", "pipeline_id_name"]);
  if (direct) return direct;
  const nestedPipeline = firstNamedValue(o?.pipeline, ["name", "pipelineName", "title"]);
  if (nestedPipeline) return nestedPipeline;
  const nestedOppPipeline = firstNamedValue(o?.opportunity?.pipeline, ["name", "pipelineName", "title"]);
  if (nestedOppPipeline) return nestedOppPipeline;
  const alt = firstNamedValue(o, ["name"]);
  return alt;
}

function getOpportunityStageName(o: any) {
  const direct = firstNamedValue(o, ["stageName", "pipelineStageName", "statusTitle", "stage_title"]);
  if (direct) return direct;
  const nestedStage = firstNamedValue(o?.stage, ["name", "stageName", "title"]);
  if (nestedStage) return nestedStage;
  const nestedOppStage = firstNamedValue(o?.opportunity?.stage, ["name", "stageName", "title"]);
  if (nestedOppStage) return nestedOppStage;
  const pipelineStage = firstNamedValue(o?.pipelineStage, ["name", "stageName", "title"]);
  if (pipelineStage) return pipelineStage;
  return "";
}

function parseLostOpportunityEventMs(o: any, contactUpdatedMs: number) {
  const ms = dateMsFromUnknown(
    o?.dateAdded ??
      o?.createdAt ??
      o?.created_at ??
      o?.dateUpdated ??
      o?.updatedAt ??
      o?.updated_at,
  );
  return Number.isFinite(ms) ? ms : contactUpdatedMs;
}

function sourceMatchIncludes(sourceToken: string, token: string) {
  if (!sourceToken || !token) return false;
  return sourceToken.includes(token);
}

function sourceAccountPrefix(source: string) {
  const raw = norm(source);
  if (!raw) return "";
  const prefix = raw.split("-")[0]?.trim() || raw;
  return safeSearchToken(prefix);
}

function matchCountyBySourceAccount(
  directory: LocationDirectoryCache,
  source: string,
): { county: string; state: string; city: string; accountName: string } | null {
  const prefixToken = sourceAccountPrefix(source);
  if (!prefixToken) return null;

  const exact = directory.countiesByAccountToken.get(prefixToken);
  if (exact) return exact;

  let best: { county: string; state: string; city: string; accountName: string } | null = null;
  let bestLen = 0;
  for (const [accountToken, item] of directory.countiesByAccountToken.entries()) {
    if (!accountToken) continue;
    if (prefixToken.includes(accountToken) || accountToken.includes(prefixToken)) {
      if (accountToken.length > bestLen) {
        best = item;
        bestLen = accountToken.length;
      }
    }
  }
  return best;
}

function resolveLostOrigin(
  locationId: string,
  source: string,
  _opportunityName: string,
  explicitState: string,
  explicitCity: string,
  directory: LocationDirectoryCache,
) {
  const sourceToken = safeSearchToken(source);
  const locMeta = directory.byLocationId.get(locationId);
  const matchedByName = matchCountyBySourceAccount(directory, source);

  let state = normalizeStateName(explicitState || matchedByName?.state || locMeta?.state || inferStateFromText(source));
  let city = norm(explicitCity || matchedByName?.city || locMeta?.city);
  let county = norm(matchedByName?.county || locMeta?.county);
  const accountName =
    norm(matchedByName?.accountName || locMeta?.accountName) ||
    norm(source.split("-")[0] || "");

  if (state && !city) {
    const cities = Array.from(directory.citiesByState.get(safeSearchToken(state)) || []);
    let bestCity = "";
    for (const c of cities) {
      if (!sourceMatchIncludes(sourceToken, c)) continue;
      if (c.length > bestCity.length) bestCity = c;
    }
    if (bestCity) city = bestCity;
  }

  if (state && !county) {
    const counties = Array.from(directory.countiesByState.get(safeSearchToken(state)) || []);
    let bestCounty = "";
    for (const c of counties) {
      if (!sourceMatchIncludes(sourceToken, c)) continue;
      if (c.length > bestCounty.length) bestCounty = c;
    }
    if (bestCounty) county = bestCounty;
  }

  if (!state) {
    state = normalizeStateName(inferStateFromText(sourceToken));
  }

  return {
    state: normalizeStateName(state),
    city: toDisplayPlaceName(city),
    county: toDisplayPlaceName(county),
    accountName,
  };
}

function isLostQualifiedOpportunityWithDiscovery(
  o: any,
  discovered: LostDiscovery,
) {
  const pipelineName = getOpportunityPipelineName(o);
  const stageName = getOpportunityStageName(o);
  const pipelineId = extractPipelineIdFromOpportunity(o);
  const stageId = extractStageIdFromOpportunity(o);
  const pipelineText = safeSearchToken([pipelineName, pipelineId].join(" "));
  const stageText = safeSearchToken([stageName, stageId, norm(o?.status), norm(o?.statusTitle)].join(" "));

  const byPipelineId =
    (!!LOST_BOOKINGS_PIPELINE_ID && pipelineId === LOST_BOOKINGS_PIPELINE_ID) ||
    (!!pipelineId && discovered.pipelineIds.includes(pipelineId));
  const byStageId =
    (!!LOST_BOOKINGS_STAGE_ID && stageId === LOST_BOOKINGS_STAGE_ID) ||
    (!!stageId && discovered.stageIds.includes(stageId));

  const fullText = safeSearchToken(
    [
      norm(o?.name),
      pipelineName,
      stageName,
      pipelineId,
      stageId,
      norm(o?.status),
      norm(o?.statusTitle),
      getOpportunitySource(o),
      (() => {
        try {
          return JSON.stringify(o);
        } catch {
          return "";
        }
      })(),
    ].join(" "),
  );

  const pipelineMatch =
    textIncludesAllWords(pipelineText || fullText, LOST_BOOKINGS_PIPELINE) ||
    fullText.includes(safeSearchToken(LOST_BOOKINGS_PIPELINE));
  const stageMatch =
    textIncludesAllWords(stageText || fullText, LOST_BOOKINGS_STAGE) ||
    fullText.includes(safeSearchToken(LOST_BOOKINGS_STAGE));
  const pipelineOk = byPipelineId || pipelineMatch;
  const stageOk = byStageId || stageMatch;
  if (!pipelineOk || !stageOk) return false;

  const statusToken = safeSearchToken(
    [
      norm(o?.status),
      norm(o?.opportunityStatus),
      norm(o?.stageStatus),
      norm(o?.statusTitle),
      norm(o?.pipelineStageName),
    ].join(" "),
  );
  const isOpen = /\bopen\b/.test(statusToken);
  const isClosedLike =
    /\babandon/.test(statusToken) ||
    /\bclose/.test(statusToken) ||
    /\blost\b/.test(statusToken) ||
    /\bwon\b/.test(statusToken) ||
    /\bcancel/.test(statusToken);
  return isOpen && !isClosedLike;
}

function discoverLostPipelineStageIdsFromContacts(contacts: ContactLite[]) {
  const pipelineIds = new Set<string>();
  const stageIds = new Set<string>();
  for (const c of contacts) {
    for (const o of c.opportunities || []) {
      const pipelineName = getOpportunityPipelineName(o);
      const stageName = getOpportunityStageName(o);
      const pipelineId = extractPipelineIdFromOpportunity(o);
      const stageId = extractStageIdFromOpportunity(o);

      const pipelineNameMatch =
        textIncludesAllWords(pipelineName, LOST_BOOKINGS_PIPELINE) ||
        safeSearchToken(pipelineName).includes(safeSearchToken(LOST_BOOKINGS_PIPELINE));
      const stageNameMatch =
        textIncludesAllWords(stageName, LOST_BOOKINGS_STAGE) ||
        safeSearchToken(stageName).includes(safeSearchToken(LOST_BOOKINGS_STAGE));

      if (pipelineNameMatch && pipelineId) pipelineIds.add(pipelineId);
      if (stageNameMatch && stageId) stageIds.add(stageId);
      if (pipelineNameMatch && stageNameMatch) {
        if (pipelineId) pipelineIds.add(pipelineId);
        if (stageId) stageIds.add(stageId);
      }
    }
  }
  if (LOST_BOOKINGS_PIPELINE_ID) pipelineIds.add(LOST_BOOKINGS_PIPELINE_ID);
  if (LOST_BOOKINGS_STAGE_ID) stageIds.add(LOST_BOOKINGS_STAGE_ID);
  return {
    pipelineIds: Array.from(pipelineIds),
    stageIds: Array.from(stageIds),
  } as LostDiscovery;
}

function deriveLostRowsFromContacts(
  locationId: string,
  contacts: ContactLite[],
  prevRows: LostBookingRow[],
  discovered: LostDiscovery,
) {
  const merged = [...prevRows];
  const directory = LOCATION_DIRECTORY_CACHE;
  for (const c of contacts) {
    for (const o of c.opportunities || []) {
      if (!isLostQualifiedOpportunityWithDiscovery(o, discovered)) continue;
      const source = getOpportunitySource(o);
      const opportunityName = norm(o?.name || o?.opportunity?.name || "");
      const explicitState = normalizeStateName(norm(o?.state || o?.contact?.state || c.state));
      const explicitCity = norm(o?.city || o?.contact?.city || c.city);
      const origin = directory
        ? resolveLostOrigin(locationId, source, opportunityName, explicitState, explicitCity, directory)
        : { state: explicitState, city: explicitCity, county: "", accountName: "" };
      const eventMs = parseLostOpportunityEventMs(o, c.updatedMs);
      const createdAt = Number.isFinite(dateMsFromUnknown(o?.dateAdded || o?.createdAt))
        ? new Date(dateMsFromUnknown(o?.dateAdded || o?.createdAt)).toISOString()
        : "";
      const updatedAt = Number.isFinite(dateMsFromUnknown(o?.dateUpdated || o?.updatedAt))
        ? new Date(dateMsFromUnknown(o?.dateUpdated || o?.updatedAt)).toISOString()
        : "";
      merged.push({
        id: norm(o?.id || o?.opportunityId || o?._id),
        locationId,
        contactId: c.id,
        contactName: c.name,
        pipelineId: extractPipelineIdFromOpportunity(o),
        pipelineName: getOpportunityPipelineName(o),
        stageId: extractStageIdFromOpportunity(o),
        stageName: getOpportunityStageName(o),
        source,
        state: origin.state || c.state,
        county: origin.county,
        city: origin.city || c.city,
        accountName: origin.accountName,
        value: extractOpportunityValue(o),
        currency: norm(o?.currency || "USD") || "USD",
        status: norm(o?.status || o?.opportunityStatus || o?.stageStatus || ""),
        createdAt,
        updatedAt,
        __eventMs: Number.isFinite(eventMs) ? eventMs : null,
      });
    }
  }

  const dedupe = new Map<string, LostBookingRow>();
  for (const r of merged) {
    const key = norm(r.id) || `${r.locationId}|${r.contactId}|${norm(r.createdAt)}|${norm(r.source)}`;
    if (!key) continue;
    const prev = dedupe.get(key);
    if (!prev || Number(r.__eventMs || 0) >= Number(prev.__eventMs || 0)) dedupe.set(key, r);
  }
  return Array.from(dedupe.values());
}

function deriveLostRowsFromOpportunityRows(
  locationId: string,
  opportunities: any[],
  prevRows: LostBookingRow[],
  discovered: LostDiscovery,
) {
  const merged = [...prevRows];
  const directory = LOCATION_DIRECTORY_CACHE;
  for (const o of opportunities || []) {
    if (!isLostQualifiedOpportunityWithDiscovery(o, discovered)) continue;
    const source = getOpportunitySource(o);
    const opportunityName = norm(o?.name || o?.opportunity?.name || "");
    const explicitState = normalizeStateName(norm(o?.state || o?.contact?.state || o?.address?.state));
    const explicitCity = norm(o?.city || o?.contact?.city || o?.address?.city);
    const origin = directory
      ? resolveLostOrigin(locationId, source, opportunityName, explicitState, explicitCity, directory)
      : { state: explicitState, city: explicitCity, county: "", accountName: "" };
    const eventMs = parseLostOpportunityEventMs(o, Date.now());
    const createdAt = Number.isFinite(dateMsFromUnknown(o?.dateAdded || o?.createdAt))
      ? new Date(dateMsFromUnknown(o?.dateAdded || o?.createdAt)).toISOString()
      : "";
    const updatedAt = Number.isFinite(dateMsFromUnknown(o?.dateUpdated || o?.updatedAt))
      ? new Date(dateMsFromUnknown(o?.dateUpdated || o?.updatedAt)).toISOString()
      : "";

    merged.push({
      id: norm(o?.id || o?.opportunityId || o?._id),
      locationId,
      contactId: norm(o?.contactId || o?.contact?.id),
      contactName: norm(o?.contactName || o?.name || `${norm(o?.contact?.firstName)} ${norm(o?.contact?.lastName)}`.trim()),
      pipelineId: extractPipelineIdFromOpportunity(o),
      pipelineName: getOpportunityPipelineName(o),
      stageId: extractStageIdFromOpportunity(o),
      stageName: getOpportunityStageName(o),
      source,
      state: origin.state,
      county: origin.county,
      city: origin.city,
      accountName: origin.accountName,
      value: extractOpportunityValue(o),
      currency: norm(o?.currency || "USD") || "USD",
      status: norm(o?.status || o?.opportunityStatus || o?.stageStatus || ""),
      createdAt,
      updatedAt,
      __eventMs: Number.isFinite(eventMs) ? eventMs : null,
    });
  }

  const dedupe = new Map<string, LostBookingRow>();
  for (const r of merged) {
    const key = norm(r.id) || `${r.locationId}|${r.contactId}|${norm(r.createdAt)}|${norm(r.source)}`;
    if (!key) continue;
    const prev = dedupe.get(key);
    if (!prev || Number(r.__eventMs || 0) >= Number(prev.__eventMs || 0)) dedupe.set(key, r);
  }
  return Array.from(dedupe.values());
}

async function refreshLostRowsStageOnly(
  locationId: string,
  startIso: string,
  endIso: string,
  prevRows: LostBookingRow[],
  deadlineAtMs?: number,
): Promise<LostStageRefreshResult> {
  const errors: string[] = [];
  const authToken = await getLocationTokenFor(locationId);
  let discovery = await discoverLostIdsFromPipelineCatalog(authToken, locationId);
  if (!discovery.pipelineIds.length && LOST_BOOKINGS_PIPELINE_ID) {
    discovery = {
      pipelineIds: [LOST_BOOKINGS_PIPELINE_ID],
      stageIds: discovery.stageIds,
    };
  }
  if (!discovery.stageIds.length && LOST_BOOKINGS_STAGE_ID) {
    discovery = {
      pipelineIds: discovery.pipelineIds,
      stageIds: [LOST_BOOKINGS_STAGE_ID],
    };
  }

  const rowsFromStageApi = await fetchStageOpportunitiesByIds(
    authToken,
    locationId,
    discovery,
    startIso,
    endIso,
    errors,
    deadlineAtMs,
  );
  if (rowsFromStageApi.length) {
    return {
      rows: deriveLostRowsFromOpportunityRows(locationId, rowsFromStageApi, prevRows, discovery),
      discovery,
      stageApiRows: rowsFromStageApi.length,
      errors,
    };
  }

  const rowsFromPipelineWide = await fetchPipelineOpportunitiesWide(
    authToken,
    locationId,
    discovery,
    startIso,
    endIso,
    errors,
    deadlineAtMs,
  );
  if (rowsFromPipelineWide.length) {
    return {
      rows: deriveLostRowsFromOpportunityRows(locationId, rowsFromPipelineWide, prevRows, discovery),
      discovery,
      stageApiRows: 0,
      errors,
    };
  }

  // Final fallback: discover from contacts -> opportunityIds -> opportunities/:id
  const contacts = await fetchContactsForLocation(locationId, authToken, {
    maxPages: FULL_SYNC_MAX_CONTACT_PAGES,
    maxContacts: FULL_SYNC_MAX_CONTACTS,
    stopUpdatedBeforeMs: 0,
    deadlineAtMs,
  });
  let contactsWithOpps = contacts;
  contactsWithOpps = contactsWithOpps.map((c) => {
    const ids = new Set<string>(c.opportunityIds || []);
    for (const o of c.opportunities || []) {
      const id = extractOpportunityIdFromUnknown(o);
      if (id) ids.add(id);
    }
    return { ...c, opportunityIds: Array.from(ids) };
  });
  const allOppIds = contactsWithOpps.flatMap((c) => c.opportunityIds || []);
  const details = await hydrateOpportunityDetailsByIds(authToken, allOppIds);
  if (details.size) {
    contactsWithOpps = contactsWithOpps.map((c) => {
      const detailed = (c.opportunityIds || []).map((id) => details.get(norm(id))).filter(Boolean);
      const merged = [...detailed, ...(c.opportunities || [])];
      const seen = new Set<string>();
      const unique = merged.filter((o) => {
        const oid = extractOpportunityIdFromUnknown(o);
        if (!oid) return true;
        if (seen.has(oid)) return false;
        seen.add(oid);
        return true;
      });
      return { ...c, opportunities: unique };
    });
  }

  // refresh discovery from hydrated opportunity JSON (by names + ids)
  const discoveryFromContacts = discoverLostPipelineStageIdsFromContacts(contactsWithOpps);
  discovery = {
    pipelineIds: Array.from(new Set([...discovery.pipelineIds, ...discoveryFromContacts.pipelineIds])),
    stageIds: Array.from(new Set([...discovery.stageIds, ...discoveryFromContacts.stageIds])),
  };

  const derivedRows = deriveLostRowsFromContacts(locationId, contactsWithOpps, prevRows, discovery);

  return {
    rows: derivedRows,
    discovery,
    stageApiRows: 0,
    errors,
  };
}

export async function GET(req: Request) {
  const url = new URL(req.url);
  const start = url.searchParams.get("start") || "";
  const end = url.searchParams.get("end") || "";
  const bust = url.searchParams.get("bust") === "1";
  const debug = url.searchParams.get("debug") === "1";
  const reqStartedAt = Date.now();
  const deadlineAtMs = reqStartedAt + REQUEST_MAX_MS;

  try {
    if (!start || !end) {
      return NextResponse.json({ ok: false, error: "Missing start/end" } satisfies ApiResponse, { status: 400 });
    }

    const startMs = toMs(start);
    const endMs = toMs(end);
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
      return NextResponse.json({ ok: false, error: "Invalid start/end range" } satisfies ApiResponse, { status: 400 });
    }

    if (!bust) {
      const cached = getRangeCache(start, end);
      if (cached) {
        return NextResponse.json({
          ...cached,
          cache: {
            ...(cached.cache || {}),
            source: "memory",
          },
        });
      }
    }

    const locationIds = (await getLocationDirectory()).ids;
    console.info(
      `[appointments] request start bust=${bust ? "1" : "0"} debug=${debug ? "1" : "0"} locs=${locationIds.length} maxMs=${REQUEST_MAX_MS}`,
    );
    if (!locationIds.length) {
      return NextResponse.json({ ok: true, range: { start, end }, total: 0, rows: [] } satisfies ApiResponse);
    }

    let refreshedLocations = 0;
    let usedIncremental = false;
    let refreshReason = "";

    const byLocationRows = new Map<string, ApptRow[]>();
    const byLocationLostRows = new Map<string, LostBookingRow[]>();
    const snapshotMeta = new Map<string, { updatedAt: string; newest: string; oldest: string }>();
    const discoveredPipelineIds = new Set<string>();
    const discoveredStageIds = new Set<string>();
    let discoveredStageApiRows = 0;
    const refreshErrors: string[] = [];

    for (const locationId of locationIds) {
      if (isPastDeadline(deadlineAtMs)) {
        refreshReason = refreshReason || "request_timeout_using_partial_data";
        refreshErrors.push(`request_timeout_before_location:${locationId}`);
        const snapshot = await readLocationSnapshot(locationId);
        if (snapshot?.rows?.length) {
          byLocationRows.set(locationId, snapshot.rows);
          byLocationLostRows.set(locationId, snapshot.lostRows || []);
        } else {
          byLocationRows.set(locationId, []);
          byLocationLostRows.set(locationId, []);
        }
        continue;
      }
      const snapshot = await readLocationSnapshot(locationId);
      const fresh =
        !!snapshot &&
        Array.isArray(snapshot.lostRows) &&
        Date.now() - Number(snapshot.updatedAtMs || 0) <= SNAPSHOT_TTL_MS;

      if (fresh && snapshot && !bust) {
        byLocationRows.set(locationId, snapshot.rows || []);
        byLocationLostRows.set(locationId, snapshot.lostRows || []);
        for (const id of snapshot.lostDiscovery?.pipelineIds || []) if (id) discoveredPipelineIds.add(id);
        for (const id of snapshot.lostDiscovery?.stageIds || []) if (id) discoveredStageIds.add(id);
        discoveredStageApiRows += Number(snapshot.lostDiscovery?.stageApiRows || 0);
        snapshotMeta.set(locationId, {
          updatedAt: new Date(snapshot.updatedAtMs).toISOString(),
          newest: snapshot.newestStartAt || "",
          oldest: snapshot.oldestStartAt || "",
        });
        continue;
      }

      if (refreshedLocations >= LOCATION_REFRESH_BUDGET && snapshot?.rows?.length) {
        byLocationRows.set(locationId, snapshot.rows);
        byLocationLostRows.set(locationId, snapshot.lostRows || []);
        for (const id of snapshot.lostDiscovery?.pipelineIds || []) if (id) discoveredPipelineIds.add(id);
        for (const id of snapshot.lostDiscovery?.stageIds || []) if (id) discoveredStageIds.add(id);
        discoveredStageApiRows += Number(snapshot.lostDiscovery?.stageApiRows || 0);
        snapshotMeta.set(locationId, {
          updatedAt: new Date(snapshot.updatedAtMs).toISOString(),
          newest: snapshot.newestStartAt || "",
          oldest: snapshot.oldestStartAt || "",
        });
        refreshReason = "refresh_budget_exceeded_using_stale_snapshots";
        continue;
      }

      try {
        const next = await refreshLocationRows(locationId, snapshot, bust, start, end, deadlineAtMs);
        refreshedLocations++;
        if (snapshot) usedIncremental = true;
        byLocationRows.set(locationId, next.rows || []);
        byLocationLostRows.set(locationId, next.lostRows || []);
        for (const id of next.lostDiscovery?.pipelineIds || []) if (id) discoveredPipelineIds.add(id);
        for (const id of next.lostDiscovery?.stageIds || []) if (id) discoveredStageIds.add(id);
        discoveredStageApiRows += Number(next.lostDiscovery?.stageApiRows || 0);
        snapshotMeta.set(locationId, {
          updatedAt: new Date(next.updatedAtMs).toISOString(),
          newest: next.newestStartAt || "",
          oldest: next.oldestStartAt || "",
        });
      } catch (e: any) {
        refreshErrors.push(`refresh_location_failed:${locationId}:${String(e?.message || e || "unknown_error")}`);
        if (snapshot?.rows?.length) {
          byLocationRows.set(locationId, snapshot.rows);
          let fallbackLostRows = snapshot.lostRows || [];
          try {
            const stageOnly = await refreshLostRowsStageOnly(locationId, start, end, fallbackLostRows, deadlineAtMs);
            fallbackLostRows = stageOnly.rows;
            for (const id of stageOnly.discovery.pipelineIds) if (id) discoveredPipelineIds.add(id);
            for (const id of stageOnly.discovery.stageIds) if (id) discoveredStageIds.add(id);
            discoveredStageApiRows += Number(stageOnly.stageApiRows || 0);
            if (Array.isArray(stageOnly.errors) && stageOnly.errors.length) {
              refreshErrors.push(...stageOnly.errors.map((x) => `${locationId}:${x}`));
            }
          } catch {
            // keep stale snapshot lost rows
          }
          byLocationLostRows.set(locationId, fallbackLostRows);
          for (const id of snapshot.lostDiscovery?.pipelineIds || []) if (id) discoveredPipelineIds.add(id);
          for (const id of snapshot.lostDiscovery?.stageIds || []) if (id) discoveredStageIds.add(id);
          discoveredStageApiRows += Number(snapshot.lostDiscovery?.stageApiRows || 0);
          snapshotMeta.set(locationId, {
            updatedAt: new Date(snapshot.updatedAtMs).toISOString(),
            newest: snapshot.newestStartAt || "",
            oldest: snapshot.oldestStartAt || "",
          });
          refreshReason = "api_error_using_stale_snapshot";
          continue;
        }
        byLocationRows.set(locationId, []);
        try {
          const stageOnly = await refreshLostRowsStageOnly(locationId, start, end, [], deadlineAtMs);
          byLocationLostRows.set(locationId, stageOnly.rows || []);
          for (const id of stageOnly.discovery.pipelineIds) if (id) discoveredPipelineIds.add(id);
          for (const id of stageOnly.discovery.stageIds) if (id) discoveredStageIds.add(id);
          discoveredStageApiRows += Number(stageOnly.stageApiRows || 0);
          if (Array.isArray(stageOnly.errors) && stageOnly.errors.length) {
            refreshErrors.push(...stageOnly.errors.map((x) => `${locationId}:${x}`));
          }
        } catch (e2: any) {
          refreshErrors.push(`lost_only_fallback_failed:${locationId}:${String(e2?.message || e2 || "unknown_error")}`);
          byLocationLostRows.set(locationId, []);
        }
      }
    }

    const allRows = Array.from(byLocationRows.values()).flat();
    const dedupe = new Map<string, ApptRow>();
    for (const r of allRows) {
      const key = norm(r.id) || `${r.locationId}|${r.contactId}|${norm(r.startAt)}|${norm(r.title)}`;
      if (!key) continue;
      const prev = dedupe.get(key);
      if (!prev) {
        dedupe.set(key, r);
        continue;
      }
      const p = Number(prev.__startMs || 0);
      const n = Number(r.__startMs || 0);
      if (n >= p) dedupe.set(key, r);
    }

    const deduped = Array.from(dedupe.values());
    const rows = deduped.filter((r) => {
      const ms = Number(r.__startMs ?? NaN);
      if (!Number.isFinite(ms)) return true;
      return ms >= startMs && ms <= endMs;
    });

    const allLostRows = Array.from(byLocationLostRows.values()).flat();
    const dedupedLostMap = new Map<string, LostBookingRow>();
    for (const r of allLostRows) {
      const key = norm(r.id) || `${r.locationId}|${r.contactId}|${norm(r.createdAt)}|${norm(r.source)}`;
      if (!key) continue;
      const prev = dedupedLostMap.get(key);
      if (!prev || Number(r.__eventMs || 0) >= Number(prev.__eventMs || 0)) dedupedLostMap.set(key, r);
    }
    const dedupedLost = Array.from(dedupedLostMap.values());
    const lostRowsInRange = dedupedLost.filter((r) => {
      const ms = Number(r.__eventMs ?? NaN);
      if (!Number.isFinite(ms)) return true;
      return ms >= startMs && ms <= endMs;
    });

    const byState: Record<string, number> = {};
    const byStatus: Record<string, number> = {};
    const byLocation: Record<string, number> = {};
    const contactSet = new Set<string>();

    let scheduled = 0;
    let confirmed = 0;
    let completed = 0;
    let cancelled = 0;
    let noShow = 0;
    let rescheduled = 0;
    let withState = 0;

    for (const r of rows) {
      if (r.contactId) contactSet.add(r.contactId);
      byLocation[r.locationId] = (byLocation[r.locationId] || 0) + 1;

      const st = normalizeStateName(r.state);
      if (st) {
        r.state = st;
        byState[st] = (byState[st] || 0) + 1;
        withState++;
      } else {
        byState.__unknown = (byState.__unknown || 0) + 1;
      }

      const sn = r.statusNormalized || "unknown";
      byStatus[sn] = (byStatus[sn] || 0) + 1;
      if (sn === "scheduled") scheduled++;
      else if (sn === "confirmed") confirmed++;
      else if (sn === "completed") completed++;
      else if (sn === "cancelled") cancelled++;
      else if (sn === "no_show") noShow++;
      else if (sn === "rescheduled") rescheduled++;
    }

    const total = rows.length;
    const showDen = completed + noShow;

    const kpis = {
      total,
      uniqueContacts: contactSet.size,
      scheduled,
      confirmed,
      completed,
      cancelled,
      noShow,
      rescheduled,
      showRate: showDen ? pct(completed, showDen) : 0,
      cancellationRate: total ? pct(cancelled, total) : 0,
      noShowRate: showDen ? pct(noShow, showDen) : 0,
      withState,
      stateRate: total ? pct(withState, total) : 0,
    };

    const lostByState: Record<string, number> = {};
    const lostByCounty: Record<string, number> = {};
    const lostByCity: Record<string, number> = {};
    const lostContacts = new Set<string>();
    let lostWithState = 0;
    let lostValueTotal = 0;
    for (const r of lostRowsInRange) {
      if (r.contactId) lostContacts.add(r.contactId);
      lostValueTotal += asMoney(r.value);
      const state = normalizeStateName(r.state);
      if (state) {
        r.state = state;
        lostByState[state] = (lostByState[state] || 0) + 1;
        lostWithState++;
      } else {
        lostByState.__unknown = (lostByState.__unknown || 0) + 1;
      }
      const county = norm(r.county);
      if (county) lostByCounty[county] = (lostByCounty[county] || 0) + 1;
      else lostByCounty.__unknown = (lostByCounty.__unknown || 0) + 1;
      const city = norm(r.city);
      if (city) lostByCity[city] = (lostByCity[city] || 0) + 1;
      else lostByCity.__unknown = (lostByCity.__unknown || 0) + 1;
    }

    const lostBookings: LostBookingsBlock = {
      total: lostRowsInRange.length,
      uniqueContacts: lostContacts.size,
      valueTotal: lostValueTotal,
      opportunityValueTotal: lostValueTotal,
      stageValueTotal: lostValueTotal,
      withState: lostWithState,
      stateRate: lostRowsInRange.length ? pct(lostWithState, lostRowsInRange.length) : 0,
      byState: lostByState,
      byCounty: lostByCounty,
      byCity: lostByCity,
      rows: lostRowsInRange,
    };

    const allCoverage = rowsCoverage(deduped);
    const latestSnapshotUpdatedAt = Array.from(snapshotMeta.values())
      .map((x) => toMs(x.updatedAt))
      .filter((x) => Number.isFinite(x))
      .sort((a, b) => b - a)[0];

    const resp: ApiResponse = {
      ok: true,
      range: { start, end },
      total,
      kpis,
      byState,
      byStatus,
      byLocation,
      rows,
      lostBookings,
      cache: {
        source: refreshedLocations > 0 ? "ghl_refresh" : "snapshot",
        snapshotUpdatedAt: Number.isFinite(latestSnapshotUpdatedAt)
          ? new Date(latestSnapshotUpdatedAt).toISOString()
          : undefined,
        snapshotCoverage:
          allCoverage.newestIso || allCoverage.oldestIso
            ? {
                newestStartAt: allCoverage.newestIso,
                oldestStartAt: allCoverage.oldestIso,
              }
            : undefined,
        refreshedLocations,
        totalLocations: locationIds.length,
        usedIncremental: usedIncremental || undefined,
        refreshReason: refreshReason || undefined,
      },
      ...(debug
        ? {
            debug: {
              locationIds,
              totalSnapshots: snapshotMeta.size,
              dedupedRows: deduped.length,
              rowsInRange: rows.length,
              lostRowsDeduped: dedupedLost.length,
              lostRowsInRange: lostRowsInRange.length,
              lostRowsBeforeDateFilter: allLostRows.length,
              lostPipeline: LOST_BOOKINGS_PIPELINE,
              lostStage: LOST_BOOKINGS_STAGE,
              lostPipelineId: LOST_BOOKINGS_PIPELINE_ID || null,
              lostStageId: LOST_BOOKINGS_STAGE_ID || null,
              discoveredPipelineIds: Array.from(discoveredPipelineIds),
              discoveredStageIds: Array.from(discoveredStageIds),
              discoveredStageApiRows,
              refreshErrors,
              sampleRow: rows[0] || null,
              sampleLostRow: lostRowsInRange[0] || null,
            },
          }
        : {}),
    };

    setRangeCache(start, end, resp);
    console.info(
      `[appointments] request done inMs=${Date.now() - reqStartedAt} total=${resp.total || 0} lost=${resp.lostBookings?.total || 0} refreshReason=${resp.cache?.refreshReason || "none"}`,
    );
    return NextResponse.json(resp);
  } catch (e: any) {
    return NextResponse.json(
      { ok: false, error: e?.message || "Failed to load appointments dashboard." } satisfies ApiResponse,
      { status: 500 },
    );
  }
}
