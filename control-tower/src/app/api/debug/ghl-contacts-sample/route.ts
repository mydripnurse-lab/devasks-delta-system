import { NextResponse } from "next/server";
import { getEffectiveLocationIdOrThrow, ghlFetchJson } from "@/lib/ghlHttp";

export const runtime = "nodejs";

function s(v: unknown) {
  return String(v ?? "").trim();
}

function n(v: unknown, fallback: number) {
  const x = Number(v);
  return Number.isFinite(x) ? x : fallback;
}

function asObj(v: unknown): Record<string, unknown> {
  return v && typeof v === "object" ? (v as Record<string, unknown>) : {};
}

function extractArray(res: unknown, keys: string[]) {
  const root = asObj(res);
  for (const k of keys) {
    const top = root[k];
    if (Array.isArray(top)) return top;
    const data = asObj(root.data);
    const nested = data[k];
    if (Array.isArray(nested)) return nested;
  }
  if (Array.isArray(root.items)) return root.items;
  const data = asObj(root.data);
  if (Array.isArray(data.items)) return data.items;
  if (Array.isArray(root.data)) return root.data as unknown[];
  if (Array.isArray(res)) return res;
  return [];
}

function extractOpportunityIdsFromContact(contact: Record<string, unknown>) {
  const ids = new Set<string>();
  const candidates: unknown[] = [];

  const opportunities = contact.opportunities;
  if (Array.isArray(opportunities)) candidates.push(...opportunities);

  const contactObj = asObj(contact.contact);
  const nestedOpportunities = contactObj.opportunities;
  if (Array.isArray(nestedOpportunities)) candidates.push(...nestedOpportunities);

  const opportunityIds = contact.opportunityIds;
  if (Array.isArray(opportunityIds)) candidates.push(...opportunityIds);

  const nestedOpportunityIds = contactObj.opportunityIds;
  if (Array.isArray(nestedOpportunityIds)) candidates.push(...nestedOpportunityIds);

  if (contact.opportunityId) candidates.push(contact.opportunityId);
  if (contactObj.opportunityId) candidates.push(contactObj.opportunityId);

  for (const c of candidates) {
    if (typeof c === "string" || typeof c === "number") {
      const id = s(c);
      if (id) ids.add(id);
      continue;
    }
    const obj = asObj(c);
    const id = s(obj.id || obj.opportunityId || obj._id);
    if (id) ids.add(id);
  }

  return Array.from(ids);
}

async function fetchOpportunityById(opportunityId: string) {
  const id = s(opportunityId);
  if (!id) return null;

  const variants = [
    `/opportunities/${encodeURIComponent(id)}`,
    `/opportunities/${encodeURIComponent(id)}?id=${encodeURIComponent(id)}`,
  ];

  let lastErr: string | null = null;
  for (const path of variants) {
    try {
      const res = await ghlFetchJson(path, { method: "GET" });
      const obj = asObj(res);
      const opp = asObj(obj.opportunity || obj.data || obj);
      if (Object.keys(opp).length) return { ok: true, path, opportunity: opp };
    } catch (e: unknown) {
      lastErr = e instanceof Error ? e.message : "unknown error";
    }
  }

  return { ok: false, error: lastErr || "not found" };
}

async function fetchContactsSample(locationId: string, limit: number) {
  const attempts: Array<{
    method: "POST" | "GET";
    path: string;
    body?: Record<string, unknown>;
    label: string;
  }> = [
    {
      method: "POST",
      path: "/contacts/search",
      label: "search_post_full",
      body: {
        locationId,
        page: 1,
        limit,
        pageLimit: limit,
        sort: [{ field: "dateUpdated", direction: "desc" }],
      },
    },
    {
      method: "POST",
      path: "/contacts/search",
      label: "search_post_pageLimit_only",
      body: {
        locationId,
        page: 1,
        pageLimit: limit,
      },
    },
    {
      method: "POST",
      path: "/contacts/search",
      label: "search_post_minimal",
      body: {
        locationId,
      },
    },
    {
      method: "GET",
      path: `/contacts/?locationId=${encodeURIComponent(locationId)}&limit=${limit}&page=1`,
      label: "contacts_get",
    },
  ];

  const debugAttempts: Array<Record<string, unknown>> = [];
  for (const a of attempts) {
    try {
      const res = await ghlFetchJson(a.path, {
        method: a.method,
        body: a.body,
      });
      const contacts = extractArray(res, ["contacts", "data", "items"]).map((c) => asObj(c)).slice(0, limit);
      debugAttempts.push({
        label: a.label,
        ok: true,
        path: a.path,
        method: a.method,
        body: a.body || null,
        returned: contacts.length,
      });
      if (contacts.length) return { ok: true, contacts, debugAttempts, winner: a.label };
    } catch (e: unknown) {
      debugAttempts.push({
        label: a.label,
        ok: false,
        path: a.path,
        method: a.method,
        body: a.body || null,
        error: e instanceof Error ? e.message : String(e),
      });
    }
  }

  return { ok: false, contacts: [], debugAttempts, winner: null };
}

export async function GET(req: Request) {
  const url = new URL(req.url);
  const limit = Math.max(1, Math.min(25, n(url.searchParams.get("limit"), 5)));
  const includeOppDetails = s(url.searchParams.get("includeOppDetails")) === "1";

  try {
    const locationId = await getEffectiveLocationIdOrThrow();
    const fetched = await fetchContactsSample(locationId, limit);
    const contacts = fetched.contacts;
    if (!fetched.ok) {
      return NextResponse.json(
        {
          ok: false,
          locationId,
          requestedLimit: limit,
          error: "Unable to fetch contacts sample with current payload variants",
          attempts: fetched.debugAttempts,
        },
        { status: 422 },
      );
    }

    const sample = contacts.map((c) => {
      const oppIds = extractOpportunityIdsFromContact(c);
      return {
        id: s(c.id),
        contactName: s(c.contactName || c.name),
        firstName: s(c.firstName),
        lastName: s(c.lastName),
        state: s(c.state),
        city: s(c.city),
        address: c.address || null,
        dateAdded: c.dateAdded || null,
        dateUpdated: c.dateUpdated || null,
        opportunityIds: oppIds,
        opportunitiesInline: Array.isArray(c.opportunities) ? c.opportunities.slice(0, 5) : [],
      };
    });

    let opportunityDetails: Array<Record<string, unknown>> = [];
    if (includeOppDetails) {
      const ids = Array.from(
        new Set(
          sample
            .flatMap((c) => (Array.isArray(c.opportunityIds) ? c.opportunityIds : []))
            .map((x) => s(x))
            .filter(Boolean),
        ),
      ).slice(0, 30);

      const rows = await Promise.all(ids.map((id) => fetchOpportunityById(id)));
      opportunityDetails = rows.map((r, idx) => ({
        opportunityId: ids[idx],
        ...(r || { ok: false, error: "unknown" }),
      }));
    }

    return NextResponse.json({
      ok: true,
      locationId,
      requestedLimit: limit,
      returnedContacts: sample.length,
      contactsFetchVariant: fetched.winner,
      attempts: fetched.debugAttempts,
      contacts: sample,
      opportunityDetails,
      hint:
        "Use includeOppDetails=1 to inspect opportunity JSON by opportunityId and verify pipeline/stage names/ids.",
    });
  } catch (e: unknown) {
    return NextResponse.json(
      {
        ok: false,
        error: e instanceof Error ? e.message : "Failed to fetch contacts sample",
      },
      { status: 500 },
    );
  }
}
