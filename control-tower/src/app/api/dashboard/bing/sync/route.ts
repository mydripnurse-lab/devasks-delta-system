import fs from "fs/promises";
import path from "path";

export const runtime = "nodejs";

type RangePreset =
  | "last_7_days"
  | "last_28_days"
  | "last_month"
  | "last_quarter"
  | "last_6_months"
  | "last_year"
  | "custom";

type BingApiRow = Record<string, unknown>;

function s(v: unknown) {
  return String(v ?? "").trim();
}

function uniqueStrings(values: string[]) {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const v of values) {
    const x = s(v);
    if (!x) continue;
    if (seen.has(x)) continue;
    seen.add(x);
    out.push(x);
  }
  return out;
}

function parseSiteUrls(singleSite: string, multiRaw: string) {
  const fromMulti = multiRaw
    .split(/[\n,;]+/)
    .map((x) => s(x))
    .filter(Boolean);
  return uniqueStrings([...fromMulti, s(singleSite)]);
}

function toNum(v: unknown) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function isoDateOnly(d: Date) {
  return d.toISOString().slice(0, 10);
}

function toUsDate(iso: string) {
  const d = new Date(`${iso}T00:00:00Z`);
  if (!Number.isFinite(d.getTime())) return iso;
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const yyyy = String(d.getUTCFullYear());
  return `${mm}/${dd}/${yyyy}`;
}

function rangeFromPreset(preset: RangePreset, startRaw: string, endRaw: string) {
  const now = new Date();
  const end = new Date(now);
  const start = new Date(now);

  const explicitStart = s(startRaw);
  const explicitEnd = s(endRaw);
  if (explicitStart && explicitEnd) {
    return { startDate: explicitStart, endDate: explicitEnd };
  }

  if (preset === "custom") {
    const cs = new Date(startRaw || now);
    const ce = new Date(endRaw || now);
    return {
      startDate: isoDateOnly(new Date(cs.getFullYear(), cs.getMonth(), cs.getDate())),
      endDate: isoDateOnly(new Date(ce.getFullYear(), ce.getMonth(), ce.getDate())),
    };
  }

  if (preset === "last_7_days") start.setDate(start.getDate() - 6);
  else if (preset === "last_28_days") start.setDate(start.getDate() - 27);
  else if (preset === "last_month") start.setMonth(start.getMonth() - 1);
  else if (preset === "last_quarter") start.setMonth(start.getMonth() - 3);
  else if (preset === "last_6_months") start.setMonth(start.getMonth() - 6);
  else if (preset === "last_year") {
    const prevYear = now.getFullYear() - 1;
    return {
      startDate: `${prevYear}-01-01`,
      endDate: `${prevYear}-12-31`,
    };
  }

  return {
    startDate: isoDateOnly(new Date(start.getFullYear(), start.getMonth(), start.getDate())),
    endDate: isoDateOnly(new Date(end.getFullYear(), end.getMonth(), end.getDate())),
  };
}

function parseDateFromRow(row: BingApiRow) {
  const keys = ["Date", "date", "Day", "day", "Timestamp", "timestamp"];
  for (const k of keys) {
    const raw = s(row[k]);
    if (!raw) continue;
    const m = raw.match(/\/Date\((\d+)(?:[+-]\d+)?\)\//);
    if (m) {
      const ms = Number(m[1]);
      if (Number.isFinite(ms)) return isoDateOnly(new Date(ms));
    }
    const d = new Date(raw);
    if (Number.isFinite(d.getTime())) return isoDateOnly(d);
  }
  return "";
}

function pickQuery(row: BingApiRow) {
  return s(row.Query ?? row.query ?? row.Keyword ?? row.keyword ?? row.SearchQuery ?? row.searchQuery);
}

function pickPage(row: BingApiRow) {
  return s(
    row.Page ??
      row.page ??
      row.Url ??
      row.url ??
      row.LandingPage ??
      row.landingPage ??
      row.PageUrl ??
      row.pageUrl ??
      row.Query ??
      row.query,
  );
}

function pickImpressions(row: BingApiRow) {
  return toNum(row.Impressions ?? row.impressions ?? row.Views ?? row.views);
}

function pickClicks(row: BingApiRow) {
  return toNum(row.Clicks ?? row.clicks);
}

function pickCtr(row: BingApiRow) {
  const direct = toNum(row.Ctr ?? row.ctr ?? row.CTR);
  if (direct > 0) return direct;
  const i = pickImpressions(row);
  const c = pickClicks(row);
  return i > 0 ? c / i : 0;
}

function pickPosition(row: BingApiRow) {
  return toNum(
    row.Position ??
      row.position ??
      row.AveragePosition ??
      row.averagePosition ??
      row.AvgPosition ??
      row.avgPosition ??
      row.AvgImpressionPosition ??
      row.avgImpressionPosition ??
      row.AvgClickPosition ??
      row.avgClickPosition,
  );
}

function isLikelyUrl(v: string) {
  const x = s(v).toLowerCase();
  if (!x) return false;
  return x.startsWith("http://") || x.startsWith("https://") || x.includes(".mydripnurse.");
}

function deepCollectRows(node: unknown, out: BingApiRow[]) {
  if (!node) return;
  if (Array.isArray(node)) {
    for (const x of node) deepCollectRows(x, out);
    return;
  }
  if (typeof node !== "object") return;

  const rec = node as Record<string, unknown>;
  const hasMetrics =
    rec.Impressions !== undefined ||
    rec.impressions !== undefined ||
    rec.Clicks !== undefined ||
    rec.clicks !== undefined;

  if (hasMetrics) out.push(rec);

  for (const v of Object.values(rec)) {
    if (Array.isArray(v) || (v && typeof v === "object")) deepCollectRows(v, out);
  }
}

async function callBingMethod(opts: {
  endpoint: string;
  apiKey: string;
  siteUrl: string;
  method: string;
  startDate: string;
  endDate: string;
}) {
  const startIso = opts.startDate;
  const endIso = opts.endDate;
  const startUs = toUsDate(startIso);
  const endUs = toUsDate(endIso);

  const variants: Array<Record<string, string>> = [
    { siteUrl: opts.siteUrl, startDate: startIso, endDate: endIso, apikey: opts.apiKey },
    { siteurl: opts.siteUrl, startDate: startIso, endDate: endIso, apikey: opts.apiKey },
    { siteUrl: opts.siteUrl, start: startIso, end: endIso, apikey: opts.apiKey },
    { siteurl: opts.siteUrl, start: startIso, end: endIso, apikey: opts.apiKey },
    { siteUrl: opts.siteUrl, fromDate: startIso, toDate: endIso, apikey: opts.apiKey },
    { siteUrl: opts.siteUrl, startDate: startUs, endDate: endUs, apikey: opts.apiKey },
    { siteurl: opts.siteUrl, startDate: startUs, endDate: endUs, apikey: opts.apiKey },
    { siteUrl: opts.siteUrl, start: startUs, end: endUs, apikey: opts.apiKey },
    { siteurl: opts.siteUrl, start: startUs, end: endUs, apikey: opts.apiKey },
    { siteUrl: opts.siteUrl, fromDate: startUs, toDate: endUs, apikey: opts.apiKey },
  ];

  let lastErr = "";
  for (const q of variants) {
    const url = new URL(`${opts.endpoint.replace(/\/+$/, "")}/${opts.method}`);
    for (const [k, v] of Object.entries(q)) {
      if (s(v)) url.searchParams.set(k, v);
    }

    const res = await fetch(url.toString(), { cache: "no-store" });
    const txt = await res.text();
    if (!res.ok) {
      lastErr = `HTTP ${res.status} ${opts.method}`;
      continue;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(txt);
    } catch {
      lastErr = `Invalid JSON in ${opts.method}`;
      continue;
    }

    const rows: BingApiRow[] = [];
    deepCollectRows(parsed, rows);
    if (rows.length) return { ok: true, rows, raw: parsed };
    lastErr = `No metric rows in ${opts.method}`;
  }

  return { ok: false, rows: [] as BingApiRow[], error: lastErr || `No data from ${opts.method}` };
}

function aggregateTrendRows(queryRows: BingApiRow[]) {
  const byDate = new Map<string, { impressions: number; clicks: number; ctrAcc: number; ctrW: number; posAcc: number; posW: number }>();
  for (const r of queryRows) {
    const d = parseDateFromRow(r);
    if (!d) continue;
    const impressions = pickImpressions(r);
    const clicks = pickClicks(r);
    const ctr = pickCtr(r);
    const position = pickPosition(r);
    const prev = byDate.get(d) || { impressions: 0, clicks: 0, ctrAcc: 0, ctrW: 0, posAcc: 0, posW: 0 };
    prev.impressions += impressions;
    prev.clicks += clicks;
    prev.ctrAcc += ctr * Math.max(impressions, 1);
    prev.ctrW += Math.max(impressions, 1);
    if (position > 0) {
      prev.posAcc += position * Math.max(impressions, 1);
      prev.posW += Math.max(impressions, 1);
    }
    byDate.set(d, prev);
  }

  return Array.from(byDate.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([date, x]) => ({
      date,
      impressions: x.impressions,
      clicks: x.clicks,
      ctr: x.ctrW > 0 ? x.ctrAcc / x.ctrW : 0,
      position: x.posW > 0 ? x.posAcc / x.posW : 0,
    }));
}

function aggregateTrendFromNormalizedRows(
  rows: Array<{ date: string; impressions: number; clicks: number; ctr: number; position: number }>,
) {
  const byDate = new Map<string, { impressions: number; clicks: number; ctrAcc: number; ctrW: number; posAcc: number; posW: number }>();
  for (const r of rows) {
    const d = s(r.date);
    if (!d) continue;
    const impressions = toNum(r.impressions);
    const clicks = toNum(r.clicks);
    const ctr = toNum(r.ctr);
    const position = toNum(r.position);
    const prev = byDate.get(d) || { impressions: 0, clicks: 0, ctrAcc: 0, ctrW: 0, posAcc: 0, posW: 0 };
    prev.impressions += impressions;
    prev.clicks += clicks;
    prev.ctrAcc += ctr * Math.max(impressions, 1);
    prev.ctrW += Math.max(impressions, 1);
    if (position > 0) {
      prev.posAcc += position * Math.max(impressions, 1);
      prev.posW += Math.max(impressions, 1);
    }
    byDate.set(d, prev);
  }

  return Array.from(byDate.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([date, x]) => ({
      date,
      impressions: x.impressions,
      clicks: x.clicks,
      ctr: x.ctrW > 0 ? x.ctrAcc / x.ctrW : 0,
      position: x.posW > 0 ? x.posAcc / x.posW : 0,
    }));
}

export async function GET(req: Request) {
  const cacheDir = path.join(process.cwd(), "data", "cache", "bing");
  const metaPath = path.join(cacheDir, "meta.json");
  const queriesPath = path.join(cacheDir, "queries.json");
  const pagesPath = path.join(cacheDir, "pages.json");
  const trendPath = path.join(cacheDir, "trend.json");
  try {
    const { searchParams } = new URL(req.url);
    const preset = (s(searchParams.get("range")) || "last_28_days") as RangePreset;
    const start = s(searchParams.get("start"));
    const end = s(searchParams.get("end"));
    const force = s(searchParams.get("force")) === "1";

    const apiKey = s(process.env.BING_WEBMASTER_API_KEY);
    const siteUrl = s(process.env.BING_WEBMASTER_SITE_URL);
    const siteUrlsRaw = s(process.env.BING_WEBMASTER_SITE_URLS);
    const endpoint = s(process.env.BING_WEBMASTER_API_ENDPOINT) || "https://ssl.bing.com/webmaster/api.svc/json";
    const siteUrls = parseSiteUrls(siteUrl, siteUrlsRaw);
    const siteUrlsKey = siteUrls.join("|");

    if (!apiKey || !siteUrls.length) {
      return Response.json(
        {
          ok: false,
          error: "Missing BING_WEBMASTER_API_KEY and/or BING_WEBMASTER_SITE_URL(S)",
          requiredEnv: ["BING_WEBMASTER_API_KEY", "BING_WEBMASTER_SITE_URL or BING_WEBMASTER_SITE_URLS"],
        },
        { status: 400 },
      );
    }

    const range = rangeFromPreset(preset, start, end);
    const freshnessMs = Math.max(60_000, Number(process.env.BING_SYNC_MAX_AGE_MS || 10 * 60_000));

    if (!force) {
      try {
        const st = await fs.stat(metaPath);
        const age = Date.now() - st.mtimeMs;
        const prevMetaRaw = await fs.readFile(metaPath, "utf8");
        const prevMeta = JSON.parse(prevMetaRaw) as {
          range?: string;
          startDate?: string;
          endDate?: string;
          siteUrlsKey?: string;
        };
        const sameWindow =
          s(prevMeta?.range) === preset &&
          s(prevMeta?.startDate) === range.startDate &&
          s(prevMeta?.endDate) === range.endDate &&
          s(prevMeta?.siteUrlsKey) === siteUrlsKey;
        if (age <= freshnessMs && sameWindow) {
          return Response.json({ ok: true, cached: true, ageMs: age, range, message: "Bing cache is fresh" });
        }
      } catch {
        // no cache yet
      }
    }

    const perSite = await Promise.all(
      siteUrls.map(async (site) => {
        const [queriesRes, pagesRes] = await Promise.all([
          callBingMethod({ endpoint, apiKey, siteUrl: site, method: "GetQueryStats", startDate: range.startDate, endDate: range.endDate }),
          callBingMethod({ endpoint, apiKey, siteUrl: site, method: "GetPageStats", startDate: range.startDate, endDate: range.endDate }),
        ]);
        return { siteUrl: site, queriesRes, pagesRes };
      }),
    );

    const atLeastOneSiteOk = perSite.some((x) => x.queriesRes.ok || x.pagesRes.ok);
    if (!atLeastOneSiteOk) {
      const sample = perSite[0];
      return Response.json(
        {
          ok: false,
          error: `Unable to fetch Bing data for all sites. Query error: ${sample?.queriesRes.error || "unknown"}. Page error: ${sample?.pagesRes.error || "unknown"}`,
        },
        { status: 502 },
      );
    }

    const rawQueryRows = perSite.flatMap((x) => x.queriesRes.rows.map((r) => ({ ...r, __siteUrl: x.siteUrl })));
    const rawPageRows = perSite.flatMap((x) => x.pagesRes.rows.map((r) => ({ ...r, __siteUrl: x.siteUrl })));

    const queryRows = rawQueryRows
      .map((r) => ({
        query: pickQuery(r),
        date: parseDateFromRow(r),
        impressions: pickImpressions(r),
        clicks: pickClicks(r),
        ctr: pickCtr(r),
        position: pickPosition(r),
        siteUrl: s(r.__siteUrl),
        raw: r,
      }))
      .filter((r) => !!s(r.query) && !isLikelyUrl(r.query));

    let pageRows = rawPageRows
      .map((r) => ({
        page: pickPage(r),
        date: parseDateFromRow(r),
        impressions: pickImpressions(r),
        clicks: pickClicks(r),
        ctr: pickCtr(r),
        position: pickPosition(r),
        siteUrl: s(r.__siteUrl),
        raw: r,
      }))
      .filter((r) => !!s(r.page) && isLikelyUrl(r.page));

    // Some Bing responses return URL-like rows only in query payload.
    if (!pageRows.length) {
      pageRows = rawQueryRows
        .map((r) => ({
          page: pickPage(r),
          date: parseDateFromRow(r),
          impressions: pickImpressions(r),
          clicks: pickClicks(r),
          ctr: pickCtr(r),
          position: pickPosition(r),
          siteUrl: s(r.__siteUrl),
          raw: r,
        }))
        .filter((r) => !!s(r.page) && isLikelyUrl(r.page));
    }

    const trendRowsFromQueries = aggregateTrendFromNormalizedRows(queryRows);
    const trendRowsFromPages = aggregateTrendFromNormalizedRows(pageRows);
    const trendRows =
      trendRowsFromPages.length >= trendRowsFromQueries.length
        ? trendRowsFromPages
        : trendRowsFromQueries.length > 0
          ? trendRowsFromQueries
          : aggregateTrendRows(rawQueryRows);

    const meta = {
      ok: true,
      source: "bing_webmaster",
      siteUrl: siteUrls[0] || "",
      siteUrls,
      siteUrlsKey,
      range: preset,
      startDate: range.startDate,
      endDate: range.endDate,
      fetchedAt: new Date().toISOString(),
      endpoint,
    };

    await fs.mkdir(cacheDir, { recursive: true });
    await fs.writeFile(path.join(cacheDir, "meta.json"), JSON.stringify(meta, null, 2), "utf8");
    await fs.writeFile(queriesPath, JSON.stringify({ rows: queryRows }, null, 2), "utf8");
    await fs.writeFile(pagesPath, JSON.stringify({ rows: pageRows }, null, 2), "utf8");
    await fs.writeFile(trendPath, JSON.stringify({ rows: trendRows }, null, 2), "utf8");

    return Response.json({
      ok: true,
      cached: false,
      range,
      counts: {
        queries: queryRows.length,
        pages: pageRows.length,
        trend: trendRows.length,
        sites: siteUrls.length,
      },
      debug: {
        siteResults: perSite.map((x) => ({
          siteUrl: x.siteUrl,
          queryMethodOk: x.queriesRes.ok,
          pageMethodOk: x.pagesRes.ok,
          queryError: x.queriesRes.ok ? null : x.queriesRes.error,
          pageError: x.pagesRes.ok ? null : x.pagesRes.error,
          queryRows: x.queriesRes.rows.length,
          pageRows: x.pagesRes.rows.length,
        })),
      },
    });
  } catch (e: unknown) {
    const rootErr = e instanceof Error ? e.message : "bing sync failed";
    try {
      await Promise.all([
        fs.access(metaPath),
        fs.access(queriesPath),
        fs.access(pagesPath),
        fs.access(trendPath),
      ]);
      let snapshotAgeMs: number | null = null;
      try {
        const st = await fs.stat(metaPath);
        snapshotAgeMs = Math.max(0, Date.now() - st.mtimeMs);
      } catch {
        snapshotAgeMs = null;
      }
      return Response.json({
        ok: true,
        cached: true,
        stale: true,
        usedSnapshot: true,
        warning: rootErr,
        snapshotAgeMs,
        message: "Bing sync failed; using cached snapshot",
      });
    } catch {
      return Response.json({ ok: false, error: rootErr }, { status: 500 });
    }
  }
}
