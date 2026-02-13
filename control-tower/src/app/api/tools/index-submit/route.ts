import { NextResponse } from "next/server";
import { google } from "googleapis";
import path from "path";
import fs from "fs";
import fsp from "fs/promises";
import { getGscOAuthClient } from "@/lib/gscAuth";

export const runtime = "nodejs";

function s(v: unknown) {
  return String(v ?? "").trim();
}

function toUrlMaybe(v: string) {
  const d = s(v);
  if (!d) return "";
  if (d.startsWith("http://") || d.startsWith("https://")) return d;
  return `https://${d}`;
}

function toOriginUrlMaybe(v: string) {
  const full = toUrlMaybe(v);
  if (!full) return "";
  try {
    const u = new URL(full);
    return `${u.protocol}//${u.host}/`;
  } catch {
    return "";
  }
}

function apexFromHost(host: string) {
  const h = s(host).toLowerCase().replace(/\.+$/, "");
  const parts = h.split(".").filter(Boolean);
  if (parts.length <= 2) return h;
  return `${parts[parts.length - 2]}.${parts[parts.length - 1]}`;
}

function googleErr(e: unknown) {
  const anyE = e as any;
  const msg =
    s(anyE?.response?.data?.error?.message) ||
    s(anyE?.response?.data?.error) ||
    s(anyE?.message) ||
    "Google request failed.";
  const status = Number(anyE?.response?.status || 0) || undefined;
  return { msg, status };
}

function resolveKeyfilePathSmart(relOrAbs: string) {
  const raw = s(relOrAbs);
  if (!raw) return "";
  if (path.isAbsolute(raw)) return raw;

  const cwd = process.cwd();
  const bases: string[] = [cwd, path.join(cwd, ".."), path.join(cwd, "..", "..")];
  const parts = cwd.split(path.sep);
  const idx = parts.lastIndexOf("control-tower");
  if (idx >= 0) {
    const repoRoot = parts.slice(0, idx).join(path.sep);
    if (repoRoot) {
      bases.unshift(repoRoot);
      bases.unshift(path.join(repoRoot, "control-tower"));
    }
  }
  for (const base of bases) {
    const candidate = path.normalize(path.join(base, raw));
    if (fs.existsSync(candidate)) return candidate;
  }
  return path.normalize(path.join(cwd, raw));
}

type AuthCandidate = {
  name: string;
  auth: any;
};

type InspectSnapshot = {
  verdict?: string;
  coverageState?: string;
  indexingState?: string;
  lastCrawlTime?: string;
  robotsTxtState?: string;
};

async function getLegacyOAuthFromGoogleApplicationCredentials() {
  const tokenPathRaw = s(process.env.GOOGLE_APPLICATION_CREDENTIALS);
  const clientId = s(process.env.GSC_CLIENT_ID);
  const clientSecret = s(process.env.GSC_CLIENT_SECRET);
  if (!tokenPathRaw || !clientId || !clientSecret) return null;

  const tokenPath = resolveKeyfilePathSmart(tokenPathRaw);
  if (!fs.existsSync(tokenPath)) return null;

  try {
    const raw = await fsp.readFile(tokenPath, "utf8");
    const parsed = JSON.parse(raw || "{}");
    const tokens = parsed?.tokens || parsed;
    const refreshToken = s(tokens?.refresh_token);
    if (!refreshToken) return null;

    const oauth2 = new google.auth.OAuth2(clientId, clientSecret, "http://localhost");
    oauth2.setCredentials({
      refresh_token: refreshToken,
      access_token: s(tokens?.access_token) || undefined,
      expiry_date: Number(tokens?.expiry_date) || undefined,
    });
    await oauth2.getAccessToken();
    return oauth2;
  } catch {
    return null;
  }
}

async function getGoogleAuthCandidates(): Promise<AuthCandidate[]> {
  const out: AuthCandidate[] = [];

  const useOauth = s(process.env.GSC_TOKENS_FILE) && s(process.env.GSC_OAUTH_CLIENT_FILE);
  if (useOauth) {
    try {
      const { oauth2 } = await getGscOAuthClient();
      out.push({ name: "gsc_oauth_files", auth: oauth2 });
    } catch {}
  }

  const legacyOAuth = await getLegacyOAuthFromGoogleApplicationCredentials();
  if (legacyOAuth) {
    out.push({ name: "legacy_google_application_credentials_oauth", auth: legacyOAuth });
  }

  const clientEmail = s(process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL);
  const privateKeyRaw = s(process.env.GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY);
  if (clientEmail && privateKeyRaw) {
    const privateKey = privateKeyRaw.replace(/\\n/g, "\n");
    const jwt = new google.auth.JWT({
      email: clientEmail,
      key: privateKey,
      scopes: ["https://www.googleapis.com/auth/webmasters"],
    });
    out.push({ name: "service_account_inline", auth: jwt });
  }

  const keyfileEnv = s(process.env.GOOGLE_SERVICE_ACCOUNT_KEYFILE);
  if (keyfileEnv) {
    const keyFile = resolveKeyfilePathSmart(keyfileEnv);
    if (fs.existsSync(keyFile)) {
      const ga = new google.auth.GoogleAuth({
        keyFile,
        scopes: ["https://www.googleapis.com/auth/webmasters"],
      });
      out.push({ name: "service_account_keyfile", auth: ga });
    }
  }

  return out;
}

async function inspectWithCandidates(
  candidates: AuthCandidate[],
  inspectionUrl: string,
  siteUrlForInspection: string,
) {
  let inspectRes: any = null;
  const authErrors: string[] = [];
  let winnerAuthName = "";

  for (const candidate of candidates) {
    try {
      const searchconsole = google.searchconsole({ version: "v1", auth: candidate.auth as any });
      inspectRes = await (searchconsole as any).urlInspection.index.inspect({
        requestBody: {
          inspectionUrl,
          siteUrl: siteUrlForInspection,
          languageCode: "en-US",
        },
      });
      winnerAuthName = candidate.name;
      break;
    } catch (e) {
      const ge = googleErr(e);
      authErrors.push(`auth=${candidate.name}:${ge.status || 500}:${ge.msg}`);
    }
  }

  if (!inspectRes) {
    return {
      ok: false as const,
      winnerAuthName,
      authErrors,
      inspect: null,
      snapshot: {} as InspectSnapshot,
    };
  }

  const result = inspectRes?.data?.inspectionResult?.indexStatusResult || {};
  const snapshot: InspectSnapshot = {
    verdict: s(result?.verdict).toUpperCase() || undefined,
    coverageState: s(result?.coverageState) || undefined,
    indexingState: s(result?.indexingState) || undefined,
    lastCrawlTime: s(result?.lastCrawlTime) || undefined,
    robotsTxtState: s(result?.robotsTxtState) || undefined,
  };

  return {
    ok: true as const,
    winnerAuthName,
    authErrors,
    inspect: inspectRes,
    snapshot,
  };
}

async function requestDiscoveryForUnknown(
  candidates: AuthCandidate[],
  preferredAuthName: string,
  siteUrlForInspection: string,
  domainOrigin: string,
) {
  const sitemapUrl = `${domainOrigin.replace(/\/+$/, "")}/sitemap.xml`;
  const chosen: AuthCandidate[] = [];
  const preferred = candidates.find((c) => c.name === preferredAuthName);
  if (preferred) chosen.push(preferred);
  for (const c of candidates) {
    if (!chosen.find((x) => x.name === c.name)) chosen.push(c);
  }

  let submitted = false;
  let submittedBy = "";
  let submitError = "";
  for (const candidate of chosen) {
    try {
      const webmasters = google.webmasters({ version: "v3", auth: candidate.auth as any });
      await webmasters.sitemaps.submit({
        siteUrl: siteUrlForInspection,
        feedpath: sitemapUrl,
      });
      submitted = true;
      submittedBy = candidate.name;
      break;
    } catch (e) {
      const ge = googleErr(e);
      submitError = `auth=${candidate.name}:${ge.status || 500}:${ge.msg}`;
    }
  }

  return {
    attempted: true,
    sitemapUrl,
    submitted,
    submittedBy: submittedBy || undefined,
    submitError: submitError || undefined,
  };
}

type GoogleSubmitMode = "inspect" | "discovery" | "auto";

async function inspectGoogleDomain(domainUrl: string, mode: GoogleSubmitMode = "auto") {
  const enabled = s(process.env.INDEX_GOOGLE_ENABLED || "true").toLowerCase();
  if (enabled === "false" || enabled === "0" || enabled === "no") {
    return { ok: false, error: "Google indexing disabled by env (INDEX_GOOGLE_ENABLED=false)." };
  }

  const target = toOriginUrlMaybe(domainUrl);
  if (!target) {
    return { ok: false, error: "Invalid domainUrl." };
  }

  let fetchStatus = 0;
  let fetchFinalUrl = "";
  let fetchContentType = "";
  let fetchError = "";
  try {
    const res = await fetch(target, {
      method: "GET",
      redirect: "follow",
      cache: "no-store",
      headers: {
        "user-agent": "DeltaControlTower/1.0 (+index-check)",
        accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      },
    });
    fetchStatus = res.status;
    fetchFinalUrl = s(res.url || target);
    fetchContentType = s(res.headers.get("content-type"));
  } catch (e: any) {
    fetchError = s(e?.message) || "Fetch failed";
  }

  const authCandidates = await getGoogleAuthCandidates();
  if (!authCandidates.length) {
    return {
      ok: false,
      mode: "inspect" as const,
      status: 500,
      siteUrl: target,
      fetch: {
        status: fetchStatus || undefined,
        finalUrl: fetchFinalUrl || target,
        contentType: fetchContentType || undefined,
        error: fetchError || undefined,
      },
      inspection: {},
      error:
        "No Google auth available. Configure GSC OAuth (GSC_*) or legacy OAuth (GOOGLE_APPLICATION_CREDENTIALS + GSC_CLIENT_ID/GSC_CLIENT_SECRET) or service account (GOOGLE_SERVICE_ACCOUNT_*).",
    };
  }

  const host = new URL(target).host.toLowerCase();
  const configuredSiteUrl = s(process.env.GSC_SITE_URL);
  const siteUrlForInspection =
    configuredSiteUrl || `sc-domain:${apexFromHost(host)}`;
  if (mode === "discovery") {
    const discovery = await requestDiscoveryForUnknown(
      authCandidates,
      "",
      siteUrlForInspection,
      target,
    );
    const discoveryOk = !!discovery?.submitted;
    return {
      ok: discoveryOk,
      mode: "discovery" as const,
      status: 200,
      siteUrl: target,
      siteProperty: siteUrlForInspection,
      fetch: {
        status: fetchStatus || undefined,
        finalUrl: fetchFinalUrl || target,
        contentType: fetchContentType || undefined,
        error: fetchError || undefined,
      },
      inspection: {},
      discovery,
      error: discoveryOk
        ? undefined
        : `Sitemap discovery submit failed${discovery?.submitError ? ` (${discovery.submitError})` : "."}`,
    };
  }

  const firstInspection = await inspectWithCandidates(authCandidates, target, siteUrlForInspection);
  if (!firstInspection.ok) {
    return {
      ok: false,
      mode: "inspect" as const,
      status: 500,
      siteUrl: target,
      fetch: {
        status: fetchStatus || undefined,
        finalUrl: fetchFinalUrl || target,
        contentType: fetchContentType || undefined,
        error: fetchError || undefined,
      },
      inspection: {
        verdict: undefined,
        coverageState: undefined,
        indexingState: undefined,
        lastCrawlTime: undefined,
        robotsTxtState: undefined,
      },
      error: `Google inspection failed using ${siteUrlForInspection}. ${firstInspection.authErrors.join(" | ")}`,
    };
  }

  let snapshot = firstInspection.snapshot;
  const verdict = s(snapshot.verdict).toUpperCase();
  const coverageState = s(snapshot.coverageState);
  const indexingState = s(snapshot.indexingState);
  const crawlState = s(snapshot.lastCrawlTime);
  const robots = s(snapshot.robotsTxtState);

  const fetchedOk = fetchStatus >= 200 && fetchStatus < 500;
  const inspectionOk =
    verdict === "PASS" ||
    /indexed|submitted and indexed|crawled currently not indexed|discovered currently not indexed/i.test(
      `${coverageState} ${indexingState}`,
    );

  const unknownToGoogle = /unknown to google/i.test(coverageState);
  let discovery:
    | {
        attempted: boolean;
        sitemapUrl: string;
        submitted: boolean;
        submittedBy?: string;
        submitError?: string;
      }
    | undefined;

  if (unknownToGoogle && mode === "auto") {
    discovery = await requestDiscoveryForUnknown(
      authCandidates,
      firstInspection.winnerAuthName,
      siteUrlForInspection,
      target,
    );
    // Re-check immediately after discovery request. Might still be unknown, but we return latest state.
    const afterInspection = await inspectWithCandidates(authCandidates, target, siteUrlForInspection);
    if (afterInspection.ok) {
      snapshot = afterInspection.snapshot;
    }
  }

  const discoveryRequested =
    discovery?.attempted === true && discovery?.submitted === true;
  const effectivelyIndexed = fetchedOk && inspectionOk;

  return {
    ok: effectivelyIndexed,
    mode: "inspect" as const,
    status: 200,
    siteUrl: target,
    siteProperty: siteUrlForInspection,
    fetch: {
      status: fetchStatus || undefined,
      finalUrl: fetchFinalUrl || target,
      contentType: fetchContentType || undefined,
      error: fetchError || undefined,
    },
    inspection: {
      verdict: snapshot.verdict || undefined,
      coverageState: snapshot.coverageState || undefined,
      indexingState: snapshot.indexingState || undefined,
      lastCrawlTime: snapshot.lastCrawlTime || undefined,
      robotsTxtState: snapshot.robotsTxtState || undefined,
    },
    discovery,
    error:
      effectivelyIndexed
        ? undefined
        : !fetchedOk
          ? fetchError || `Website check failed (HTTP ${fetchStatus || 0}).`
          : discoveryRequested
            ? `Discovery requested. Google still reports ${snapshot.coverageState || "unknown"}${discovery?.submittedBy ? ` [submit by ${discovery.submittedBy}]` : ""}${discovery?.submitError ? `; submitError=${discovery.submitError}` : ""}.`
            : `Google inspection indicates not indexable yet (${snapshot.coverageState || "unknown"}).`,
  };
}

export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => ({} as any));
    const target = s(body?.target || "google").toLowerCase();
    const domainUrl = toOriginUrlMaybe(s(body?.domainUrl));
    const modeRaw = s(body?.mode || "auto").toLowerCase();
    const mode: GoogleSubmitMode =
      modeRaw === "inspect" || modeRaw === "discovery" ? (modeRaw as GoogleSubmitMode) : "auto";
    if (target !== "google") {
      return NextResponse.json(
        { ok: false, error: 'Only target="google" is supported.' },
        { status: 400 },
      );
    }

    if (!domainUrl) {
      return NextResponse.json({ ok: false, error: "Missing domainUrl" }, { status: 400 });
    }

    const out: {
      ok: boolean;
      target: "google";
      domainUrl: string;
      host: string;
      google?: {
        ok: boolean;
        mode?: "inspect" | "discovery";
        status?: number;
        siteUrl?: string;
        siteProperty?: string;
        fetch?: {
          status?: number;
          finalUrl?: string;
          contentType?: string;
          error?: string;
        };
        inspection?: {
          verdict?: string;
          coverageState?: string;
          indexingState?: string;
          lastCrawlTime?: string;
          robotsTxtState?: string;
        };
        discovery?: {
          attempted: boolean;
          sitemapUrl: string;
          submitted: boolean;
          submittedBy?: string;
          submitError?: string;
        };
        bodyPreview?: string;
        error?: string;
      };
      error?: string;
    } = {
      ok: true,
      target: "google",
      domainUrl,
      host: new URL(domainUrl).host.toLowerCase(),
    };

    try {
      out.google = await inspectGoogleDomain(domainUrl, mode);
    } catch (e: any) {
      out.google = { ok: false, error: s(e?.message) || "Google index submit failed." };
    }
    out.ok = !!out.google?.ok;
    if (!out.ok) {
      out.error = out.google?.error || "Google indexability check failed.";
    }

    return NextResponse.json(out, { status: 200 });
  } catch (e: any) {
    return NextResponse.json(
      { ok: false, error: s(e?.message) || "Index submit failed." },
      { status: 500 },
    );
  }
}
