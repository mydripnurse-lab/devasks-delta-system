import { NextResponse } from "next/server";

export const runtime = "nodejs";

function s(v: unknown) {
  return String(v ?? "").trim();
}

function toUrlMaybe(domainOrUrl: string) {
  const d = s(domainOrUrl);
  if (!d) return "";
  if (d.startsWith("http://") || d.startsWith("https://")) return d;
  return `https://${d}`;
}

function hostFromAny(domainOrUrl: string) {
  try {
    return new URL(toUrlMaybe(domainOrUrl)).host.toLowerCase();
  } catch {
    return "";
  }
}

function decodeXmlEntities(v: string) {
  return v
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

function extractLocHosts(xmlText: string) {
  const out = new Set<string>();
  const re = /<loc>\s*([^<]+)\s*<\/loc>/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(xmlText))) {
    const raw = decodeXmlEntities(s(m[1]));
    if (!raw) continue;
    try {
      const h = new URL(raw).host.toLowerCase();
      if (h) out.add(h);
    } catch {
      // ignore invalid url
    }
  }
  return Array.from(out);
}

type HealthColor = "green" | "yellow" | "red";

export async function GET(req: Request) {
  try {
    const url = new URL(req.url);
    const sitemapUrl = s(url.searchParams.get("url"));
    const expectedDomain = s(url.searchParams.get("expectedDomain"));

    if (!sitemapUrl) {
      return NextResponse.json(
        { ok: false, error: "Missing query param: url" },
        { status: 400 },
      );
    }

    const target = toUrlMaybe(sitemapUrl);
    const targetUrl = new URL(target);
    const requestedPath = targetUrl.pathname.toLowerCase();
    if (!requestedPath.endsWith("/sitemap.xml") && requestedPath !== "/sitemap.xml") {
      return NextResponse.json(
        { ok: false, error: "URL must point to /sitemap.xml" },
        { status: 400 },
      );
    }
    const expectedHost = hostFromAny(expectedDomain || sitemapUrl);

    const runProbe = async (headers: Record<string, string>) => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 12_000);
      try {
        return await fetch(target, {
          method: "GET",
          redirect: "follow",
          signal: controller.signal,
          headers,
          cache: "no-store",
        });
      } finally {
        clearTimeout(timer);
      }
    };

    // Probe #1 (browser-like headers) to reduce false 403 from WAF/bot filters.
    let res = await runProbe({
      "user-agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      accept: "application/xml,text/xml,application/xhtml+xml,text/html;q=0.9,*/*;q=0.8",
      "accept-language": "en-US,en;q=0.9",
    });

    // Probe #2 fallback when blocked.
    if (res.status === 403 || res.status === 401) {
      res = await runProbe({
        accept: "application/xml,text/xml,text/plain,*/*",
        "user-agent": "curl/8.5.0",
      });
    }

    const body = await res.text();
    const responseHost = hostFromAny(res.url || target);
    const responseUrl = new URL(res.url || target);
    const responsePath = responseUrl.pathname.toLowerCase();
    const contentType = s(res.headers.get("content-type")).toLowerCase();
    const locHosts = extractLocHosts(body);

    const isXmlLike =
      /<\?xml/i.test(body) ||
      /<urlset/i.test(body) ||
      /<sitemapindex/i.test(body) ||
      /<loc>/i.test(body);
    const contentTypeXml =
      contentType.includes("xml") || contentType.includes("text/plain");
    const pathMatchesSitemap =
      responsePath.endsWith("/sitemap.xml") || responsePath === "/sitemap.xml";
    const blockedByProtection = (res.status === 401 || res.status === 403) && pathMatchesSitemap;
    const active =
      (res.ok && pathMatchesSitemap && (isXmlLike || contentTypeXml || locHosts.length > 0)) ||
      blockedByProtection;

    const matches =
      !!expectedHost &&
      (responseHost === expectedHost || locHosts.some((h) => h === expectedHost));

    const checks = {
      statusOk: res.ok,
      pathIsSitemapXml: pathMatchesSitemap,
      xmlDetected: isXmlLike || locHosts.length > 0,
      hostMatches: !!matches,
      protectedByWaf: blockedByProtection,
    };

    let health: HealthColor = "red";
    let summary = "Sitemap validation failed";
    if (checks.pathIsSitemapXml && checks.hostMatches && checks.xmlDetected && checks.statusOk) {
      health = "green";
      summary = "Sitemap active and valid";
    } else if (checks.pathIsSitemapXml && checks.hostMatches && checks.protectedByWaf) {
      health = "yellow";
      summary = "Sitemap likely active but protected by WAF";
    } else if (checks.pathIsSitemapXml && checks.hostMatches && checks.statusOk) {
      health = "yellow";
      summary = "Sitemap reachable but XML markers are weak";
    }

    return NextResponse.json({
      ok: true,
      health,
      summary,
      active,
      matches,
      expectedHost: expectedHost || undefined,
      responseHost: responseHost || undefined,
      responseStatus: res.status,
      requestedPath,
      responsePath,
      contentType,
      pathMatchesSitemap,
      xmlDetected: isXmlLike,
      blockedByProtection,
      checks,
      sampleHosts: locHosts.slice(0, 8),
      checkedAt: new Date().toISOString(),
    });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : "Sitemap verify failed";
    return NextResponse.json(
      { ok: false, error: msg },
      { status: 500 },
    );
  }
}
