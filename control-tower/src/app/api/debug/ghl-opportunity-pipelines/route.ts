import { NextResponse } from "next/server";
import { getEffectiveLocationIdOrThrow, ghlFetchJson } from "@/lib/ghlHttp";

export const runtime = "nodejs";

function s(v: unknown) {
  return String(v ?? "").trim();
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

export async function GET(req: Request) {
  const url = new URL(req.url);
  const includeLocation = s(url.searchParams.get("includeLocation")) !== "0";
  const pipelineName = s(url.searchParams.get("name")) || "Lead Generator Bookings";

  try {
    const locationId = await getEffectiveLocationIdOrThrow();

    const variants = [
      { label: "exact", method: "GET" as const, path: "/opportunities/pipelines" },
      {
        label: "with_locationId",
        method: "GET" as const,
        path: `/opportunities/pipelines?locationId=${encodeURIComponent(locationId)}`,
      },
      {
        label: "with_location_id",
        method: "GET" as const,
        path: `/opportunities/pipelines?location_id=${encodeURIComponent(locationId)}`,
      },
    ];

    const attempts: Array<Record<string, unknown>> = [];
    const toRun = includeLocation ? variants : variants.slice(0, 1);

    for (const v of toRun) {
      try {
        const data = await ghlFetchJson(v.path, { method: v.method });
        const pipelines = extractArray(data, ["pipelines", "data", "items"]).map((x) => asObj(x));
        const targetLower = pipelineName.toLowerCase();
        const exact = pipelines.find((p) => s(p.name || p.pipelineName || p.title).toLowerCase() === targetLower) || null;
        const fallback =
          exact ||
          pipelines.find((p) => s(p.name || p.pipelineName || p.title).toLowerCase().includes(targetLower)) ||
          null;

        attempts.push({
          label: v.label,
          ok: true,
          path: v.path,
          pipelinesReturned: pipelines.length,
          targetFound: !!fallback,
        });
        return NextResponse.json({
          ok: true,
          locationId,
          requestedName: pipelineName,
          variant: v.label,
          attempts,
          pipeline: fallback,
          data,
        });
      } catch (e: unknown) {
        attempts.push({
          label: v.label,
          ok: false,
          path: v.path,
          error: e instanceof Error ? e.message : String(e),
        });
      }
    }

    return NextResponse.json(
      {
        ok: false,
        locationId,
        error: "All variants failed",
        attempts,
      },
      { status: 422 },
    );
  } catch (e: unknown) {
    return NextResponse.json(
      {
        ok: false,
        error: e instanceof Error ? e.message : "Failed to fetch opportunity pipelines",
      },
      { status: 500 },
    );
  }
}
