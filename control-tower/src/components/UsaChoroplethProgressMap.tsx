"use client";

import { useMemo, useState } from "react";
import { geoAlbersUsa, geoPath, geoMercator } from "d3-geo";
import { feature } from "topojson-client";

// US states (FIPS 01..56)
import usStates from "us-atlas/states-10m.json";
// US counties includes PR counties (FIPS starts with 72xxx)
import usCounties from "us-atlas/counties-10m.json";

type SheetStateRow = {
  state: string;

  // para tu “control tower” / progress
  counties?: { total: number; ready: number; domainsActive?: number };
  cities?: { total: number; ready: number; domainsActive?: number };

  // ✅ para calls dashboard (map labels = calls)
  calls?: number;
  missed?: number;
  completed?: number;
  talkSeconds?: number;

  // allow arbitrary fields from caller (e.g. __value)
  [key: string]: any;
};

type Metric = "ready" | "domains" | "calls";
type LabelMode = "pct" | "value";

type Props = {
  rows: SheetStateRow[];

  /** ready/domains/calls */
  metric?: Metric;

  /** si metric=calls, default = value */
  labelMode?: LabelMode;

  /** click state */
  onPick?: (stateName: string) => void;

  selectedState?: string;

  /** opcional: override de calls por estado si tu data no está en rows.calls */
  getCallsByState?: (stateName: string) => number;

  /**
   * ✅ NUEVO: cuando metric="calls", de dónde leer el valor.
   * Ej: valueField="__value" si estás pasando { __value: calls } desde el page.
   */
  valueField?: string;

  /** Optional value prefix for value labels, e.g. "$" for revenue maps. */
  valuePrefix?: string;
};

function norm(s: any) {
  return String(s ?? "").trim();
}

function clamp01(x: number) {
  return Math.max(0, Math.min(1, x));
}

function pct(n: number, d: number) {
  if (!d) return 0;
  return clamp01(n / d);
}

function stateToKey(name: string) {
  const n = norm(name).toLowerCase();
  if (n === "puerto rico") return "pr";
  return n;
}

function labelForPct(p: number) {
  return `${Math.round(p * 100)}%`;
}

function labelForValue(v: number, prefix = "") {
  const n = Number(v);
  const safe = Number.isFinite(n) ? Math.round(n) : 0;
  return `${prefix}${safe.toLocaleString()}`;
}

/**
 * Apple-like ramp:
 * - solo alpha sobre tono “mint” (CSS usa --fillA)
 */
function alphaFromNormalized(n01: number) {
  return 0.08 + clamp01(n01) * 0.72; // 0.08..0.80
}

/**
 * us-atlas states-10m ids: 01 Alabama, 02 Alaska, etc (FIPS)
 */
const FIPS_TO_STATE: Record<string, string> = {
  "01": "Alabama",
  "02": "Alaska",
  "04": "Arizona",
  "05": "Arkansas",
  "06": "California",
  "08": "Colorado",
  "09": "Connecticut",
  "10": "Delaware",
  "11": "District of Columbia",
  "12": "Florida",
  "13": "Georgia",
  "15": "Hawaii",
  "16": "Idaho",
  "17": "Illinois",
  "18": "Indiana",
  "19": "Iowa",
  "20": "Kansas",
  "21": "Kentucky",
  "22": "Louisiana",
  "23": "Maine",
  "24": "Maryland",
  "25": "Massachusetts",
  "26": "Michigan",
  "27": "Minnesota",
  "28": "Mississippi",
  "29": "Missouri",
  "30": "Montana",
  "31": "Nebraska",
  "32": "Nevada",
  "33": "New Hampshire",
  "34": "New Jersey",
  "35": "New Mexico",
  "36": "New York",
  "37": "North Carolina",
  "38": "North Dakota",
  "39": "Ohio",
  "40": "Oklahoma",
  "41": "Oregon",
  "42": "Pennsylvania",
  "44": "Rhode Island",
  "45": "South Carolina",
  "46": "South Dakota",
  "47": "Tennessee",
  "48": "Texas",
  "49": "Utah",
  "50": "Vermont",
  "51": "Virginia",
  "53": "Washington",
  "54": "West Virginia",
  "55": "Wisconsin",
  "56": "Wyoming",
};

export default function UsaChoroplethProgressMap({
  rows,
  metric = "calls",
  labelMode,
  onPick,
  selectedState,
  getCallsByState,
  valueField,
  valuePrefix = "",
}: Props) {
  const [hover, setHover] = useState<string>("");

  const selKey = stateToKey(selectedState || "");
  const effectiveLabelMode: LabelMode =
    labelMode ?? (metric === "calls" ? "value" : "pct");

  // ==============
  // Build metrics per state
  // ==============
  const valueByState = useMemo(() => {
    const m = new Map<
      string,
      {
        pct: number; // used for alpha when metric != calls
        readyPct: number;
        domainsPct: number;

        calls: number;
        callsNorm: number; // used for alpha when metric=calls
      }
    >();

    let maxCalls = 0;

    for (const r of rows || []) {
      const state = norm(r.state);
      if (!state) continue;
      const key = stateToKey(state);

      const cTotal = r.counties?.total || 0;
      const ciTotal = r.cities?.total || 0;

      const readyDone = (r.counties?.ready || 0) + (r.cities?.ready || 0);
      const readyTotal = cTotal + ciTotal;

      const domainDone =
        (r.counties?.domainsActive || 0) + (r.cities?.domainsActive || 0);
      const domainsTotal = readyTotal;

      const readyPct = pct(readyDone, readyTotal);
      const domainsPct = pct(domainDone, domainsTotal);

      // ✅ Calls resolution order:
      // 1) getCallsByState override
      // 2) valueField provided (e.g. "__value")
      // 3) r.calls
      let calls = 0;

      if (typeof getCallsByState === "function") {
        calls = Number(
          getCallsByState(state === "Puerto Rico" ? "Puerto Rico" : state) || 0,
        );
      } else if (valueField) {
        calls = Number((r as any)?.[valueField] || 0);
      } else {
        calls = Number(r.calls || 0);
      }

      if (calls > maxCalls) maxCalls = calls;
      const prev = m.get(key);
      if (!prev) {
        m.set(key, {
          pct: metric === "ready" ? readyPct : domainsPct,
          readyPct,
          domainsPct,
          calls,
          callsNorm: 0,
        });
      } else {
        // Aggregate repeated state rows instead of overwriting the previous one.
        const nextCalls = prev.calls + calls;
        if (nextCalls > maxCalls) maxCalls = nextCalls;
        m.set(key, {
          pct:
            metric === "ready"
              ? clamp01((prev.readyPct + readyPct) / 2)
              : clamp01((prev.domainsPct + domainsPct) / 2),
          readyPct: clamp01((prev.readyPct + readyPct) / 2),
          domainsPct: clamp01((prev.domainsPct + domainsPct) / 2),
          calls: nextCalls,
          callsNorm: 0,
        });
      }
    }

    // second pass: normalize calls
    for (const [k, v] of m.entries()) {
      const callsNorm = maxCalls ? clamp01(v.calls / maxCalls) : 0;
      m.set(k, { ...v, callsNorm });
    }

    return m;
  }, [rows, metric, getCallsByState, valueField]);

  // ==============
  // Geo (states)
  // ==============
  const geo = useMemo(() => {
    const states = feature(usStates as any, (usStates as any).objects.states)
      .features as any[];

    // ✅ Slightly bigger map
    const projection = geoAlbersUsa()
      .translate([520, 292]) // a touch up
      .scale(1280); // a touch bigger

    const path = geoPath(projection);

    const centroids = new Map<string, [number, number]>();
    for (const f of states) {
      const id = String(f.id).padStart(2, "0");
      const name = FIPS_TO_STATE[id];
      if (!name) continue;
      const c = path.centroid(f) as any;
      if (Array.isArray(c) && Number.isFinite(c[0]) && Number.isFinite(c[1])) {
        centroids.set(name.toLowerCase(), [c[0], c[1]]);
      }
    }

    return { states, path, centroids };
  }, []);

  // ==============
  // Geo (Puerto Rico) silhouette from counties-10m (FIPS 72xxx)
  // ==============
  const prGeo = useMemo(() => {
    const allCounties = feature(
      usCounties as any,
      (usCounties as any).objects.counties,
    ).features as any[];

    const prCounties = allCounties.filter((f2: any) => {
      const id2 = String(f2.id).padStart(5, "0");
      return id2.startsWith("72");
    });

    const prW = 150;
    const prH = 80;

    const proj = geoMercator();
    const p = geoPath(proj);

    proj.fitSize([prW, prH], {
      type: "FeatureCollection",
      features: prCounties,
    });

    const path = geoPath(proj);

    // centroid for labels in PR box (we will use this to center the value)
    let cx = prW / 2;
    let cy = prH / 2;
    try {
      const c = p.centroid({
        type: "FeatureCollection",
        features: prCounties,
      } as any) as any;

      if (Array.isArray(c) && Number.isFinite(c[0]) && Number.isFinite(c[1])) {
        cx = c[0];
        cy = c[1];
      }
    } catch {}

    return { prCounties, path, prW, prH, cx, cy };
  }, []);

  const hideTinyNE = new Set([
    "rhode island",
    "connecticut",
    "delaware",
    "new jersey",
    "massachusetts",
    "maryland",
    "district of columbia",
  ]);

  return (
    <div className="choroplethWrap">
      <div className="choroplethTop">
        <div>
          <div className="choroplethTitle">Delta System Map</div>
          <div className="mini" style={{ opacity: 0.85 }}>
            {metric === "calls"
              ? "Calls per state"
              : metric === "ready"
                ? "Ready% per state"
                : "Domains% per state"}
          </div>
        </div>

        {hover ? (
          <div className="choroplethHover mini">
            <b>{hover}</b>
          </div>
        ) : (
          <div className="choroplethHover mini" style={{ opacity: 0.6 }}>
            Hover a state
          </div>
        )}
      </div>

      <div className="choroplethSvgWrap">
        <svg
          className="choroplethSvg"
          viewBox="0 0 1040 620"
          role="img"
          aria-label="US choropleth map"
        >
          {/* States */}
          <g>
            {geo.states.map((f: any) => {
              const id = String(f.id).padStart(2, "0");
              const name = FIPS_TO_STATE[id];
              if (!name) return null;

              const key = name.toLowerCase();
              const cell = valueByState.get(key);

              // alpha depends on selected metric
              const n01 =
                metric === "calls" ? (cell?.callsNorm ?? 0) : (cell?.pct ?? 0);
              const a = alphaFromNormalized(n01);

              const isSel = selKey && selKey === key;

              return (
                <path
                  key={id}
                  d={geo.path(f) || ""}
                  className={`stateShape ${isSel ? "stateShapeSel" : ""}`}
                  style={
                    {
                      ["--fillA" as any]: String(a),
                    } as any
                  }
                  onMouseEnter={() => setHover(name)}
                  onMouseLeave={() => setHover("")}
                  onClick={() => onPick?.(name)}
                />
              );
            })}
          </g>

          {/* Labels (calls or %) */}
          <g>
            {geo.states.map((f: any) => {
              const id = String(f.id).padStart(2, "0");
              const name = FIPS_TO_STATE[id];
              if (!name) return null;

              const key = name.toLowerCase();
              const c = geo.centroids.get(key);
              if (!c) return null;

              if (hideTinyNE.has(key)) return null;

              const cell = valueByState.get(key);
              const p = cell?.pct ?? 0;
              const calls = cell?.calls ?? 0;

              const text =
                effectiveLabelMode === "value"
                  ? labelForValue(calls, valuePrefix)
                  : labelForPct(p);

              return (
                <text
                  key={`t-${id}`}
                  x={c[0]}
                  y={c[1]}
                  textAnchor="middle"
                  dominantBaseline="central"
                  className="stateLabel"
                >
                  {text}
                </text>
              );
            })}
          </g>

          {/* Puerto Rico silhouette + value INSIDE (no "PR" text) */}
          {(() => {
            const key = "pr";
            const cell = valueByState.get(key);

            const p = cell?.pct ?? 0;
            const calls = cell?.calls ?? 0;
            const n01 = metric === "calls" ? (cell?.callsNorm ?? 0) : p;
            const a = alphaFromNormalized(n01);

            const isSel = selKey === "pr";

            // position for PR inset
            const x0 = 860;
            const y0 = 510;

            const text =
              effectiveLabelMode === "value"
                ? labelForValue(calls, valuePrefix)
                : labelForPct(p);

            return (
              <g
                transform={`translate(${x0}, ${y0})`}
                onMouseEnter={() => setHover("Puerto Rico")}
                onMouseLeave={() => setHover("")}
                onClick={() => onPick?.("Puerto Rico")}
                style={{ cursor: "pointer" }}
              >
                {/* Invisible hitbox for hover/click */}
                <rect
                  x={-10}
                  y={-10}
                  width={prGeo.prW + 20}
                  height={prGeo.prH + 20}
                  fill="transparent"
                />

                {/* PR silhouette */}
                <g className={isSel ? "stateShapeSel" : ""}>
                  {prGeo.prCounties.map((f2: any) => (
                    <path
                      key={`pr-${f2.id}`}
                      d={prGeo.path(f2) || ""}
                      className="stateShape"
                      style={{ ["--fillA" as any]: String(a) } as any}
                    />
                  ))}
                </g>

                {/* ✅ Value centered INSIDE PR */}
                <text
                  x={prGeo.cx}
                  y={prGeo.cy}
                  textAnchor="middle"
                  dominantBaseline="central"
                  className="stateLabel"
                >
                  {text}
                </text>
              </g>
            );
          })()}
        </svg>
      </div>
    </div>
  );
}
