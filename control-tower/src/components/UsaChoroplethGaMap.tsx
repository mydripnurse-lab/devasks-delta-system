"use client";

import { useMemo, useState } from "react";
import { geoAlbersUsa, geoPath, geoMercator } from "d3-geo";
import { feature } from "topojson-client";

// US states (FIPS 01..56)
import usStates from "us-atlas/states-10m.json";
// US counties includes PR counties (FIPS starts with 72xxx)
import usCounties from "us-atlas/counties-10m.json";

type GaMetric =
  | "sessions"
  | "users"
  | "views"
  | "conversions"
  | "engagementRate";
type LabelMode = "value" | "pct";

export type GaStateRow = {
  // legacy / expected
  state?: string; // "Florida" / "Puerto Rico"
  stateCode?: string;

  // what your API returns
  country?: string; // "United States" / "Puerto Rico"
  region?: string; // "Florida" / "(not set)" / "Puerto Rico"

  sessions?: number;
  users?: number;
  views?: number;
  conversions?: number;
  engagementRate?: number; // 0..1
  [k: string]: any;
};

type Props = {
  rows: GaStateRow[];

  metric?: GaMetric;
  labelMode?: LabelMode;

  selectedState?: string;
  onPick?: (payload: { stateName: string; stateCode?: string }) => void;

  valueField?: string;
};

function norm(s: any) {
  return String(s ?? "").trim();
}
function clamp01(x: number) {
  return Math.max(0, Math.min(1, x));
}

function stateKey(name: string) {
  const n = norm(name).toLowerCase();
  if (n === "puerto rico") return "pr";
  return n;
}

function fmtInt(x: any) {
  const n = Number(x);
  if (!Number.isFinite(n)) return "0";
  return Math.round(n).toLocaleString();
}
function fmtPct01(x: any) {
  const n = Number(x);
  if (!Number.isFinite(n)) return "0%";
  return `${Math.round(n * 100)}%`;
}

/** Alpha-only ramp using CSS var --fillA in .gaMapState */
function alphaFromNormalized(n01: number) {
  return 0.08 + clamp01(n01) * 0.72; // 0.08..0.80
}

/** ✅ HYDRATION FIX: quantize floats so Node + browser match */
function q(x: number, decimals = 2) {
  if (!Number.isFinite(x)) return 0;
  const p = Math.pow(10, decimals);
  return Math.round(x * p) / p;
}

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

const STATE_TO_CODE: Record<string, string> = {
  Alabama: "AL",
  Alaska: "AK",
  Arizona: "AZ",
  Arkansas: "AR",
  California: "CA",
  Colorado: "CO",
  Connecticut: "CT",
  Delaware: "DE",
  "District of Columbia": "DC",
  Florida: "FL",
  Georgia: "GA",
  Hawaii: "HI",
  Idaho: "ID",
  Illinois: "IL",
  Indiana: "IN",
  Iowa: "IA",
  Kansas: "KS",
  Kentucky: "KY",
  Louisiana: "LA",
  Maine: "ME",
  Maryland: "MD",
  Massachusetts: "MA",
  Michigan: "MI",
  Minnesota: "MN",
  Mississippi: "MS",
  Missouri: "MO",
  Montana: "MT",
  Nebraska: "NE",
  Nevada: "NV",
  "New Hampshire": "NH",
  "New Jersey": "NJ",
  "New Mexico": "NM",
  "New York": "NY",
  "North Carolina": "NC",
  "North Dakota": "ND",
  Ohio: "OH",
  Oklahoma: "OK",
  Oregon: "OR",
  Pennsylvania: "PA",
  "Rhode Island": "RI",
  "South Carolina": "SC",
  "South Dakota": "SD",
  Tennessee: "TN",
  Texas: "TX",
  Utah: "UT",
  Vermont: "VT",
  Virginia: "VA",
  Washington: "WA",
  "West Virginia": "WV",
  Wisconsin: "WI",
  Wyoming: "WY",
  "Puerto Rico": "PR",
};

// ✅ Normalize row -> state name (supports {state} or {region}, and PR by country)
function resolveStateName(r: GaStateRow) {
  const st = norm((r as any)?.state);
  const rg = norm((r as any)?.region);
  const co = norm((r as any)?.country);

  // If GA returns PR as country="Puerto Rico" and region="(not set)" or empty:
  if (co.toLowerCase() === "puerto rico") return "Puerto Rico";

  // Prefer explicit state, fallback to region
  const name = st || rg;

  // Avoid placeholders
  if (!name || name === "__unknown" || name.toLowerCase() === "(not set)") {
    return "";
  }

  return name;
}

export default function UsaChoroplethGaMap({
  rows,
  metric = "sessions",
  labelMode,
  selectedState,
  onPick,
  valueField,
}: Props) {
  const [hover, setHover] = useState("");

  const selKey = stateKey(selectedState || "");

  const effectiveLabelMode: LabelMode =
    labelMode ?? (metric === "engagementRate" ? "pct" : "value");

  // =========================
  // Build metric values per state (seed ALL states + PR with 0)
  // =========================
  const valueByState = useMemo(() => {
    type Cell = {
      value: number;
      norm: number;
      engagementRate: number;
      stateName: string;
      stateCode?: string;
    };

    const m = new Map<string, Cell>();

    for (const name of Object.values(FIPS_TO_STATE)) {
      const key = name.toLowerCase();
      m.set(key, {
        value: 0,
        norm: 0,
        engagementRate: 0,
        stateName: name,
        stateCode: STATE_TO_CODE[name],
      });
    }

    m.set("pr", {
      value: 0,
      norm: 0,
      engagementRate: 0,
      stateName: "Puerto Rico",
      stateCode: "PR",
    });

    let maxVal = 0;

    for (const r of rows || []) {
      const stateName = resolveStateName(r);
      if (!stateName) continue;

      const key = stateKey(stateName);

      let v = 0;
      if (valueField) {
        v = Number((r as any)?.[valueField] || 0);
      } else {
        v =
          metric === "sessions"
            ? Number(r.sessions || 0)
            : metric === "users"
              ? Number(r.users || 0)
              : metric === "views"
                ? Number(r.views || 0)
                : metric === "conversions"
                  ? Number(r.conversions || 0)
                  : Number(r.engagementRate || 0);
      }

      const value = Number.isFinite(v) ? v : 0;
      const er = clamp01(Number(r.engagementRate || 0));

      if (Number.isFinite(value) && value > maxVal) maxVal = value;

      const prettyName = key === "pr" ? "Puerto Rico" : stateName;

      m.set(key, {
        value,
        norm: 0,
        engagementRate: er,
        stateName: prettyName,
        stateCode: r.stateCode || STATE_TO_CODE[prettyName] || undefined,
      });
    }

    for (const [k, o] of m.entries()) {
      let n01 = 0;
      if (metric === "engagementRate") n01 = clamp01(o.value);
      else n01 = maxVal ? clamp01(o.value / maxVal) : 0;
      m.set(k, { ...o, norm: n01 });
    }

    return m;
  }, [rows, metric, valueField]);

  // =========================
  // Geo (states)
  // =========================
  const geo = useMemo(() => {
    const states = feature(usStates as any, (usStates as any).objects.states)
      .features as any[];

    const projection = geoAlbersUsa().translate([520, 292]).scale(1280);

    const path = geoPath(projection);

    const centroids = new Map<string, [number, number]>();
    for (const f of states) {
      const id = String(f.id).padStart(2, "0");
      const name = FIPS_TO_STATE[id];
      if (!name) continue;

      const c = path.centroid(f) as any;
      if (Array.isArray(c) && Number.isFinite(c[0]) && Number.isFinite(c[1])) {
        centroids.set(name.toLowerCase(), [q(c[0], 2), q(c[1], 2)]);
      }
    }

    return { states, path, centroids };
  }, []);

  // =========================
  // Puerto Rico silhouette from counties-10m (FIPS 72xxx)
  // =========================
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
    proj.fitSize([prW, prH], {
      type: "FeatureCollection",
      features: prCounties,
    });

    const path = geoPath(proj);

    let cx = prW / 2;
    let cy = prH / 2;
    try {
      const p = geoPath(proj);
      const c = p.centroid({
        type: "FeatureCollection",
        features: prCounties,
      } as any) as any;

      if (Array.isArray(c) && Number.isFinite(c[0]) && Number.isFinite(c[1])) {
        cx = q(c[0], 2);
        cy = q(c[1], 2);
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

  function titleFor(metric: GaMetric) {
    if (metric === "sessions") return "Sessions";
    if (metric === "users") return "Users";
    if (metric === "views") return "Pageviews";
    if (metric === "conversions") return "Conversions";
    return "Engagement rate";
  }

  function formatLabel(cell: any) {
    const safe = cell || { value: 0, norm: 0 };
    if (metric === "engagementRate") return fmtPct01(safe.value);
    if (effectiveLabelMode === "pct") return fmtPct01(safe.norm);
    return fmtInt(safe.value);
  }

  return (
    <div className="gaMapWrap">
      <div className="gaMapTop">
        <div>
          <div className="gaMapTitle">Google Analytics Map</div>
          <div className="mini" style={{ opacity: 0.85 }}>
            Heatmap: <b>{titleFor(metric)}</b> • Click state to drill down
          </div>
        </div>

        {hover ? (
          <div className="gaMapHover mini">
            <b>{hover}</b>
          </div>
        ) : (
          <div className="gaMapHover mini" style={{ opacity: 0.6 }}>
            Hover a state
          </div>
        )}
      </div>

      <div className="gaMapSvgWrap">
        <svg
          className="gaMapSvg"
          viewBox="0 0 1040 620"
          role="img"
          aria-label="US choropleth map (GA4)"
        >
          {/* States */}
          <g>
            {geo.states.map((f: any) => {
              const id = String(f.id).padStart(2, "0");
              const name = FIPS_TO_STATE[id];
              if (!name) return null;

              const key = name.toLowerCase();
              const cell = valueByState.get(key);

              const a = alphaFromNormalized(cell?.norm ?? 0);
              const isSel = selKey && selKey === key;

              return (
                <path
                  key={id}
                  d={geo.path(f) || ""}
                  className={`gaMapState ${isSel ? "gaMapStateSel" : ""}`}
                  style={{ ["--fillA" as any]: String(a) } as any}
                  onMouseEnter={() => setHover(name)}
                  onMouseLeave={() => setHover("")}
                  onClick={() =>
                    onPick?.({
                      stateName: name,
                      stateCode: STATE_TO_CODE[name],
                    })
                  }
                />
              );
            })}
          </g>

          {/* Labels */}
          <g>
            {geo.states.map((f: any) => {
              const id = String(f.id).padStart(2, "0");
              const name = FIPS_TO_STATE[id];
              if (!name) return null;

              const key = name.toLowerCase();
              if (hideTinyNE.has(key)) return null;

              const c = geo.centroids.get(key);
              if (!c) return null;

              const cell = valueByState.get(key);
              const text = formatLabel(cell);

              return (
                <text
                  key={`t-${id}`}
                  x={c[0]}
                  y={c[1]}
                  textAnchor="middle"
                  dominantBaseline="central"
                  className="gaMapLabel"
                >
                  {text}
                </text>
              );
            })}
          </g>

          {/* Puerto Rico inset */}
          {(() => {
            const cell = valueByState.get("pr");
            const a = alphaFromNormalized(cell?.norm ?? 0);
            const isSel = selKey === "pr";

            const x0 = 860;
            const y0 = 510;

            const text = formatLabel(cell);

            return (
              <g
                transform={`translate(${x0}, ${y0})`}
                onMouseEnter={() => setHover("Puerto Rico")}
                onMouseLeave={() => setHover("")}
                onClick={() =>
                  onPick?.({
                    stateName: "Puerto Rico",
                    stateCode: "PR",
                  })
                }
                style={{ cursor: "pointer" }}
              >
                <rect
                  x={-10}
                  y={-10}
                  width={prGeo.prW + 20}
                  height={prGeo.prH + 20}
                  fill="transparent"
                />

                <g className={isSel ? "gaMapStateSel" : ""}>
                  {prGeo.prCounties.map((f2: any) => (
                    <path
                      key={`pr-${f2.id}`}
                      d={prGeo.path(f2) || ""}
                      className="gaMapState"
                      style={{ ["--fillA" as any]: String(a) } as any}
                    />
                  ))}
                </g>

                <text
                  x={prGeo.cx}
                  y={prGeo.cy}
                  textAnchor="middle"
                  dominantBaseline="central"
                  className="gaMapLabel"
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
