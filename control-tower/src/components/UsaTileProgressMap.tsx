"use client";

import { useMemo, useState } from "react";

type SheetStateRow = {
  state: string;
  counties: { total: number; ready: number; domainsActive?: number };
  cities: { total: number; ready: number; domainsActive?: number };
};

type Props = {
  rows?: SheetStateRow[]; // opcional (si no lo pasas, el componente puede quedarse “vacío”)
};

type Metric = "ready" | "domains";

/** 12x8 tile layout (simplificado, pero correcto) + PR */
const TILE_POS: Record<string, { x: number; y: number; abbr: string }> = {
  WA: { x: 0, y: 0, abbr: "WA" },
  OR: { x: 0, y: 1, abbr: "OR" },
  CA: { x: 0, y: 2, abbr: "CA" },
  AK: { x: 0, y: 6, abbr: "AK" },

  ID: { x: 1, y: 1, abbr: "ID" },
  NV: { x: 1, y: 2, abbr: "NV" },
  UT: { x: 2, y: 2, abbr: "UT" },
  AZ: { x: 2, y: 3, abbr: "AZ" },
  HI: { x: 1, y: 6, abbr: "HI" },

  MT: { x: 2, y: 1, abbr: "MT" },
  WY: { x: 3, y: 2, abbr: "WY" },
  CO: { x: 3, y: 3, abbr: "CO" },
  NM: { x: 3, y: 4, abbr: "NM" },

  ND: { x: 4, y: 1, abbr: "ND" },
  SD: { x: 4, y: 2, abbr: "SD" },
  NE: { x: 4, y: 3, abbr: "NE" },
  KS: { x: 4, y: 4, abbr: "KS" },
  OK: { x: 4, y: 5, abbr: "OK" },
  TX: { x: 4, y: 6, abbr: "TX" },

  MN: { x: 5, y: 1, abbr: "MN" },
  IA: { x: 5, y: 3, abbr: "IA" },
  MO: { x: 5, y: 4, abbr: "MO" },
  AR: { x: 5, y: 5, abbr: "AR" },
  LA: { x: 5, y: 6, abbr: "LA" },

  WI: { x: 6, y: 2, abbr: "WI" },
  IL: { x: 6, y: 3, abbr: "IL" },
  KY: { x: 6, y: 4, abbr: "KY" },
  TN: { x: 6, y: 5, abbr: "TN" },
  MS: { x: 6, y: 6, abbr: "MS" },

  MI: { x: 7, y: 2, abbr: "MI" },
  IN: { x: 7, y: 3, abbr: "IN" },
  OH: { x: 7, y: 4, abbr: "OH" },
  AL: { x: 7, y: 6, abbr: "AL" },

  PA: { x: 8, y: 3, abbr: "PA" },
  WV: { x: 8, y: 4, abbr: "WV" },
  VA: { x: 8, y: 5, abbr: "VA" },
  NC: { x: 8, y: 6, abbr: "NC" },
  SC: { x: 8, y: 7, abbr: "SC" },

  NY: { x: 9, y: 2, abbr: "NY" },
  NJ: { x: 9, y: 3, abbr: "NJ" },
  MD: { x: 9, y: 5, abbr: "MD" },
  DC: { x: 10, y: 5, abbr: "DC" },
  GA: { x: 9, y: 7, abbr: "GA" },
  FL: { x: 9, y: 8, abbr: "FL" }, // fuera del 8, ajustamos grid a 9 rows en CSS

  VT: { x: 10, y: 1, abbr: "VT" },
  NH: { x: 11, y: 1, abbr: "NH" },
  MA: { x: 11, y: 2, abbr: "MA" },
  CT: { x: 11, y: 3, abbr: "CT" },
  RI: { x: 11, y: 4, abbr: "RI" },

  ME: { x: 12, y: 1, abbr: "ME" }, // expandimos columnas en CSS
  DE: { x: 10, y: 4, abbr: "DE" },

  // Puerto Rico (tile)
  PR: { x: 12, y: 8, abbr: "PR" },
};

function pct(n: number, d: number) {
  if (!d) return 0;
  return Math.max(0, Math.min(1, n / d));
}

function stateToAbbr(name: string) {
  const n = String(name || "").trim().toLowerCase();

  // si en tu sheet viene "Puerto Rico"
  if (n === "puerto rico") return "PR";

  // si en tu sheet viene "Alabama" etc, aquí puedes extender (rápido):
  const map: Record<string, string> = {
    alabama: "AL",
    alaska: "AK",
    arizona: "AZ",
    arkansas: "AR",
    california: "CA",
    colorado: "CO",
    connecticut: "CT",
    delaware: "DE",
    florida: "FL",
    georgia: "GA",
    hawaii: "HI",
    idaho: "ID",
    illinois: "IL",
    indiana: "IN",
    iowa: "IA",
    kansas: "KS",
    kentucky: "KY",
    louisiana: "LA",
    maine: "ME",
    maryland: "MD",
    massachusetts: "MA",
    michigan: "MI",
    minnesota: "MN",
    mississippi: "MS",
    missouri: "MO",
    montana: "MT",
    nebraska: "NE",
    nevada: "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    ohio: "OH",
    oklahoma: "OK",
    oregon: "OR",
    pennsylvania: "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    tennessee: "TN",
    texas: "TX",
    utah: "UT",
    vermont: "VT",
    virginia: "VA",
    washington: "WA",
    "west virginia": "WV",
    wisconsin: "WI",
    wyoming: "WY",
  };

  return map[n] || "";
}

export default function UsaTileProgressMap({ rows = [] }: Props) {
  const [metric, setMetric] = useState<Metric>("ready");
  const [selected, setSelected] = useState<string>("");

  const byAbbr = useMemo(() => {
    const m = new Map<string, SheetStateRow>();
    for (const r of rows) {
      const abbr = stateToAbbr(r.state);
      if (abbr) m.set(abbr, r);
    }
    return m;
  }, [rows]);

  const tiles = useMemo(() => {
    const out: Array<{
      abbr: string;
      name: string;
      x: number;
      y: number;
      value: number; // 0..1
      readyPct: number;
      domainsPct: number;
    }> = [];

    for (const [abbr, pos] of Object.entries(TILE_POS)) {
      const row = byAbbr.get(abbr);

      const cTotal = row?.counties?.total || 0;
      const ciTotal = row?.cities?.total || 0;

      const readyDone = (row?.counties?.ready || 0) + (row?.cities?.ready || 0);
      const readyTotal = cTotal + ciTotal;

      const domainDone =
        (row?.counties?.domainsActive || 0) + (row?.cities?.domainsActive || 0);

      // domains% lo normalizamos también contra total rows
      const domainsTotal = readyTotal;

      const readyPct = pct(readyDone, readyTotal);
      const domainsPct = pct(domainDone, domainsTotal);

      out.push({
        abbr,
        name: row?.state || abbr,
        x: pos.x,
        y: pos.y,
        value: metric === "ready" ? readyPct : domainsPct,
        readyPct,
        domainsPct,
      });
    }

    return out;
  }, [byAbbr, metric]);

  const selectedTile = useMemo(() => {
    if (!selected) return null;
    return tiles.find((t) => t.abbr === selected) || null;
  }, [tiles, selected]);

  return (
    <div className="tileMapWrap">
      <div className="tileMapHeader">
        <div>
          <div className="tileMapTitle">US Progress Map</div>
          <div className="mini" style={{ opacity: 0.85 }}>
            Coverage (Ready%) / Domains Activated
          </div>
        </div>

        <div className="tileMapToggles">
          <button
            className={`smallBtn ${metric === "ready" ? "smallBtnOn" : ""}`}
            onClick={() => setMetric("ready")}
            type="button"
          >
            Ready%
          </button>
          <button
            className={`smallBtn ${metric === "domains" ? "smallBtnOn" : ""}`}
            onClick={() => setMetric("domains")}
            type="button"
          >
            Domains%
          </button>
        </div>
      </div>

      <div className="tileMapGrid" role="region" aria-label="US tile map">
        {tiles.map((t) => {
          const v = t.value; // 0..1
          const intensity = Math.round(v * 100);

          const isSel = selected === t.abbr;
          const hasData = byAbbr.has(t.abbr);

          return (
            <button
              key={t.abbr}
              className={`tile ${isSel ? "tileSel" : ""} ${hasData ? "" : "tileNoData"}`}
              style={{
                gridColumnStart: t.x + 1,
                gridRowStart: t.y + 1,
                // Apple-like: usamos opacity de un overlay (no colores hardcodeados “feos”)
                ["--tileAlpha" as any]: String(0.10 + v * 0.70),
              }}
              onClick={() => setSelected(t.abbr)}
              type="button"
              title={`${t.name} • Ready ${Math.round(t.readyPct * 100)}% • Domains ${Math.round(t.domainsPct * 100)}%`}
            >
              <div className="tileTop">
                <span className="tileAbbr">{t.abbr}</span>
                <span className="tilePct">{intensity}%</span>
              </div>
              <div className="tileName">{t.name}</div>
            </button>
          );
        })}
      </div>

      <div className="tileMapFooter mini" style={{ opacity: 0.85 }}>
        {selectedTile ? (
          <>
            <b>{selectedTile.name}</b> • Ready{" "}
            <b>{Math.round(selectedTile.readyPct * 100)}%</b> • Domains{" "}
            <b>{Math.round(selectedTile.domainsPct * 100)}%</b>
          </>
        ) : (
          <>Click a tile to inspect.</>
        )}
      </div>
    </div>
  );
}
