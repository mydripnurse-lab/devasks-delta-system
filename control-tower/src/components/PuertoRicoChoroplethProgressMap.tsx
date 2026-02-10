// src/components/PuertoRicoChoroplethProgressMap.tsx
"use client";

import { memo, useMemo, useState } from "react";
import { ComposableMap, Geographies, Geography } from "react-simple-maps";

type SheetStateRow = {
  state: string;
  counties: { total: number; ready: number; domainsActive?: number };
  cities: { total: number; ready: number; domainsActive?: number };
};

export type MapMetric = "ready" | "domains";

type Props = {
  rows: SheetStateRow[];
  metric: MapMetric;
  selected: string;
  onPick: (name: string) => void;
};

/**
 * PR map (municipios).
 * Requiere un topojson local:
 *   public/geo/pr-municipios.topo.json
 * Y que el "name" del municipio venga como: geo.properties.name (o ajustas aquí)
 *
 * Mapping de data:
 * - Hoy tu overview NO trae municipios.
 * - Este mapa está listo para cuando tengas dataset municipal.
 * - Mientras tanto, pinta PR completo y permite hover/click por municipio (sin % real).
 */
const PR_TOPOJSON = "/geo/pr-municipios.topo.json";

// helpers
function s(v: any) {
  return String(v ?? "").trim();
}

// Si aún no tienes data por municipio, esto vuelve null y el mapa se pinta “neutral”.
function getMunicipioMetricValue(
  _municipioName: string,
  _rows: SheetStateRow[],
  _metric: MapMetric,
) {
  // TODO: cuando tengas data por municipio, calcula aquí:
  // Ejemplo esperado:
  // - rows podría venir con structure municipal (no ahora)
  // - o consumir /api/sheet/pr?metric=ready que devuelva { municipio: pct }
  return null as null | number;
}

function colorFor(v: number | null) {
  // Paleta simple (sin hardcodear “bonito” pero usable)
  // v: 0..1
  if (v === null) return "rgba(255,255,255,0.06)";
  const x = Math.max(0, Math.min(1, v));
  // verde más intenso con progreso
  return `rgba(80, 255, 180, ${0.18 + 0.62 * x})`;
}

function strokeForSelected(isSelected: boolean) {
  return isSelected ? "rgba(255,255,255,0.85)" : "rgba(255,255,255,0.10)";
}

function PuertoRicoChoroplethProgressMapImpl({
  rows,
  metric,
  selected,
  onPick,
}: Props) {
  const [hoverName, setHoverName] = useState("");

  // memoize lookup (aunque hoy no devuelve data real)
  const valueLookup = useMemo(() => {
    const m = new Map<string, number | null>();
    // Si no tienes data por municipio, quedará todo null.
    // A futuro: aquí podrías precalcular en base a un dataset municipal.
    return m;
  }, [rows, metric]);

  return (
    <div style={{ width: "100%", height: "100%" }}>
      <div className="mapTopOverlay">
        <div className="mapHintChip">
          {hoverName ? hoverName : "Hover a municipio"}
        </div>
      </div>

      <ComposableMap
        projection="geoMercator"
        style={{ width: "100%", height: "100%" }}
        projectionConfig={{
          // Ajuste para PR (estos números suelen funcionar bien para municipales)
          scale: 8500,
          center: [-66.4, 18.2],
        }}
      >
        <Geographies geography={PR_TOPOJSON}>
          {({ geographies }) =>
            geographies.map((geo) => {
              const name =
                s((geo.properties as any)?.name) ||
                s((geo.properties as any)?.NAME) ||
                s((geo.properties as any)?.municipio) ||
                "—";

              const isSelected = selected === name;

              const v =
                valueLookup.get(name) ??
                getMunicipioMetricValue(name, rows, metric);

              return (
                <Geography
                  key={geo.rsmKey}
                  geography={geo}
                  onMouseEnter={() => setHoverName(name)}
                  onMouseLeave={() => setHoverName("")}
                  onClick={() => onPick(name)}
                  style={{
                    default: {
                      fill: colorFor(v),
                      stroke: strokeForSelected(isSelected),
                      strokeWidth: isSelected ? 1.6 : 0.8,
                      outline: "none",
                      cursor: "pointer",
                    },
                    hover: {
                      fill:
                        v === null
                          ? "rgba(255,255,255,0.10)"
                          : colorFor((v ?? 0) + 0.08),
                      stroke: "rgba(255,255,255,0.85)",
                      strokeWidth: 1.2,
                      outline: "none",
                      cursor: "pointer",
                    },
                    pressed: {
                      fill: "rgba(255,255,255,0.12)",
                      stroke: "rgba(255,255,255,0.85)",
                      outline: "none",
                    },
                  }}
                />
              );
            })
          }
        </Geographies>
      </ComposableMap>
    </div>
  );
}

export default memo(PuertoRicoChoroplethProgressMapImpl);
