"use client";

import { useMemo, useState } from "react";
import { geoCentroid, geoMercator, geoPath } from "d3-geo";

import type { MarketMapFeature } from "@/lib/api";
import { metricFill } from "@/lib/mapStyle";

type MarketGeoMapProps = {
  features: MarketMapFeature[];
  metric: string;
  selectedFeature: MarketMapFeature | null;
  onHover: (feature: MarketMapFeature | null) => void;
  onSelect: (feature: MarketMapFeature) => void;
};

const SVG_WIDTH = 960;
const SVG_HEIGHT = 600;
const MAP_PADDING = 24;

export function MarketGeoMap({
  features,
  metric,
  selectedFeature,
  onHover,
  onSelect,
}: MarketGeoMapProps) {
  const [focusedGeoId, setFocusedGeoId] = useState<string | null>(null);
  const [focusedFeature, setFocusedFeature] = useState<MarketMapFeature | null>(null);
  const [zoom, setZoom] = useState(1);

  const drawableFeatures = useMemo(
    () => features.filter((feature) => feature.geometry),
    [features],
  );

  const projection = useMemo(() => {
    const nextProjection = geoMercator();

    if (drawableFeatures.length > 0) {
      const featureCollection = {
        type: "FeatureCollection",
        features: drawableFeatures,
      };

      nextProjection.fitExtent(
        [
          [MAP_PADDING, MAP_PADDING],
          [SVG_WIDTH - MAP_PADDING, SVG_HEIGHT - MAP_PADDING],
        ],
        featureCollection as never,
      );
    } else {
      nextProjection.scale(1).translate([SVG_WIDTH / 2, SVG_HEIGHT / 2]);
    }

    return nextProjection;
  }, [drawableFeatures]);

  const pathGenerator = useMemo(() => geoPath(projection), [projection]);

  const emphasizedFeatures = useMemo(() => {
    const items: MarketMapFeature[] = [];

    if (selectedFeature?.geometry) {
      items.push(selectedFeature);
    }

    if (
      focusedFeature?.geometry &&
      focusedFeature.properties.geo_id !== selectedFeature?.properties.geo_id
    ) {
      items.push(focusedFeature);
    }

    return items;
  }, [focusedFeature, selectedFeature]);

  return (
    <div className="relative overflow-hidden rounded-2xl border border-slate-800 bg-slate-950">
      <div className="flex items-center justify-between border-b border-slate-800 px-4 py-3">
        <div>
          <p className="text-sm font-semibold text-white">Market boundaries</p>
          <p className="text-xs text-slate-400">
            Loaded markets: {drawableFeatures.length}. Zoom controls affect the current scope.
          </p>
        </div>

        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setZoom((value) => Math.max(1, Number((value - 0.25).toFixed(2))))}
            className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-2 text-sm font-medium text-slate-200 transition hover:bg-slate-800"
          >
            −
          </button>
          <button
            type="button"
            onClick={() => setZoom(1)}
            className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-2 text-sm font-medium text-slate-200 transition hover:bg-slate-800"
          >
            Reset
          </button>
          <button
            type="button"
            onClick={() => setZoom((value) => Math.min(8, Number((value + 0.25).toFixed(2))))}
            className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-2 text-sm font-medium text-slate-200 transition hover:bg-slate-800"
          >
            +
          </button>
        </div>
      </div>

      <svg
        viewBox={`0 0 ${SVG_WIDTH} ${SVG_HEIGHT}`}
        role="img"
        aria-label="Market boundary map"
        className="h-[560px] w-full"
      >
        <rect width={SVG_WIDTH} height={SVG_HEIGHT} fill="#020617" />

        <g
          transform={`translate(${SVG_WIDTH / 2} ${SVG_HEIGHT / 2}) scale(${zoom}) translate(${-SVG_WIDTH / 2} ${-SVG_HEIGHT / 2})`}
        >
          {drawableFeatures.map((feature) => {
            const path = pathGenerator(feature as never);
            const selected =
              selectedFeature?.properties.geo_id === feature.properties.geo_id;
            const focused = focusedGeoId === feature.properties.geo_id;

            if (!path) {
              return null;
            }

            return (
              <path
                key={feature.properties.geo_id}
                d={path}
                fill={metricFill(feature.properties.value, metric)}
                stroke={selected || focused ? "#ffffff" : "#0f172a"}
                strokeWidth={selected || focused ? 1.8 : 0.8}
                opacity={selected || focused ? 1 : 0.9}
                className="cursor-pointer transition-opacity hover:opacity-100"
                onMouseEnter={() => {
                  setFocusedGeoId(feature.properties.geo_id);
                  setFocusedFeature(feature);
                  onHover(feature);
                }}
                onMouseLeave={() => {
                  setFocusedGeoId(null);
                  setFocusedFeature(null);
                  onHover(null);
                }}
                onClick={() => onSelect(feature)}
              />
            );
          })}

          {emphasizedFeatures.map((feature) => {
            const centroid = geoCentroid(feature as never);
            const point = projection(centroid);

            if (!point) {
              return null;
            }

            const [x, y] = point;
            const label =
              feature.properties.display_name ?? feature.properties.name;

            return (
              <g key={`label-${feature.properties.geo_id}`}>
                <circle cx={x} cy={y} r={4} fill="#ffffff" />
                <rect
                  x={x + 8}
                  y={y - 18}
                  rx={6}
                  ry={6}
                  width={Math.max(120, label.length * 6.4)}
                  height={24}
                  fill="#0f172a"
                  stroke="#334155"
                />
                <text
                  x={x + 16}
                  y={y - 2}
                  fill="#ffffff"
                  fontSize="12"
                  fontWeight="600"
                >
                  {label}
                </text>
              </g>
            );
          })}
        </g>
      </svg>
    </div>
  );
}
