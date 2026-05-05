"use client";

import { useMemo, useState } from "react";
import { geoAlbersUsa, geoPath } from "d3-geo";

import type { MarketMapFeature } from "@/lib/api";
import { metricFill } from "@/lib/mapStyle";

type MarketGeoMapProps = {
  features: MarketMapFeature[];
  metric: string;
  selectedFeature: MarketMapFeature | null;
  onHover: (feature: MarketMapFeature | null) => void;
  onSelect: (feature: MarketMapFeature) => void;
};

export function MarketGeoMap({
  features,
  metric,
  selectedFeature,
  onHover,
  onSelect,
}: MarketGeoMapProps) {
  const [focusedGeoId, setFocusedGeoId] = useState<string | null>(null);

  const pathGenerator = useMemo(() => {
    const projection = geoAlbersUsa()
      .translate([480, 300])
      .scale(1100);

    return geoPath(projection);
  }, []);

  const drawableFeatures = features.filter((feature) => feature.geometry);

  return (
    <div className="overflow-hidden rounded-2xl border border-slate-800 bg-slate-950">
      <svg
        viewBox="0 0 960 600"
        role="img"
        aria-label="United States metro market map"
        className="h-[520px] w-full"
      >
        <rect width="960" height="600" fill="#020617" />

        {drawableFeatures.map((feature) => {
          const path = pathGenerator(feature as unknown as GeoJSON.Feature);
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
              strokeWidth={selected || focused ? 1.8 : 0.6}
              opacity={selected || focused ? 1 : 0.86}
              className="cursor-pointer transition-opacity hover:opacity-100"
              onMouseEnter={() => {
                setFocusedGeoId(feature.properties.geo_id);
                onHover(feature);
              }}
              onMouseLeave={() => {
                setFocusedGeoId(null);
                onHover(null);
              }}
              onClick={() => onSelect(feature)}
            />
          );
        })}
      </svg>
    </div>
  );
}
