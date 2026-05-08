"use client";

import Link from "next/link";
import L from "leaflet";
import {
  CircleMarker,
  MapContainer,
  Popup,
  TileLayer,
  useMap,
} from "react-leaflet";
import { useEffect, useMemo } from "react";

import type { ScreenerMarketResult } from "@/lib/api";
import { getStateOption } from "@/lib/map/stateBounds";
import { theme } from "@/lib/theme";

type MarketLeafletMapProps = {
  items: ScreenerMarketResult[];
  selectedState: string;
};

function numeric(value: unknown): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function marketPosition(item: ScreenerMarketResult): [number, number] | null {
  const latitude = numeric(item.market.latitude);
  const longitude = numeric(item.market.longitude);

  if (latitude === null || longitude === null) {
    return null;
  }

  return [latitude, longitude];
}

function markerColor(stance: string | null | undefined): string {
  switch (stance) {
    case "attractive":
      return "#7CFFB2";
    case "watchlist":
      return "#58A6FF";
    case "mixed":
      return "#FBBF24";
    case "avoid":
      return "#FB7185";
    case "insufficient_data":
      return "#94A3B8";
    default:
      return "#CBD5E1";
  }
}

function MapViewport({
  selectedState,
  positionedItems,
}: {
  selectedState: string;
  positionedItems: Array<{ position: [number, number] }>;
}) {
  const map = useMap();

  useEffect(() => {
    const stateOption = getStateOption(selectedState);

    if (positionedItems.length > 0 && selectedState) {
      const bounds = L.latLngBounds(positionedItems.map((item) => item.position));
      map.fitBounds(bounds.pad(0.25), { animate: true, maxZoom: 9 });
      return;
    }

    map.fitBounds(stateOption.bounds, { animate: true });
  }, [map, positionedItems, selectedState]);

  return null;
}

export function MarketLeafletMap({ items, selectedState }: MarketLeafletMapProps) {
  const stateOption = getStateOption(selectedState);

  const positionedItems = useMemo(
    () =>
      items
        .map((item) => {
          const position = marketPosition(item);

          if (!position) {
            return null;
          }

          return {
            item,
            position,
          };
        })
        .filter(
          (
            entry,
          ): entry is {
            item: ScreenerMarketResult;
            position: [number, number];
          } => entry !== null,
        ),
    [items],
  );

  return (
    <div className="overflow-hidden rounded-[2rem] border border-white/10 bg-[#050608] shadow-[0_30px_100px_rgba(0,0,0,0.45)]">
      <div className="flex flex-col gap-3 border-b border-white/10 bg-white/[0.035] px-5 py-4 md:flex-row md:items-center md:justify-between">
        <div>
          <h2 className={theme.h2}>
            {stateOption.value ? `${stateOption.label} market map` : "USA market map"}
          </h2>
          <p className={`mt-1 ${theme.body}`}>
            {positionedItems.length} mapped markets from {items.length} loaded rows.
            Use mouse wheel or map controls to zoom.
          </p>
        </div>

        <div className="flex flex-wrap gap-2">
          <span className={theme.chip}>Zoom enabled</span>
          <span className={theme.chip}>Pan enabled</span>
          <span className={theme.chip}>Click markers</span>
        </div>
      </div>

      <div className="h-[640px] w-full">
        <MapContainer
          center={stateOption.center}
          zoom={stateOption.zoom}
          minZoom={3}
          maxZoom={12}
          scrollWheelZoom
          zoomControl
          className="h-full w-full"
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />

          <MapViewport
            selectedState={selectedState}
            positionedItems={positionedItems}
          />

          {positionedItems.map(({ item, position }) => {
            const stance = item.investor_stance ?? null;
            const color = markerColor(stance);
            const marketName =
              item.market.display_name ?? item.market.name ?? item.market.geo_id;

            return (
              <CircleMarker
                key={item.market.geo_id}
                center={position}
                radius={9}
                pathOptions={{
                  color,
                  fillColor: color,
                  fillOpacity: 0.75,
                  weight: 2,
                }}
              >
                <Popup>
                  <div className="min-w-[220px] space-y-2">
                    <div className="text-sm font-bold text-slate-950">
                      {marketName}
                    </div>
                    <div className="text-xs text-slate-700">
                      {item.market.geo_id}
                    </div>
                    <div className="grid grid-cols-2 gap-2 text-xs">
                      <div>
                        <div className="text-slate-500">Stance</div>
                        <div className="font-semibold">
                          {item.investor_stance_label ??
                            item.investor_signal ??
                            "Not scored"}
                        </div>
                      </div>
                      <div>
                        <div className="text-slate-500">Confidence</div>
                        <div className="font-semibold">
                          {item.confidence_score === null ||
                          item.confidence_score === undefined
                            ? "—"
                            : item.confidence_score.toFixed(2)}
                        </div>
                      </div>
                    </div>
                    <Link
                      href={`/markets/${item.market.geo_id}`}
                      className="inline-flex rounded-lg bg-slate-950 px-3 py-2 text-xs font-semibold text-white"
                    >
                      Open command center
                    </Link>
                  </div>
                </Popup>
              </CircleMarker>
            );
          })}
        </MapContainer>
      </div>
    </div>
  );
}
