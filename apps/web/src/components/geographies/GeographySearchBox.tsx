"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import { searchGeographies, type GeographySearchItem } from "@/lib/api";

const GEO_TYPES = [
  { label: "All", value: "" },
  { label: "National", value: "national" },
  { label: "State", value: "state" },
  { label: "County", value: "county" },
  { label: "Metro", value: "metro" },
  { label: "Place", value: "place" },
  { label: "ZCTA", value: "zcta" },
];

function geoHref(geoId: string): string {
  return `/geographies/${encodeURIComponent(geoId)}`;
}

export function GeographySearchBox() {
  const [query, setQuery] = useState("");
  const [geoType, setGeoType] = useState("");
  const [items, setItems] = useState<GeographySearchItem[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const trimmedQuery = useMemo(() => query.trim(), [query]);

  useEffect(() => {
    let cancelled = false;

    async function runSearch() {
      setIsLoading(true);
      setError(null);

      try {
        const response = await searchGeographies({
          q: trimmedQuery || null,
          geo_type: geoType || null,
          limit: 20,
        });

        if (!cancelled) {
          setItems(response.items);
        }
      } catch {
        if (!cancelled) {
          setError("Geography search is unavailable.");
          setItems([]);
        }
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    }

    const timeoutId = window.setTimeout(runSearch, 250);

    return () => {
      cancelled = true;
      window.clearTimeout(timeoutId);
    };
  }, [trimmedQuery, geoType]);

  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="mb-4">
        <h2 className="text-base font-semibold text-slate-900">Find a geography</h2>
        <p className="mt-1 text-sm text-slate-500">
          Search states, counties, metros, places, and ZCTAs.
        </p>
      </div>

      <div className="grid gap-3 md:grid-cols-[1fr_180px]">
        <input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search Detroit, 48201, Michigan..."
          className="rounded-xl border border-slate-300 px-3 py-2 text-sm outline-none focus:border-slate-500"
        />

        <select
          value={geoType}
          onChange={(event) => setGeoType(event.target.value)}
          className="rounded-xl border border-slate-300 px-3 py-2 text-sm outline-none focus:border-slate-500"
        >
          {GEO_TYPES.map((option) => (
            <option key={option.value || "all"} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </div>

      <div className="mt-4">
        {isLoading ? (
          <p className="text-sm text-slate-500">Searching…</p>
        ) : error ? (
          <p className="text-sm text-red-600">{error}</p>
        ) : items.length === 0 ? (
          <p className="text-sm text-slate-500">No geographies found.</p>
        ) : (
          <div className="divide-y divide-slate-100 rounded-xl border border-slate-100">
            {items.map((item) => (
              <Link
                key={item.geography.geo_id}
                href={geoHref(item.geography.geo_id)}
                className="block p-3 transition hover:bg-slate-50"
              >
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <div className="text-sm font-medium text-slate-900">
                      {item.geography.display_name}
                    </div>
                    <div className="mt-1 text-xs text-slate-500">
                      {item.geography.geo_type} · {item.geography.geo_id}
                    </div>
                  </div>

                  <div className="text-right text-xs text-slate-500">
                    <div>{item.parent_count} parents</div>
                    <div>{item.child_count} children</div>
                  </div>
                </div>
              </Link>
            ))}
          </div>
        )}
      </div>
    </section>
  );
}
