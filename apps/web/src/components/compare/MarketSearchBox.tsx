"use client";

import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import type { MarketListItem } from "@/lib/api";
import { searchMarkets } from "@/lib/api";

type MarketSearchBoxProps = {
  selectedGeoIds: string[];
  onSelect: (market: MarketListItem) => void;
};

export function MarketSearchBox({
  selectedGeoIds,
  onSelect,
}: MarketSearchBoxProps) {
  const [search, setSearch] = useState("");

  const marketsQuery = useQuery({
    queryKey: ["market-search", search],
    queryFn: () =>
      searchMarkets({
        search: search || undefined,
        geoType: "metro",
        limit: 20,
      }),
    staleTime: 60_000,
  });

  const results = useMemo(() => {
    return (marketsQuery.data?.items ?? []).filter(
      (item) => !selectedGeoIds.includes(item.geo_id),
    );
  }, [marketsQuery.data, selectedGeoIds]);

  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5">
      <label className="block">
        <span className="text-sm font-medium text-slate-300">
          Search markets
        </span>
        <input
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Search Detroit, Chicago, Tampa..."
          className="mt-2 w-full rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-white outline-none transition placeholder:text-slate-600 focus:border-slate-400"
        />
      </label>

      <div className="mt-4 max-h-80 space-y-2 overflow-auto">
        {marketsQuery.isLoading ? (
          <p className="text-sm text-slate-400">Loading markets...</p>
        ) : null}

        {marketsQuery.isError ? (
          <p className="text-sm text-red-300">Could not load markets.</p>
        ) : null}

        {marketsQuery.isSuccess && results.length === 0 ? (
          <p className="text-sm text-slate-500">No additional markets found.</p>
        ) : null}

        {results.map((market) => (
          <button
            key={market.geo_id}
            type="button"
            onClick={() => onSelect(market)}
            className="block w-full rounded-xl border border-slate-800 bg-slate-950 px-4 py-3 text-left transition hover:border-slate-600"
          >
            <p className="text-sm font-medium text-white">
              {market.display_name || market.name}
            </p>
            <p className="mt-1 text-xs text-slate-500">
              {market.geo_id}
              {market.cbsa_code ? ` · CBSA ${market.cbsa_code}` : ""}
            </p>
          </button>
        ))}
      </div>
    </div>
  );
}
