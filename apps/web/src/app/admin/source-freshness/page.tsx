"use client";

import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import {
  SourceFreshnessFilters,
  type SourceFreshnessFilterState,
} from "@/components/admin/SourceFreshnessFilters";
import { SourceFreshnessTable } from "@/components/admin/SourceFreshnessTable";
import { AppShell } from "@/components/layout/AppShell";
import { EmptyState } from "@/components/ui/EmptyState";
import { ErrorState } from "@/components/ui/ErrorState";
import { LoadingState } from "@/components/ui/LoadingState";
import { MetricCard } from "@/components/ui/MetricCard";
import { getSourceFreshness, type SourceFreshnessItem } from "@/lib/api";

const DEFAULT_FILTERS: SourceFreshnessFilterState = {
  search: "",
  source: "",
  dataset: "",
  status: "",
  staleState: "",
};

function uniqueSorted(values: Array<string | null | undefined>): string[] {
  return Array.from(
    new Set(values.filter((value): value is string => Boolean(value))),
  ).sort((a, b) => a.localeCompare(b));
}

function matchesStaleState(
  item: SourceFreshnessItem,
  staleState: string,
): boolean {
  if (!staleState) return true;
  if (staleState === "fresh") return item.is_stale === false;
  if (staleState === "stale") return item.is_stale === true;
  if (staleState === "unknown") return item.is_stale === null || item.is_stale === undefined;
  return true;
}

export default function SourceFreshnessPage() {
  const [filters, setFilters] =
    useState<SourceFreshnessFilterState>(DEFAULT_FILTERS);

  const freshnessQuery = useQuery({
    queryKey: ["source-freshness"],
    queryFn: getSourceFreshness,
    refetchInterval: 60_000,
  });

  const allItems = freshnessQuery.data ?? [];

  const sources = useMemo(
    () => uniqueSorted(allItems.map((item) => item.source)),
    [allItems],
  );

  const datasets = useMemo(
    () => uniqueSorted(allItems.map((item) => item.dataset)),
    [allItems],
  );

  const statuses = useMemo(
    () => uniqueSorted(allItems.map((item) => item.last_status ?? "unknown")),
    [allItems],
  );

  const filteredItems = useMemo(() => {
    const search = filters.search.trim().toLowerCase();

    return allItems.filter((item) => {
      const source = item.source.toLowerCase();
      const dataset = item.dataset.toLowerCase();
      const status = item.last_status ?? "unknown";

      if (search && !source.includes(search) && !dataset.includes(search)) {
        return false;
      }

      if (filters.source && item.source !== filters.source) {
        return false;
      }

      if (filters.dataset && item.dataset !== filters.dataset) {
        return false;
      }

      if (filters.status && status !== filters.status) {
        return false;
      }

      if (!matchesStaleState(item, filters.staleState)) {
        return false;
      }

      return true;
    });
  }, [allItems, filters]);

  const stats = useMemo(() => {
    const total = allItems.length;
    const failed = allItems.filter(
      (item) => (item.last_status ?? "unknown") !== "success",
    ).length;
    const stale = allItems.filter((item) => item.is_stale === true).length;
    const unknown = allItems.filter(
      (item) => item.is_stale === null || item.is_stale === undefined,
    ).length;

    return {
      total,
      failed,
      stale,
      unknown,
      filtered: filteredItems.length,
    };
  }, [allItems, filteredItems]);

  return (
    <AppShell>
      <section className="space-y-8">
        <div>
          <p className="text-sm font-medium uppercase tracking-[0.2em] text-slate-500">
            Admin
          </p>
          <h1 className="mt-3 text-3xl font-bold tracking-tight text-white md:text-4xl">
            Source freshness
          </h1>
          <p className="mt-3 max-w-3xl text-slate-400">
            Operational visibility for ingestion freshness, stale data, failed
            source jobs, and record counts.
          </p>
        </div>

        <div className="grid gap-4 md:grid-cols-5">
          <MetricCard
            label="Sources"
            value={stats.total}
            helperText="Total source/dataset records."
          />
          <MetricCard
            label="Filtered"
            value={stats.filtered}
            helperText="Records matching filters."
          />
          <MetricCard
            label="Failed"
            value={stats.failed}
            helperText="Last status is not success."
            tone={stats.failed > 0 ? "bad" : "good"}
          />
          <MetricCard
            label="Stale"
            value={stats.stale}
            helperText="Source freshness threshold exceeded."
            tone={stats.stale > 0 ? "warning" : "good"}
          />
          <MetricCard
            label="Unknown"
            value={stats.unknown}
            helperText="Staleness has not been classified."
            tone={stats.unknown > 0 ? "warning" : "neutral"}
          />
        </div>

        {freshnessQuery.isLoading ? (
          <LoadingState title="Loading source freshness" />
        ) : null}

        {freshnessQuery.isError ? (
          <ErrorState
            title="Could not load source freshness"
            error={freshnessQuery.error}
          />
        ) : null}

        {freshnessQuery.isSuccess ? (
          <>
            <SourceFreshnessFilters
              filters={filters}
              sources={sources}
              datasets={datasets}
              statuses={statuses}
              onChange={setFilters}
            />

            {filteredItems.length === 0 ? (
              <EmptyState
                title="No source freshness records match these filters"
                message="Clear filters or run ingestion jobs to populate freshness records."
              />
            ) : (
              <SourceFreshnessTable items={filteredItems} />
            )}
          </>
        ) : null}
      </section>
    </AppShell>
  );
}
