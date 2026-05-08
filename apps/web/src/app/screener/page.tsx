"use client";

import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { AppShell } from "@/components/layout/AppShell";
import {
  ScreenerFilters,
  type ScreenerFiltersState,
} from "@/components/screener/ScreenerFilters";
import { ScreenerResultsTable } from "@/components/screener/ScreenerResultsTable";
import { EmptyState } from "@/components/ui/EmptyState";
import { ErrorState } from "@/components/ui/ErrorState";
import { LoadingState } from "@/components/ui/LoadingState";
import { MetricCard } from "@/components/ui/MetricCard";
import { screenMarkets, type ScreenerMarketResult } from "@/lib/api";
import { theme } from "@/lib/theme";
import {
  getInvestorPreset,
  INVESTOR_PRESETS,
  type InvestorPresetId,
} from "@/lib/investorPresets";

const DEFAULT_FILTERS: ScreenerFiltersState = {
  geoType: "metro",
  state: "",
  cyclePhase: "",
  investorSignal: "",
  minConfidence: "0.5",
  minPriceGrowth: "",
  maxPriceGrowth: "",
  minRentGrowth: "",
  maxInventoryGrowth: "",
  maxPaymentToIncome: "",
};

function parseOptionalNumber(value: string): number | undefined {
  const trimmed = value.trim();

  if (!trimmed) {
    return undefined;
  }

  const parsed = Number(trimmed);

  if (Number.isNaN(parsed)) {
    return undefined;
  }

  return parsed;
}

function normalizeSignal(value: string | null | undefined): string {
  return (value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
}

function rowValue(row: ScreenerMarketResult, key: string): number | null {
  const value = row.values?.[key];

  if (value === null || value === undefined) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function rowHasMaterialMissingData(row: ScreenerMarketResult): boolean {
  const missingMetrics = row.missing_metrics ?? [];

  if (missingMetrics.length >= 4) {
    return true;
  }

  const confidence = row.confidence_score;
  return confidence !== null && confidence !== undefined && confidence < 0.7;
}

function rowMatchesInvestorPreset(
  row: ScreenerMarketResult,
  preset: InvestorPresetId,
): boolean {
  const signal = normalizeSignal(row.investor_stance ?? row.investor_signal);
  const confidence = row.confidence_score;
  const paymentToIncome = rowValue(row, "payment_to_income_ratio");
  const rentYoy = rowValue(row, "rent_yoy");
  const materialMissing = rowHasMaterialMissingData(row);

  switch (preset) {
    case "all":
      return true;

    case "attractive":
      return signal === "attractive";

    case "watchlist":
      return signal === "watchlist";

    case "mixed":
      return signal === "mixed";

    case "avoid":
      return signal === "avoid";

    case "insufficient_data":
      return signal === "insufficient_data";

    case "affordable_watchlist":
      return (
        paymentToIncome !== null &&
        paymentToIncome <= 0.30 &&
        signal !== "avoid" &&
        signal !== "insufficient_data"
      );

    case "rent_momentum":
      return (
        rentYoy !== null &&
        rentYoy > 0 &&
        signal !== "avoid" &&
        signal !== "insufficient_data"
      );

    case "high_confidence":
      return (
        confidence !== null &&
        confidence !== undefined &&
        confidence >= 0.85 &&
        !materialMissing &&
        signal !== "insufficient_data"
      );

    case "missing_data_review":
      return materialMissing || signal === "insufficient_data";

    default:
      return true;
  }
}

export default function ScreenerPage() {
  const [filters, setFilters] = useState<ScreenerFiltersState>(DEFAULT_FILTERS);
  const [activePreset, setActivePreset] = useState<InvestorPresetId>("all");

  const queryArgs = useMemo(
    () => ({
      geoType: filters.geoType || undefined,
      state: filters.state.trim() || undefined,
      cyclePhase: filters.cyclePhase || undefined,
      investorSignal: filters.investorSignal || undefined,
      minConfidence: parseOptionalNumber(filters.minConfidence),
      minPriceGrowth: parseOptionalNumber(filters.minPriceGrowth),
      maxPriceGrowth: parseOptionalNumber(filters.maxPriceGrowth),
      minRentGrowth: parseOptionalNumber(filters.minRentGrowth),
      maxInventoryGrowth: parseOptionalNumber(filters.maxInventoryGrowth),
      maxPaymentToIncome: parseOptionalNumber(filters.maxPaymentToIncome),
      limit: 50,
      offset: 0,
    }),
    [filters],
  );

  const screenerQuery = useQuery({
    queryKey: ["market-screener", queryArgs],
    queryFn: () => screenMarkets(queryArgs),
  });

  const rawItems = useMemo(
    () => screenerQuery.data?.items ?? [],
    [screenerQuery.data?.items],
  );

  const presetItems = useMemo(
    () =>
      rawItems.filter((row) =>
        rowMatchesInvestorPreset(row, activePreset),
      ),
    [rawItems, activePreset],
  );

  const activePresetDetails = getInvestorPreset(activePreset);

  return (
    <AppShell>
      <section className="space-y-8">
        <div>
          <p className="text-sm font-medium uppercase tracking-[0.2em] text-slate-500">
            Screener
          </p>
          <h1 className={`${theme.h1} mt-3`}>
            Market screener
          </h1>
          <p className="mt-3 max-w-3xl text-[#AEB8C6]">
            Filter markets by cycle phase, signal, confidence, price growth,
            rent growth, affordability, and inventory pressure.
          </p>
        </div>

        <div className="grid gap-4 md:grid-cols-4">
          <MetricCard
            label="Results"
            value={screenerQuery.data?.total ?? "—"}
            helperText="Total matching markets before preset filter."
          />
          <MetricCard
            label="Preset matches"
            value={screenerQuery.isSuccess ? presetItems.length : "—"}
            helperText={activePresetDetails.label}
          />
          <MetricCard
            label="Geo type"
            value={filters.geoType || "Any"}
            helperText="Current jurisdiction level."
          />
          <MetricCard
            label="Minimum confidence"
            value={filters.minConfidence || "Any"}
            helperText="0 to 1 scale."
          />
        </div>

        <div className={`${theme.cardTight} p-5`}>
          <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
            <div>
              <h2 className={theme.h3}>
                Investor presets
              </h2>
              <p className="mt-1 max-w-3xl text-sm text-[#AEB8C6]">
                Use these deterministic presets to turn the screener into a
                market shortlist. Presets do not replace underwriting.
              </p>
            </div>
            <div className="text-sm text-slate-400">
              Showing {presetItems.length} of {rawItems.length} loaded markets
            </div>
          </div>

          <div className="mt-4 flex flex-wrap gap-2">
            {INVESTOR_PRESETS.map((preset) => {
              const isActive = preset.id === activePreset;

              return (
                <button
                  key={preset.id}
                  type="button"
                  onClick={() => setActivePreset(preset.id)}
                  className={[
                    "rounded-full border px-3 py-1.5 text-sm font-medium transition",
                    isActive
                      ? "border-cyan-300 bg-cyan-300 text-slate-950"
                      : "border-slate-700 bg-slate-900 text-slate-300 hover:border-slate-400",
                  ].join(" ")}
                  title={preset.description}
                >
                  {preset.label}
                </button>
              );
            })}
          </div>

          <p className="mt-3 text-sm text-slate-400">
            {activePresetDetails.description}
          </p>
        </div>

        <ScreenerFilters filters={filters} onChange={setFilters} />

        {screenerQuery.isLoading ? (
          <LoadingState title="Loading screener results" />
        ) : null}

        {screenerQuery.isError ? (
          <ErrorState
            title="Could not load screener results"
            error={screenerQuery.error}
          />
        ) : null}

        {screenerQuery.isSuccess && rawItems.length === 0 ? (
          <EmptyState
            title="No markets match these filters"
            message="Try lowering the confidence threshold or clearing metric filters."
          />
        ) : null}

        {screenerQuery.isSuccess && rawItems.length > 0 && presetItems.length === 0 ? (
          <EmptyState
            title="No markets match this investor preset"
            message="Try All Markets, Watchlist-style screener filters, or lower the confidence threshold."
          />
        ) : null}

        {screenerQuery.isSuccess && presetItems.length > 0 ? (
          <ScreenerResultsTable items={presetItems} />
        ) : null}
      </section>
    </AppShell>
  );
}
