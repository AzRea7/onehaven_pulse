"use client";

export type SourceFreshnessFilterState = {
  search: string;
  source: string;
  dataset: string;
  status: string;
  staleState: string;
};

type SourceFreshnessFiltersProps = {
  filters: SourceFreshnessFilterState;
  sources: string[];
  datasets: string[];
  statuses: string[];
  onChange: (filters: SourceFreshnessFilterState) => void;
};

function updateFilter(
  filters: SourceFreshnessFilterState,
  key: keyof SourceFreshnessFilterState,
  value: string,
): SourceFreshnessFilterState {
  return {
    ...filters,
    [key]: value,
  };
}

export function SourceFreshnessFilters({
  filters,
  sources,
  datasets,
  statuses,
  onChange,
}: SourceFreshnessFiltersProps) {
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5">
      <div>
        <p className="text-base font-semibold text-white">Filters</p>
        <p className="mt-1 text-sm text-slate-400">
          Narrow source freshness records by source, dataset, status, or stale
          state.
        </p>
      </div>

      <div className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <label className="block xl:col-span-2">
          <span className="text-sm font-medium text-slate-300">Search</span>
          <input
            value={filters.search}
            onChange={(event) =>
              onChange(updateFilter(filters, "search", event.target.value))
            }
            placeholder="Search source or dataset..."
            className="mt-2 w-full rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-white outline-none transition placeholder:text-slate-600 focus:border-slate-400"
          />
        </label>

        <SelectFilter
          label="Source"
          value={filters.source}
          onChange={(value) => onChange(updateFilter(filters, "source", value))}
          options={["", ...sources]}
        />

        <SelectFilter
          label="Dataset"
          value={filters.dataset}
          onChange={(value) =>
            onChange(updateFilter(filters, "dataset", value))
          }
          options={["", ...datasets]}
        />

        <SelectFilter
          label="Status"
          value={filters.status}
          onChange={(value) => onChange(updateFilter(filters, "status", value))}
          options={["", ...statuses]}
        />

        <SelectFilter
          label="Stale state"
          value={filters.staleState}
          onChange={(value) =>
            onChange(updateFilter(filters, "staleState", value))
          }
          options={["", "fresh", "stale", "unknown"]}
        />
      </div>
    </div>
  );
}

function SelectFilter({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: string;
  options: string[];
  onChange: (value: string) => void;
}) {
  return (
    <label className="block">
      <span className="text-sm font-medium text-slate-300">{label}</span>
      <select
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="mt-2 w-full rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-white outline-none transition focus:border-slate-400"
      >
        {options.map((option) => (
          <option key={option || "all"} value={option}>
            {option || "All"}
          </option>
        ))}
      </select>
    </label>
  );
}
