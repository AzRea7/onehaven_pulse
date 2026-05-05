"use client";

export type ScreenerFiltersState = {
  geoType: string;
  state: string;
  cyclePhase: string;
  investorSignal: string;
  minConfidence: string;
  minPriceGrowth: string;
  maxPriceGrowth: string;
  minRentGrowth: string;
  maxInventoryGrowth: string;
  maxPaymentToIncome: string;
};

type ScreenerFiltersProps = {
  filters: ScreenerFiltersState;
  onChange: (filters: ScreenerFiltersState) => void;
};

function updateFilter(
  filters: ScreenerFiltersState,
  key: keyof ScreenerFiltersState,
  value: string,
): ScreenerFiltersState {
  return {
    ...filters,
    [key]: value,
  };
}

export function ScreenerFilters({ filters, onChange }: ScreenerFiltersProps) {
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5">
      <p className="text-base font-semibold text-white">Filters</p>
      <p className="mt-1 text-sm text-slate-400">
        Server-backed market filters.
      </p>

      <div className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        <SelectFilter
          label="Geo type"
          value={filters.geoType}
          onChange={(value) => onChange(updateFilter(filters, "geoType", value))}
          options={[
            ["metro", "Metro"],
            ["state", "State"],
            ["county", "County"],
            ["place", "City / Place"],
            ["zcta", "ZCTA"],
          ]}
        />

        <TextFilter
          label="State"
          placeholder="MI"
          value={filters.state}
          onChange={(value) => onChange(updateFilter(filters, "state", value))}
        />

        <SelectFilter
          label="Cycle phase"
          value={filters.cyclePhase}
          onChange={(value) =>
            onChange(updateFilter(filters, "cyclePhase", value))
          }
          options={[
            ["", "Any"],
            ["Expansion", "Expansion"],
            ["Recovery", "Recovery"],
            ["Stabilizing", "Stabilizing"],
            ["Contraction", "Contraction"],
            ["Insufficient Data", "Insufficient Data"],
          ]}
        />

        <SelectFilter
          label="Investor signal"
          value={filters.investorSignal}
          onChange={(value) =>
            onChange(updateFilter(filters, "investorSignal", value))
          }
          options={[
            ["", "Any"],
            ["Buy Watch", "Buy Watch"],
            ["Hold", "Hold"],
            ["Caution", "Caution"],
            ["Avoid", "Avoid"],
            ["Insufficient Data", "Insufficient Data"],
          ]}
        />

        <NumberFilter
          label="Minimum confidence"
          placeholder="0.5"
          value={filters.minConfidence}
          onChange={(value) =>
            onChange(updateFilter(filters, "minConfidence", value))
          }
        />

        <NumberFilter
          label="Minimum price growth"
          placeholder="2"
          value={filters.minPriceGrowth}
          onChange={(value) =>
            onChange(updateFilter(filters, "minPriceGrowth", value))
          }
        />

        <NumberFilter
          label="Maximum price growth"
          placeholder="10"
          value={filters.maxPriceGrowth}
          onChange={(value) =>
            onChange(updateFilter(filters, "maxPriceGrowth", value))
          }
        />

        <NumberFilter
          label="Minimum rent growth"
          placeholder="2"
          value={filters.minRentGrowth}
          onChange={(value) =>
            onChange(updateFilter(filters, "minRentGrowth", value))
          }
        />

        <NumberFilter
          label="Maximum inventory growth"
          placeholder="5"
          value={filters.maxInventoryGrowth}
          onChange={(value) =>
            onChange(updateFilter(filters, "maxInventoryGrowth", value))
          }
        />

        <NumberFilter
          label="Max payment-to-income"
          placeholder="35"
          value={filters.maxPaymentToIncome}
          onChange={(value) =>
            onChange(updateFilter(filters, "maxPaymentToIncome", value))
          }
        />
      </div>
    </div>
  );
}

function TextFilter({
  label,
  value,
  placeholder,
  onChange,
}: {
  label: string;
  value: string;
  placeholder?: string;
  onChange: (value: string) => void;
}) {
  return (
    <label className="block">
      <span className="text-sm font-medium text-slate-300">{label}</span>
      <input
        value={value}
        placeholder={placeholder}
        onChange={(event) => onChange(event.target.value)}
        className="mt-2 w-full rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-white outline-none transition placeholder:text-slate-600 focus:border-slate-400"
      />
    </label>
  );
}

function NumberFilter(props: {
  label: string;
  value: string;
  placeholder?: string;
  onChange: (value: string) => void;
}) {
  return <TextFilter {...props} />;
}

function SelectFilter({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: string;
  options: Array<[string, string]>;
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
        {options.map(([optionValue, label]) => (
          <option key={optionValue || "any"} value={optionValue}>
            {label}
          </option>
        ))}
      </select>
    </label>
  );
}
