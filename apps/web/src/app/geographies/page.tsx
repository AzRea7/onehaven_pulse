import { GeographySearchBox } from "@/components/geographies/GeographySearchBox";

export default function GeographiesPage() {
  return (
    <div className="space-y-6 p-6">
      <div>
        <p className="text-sm text-slate-500">Geographies</p>
        <h1 className="text-2xl font-semibold text-slate-950">
          Geography search
        </h1>
        <p className="mt-2 max-w-3xl text-sm text-slate-600">
          Search canonical jurisdictions and navigate through the geography relationship graph.
        </p>
      </div>

      <GeographySearchBox />
    </div>
  );
}
